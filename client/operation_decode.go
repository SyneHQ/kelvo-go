// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// ReadOperationResult verifies bounded Arrow framing, decoded buffers, exact
// receipt rows/bytes/hash and physical EOF. It performs no source execution.
// A nil error is the only signal that callers may finish their output stream.
func (c *Client) ReadOperationResult(ctx context.Context, expected operations.Response, auth Authority, sink Sink, limits Limits) (stats Stats, err error) {
	started := time.Now()
	defer func() { stats.Elapsed = time.Since(started) }()
	if c == nil || ctx == nil || sink == nil || expected.Receipt == nil || expected.Receipt.Result == nil || expected.Receipt.Result.Format != "arrow_ipc" || limits.MaxRows < 1 || limits.MaxDecodedBytes < 1 || limits.MaxWireBytes < 1 || limits.MaxRows > c.cfg.MaxRows || limits.MaxDecodedBytes > c.cfg.MaxDecodedBytes || limits.MaxWireBytes > c.cfg.MaxWireBytes {
		return stats, failure("INVALID_ARGUMENT")
	}
	if expected.Receipt.Result.Rows > limits.MaxRows || expected.Receipt.Result.Bytes > limits.MaxWireBytes {
		return stats, failure("RESOURCE_EXHAUSTED")
	}
	result, release, err := c.openOperationResult(ctx, expected, auth, true)
	if err != nil {
		return stats, err
	}
	defer release()
	defer result.Close()
	cfg := c.cfg
	cfg.MaxRows, cfg.MaxDecodedBytes, cfg.MaxWireBytes = limits.MaxRows, limits.MaxDecodedBytes, limits.MaxWireBytes
	stats, err = decodeStream(result.ctx, result, sink, cfg)
	if err != nil {
		return stats, err
	}
	if stats.Rows != expected.Receipt.Result.Rows || !result.Verified() {
		return stats, failure("PROTOCOL_ERROR")
	}
	err = result.Complete(func() error { return nil })
	return stats, err
}

// DecodeOperationResult consumes and verifies an immutable Arrow operation result.
func (c *Client) DecodeOperationResult(ctx context.Context, expected operations.Response, auth Authority, sink Sink) (Stats, error) {
	if c == nil {
		return Stats{}, failure("CLIENT_CLOSED")
	}
	return c.ReadOperationResult(ctx, expected, auth, sink, c.defaultLimits())
}
