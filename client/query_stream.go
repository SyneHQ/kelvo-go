// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
)

const (
	receiptTrailer  = "Kelvo-Query-Receipt"
	maxReceiptBytes = 8 << 10
	maxRetryAfter   = time.Minute
)

// ErrStreamUnsupported is returned when the node has no single-call read, so a
// caller can fall back to Query. Nothing was submitted.
var ErrStreamUnsupported = failure("UNSUPPORTED")

type streamReceipt struct {
	ID    string      `json:"id"`
	State string      `json:"state"`
	Stats query.Stats `json:"stats"`
}

// QueryStream is Query in one request: the node submits, waits and streams the
// Arrow result on one HTTP call, then ends it with a receipt trailer that is
// checked against what was received. Against a node without the endpoint it
// returns ErrStreamUnsupported before anything was submitted. A capacity
// refusal is a RESOURCE_EXHAUSTED Error whose RetryAfter is the node's hint.
func (c *Client) QueryStream(ctx context.Context, request query.Request, auth Authority, sink Sink) (Stats, error) {
	if c == nil {
		return Stats{}, failure("CLIENT_CLOSED")
	}
	return c.QueryStreamWithLimits(ctx, request, auth, sink, c.defaultLimits())
}

func (c *Client) QueryStreamWithLimits(ctx context.Context, request query.Request, auth Authority, sink Sink, limits Limits) (stats Stats, err error) {
	start := time.Now()
	defer func() { stats.Elapsed = time.Since(start) }()
	raw, auth, err := c.queryRequest(request, auth)
	if err != nil {
		return stats, err
	}
	if sink == nil || !c.validLimits(limits) {
		return stats, failure("INVALID_ARGUMENT")
	}
	ctx, release, err := c.begin(ctx, false)
	if err != nil {
		return stats, err
	}
	defer release()
	response, err := c.do(ctx, http.MethodPost, "/v1/queries/stream", auth, raw)
	if err != nil {
		return stats, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return stats, streamStatusFailure(response)
	}
	id := response.Header.Get("Kelvo-Query-Id")
	completion := response.Header.Values("Kelvo-Result-Completion")
	if !operationContentType(response, "application/vnd.apache.arrow.stream") || len(completion) != 1 || completion[0] != "durable-eos-v1" || !handleName.MatchString(id) {
		return stats, failure("PROTOCOL_ERROR")
	}
	if response.ContentLength > limits.MaxWireBytes {
		return stats, failure("RESOURCE_EXHAUSTED")
	}
	defer func() {
		// The node cancels what it cannot finish; this covers a caller that
		// stopped consuming a body that had started.
		if err != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer cancel()
			_, _ = c.cancelQuery(cleanup, id, auth)
		}
	}()
	cfg := c.cfg
	cfg.MaxRows, cfg.MaxDecodedBytes, cfg.MaxWireBytes = limits.MaxRows, limits.MaxDecodedBytes, limits.MaxWireBytes
	stats, err = decodeStream(ctx, response.Body, sink, cfg)
	if ctx.Err() != nil {
		return stats, contextFailure(ctx)
	}
	if err != nil {
		return stats, err
	}
	// Trailers are complete only after the body's EOF, which decodeStream read.
	var receipt streamReceipt
	value := response.Trailer.Get(receiptTrailer)
	if value == "" || len(value) > maxReceiptBytes || operations.DecodeStrict([]byte(value), &receipt, maxReceiptBytes) != nil {
		return stats, failure("PROTOCOL_ERROR")
	}
	if receipt.ID != id || receipt.State != "succeeded" || receipt.Stats.Rows != stats.Rows || receipt.Stats.Batches != stats.Batches || receipt.Stats.WireBytes != stats.WireBytes {
		return stats, failure("PROTOCOL_ERROR")
	}
	stats.Server = receipt.Stats
	return stats, nil
}

// streamStatusFailure maps a refusal before the first byte. A missing endpoint
// is not a missing query, so it must not surface as NOT_FOUND.
func streamStatusFailure(response *http.Response) error {
	switch response.StatusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return ErrStreamUnsupported
	case http.StatusTooManyRequests:
		e := failure("RESOURCE_EXHAUSTED")
		if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
			e.RetryAfter = min(time.Duration(seconds)*time.Second, maxRetryAfter)
		}
		return e
	}
	return statusFailure(response.StatusCode)
}
