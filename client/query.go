// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
)

func validQueryState(state string) bool {
	switch state {
	case "queued", "assigned", "claimed", "running", "streaming", "result_ready", "succeeded", "failed", "cancelled", "expired":
		return true
	}
	return false
}
func (c *Client) queryRequest(request query.Request, auth Authority) ([]byte, Authority, error) {
	if request.Delegation != "" && auth.Delegation != "" && request.Delegation != auth.Delegation {
		return nil, auth, failure("INVALID_ARGUMENT")
	}
	if auth.Delegation == "" {
		auth.Delegation = request.Delegation
	}
	request.Delegation = auth.Delegation
	auth, err := c.authority(auth, "query")
	if err != nil {
		return nil, auth, err
	}
	if query.ValidateRequest(request) != nil {
		return nil, auth, failure("INVALID_ARGUMENT")
	}
	raw, err := json.Marshal(request)
	if err != nil || len(raw) > maxRequestBytes {
		return nil, auth, failure("INVALID_ARGUMENT")
	}
	return raw, auth, nil
}
func (c *Client) submit(ctx context.Context, raw []byte, auth Authority) (QueryHandle, error) {
	response, err := c.do(ctx, http.MethodPost, "/v1/queries", auth, raw)
	if err != nil {
		return QueryHandle{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return QueryHandle{}, statusFailure(response.StatusCode)
	}

	if !operationContentType(response, "application/json") || response.ContentLength > maxHandleBytes {
		return QueryHandle{}, failure("PROTOCOL_ERROR")
	}
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, maxHandleBytes+1))
	var handle QueryHandle
	if len(raw) > maxHandleBytes || operations.DecodeStrict(raw, &handle, maxHandleBytes) != nil || !handleName.MatchString(handle.ID) || (handle.State != "queued" && handle.State != "assigned") {
		return QueryHandle{}, failure("PROTOCOL_ERROR")
	}
	if readErr != nil {
		return handle, failure("PROTOCOL_ERROR")
	}

	return handle, nil
}

// Submit sends one query. Its accepted handle is not proof of query completion.
// A verified handle may accompany an envelope EOF error; it can be cancelled,
// but the submission must not be replayed automatically.
func (c *Client) Submit(ctx context.Context, request query.Request, auth Authority) (QueryHandle, error) {
	raw, auth, err := c.queryRequest(request, auth)
	if err != nil {
		return QueryHandle{}, err
	}
	ctx, release, err := c.begin(ctx, false)
	if err != nil {
		return QueryHandle{}, err
	}
	defer release()
	return c.submit(ctx, raw, auth)
}
func (c *Client) status(ctx context.Context, id string, auth Authority) (QueryStatus, error) {
	response, err := c.do(ctx, http.MethodGet, "/v1/queries/"+id, auth, nil)
	if err != nil {
		return QueryStatus{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return QueryStatus{}, statusFailure(response.StatusCode)
	}
	var status QueryStatus
	if err = decodeJSON(response, &status, maxStatusBytes); err != nil {
		return QueryStatus{}, err
	}
	if status.ID != id || !validQueryState(status.State) || status.Stats.Rows < 0 || status.Stats.Batches < 0 || status.Stats.Bytes < 0 || status.Stats.WireBytes < 0 {
		return QueryStatus{}, failure("PROTOCOL_ERROR")
	}
	// Never surface arbitrary upstream diagnostic strings through SDK errors.
	if status.Error != nil {
		safe := failure(status.Error.Code)
		status.Error = &query.Error{Code: safe.Code, Message: safe.Message}
	}
	return status, nil
}

// Status reads the current state without consuming results or replaying work.
func (c *Client) Status(ctx context.Context, id string, auth Authority) (QueryStatus, error) {
	auth, err := c.authority(auth, "query")
	if err != nil {
		return QueryStatus{}, err
	}
	if !handleName.MatchString(id) {
		return QueryStatus{}, failure("INVALID_ARGUMENT")
	}
	ctx, release, err := c.begin(ctx, true)
	if err != nil {
		return QueryStatus{}, err
	}
	defer release()
	return c.status(ctx, id, auth)
}
func (c *Client) cancelQuery(ctx context.Context, id string, auth Authority) (QueryHandle, error) {
	response, err := c.do(ctx, http.MethodPost, "/v1/queries/"+id+"/cancel", auth, []byte(`{}`))
	if err != nil {
		return QueryHandle{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return QueryHandle{}, statusFailure(response.StatusCode)
	}
	var handle QueryHandle
	if err = decodeJSON(response, &handle, maxHandleBytes); err != nil {
		return QueryHandle{}, err
	}
	if handle.ID != id || !validQueryState(handle.State) {
		return QueryHandle{}, failure("PROTOCOL_ERROR")
	}
	return handle, nil
}

// Cancel requests cancellation once. Poll Status to inspect its resulting state.
func (c *Client) Cancel(ctx context.Context, id string, auth Authority) (QueryHandle, error) {
	auth, err := c.authority(auth, "query")
	if err != nil {
		return QueryHandle{}, err
	}
	if !handleName.MatchString(id) {
		return QueryHandle{}, failure("INVALID_ARGUMENT")
	}
	ctx, release, err := c.begin(ctx, true)
	if err != nil {
		return QueryHandle{}, err
	}
	defer release()
	return c.cancelQuery(ctx, id, auth)
}
func (c *Client) results(ctx context.Context, id string, auth Authority, sink Sink, limits Limits) (stats Stats, err error) {
	response, err := c.do(ctx, http.MethodGet, "/v1/queries/"+id+"/results", auth, nil)
	if err != nil {
		return stats, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return stats, statusFailure(response.StatusCode)
	}
	completion := response.Header.Values("Kelvo-Result-Completion")
	if !operationContentType(response, "application/vnd.apache.arrow.stream") || len(completion) > 1 || (len(completion) == 1 && completion[0] != "durable-eos-v1") {
		return stats, failure("PROTOCOL_ERROR")
	}
	if response.ContentLength > limits.MaxWireBytes {
		return stats, failure("RESOURCE_EXHAUSTED")
	}
	cfg := c.cfg
	cfg.MaxRows, cfg.MaxDecodedBytes, cfg.MaxWireBytes = limits.MaxRows, limits.MaxDecodedBytes, limits.MaxWireBytes
	stats, err = decodeStream(ctx, response.Body, sink, cfg)
	if ctx.Err() != nil {
		return stats, contextFailure(ctx)
	}
	if err != nil {
		return stats, err
	}
	// Standalone does not emit the durable completion header. Both modes must
	// confirm terminal state after physical Arrow EOS and the HTTP body's EOF.
	status, err := c.status(ctx, id, auth)
	if err != nil {
		return stats, err
	}
	if status.State != "succeeded" || status.Error != nil || status.Stats.Rows != stats.Rows || status.Stats.Batches != stats.Batches || status.Stats.WireBytes != stats.WireBytes {
		return stats, failure("PROTOCOL_ERROR")
	}
	stats.Server = status.Stats
	return stats, nil
}

// Results consumes a single-consumer Arrow result exactly once. A nil error
// verifies framing, HTTP EOF, terminal status and observed row/batch/byte counts.
// Batches remain borrowed until the synchronous Sink.Write call returns.
func (c *Client) Results(ctx context.Context, id string, auth Authority, sink Sink) (Stats, error) {
	if c == nil {
		return Stats{}, failure("CLIENT_CLOSED")
	}
	return c.ResultsWithLimits(ctx, id, auth, sink, c.defaultLimits())
}

// ResultsWithLimits can reduce, but cannot increase, the client's resource bounds.
func (c *Client) ResultsWithLimits(ctx context.Context, id string, auth Authority, sink Sink, limits Limits) (stats Stats, err error) {
	start := time.Now()
	defer func() { stats.Elapsed = time.Since(start) }()
	auth, err = c.authority(auth, "query")
	if err != nil {
		return stats, err
	}
	if !handleName.MatchString(id) || sink == nil || !c.validLimits(limits) {
		return stats, failure("INVALID_ARGUMENT")
	}
	ctx, release, err := c.begin(ctx, false)
	if err != nil {
		return stats, err
	}
	defer release()
	return c.results(ctx, id, auth, sink, limits)
}

// Query submits once and streams the result within one admission and deadline.
// On failure after acceptance, it makes one bounded cancellation attempt.
func (c *Client) Query(ctx context.Context, request query.Request, auth Authority, sink Sink) (Stats, error) {
	if c == nil {
		return Stats{}, failure("CLIENT_CLOSED")
	}
	return c.QueryWithLimits(ctx, request, auth, sink, c.defaultLimits())
}
func (c *Client) QueryWithLimits(ctx context.Context, request query.Request, auth Authority, sink Sink, limits Limits) (stats Stats, err error) {
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
	handle, err := c.submit(ctx, raw, auth)
	defer func() {
		if err != nil && handle.ID != "" {
			cleanup, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer cancel()
			_, _ = c.cancelQuery(cleanup, handle.ID, auth)
		}
	}()
	if err != nil {
		return stats, err
	}
	return c.results(ctx, handle.ID, auth, sink, limits)
}
