// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package client provides a bounded HTTPS client for Kelvo queries and operations.
package client

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/SYNEHQ/kelvo-go/query"
)

// Config is immutable after New. Zero bounds use conservative defaults.
// TLSConfig must verify certificates and permit TLS 1.3. Client clones it.
type Config struct {
	URL, BearerToken                       string
	TLSConfig                              *tls.Config
	Timeout                                time.Duration
	MaxConcurrent, MaxControlConcurrent    int
	MaxRows, MaxDecodedBytes, MaxWireBytes int64
}

// Authority is scoped to one call. Set only the grant for that endpoint family.
// BearerToken overrides the client default; no credentials belong in query SQL.
type Authority struct {
	BearerToken    string
	Delegation     string
	OperationGrant string
	InputGrant     string
}

// QueryHandle identifies an accepted query, not a completed result.
type QueryHandle struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// QueryStatus is the server's bounded control-plane status response.
type QueryStatus struct {
	ID    string       `json:"id"`
	State string       `json:"state"`
	Stats query.Stats  `json:"stats"`
	Error *query.Error `json:"error,omitempty"`
}

// Limits can only reduce the client-wide limits. Each field must be positive.
type Limits struct {
	MaxRows, MaxDecodedBytes, MaxWireBytes int64
}

// Stats describes bytes received and batches handed to the sink. On error these
// are partial observations, not proof of a complete result. HTTP headers and
// submission/cancellation bodies are excluded from WireBytes.
type Stats struct {
	Batches      int64         `json:"batches"`
	Server       query.Stats   `json:"server"`
	Rows         int64         `json:"rows"`
	DecodedBytes int64         `json:"decoded_bytes"`
	WireBytes    int64         `json:"wire_bytes"`
	Elapsed      time.Duration `json:"elapsed_ns"`
}

// Sink receives borrowed Arrow batches synchronously.
type Sink = query.Sink

// Error never contains remote diagnostics, SQL, source names or credentials.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Message }

func (e *Error) Unwrap() error {
	switch e.Code {
	case "CANCELLED":
		return context.Canceled
	case "DEADLINE_EXCEEDED":
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func failure(code string) *Error {
	message := "Kelvo service unavailable"
	switch code {
	case "INVALID_CONFIG":
		message = "Invalid Kelvo service configuration"
	case "INVALID_ARGUMENT":
		message = "Invalid Kelvo query request"
	case "RESOURCE_EXHAUSTED":
		message = "Kelvo query limit reached"
	case "PROTOCOL_ERROR":
		message = "Kelvo response was incomplete or invalid"
	case "CANCELLED":
		message = "Kelvo query cancelled"
	case "DEADLINE_EXCEEDED":
		message = "Kelvo query deadline exceeded"
	case "UNAUTHENTICATED":
		message = "Kelvo service authentication failed"
	case "PERMISSION_DENIED":
		message = "Kelvo query access denied"
	case "QUERY_FAILED":
		message = "Kelvo query failed"
	case "NOT_FOUND":
		message = "Kelvo resource not found"
	case "CLIENT_CLOSED":
		message = "Kelvo client is closed"
	case "SINK_FAILED":
		message = "Kelvo result delivery failed"
	default:
		code = "UNAVAILABLE"
	}
	return &Error{Code: code, Message: message}
}
