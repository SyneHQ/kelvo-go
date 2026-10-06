// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package business

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Driver struct{ Engine string }

func (d Driver) Capabilities() operations.Capabilities {
	ops := []operations.Capability{
		{Kind: operations.NativeRead, ParameterTypes: []string{"json"}, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.ConnectionTest, Idempotency: "none", Cancellation: "best_effort"},
	}
	if d.Engine != "ramp" {
		ops = append(ops, operations.Capability{Kind: operations.NativeExecute, ParameterTypes: []string{"json"}, Idempotency: "none", Cancellation: "best_effort"})
	}
	return operations.Capabilities{Version: operations.Version, Engine: d.Engine, Operations: ops}
}

func (d Driver) Open(ctx context.Context, c adapter.Connection) (adapter.Session, error) {
	if ctx == nil || ctx.Err() != nil || !provider.Supported(d.Engine) || c.Engine != d.Engine || c.TenantID == "" || c.ConnectionID == "" || c.Revision == "" || c.Host != "" || c.Port != 0 || c.Endpoint != "" && d.Engine != "posthog" || c.Password != "" || c.TLS != nil || c.Schema != "" {
		return nil, adapter.ErrInvalid
	}
	if c.Token == "" || len(c.Token) > 32<<10 || strings.ContainsAny(c.Token+c.Username, "\x00\r\n") || len(c.Username) > 1024 || c.Username != "" && d.Engine != "ramp" {
		return nil, adapter.ErrInvalid
	}
	for key := range c.Options {
		if key != "environment" || d.Engine == "posthog" {
			return nil, adapter.ErrUnsupported
		}
	}
	client, err := New(Config{Type: d.Engine, Environment: c.Options["environment"], Token: c.Token, ClientID: c.Username, Endpoint: c.Endpoint, Project: c.Namespace})
	if err != nil {
		return nil, adapter.ErrInvalid
	}
	return &Session{client: client, engine: d.Engine}, nil
}

type Session struct {
	mu     sync.Mutex
	client *Client
	engine string
}

func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		s.client.http.CloseIdleConnections()
		s.client.config.Token = ""
		s.client = nil
	}
	return nil
}
func (s *Session) Test(ctx context.Context) error {
	if s == nil || ctx == nil {
		return adapter.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		return adapter.ErrInvalid
	}
	query := `{"operation":"list_tools"}`
	if s.engine == "posthog" {
		query = `{"method":"GET","path":"/api/projects/:project_id/"}`
	}
	_, err := s.client.Query(ctx, query)
	return err
}

func (s *Session) RunNative(ctx context.Context, n adapter.Native, sink adapter.Sink) (adapter.NativeResult, error) {
	result := adapter.NativeResult{Outcome: operations.Rejected, Effect: operations.EffectNone}
	if s == nil || ctx == nil || n.Validate() != nil || n.Spec.Provider != s.engine || n.Spec.Command != "query" || len(n.Spec.Parameters) != 1 || n.Spec.Parameters[0].Type != "json" {
		return result, adapter.ErrInvalid
	}
	wantsResult := n.Kind == operations.NativeRead || n.Spec.ReturnResult
	if wantsResult != (sink != nil) {
		return result, adapter.ErrInvalid
	}
	raw := n.Spec.Parameters[0].Value
	kind, _, err := provider.Invocation(s.engine, raw)
	if err != nil || kind != n.Kind {
		return result, adapter.ErrUnsupported
	}
	if ctx.Err() != nil {
		result.Outcome = operations.CancelledBeforeStart
		return result, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		return result, adapter.ErrInvalid
	}
	rows, err := s.client.Query(ctx, string(raw))
	if n.Kind == operations.NativeExecute {
		switch {
		case s.client.confirmed:
			result.Outcome, result.Effect = operations.Completed, operations.EffectCommitted
		case s.client.dispatched:
			result.Outcome, result.Effect = operations.OutcomeUnknown, operations.EffectUnknown
		default:
			result.Outcome = operations.Failed
		}
	} else {
		result.Outcome = operations.Failed
	}
	if err != nil {
		return result, err
	}
	if wantsResult {
		result.Stats, err = writeObjects(ctx, rows, n.Limits, sink)
		if err != nil {
			return result, err
		}
	}
	result.Outcome = operations.Completed
	return result, nil
}

func writeObjects(ctx context.Context, rows []map[string]any, limits adapter.Limits, sink adapter.Sink) (stats adapter.QueryStats, err error) {
	started := time.Now()
	defer func() { stats.Elapsed = time.Since(started) }()
	if int64(len(rows)) > limits.MaxRows {
		return stats, adapter.ErrLimit
	}
	meta := arrow.NewMetadata([]string{"kelvo_document_format"}, []string{provider.ResultFormat})
	schema := arrow.NewSchema([]arrow.Field{{Name: "document", Type: arrow.BinaryTypes.Binary}}, &meta)
	if err = sink.Schema(schema); err != nil {
		return stats, err
	}
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer b.Release()
	flush := func() error {
		values := b.NewArray()
		defer values.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{values}, int64(values.Len()))
		defer record.Release()
		if err := sink.Write(record); err != nil {
			return err
		}
		stats.Rows += record.NumRows()
		return nil
	}
	for _, row := range rows {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		raw, e := json.Marshal(row)
		if e != nil {
			return stats, adapter.ErrInvalid
		}
		size := int64(len(raw) + 8)
		if size > limits.MaxBytes-stats.Bytes {
			return stats, adapter.ErrLimit
		}
		stats.Bytes += size
		b.Append(raw)
		if b.Len() == limits.BatchRows {
			if err = flush(); err != nil {
				return stats, err
			}
		}
	}
	if b.Len() > 0 {
		err = flush()
	}
	return stats, err
}
