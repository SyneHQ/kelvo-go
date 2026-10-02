// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package federation provides bounded source-side scans to DuckDB's Arrow bridge.
package federation

import (
	"context"
	"sync"
	"sync/atomic"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type execution interface {
	query.Executor
	Close() error
}
type executorFactory func(catalog.Config, query.Limits) (execution, error)

// Table describes one operator-selected relation. Every concurrent scan owns
// its native executor, source connection, cancellation and one Arrow batch handoff.
type Table struct {
	ctx                         context.Context
	cancel                      context.CancelFunc
	schema                      *arrow.Schema
	columns                     map[string]arrow.Field
	remoteName, sourceID        string
	dialect                     scanDialect
	limits                      query.Limits
	config                      catalog.Config
	factory                     executorFactory
	customDriver                federationapi.Driver
	selected                    catalog.FederationTable
	budget                      *scanBudget
	mu                          sync.Mutex
	closed                      bool
	active                      map[*scanReader]struct{}
	producers                   sync.WaitGroup
	closeDone                   chan struct{}
	closeOnce                   sync.Once
	rows, bytes, batches, scans atomic.Int64
	sourceWireBytes             atomic.Int64
}

// Stats counts source batches accepted for handoff, including a batch abandoned
// by a cancelling consumer. Describe queries and rejected batches are excluded;
// Bytes measures Arrow buffers, not HTTP framing or source storage read bytes.
// SourceWireBytes additionally includes encoded body bytes consumed by rejected
// or incomplete scans; it is available after each producer finishes.
type Stats struct {
	Rows            int64 `json:"rows"`
	Bytes           int64 `json:"bytes"`
	Batches         int64 `json:"batches"`
	SourceWireBytes int64 `json:"source_wire_bytes,omitempty"`
	Scans           int64 `json:"scans"`
}

func New(ctx context.Context, source catalog.Source, table catalog.FederationTable, limits query.Limits) (*Table, error) {
	dialect, err := dialectFor(source.Type)
	if err != nil {
		if driver, ok := federationapi.Lookup(source.Type); ok {
			return newCustomTable(ctx, source, table, limits, driver)
		}
		return nil, err
	}
	return newTable(ctx, source, table, limits, dialect.executor)
}
func newTable(ctx context.Context, source catalog.Source, table catalog.FederationTable, limits query.Limits, factory executorFactory) (*Table, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if err := source.ValidateFederation(); err != nil {
		return nil, err
	}
	if source.Federation == nil || !catalog.ValidID(source.ID) {
		return nil, query.NewError("PERMISSION_DENIED", "Source does not expose federated tables")
	}
	dialect, err := dialectFor(source.Type)
	if err != nil {
		return nil, err
	}
	found := false
	for _, allowed := range source.Federation.Tables {
		if allowed == table {
			found = true
			break
		}
	}
	if !found {
		return nil, query.NewError("PERMISSION_DENIED", "Federated table is not registered")
	}
	if source.Federation.MaxScanRows > 0 {
		limits.MaxRows = source.Federation.MaxScanRows
	}
	if source.Federation.MaxScanBytes > 0 {
		limits.MaxBytes = source.Federation.MaxScanBytes
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	remoteName, err := dialect.tableName(table)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(ctx)
	t := &Table{ctx: lifetime, cancel: cancel, remoteName: remoteName, sourceID: source.ID, dialect: dialect, limits: limits, config: catalog.Config{Sources: []catalog.Source{source}}, factory: factory, active: make(map[*scanReader]struct{}), closeDone: make(chan struct{}), budget: budgetFromContext(ctx)}
	executor, err := factory(t.config, limits)
	if err != nil {
		cancel()
		return nil, err
	}
	sink := &describeSink{}
	_, err = executor.Execute(lifetime, query.Request{Mode: "native", ConnectionID: source.ID, SQL: dialect.describeSQL(t.remoteName)}, sink)
	closeErr := executor.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = sink.validate()
	}
	if err != nil {
		cancel()
		return nil, err
	}
	t.schema, t.columns = sink.schema, make(map[string]arrow.Field, sink.schema.NumFields())
	for _, field := range sink.schema.Fields() {
		if _, duplicate := t.columns[field.Name]; duplicate || field.Type == nil {
			cancel()
			return nil, query.NewError("UNSUPPORTED", "Federation source has invalid or duplicate column names")
		}
		if _, err := dialect.quoteIdentifier(field.Name); err != nil {
			cancel()
			return nil, err
		}
		t.columns[field.Name] = field
	}
	return t, nil
}
func (t *Table) Schema() *arrow.Schema { return t.schema }
func (t *Table) Stats() Stats {
	return Stats{Rows: t.rows.Load(), Bytes: t.bytes.Load(), Batches: t.batches.Load(), Scans: t.scans.Load(), SourceWireBytes: t.sourceWireBytes.Load()}
}
func (t *Table) Scan(ctx context.Context, plan duckbridge.ScanPlan) (array.RecordReader, error) {
	var sql string
	var schema *arrow.Schema
	var err error
	if t.customDriver != nil {
		plan, schema, err = t.prepareCustomScan(plan)
	} else {
		sql, schema, err = t.compileScan(plan)
	}
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.ctx.Err() != nil {
		return nil, query.NewError("CANCELLED", "Federation table is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, query.PublicError(err)
	}
	if !t.budget.acquire() {
		return nil, query.NewError("RESOURCE_EXHAUSTED", "Federation concurrent scan budget is full")
	}
	var executor execution
	if t.customDriver != nil {
		executor = &customExecution{driver: t.customDriver, source: publicSource(t.config.Sources[0]), table: publicTable(t.selected), limits: publicLimits(t.limits), plan: plan, schema: t.schema}
	} else {
		executor, err = t.factory(t.config, t.limits)
	}
	if err != nil {
		t.budget.release()
		return nil, err
	}
	reader := newScanReader(t, ctx, schema)
	t.active[reader] = struct{}{}
	t.scans.Add(1)
	t.producers.Add(1)
	go reader.produce(executor, query.Request{Mode: "native", ConnectionID: t.sourceID, SQL: sql})
	return reader, nil
}

// Close cancels and joins all active producers. Retained caller batches remain
// valid until their owners Release them; it never frees borrowed caller data.
func (t *Table) Close() error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		t.cancel()
		t.mu.Unlock()
		t.producers.Wait()
		close(t.closeDone)
	})
	<-t.closeDone
	return nil
}

type describeSink struct{ schema *arrow.Schema }

func (s *describeSink) Schema(schema *arrow.Schema) error {
	if s.schema != nil || schema == nil {
		return query.NewError("QUERY_FAILED", "Federation source returned an invalid schema")
	}
	s.schema = schema
	return nil
}
func (s *describeSink) Write(record arrow.RecordBatch) error {
	if record == nil || record.NumRows() != 0 {
		return query.NewError("QUERY_FAILED", "Federation schema query unexpectedly returned rows")
	}
	return nil
}
func (s *describeSink) validate() error {
	if s.schema == nil || s.schema.NumFields() == 0 || s.schema.NumFields() > 1024 {
		return query.NewError("UNSUPPORTED", "Federation source schema is unavailable or too wide")
	}
	return nil
}
