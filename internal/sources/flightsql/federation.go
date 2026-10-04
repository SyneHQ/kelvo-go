// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package flightsql

import (
	"context"
	"errors"
	"strings"
	"sync"
	"unicode/utf8"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

// FederationDriver exposes registered ANSI SQL tables through the public
// federation contract. It has no shared clients, credentials or schema cache.
// Only projection is pushed down; every required predicate is refused.
type FederationDriver struct{}

func (FederationDriver) FederationCapabilities() federationapi.Capabilities {
	return federationapi.Capabilities{Version: federationapi.CapabilityVersion, Projection: true}
}

func (FederationDriver) Validate(source federationapi.Source, table federationapi.Table) error {
	if source.Type != "arrow_flight" {
		return federationapi.ErrUnsupported
	}
	return federationSource(source, table).ValidateFederation()
}

func federationSource(source federationapi.Source, table federationapi.Table) catalog.Source {
	options := make(map[string]string, len(source.Options))
	for key, value := range source.Options {
		options[key] = value
	}
	return catalog.Source{
		ID: source.ID, Type: source.Type, DSNEnv: source.DSNEnv, URLEnv: source.URLEnv,
		UsernameEnv: source.UsernameEnv, PasswordEnv: source.PasswordEnv, TokenEnv: source.TokenEnv,
		Options: options, Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{
			Name: table.Name, Database: table.Database, Schema: table.Schema, Table: table.Table,
		}}},
	}
}

func (FederationDriver) Open(ctx context.Context, source federationapi.Source, table federationapi.Table, limits federationapi.Limits) (federationapi.Relation, error) {
	relation, err := openFederation(ctx, source, table, limits, New)
	if err != nil {
		return nil, err
	}
	return relation, nil
}

// The factory keeps the existing native engine's TLS test seam local to this
// package. Production always uses New; no query or configuration can replace it.
func openFederation(ctx context.Context, source federationapi.Source, table federationapi.Table, limits federationapi.Limits, factory func(catalog.Config, query.Limits) (*Engine, error)) (_ *federationRelation, err error) {
	selected := federationSource(source, table)
	if source.Type != "arrow_flight" {
		return nil, federationapi.ErrUnsupported
	}
	if err := selected.ValidateFederation(); err != nil {
		return nil, err
	}
	bound := query.DefaultLimits()
	bound.MaxRows, bound.MaxBytes, bound.Timeout = limits.MaxRows, limits.MaxBytes, limits.Timeout
	bound.MemoryMB, bound.Threads = limits.MemoryMB, limits.Threads
	if err := bound.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Static validation precedes New, which resolves the source URL. Native
	// Execute owns the verified TLS client, token and decoder for each request.
	engine, err := factory(catalog.Config{Sources: []catalog.Source{selected}}, bound)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(ctx)
	r := &federationRelation{engine: engine, sourceID: selected.ID,
		remoteName: `"` + table.Schema + `"."` + table.Table + `"`,
		ctx:        lifetime, cancel: cancel, closeDone: make(chan struct{})}
	defer func() {
		if err != nil {
			_ = r.Close()
		}
	}()
	sink := &federationDiscovery{}
	_, err = engine.Execute(lifetime, query.Request{Mode: "native", ConnectionID: selected.ID,
		SQL: "SELECT * FROM " + r.remoteName + " WHERE 1 = 0"}, sink)
	if err != nil {
		return nil, err
	}
	if sink.schema == nil {
		return nil, errors.New("Flight SQL federation discovery returned no schema")
	}
	r.schema = sink.schema
	r.columns = make(map[string]arrow.Field, r.schema.NumFields())
	for _, field := range r.schema.Fields() {
		r.columns[field.Name] = field
	}
	return r, nil
}

type federationRelation struct {
	engine               *Engine
	sourceID, remoteName string
	schema               *arrow.Schema
	columns              map[string]arrow.Field
	ctx                  context.Context
	cancel               context.CancelFunc
	mu                   sync.Mutex
	scanDone             chan struct{}
	closed               bool
	closeDone            chan struct{}
	closeErr             error
}

func (r *federationRelation) Schema() *arrow.Schema { return r.schema }

func (r *federationRelation) Scan(parent context.Context, plan federationapi.ScanPlan, sink federationapi.Sink) (federationapi.ScanStats, error) {
	// Repeat the core's refusal for direct SDK callers, before any source call.
	if len(plan.Filters) != 0 || len(plan.Columns) == 0 || len(plan.Columns) > 1024 || sink == nil {
		return federationapi.ScanStats{}, federationapi.ErrUnsupported
	}
	fields := make([]arrow.Field, len(plan.Columns))
	quoted := make([]string, len(plan.Columns))
	for i, name := range plan.Columns {
		field, ok := r.columns[name]
		if !ok {
			return federationapi.ScanStats{}, federationapi.ErrUnsupported
		}
		var err error
		quoted[i], err = federationIdentifier(name)
		if err != nil {
			return federationapi.ScanStats{}, err
		}
		fields[i] = field
	}
	sql := "SELECT " + strings.Join(quoted, ", ") + " FROM " + r.remoteName
	if len(sql) > 64<<10 {
		return federationapi.ScanStats{}, federationapi.ErrUnsupported
	}
	metadata := r.schema.Metadata()
	expected := arrow.NewSchema(fields, &metadata)
	r.mu.Lock()
	if r.closed || r.scanDone != nil {
		r.mu.Unlock()
		return federationapi.ScanStats{}, errors.New("Flight SQL federation relation is closed or busy")
	}
	if err := r.ctx.Err(); err != nil {
		r.mu.Unlock()
		return federationapi.ScanStats{}, err
	}
	if err := parent.Err(); err != nil {
		r.mu.Unlock()
		return federationapi.ScanStats{}, err
	}
	ctx, cancel := context.WithCancel(r.ctx)
	stop := context.AfterFunc(parent, cancel)
	if parent.Err() != nil {
		cancel()
	}
	done := make(chan struct{})
	r.scanDone = done
	r.mu.Unlock()
	defer func() {
		stop()
		cancel()
		r.mu.Lock()
		r.scanDone = nil
		close(done)
		r.mu.Unlock()
	}()
	checked := &federationProjection{sink: sink, expected: expected}
	_, err := r.engine.Execute(ctx, query.Request{Mode: "native", ConnectionID: r.sourceID, SQL: sql}, checked)
	// The native connector does not measure source wire bytes. Zero keeps that
	// unavailable measure distinct from Arrow-buffer accounting in the core.
	return federationapi.ScanStats{}, err
}

// Close waits for the native query's bounded cleanup. The core's synchronous
// sinks honor cancellation; a caller-owned sink must return before Close can.
func (r *federationRelation) Close() error {
	r.mu.Lock()
	if r.closed {
		done := r.closeDone
		r.mu.Unlock()
		<-done
		return r.closeErr
	}
	r.closed = true
	r.cancel()
	done := r.scanDone
	r.mu.Unlock()
	if done != nil {
		<-done
	}
	r.closeErr = r.engine.Close()
	close(r.closeDone)
	return r.closeErr
}

func federationIdentifier(name string) (string, error) {
	if name == "" || len(name) > 1024 || !utf8.ValidString(name) || strings.ContainsRune(name, '\\') {
		return "", federationapi.ErrUnsupported
	}
	for _, value := range name {
		if value < 32 || value == 127 {
			return "", federationapi.ErrUnsupported
		}
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`, nil
}

type federationDiscovery struct{ schema *arrow.Schema }

func (s *federationDiscovery) Schema(schema *arrow.Schema) error {
	if s.schema != nil || schema == nil || schema.NumFields() == 0 || schema.NumFields() > 1024 {
		return federationapi.ErrUnsupported
	}
	seen := make(map[string]bool, schema.NumFields())
	for _, field := range schema.Fields() {
		if _, err := federationIdentifier(field.Name); err != nil || field.Type == nil || seen[field.Name] {
			return federationapi.ErrUnsupported
		}
		seen[field.Name] = true
	}
	s.schema = schema
	return nil
}

func (s *federationDiscovery) Write(record arrow.RecordBatch) error {
	if record == nil || record.NumRows() != 0 || !federationSameSchema(s.schema, record.Schema()) {
		return errors.New("Flight SQL federation discovery returned data or changed schema")
	}
	return nil
}

type federationProjection struct {
	sink     federationapi.Sink
	expected *arrow.Schema
	seen     bool
}

func (s *federationProjection) Schema(schema *arrow.Schema) error {
	if s.seen || !federationSameSchema(s.expected, schema) {
		return errors.New("Flight SQL federation projection schema changed")
	}
	s.seen = true
	return s.sink.Schema(schema)
}

func (s *federationProjection) Write(record arrow.RecordBatch) error {
	if !s.seen || record == nil || !federationSameSchema(s.expected, record.Schema()) {
		return errors.New("Flight SQL federation projection batch changed schema")
	}
	return s.sink.Write(record)
}

func federationSameSchema(expected, actual *arrow.Schema) bool {
	return expected != nil && actual != nil && expected.Equal(actual) && expected.Metadata().Equal(actual.Metadata())
}

var _ federationapi.Driver = FederationDriver{}
var _ federationapi.CapabilityProvider = FederationDriver{}
var _ federationapi.Relation = (*federationRelation)(nil)
