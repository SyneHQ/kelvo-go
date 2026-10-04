// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

func publicSource(source catalog.Source) federationapi.Source {
	options := make(map[string]string, len(source.Options))
	for key, value := range source.Options {
		options[key] = value
	}
	return federationapi.Source{ID: source.ID, Type: source.Type, DSNEnv: source.DSNEnv, URLEnv: source.URLEnv, UsernameEnv: source.UsernameEnv, PasswordEnv: source.PasswordEnv, TokenEnv: source.TokenEnv, Options: options}
}
func publicTable(table catalog.FederationTable) federationapi.Table {
	return federationapi.Table{Name: table.Name, Database: table.Database, Schema: table.Schema, Table: table.Table}
}
func publicLimits(limits query.Limits) federationapi.Limits {
	return federationapi.Limits{MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, Timeout: limits.Timeout, MemoryMB: limits.MemoryMB, Threads: limits.Threads}
}

func newCustomTable(ctx context.Context, source catalog.Source, selected catalog.FederationTable, limits query.Limits, driver federationapi.Driver) (*Table, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if err := source.ValidateFederation(); err != nil {
		return nil, query.NewError("CONFIGURATION_ERROR", "Invalid custom federation source")
	}
	if source.Federation == nil {
		return nil, query.NewError("PERMISSION_DENIED", "Source does not expose federated tables")
	}
	found := false
	for _, table := range source.Federation.Tables {
		if table == selected {
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
	lifetime, cancel := context.WithCancel(ctx)
	t := &Table{ctx: lifetime, cancel: cancel, sourceID: source.ID, limits: limits, config: catalog.Config{Sources: []catalog.Source{source}}, customDriver: driver, selected: selected, active: make(map[*scanReader]struct{}), closeDone: make(chan struct{}), budget: budgetFromContext(ctx)}
	discovery, stop := context.WithTimeout(lifetime, limits.Timeout)
	defer stop()
	schema, err := discoverCustom(discovery, driver, publicSource(source), publicTable(selected), publicLimits(limits))
	if err != nil {
		cancel()
		return nil, err
	}
	t.schema, t.columns = schema, make(map[string]arrow.Field, schema.NumFields())
	for _, field := range schema.Fields() {
		t.columns[field.Name] = field
	}
	return t, nil
}

func discoverCustom(ctx context.Context, driver federationapi.Driver, source federationapi.Source, table federationapi.Table, limits federationapi.Limits) (schema *arrow.Schema, err error) {
	var relation federationapi.Relation
	defer func() {
		if recover() != nil {
			err = query.NewError("QUERY_FAILED", "Custom federation adapter failed")
		}
		if relation != nil {
			if closeErr := closeCustom(relation); err == nil {
				err = closeErr
			}
		}
		if ctx.Err() != nil {
			err = query.PublicError(ctx.Err())
		}
	}()
	relation, err = driver.Open(ctx, source, table, limits)
	if err != nil {
		return nil, customError(err)
	}
	if relation == nil {
		return nil, query.NewError("QUERY_FAILED", "Custom federation adapter returned no relation")
	}
	schema = relation.Schema()
	if err := validateCustomSchema(schema); err != nil {
		return nil, err
	}
	return schema, nil
}

func validateCustomSchema(schema *arrow.Schema) error {
	invalid := query.NewError("UNSUPPORTED", "Custom federation schema is unavailable or invalid")
	if schema == nil || schema.NumFields() == 0 || schema.NumFields() > 1024 {
		return invalid
	}
	seen := make(map[string]bool, schema.NumFields())
	for _, field := range schema.Fields() {
		if field.Name == "" || len(field.Name) > 1024 || !utf8.ValidString(field.Name) || strings.IndexFunc(field.Name, func(r rune) bool { return r < 32 || r == 127 }) >= 0 || seen[field.Name] || field.Type == nil {
			return invalid
		}
		seen[field.Name] = true
	}
	return nil
}

type customExecution struct {
	driver   federationapi.Driver
	source   federationapi.Source
	table    federationapi.Table
	limits   federationapi.Limits
	plan     federationapi.ScanPlan
	schema   *arrow.Schema
	relation federationapi.Relation
}

func (e *customExecution) Execute(parent context.Context, _ query.Request, sink query.Sink) (stats query.Stats, err error) {
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	defer func() {
		if recover() != nil {
			err = query.NewError("QUERY_FAILED", "Custom federation adapter failed")
		}
		if ctx.Err() != nil {
			err = query.PublicError(ctx.Err())
		}
	}()
	e.relation, err = e.driver.Open(ctx, e.source, e.table, e.limits)
	if err != nil {
		return stats, customError(err)
	}
	if e.relation == nil {
		return stats, query.NewError("QUERY_FAILED", "Custom federation adapter returned no relation")
	}
	actual := e.relation.Schema()
	if actual == nil || !sameSchema(e.schema, actual) {
		return stats, query.NewError("QUERY_FAILED", "Custom federation source schema changed during scan")
	}
	checked := &customSink{Sink: sink}
	result, err := e.relation.Scan(ctx, e.plan, checked)
	if result.SourceWireBytes < 0 {
		return stats, query.NewError("QUERY_FAILED", "Custom federation source returned invalid statistics")
	}
	stats.SourceWireBytes = result.SourceWireBytes
	if checked.err != nil {
		return stats, checked.err
	}
	return stats, customError(err)
}

// Keep errors produced by Kelvo's schema/limit/ownership boundary even if an
// adapter wraps or accidentally ignores a sink failure. Source error strings
// themselves are always sanitized separately.
type customSink struct {
	query.Sink
	err error
}

func (s *customSink) Schema(schema *arrow.Schema) error {
	if s.err == nil {
		s.err = s.Sink.Schema(schema)
	}
	return s.err
}
func (s *customSink) Write(record arrow.RecordBatch) error {
	if s.err == nil {
		s.err = s.Sink.Write(record)
	}
	return s.err
}
func (e *customExecution) Close() error {
	if e.relation == nil {
		return nil
	}
	err := closeCustom(e.relation)
	e.relation = nil
	return err
}
func closeCustom(relation federationapi.Relation) (err error) {
	defer func() {
		if recover() != nil {
			err = query.NewError("QUERY_FAILED", "Custom federation adapter cleanup failed")
		}
	}()
	return customError(relation.Close())
}
func customError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, federationapi.ErrUnsupported) {
		return query.NewError("UNSUPPORTED", "Required custom federation behavior is unsupported")
	}
	// Do not pass through internal query.Error from compiled-in contributors:
	// arbitrary source diagnostics must not become public error messages.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return query.PublicError(err)
	}
	return query.NewError("QUERY_FAILED", "Custom federation source failed")
}

func (t *Table) prepareCustomScan(plan federationapi.ScanPlan) (federationapi.ScanPlan, *arrow.Schema, error) {
	// This transport does not define predicate semantics. Refuse required
	// filters before opening a scan relation (Open also performs discovery).
	// Bound user predicates remain in DuckDB; access.Relation applies policy
	// predicates locally before exposing protected rows to the query engine.
	if len(plan.Filters) != 0 && len(t.config.Sources) == 1 && t.config.Sources[0].Type == "arrow_flight" {
		return plan, nil, query.NewError("UNSUPPORTED", "Flight SQL federation does not support required source predicates")
	}
	if len(plan.Columns) > 1024 || len(plan.Filters) > 256 {
		return plan, nil, query.NewError("UNSUPPORTED", "Federation scan plan exceeds supported complexity")
	}
	// Copy the caller's slices before handing control to compiled-in code.
	plan.Columns = append([]string(nil), plan.Columns...)
	if len(plan.Columns) == 0 {
		plan.Columns = []string{t.schema.Field(0).Name}
	}
	fields := make([]arrow.Field, len(plan.Columns))
	for i, name := range plan.Columns {
		field, ok := t.columns[name]
		if !ok {
			return plan, nil, query.NewError("PERMISSION_DENIED", "Federation projection names an unavailable column")
		}
		fields[i] = field
	}
	compiler := customPredicateValidator{columns: t.columns}
	filters := make([]federationapi.Filter, len(plan.Filters))
	for i, filter := range plan.Filters {
		var err error
		filters[i], err = compiler.validate(filter, 0)
		if err != nil {
			return plan, nil, err
		}
	}
	plan.Filters = filters
	metadata := t.schema.Metadata()
	return plan, arrow.NewSchema(fields, &metadata), nil
}

type customPredicateValidator struct {
	columns map[string]arrow.Field
	nodes   int
}

func (v *customPredicateValidator) validate(filter federationapi.Filter, depth int) (federationapi.Filter, error) {
	v.nodes++
	unsupported := query.NewError("UNSUPPORTED", "Required federation predicate is unsupported")
	if depth > 32 || v.nodes > 1024 {
		return filter, unsupported
	}
	if filter.Kind == "and" || filter.Kind == "or" {
		if len(filter.Children) == 0 || len(filter.Children) > 256 || filter.Column != "" || filter.Op != "" || filter.Type != "" || filter.Value != "" {
			return filter, unsupported
		}
		children := make([]federationapi.Filter, len(filter.Children))
		for i, child := range filter.Children {
			var err error
			children[i], err = v.validate(child, depth+1)
			if err != nil {
				return filter, err
			}
		}
		filter.Children = children
		return filter, nil
	}
	field, ok := v.columns[filter.Column]
	if !ok || len(filter.Children) != 0 {
		return filter, unsupported
	}
	if filter.Kind == "is_null" || filter.Kind == "is_not_null" {
		if filter.Op != "" || filter.Type != "" || filter.Value != "" {
			return filter, unsupported
		}
		return filter, nil
	}
	if filter.Kind != "comparison" {
		return filter, unsupported
	}
	switch filter.Op {
	case "eq", "ne", "lt", "le", "gt", "ge":
	default:
		return filter, unsupported
	}
	if filter.Type == "bool" {
		if field.Type.ID() != arrow.BOOL || (filter.Value != "true" && filter.Value != "false") {
			return filter, unsupported
		}
		return filter, nil
	}
	types := map[string]struct {
		id     arrow.Type
		bits   int
		signed bool
	}{
		"int8": {arrow.INT8, 8, true}, "int16": {arrow.INT16, 16, true}, "int32": {arrow.INT32, 32, true}, "int64": {arrow.INT64, 64, true},
		"uint8": {arrow.UINT8, 8, false}, "uint16": {arrow.UINT16, 16, false}, "uint32": {arrow.UINT32, 32, false}, "uint64": {arrow.UINT64, 64, false},
	}
	typ, ok := types[filter.Type]
	if !ok || field.Type.ID() != typ.id || len(filter.Value) > 21 {
		return filter, unsupported
	}
	if typ.signed {
		n, err := strconv.ParseInt(filter.Value, 10, typ.bits)
		if err != nil || strconv.FormatInt(n, 10) != filter.Value {
			return filter, unsupported
		}
	} else {
		n, err := strconv.ParseUint(filter.Value, 10, typ.bits)
		if err != nil || strconv.FormatUint(n, 10) != filter.Value {
			return filter, unsupported
		}
	}
	return filter, nil
}
