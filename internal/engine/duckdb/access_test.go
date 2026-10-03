//go:build duckdb_arrow && duckbridge && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type accessFixtureDriver struct {
	mu    sync.Mutex
	opens map[string]int
	scans int
}

var accessFixture = &accessFixtureDriver{opens: map[string]int{}}

func init()                                                                           { federationapi.MustRegister("access_fixture", accessFixture) }
func (*accessFixtureDriver) Validate(federationapi.Source, federationapi.Table) error { return nil }
func (d *accessFixtureDriver) Open(_ context.Context, _ federationapi.Source, t federationapi.Table, _ federationapi.Limits) (federationapi.Relation, error) {
	d.mu.Lock()
	d.opens[t.Name]++
	d.mu.Unlock()
	return &accessFixtureRelation{table: t.Name}, nil
}

type accessFixtureRelation struct{ table string }

func (*accessFixtureRelation) Schema() *arrow.Schema {
	meta := arrow.MetadataFrom(map[string]string{"private-schema": "must-not-escape"})
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: meta}, {Name: "tenant_id", Type: arrow.PrimitiveTypes.Int64}, {Name: "amount", Type: arrow.PrimitiveTypes.Int64}, {Name: "secret", Type: arrow.BinaryTypes.String}}, &meta)
}
func (*accessFixtureRelation) Close() error { return nil }
func (r *accessFixtureRelation) Scan(ctx context.Context, p federationapi.ScanPlan, s federationapi.Sink) (federationapi.ScanStats, error) {
	accessFixture.mu.Lock()
	accessFixture.scans++
	accessFixture.mu.Unlock()
	if len(p.Filters) != 0 {
		return federationapi.ScanStats{}, errors.New("adapter must not enforce authority")
	}
	ids := []int64{1, 2, 3, 4, 5}
	tenants := []int64{7, 7, 8, 7, 8}
	amounts := []int64{10, 20, 9000, -1, 8000}
	if r.table == "customers" {
		ids = []int64{1, 2, 4}
		tenants = []int64{7, 8, 7}
		amounts = []int64{0, 0, 0}
	}
	fields := make([]arrow.Field, len(p.Columns))
	full := r.Schema()
	for i, name := range p.Columns {
		index := full.FieldIndices(name)
		if len(index) != 1 {
			return federationapi.ScanStats{}, errors.New("unknown column")
		}
		fields[i] = full.Field(index[0])
	}
	meta := full.Metadata()
	schema := arrow.NewSchema(fields, &meta)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for i, name := range p.Columns {
		switch name {
		case "id":
			builder.Field(i).(*array.Int64Builder).AppendValues(ids, nil)
		case "tenant_id":
			builder.Field(i).(*array.Int64Builder).AppendValues(tenants, nil)
		case "amount":
			builder.Field(i).(*array.Int64Builder).AppendValues(amounts, nil)
		case "secret":
			for range ids {
				builder.Field(i).(*array.StringBuilder).Append("private source value")
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return federationapi.ScanStats{}, err
	}
	if err := s.Schema(schema); err != nil {
		return federationapi.ScanStats{}, err
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	return federationapi.ScanStats{SourceWireBytes: 99999}, s.Write(record)
}

func accessEngine(t *testing.T) (*Engine, context.Context) {
	t.Helper()
	accessFixture.mu.Lock()
	accessFixture.opens = map[string]int{}
	accessFixture.scans = 0
	accessFixture.mu.Unlock()
	policy := access.Policy{Sources: map[string]access.SourcePolicy{"warehouse": {Tables: map[string]access.TablePolicy{
		"orders":    {Columns: []string{"id", "amount"}, Rows: &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"}},
		"customers": {Columns: []string{"id", "secret"}, Rows: &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"}},
	}}}}
	ctx, err := access.WithPolicy(context.Background(), policy)
	if err != nil {
		t.Fatal(err)
	}
	config := catalog.Config{Sources: []catalog.Source{{ID: "warehouse", Type: "access_fixture", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "orders", Table: "orders"}, {Name: "customers", Table: "customers"}, {Name: "hidden", Table: "hidden"}}}}}}
	l := query.DefaultLimits()
	l.Threads = 2
	l.Timeout = 10 * time.Second
	engine, err := New(config, l)
	if err != nil {
		t.Fatal(err)
	}
	return engine, ctx
}

type accessValueSink struct {
	schema *arrow.Schema
	values [][]int64
	rows   int64
}

func (s *accessValueSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *accessValueSink) Write(record arrow.RecordBatch) error {
	s.rows += record.NumRows()
	for row := 0; row < int(record.NumRows()); row++ {
		values := []int64{}
		for _, column := range record.Columns() {
			if v, ok := column.(*array.Int64); ok {
				values = append(values, v.Value(row))
			}
		}
		s.values = append(s.values, values)
	}
	return nil
}

func TestAccessPoliciesPrecedeJoinsAggregatesCTEsAndResiduals(t *testing.T) {
	engine, ctx := accessEngine(t)
	cases := []struct {
		sql  string
		want [][]int64
	}{
		{"SELECT count(*)::BIGINT, sum(amount)::BIGINT FROM warehouse.orders", [][]int64{{3, 29}}},
		{"SELECT count(*)::BIGINT, sum(o.amount)::BIGINT FROM warehouse.orders o JOIN warehouse.customers c ON o.id=c.id", [][]int64{{2, 9}}},
		{"WITH positive AS (SELECT id,amount FROM warehouse.orders WHERE amount>0), totals AS (SELECT count(*) n,sum(amount) total FROM positive) SELECT n::BIGINT,total::BIGINT FROM totals", [][]int64{{2, 30}}},
		{"SELECT count(*)::BIGINT FROM warehouse.orders a JOIN warehouse.orders b ON a.id=b.id", [][]int64{{3}}},
		{"SELECT sum(CASE WHEN amount=9000 THEN error('hidden row reached expression') ELSE amount END)::BIGINT FROM warehouse.orders", [][]int64{{29}}},
		{"SELECT id::BIGINT, row_number() OVER (ORDER BY id)::BIGINT FROM warehouse.orders ORDER BY id", [][]int64{{1, 1}, {2, 2}, {4, 3}}},
		{"SELECT count(*)::BIGINT FROM information_schema.columns WHERE table_schema='warehouse' AND column_name='tenant_id'", [][]int64{{0}}},
	}
	for _, tc := range cases {
		sink := &accessValueSink{}
		stats, err := engine.Execute(ctx, query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: tc.sql}, sink)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if !reflect.DeepEqual(sink.values, tc.want) {
			t.Fatalf("%s: got %v want %v", tc.sql, sink.values, tc.want)
		}
		if stats.Federation != nil || stats.SourceWireBytes != 0 || stats.ScanDiagnostics != nil {
			t.Fatal("pre-policy statistics escaped")
		}
	}
	accessFixture.mu.Lock()
	defer accessFixture.mu.Unlock()
	if accessFixture.opens["hidden"] != 0 {
		t.Fatal("hidden table discovered")
	}
}

func TestAccessPoliciesHideColumnsMetadataAndDirectReaders(t *testing.T) {
	engine, ctx := accessEngine(t)
	for _, sql := range []string{"SELECT tenant_id FROM warehouse.orders", "SELECT secret FROM warehouse.orders", "SELECT * FROM warehouse.hidden", "SELECT count(*) FROM warehouse.orders WHERE tenant_id=8", "SELECT * FROM read_parquet('private')", "SELECT * FROM kelvo_arrow_scan_1(NULL,NULL,NULL)"} {
		sink := &accessValueSink{}
		_, err := engine.Execute(ctx, query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: sql}, sink)
		if err == nil || sink.rows != 0 {
			t.Fatal("restricted SQL succeeded", sql)
		}
		if strings.Contains(err.Error(), "must-not-escape") || strings.Contains(err.Error(), "private source value") {
			t.Fatal("private metadata leaked")
		}
	}
	sink := &accessValueSink{}
	_, err := engine.Execute(ctx, query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: "SELECT * FROM warehouse.orders"}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if sink.schema.NumFields() != 2 || sink.schema.Metadata().Len() != 0 {
		t.Fatal("hidden schema exposed")
	}
	for _, field := range sink.schema.Fields() {
		if field.Metadata.Len() != 0 || field.Name == "tenant_id" || field.Name == "secret" {
			t.Fatal("hidden field metadata exposed")
		}
	}
	accessFixture.mu.Lock()
	defer accessFixture.mu.Unlock()
	if accessFixture.opens["hidden"] != 0 {
		t.Fatal("hidden table discovered")
	}
}
