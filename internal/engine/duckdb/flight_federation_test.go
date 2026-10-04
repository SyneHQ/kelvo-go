//go:build linux && duckdb_arrow && duckbridge && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type flightFederationResult struct {
	schema *arrow.Schema
	values [][]string
}

func (s *flightFederationResult) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *flightFederationResult) Write(record arrow.RecordBatch) error {
	for row := 0; row < int(record.NumRows()); row++ {
		values := make([]string, record.NumCols())
		for col := range values {
			values[col] = "<NULL>"
			if !record.Column(col).IsNull(row) {
				values[col] = record.Column(col).ValueStr(row)
			}
		}
		s.values = append(s.values, values)
	}
	return nil
}

// A fresh test process loads this test's CA through the standard Linux trust
// path. No production TLS override, insecure flag or process-global cert pool.
func TestFlightFederationTLSRealEngine(t *testing.T) {
	if os.Getenv("KELVO_TEST_FLIGHT_CHILD") != "1" {
		directory := t.TempDir()
		certificate, key := flightFixtureCertificate(t, directory)
		trustDirectory := t.TempDir()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFlightFederationTLSRealEngine$", "-test.v", "-test.timeout=75s")
		for _, value := range os.Environ() {
			name, _, _ := strings.Cut(value, "=")
			if name != "SSL_CERT_FILE" && name != "SSL_CERT_DIR" && !strings.HasPrefix(name, "KELVO_TEST_FLIGHT_") {
				command.Env = append(command.Env, value)
			}
		}
		command.Env = append(command.Env, "SSL_CERT_FILE="+certificate, "SSL_CERT_DIR="+trustDirectory,
			"KELVO_TEST_FLIGHT_CHILD=1", "KELVO_TEST_FLIGHT_CERT="+certificate, "KELVO_TEST_FLIGHT_KEY="+key)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("trusted Flight SQL subprocess failed: %v\n%s", err, output)
		}
		t.Log(string(output))
		return
	}
	certificate, err := tls.LoadX509KeyPair(os.Getenv("KELVO_TEST_FLIGHT_CERT"), os.Getenv("KELVO_TEST_FLIGHT_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	newToken := func() string {
		var token [24]byte
		if _, err := rand.Read(token[:]); err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(token[:])
	}
	ordersToken, customersToken := newToken(), newToken()
	orders, ordersURL := startFlightSQLFixture(t, certificate, "orders", ordersToken)
	customers, customersURL := startFlightSQLFixture(t, certificate, "customers", customersToken)
	t.Setenv("KELVO_SOURCE_FLIGHT_ORDERS_URL", ordersURL)
	t.Setenv("KELVO_SOURCE_FLIGHT_ORDERS_TOKEN", ordersToken)
	t.Setenv("KELVO_SOURCE_FLIGHT_CUSTOMERS_URL", customersURL)
	t.Setenv("KELVO_SOURCE_FLIGHT_CUSTOMERS_TOKEN", customersToken)
	baseline := flightFixtureDB(t, "orders", "customers")
	sources := []catalog.Source{
		{ID: "ledger", Type: "arrow_flight", URLEnv: "KELVO_SOURCE_FLIGHT_ORDERS_URL", TokenEnv: "KELVO_SOURCE_FLIGHT_ORDERS_TOKEN",
			Options:    map[string]string{"protocol": "flightsql", "federation_dialect": "ansi"},
			Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "orders", Schema: "analytics", Table: "orders"}}}},
		{ID: "directory", Type: "arrow_flight", URLEnv: "KELVO_SOURCE_FLIGHT_CUSTOMERS_URL", TokenEnv: "KELVO_SOURCE_FLIGHT_CUSTOMERS_TOKEN",
			Options:    map[string]string{"protocol": "flightsql", "federation_dialect": "ansi"},
			Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "customers", Schema: "analytics", Table: "customers"}}}},
	}
	limits := query.DefaultLimits()
	limits.Threads, limits.MemoryMB, limits.Timeout = 1, 128, 10*time.Second
	engine := func(t *testing.T, scanRows, scanBytes int64, resultLimits query.Limits) *Engine {
		t.Helper()
		configured := append([]catalog.Source(nil), sources...)
		for i := range configured {
			federation := *configured[i].Federation
			federation.MaxScanRows, federation.MaxScanBytes = scanRows, scanBytes
			configured[i].Federation = &federation
		}
		e, err := New(catalog.Config{Sources: configured}, resultLimits)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	parity := func(t *testing.T, ctx context.Context, statement string, selected []string) (*flightFederationResult, query.Stats) {
		t.Helper()
		got := &flightFederationResult{}
		stats, err := engine(t, 0, 0, limits).Execute(ctx, query.Request{Mode: "federated", SQL: statement, Sources: selected}, got)
		if err != nil {
			t.Fatal(err)
		}
		directSQL := strings.NewReplacer("ledger.orders", "analytics.orders", "directory.customers", "analytics.customers").Replace(statement)
		wire, err := flightFixtureIPC(context.Background(), baseline, directSQL)
		if err != nil {
			t.Fatal(err)
		}
		reader, err := ipc.NewReader(bytes.NewReader(wire))
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Release()
		want := &flightFederationResult{schema: reader.Schema()}
		for reader.Next() {
			_ = want.Write(reader.RecordBatch())
		}
		if err := reader.Err(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.values, want.values) || got.schema == nil || got.schema.NumFields() != want.schema.NumFields() {
			t.Fatalf("Flight result differs from direct SQL: got=%v want=%v", got.values, want.values)
		}
		for i, field := range want.schema.Fields() {
			if actual := got.schema.Field(i); actual.Name != field.Name || !arrow.TypeEqual(actual.Type, field.Type) {
				t.Fatalf("Flight result type differs from direct SQL: got=%v want=%v", actual, field)
			}
		}
		return got, stats
	}
	assertProjectionOnly := func(t *testing.T, fixture *flightSQLFixture, requireScan bool) {
		t.Helper()
		scans := 0
		for _, statement := range fixture.takeStatements() {
			if strings.HasSuffix(statement, " WHERE 1 = 0") {
				continue
			}
			scans++
			if strings.Contains(statement, " WHERE ") || strings.Contains(statement, " LIMIT ") || strings.HasPrefix(statement, "SELECT * ") || strings.Contains(statement, `"payload"`) {
				t.Fatalf("Flight scan changed residual predicates or projected unused payload: %s", statement)
			}
		}
		if requireScan && scans == 0 {
			t.Fatal("query did not scan its Flight source")
		}
	}

	t.Run("cte_cross_source_join", func(t *testing.T) {
		orders.takeStatements()
		customers.takeStatements()
		statement := `WITH eligible AS (
 SELECT customer_id, amount, event_day, big_id FROM ledger.orders
 WHERE amount IS NOT NULL AND (label LIKE 'ri%' OR customer_id=2)
), totals AS (
 SELECT customer_id, SUM(amount) AS revenue, MIN(event_day) AS first_day, MAX(big_id) AS large_id
 FROM eligible GROUP BY customer_id
) SELECT c.name, t.revenue, t.first_day, t.large_id
 FROM totals t JOIN directory.customers c ON c.id=t.customer_id
 WHERE c.region='west' ORDER BY c.name`
		got, stats := parity(t, context.Background(), statement, []string{"ledger", "directory"})
		if len(got.values) != 2 || stats.Rows != 2 || len(stats.Federation) < 2 {
			t.Fatalf("cross-source join did not execute both sources: rows=%v stats=%+v", got.values, stats)
		}
		assertProjectionOnly(t, orders, true)
		assertProjectionOnly(t, customers, true)
	})
	t.Run("null_decimal_date_and_integer_widths", func(t *testing.T) {
		orders.takeStatements()
		got, _ := parity(t, context.Background(), "SELECT id, small_value, big_id, uint_id, amount, event_day, label FROM ledger.orders ORDER BY id", []string{"ledger"})
		wantTypes := []arrow.Type{arrow.INT64, arrow.INT16, arrow.INT64, arrow.UINT64, arrow.DECIMAL128, arrow.DATE32, arrow.STRING}
		for i, want := range wantTypes {
			if got.schema.Field(i).Type.ID() != want {
				t.Fatalf("column %d lost its Arrow type: %v", i, got.schema.Field(i).Type)
			}
		}
		if len(got.values) != 6 || got.values[0][2] != "9007199254740993" || got.values[0][3] != "18446744073709551615" || got.values[3][4] != "<NULL>" || got.values[3][5] != "<NULL>" {
			t.Fatalf("exact values or NULLs changed: %v", got.values)
		}
		assertProjectionOnly(t, orders, true)
	})
	t.Run("residual_filters_and_nulls", func(t *testing.T) {
		orders.takeStatements()
		got, _ := parity(t, context.Background(), `SELECT id FROM ledger.orders WHERE (amount IS NULL OR event_day < DATE '1970-01-01' OR label LIKE 'ri%') AND id <> 2 ORDER BY id`, []string{"ledger"})
		if !reflect.DeepEqual(got.values, [][]string{{"1"}, {"4"}, {"6"}}) {
			t.Fatalf("residual filters returned wrong rows: %v", got.values)
		}
		assertProjectionOnly(t, orders, true)
	})
	t.Run("row_policy_column_denial_and_source_denial", func(t *testing.T) {
		ctx, err := access.WithPolicy(context.Background(), access.Policy{Sources: map[string]access.SourcePolicy{
			"ledger": {Tables: map[string]access.TablePolicy{"orders": {
				Columns: []string{"id", "amount", "event_day"},
				Rows:    &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"},
			}}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		orders.takeStatements()
		got := &flightFederationResult{}
		_, err = engine(t, 0, 0, limits).Execute(ctx, query.Request{Sources: []string{"ledger"}, SQL: "SELECT id FROM ledger.orders ORDER BY id"}, got)
		if err != nil || !reflect.DeepEqual(got.values, [][]string{{"1"}, {"2"}, {"4"}, {"6"}}) {
			t.Fatalf("local Flight row policy failed: values=%v err=%v", got.values, err)
		}
		assertProjectionOnly(t, orders, true)
		_, err = engine(t, 0, 0, limits).Execute(ctx, query.Request{Sources: []string{"ledger"}, SQL: "SELECT label FROM ledger.orders"}, &flightFederationResult{})
		if err == nil {
			t.Fatal("hidden Flight column became queryable")
		}
		orders.takeStatements()
		customers.takeStatements()
		_, err = engine(t, 0, 0, limits).Execute(ctx, query.Request{Sources: []string{"directory"}, SQL: "SELECT * FROM directory.customers"}, &flightFederationResult{})
		if err == nil || query.PublicError(err).Code != "PERMISSION_DENIED" || len(orders.takeStatements()) != 0 || len(customers.takeStatements()) != 0 {
			t.Fatal("denied source reached Flight service or lost permission error", err)
		}
	})
	t.Run("scan_and_result_limits_fail_without_truncation", func(t *testing.T) {
		count, _ := parity(t, context.Background(), "SELECT count(*) AS rows FROM ledger.orders", []string{"ledger"})
		if !reflect.DeepEqual(count.values, [][]string{{"6"}}) {
			t.Fatalf("generous count control changed: %v", count.values)
		}
		payload, _ := parity(t, context.Background(), "SELECT payload FROM ledger.orders ORDER BY id", []string{"ledger"})
		if len(payload.values) != 6 || len(payload.values[0][0]) != 4096 {
			t.Fatal("generous payload control did not deliver the full source value")
		}
		for _, tc := range []struct {
			name, statement string
			rows, bytes     int64
			limits          query.Limits
		}{
			{"scan_rows_before_count", "SELECT count(*) FROM ledger.orders", 2, 0, limits},
			{"scan_bytes", "SELECT payload FROM ledger.orders", 0, 1024, limits},
			{"result_rows", "SELECT id FROM ledger.orders ORDER BY id", 100, 0, func() query.Limits { l := limits; l.MaxRows = 2; return l }()},
		} {
			t.Run(tc.name, func(t *testing.T) {
				orders.takeStatements()
				_, err := engine(t, tc.rows, tc.bytes, tc.limits).Execute(context.Background(), query.Request{Sources: []string{"ledger"}, SQL: tc.statement}, &flightFederationResult{})
				if err == nil {
					t.Fatal("Flight limit silently truncated a relation or result")
				}
				scanned := false
				for _, statement := range orders.takeStatements() {
					if !strings.HasSuffix(statement, " WHERE 1 = 0") {
						scanned = true
						if strings.Contains(statement, " LIMIT ") {
							t.Fatal("Flight limit truncated the source SQL")
						}
					}
				}
				if !scanned {
					t.Fatal("limit test failed before reaching the source scan", err)
				}
			})
		}
	})
	t.Run("deadline_cancels_a_blocked_flight_scan", func(t *testing.T) {
		before := orders.blockStarts.Load()
		orders.blockScans.Store(true)
		defer orders.blockScans.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		started := time.Now()
		_, err := engine(t, 0, 0, limits).Execute(ctx, query.Request{Sources: []string{"ledger"}, SQL: "SELECT id FROM ledger.orders"}, &flightFederationResult{})
		if err == nil || time.Since(started) > 5*time.Second || orders.blockStarts.Load() <= before {
			t.Fatal("blocked Flight query did not honor its deadline", err)
		}
	})
}
