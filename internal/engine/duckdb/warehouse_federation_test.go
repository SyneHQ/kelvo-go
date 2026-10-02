//go:build duckdb_arrow && duckbridge && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type warehouseScan struct {
	kind, sql string
	fields    []string
}

type warehouseJob struct {
	ref      map[string]string
	scan     warehouseScan
	describe bool
	rows     []map[string]any
}

type warehouseProtocol struct {
	t       *testing.T
	mu      sync.Mutex
	jobs    map[string]warehouseJob
	scans   []warehouseScan
	fail    atomic.Bool
	cancels atomic.Int32
}

func warehouseRemote(kind string) string {
	if kind == "bigquery" {
		return "`data-project.reports.orders`"
	}
	return "`analytics`.`reports`.`customers`"
}

func warehouseRows(kind string) []map[string]any {
	if kind == "databricks" {
		return []map[string]any{
			{"id": "1", "name": "Ada", "payload": "unused"},
			{"id": "2", "name": "Lin", "payload": "unused"},
			{"id": "3", "name": "Null", "payload": "unused"},
			{"id": "4", "name": "Excluded", "payload": "unused"},
		}
	}
	return []map[string]any{
		{"id": "1", "customer_id": "4", "amount": "99.000000000", "note": "excluded", "payload": "unused"},
		{"id": "2", "customer_id": "1", "amount": "9007199254740993.123456789", "note": "sale", "payload": "unused"},
		{"id": "3", "customer_id": "1", "amount": "0.000000001", "note": "sale", "payload": "unused"},
		{"id": "4", "customer_id": "2", "amount": "2.250000000", "note": "sale", "payload": "unused"},
		{"id": "5", "customer_id": "3", "amount": nil, "note": nil, "payload": "unused"},
	}
}

func warehouseFields(kind string) []string {
	if kind == "databricks" {
		return []string{"id", "name", "payload"}
	}
	return []string{"id", "customer_id", "amount", "note", "payload"}
}

var warehouseComparison = regexp.MustCompile("`([a-z_]+)` (<=|>=|!=|=|<|>) CAST\\('(-?[0-9]+)' AS (?:INT64|BIGINT)\\)")
var warehouseNull = regexp.MustCompile("`([a-z_]+)` IS (NOT )?NULL")

// The protocol fixture evaluates every required pushed predicate. An unknown
// predicate fails the fixture instead of silently returning unfiltered rows.
func warehouseMatches(row map[string]any, predicate string) (bool, error) {
	matched := true
	for _, parts := range warehouseComparison.FindAllStringSubmatch(predicate, -1) {
		value, present := row[parts[1]]
		if !present {
			return false, fmt.Errorf("unknown filter column %s", parts[1])
		}
		if value == nil {
			matched = false
			continue
		}
		left, err := strconv.ParseInt(value.(string), 10, 64)
		if err != nil {
			return false, err
		}
		right, err := strconv.ParseInt(parts[3], 10, 64)
		if err != nil {
			return false, err
		}
		valid := false
		switch parts[2] {
		case "=":
			valid = left == right
		case "!=":
			valid = left != right
		case "<":
			valid = left < right
		case "<=":
			valid = left <= right
		case ">":
			valid = left > right
		case ">=":
			valid = left >= right
		}
		matched = matched && valid
	}
	for _, parts := range warehouseNull.FindAllStringSubmatch(predicate, -1) {
		value, present := row[parts[1]]
		if !present {
			return false, fmt.Errorf("unknown NULL filter column %s", parts[1])
		}
		matched = matched && ((value != nil) == (parts[2] == "NOT "))
	}
	remainder := warehouseComparison.ReplaceAllString(predicate, "")
	remainder = warehouseNull.ReplaceAllString(remainder, "")
	remainder = strings.NewReplacer("AND", "", "(", "", ")", "", " ", "").Replace(remainder)
	if remainder != "" {
		return false, fmt.Errorf("unsupported required fixture predicate: %s", predicate)
	}
	return matched, nil
}

func (f *warehouseProtocol) plan(kind, sql string) (warehouseJob, error) {
	job := warehouseJob{scan: warehouseScan{kind: kind, sql: sql}}
	remote := warehouseRemote(kind)
	if sql == "SELECT * FROM "+remote+" LIMIT 0" {
		job.describe = true
		job.scan.fields = warehouseFields(kind)
		return job, nil
	}
	projection, after, ok := strings.Cut(strings.TrimPrefix(sql, "SELECT "), " FROM ")
	if !strings.HasPrefix(sql, "SELECT ") || !ok || !strings.HasPrefix(after, remote) {
		return job, fmt.Errorf("unqualified or invalid source query: %s", sql)
	}
	predicate := strings.TrimPrefix(after, remote)
	if predicate != "" {
		if !strings.HasPrefix(predicate, " WHERE ") {
			return job, fmt.Errorf("scan contains unsupported suffix or LIMIT: %s", sql)
		}
		predicate = strings.TrimPrefix(predicate, " WHERE ")
	}
	allowed := make(map[string]bool)
	for _, field := range warehouseFields(kind) {
		allowed[field] = true
	}
	for _, quoted := range strings.Split(projection, ", ") {
		field := strings.Trim(quoted, "`")
		if quoted != "`"+field+"`" || !allowed[field] {
			return job, fmt.Errorf("unknown source projection: %s", quoted)
		}
		job.scan.fields = append(job.scan.fields, field)
	}
	for _, row := range warehouseRows(kind) {
		match, err := warehouseMatches(row, predicate)
		if err != nil {
			return job, err
		}
		if match {
			job.rows = append(job.rows, row)
		}
	}
	f.mu.Lock()
	f.scans = append(f.scans, job.scan)
	f.mu.Unlock()
	return job, nil
}

func warehouseColumns(job warehouseJob) []map[string]any {
	columns := make([]map[string]any, len(job.scan.fields))
	for i, name := range job.scan.fields {
		typ := "STRING"
		if name == "id" || name == "customer_id" {
			typ = "INTEGER"
		} else if name == "amount" {
			typ = "NUMERIC"
		}
		column := map[string]any{"name": name, "type": typ, "mode": "NULLABLE"}
		if job.scan.kind == "databricks" {
			if typ == "INTEGER" {
				typ = "BIGINT"
			}
			column = map[string]any{"name": name, "type_name": typ}
		}
		columns[i] = column
	}
	return columns
}

func (f *warehouseProtocol) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Header.Get("Authorization") != "Bearer fixture-only" {
		f.t.Error("warehouse request lost its source token")
	}
	if strings.HasSuffix(r.URL.Path, "/cancel") {
		f.cancels.Add(1)
		fmt.Fprint(w, `{}`)
		return
	}
	if r.Method == http.MethodPost {
		var body struct {
			Statement string            `json:"statement"`
			Ref       map[string]string `json:"jobReference"`
			Config    struct {
				Query struct {
					SQL string `json:"query"`
				} `json:"query"`
			} `json:"configuration"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			f.t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		kind, sql := "databricks", body.Statement
		if r.URL.Path == "/bigquery/v2/projects/billing-project/jobs" {
			kind, sql = "bigquery", body.Config.Query.SQL
		} else if r.URL.Path != "/api/2.0/sql/statements" {
			f.t.Errorf("unexpected source submission: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		job, err := f.plan(kind, sql)
		if err != nil {
			f.t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if kind == "bigquery" {
			job.ref = body.Ref
			f.mu.Lock()
			f.jobs[body.Ref["jobId"]] = job
			f.mu.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"jobReference": body.Ref})
			return
		}
		if f.fail.Load() && !job.describe {
			json.NewEncoder(w).Encode(map[string]any{"statement_id": "fixture-statement", "status": map[string]string{"state": "FAILED"}})
			return
		}
		rows := make([][]any, len(job.rows))
		for i, row := range job.rows {
			for _, field := range job.scan.fields {
				rows[i] = append(rows[i], row[field])
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"statement_id": "fixture-statement", "status": map[string]string{"state": "SUCCEEDED"}, "manifest": map[string]any{"format": "JSON_ARRAY", "schema": map[string]any{"columns": warehouseColumns(job)}, "total_row_count": len(rows), "total_chunk_count": 1, "truncated": false}, "result": map[string]any{"chunk_index": 0, "row_offset": 0, "row_count": len(rows), "data_array": rows}})
		return
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/bigquery/v2/projects/billing-project/queries/") {
		id := strings.TrimPrefix(r.URL.Path, "/bigquery/v2/projects/billing-project/queries/")
		f.mu.Lock()
		job, found := f.jobs[id]
		f.mu.Unlock()
		if !found {
			f.t.Error("unknown BigQuery job")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if f.fail.Load() && !job.describe {
			json.NewEncoder(w).Encode(map[string]any{"jobReference": job.ref, "jobComplete": true, "errors": []map[string]string{{"message": "fixture provider failure"}}})
			return
		}
		rows := make([]map[string]any, len(job.rows))
		for i, row := range job.rows {
			cells := make([]map[string]any, len(job.scan.fields))
			for j, field := range job.scan.fields {
				cells[j] = map[string]any{"v": row[field]}
			}
			rows[i] = map[string]any{"f": cells}
		}
		json.NewEncoder(w).Encode(map[string]any{"jobReference": job.ref, "jobComplete": true, "totalRows": strconv.Itoa(len(rows)), "schema": map[string]any{"fields": warehouseColumns(job)}, "rows": rows})
		return
	}
	f.t.Errorf("unexpected source request: %s %s", r.Method, r.URL.Path)
	w.WriteHeader(http.StatusNotFound)
}

func warehouseEngine(t *testing.T) (*Engine, *warehouseProtocol) {
	t.Helper()
	f := &warehouseProtocol{t: t, jobs: make(map[string]warehouseJob)}
	server := httptest.NewTLSServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(server.Close)
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	dialer := &tls.Dialer{Config: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	previous := http.DefaultTransport
	transport := previous.(*http.Transport).Clone()
	transport.DialTLSContext = dialer.DialContext
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = previous; transport.CloseIdleConnections() })
	t.Setenv("KELVO_SOURCE_WAREHOUSE_FIXTURE_URL", server.URL)
	t.Setenv("KELVO_SOURCE_WAREHOUSE_FIXTURE_TOKEN", "fixture-only")
	csv := filepath.Join(t.TempDir(), "segments.csv")
	if err := os.WriteFile(csv, []byte("customer_id,segment\n1,priority\n2,standard\n3,priority\n"), 0600); err != nil {
		t.Fatal(err)
	}
	sources := []catalog.Source{
		{ID: "bq", Type: "bigquery", URLEnv: "KELVO_SOURCE_WAREHOUSE_FIXTURE_URL", TokenEnv: "KELVO_SOURCE_WAREHOUSE_FIXTURE_TOKEN", Options: map[string]string{"project": "billing-project", "location": "us"}, Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "orders", Database: "data-project", Schema: "reports", Table: "orders"}}}},
		{ID: "db", Type: "databricks", URLEnv: "KELVO_SOURCE_WAREHOUSE_FIXTURE_URL", TokenEnv: "KELVO_SOURCE_WAREHOUSE_FIXTURE_TOKEN", Options: map[string]string{"warehouse_id": "fixture-warehouse"}, Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "customers", Database: "analytics", Schema: "reports", Table: "customers"}}}},
		{ID: "segments", Type: "csv", Path: csv},
	}
	l := limits()
	l.Timeout, l.Threads = 15*time.Second, 4
	e, err := New(catalog.Config{Sources: sources}, l)
	if err != nil {
		t.Fatal(err)
	}
	return e, f
}

type warehouseResult struct {
	schema *arrow.Schema
	rows   [][]string
}

func (s *warehouseResult) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *warehouseResult) Write(record arrow.RecordBatch) error {
	for row := 0; row < int(record.NumRows()); row++ {
		values := make([]string, int(record.NumCols()))
		for i, column := range record.Columns() {
			if column.IsNull(row) {
				values[i] = "<null>"
				continue
			}
			switch a := column.(type) {
			case *array.String:
				values[i] = strings.Clone(a.Value(row))
			case *array.Int64:
				values[i] = strconv.FormatInt(a.Value(row), 10)
			case *array.Decimal128:
				values[i] = a.Value(row).ToString(a.DataType().(*arrow.Decimal128Type).Scale)
			default:
				return fmt.Errorf("unexpected result type %s", column.DataType())
			}
		}
		s.rows = append(s.rows, values)
	}
	return nil
}

func TestWarehouseFederationJoinCTEWindowAndExactAggregate(t *testing.T) {
	e, fixture := warehouseEngine(t)
	sql := `WITH joined AS (
 SELECT c.name, s.segment, o.amount
 FROM bq.orders o JOIN db.customers c ON o.customer_id=c.id
 JOIN segments s ON s.customer_id=c.id WHERE o.id >= 2
), totals AS (
 SELECT name, segment, SUM(amount) AS amount FROM joined GROUP BY name, segment
)
SELECT name || ':' || segment AS label, amount, ROW_NUMBER() OVER (ORDER BY name) AS rank
FROM totals ORDER BY name`
	sink := &warehouseResult{}
	stats, err := e.Execute(context.Background(), query.Request{Mode: "federated", Sources: []string{"bq", "db", "segments"}, SQL: sql}, sink)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"Ada:priority", "9007199254740993.123456790", "1"}, {"Lin:standard", "2.250000000", "2"}, {"Null:priority", "<null>", "3"}}
	if !reflect.DeepEqual(sink.rows, want) || stats.Rows != 3 || sink.schema.Field(1).Type.ID() != arrow.DECIMAL128 {
		t.Fatalf("warehouse join/window/aggregate changed: rows=%v stats=%+v", sink.rows, stats)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	seen := make(map[string]bool)
	pushed := false
	for _, scan := range fixture.scans {
		seen[scan.kind] = true
		for _, field := range scan.fields {
			if field == "payload" || field == "note" {
				t.Fatalf("unused warehouse field was fetched: %+v", scan)
			}
		}
		if scan.kind == "bigquery" && strings.Contains(scan.sql, "`id` >= CAST('2' AS INT64)") {
			pushed = true
		}
	}
	if !seen["bigquery"] || !seen["databricks"] || !pushed || len(stats.Federation) != 2 {
		t.Fatalf("warehouse scan scope or predicate pushdown changed: scans=%+v stats=%+v", fixture.scans, stats.Federation)
	}
}

func TestWarehouseFederationEmptyAndCountKeepCardinality(t *testing.T) {
	e, fixture := warehouseEngine(t)
	for _, test := range []struct{ sql, want string }{
		{"SELECT count(*) FROM bq.orders", "5"},
		{"SELECT count(*) FROM bq.orders WHERE id < 0", "0"},
		{"SELECT count(*) FROM db.customers", "4"},
		{"SELECT count(*) FROM db.customers WHERE id < 0", "0"},
	} {
		sink := &warehouseResult{}
		source := "bq"
		if strings.Contains(test.sql, "db.customers") {
			source = "db"
		}
		_, err := e.Execute(context.Background(), query.Request{Mode: "federated", Sources: []string{source}, SQL: test.sql}, sink)
		if err != nil || !reflect.DeepEqual(sink.rows, [][]string{{test.want}}) {
			t.Fatalf("empty/count query %q: rows=%v error=%v", test.sql, sink.rows, err)
		}
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.scans) != 4 {
		t.Fatalf("count queries made %d scans, want 4", len(fixture.scans))
	}
	for _, scan := range fixture.scans {
		if !reflect.DeepEqual(scan.fields, []string{"id"}) {
			t.Fatalf("count scan did not retain the first real source field: %+v", scan)
		}
	}
}

func TestWarehouseFederationProviderFailureCannotBecomeEmptySuccess(t *testing.T) {
	for _, source := range []string{"bq", "db"} {
		t.Run(source, func(t *testing.T) {
			e, fixture := warehouseEngine(t)
			fixture.fail.Store(true)
			from := "bq.orders"
			if source == "db" {
				from = "db.customers"
			}
			sink := &warehouseResult{}
			_, err := e.Execute(context.Background(), query.Request{Mode: "federated", Sources: []string{source}, SQL: "SELECT count(*) FROM " + from}, sink)
			if err == nil || query.PublicError(err).Code != "QUERY_FAILED" || len(sink.rows) != 0 || fixture.cancels.Load() != 1 {
				t.Fatalf("source failure escaped as partial/empty success: rows=%v cancels=%d error=%v", sink.rows, fixture.cancels.Load(), err)
			}
		})
	}
}

func TestWarehouseFederationOtherComparisonsAndNullChecksStayLocal(t *testing.T) {
	for _, test := range []struct {
		source, sql string
		want        [][]string
	}{
		{"bq", "SELECT id FROM bq.orders WHERE note='sale' ORDER BY id", [][]string{{"2"}, {"3"}, {"4"}}},
		{"bq", "SELECT id FROM bq.orders WHERE amount > 1.0 ORDER BY id", [][]string{{"1"}, {"2"}, {"4"}}},
		{"db", "SELECT id FROM db.customers WHERE name='Ada' ORDER BY id", [][]string{{"1"}}},
		{"bq", "SELECT id FROM bq.orders WHERE note IS NULL ORDER BY id", [][]string{{"5"}}},
		{"bq", "SELECT id FROM bq.orders WHERE amount IS NULL ORDER BY id", [][]string{{"5"}}},
	} {
		t.Run(test.sql, func(t *testing.T) {
			e, fixture := warehouseEngine(t)
			sink := &warehouseResult{}
			stats, err := e.Execute(context.Background(), query.Request{Mode: "federated", Sources: []string{test.source}, SQL: test.sql}, sink)
			if err != nil || !reflect.DeepEqual(sink.rows, test.want) {
				t.Fatalf("local comparison changed result semantics: rows=%v want=%v error=%v", sink.rows, test.want, err)
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			// The source sends every contrasting row. DuckDB must evaluate these
			// predicates locally instead of dropping or serializing them.
			if len(fixture.scans) != 1 || strings.Contains(fixture.scans[0].sql, " WHERE ") || len(stats.Federation) != 1 || stats.Federation[0].Rows <= int64(len(test.want)) {
				t.Fatalf("local predicate leaked to source or did not see contrasting rows: scans=%+v stats=%+v", fixture.scans, stats.Federation)
			}
		})
	}
}
