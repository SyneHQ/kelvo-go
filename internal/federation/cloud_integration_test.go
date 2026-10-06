// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

var cloudKinds = []string{"snowflake", "databricks", "bigquery"}
var cloudFieldNames = []string{"id", "active", "amount", "at", "label"}

type cloudStatement struct {
	sql, scenario string
	fields        []string
}

type cloudJob struct {
	statement cloudStatement
	id        string
	ref       map[string]string
}

// These fixtures exercise New and Scan through the production native factories,
// including the HTTPS protocol, row decoder and Arrow handoff. They intentionally
// do not run in parallel because their TLS dialer replaces DefaultTransport.
type cloudFixture struct {
	t          *testing.T
	kind       string
	statements []cloudStatement
	mu         sync.Mutex
	jobs       map[string]*cloudJob
	submitted  int
	pages      atomic.Int32
	cancels    atomic.Int32
	polling    chan struct{}
	pollOnce   sync.Once
}

func cloudLocation(kind string) (catalog.FederationTable, string) {
	switch kind {
	case "snowflake":
		return catalog.FederationTable{Name: "events", Database: "ANALYTICS", Schema: "REPORTS", Table: "EVENTS"}, `"ANALYTICS"."REPORTS"."EVENTS"`
	case "databricks":
		return catalog.FederationTable{Name: "events", Database: "analytics", Schema: "reports", Table: "events"}, "`analytics`.`reports`.`events`"
	default:
		return catalog.FederationTable{Name: "events", Database: "fixture-project", Schema: "reports", Table: "events"}, "`fixture-project.reports.events`"
	}
}

func cloudQuote(kind, name string) string {
	if kind == "snowflake" {
		return `"` + name + `"`
	}
	return "`" + name + "`"
}

func cloudSelect(kind string, fields []string) string {
	_, remote := cloudLocation(kind)
	quoted := make([]string, len(fields))
	for i, name := range fields {
		quoted[i] = cloudQuote(kind, name)
	}
	return "SELECT " + strings.Join(quoted, ", ") + " FROM " + remote
}

func newCloudFixture(t *testing.T, kind string, statements ...cloudStatement) (*Table, *cloudFixture) {
	t.Helper()
	selected, remote := cloudLocation(kind)
	f := &cloudFixture{t: t, kind: kind, jobs: make(map[string]*cloudJob), polling: make(chan struct{})}
	f.statements = append([]cloudStatement{{sql: "SELECT * FROM " + remote + " LIMIT 0", fields: cloudFieldNames, scenario: "empty"}}, statements...)
	server := httptest.NewTLSServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(server.Close)

	// Cloud transports clone DefaultTransport and install their own TLS config.
	// A verified TLS dialer trusts only this fixture certificate, keeping the
	// actual native constructors intact without adding a production test hook.
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	dialer := &tls.Dialer{Config: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	previous := http.DefaultTransport
	transport := previous.(*http.Transport).Clone()
	transport.DialTLSContext = dialer.DialContext
	http.DefaultTransport = transport
	t.Cleanup(func() {
		http.DefaultTransport = previous
		transport.CloseIdleConnections()
	})
	source := catalog.Source{
		ID: "warehouse", Type: kind, URLEnv: "KELVO_SOURCE_FEDERATION_CLOUD_URL", TokenEnv: "KELVO_SOURCE_FEDERATION_CLOUD_TOKEN",
		Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{selected}},
	}
	switch kind {
	case "snowflake":
		source.Options = map[string]string{"database": "DEFAULT_DB", "schema": "DEFAULT_SCHEMA", "warehouse": "FIXTURE_WH"}
	case "databricks":
		source.Options = map[string]string{"warehouse_id": "fixture-warehouse", "catalog": "default_catalog", "schema": "default_schema"}
	case "bigquery":
		source.Options = map[string]string{"project": "billing-project", "location": "us", "dataset": "default_dataset", "maximum_bytes_billed": "1000000"}
	}
	t.Setenv(source.URLEnv, server.URL)
	t.Setenv(source.TokenEnv, "fixture-token")
	table, err := New(context.Background(), source, selected, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = table.Close() })
	return table, f
}

func (f *cloudFixture) submittedAll() {
	f.t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.submitted != len(f.statements) {
		f.t.Fatalf("submitted %d native statements, want %d", f.submitted, len(f.statements))
	}
}

func (f *cloudFixture) submit(sql, id string, ref map[string]string) *cloudJob {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.submitted >= len(f.statements) {
		f.t.Errorf("unexpected native query: %s", sql)
		return nil
	}
	expected := f.statements[f.submitted]
	f.submitted++
	if sql != expected.sql {
		f.t.Errorf("native query changed qualification, projection, predicates or limits:\ngot  %s\nwant %s", sql, expected.sql)
	}
	if id == "" {
		id = fmt.Sprintf("fixture-%d", f.submitted)
	}
	job := &cloudJob{statement: expected, id: id, ref: ref}
	f.jobs[id] = job
	return job
}

func (f *cloudFixture) job(id string) *cloudJob {
	f.mu.Lock()
	defer f.mu.Unlock()
	job := f.jobs[id]
	if job == nil {
		f.t.Errorf("unexpected native statement handle %q", id)
	}
	return job
}

func (f *cloudFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer fixture-token" {
		f.t.Error("native source credential was not preserved")
	}
	w.Header().Set("Content-Type", "application/json")
	if strings.HasSuffix(r.URL.Path, "/cancel") {
		if r.Method != http.MethodPost {
			f.t.Error("source cancellation used the wrong method")
		}
		parts := strings.Split(r.URL.Path, "/")
		job := f.job(parts[len(parts)-2])
		if job == nil || job.statement.scenario == "empty" {
			f.t.Error("source cancellation targeted an unavailable or completed describe statement")
		}
		if f.kind == "bigquery" && (!strings.HasPrefix(r.URL.Path, "/bigquery/v2/projects/billing-project/jobs/") || r.URL.Query().Get("location") != "us") {
			f.t.Error("BigQuery cancellation changed the billing project or location")
		}
		f.cancels.Add(1)
		fmt.Fprint(w, `{}`)
		return
	}
	if f.kind == "bigquery" {
		f.serveBigQuery(w, r)
		return
	}
	endpoint := "/api/v2/statements"
	if f.kind == "databricks" {
		endpoint = "/api/2.0/sql/statements"
	} else if r.Header.Get("X-Snowflake-Authorization-Token-Type") != "OAUTH" {
		f.t.Error("Snowflake token type changed")
	}
	var job *cloudJob
	page := false
	if r.Method == http.MethodPost && r.URL.Path == endpoint {
		var body struct {
			SQL         string          `json:"statement"`
			Format      string          `json:"format"`
			Disposition string          `json:"disposition"`
			Warehouse   string          `json:"warehouse_id"`
			Parameters  json.RawMessage `json:"parameters"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			f.t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch f.kind {
		case "snowflake":
			var parameters map[string]string
			if err := json.Unmarshal(body.Parameters, &parameters); err != nil || parameters["MULTI_STATEMENT_COUNT"] != "1" || r.URL.Query().Get("async") != "true" || r.URL.Query().Get("requestId") == "" {
				f.t.Error("Snowflake submission lost its single-statement contract")
			}
		case "databricks":
			// Databricks parameters are a list of named bindings. Federation
			// renders these fixture predicates into SQL, so the list is empty.
			var parameters []json.RawMessage
			if err := json.Unmarshal(body.Parameters, &parameters); err != nil || parameters == nil || len(parameters) != 0 {
				f.t.Error("Databricks submission changed its unbound parameter array")
			}
			if body.Format != "JSON_ARRAY" || body.Disposition != "INLINE" || body.Warehouse != "fixture-warehouse" {
				f.t.Error("Databricks submission lost its warehouse or result format")
			}
		}
		job = f.submit(body.SQL, "", nil)
	} else if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, endpoint+"/") {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, endpoint+"/"), "/")
		job = f.job(parts[0])
		page = r.URL.Query().Get("partition") == "1" || strings.HasSuffix(r.URL.Path, "/result/chunks/1")
		if !page && job != nil && job.statement.scenario == "pending" {
			f.pollOnce.Do(func() { close(f.polling) })
		}
	} else {
		f.t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
	}
	if job == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if job.statement.scenario == "pending" {
		if f.kind == "snowflake" {
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]any{"statementHandle": job.id, "code": "333334"})
		} else {
			json.NewEncoder(w).Encode(map[string]any{"statement_id": job.id, "status": map[string]string{"state": "RUNNING"}})
		}
		return
	}
	if page {
		f.pages.Add(1)
	}
	json.NewEncoder(w).Encode(f.result(job, page))
}

func (f *cloudFixture) serveBigQuery(w http.ResponseWriter, r *http.Request) {
	base := "/bigquery/v2/projects/billing-project"
	if r.Method == http.MethodPost && r.URL.Path == base+"/jobs" {
		var body struct {
			Ref    map[string]string `json:"jobReference"`
			Config struct {
				Query struct {
					SQL     string `json:"query"`
					Legacy  bool   `json:"useLegacySql"`
					Billing string `json:"maximumBytesBilled"`
				} `json:"query"`
			} `json:"configuration"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			f.t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if body.Config.Query.Legacy || body.Config.Query.Billing != "1000000" || body.Ref["projectId"] != "billing-project" || body.Ref["location"] != "us" || body.Ref["jobId"] == "" {
			f.t.Error("BigQuery native job options changed")
		}
		if f.submit(body.Config.Query.SQL, body.Ref["jobId"], body.Ref) == nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"jobReference": body.Ref})
		return
	}
	if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, base+"/queries/") {
		f.t.Errorf("unexpected BigQuery request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	job := f.job(strings.TrimPrefix(r.URL.Path, base+"/queries/"))
	if job == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.URL.Query().Get("location") != "us" || r.URL.Query().Get("formatOptions.useInt64Timestamp") != "true" {
		f.t.Error("BigQuery result request lost location or exact timestamp option")
	}
	if job.statement.scenario == "pending" {
		f.pollOnce.Do(func() { close(f.polling) })
		json.NewEncoder(w).Encode(map[string]any{"jobReference": job.ref, "jobComplete": false})
		return
	}
	page := r.URL.Query().Get("pageToken") != ""
	if page {
		if r.URL.Query().Get("pageToken") != "next+/=" {
			f.t.Error("BigQuery page token was altered")
		}
		f.pages.Add(1)
	}
	json.NewEncoder(w).Encode(f.result(job, page))
}

func (f *cloudFixture) columns(statement cloudStatement) []map[string]any {
	columns := make([]map[string]any, len(statement.fields))
	for i, name := range statement.fields {
		typ := map[string]string{"id": "BIGINT", "active": "BOOLEAN", "amount": "DECIMAL", "at": "TIMESTAMP", "label": "STRING"}[name]
		column := map[string]any{"name": name}
		switch f.kind {
		case "snowflake":
			typ = map[string]string{"id": "fixed", "active": "boolean", "amount": "fixed", "at": "timestamp_ltz", "label": "text"}[name]
			column["type"] = typ
			if name == "id" || name == "amount" {
				column["precision"], column["scale"] = 38, 0
				if name == "amount" {
					column["scale"] = 9
				}
			}
		case "databricks":
			column["type_name"] = typ
			if name == "amount" {
				column["type_precision"], column["type_scale"] = 38, 9
			}
		case "bigquery":
			if name == "id" {
				typ = "INTEGER"
			} else if name == "amount" {
				typ = "NUMERIC"
			}
			column["type"], column["mode"] = typ, "NULLABLE"
		}
		if statement.scenario == "drift" && name == "active" {
			key := "type"
			if f.kind == "databricks" {
				key = "type_name"
			}
			// BOOL maps to the same Arrow type but different native metadata.
			column[key] = "BOOL"
		}
		columns[i] = column
	}
	return columns
}

func (f *cloudFixture) rows(fields []string) [][]any {
	at, beforeEpoch := "2025-01-01T00:00:00.123456789Z", "1969-12-31T23:59:59.999999999Z"
	if f.kind == "snowflake" {
		at, beforeEpoch = "1735689600.123456789", "-0.000000001"
	} else if f.kind == "bigquery" {
		at, beforeEpoch = "1735689600123456", "-1"
	}
	data := []map[string]any{
		{},
		{"id": "9223372036854775807", "active": "true", "amount": "12345678901234567890.123456789", "at": at, "label": "kept"},
		{"id": "-9223372036854775808", "active": "false", "amount": "-0.000000001", "at": beforeEpoch, "label": "last"},
	}
	rows := make([][]any, len(data))
	for i, row := range data {
		rows[i] = make([]any, len(fields))
		for j, name := range fields {
			rows[i][j] = row[name]
		}
	}
	return rows
}

func (f *cloudFixture) result(job *cloudJob, page bool) map[string]any {
	statement := job.statement
	rows := f.rows(statement.fields)
	total, chunks := len(rows), 2
	if statement.scenario == "empty" || statement.scenario == "drift" {
		total, chunks, rows = 0, 1, nil
	} else if page {
		rows = rows[2:]
		if statement.scenario == "incomplete" {
			rows = nil
		}
	} else {
		rows = rows[:2]
	}
	switch f.kind {
	case "snowflake":
		if page {
			return map[string]any{"data": rows}
		}
		parts := []map[string]int{{"rowCount": len(rows)}}
		if chunks == 2 {
			parts = append(parts, map[string]int{"rowCount": 1})
		}
		return map[string]any{"statementHandle": job.id, "code": "090001", "resultSetMetaData": map[string]any{"format": "jsonv2", "numRows": total, "rowType": f.columns(statement), "partitionInfo": parts}, "data": rows}
	case "databricks":
		chunk := map[string]any{"chunk_index": 0, "row_offset": 0, "row_count": len(rows), "data_array": rows}
		if page {
			chunk["chunk_index"], chunk["row_offset"] = 1, 2
			return chunk
		}
		if chunks == 2 {
			chunk["next_chunk_internal_link"] = "/api/2.0/sql/statements/" + job.id + "/result/chunks/1"
		}
		return map[string]any{"statement_id": job.id, "status": map[string]string{"state": "SUCCEEDED"}, "manifest": map[string]any{"format": "JSON_ARRAY", "schema": map[string]any{"columns": f.columns(statement)}, "total_row_count": total, "total_chunk_count": chunks, "truncated": statement.scenario == "truncated"}, "result": chunk}
	default:
		cells := make([]map[string]any, len(rows))
		for i, row := range rows {
			fields := make([]map[string]any, len(row))
			for j, value := range row {
				fields[j] = map[string]any{"v": value}
			}
			cells[i] = map[string]any{"f": fields}
		}
		result := map[string]any{"jobReference": job.ref, "jobComplete": true, "totalRows": strconv.Itoa(total), "rows": cells}
		if !page {
			result["schema"] = map[string]any{"fields": f.columns(statement)}
			if chunks == 2 {
				result["pageToken"] = "next+/="
			}
		}
		return result
	}
}

func TestCloudNativeFederationProjectionPaginationAndCount(t *testing.T) {
	for _, kind := range cloudKinds {
		t.Run(kind, func(t *testing.T) {
			projection := []string{"label", "amount", "id", "active", "at"}
			table, fixture := newCloudFixture(t, kind,
				cloudStatement{sql: cloudSelect(kind, projection), fields: projection},
				cloudStatement{sql: cloudSelect(kind, []string{"id"}), fields: []string{"id"}},
			)
			if table.Schema().NumFields() != len(cloudFieldNames) || table.Stats().Rows != 0 {
				t.Fatal("empty schema discovery lost columns or counted rows")
			}
			reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: projection})
			if err != nil {
				t.Fatal(err)
			}
			var record arrow.RecordBatch
			func() {
				defer reader.Release()
				if !reader.Next() {
					t.Fatalf("missing cloud batch: %v", reader.Err())
				}
				record = reader.RecordBatch()
				record.Retain()
				if reader.Next() || reader.Err() != nil {
					record.Release()
					t.Fatalf("cloud scan failed at EOF: %v", reader.Err())
				}
			}()
			defer record.Release()
			if record.NumRows() != 3 {
				t.Fatalf("pagination delivered %d rows, want 3", record.NumRows())
			}
			for i, sourceIndex := range []int{4, 2, 0, 1, 3} {
				field := record.Schema().Field(i)
				if !field.Equal(table.Schema().Field(sourceIndex)) || field.Metadata.Len() == 0 || !record.Column(i).IsNull(0) {
					t.Fatalf("projection lost field order, metadata or NULL: %s", field.Name)
				}
			}
			if record.Column(0).(*array.String).Value(1) != "kept" || record.Column(1).(*array.Decimal128).Value(1).ToString(9) != "12345678901234567890.123456789" || record.Column(1).(*array.Decimal128).Value(2).ToString(9) != "-0.000000001" {
				t.Fatal("cloud decimal or text values changed")
			}
			if kind == "snowflake" {
				id := record.Column(2).(*array.Decimal128)
				if id.Value(1).ToString(0) != "9223372036854775807" || id.Value(2).ToString(0) != "-9223372036854775808" {
					t.Fatal("Snowflake scale-zero exact numeric values changed")
				}
			} else {
				id := record.Column(2).(*array.Int64)
				if id.Value(1) != 9223372036854775807 || id.Value(2) != -9223372036854775808 {
					t.Fatal("cloud Int64 boundary values changed")
				}
			}
			if !record.Column(3).(*array.Boolean).Value(1) || record.Column(3).(*array.Boolean).Value(2) {
				t.Fatal("cloud booleans changed")
			}
			expectedTime, expectedUnit := arrow.Timestamp(1735689600123456789), arrow.Nanosecond
			if kind == "bigquery" {
				expectedTime, expectedUnit = 1735689600123456, arrow.Microsecond
			}
			if record.Column(4).(*array.Timestamp).Value(1) != expectedTime || record.Column(4).(*array.Timestamp).Value(2) != -1 || record.Schema().Field(4).Type.(*arrow.TimestampType).Unit != expectedUnit {
				t.Fatal("cloud timestamp precision or pre-epoch value changed")
			}
			count, err := table.Scan(context.Background(), duckbridge.ScanPlan{})
			if err != nil {
				t.Fatal(err)
			}
			defer count.Release()
			if count.Schema().NumFields() != 1 || !count.Schema().Field(0).Equal(table.Schema().Field(0)) || !count.Next() || count.RecordBatch().NumRows() != 3 || !count.RecordBatch().Column(0).IsNull(0) {
				t.Fatalf("zero-column scan changed the first field or NULL row cardinality: %v", count.Err())
			}
			if count.Next() || count.Err() != nil {
				t.Fatalf("count scan failed at EOF: %v", count.Err())
			}
			if fixture.pages.Load() != 2 || fixture.cancels.Load() != 0 || table.Stats().Rows != 6 {
				t.Fatalf("native pagination/count stats changed: pages=%d cancellations=%d stats=%+v", fixture.pages.Load(), fixture.cancels.Load(), table.Stats())
			}
			fixture.submittedAll()
		})
	}
}

func TestCloudNativeFederationExactPredicatesAndEmptyScans(t *testing.T) {
	for _, kind := range cloudKinds {
		t.Run(kind, func(t *testing.T) {
			fields := []string{"active", "id"}
			filters := []duckbridge.Filter{{Kind: "comparison", Column: "active", Type: "bool", Op: "eq", Value: "true"}, {Kind: "is_null", Column: "label"}}
			sql := cloudSelect(kind, fields) + " WHERE (" + cloudQuote(kind, "active") + " = true) AND (" + cloudQuote(kind, "label") + " IS NULL)"
			if kind != "snowflake" {
				filters = append(filters, duckbridge.Filter{Kind: "comparison", Column: "id", Type: "int64", Op: "ge", Value: "-9223372036854775808"})
				cast := "BIGINT"
				if kind == "bigquery" {
					cast = "INT64"
				}
				sql += " AND (" + cloudQuote(kind, "id") + " >= CAST('-9223372036854775808' AS " + cast + "))"
			}
			table, fixture := newCloudFixture(t, kind, cloudStatement{sql: sql, fields: fields, scenario: "empty"})
			reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: fields, Filters: filters})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Release()
			if reader.Schema().NumFields() != 2 || !reader.Schema().Field(0).Equal(table.Schema().Field(1)) || !reader.Schema().Field(1).Equal(table.Schema().Field(0)) || reader.Next() || reader.Err() != nil {
				t.Fatalf("empty scan failed to preserve its projected schema: %v", reader.Err())
			}
			for _, filter := range []duckbridge.Filter{
				{Kind: "comparison", Column: "label", Type: "string", Op: "eq", Value: "kept"},
				{Kind: "comparison", Column: "amount", Type: "decimal", Op: "eq", Value: "1.0"},
				{Kind: "comparison", Column: "at", Type: "timestamp", Op: "eq", Value: "2025-01-01"},
			} {
				if extra, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: fields, Filters: []duckbridge.Filter{filter}}); err == nil {
					extra.Release()
					t.Fatal("unsupported required predicate reached the native provider")
				} else {
					checkCode(t, err, "UNSUPPORTED")
				}
			}
			fixture.submittedAll()
		})
	}
}

func TestCloudNativeFederationRejectsSchemaDriftAndIncompletePages(t *testing.T) {
	for _, kind := range cloudKinds {
		for _, scenario := range []string{"drift", "incomplete", "truncated"} {
			if scenario == "truncated" && kind != "databricks" {
				continue
			}
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				fields := []string{"active"}
				table, fixture := newCloudFixture(t, kind, cloudStatement{sql: cloudSelect(kind, fields), fields: fields, scenario: scenario})
				reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: fields})
				if err != nil {
					t.Fatal(err)
				}
				if reader.Next() {
					reader.Release()
					t.Fatal("invalid native result was delivered")
				}
				code := "QUERY_FAILED"
				if scenario == "truncated" {
					code = "RESOURCE_EXHAUSTED"
				}
				checkCode(t, reader.Err(), code)
				reader.Release()
				if fixture.cancels.Load() != 1 {
					t.Fatalf("native failure did not cancel its statement: %d", fixture.cancels.Load())
				}
				fixture.submittedAll()
			})
		}
	}
}

func TestCloudNativeFederationCancellationReachesProvider(t *testing.T) {
	for _, kind := range cloudKinds {
		t.Run(kind, func(t *testing.T) {
			fields := []string{"id"}
			table, fixture := newCloudFixture(t, kind, cloudStatement{sql: cloudSelect(kind, fields), fields: fields, scenario: "pending"})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader, err := table.Scan(ctx, duckbridge.ScanPlan{Columns: fields})
			if err != nil {
				t.Fatal(err)
			}
			// Waiting for a poll proves the native executor received its handle
			// before cancellation, so the provider cleanup path must execute.
			await(t, fixture.polling)
			cancel()
			if reader.Next() {
				reader.Release()
				t.Fatal("cancelled cloud statement delivered a batch")
			}
			checkCode(t, reader.Err(), "CANCELLED")
			reader.Release()
			if fixture.cancels.Load() != 1 {
				t.Fatalf("source cancellation was not joined: %d", fixture.cancels.Load())
			}
			fixture.submittedAll()
		})
	}
}
