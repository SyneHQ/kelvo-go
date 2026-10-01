//go:build duckdb_arrow

package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type captureSink struct {
	schema       *arrow.Schema
	rows         int64
	batches      int64
	values       []string
	nulls        int
	ids          []int64
	decimals     []string
	decimalNulls int
}

func (s *captureSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *captureSink) Write(record arrow.RecordBatch) error {
	s.rows += record.NumRows()
	s.batches++
	if record.NumCols() > 0 {
		if value, ok := record.Column(0).(*array.String); ok {
			for i := 0; i < value.Len(); i++ {
				if value.IsNull(i) {
					s.nulls++
				} else {
					s.values = append(s.values, strings.Clone(value.Value(i)))
				}
			}
		}
	}
	if record.NumCols() > 1 {
		if value, ok := record.Column(1).(*array.Decimal128); ok {
			decimalType := value.DataType().(arrow.DecimalType)
			for i := 0; i < value.Len(); i++ {
				if value.IsNull(i) {
					s.decimalNulls++
				} else {
					s.decimals = append(s.decimals, value.Value(i).ToString(decimalType.GetScale()))
				}
			}
		}
	}
	if record.NumCols() > 2 {
		if value, ok := record.Column(2).(*array.Int64); ok {
			for i := 0; i < value.Len(); i++ {
				if !value.IsNull(i) {
					s.ids = append(s.ids, value.Value(i))
				}
			}
		}
	}
	return nil
}
func limits() query.Limits {
	return query.Limits{MaxRows: 10, MaxBytes: 1 << 20, Timeout: time.Second, MemoryMB: 256, Threads: 1, MaxTempMB: 64}
}
func makeEngine(t *testing.T, csv string) *Engine {
	t.Helper()
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "events", Type: "csv", Path: csv}}}, limits())
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func TestExecuteJoinNullPrecisionAndParameters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(path, []byte("id,name,amount\n1,one,1.10\n2,,2.20\n3,three,3.30\n"), 0600); err != nil {
		t.Fatal(err)
	}
	e := makeEngine(t, path)
	sink := new(captureSink)
	req := query.Request{Sources: []string{"events"}, SQL: "SELECT e.name, CAST(e.amount AS DECIMAL(10,2)) AS amount, e.id FROM events e JOIN events e2 USING (id) WHERE e.id > ? ORDER BY e.id;", Parameters: []query.Parameter{{Type: "int64", Value: json.RawMessage(`"1"`)}}}
	stats, err := e.Execute(context.Background(), req, sink)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 2 || stats.Batches < 1 || sink.schema == nil {
		t.Fatalf("unexpected stats %#v", stats)
	}
	if sink.nulls != 1 || len(sink.values) != 1 || sink.values[0] != "three" {
		t.Fatalf("NULL/string values: nulls=%d values=%#v", sink.nulls, sink.values)
	}
	if len(sink.decimals) != 2 || sink.decimals[0] != "2.20" || sink.decimals[1] != "3.30" {
		t.Fatalf("decimal values %#v", sink.decimals)
	}
	if len(sink.ids) != 2 || sink.ids[0] != 2 || sink.ids[1] != 3 {
		t.Fatalf("join/parameter ids %#v", sink.ids)
	}
	if sink.schema.Field(1).Type.ID() != arrow.DECIMAL128 {
		t.Fatalf("decimal type = %s", sink.schema.Field(1).Type)
	}
}
func TestExecuteRejectsDeniedSQLAndUnknownSource(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(path, []byte("id\n1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	e := makeEngine(t, path)
	for _, req := range []query.Request{{Sources: []string{"events"}, SQL: "DELETE FROM events"}, {Sources: []string{"events"}, SQL: "SELECT * FROM read_csv_auto('/tmp/nope')"}, {Sources: []string{"missing"}, SQL: "SELECT 1"}} {
		_, err := e.Execute(context.Background(), req, new(captureSink))
		if err == nil {
			t.Fatalf("expected rejection for %q", req.SQL)
		}
		if got := query.PublicError(err).Code; got != "PERMISSION_DENIED" {
			t.Fatalf("code %s for %q", got, req.SQL)
		}
	}
	// duckdb-go extracts and counts statements before it prepares the selected
	// statement, so this must fail without running the DROP statement.
	_, err := e.Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: "SELECT 1; DROP TABLE events"}, new(captureSink))
	if err == nil || query.PublicError(err).Code != "PERMISSION_DENIED" {
		t.Fatalf("expected public multi-statement denial, got %v", err)
	}
	_, err = e.Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: "SELECT 1; SELECT 2"}, new(captureSink))
	if err == nil || query.PublicError(err).Code != "PERMISSION_DENIED" {
		t.Fatalf("expected public multi-select denial, got %v", err)
	}
}

func createFileSource(t *testing.T, path, sourceType string) {
	t.Helper()
	literal := strings.ReplaceAll(path, "'", "''")
	if sourceType == "parquet" {
		db, err := sql.Open("duckdb", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for _, statement := range []string{
			"CREATE TABLE events (id INTEGER, name VARCHAR)",
			"INSERT INTO events VALUES (1, 'one'), (2, NULL)",
			"COPY events TO '" + literal + "' (FORMAT PARQUET)",
		} {
			if _, err := db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE events (id INTEGER, name VARCHAR)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO events VALUES (1, 'one'), (2, NULL)"); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteParquetAndDuckDBFileSources(t *testing.T) {
	for _, sourceType := range []string{"parquet", "duckdb"} {
		t.Run(sourceType, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events."+sourceType)
			createFileSource(t, path, sourceType)
			e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "data", Type: sourceType, Path: path}}}, limits())
			if err != nil {
				t.Fatal(err)
			}
			from := "data"
			if sourceType == "duckdb" {
				from = "data.main.events"
			}
			sink := new(captureSink)
			stats, err := e.Execute(context.Background(), query.Request{
				Sources: []string{"data"},
				SQL:     "SELECT name, CAST(id AS DECIMAL(10,2)) AS amount, id FROM " + from + " ORDER BY id",
			}, sink)
			if err != nil {
				t.Fatal(err)
			}
			if stats.Rows != 2 || sink.nulls != 1 || len(sink.values) != 1 || sink.values[0] != "one" {
				t.Fatalf("rows/null/string: stats=%#v nulls=%d values=%#v", stats, sink.nulls, sink.values)
			}
			if len(sink.decimals) != 2 || sink.decimals[0] != "1.00" || sink.decimals[1] != "2.00" {
				t.Fatalf("decimal values %#v", sink.decimals)
			}
			if sink.schema == nil || sink.schema.Field(1).Type.ID() != arrow.DECIMAL128 {
				t.Fatalf("decimal schema %#v", sink.schema)
			}
		})
	}
}

func TestNewRejectsInvalidDirectSourceID(t *testing.T) {
	_, err := New(catalog.Config{Sources: []catalog.Source{{ID: "data;drop", Type: "csv", Path: "/tmp/example.csv"}}}, limits())
	if err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" {
		t.Fatalf("expected invalid source ID rejection, got %v", err)
	}
}

func TestExecuteRealPostgresMySQLFederation(t *testing.T) {
	catalogPath := os.Getenv("KELVO_REAL_INTEGRATION_CATALOG")
	if catalogPath == "" || os.Getenv("KELVO_REAL_INTEGRATION") != "1" {
		t.Skip("set KELVO_REAL_INTEGRATION=1 and KELVO_REAL_INTEGRATION_CATALOG to run")
	}
	config, err := catalog.Load(catalogPath)
	if err != nil {
		t.Fatal("load integration catalog:", err)
	}
	e, err := New(config, query.Limits{MaxRows: 10, MaxBytes: 1 << 20, Timeout: 15 * time.Second, MemoryMB: 512, Threads: 1, MaxTempMB: 128})
	if err != nil {
		t.Fatal(err)
	}
	sink := new(captureSink)
	stats, err := e.Execute(context.Background(), query.Request{
		Sources: []string{"pg", "my"},
		SQL:     "SELECT c.name, SUM(o.amount) AS amount FROM pg.public.customers c LEFT JOIN my.kelvo_test.orders o ON c.id=o.customer_id GROUP BY c.name ORDER BY c.name",
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 3 || len(sink.values) != 3 || sink.values[0] != "Ada" || sink.values[1] != "Lin" || sink.values[2] != "Null" {
		t.Fatalf("federated names/stats: rows=%d names=%#v", stats.Rows, sink.values)
	}
	if len(sink.decimals) != 2 || sink.decimals[0] != "19.75" || sink.decimals[1] != "20.00" || sink.decimalNulls != 1 {
		t.Fatalf("federated amounts: values=%#v nulls=%d", sink.decimals, sink.decimalNulls)
	}
}

func TestExecuteRowLimitFailsRatherThanTruncates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	if err := os.WriteFile(path, []byte("id\n1\n2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	e := makeEngine(t, path)
	e.limits.MaxRows = 1
	_, err := e.Execute(context.Background(), query.Request{Sources: []string{"events"}, SQL: "SELECT * FROM events"}, new(captureSink))
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("expected explicit row limit error, got %v", err)
	}
}
func TestExecuteHonorsAlreadyCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e, err := New(catalog.Config{}, limits())
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Execute(ctx, query.Request{SQL: "SELECT 1"}, new(captureSink))
	if err == nil || query.PublicError(err).Code != "CANCELLED" {
		t.Fatalf("expected cancellation, got %v", err)
	}
}
