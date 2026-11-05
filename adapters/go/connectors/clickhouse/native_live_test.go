package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	native "github.com/SYNEHQ/kelvo-go/internal/sources/clickhouse"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

// Live against a real ClickHouse; CI skips. Env: KELVO_TEST_CLICKHOUSE_{NATIVE,HTTPS}_URL
// (clickhouses://host:port?database=db / https://host:port/?database=db),
// _CA and _OTHER_CA (PEM file paths), _USER, _PASSWORD.
func TestClickHouseLiveProtocols(t *testing.T) {
	for _, name := range []string{"NATIVE", "HTTPS"} {
		raw := os.Getenv("KELVO_TEST_CLICKHOUSE_" + name + "_URL")
		if raw == "" {
			t.Skipf("KELVO_TEST_CLICKHOUSE_%s_URL not set", name)
		}
		t.Run(name, func(t *testing.T) { liveProtocol(t, raw) })
	}
}

func liveRoots(t *testing.T, env string) *x509.CertPool {
	pem, err := os.ReadFile(os.Getenv(env))
	roots := x509.NewCertPool()
	if err != nil || !roots.AppendCertsFromPEM(pem) {
		t.Fatalf("%s unreadable", env)
	}
	return roots
}

func liveProtocol(t *testing.T, raw string) {
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(u.Port())
	conn := adapter.Connection{Engine: "clickhouse", TenantID: "team", ConnectionID: "saved", Revision: "r", Host: u.Hostname(), Port: port, Namespace: u.Query().Get("database"),
		Username: os.Getenv("KELVO_TEST_CLICKHOUSE_USER"), Password: os.Getenv("KELVO_TEST_CLICKHOUSE_PASSWORD"),
		TLS: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname(), RootCAs: liveRoots(t, "KELVO_TEST_CLICKHOUSE_CA")}}
	if u.Scheme == "clickhouses" {
		conn.Options = map[string]string{"protocol": "native"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	session, err := Driver{}.Open(ctx, conn) // Open runs Test (SELECT 1)
	if err != nil {
		t.Fatalf("open/test: %s", logLine(err))
	}
	defer session.Close()
	sink := &collectSink{}
	const sql = "SELECT toInt32(-7) AS i32, toUInt64(42) AS u64, 'hello' AS s, CAST(NULL, 'Nullable(Int64)') AS n, toNullable(toInt64(5)) AS nn, " +
		"toDateTime('2026-01-02 03:04:05', 'UTC') AS dt, toDateTime64('2026-01-02 03:04:05.678', 3, 'UTC') AS dt64, toDecimal64(12.34, 2) AS dec, " +
		"toUUID('123e4567-e89b-12d3-a456-426614174000') AS id, [1, 2, 3] AS arr, toDate('2026-01-02') AS d, toFixedString('ab', 2) AS fs"
	statement := sql
	if u.Scheme == "https" {
		// ClickHouse's ArrowStream output cannot encode UUID (server code 50).
		statement = strings.Replace(sql, "toUUID('123e4567-e89b-12d3-a456-426614174000')", "toString(toUUID('123e4567-e89b-12d3-a456-426614174000'))", 1)
	}
	stats, err := session.(*Session).Query(ctx, adapter.Query{Statement: statement, MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 10}, sink)
	if err != nil || stats.Rows != 1 || len(sink.records) != 1 {
		t.Fatalf("query: rows=%d err=%s", stats.Rows, logLine(err))
	}
	t.Logf("schema: %s", sink.schema)
	record := sink.records[0]
	var row []string
	for i, column := range record.Columns() {
		row = append(row, record.ColumnName(i)+"="+column.ValueStr(0))
	}
	t.Logf("row 0: %s", strings.Join(row, " "))
	field := func(name string) arrow.DataType {
		f, _ := sink.schema.FieldsByName(name)
		return f[0].Type
	}
	if u.Scheme == "clickhouses" {
		for name, want := range map[string]string{"i32": "int32", "u64": "uint64", "s": "utf8", "n": "int64", "nn": "int64", "dt": "timestamp[s, tz=UTC]", "dt64": "timestamp[ms, tz=UTC]",
			"dec": "decimal(18, 2)", "id": "utf8", "arr": "utf8", "d": "date32", "fs": "binary"} {
			if got := field(name).String(); got != want {
				t.Errorf("%s: %s want %s", name, got, want)
			}
		}
		for i, want := range []string{"-7", "42", "hello", "(null)", "5", "2026-01-02T03:04:05Z", "2026-01-02T03:04:05.678Z", "12.34", "123e4567-e89b-12d3-a456-426614174000", "[1 2 3]", "2026-01-02", "YWI="} {
			if got := record.Column(i).ValueStr(0); got != want {
				t.Errorf("%s: %q want %q", record.ColumnName(i), got, want)
			}
		}
	}

	// Server-side readonly: bypass the adapter's SQL guard and send DDL to the engine.
	engine, err := native.NewResolved(session.(*Session).source, query.Limits{MaxRows: 10, MaxBytes: 1 << 20, Timeout: 10 * time.Second, MemoryMB: 64, Threads: 1, MaxTempMB: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	_, err = engine.Execute(ctx, query.Request{Mode: "native", ConnectionID: "operation_source", SQL: "CREATE TABLE kelvo_readonly_probe (x Int32) ENGINE = Memory"}, &collectSink{})
	t.Logf("readonly: %s", logLine(err))
	if failure := asFailure(err); failure == nil || failure.ClickHouseCode != "164" {
		t.Fatalf("CREATE not refused as READONLY: %v", err)
	}

	wrongCA := conn
	wrongCA.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname(), RootCAs: liveRoots(t, "KELVO_TEST_CLICKHOUSE_OTHER_CA")}
	_, err = Driver{}.Open(ctx, wrongCA)
	t.Logf("wrong CA: %s", logLine(err))
	if failure := asFailure(err); failure == nil || failure.Reason != "tls_unknown_authority" {
		t.Fatalf("wrong CA: %v", err)
	}

	wrongPassword := conn
	wrongPassword.Password = "not-the-password"
	_, err = Driver{}.Open(ctx, wrongPassword)
	t.Logf("wrong password: %s", logLine(err))
	if failure := asFailure(err); failure == nil || failure.ClickHouseCode != "516" {
		t.Fatalf("wrong password: %v", err)
	}
}

func asFailure(err error) *operations.SourceFailure {
	var f *native.Failure
	if errors.As(err, &f) {
		return &f.SourceFailure
	}
	return nil
}

func logLine(err error) string {
	if f := asFailure(err); f != nil {
		return f.LogLine() + " (public: " + err.Error() + ")"
	}
	return "no failure: " + strings.TrimSpace(func() string {
		if err == nil {
			return "<nil>"
		}
		return err.Error()
	}())
}

type collectSink struct {
	schema  *arrow.Schema
	records []arrow.RecordBatch
}

func (s *collectSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *collectSink) Write(record arrow.RecordBatch) error {
	record.Retain()
	s.records = append(s.records, record)
	return nil
}
