// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/mysql"
	"github.com/SYNEHQ/kelvo-go/internal/sources/postgres"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlnative"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"os"
	"testing"
	"time"
)

type liveSink struct {
	schema  *arrow.Schema
	records []arrow.RecordBatch
}

func (s *liveSink) Schema(v *arrow.Schema) error { s.schema = v; return nil }
func (s *liveSink) Write(v arrow.RecordBatch) error {
	v.Retain()
	s.records = append(s.records, v)
	return nil
}
func (s *liveSink) Close() {
	for _, v := range s.records {
		v.Release()
	}
}
func openFixture(t *testing.T, driver, env string) *sql.DB {
	t.Helper()
	dsn := os.Getenv(env)
	if dsn == "" {
		t.Skip("dedicated TLS relational fixture is not configured")
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal("fixture connection setup failed")
	}
	t.Cleanup(func() { db.Close() })
	return db
}
func execFixture(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, statement); err != nil {
		t.Fatalf("fixture SQL failed (%T)", err)
	}
}
func execute(t *testing.T, e *sqlnative.Engine, r query.Request) (query.Stats, *liveSink, error) {
	t.Helper()
	s := new(liveSink)
	t.Cleanup(s.Close)
	stats, err := e.Execute(context.Background(), r, s)
	return stats, s, err
}
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || query.PublicError(err).Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}
func nativeConfig(kind string) catalog.Config {
	return catalog.Config{Sources: []catalog.Source{{ID: "source", Type: kind, DSNEnv: "KELVO_SOURCE_RELATIONAL_DSN"}}}
}
func nativeQuery(statement string) query.Request {
	return query.Request{Mode: "native", ConnectionID: "source", SQL: statement}
}
func assertTime(t *testing.T, a arrow.Array, zone string, hour int) {
	t.Helper()
	typ := a.DataType().(*arrow.TimestampType)
	stamp := a.(*array.Timestamp).Value(0).ToTime(typ.Unit)
	if typ.Unit != arrow.Microsecond || typ.TimeZone != zone || stamp.Hour() != hour || stamp.Nanosecond() != 123456000 {
		t.Fatal("timestamp precision/zone changed")
	}
}

func TestPostgresLiveNativeTLS(t *testing.T) {
	admin := openFixture(t, "pgx", "KELVO_TEST_POSTGRES_ADMIN_DSN")
	reader := openFixture(t, "pgx", "KELVO_TEST_POSTGRES_READER_DSN")
	t.Setenv("KELVO_SOURCE_RELATIONAL_DSN", os.Getenv("KELVO_TEST_POSTGRES_READER_DSN"))
	table := fmt.Sprintf("kelvo_native_%d", time.Now().UnixNano())
	execFixture(t, admin, "CREATE TABLE "+table+" (s SMALLINT, i INTEGER, b BIGINT, n NUMERIC(30,10), d DATE, ts TIMESTAMP(6), tz TIMESTAMPTZ(6), u UUID, js JSONB, raw BYTEA, yes BOOLEAN)")
	defer execFixture(t, admin, "DROP TABLE "+table)
	execFixture(t, admin, "INSERT INTO "+table+" VALUES (-32768,2147483647,9007199254740993,12345678901234567890.0123456789,'1969-12-31','2026-10-01 12:30:00.123456','2026-10-01 12:30:00.123456+05:30','123e4567-e89b-12d3-a456-426614174000','{\"nested\":[1,null]}',decode('00ff','hex'),true), (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
	execFixture(t, admin, "GRANT SELECT ON "+table+" TO kelvo_reader")
	e, err := postgres.New(nativeConfig("postgres"), query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	stats, sink, err := execute(t, e, nativeQuery("SELECT * FROM "+table+" ORDER BY b NULLS LAST"))
	if err != nil || stats.Rows != 2 {
		t.Fatalf("native result: %+v %v", stats, err)
	}
	r := sink.records[0]
	if r.Column(0).(*array.Int16).Value(0) != -32768 || r.Column(1).(*array.Int32).Value(0) != 2147483647 || r.Column(2).(*array.Int64).Value(0) != 9007199254740993 || r.Column(3).(*array.Decimal128).Value(0).ToString(10) != "12345678901234567890.0123456789" {
		t.Fatal("numeric value/width changed")
	}
	if r.Column(4).(*array.Date32).Value(0) != -1 {
		t.Fatal("date changed")
	}
	assertTime(t, r.Column(5), "", 12)
	assertTime(t, r.Column(6), "UTC", 7)
	if r.Column(7).(*array.String).Value(0) != "123e4567-e89b-12d3-a456-426614174000" || string(r.Column(9).(*array.Binary).Value(0)) != string([]byte{0, 255}) || !r.Column(10).(*array.Boolean).Value(0) {
		t.Fatal("typed values changed")
	}
	for _, index := range []int{7, 8} {
		kind, _ := sink.schema.Field(index).Metadata.GetValue("native_type")
		if kind != []string{"UUID", "JSONB"}[index-7] {
			t.Fatal("source logical type lost")
		}
	}
	var js map[string]any
	if json.Unmarshal([]byte(r.Column(8).(*array.String).Value(0)), &js) != nil || len(js) != 1 {
		t.Fatal("JSON changed")
	}
	for i := 0; i < int(r.NumCols()); i++ {
		if !r.Column(i).IsNull(1) {
			t.Fatal("NULL changed")
		}
	}
	req := nativeQuery("SELECT b FROM " + table + " WHERE b=$1")
	req.Parameters = []query.Parameter{{Type: "int64", Value: json.RawMessage(`"9007199254740993"`)}}
	stats, _, err = execute(t, e, req)
	if err != nil || stats.Rows != 1 {
		t.Fatalf("PostgreSQL parameter failed: %v", err)
	}
	stats, sink, err = execute(t, e, nativeQuery("SELECT current_setting('transaction_read_only') AS mode"))
	if err != nil || stats.Rows != 1 || sink.records[0].Column(0).(*array.String).Value(0) != "on" {
		t.Fatal("transaction not read-only")
	}
	_, _, err = execute(t, e, nativeQuery("SELECT 1.23::numeric AS ambiguous"))
	requireCode(t, err, "UNSUPPORTED")
	var ssl bool
	if err := reader.QueryRow("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()").Scan(&ssl); err != nil || !ssl {
		t.Fatal("fixture did not use TLS")
	}
	if _, err := reader.Exec("INSERT INTO " + table + " (i) VALUES (1)"); err == nil {
		t.Fatal("reader could write")
	}
	limits := query.DefaultLimits()
	limits.Timeout = 50 * time.Millisecond
	e, err = postgres.New(nativeConfig("postgres"), limits)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = execute(t, e, nativeQuery("SELECT pg_sleep(5)"))
	requireCode(t, err, "DEADLINE_EXCEEDED")
}

func TestMySQLLiveNativeTLS(t *testing.T) {
	admin := openFixture(t, "mysql", "KELVO_TEST_MYSQL_ADMIN_DSN")
	reader := openFixture(t, "mysql", "KELVO_TEST_MYSQL_READER_DSN")
	t.Setenv("KELVO_SOURCE_RELATIONAL_DSN", os.Getenv("KELVO_TEST_MYSQL_READER_DSN"))
	table := fmt.Sprintf("kelvo_native_%d", time.Now().UnixNano())
	execFixture(t, admin, "CREATE TABLE "+table+" (s TINYINT, u SMALLINT UNSIGNED, b BIGINT UNSIGNED, n DECIMAL(30,10), d DATE, ts DATETIME(6), tz TIMESTAMP(6), bits BIT(9), js JSON)")
	defer execFixture(t, admin, "DROP TABLE "+table)
	execFixture(t, admin, "INSERT INTO "+table+" VALUES (-128,65535,18446744073709551615,12345678901234567890.0123456789,'1969-12-31','2026-10-01 12:30:00.123456','2026-10-01 12:30:00.123456',b'100000001','{}'), (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
	e, err := mysql.New(nativeConfig("mysql"), query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	stats, sink, err := execute(t, e, nativeQuery("SELECT * FROM "+table+" ORDER BY s IS NULL"))
	if err != nil || stats.Rows != 2 {
		t.Fatalf("MySQL result %+v %v", stats, err)
	}
	r := sink.records[0]
	if r.Column(0).(*array.Int8).Value(0) != -128 || r.Column(1).(*array.Uint16).Value(0) != 65535 || r.Column(2).(*array.Uint64).Value(0) != ^uint64(0) {
		t.Fatal("MySQL integer sign, width or value changed")
	}
	if r.Column(3).(*array.Decimal128).Value(0).ToString(10) != "12345678901234567890.0123456789" || r.Column(4).(*array.Date32).Value(0) != -1 {
		t.Fatal("decimal/date changed")
	}
	assertTime(t, r.Column(5), "", 12)
	assertTime(t, r.Column(6), "UTC", 12)
	if string(r.Column(7).(*array.Binary).Value(0)) != string([]byte{1, 1}) {
		t.Fatal("BIT(9) was coerced")
	}
	for i := 0; i < int(r.NumCols()); i++ {
		if !r.Column(i).IsNull(1) {
			t.Fatal("NULL changed")
		}
	}
	req := nativeQuery("SELECT s FROM " + table + " WHERE s=?")
	req.Parameters = []query.Parameter{{Type: "int64", Value: json.RawMessage(`-128`)}}
	stats, _, err = execute(t, e, req)
	if err != nil || stats.Rows != 1 {
		t.Fatalf("MySQL parameter failed: %v", err)
	}
	var key, cipher string
	if err := reader.QueryRow("SHOW STATUS LIKE 'Ssl_cipher'").Scan(&key, &cipher); err != nil || cipher == "" {
		t.Fatal("fixture did not use TLS")
	}
	if _, err := reader.Exec("INSERT INTO " + table + " (s) VALUES (1)"); err == nil {
		t.Fatal("reader could write")
	}
	limits := query.DefaultLimits()
	limits.MaxRows = 1
	e, err = mysql.New(nativeConfig("mysql"), limits)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = execute(t, e, nativeQuery("SELECT s FROM "+table))
	requireCode(t, err, "RESOURCE_EXHAUSTED")
	limits = query.DefaultLimits()
	limits.Timeout = 50 * time.Millisecond
	e, err = mysql.New(nativeConfig("mysql"), limits)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = execute(t, e, nativeQuery("SELECT SLEEP(5)"))
	requireCode(t, err, "DEADLINE_EXCEEDED")
}

func TestRelationalFamilyIdentity(t *testing.T) {
	t.Setenv("KELVO_SOURCE_RELATIONAL_DSN", "invalid source DSN")
	for _, test := range []struct {
		kind        string
		constructor func(catalog.Config, query.Limits) (*sqlnative.Engine, error)
	}{
		{"postgres", postgres.New}, {"postgresql", postgres.NewPostgreSQL}, {"cockroachdb", postgres.NewCockroachDB}, {"alloydb", postgres.NewAlloyDB}, {"redshift", postgres.NewRedshift}, {"mysql", mysql.New}, {"mariadb", mysql.NewMariaDB},
	} {
		t.Run(test.kind, func(t *testing.T) {
			e, err := test.constructor(nativeConfig(test.kind), query.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			stats, _, err := execute(t, e, nativeQuery("SELECT 1"))
			requireCode(t, err, "CONFIGURATION_ERROR")
			if stats.Backend != test.kind {
				t.Fatal("source family identity was rewritten")
			}
		})
	}
}
