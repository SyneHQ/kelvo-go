// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package pgwire

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/transport/httpapi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const token = "0123456789abcdef0123456789abcdef-pgwire"

// fakeExecutor stands in for DuckDB behind the real httpapi.Server.
type fakeExecutor struct {
	mu       sync.Mutex
	requests []query.Request
	canceled chan struct{}
}

func (f *fakeExecutor) last() query.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[len(f.requests)-1]
}

var typesSchema = arrow.NewSchema([]arrow.Field{
	{Name: "i2", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
	{Name: "i4", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "i8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "num", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}, Nullable: true},
	{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	{Name: "t", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
	{Name: "tstz", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	{Name: "d", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
	{Name: "f8", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
}, nil)

var when = time.Date(2024, 3, 4, 5, 6, 7, 123456000, time.UTC)

func typesBatch() arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, typesSchema)
	defer b.Release()
	b.Field(0).(*array.Int16Builder).AppendValues([]int16{-32768, 0}, []bool{true, false})
	b.Field(1).(*array.Int32Builder).AppendValues([]int32{2147483647, 0}, []bool{true, false})
	b.Field(2).(*array.Int64Builder).AppendValues([]int64{-9223372036854775808, 0}, []bool{true, false})
	b.Field(3).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(-1234567)}, nil)
	b.Field(3).AppendNull()
	b.Field(4).(*array.BooleanBuilder).AppendValues([]bool{true, false}, []bool{true, false})
	b.Field(5).(*array.StringBuilder).AppendValues([]string{"héllo 'x'", ""}, []bool{true, false})
	ts := arrow.Timestamp(when.UnixMicro())
	b.Field(6).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{ts, 0}, []bool{true, false})
	b.Field(7).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{ts, 0}, []bool{true, false})
	b.Field(8).(*array.Date32Builder).AppendValues([]arrow.Date32{arrow.Date32FromTime(when)}, nil)
	b.Field(8).AppendNull()
	b.Field(9).(*array.Float64Builder).AppendValues([]float64{0.1, 0}, []bool{true, false})
	b.Field(10).(*array.BinaryBuilder).AppendValues([][]byte{{0, 1, 0xff}, nil}, []bool{true, false})
	return b.NewRecordBatch()
}

func (f *fakeExecutor) Execute(ctx context.Context, r query.Request, sink query.Sink) (query.Stats, error) {
	f.mu.Lock()
	f.requests = append(f.requests, r)
	f.mu.Unlock()
	if r.Sources[0] != "sales" {
		return query.Stats{}, query.NewError("PERMISSION_DENIED", "Query access denied")
	}
	switch {
	case strings.Contains(r.SQL, "sleep"):
		<-ctx.Done()
		close(f.canceled)
		return query.Stats{}, ctx.Err()
	case strings.Contains(r.SQL, "types"):
		batch := typesBatch()
		defer batch.Release()
		if err := sink.Schema(typesSchema); err != nil {
			return query.Stats{}, err
		}
		if strings.Contains(r.SQL, "LIMIT 0") {
			return query.Stats{}, nil
		}
		return query.Stats{}, sink.Write(batch)
	case strings.Contains(r.SQL, "truncated"):
		batch := typesBatch()
		defer batch.Release()
		_ = sink.Schema(typesSchema)
		_ = sink.Write(batch)
		return query.Stats{}, query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds row limit")
	case strings.Contains(r.SQL, "listtype"):
		s := arrow.NewSchema([]arrow.Field{{Name: "l", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32)}}, nil)
		return query.Stats{}, sink.Schema(s)
	}
	s := arrow.NewSchema([]arrow.Field{{Name: "one", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, s)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	batch := b.NewRecordBatch()
	defer batch.Release()
	if err := sink.Schema(s); err != nil {
		return query.Stats{}, err
	}
	if strings.Contains(r.SQL, "LIMIT 0") {
		return query.Stats{}, nil
	}
	return query.Stats{}, sink.Write(batch)
}

func certFiles(t *testing.T) (string, string) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kd, _ := x509.MarshalECPrivateKey(key)
	dir := t.TempDir()
	cert, keyFile := filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
	_ = os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	_ = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd}), 0o600)
	return cert, keyFile
}

func start(t *testing.T, mutate func(*Config)) (*fakeExecutor, string) {
	t.Helper()
	exec := &fakeExecutor{canceled: make(chan struct{})}
	limits := query.DefaultLimits()
	h, err := httpapi.New(exec, httpapi.Options{Token: token, Limits: limits, MaxQueries: 64, MaxConcurrent: 4, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	cert, key := certFiles(t)
	cfg := Config{Listen: "127.0.0.1:0", TLS: TLSConfig{CertFile: cert, KeyFile: key}, MaxConnections: 8, IdleTimeout: time.Minute, StatementTimeout: 10 * time.Second}
	if mutate != nil {
		mutate(&cfg)
	}
	s, err := New(cfg, h)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() { _ = s.Close(); _ = h.Close() })
	return exec, ln.Addr().String()
}

func dsn(addr, db, password, sslmode string) string {
	host, port, _ := net.SplitHostPort(addr)
	return "host=" + host + " port=" + port + " user=analyst dbname=" + db + " password=" + password + " sslmode=" + sslmode
}

func connect(t *testing.T, addr string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(context.Background(), dsn(addr, "sales", token, "require"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func sqlState(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func TestTypesTextAndBinary(t *testing.T) {
	_, addr := start(t, nil)
	c := connect(t, addr)
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement, pgx.QueryExecModeExec} {
		rows, err := c.Query(context.Background(), "SELECT * FROM types", mode)
		if err != nil {
			t.Fatal(mode, err)
		}
		fields := rows.FieldDescriptions()
		wantOIDs := []uint32{pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID, pgtype.NumericOID, pgtype.BoolOID, pgtype.TextOID, pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.DateOID, pgtype.Float8OID, pgtype.ByteaOID}
		for i, f := range fields {
			if f.DataTypeOID != wantOIDs[i] {
				t.Fatalf("%v column %s oid %d", mode, f.Name, f.DataTypeOID)
			}
		}
		if fields[3].TypeModifier != (18<<16|2)+4 {
			t.Fatalf("numeric typmod %d", fields[3].TypeModifier)
		}
		var (
			i2      *int16
			i4      *int32
			i8      *int64
			num     pgtype.Numeric
			b       *bool
			s       *string
			ts, tz  *time.Time
			d       *time.Time
			f8      *float64
			bin     []byte
			n       int
			numText string
		)
		for rows.Next() {
			if err := rows.Scan(&i2, &i4, &i8, &num, &b, &s, &ts, &tz, &d, &f8, &bin); err != nil {
				t.Fatal(mode, err)
			}
			if n == 0 {
				v, _ := num.Value()
				numText, _ = v.(string)
				if *i2 != -32768 || *i4 != 2147483647 || *i8 != -9223372036854775808 || numText != "-12345.67" || !*b || *s != "héllo 'x'" ||
					!ts.Equal(when) || !tz.Equal(when) || d.Format("2006-01-02") != "2024-03-04" || *f8 != 0.1 || string(bin) != "\x00\x01\xff" {
					t.Fatalf("%v row 0: %v %v %v %s %v %q %v %v %v %v %x", mode, *i2, *i4, *i8, numText, *b, *s, ts, tz, d, *f8, bin)
				}
			} else if i2 != nil || i4 != nil || i8 != nil || num.Valid || b != nil || s != nil || ts != nil || tz != nil || d != nil || f8 != nil || bin != nil {
				t.Fatalf("%v row 1 should be all NULL", mode)
			}
			n++
		}
		if rows.Err() != nil || n != 2 {
			t.Fatal(mode, rows.Err(), n)
		}
	}
}

func TestParametersKeepExactTypes(t *testing.T) {
	exec, addr := start(t, nil)
	c := connect(t, addr)
	m := pgtype.NewMap()
	var num pgtype.Numeric
	_ = num.Scan("123.45")
	oids := []uint32{pgtype.Int8OID, pgtype.NumericOID, pgtype.TextOID, pgtype.TimestamptzOID, pgtype.Int4OID, pgtype.Float4OID, pgtype.BoolOID}
	args := []any{int64(9223372036854775807), num, "it's", when, nil, float32(0.1), true}
	values := make([][]byte, len(args))
	for i, a := range args {
		if a != nil {
			values[i], _ = m.Encode(oids[i], pgtype.BinaryFormatCode, a, nil)
		}
	}
	formats := []int16{1, 1, 1, 1, 1, 1, 1}
	res := c.PgConn().ExecParams(context.Background(), "SELECT 1 WHERE $1 > 0 AND $2 = x AND $3 = y AND $4 = z AND $5 IS NULL AND $6 > 0 AND $7", values, oids, formats, []int16{1}).Read()
	if res.Err != nil || len(res.Rows) != 1 {
		t.Fatal(res.Err)
	}
	r := exec.last()
	if strings.Contains(r.SQL, "kelvo_describe") || r.Sources[0] != "sales" || r.Mode != "federated" {
		t.Fatalf("unexpected request %+v", r)
	}
	got := []string{}
	for _, p := range r.Parameters {
		got = append(got, p.Type+"="+string(p.Value))
	}
	want := `int64="9223372036854775807" string="123.45" string="it's" string="2024-03-04 05:06:07.123456Z" null=null float64=0.1 bool=true`
	if strings.Join(got, " ") != want {
		t.Fatalf("parameters %s", strings.Join(got, " "))
	}
	if !strings.Contains(r.SQL, "CAST($2 AS DECIMAL(5,2))") || !strings.Contains(r.SQL, "CAST($4 AS TIMESTAMPTZ)") {
		t.Fatalf("missing exact casts: %s", r.SQL)
	}
	// Untyped parameters (pgx exec mode, psycopg strings) stay text for the engine to cast.
	var one int64
	if err := c.QueryRow(context.Background(), "SELECT 1 WHERE $1::BIGINT > 0", pgx.QueryExecModeExec, int64(7)).Scan(&one); err != nil || one != 1 {
		t.Fatal(err)
	}
	if p := exec.last().Parameters[0]; p.Type != "string" || string(p.Value) != `"7"` {
		t.Fatalf("untyped %+v", p)
	}
}

func TestAuthenticationFailsClosed(t *testing.T) {
	_, addr := start(t, nil)
	_, err := pgx.Connect(context.Background(), dsn(addr, "sales", "wrong-password-wrong-password-xx", "require"))
	if sqlState(err) != "28P01" {
		t.Fatalf("wrong key: %v", err)
	}
	_, err = pgx.Connect(context.Background(), dsn(addr, "sales", token, "disable"))
	if sqlState(err) != "28000" {
		t.Fatalf("plaintext: %v", err)
	}
}

func TestSourceDenialAndUnsupportedType(t *testing.T) {
	_, addr := start(t, nil)
	c, err := pgx.Connect(context.Background(), dsn(addr, "secret", token, "require"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	_, err = c.Exec(context.Background(), "SELECT 1", pgx.QueryExecModeSimpleProtocol)
	if sqlState(err) != "42501" {
		t.Fatalf("denied source: %v", err)
	}
	c2 := connect(t, addr)
	_, err = c2.Exec(context.Background(), "SELECT listtype", pgx.QueryExecModeSimpleProtocol)
	if sqlState(err) != "0A000" {
		t.Fatalf("list type: %v", err)
	}
}

func TestWritesRejectedAndTransactionsNoop(t *testing.T) {
	exec, addr := start(t, nil)
	c := connect(t, addr)
	for _, sql := range []string{"INSERT INTO t VALUES (1)", "create table x(a int)", "SELECT 1; DROP TABLE sales", "COPY t FROM STDIN",
		"WITH a AS (SELECT 1) DELETE FROM t", "UPDATE t SET a=1", "ATTACH 'x.db'", "SELECT * FROM read_csv('x') ; pragma version"} {
		_, err := c.Exec(context.Background(), sql, pgx.QueryExecModeSimpleProtocol)
		if sqlState(err) != "25006" {
			t.Fatalf("%q: %v", sql, err)
		}
	}
	exec.mu.Lock()
	n := len(exec.requests)
	exec.mu.Unlock()
	if n != 0 {
		t.Fatalf("rejected writes reached the executor %d times", n)
	}
	_, err := c.Exec(context.Background(), "INSERT INTO t VALUES ($1)", 1)
	if sqlState(err) != "25006" {
		t.Fatalf("extended insert: %v", err)
	}
	tx, err := c.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var v string
	if err := tx.QueryRow(context.Background(), "SHOW transaction_read_only").Scan(&v); err != nil || v != "on" {
		t.Fatal(err, v)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(context.Background(), "SET application_name = 'metabase'; SET extra_float_digits = 3"); err != nil {
		t.Fatal(err)
	}
	if err := c.QueryRow(context.Background(), "SHOW application_name").Scan(&v); err != nil || v != "metabase" {
		t.Fatal(err, v)
	}
}

func TestTruncationIsNeverSuccess(t *testing.T) {
	_, addr := start(t, nil)
	c := connect(t, addr)
	rows, _ := c.Query(context.Background(), "SELECT truncated", pgx.QueryExecModeSimpleProtocol)
	for rows.Next() {
	}
	if rows.Err() == nil {
		t.Fatal("truncated stream reported success")
	}
}

func TestCancelRequestAndStatementTimeout(t *testing.T) {
	exec, addr := start(t, nil)
	c := connect(t, addr)
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = c.PgConn().CancelRequest(context.Background())
	}()
	_, err := c.Exec(context.Background(), "SELECT sleep", pgx.QueryExecModeSimpleProtocol)
	if sqlState(err) != "57014" || !strings.Contains(err.Error(), "user request") {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case <-exec.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("executor was not cancelled")
	}
	var one int64
	if err := c.QueryRow(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatal("session unusable after cancel", err)
	}

	_, addr2 := start(t, func(c *Config) { c.StatementTimeout = 200 * time.Millisecond })
	c2 := connect(t, addr2)
	_, err = c2.Exec(context.Background(), "SELECT sleep", pgx.QueryExecModeSimpleProtocol)
	if sqlState(err) != "57014" || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("timeout: %v", err)
	}
}

func TestMaxConnections(t *testing.T) {
	_, addr := start(t, func(c *Config) { c.MaxConnections = 1 })
	_ = connect(t, addr)
	_, err := pgx.Connect(context.Background(), dsn(addr, "sales", token, "require"))
	if sqlState(err) != "53300" {
		t.Fatalf("second connection: %v", err)
	}
}

func TestLexAndClassify(t *testing.T) {
	if got := split("SELECT ';'; SELECT $$a;b$$; -- x;\n SELECT \"a;\" /* ; */"); len(got) != 3 {
		t.Fatalf("split %q", got)
	}
	if _, err := classify("SELECT 'insert' AS \"update\""); err != nil {
		t.Fatal("quoted words must not be write keywords")
	}
	if _, err := classify("select t.a as update, t.b as delete from t"); err != nil {
		t.Fatal("reserved word aliases are names", err)
	}
	if got := rewrite("select 1 as update, 'as delete' AS Insert", "s", nil); got != `select 1 AS "update", 'as delete' AS "insert"` {
		t.Fatalf("alias quoting %q", got)
	}
	for _, sql := range []string{"WITH x AS (DELETE FROM t) SELECT 1", "SELECT 1 AS x; DELETE FROM t", "SELECT 'open"} {
		if _, err := classify(sql); err == nil {
			t.Fatalf("accepted %q", sql)
		}
	}
	sql := rewrite("SELECT c.relname OPERATOR(pg_catalog.~) '^(x)$' COLLATE pg_catalog.default, version(), '$1'", "sales", map[int]string{1: "DATE"})
	if sql != "SELECT c.relname ~ '^(x)$' , 'PostgreSQL 16.0 (Kelvo)', '$1'" {
		t.Fatalf("rewrite %q", sql)
	}
	if cast, ok := decimalCast("-00123.4500"); !ok || cast != "DECIMAL(7,4)" {
		t.Fatal(cast)
	}
}

func TestIntrospectionBurstDoesNotExhaustHandles(t *testing.T) {
	exec, addr := start(t, nil) // httpapi MaxQueries is 64
	c := connect(t, addr)
	ok, limited := 0, 0
	for i := 0; i < 200; i++ {
		var one int64
		err := c.QueryRow(context.Background(), "SELECT 1 FROM pg_catalog.pg_class WHERE "+strings.Repeat("1=1 AND ", i%50)+"true", pgx.QueryExecModeSimpleProtocol).Scan(&one)
		switch {
		case err == nil && one == 1:
			ok++
		case sqlState(err) == "53400":
			limited++
		default:
			t.Fatalf("query %d: %v", i, err)
		}
	}
	exec.mu.Lock()
	executed := len(exec.requests)
	exec.mu.Unlock()
	if executed != catalogBudget || limited == 0 || ok+limited != 200 {
		t.Fatalf("executed=%d ok=%d limited=%d", executed, ok, limited)
	}
	other := connect(t, addr)
	var one int64
	if err := other.QueryRow(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatal("second client starved", err)
	}
}

func TestHandlesAreReleasedAfterEachStatement(t *testing.T) {
	_, addr := start(t, nil) // httpapi MaxQueries is 64
	c := connect(t, addr)
	for i := 0; i < 150; i++ {
		var one int64
		if err := c.QueryRow(context.Background(), "SELECT "+strings.Repeat("1+", i)+"0 AS one", pgx.QueryExecModeSimpleProtocol).Scan(&one); err != nil {
			t.Fatalf("statement %d: %v", i, err)
		}
	}
}

func TestUnauthenticatedPeersDoNotHoldSessionSlots(t *testing.T) {
	_, addr := start(t, func(c *Config) { c.MaxConnections = 1 })
	for i := 0; i < 3; i++ {
		idle, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer idle.Close()
	}
	if _, err := pgx.Connect(context.Background(), dsn(addr, "sales", "wrong-password-wrong-password-xx", "require")); sqlState(err) != "28P01" {
		t.Fatal(err)
	}
	_ = connect(t, addr) // the only slot is still free
}
