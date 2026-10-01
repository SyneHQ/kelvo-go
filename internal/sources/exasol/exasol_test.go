// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exasol

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
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
	"github.com/exasol/exasol-driver-go/pkg/dsn"
	"github.com/gorilla/websocket"
)

type capture struct {
	schema              *arrow.Schema
	records             []arrow.RecordBatch
	schemaErr, writeErr error
}

func (s *capture) Schema(schema *arrow.Schema) error { s.schema = schema; return s.schemaErr }
func (s *capture) Write(record arrow.RecordBatch) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	record.Retain()
	s.records = append(s.records, record)
	return nil
}
func sink(t *testing.T) *capture {
	t.Helper()
	s := &capture{}
	t.Cleanup(func() {
		for _, r := range s.records {
			r.Release()
		}
	})
	return s
}
func config() catalog.Config {
	return catalog.Config{Sources: []catalog.Source{{ID: "warehouse", Type: "exasol", DSNEnv: "KELVO_SOURCE_EXASOL_DSN"}}}
}
func request() query.Request {
	return query.Request{Mode: "native", ConnectionID: "warehouse", SQL: "SELECT * FROM analytics"}
}

var fixtureKeyOnce sync.Once
var fixtureKey *rsa.PrivateKey
var fixtureKeyErr error

func key(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	fixtureKeyOnce.Do(func() { fixtureKey, fixtureKeyErr = rsa.GenerateKey(rand.Reader, 2048) })
	if fixtureKeyErr != nil {
		t.Fatal(fixtureKeyErr)
	}
	return fixtureKey
}
func reply(c *websocket.Conn, data string) {
	_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"ok","responseData":`+data+`}`))
}
func ok(c *websocket.Conn) { _ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"ok"}`)) }

type fixture struct {
	commandsMu sync.Mutex
	commands   []string
	connects   atomic.Int32
}

func (f *fixture) snapshot() string {
	f.commandsMu.Lock()
	defer f.commandsMu.Unlock()
	return strings.Join(f.commands, ",")
}
func setup(t *testing.T, custom func(*websocket.Conn, map[string]any) bool) (*Engine, *fixture, *httptest.Server) {
	t.Helper()
	private := key(t)
	f := &fixture{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.connects.Add(1)
		if r.URL.RawQuery != "" || r.URL.User != nil || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("credentials in handshake")
		}
		c, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			_, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			var in map[string]any
			d := json.NewDecoder(strings.NewReader(string(data)))
			d.UseNumber()
			if err := d.Decode(&in); err != nil {
				t.Error(err)
				return
			}
			command, _ := in["command"].(string)
			if command == "" {
				command = "auth"
			}
			f.commandsMu.Lock()
			f.commands = append(f.commands, command)
			f.commandsMu.Unlock()
			if custom != nil && custom(c, in) {
				continue
			}
			switch command {
			case "login":
				if in["protocolVersion"] != json.Number("2") {
					t.Error("wrong protocol version")
				}
				reply(c, fmt.Sprintf(`{"publicKeyModulus":"%s","publicKeyExponent":"10001"}`, hex.EncodeToString(private.N.Bytes())))
			case "auth":
				if in["username"] != "reader" || in["useCompression"] != false || in["clientOsUsername"] != nil {
					t.Error("wrong or ambient login fields")
				}
				encoded, _ := in["password"].(string)
				encrypted, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil {
					t.Error(err)
				}
				plain, err := rsa.DecryptPKCS1v15(rand.Reader, private, encrypted)
				if err != nil || string(plain) != "fixture;password" {
					t.Error("password was not encrypted/preserved")
				}
				attrs, _ := in["attributes"].(map[string]any)
				if attrs["autocommit"] != false || attrs["timestampUtcEnabled"] != true || attrs["queryTimeout"] == nil {
					t.Error("unsafe login attributes")
				}
				reply(c, `{"sessionId":9007199254740993,"protocolVersion":2}`)
			case "getAttributes":
				_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"ok","attributes":{"autocommit":false,"timestampUtcEnabled":true}}`))
			case "execute":
				attrs, _ := in["attributes"].(map[string]any)
				if attrs["autocommit"] != false {
					t.Error("query or rollback enabled autocommit")
				}
				if in["sqlText"] != "ROLLBACK" {
					t.Error("unexpected execute")
					return
				}
				reply(c, `{"numResults":1,"results":[{"resultType":"rowCount","rowCount":0}]}`)
			case "closeResultSet", "disconnect":
				ok(c)
			case "abortQuery":
				t.Error("unexpected abort")
			default:
				t.Errorf("unexpected command %s", command)
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	host, portText, _ := net.SplitHostPort(strings.TrimPrefix(server.URL, "https://"))
	port, _ := strconv.Atoi(portText)
	raw := (&dsn.DSNConfig{Host: host, Port: port, User: "reader", Password: "fixture;password"}).ToDSN()
	t.Setenv("KELVO_SOURCE_EXASOL_DSN", raw)
	e, err := New(config(), query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e.dialer.TLSClientConfig.RootCAs = server.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	return e, f, server
}

const decimalColumn = `[{"name":"ID","dataType":{"type":"DECIMAL","precision":36,"scale":0}}]`

func resultJSON(columns, data string, total, inMessage int, handle string) string {
	h := ""
	if handle != "" {
		h = `"resultSetHandle":` + handle + `,`
	}
	var cols []any
	_ = json.Unmarshal([]byte(columns), &cols)
	return fmt.Sprintf(`{"numResults":1,"results":[{"resultType":"resultSet","resultSet":{%s"numColumns":%d,"numRows":%d,"numRowsInMessage":%d,"columns":%s,"data":%s}}]}`, h, len(cols), total, inMessage, columns, data)
}
func isQuery(in map[string]any) bool {
	return in["command"] == "execute" && in["sqlText"] != "ROLLBACK"
}

func TestExactValuesPaginationAndRollback(t *testing.T) {
	columns := `[{"name":"I","dataType":{"type":"DECIMAL","precision":36,"scale":0}},{"name":"D","dataType":{"type":"DECIMAL","precision":36,"scale":6}},{"name":"T","dataType":{"type":"TIMESTAMP","withLocalTimeZone":false}},{"name":"TZ","dataType":{"type":"TIMESTAMP","withLocalTimeZone":true}},{"name":"DATE","dataType":{"type":"DATE"}},{"name":"BOOL","dataType":{"type":"BOOLEAN"}},{"name":"F","dataType":{"type":"DOUBLE"}},{"name":"TEXT","dataType":{"type":"VARCHAR","size":100}}]`
	e, f, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
		if isQuery(in) {
			attrs := in["attributes"].(map[string]any)
			if in["sqlText"] != request().SQL || attrs["resultSetMaxRows"] != json.Number("1000001") {
				t.Error("incorrect query request")
			}
			reply(c, resultJSON(columns, `[[9223372036854775807,null],[123456789012345678901234567890.123456,null],["2026-10-01 12:30:00.123456789",null],["2026-10-01 12:30:00.123456",null],["1960-01-02",null],[true,null],[1.25,null],["hello",null]]`, 3, 2, "9007199254740993"))
			return true
		}
		if in["command"] == "fetch" {
			if in["resultSetHandle"] != json.Number("9007199254740993") || in["startPosition"] != json.Number("2") || in["numBytes"] != json.Number("2048000") {
				t.Error("bad fetch position/handle/budget")
			}
			reply(c, `{"numRows":1,"data":[[-9223372036854775808],[1.234567E+2],[null],[null],[null],[false],[2.5],["next"]]}`)
			return true
		}
		return false
	})
	s := sink(t)
	stats, err := e.Execute(context.Background(), request(), s)
	if err != nil || stats.Rows != 3 || stats.Batches != 1 || stats.WireBytes <= 0 || stats.Backend != "exasol" {
		t.Fatalf("stats=%+v err=%v commands=%s", stats, err, f.snapshot())
	}
	if f.snapshot() != "login,auth,getAttributes,execute,fetch,closeResultSet,execute,disconnect" {
		t.Fatal(f.snapshot())
	}
	r := s.records[0]
	integers := r.Column(0).(*array.Decimal128)
	if integers.Value(0).ToString(0) != "9223372036854775807" || !integers.IsNull(1) || integers.Value(2).ToString(0) != "-9223372036854775808" {
		t.Fatal("lost integer precision or NULL")
	}
	decimals := r.Column(1).(*array.Decimal128)
	if decimals.Value(0).ToString(6) != "123456789012345678901234567890.123456" || decimals.Value(2).ToString(6) != "123.456700" {
		t.Fatal("lost decimal precision")
	}
	if r.Column(2).(*array.Timestamp).Value(0).ToTime(arrow.Nanosecond).Nanosecond() != 123456789 || s.schema.Field(2).Type.(*arrow.TimestampType).TimeZone != "" || s.schema.Field(3).Type.(*arrow.TimestampType).TimeZone != "UTC" {
		t.Fatal("timestamp precision/timezone changed")
	}
	if r.Column(4).(*array.Date32).Value(0).ToTime().Format("2006-01-02") != "1960-01-02" {
		t.Fatal("date changed")
	}
}

func TestBatchingAndEmptyResult(t *testing.T) {
	for _, size := range []int{0, 2050} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			position := 0
			e, f, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
				if isQuery(in) {
					reply(c, resultJSON(decimalColumn, `[]`, size, 0, "0"))
					return true
				}
				if in["command"] == "fetch" {
					n := min(700, size-position)
					position += n
					reply(c, fmt.Sprintf(`{"numRows":%d,"data":[[%s]]}`, n, strings.TrimSuffix(strings.Repeat("1,", n), ",")))
					return true
				}
				return false
			})
			s := sink(t)
			stats, err := e.Execute(context.Background(), request(), s)
			if err != nil || stats.Rows != int64(size) || stats.Batches != int64((size+1023)/1024) || s.schema == nil || !strings.Contains(f.snapshot(), "closeResultSet,execute,disconnect") {
				t.Fatalf("stats=%+v err=%v", stats, err)
			}
		})
	}
}

func TestResultFailuresCloseAndRollback(t *testing.T) {
	cases := []struct{ name, payload, code string }{
		{"row-limit", resultJSON(decimalColumn, `[[1,2]]`, 2, 2, "0"), "RESOURCE_EXHAUSTED"},
		{"unknown-type", resultJSON(`[{"name":"G","dataType":{"type":"GEOMETRY"}}]`, `[["POINT(0 0)"]]`, 1, 1, "0"), "UNSUPPORTED"},
		{"decimal-metadata", resultJSON(`[{"name":"D","dataType":{"type":"DECIMAL"}}]`, `[[1]]`, 1, 1, "0"), "UNSUPPORTED"},
		{"decimal-overflow", resultJSON(decimalColumn, `[[1234567890123456789012345678901234567]]`, 1, 1, "0"), "UNSUPPORTED"},
		{"decimal-scale", resultJSON(decimalColumn, `[[1.25]]`, 1, 1, "0"), "UNSUPPORTED"},
		{"boolean-number", resultJSON(`[{"name":"B","dataType":{"type":"BOOLEAN"}}]`, `[[1]]`, 1, 1, "0"), "UNSUPPORTED"},
		{"boolean-string", resultJSON(`[{"name":"B","dataType":{"type":"BOOLEAN"}}]`, `[["true"]]`, 1, 1, "0"), "UNSUPPORTED"},
		{"decimal-exponent", resultJSON(decimalColumn, `[[1e-999]]`, 1, 1, "0"), "UNSUPPORTED"},
		{"column-length", resultJSON(decimalColumn, `[[1,2]]`, 1, 1, "0"), "QUERY_FAILED"},
		{"column-count", resultJSON(decimalColumn, `[[1],[2]]`, 1, 1, "0"), "QUERY_FAILED"},
		{"timestamp-precision", resultJSON(`[{"name":"T","dataType":{"type":"TIMESTAMP"}}]`, `[["2026-10-01 00:00:00.1234567891"]]`, 1, 1, "0"), "UNSUPPORTED"},
		{"missing-cursor", resultJSON(decimalColumn, `[]`, 1, 0, ""), "QUERY_FAILED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, f, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
				if isQuery(in) {
					reply(c, tc.payload)
					return true
				}
				return false
			})
			e.limits.MaxRows = 1
			_, err := e.Execute(context.Background(), request(), sink(t))
			if err == nil || query.PublicError(err).Code != tc.code || !strings.HasSuffix(f.snapshot(), "execute,disconnect") {
				t.Fatalf("err=%v commands=%s", err, f.snapshot())
			}
			if tc.name != "missing-cursor" && !strings.Contains(f.snapshot(), "closeResultSet") {
				t.Fatal("cursor not closed")
			}
		})
	}
}

func TestFetchFailureSchemaAndBudgets(t *testing.T) {
	for _, name := range []string{"zero-page", "extra-rows", "changed-schema", "changed-handle", "provider", "pages", "frame-bytes", "total-wire", "bytes", "sink-schema", "sink-write"} {
		t.Run(name, func(t *testing.T) {
			e, f, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
				if isQuery(in) {
					reply(c, resultJSON(decimalColumn, `[]`, 3000, 0, "4"))
					return true
				}
				if in["command"] != "fetch" {
					return false
				}
				switch name {
				case "zero-page":
					reply(c, `{"numRows":0,"data":[[]]}`)
				case "extra-rows":
					reply(c, `{"numRows":3001,"data":[[1]]}`)
				case "changed-schema":
					reply(c, `{"numRows":1,"columns":[{"name":"X","dataType":{"type":"VARCHAR"}}],"data":[["x"]]}`)
				case "changed-handle":
					reply(c, `{"numRows":1,"resultSetHandle":5,"data":[[1]]}`)
				case "provider":
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"error","exception":{"text":"private-secret","sqlCode":"42000"}}`))
				case "frame-bytes":
					_ = c.WriteMessage(websocket.TextMessage, []byte(strings.Repeat(" ", 5000)))
				case "total-wire":
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"ok","responseData":{"numRows":1,"data":[[1]]}}`+strings.Repeat(" ", 4000)))
				case "sink-write", "bytes":
					reply(c, fmt.Sprintf(`{"numRows":1500,"data":[[%s]]}`, strings.TrimSuffix(strings.Repeat("1,", 1500), ",")))
				default:
					reply(c, `{"numRows":1,"data":[[1]]}`)
				}
				return true
			})
			s := sink(t)
			switch name {
			case "pages":
				e.maxPages = 2
			case "frame-bytes":
				e.responseLimit = 4096
			case "total-wire":
				e.responseLimit = 8192
				e.limits.MaxBytes = 4096
			case "bytes":
				e.limits.MaxBytes = 1024
			case "sink-schema":
				s.schemaErr = errors.New("sink failed")
			case "sink-write":
				s.writeErr = errors.New("sink failed")
			}
			_, err := e.Execute(context.Background(), request(), s)
			if err == nil || strings.Contains(err.Error(), "private-secret") {
				t.Fatalf("err=%v", err)
			}
			if name != "frame-bytes" && name != "total-wire" && !strings.Contains(f.snapshot(), "closeResultSet,execute,disconnect") {
				t.Fatalf("missing cleanup: %s", f.snapshot())
			}
		})
	}
}

func TestCancellationAbortsDrainsAndRollsBack(t *testing.T) {
	for _, name := range []string{"cancel", "timeout"} {
		t.Run(name, func(t *testing.T) {
			started := make(chan struct{})
			e, f, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
				if isQuery(in) {
					reply(c, resultJSON(decimalColumn, `[]`, 1, 0, "4"))
					return true
				}
				if in["command"] == "fetch" {
					close(started)
					return true
				}
				if in["command"] == "abortQuery" {
					_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"error","exception":{"text":"query aborted"}}`))
					return true
				}
				return false
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := context.Canceled
			if name == "timeout" {
				e.limits.Timeout = 80 * time.Millisecond
				want = context.DeadlineExceeded
			} else {
				go func() { <-started; cancel() }()
			}
			_, err := e.Execute(ctx, request(), sink(t))
			if !errors.Is(err, want) || !strings.Contains(f.snapshot(), "fetch,abortQuery,closeResultSet,execute,disconnect") {
				t.Fatalf("err=%v commands=%s", err, f.snapshot())
			}
		})
	}
}

func TestRollbackFailureFailsQuery(t *testing.T) {
	e, f, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
		if isQuery(in) {
			reply(c, resultJSON(decimalColumn, `[[1]]`, 1, 1, ""))
			return true
		}
		if in["sqlText"] == "ROLLBACK" {
			_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"error","exception":{"text":"private-secret"}}`))
			return true
		}
		return false
	})
	s := sink(t)
	_, err := e.Execute(context.Background(), request(), s)
	if err == nil || len(s.records) != 0 || strings.Contains(err.Error(), "private-secret") || !strings.HasSuffix(f.snapshot(), "execute,disconnect") {
		t.Fatalf("err=%v commands=%s", err, f.snapshot())
	}
}

func TestAuthenticationAttributesAndProtocolFailures(t *testing.T) {
	for _, name := range []string{"key", "auth", "attributes", "version", "binary", "trailing-json"} {
		t.Run(name, func(t *testing.T) {
			e, _, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
				if in["command"] == "login" && name == "key" {
					reply(c, `{"publicKeyModulus":"bad","publicKeyExponent":"10001"}`)
					return true
				}
				if in["username"] != nil {
					switch name {
					case "auth":
						_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"error","exception":{"text":"private-secret"}}`))
						return true
					case "version":
						reply(c, `{"protocolVersion":99}`)
						return true
					}
				}
				if in["command"] == "getAttributes" {
					switch name {
					case "attributes":
						_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"ok","attributes":{"autocommit":true,"timestampUtcEnabled":true}}`))
						return true
					case "binary":
						_ = c.WriteMessage(websocket.BinaryMessage, []byte(`{}`))
						return true
					case "trailing-json":
						_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"ok"} {}`))
						return true
					}
				}
				return false
			})
			if _, err := e.Execute(context.Background(), request(), sink(t)); err == nil || strings.Contains(err.Error(), "private-secret") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestInvalidRequestsNeverConnect(t *testing.T) {
	e, f, _ := setup(t, nil)
	for _, req := range []query.Request{
		{Mode: "native", ConnectionID: "other", SQL: "SELECT 1"},
		{Mode: "federated", SQL: "SELECT 1"},
		{Mode: "native", ConnectionID: "warehouse", SQL: "DELETE FROM t"},
		{Mode: "native", ConnectionID: "warehouse", SQL: "SELECT 1; DROP TABLE t"},
		{Mode: "native", ConnectionID: "warehouse", SQL: "SELECT ?", Parameters: []query.Parameter{{Type: "int64", Value: json.RawMessage(`1`)}}},
	} {
		if _, err := e.Execute(context.Background(), req, sink(t)); err == nil {
			t.Error("accepted unsupported request")
		}
	}
	if f.connects.Load() != 0 {
		t.Fatal("invalid request reached server")
	}
}

func TestConstructorValidatesDSNAndTLS(t *testing.T) {
	e, _, _ := setup(t, nil)
	if e.dialer.Proxy != nil || e.dialer.TLSClientConfig.MinVersion < tls.VersionTLS12 || e.dialer.TLSClientConfig.InsecureSkipVerify || e.dialer.EnableCompression {
		t.Fatal("unsafe transport")
	}
	valid := "exa:example.com:8563;user=reader;password=test"
	for _, extra := range []string{"encryption=0", "validateservercertificate=0", "certificatefingerprint=deadbeef", "compression=1", "accesstoken=secret", "refreshtoken=secret", "urlpath=/x?secret=yes", "resultsetmaxrows=1", "unknown=x", "fetchsize=-1", "fetchsize=9223372036854775807", "user=", "password="} {
		t.Setenv("KELVO_SOURCE_EXASOL_DSN", valid+";"+extra)
		if _, err := New(config(), query.DefaultLimits()); err == nil {
			t.Errorf("accepted %s", extra)
		}
	}
	for _, raw := range []string{"", "user:pass@host:8563", "exa:example.com:0;user=u;password=p", "exa:user@host:8563;user=u;password=p", "exa:host1,host2:8563;user=u;password=p", "exa:host1..3:8563;user=u;password=p"} {
		t.Setenv("KELVO_SOURCE_EXASOL_DSN", raw)
		if _, err := New(config(), query.DefaultLimits()); err == nil {
			t.Error("accepted invalid DSN")
		}
	}
	t.Setenv("KELVO_SOURCE_EXASOL_DSN", valid)
	if _, err := New(config(), query.DefaultLimits()); err != nil {
		t.Fatal("verified TLS defaults rejected", err)
	}
}

func TestTLSVerificationAndNoRedirect(t *testing.T) {
	e, _, _ := setup(t, nil)
	e.dialer.TLSClientConfig.RootCAs = nil
	if _, err := e.Execute(context.Background(), request(), sink(t)); err == nil {
		t.Fatal("untrusted certificate accepted")
	}
	var forwarded atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded.Add(1) }))
	defer target.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	host, portText, _ := net.SplitHostPort(strings.TrimPrefix(redirect.URL, "https://"))
	e.config.Host = host
	e.config.Port, _ = strconv.Atoi(portText)
	e.dialer.TLSClientConfig.RootCAs = redirect.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	if _, err := e.Execute(context.Background(), request(), sink(t)); err == nil || forwarded.Load() != 0 {
		t.Fatal("redirect followed")
	}
}

func TestCancelInitialExecutionStillRollsBack(t *testing.T) {
	started := make(chan struct{})
	e, f, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
		if isQuery(in) {
			close(started)
			return true
		}
		if in["command"] == "abortQuery" {
			_ = c.WriteMessage(websocket.TextMessage, []byte(`{"status":"error","exception":{"text":"aborted"}}`))
			return true
		}
		return false
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { <-started; cancel() }()
	_, err := e.Execute(ctx, request(), sink(t))
	if !errors.Is(err, context.Canceled) || !strings.HasSuffix(f.snapshot(), "execute,abortQuery,execute,disconnect") {
		t.Fatalf("err=%v commands=%s", err, f.snapshot())
	}
}

func TestUnresponsiveAbortAndRollbackAreBounded(t *testing.T) {
	for _, phase := range []string{"abort", "rollback"} {
		t.Run(phase, func(t *testing.T) {
			e, f, _ := setup(t, func(c *websocket.Conn, in map[string]any) bool {
				if isQuery(in) {
					if phase == "abort" {
						return true
					}
					reply(c, resultJSON(decimalColumn, `[[1]]`, 1, 1, ""))
					return true
				}
				if in["command"] == "abortQuery" || phase == "rollback" && in["sqlText"] == "ROLLBACK" {
					return true
				}
				return false
			})
			if phase == "abort" {
				e.limits.Timeout = 40 * time.Millisecond
			}
			started := time.Now()
			_, err := e.Execute(context.Background(), request(), sink(t))
			if err == nil || time.Since(started) > 3*time.Second {
				t.Fatalf("unbounded %s cleanup: duration=%s err=%v", phase, time.Since(started), err)
			}
			if phase == "abort" && (!errors.Is(err, context.DeadlineExceeded) || !strings.HasSuffix(f.snapshot(), "execute,abortQuery")) {
				t.Fatalf("err=%v commands=%s", err, f.snapshot())
			}
		})
	}
}
