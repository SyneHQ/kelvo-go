// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package ignite

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	return catalog.Config{Sources: []catalog.Source{{ID: "grid", Type: "ignite", URLEnv: "KELVO_SOURCE_IGNITE_URL", UsernameEnv: "KELVO_SOURCE_IGNITE_USER", PasswordEnv: "KELVO_SOURCE_IGNITE_PASSWORD", Options: map[string]string{"cache_name": "fixture cache"}}}}
}

func request() query.Request {
	return query.Request{Mode: "native", ConnectionID: "grid", SQL: "SELECT * FROM Person"}
}

func setup(t *testing.T, handler func(http.ResponseWriter, *http.Request, url.Values)) (*Engine, *httptest.Server) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/ignite" || r.URL.RawQuery != "" || r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("invalid protocol or credentials outside POST body")
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.PostForm.Get("ignite.login") != "test+user@example" || r.PostForm.Get("ignite.password") != "fixture&p=ss+word" {
			t.Error("credentials were not explicitly form-encoded")
		}
		if r.PostForm.Has("sessionToken") {
			t.Error("unexpected provider session token reuse")
		}
		w.Header().Set("Content-Type", "application/json")
		handler(w, r, r.PostForm)
	}))
	t.Cleanup(server.Close)
	t.Setenv("KELVO_SOURCE_IGNITE_URL", server.URL)
	t.Setenv("KELVO_SOURCE_IGNITE_USER", "test+user@example")
	t.Setenv("KELVO_SOURCE_IGNITE_PASSWORD", "fixture&p=ss+word")
	e, err := New(config(), query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	// Keep all production transport restrictions; trust only this TLS fixture.
	e.client.http.Transport.(*http.Transport).TLSClientConfig.RootCAs = server.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	t.Cleanup(func() { e.Close() })
	return e, server
}

const longColumn = `[{"fieldName":"ID","fieldTypeName":"java.lang.Long","schemaName":"PUBLIC","typeName":"PERSON"}]`

func pageJSON(columns, rows string, last bool, id int64) string {
	return fmt.Sprintf(`{"successStatus":0,"error":null,"sessionToken":"provider-token-must-not-be-used","response":{"fieldsMetadata":%s,"items":%s,"last":%t,"queryId":%d}}`, columns, rows, last, id)
}

func TestPaginationAndExactTypes(t *testing.T) {
	columns := `[` +
		`{"fieldName":"ID","fieldTypeName":"java.lang.Long"},` +
		`{"fieldName":"AMOUNT","fieldTypeName":"java.math.BigDecimal"},` +
		`{"fieldName":"CREATED","fieldTypeName":"java.sql.Timestamp"},` +
		`{"fieldName":"BIRTH","fieldTypeName":"java.sql.Date"},` +
		`{"fieldName":"DATA","fieldTypeName":"[B"},` +
		`{"fieldName":"TINY","fieldTypeName":"java.lang.Byte"},` +
		`{"fieldName":"SHORT","fieldTypeName":"java.lang.Short"},` +
		`{"fieldName":"INT","fieldTypeName":"java.lang.Integer"},` +
		`{"fieldName":"FLOAT","fieldTypeName":"java.lang.Float"},` +
		`{"fieldName":"DOUBLE","fieldTypeName":"java.lang.Double"},` +
		`{"fieldName":"BOOL","fieldTypeName":"java.lang.Boolean"},` +
		`{"fieldName":"NAME","fieldTypeName":"java.lang.String"},` +
		`{"fieldName":"TIME","fieldTypeName":"java.sql.Time"},` +
		`{"fieldName":"UUID","fieldTypeName":"java.util.UUID"}]`
	var calls atomic.Int32
	e, _ := setup(t, func(w http.ResponseWriter, r *http.Request, form url.Values) {
		switch calls.Add(1) {
		case 1:
			if form.Get("cmd") != "qryfldexe" || form.Get("qry") != request().SQL || form.Get("cacheName") != "fixture cache" || form.Get("pageSize") != "1000" {
				t.Error("incorrect execute form")
			}
			fmt.Fprint(w, pageJSON(columns, `[[9223372036854775807,123456789012345678901234567890.123400,"2026-10-01 12:30:00.123456789","1960-01-02","AAH/",-128,-32768,-2147483648,1.25,2.5,true,"a + & = b","23:59:59","550e8400-e29b-41d4-a716-446655440000"],[null,null,null,null,null,null,null,null,null,null,null,null,null,null]]`, false, 9007199254740993))
		case 2:
			if form.Get("cmd") != "qryfetch" || form.Get("qryId") != "9007199254740993" || form.Has("qry") {
				t.Error("incorrect fetch form or rounded cursor")
			}
			fmt.Fprint(w, pageJSON(`[]`, `[[-9223372036854775808,1E-100,null,null,null,127,32767,2147483647,null,null,false,"next",null,null]]`, true, 9007199254740993))
		default:
			t.Error("unexpected request after final page")
		}
	})
	s := sink(t)
	stats, err := e.Execute(context.Background(), request(), s)
	if err != nil || stats.Rows != 3 || stats.Batches != 1 || stats.Bytes <= 0 || stats.WireBytes <= 0 || stats.Backend != "ignite" || calls.Load() != 2 {
		t.Fatalf("stats=%+v calls=%d err=%v", stats, calls.Load(), err)
	}
	r := s.records[0]
	ids := r.Column(0).(*array.Int64)
	if ids.Value(0) != 9223372036854775807 || !ids.IsNull(1) || ids.Value(2) != -9223372036854775808 {
		t.Fatal("integer or NULL loss")
	}
	decimals := r.Column(1).(*array.String)
	if decimals.Value(0) != "123456789012345678901234567890.123400" || decimals.Value(2) != "1E-100" || !decimals.IsNull(1) {
		t.Fatal("decimal loss")
	}
	if logical, _ := s.schema.Field(1).Metadata.GetValue("logical_type"); logical != "decimal" {
		t.Fatal("missing decimal semantics")
	}
	if stamp := r.Column(2).(*array.Timestamp).Value(0).ToTime(arrow.Nanosecond); stamp.Nanosecond() != 123456789 || s.schema.Field(2).Type.(*arrow.TimestampType).TimeZone != "" {
		t.Fatal("timestamp lost precision or invented timezone")
	}
	if date := r.Column(3).(*array.Date32).Value(0).ToTime(); date.Format("2006-01-02") != "1960-01-02" {
		t.Fatal("date loss")
	}
	if data := r.Column(4).(*array.Binary).Value(0); string(data) != "\x00\x01\xff" {
		t.Fatal("binary loss")
	}
	if s.schema.Field(5).Type.ID() != arrow.INT8 || s.schema.Field(6).Type.ID() != arrow.INT16 || s.schema.Field(7).Type.ID() != arrow.INT32 || s.schema.Field(8).Type.ID() != arrow.FLOAT32 {
		t.Fatal("integer or float width loss")
	}
}

func TestUUIDTextWithBinaryMetadataIsRefused(t *testing.T) {
	// Observed with stock Ignite 2.17 SQL CAST(... AS UUID). The metadata
	// describes byte[] but the JSON value is a UUID string, not base64.
	e, _ := setup(t, func(w http.ResponseWriter, r *http.Request, form url.Values) {
		if form.Get("cmd") == "qrycls" {
			fmt.Fprint(w, `{"successStatus":0,"error":null,"response":true}`)
			return
		}
		fmt.Fprint(w, pageJSON(`[{"fieldName":"IDENT","fieldTypeName":"[B"}]`, `[["550e8400-e29b-41d4-a716-446655440000"]]`, true, 0))
	})
	s := sink(t)
	_, err := e.Execute(context.Background(), request(), s)
	if err == nil || query.PublicError(err).Code != "UNSUPPORTED" || len(s.records) != 0 || s.schema.Field(0).Type.ID() != arrow.BINARY {
		t.Fatalf("UUID/binary ambiguity silently accepted: %v", err)
	}
}

func TestEmptyAndAllNullDecimalKeepSchema(t *testing.T) {
	for _, rows := range []string{`[]`, `[[null]]`} {
		t.Run(rows, func(t *testing.T) {
			e, _ := setup(t, func(w http.ResponseWriter, r *http.Request, form url.Values) {
				fmt.Fprint(w, pageJSON(`[{"fieldName":"D","fieldTypeName":"java.math.BigDecimal"}]`, rows, true, 0))
			})
			s := sink(t)
			if _, err := e.Execute(context.Background(), request(), s); err != nil || s.schema.Field(0).Type.ID() != arrow.STRING {
				t.Fatalf("schema=%v err=%v", s.schema, err)
			}
		})
	}
}

func TestRequiresCacheName(t *testing.T) {
	c := config()
	c.Sources[0].Options = nil
	if _, err := New(c, query.DefaultLimits()); err == nil {
		t.Fatal("accepted missing cache_name required by qryfldexe")
	}
}

func TestFailureClosesCursorIncludingZero(t *testing.T) {
	cases := []struct{ name, payload, code string }{
		{"provider", `{"successStatus":1,"error":"private-secret","response":{"queryId":0}}`, "QUERY_FAILED"},
		{"missing-status", `{"error":null,"response":{"queryId":0}}`, "QUERY_FAILED"},
		{"missing-last", `{"successStatus":0,"error":null,"response":{"queryId":0,"items":[]}}`, "QUERY_FAILED"},
		{"unsupported-type", pageJSON(`[{"fieldName":"X","fieldTypeName":"java.lang.Object"}]`, `[[{}]]`, false, 0), "UNSUPPORTED"},
		{"overflow", pageJSON(longColumn, `[[9223372036854775808]]`, false, 0), "UNSUPPORTED"},
		{"float-for-integer", pageJSON(longColumn, `[[1.5]]`, false, 0), "UNSUPPORTED"},
		{"wrong-width", pageJSON(longColumn, `[[1,2]]`, false, 0), "QUERY_FAILED"},
		{"empty-page", pageJSON(longColumn, `[]`, false, 0), "QUERY_FAILED"},
		{"bad-decimal", pageJSON(`[{"fieldName":"D","fieldTypeName":"java.math.BigDecimal"}]`, `[["NaN"]]`, false, 0), "UNSUPPORTED"},
		{"bad-timestamp", pageJSON(`[{"fieldName":"T","fieldTypeName":"java.sql.Timestamp"}]`, `[["2026-10-01 00:00:00.1234567891"]]`, false, 0), "UNSUPPORTED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var closed atomic.Bool
			e, _ := setup(t, func(w http.ResponseWriter, r *http.Request, form url.Values) {
				if form.Get("cmd") == "qrycls" {
					if form.Get("qryId") != "0" {
						t.Error("zero cursor was lost")
					}
					closed.Store(true)
					fmt.Fprint(w, `{"successStatus":0,"error":null,"response":true}`)
					return
				}
				fmt.Fprint(w, tc.payload)
			})
			_, err := e.Execute(context.Background(), request(), sink(t))
			if err == nil || query.PublicError(err).Code != tc.code || strings.Contains(err.Error(), "private-secret") || !closed.Load() {
				t.Fatalf("err=%v close=%v", err, closed.Load())
			}
		})
	}
}

func TestSchemaAndCursorCannotChangeBetweenPages(t *testing.T) {
	for _, tc := range []struct {
		name, columns string
		id            int64
	}{
		{"type", `[{"fieldName":"ID","fieldTypeName":"java.lang.Integer"}]`, 12},
		{"name", `[{"fieldName":"OTHER","fieldTypeName":"java.lang.Long"}]`, 12},
		{"cursor", `[]`, 13},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var closed atomic.Bool
			e, _ := setup(t, func(w http.ResponseWriter, r *http.Request, form url.Values) {
				switch form.Get("cmd") {
				case "qryfldexe":
					fmt.Fprint(w, pageJSON(longColumn, `[[1]]`, false, 12))
				case "qryfetch":
					fmt.Fprint(w, pageJSON(tc.columns, `[[2]]`, false, tc.id))
				case "qrycls":
					if form.Get("qryId") != "12" {
						t.Error("changed cursor was trusted")
					}
					closed.Store(true)
					fmt.Fprint(w, `{"successStatus":0,"error":null,"response":true}`)
				}
			})
			if _, err := e.Execute(context.Background(), request(), sink(t)); err == nil || !closed.Load() {
				t.Fatalf("err=%v close=%v", err, closed.Load())
			}
		})
	}
}

func TestLimitsAndSinkErrorsCloseCursor(t *testing.T) {
	for _, name := range []string{"rows", "bytes", "page-size", "pages", "response-bytes", "total-wire", "sink-schema", "sink-write"} {
		t.Run(name, func(t *testing.T) {
			var closed atomic.Bool
			rows, columns := `[[1],[2]]`, longColumn
			if name == "bytes" || name == "total-wire" {
				columns = `[{"fieldName":"X","fieldTypeName":"java.lang.String"}]`
				rows = `[["` + strings.Repeat("x", 1200) + `"]]`
			}
			if name == "sink-write" {
				rows = "[" + strings.TrimSuffix(strings.Repeat("[1],", 1000), ",") + "]"
			}
			e, _ := setup(t, func(w http.ResponseWriter, r *http.Request, form url.Values) {
				if form.Get("cmd") == "qrycls" {
					closed.Store(true)
					fmt.Fprint(w, `{"successStatus":0,"error":null,"response":true}`)
					return
				}
				if name == "response-bytes" && form.Get("cmd") == "qryfetch" {
					fmt.Fprint(w, strings.Repeat("x", 2048))
					return
				}
				payload := pageJSON(columns, rows, false, 44)
				if name == "total-wire" {
					payload += strings.Repeat(" ", 3000)
				}
				fmt.Fprint(w, payload)
			})
			s := sink(t)
			switch name {
			case "rows":
				e.limits.MaxRows = 1
			case "bytes":
				e.limits.MaxBytes = 1024
			case "page-size":
				e.limits.MaxRows = 1
				rows = `[[1],[2],[3]]`
			case "pages":
				e.maxPages = 2
			case "response-bytes":
				e.client.responseLimit = 1024
			case "total-wire":
				e.limits.MaxBytes = 4096
				e.client.responseLimit = 8192
				rows = `[["x"]]`
			case "sink-schema":
				s.schemaErr = errors.New("sink failed")
			case "sink-write":
				s.writeErr = errors.New("sink failed")
			}
			_, err := e.Execute(context.Background(), request(), s)
			if err == nil || !closed.Load() {
				t.Fatalf("err=%v close=%v", err, closed.Load())
			}
			if !strings.HasPrefix(name, "sink-") && query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
				t.Fatalf("wrong limit error: %v", err)
			}
		})
	}
}

func TestCancelOrTimeoutFetchClosesOnIndependentContext(t *testing.T) {
	for _, name := range []string{"cancel", "timeout"} {
		t.Run(name, func(t *testing.T) {
			var closed atomic.Bool
			fetchStarted := make(chan struct{})
			e, _ := setup(t, func(w http.ResponseWriter, r *http.Request, form url.Values) {
				switch form.Get("cmd") {
				case "qryfldexe":
					fmt.Fprint(w, pageJSON(longColumn, `[[1]]`, false, 4))
				case "qryfetch":
					close(fetchStarted)
					select {
					case <-r.Context().Done():
					case <-time.After(time.Second):
					}
				case "qrycls":
					if r.Context().Err() != nil {
						t.Error("cleanup inherited cancellation")
					}
					closed.Store(true)
					fmt.Fprint(w, `{"successStatus":0,"error":null,"response":true}`)
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := context.Canceled
			if name == "timeout" {
				e.limits.Timeout = 60 * time.Millisecond
				want = context.DeadlineExceeded
			} else {
				go func() { <-fetchStarted; cancel() }()
			}
			_, err := e.Execute(ctx, request(), sink(t))
			if !errors.Is(err, want) || !closed.Load() {
				t.Fatalf("err=%v close=%v", err, closed.Load())
			}
		})
	}
}

func TestInvalidRequestsDoNotReachServer(t *testing.T) {
	var calls atomic.Int32
	e, _ := setup(t, func(w http.ResponseWriter, r *http.Request, form url.Values) { calls.Add(1) })
	for _, r := range []query.Request{
		{Mode: "native", ConnectionID: "other", SQL: "SELECT 1"},
		{Mode: "federated", Sources: []string{"grid"}, SQL: "SELECT 1"},
		{Mode: "native", ConnectionID: "grid", SQL: "DELETE FROM Person"},
		{Mode: "native", ConnectionID: "grid", SQL: "SELECT 1; DROP TABLE Person"},
		{Mode: "native", ConnectionID: "grid", SQL: "SELECT ?", Parameters: []query.Parameter{{Type: "string", Value: json.RawMessage(`"private-value"`)}}},
	} {
		if _, err := e.Execute(context.Background(), r, sink(t)); err == nil {
			t.Errorf("accepted %+v", r)
		}
	}
	if _, err := e.Execute(context.Background(), request(), nil); err == nil {
		t.Error("accepted nil sink")
	}
	if calls.Load() != 0 {
		t.Fatal("rejected query reached server")
	}
}

func TestTransportRejectsRedirectsTLSAndMalformedResponses(t *testing.T) {
	var forwarded atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded.Add(1) }))
	defer target.Close()
	for _, name := range []string{"redirect", "tls", "bad-json", "trailing-json", "invalid-utf8", "http-error", "gzip", "null"} {
		t.Run(name, func(t *testing.T) {
			e, _ := setup(t, func(w http.ResponseWriter, r *http.Request, form url.Values) {
				switch name {
				case "redirect":
					http.Redirect(w, r, target.URL+"/ignite", http.StatusTemporaryRedirect)
				case "http-error":
					http.Error(w, "private-secret", 500)
				case "gzip":
					w.Header().Set("Content-Encoding", "gzip")
					fmt.Fprint(w, "private-secret")
				case "trailing-json":
					fmt.Fprint(w, pageJSON(longColumn, `[[1]]`, true, 0)+" {}")
				case "invalid-utf8":
					w.Write([]byte{'"', 0xff, '"'})
				case "null":
					fmt.Fprint(w, "null")
				default:
					fmt.Fprint(w, "private-secret")
				}
			})
			if name == "tls" {
				e.client.http.Transport.(*http.Transport).TLSClientConfig.RootCAs = nil
			}
			if _, err := e.Execute(context.Background(), request(), sink(t)); err == nil || strings.Contains(err.Error(), "private-secret") {
				t.Fatalf("err=%v", err)
			}
		})
	}
	if forwarded.Load() != 0 {
		t.Fatal("redirect leaked credentials")
	}
}

func TestConfigurationAndSecureTransport(t *testing.T) {
	e, _ := setup(t, func(http.ResponseWriter, *http.Request, url.Values) {})
	transport := e.client.http.Transport.(*http.Transport)
	if transport.Proxy != nil || transport.TLSClientConfig.InsecureSkipVerify || transport.TLSClientConfig.MinVersion < tls.VersionTLS12 || e.client.http.Jar != nil {
		t.Fatal("insecure or ambient transport configuration")
	}
	for _, origin := range []string{"http://example.com", "https://user:pass@example.com", "https://example.com/path", "https://example.com?secret=x", "https://example.com#secret", "https://example.com?", "https://example.com/%2f", ""} {
		t.Setenv("KELVO_SOURCE_IGNITE_URL", origin)
		if _, err := New(config(), query.DefaultLimits()); err == nil {
			t.Errorf("accepted unsafe origin %q", origin)
		}
	}
	t.Setenv("KELVO_SOURCE_IGNITE_URL", "https://example.com")
	for _, field := range []string{"username", "password", "url", "token", "dsn", "adapter", "option", "missing-source", "duplicate-source"} {
		c := config()
		switch field {
		case "username":
			c.Sources[0].UsernameEnv = ""
		case "password":
			c.Sources[0].PasswordEnv = ""
		case "url":
			c.Sources[0].URLEnv = ""
		case "token":
			c.Sources[0].TokenEnv = "KELVO_SOURCE_TOKEN"
		case "dsn":
			c.Sources[0].DSNEnv = "KELVO_SOURCE_DSN"
		case "adapter":
			c.Sources[0].Adapter = "dbapi"
		case "option":
			c.Sources[0].Options["insecure"] = "true"
		case "missing-source":
			c.Sources = nil
		case "duplicate-source":
			c.Sources = append(c.Sources, c.Sources[0])
		}
		if _, err := New(c, query.DefaultLimits()); err == nil {
			t.Errorf("accepted invalid %s config", field)
		}
	}
	t.Setenv("KELVO_SOURCE_IGNITE_PASSWORD", "")
	if _, err := New(config(), query.DefaultLimits()); err == nil {
		t.Fatal("accepted missing password")
	}
}
