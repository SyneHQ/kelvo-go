package dbapi

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type sink struct {
	docs    [][]byte
	schemas int
}

func TestGatewayBatchingAndLateInvalidObject(t *testing.T) {
	for _, bad := range []bool{false, true} {
		var b strings.Builder
		b.WriteString(`{"results":[`)
		for i := 0; i < 2049; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			if bad && i == 2048 {
				b.WriteString(`null`)
			} else {
				b.WriteString(`{"n":9007199254740993}`)
			}
		}
		b.WriteString(`],"rowCount":2049}`)
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(b.String())) }))
		t.Setenv("URL", srv.URL)
		t.Setenv("KEY", "key")
		e, err := New(catalog.Config{Sources: []catalog.Source{source()}}, query.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		e.http = srv.Client()
		got := &sink{}
		stats, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "x", SQL: "SELECT 1"}, got)
		e.Close()
		srv.Close()
		if bad {
			if err == nil || got.schemas != 0 || len(got.docs) != 0 {
				t.Fatal("invalid late object delivered partial response")
			}
		} else if err != nil || stats.Batches != 3 || got.schemas != 1 || len(got.docs) != 2049 {
			t.Fatalf("batch contract stats=%+v schemas=%d docs=%d err=%v", stats, got.schemas, len(got.docs), err)
		}
	}
}

func TestGatewayLimitsCancellationAndSourceBinding(t *testing.T) {
	for _, scenario := range []string{"bytes", "encoding", "deadline", "substitution"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body any
				json.NewDecoder(r.Body).Decode(&body)
				calls.Add(1)
				switch scenario {
				case "bytes":
					w.Write([]byte(strings.Repeat("x", 2049)))
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
					w.Write([]byte("bad compressed data"))
				case "deadline":
					select {
					case <-r.Context().Done():
					case <-time.After(time.Second):
					}
				default:
					w.Write([]byte(`{"results":[],"rowCount":0}`))
				}
			}))
			defer srv.Close()
			t.Setenv("URL", srv.URL)
			t.Setenv("KEY", "key")
			l := query.DefaultLimits()
			l.MaxBytes = 1024
			l.Timeout = 40 * time.Millisecond
			e, err := New(catalog.Config{Sources: []catalog.Source{source()}}, l)
			if err != nil {
				t.Fatal(err)
			}
			e.http = srv.Client()
			defer e.Close()
			r := query.Request{Mode: "native", ConnectionID: "x", SQL: "SELECT 1"}
			if scenario == "substitution" {
				r.ConnectionID = "another-tenant"
			}
			got := &sink{}
			_, err = e.Execute(context.Background(), r, got)
			if err == nil || got.schemas != 0 || len(got.docs) != 0 {
				t.Fatalf("invalid result err=%v schemas=%d", err, got.schemas)
			}
			if scenario == "deadline" && !errors.Is(err, context.DeadlineExceeded) && query.PublicError(err).Code != "DEADLINE_EXCEEDED" {
				t.Fatalf("lost deadline: %v", err)
			}
			if scenario == "substitution" && calls.Load() != 0 {
				t.Fatal("unselected source reached network")
			}
		})
	}
}

func (s *sink) Schema(*arrow.Schema) error { s.schemas++; return nil }
func (s *sink) Write(r arrow.RecordBatch) error {
	a := r.Column(0).(*array.Binary)
	for i := 0; i < int(r.NumRows()); i++ {
		s.docs = append(s.docs, append([]byte(nil), a.Value(i)...))
	}
	return nil
}
func source() catalog.Source {
	return catalog.Source{ID: "x", Type: "hive", Adapter: "dbapi", URLEnv: "URL", TokenEnv: "KEY", Options: map[string]string{"remote_connection_id": "configured"}}
}
func TestBoundGatewayContract(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q map[string]string
		json.NewDecoder(r.Body).Decode(&q)
		_, authorizationPresent := r.Header["Authorization"]
		if r.URL.Path != "/api/v1/metadata/query" || r.Header.Get("X-API-KEY") != "key" || authorizationPresent || q["id"] != "configured" || len(q) != 2 {
			http.Error(w, "bad", 400)
			return
		}
		w.Write([]byte(`{"results":[{"n":9007199254740993}],"rowCount":1}`))
	}))
	defer srv.Close()
	t.Setenv("URL", srv.URL)
	t.Setenv("KEY", "key")
	e, err := New(catalog.Config{Sources: []catalog.Source{source()}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e.http = srv.Client()
	var got sink
	st, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "x", SQL: "SELECT 9007199254740993"}, &got)
	if err != nil || st.Rows != 1 || string(got.docs[0]) != `{"n":9007199254740993}` {
		t.Fatal(err, st, got.docs)
	}
}
func TestInvalidGatewayEnvelopesNeverDeliverSchema(t *testing.T) {
	cases := []string{`{}`, `{"rowCount":0}`, `{"results":[null],"rowCount":1}`, `{"results":[],"rowCount":1}`, `{"results":[],"rowCount":0,"error":"no"}`, `{"results":[1],"rowCount":1}`, `{"results":[],"rowCount":0} trailing`}
	for _, body := range cases {
		t.Run(body, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
			defer srv.Close()
			t.Setenv("URL", srv.URL)
			t.Setenv("KEY", "key")
			e, err := New(catalog.Config{Sources: []catalog.Source{source()}}, query.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			e.http = srv.Client()
			got := &sink{}
			if _, err = e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "x", SQL: "SELECT 1"}, got); err == nil {
				t.Fatal("accepted invalid envelope")
			}
			if got.schemas != 0 || len(got.docs) != 0 {
				t.Fatal("delivered invalid result")
			}
		})
	}
}
func TestDBAPIDirectConfigFailures(t *testing.T) {
	t.Setenv("URL", "https://example.test")
	t.Setenv("KEY", "key")
	s := source()
	if _, err := New(catalog.Config{Sources: []catalog.Source{s, s}}, query.DefaultLimits()); err == nil {
		t.Fatal("duplicate accepted")
	}
	s.Options["remote_connection_id"] = ""
	if _, err := New(catalog.Config{Sources: []catalog.Source{s}}, query.DefaultLimits()); err == nil {
		t.Fatal("unbound source accepted")
	}
}
