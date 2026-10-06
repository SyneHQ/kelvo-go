package clickhouse

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func arrowRows(t *testing.T, count int) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for i := 0; i < count; i++ {
		b.Append(9007199254740993 + int64(i))
	}
	values := b.NewArray()
	defer values.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, int64(count))
	defer record.Release()
	var out bytes.Buffer
	writer := ipc.NewWriter(&out, ipc.WithSchema(schema))
	if err := writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func testSession(t *testing.T, handler func(http.ResponseWriter, *http.Request, string)) (*Session, *atomic.Int32) {
	t.Helper()
	probe := arrowRows(t, 1)
	calls := &atomic.Int32{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		user, password, ok := r.BasicAuth()
		if !ok || user != "reader" || password != "test-only" {
			t.Error("lost source credentials")
		}
		if string(body) == "SELECT 1" {
			_, _ = w.Write(probe)
			return
		}
		handler(w, r, string(body))
	}))
	t.Cleanup(server.Close)
	endpoint, _ := url.Parse(server.URL)
	port, _ := strconv.Atoi(endpoint.Port())
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	opened, err := (Driver{}).Open(context.Background(), adapter.Connection{Engine: "clickhouse", TenantID: "tenant-1", ConnectionID: "saved-1", Revision: "revision-1", Host: endpoint.Hostname(), Port: port, Namespace: "app", Username: "reader", Password: "test-only", TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}})
	if err != nil {
		t.Fatal(err)
	}
	s := opened.(*Session)
	t.Cleanup(func() { _ = s.Close() })
	return s, calls
}

type captureSink struct {
	schema   *arrow.Schema
	rows     int64
	batches  int
	maxBatch int64
	first    int64
}

func (s *captureSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *captureSink) Write(record arrow.RecordBatch) error {
	if s.rows == 0 {
		s.first = record.Column(0).(*array.Int64).Value(0)
	}
	s.rows += record.NumRows()
	s.batches++
	s.maxBatch = max(s.maxBatch, record.NumRows())
	return nil
}

func TestQueryReusesNativeBoundedArrowReader(t *testing.T) {
	data := arrowRows(t, 3)
	s, calls := testSession(t, func(w http.ResponseWriter, r *http.Request, sql string) {
		if sql != "SELECT id FROM items" || r.URL.Query().Get("readonly") != "1" || r.URL.Query().Get("default_format") != "ArrowStream" {
			t.Error("query settings lost")
		}
		_, _ = w.Write(data)
	})
	sink := &captureSink{}
	stats, err := s.Query(context.Background(), adapter.Query{Statement: "SELECT id FROM items", MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 2}, sink)
	if err != nil || stats.Rows != 3 || sink.rows != 3 || sink.batches != 2 || sink.maxBatch != 2 || sink.first != 9007199254740993 || calls.Load() != 2 {
		t.Fatal(stats, err, calls.Load())
	}
	if s.client.Transport.(*http.Transport).Proxy != nil {
		t.Fatal("ambient proxy inherited")
	}
}

func TestChangesStopAfterAmbiguousAutocommitWithoutRetry(t *testing.T) {
	s, calls := testSession(t, func(w http.ResponseWriter, r *http.Request, sql string) {
		if r.URL.Query().Get("async_insert") != "0" || r.URL.Query().Get("wait_end_of_query") != "1" || r.URL.Query().Get("query_id") == "" {
			t.Error("write completion settings absent")
		}
		if strings.Contains(sql, "second") {
			http.Error(w, "source error", 503)
		}
	})
	result, err := s.Execute(context.Background(), adapter.Change{Statements: []string{"UPDATE first SET n=1", "UPDATE second SET n=2", "UPDATE third SET n=3"}})
	if err == nil || result.Outcome != "unknown" || result.Completed != 1 || result.Attempted != 2 || calls.Load() != 3 {
		t.Fatal("ambiguous write lost or retried", result, err, calls.Load())
	}
}

func TestUnsupportedModesFailBeforeAnotherSourceRequest(t *testing.T) {
	s, calls := testSession(t, func(http.ResponseWriter, *http.Request, string) { t.Error("unexpected source request") })
	for _, change := range []adapter.Change{{Statements: []string{"UPDATE items SET n=1"}, Transaction: true}, {Statements: []string{"UPDATE items SET n=1"}, Role: "admin"}, {Statements: []string{"COMMIT"}}} {
		if _, err := s.Execute(context.Background(), change); err == nil {
			t.Fatal("unsupported mode accepted")
		}
	}
	if _, err := s.Query(context.Background(), adapter.Query{Statement: "DELETE FROM items", MaxRows: 1, MaxBytes: 1024, BatchRows: 1}, discardSink{}); err == nil || calls.Load() != 1 {
		t.Fatal("write reached query path")
	}
}

func TestMetadataScopeAndEscaping(t *testing.T) {
	s := &Session{database: "app"}
	sql, err := s.metadataQuery(operations.MetadataSpec{Object: "tables", Target: operations.ObjectRef{Name: "t' OR 1=1 --"}, Limit: 10, Cursor: "20"})
	if err != nil || !strings.Contains(sql, "name='t'' OR 1=1 --'") || !strings.HasSuffix(sql, "LIMIT 10 OFFSET 20") {
		t.Fatal(sql, err)
	}
	for _, spec := range []operations.MetadataSpec{{Object: "tables", Target: operations.ObjectRef{Catalog: "other"}, Limit: 10}, {Object: "columns", Target: operations.ObjectRef{Name: "t\\x"}, Limit: 10}, {Object: "primary_keys", Limit: 10}, {Object: "tables", Limit: 10, Cursor: "01"}} {
		if _, err := s.metadataQuery(spec); err == nil {
			t.Fatal("invalid metadata scope accepted")
		}
	}
}
