package nativereader

import (
	"bytes"
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type metadataCapture struct {
	schema *arrow.Schema
	rows   [][]any
	wire   bytes.Buffer
	writer *ipc.Writer
}

func (s *metadataCapture) Schema(schema *arrow.Schema) error {
	s.schema = schema
	s.writer = ipc.NewWriter(&s.wire, ipc.WithSchema(schema))
	return nil
}
func (s *metadataCapture) Write(batch arrow.RecordBatch) error {
	if err := s.writer.Write(batch); err != nil {
		return err
	}
	for row := 0; row < int(batch.NumRows()); row++ {
		values := make([]any, batch.NumCols())
		for i, col := range batch.Columns() {
			values[i] = col.GetOneForMarshal(row)
		}
		s.rows = append(s.rows, values)
	}
	return nil
}

func (s *metadataCapture) close() error {
	if s.writer == nil {
		return nil
	}
	err := s.writer.Close()
	s.writer = nil
	return err
}
func TestBoundedNativeDiscoveryUsesSelectedNamespace(t *testing.T) {
	for _, engine := range []string{"elasticsearch", "dynamodb", "cosmosdb"} {
		t.Run(engine, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				switch engine {
				case "elasticsearch":
					switch r.URL.Path {
					case "/_cat/indices/analytics":
						fmt.Fprint(w, `[{"index":"analytics"}]`)
					case "/analytics/_field_caps":
						fmt.Fprint(w, `{"fields":{"id":{"long":{"type":"long","searchable":true,"aggregatable":true}}}}`)
					default:
						t.Error("unexpected discovery path")
						w.WriteHeader(400)
					}
				case "dynamodb":
					if r.Header.Get("X-Amz-Target") != "DynamoDB_20120810.DescribeTable" {
						t.Error("unexpected action")
					}
					fmt.Fprint(w, `{"Table":{"TableName":"analytics","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"N"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}}`)
				case "cosmosdb":
					if (r.URL.Path != "/dbs/analytics/colls/records" && r.URL.Path != "/dbs/analytics") || r.Method != "GET" || r.Header.Get("Authorization") == "" {
						t.Error("wrong Cosmos resource")
					}
					w.Header().Set("x-ms-request-charge", "1")
					if r.URL.Path == "/dbs/analytics" {
						fmt.Fprint(w, `{"id":"analytics"}`)
					} else {
						fmt.Fprint(w, `{"id":"records","partitionKey":{"paths":["/id"]}}`)
					}
				}
			}))
			defer server.Close()
			spec := readerSpec(engine, server.URL)
			spec.Options["tls_ca_pem"] = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
			s, err := Open(context.Background(), spec, adapter.ProcessLimits{MemoryMB: 64})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			limits := adapter.Limits{MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 2}
			for _, object := range []string{"databases", "schemas", "tables", "columns"} {
				out := &metadataCapture{}
				stats, err := s.Inspect(context.Background(), operations.MetadataSpec{Object: object, Limit: 5}, limits, out)
				if err != nil || stats.Rows != 1 || len(out.rows) != 1 {
					t.Fatal(engine, object, stats, err)
				}
				verifyMetadataCapture(t, s, object, out)
			}
			before := calls.Load()
			_, err = s.Inspect(context.Background(), operations.MetadataSpec{Object: "tables", Limit: 5, Target: operations.ObjectRef{Catalog: "foreign"}}, limits, &metadataCapture{})
			if err == nil || calls.Load() != before {
				t.Fatal("foreign catalog requested")
			}
		})
	}
}
func TestCosmosDiscoveryCanListContainersBeforeSelection(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if r.URL.Path != "/dbs/analytics/colls" {
			t.Error("unbound resource")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("x-ms-request-charge", "1")
		if n == 1 {
			w.Header().Set("x-ms-continuation", "next-page")
			fmt.Fprint(w, `{"_count":1,"DocumentCollections":[{"id":"first"}]}`)
		} else {
			if r.Header.Get("x-ms-continuation") != "next-page" {
				t.Error("missing continuation")
			}
			fmt.Fprint(w, `{"_count":1,"DocumentCollections":[{"id":"second"}]}`)
		}
	}))
	defer server.Close()
	spec := readerSpec("cosmosdb", server.URL)
	spec.Schema = ""
	delete(spec.Options, "container")
	spec.Options["tls_ca_pem"] = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	s, err := Open(context.Background(), spec, adapter.ProcessLimits{MemoryMB: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sink := &metadataCapture{}
	stats, err := s.Inspect(context.Background(), operations.MetadataSpec{Object: "tables", Limit: 2}, adapter.Limits{MaxRows: 2, MaxBytes: 1 << 20, BatchRows: 2}, sink)
	defer sink.close()
	if err != nil || stats.Rows != 2 || calls.Load() != 2 || sink.rows[1][2] != "second" {
		t.Fatal(stats, err)
	}
	_, err = s.Query(context.Background(), adapter.Query{Statement: "SELECT * FROM c", MaxRows: 2, MaxBytes: 1 << 20, BatchRows: 2}, discardSink{})
	if err == nil || calls.Load() != 2 {
		t.Fatal("query without container reached source")
	}
}
func TestWarehouseMetadataStaysInsideSavedNamespace(t *testing.T) {
	for _, engine := range []string{"snowflake", "bigquery"} {
		s := &Session{spec: adapter.ConnectionSpec{Engine: engine, Database: "analytics", Options: map[string]string{"project": "project"}}}
		sql, err := s.metadataQuery(operations.MetadataSpec{Object: "columns", Limit: 20})
		if err != nil || !strings.Contains(sql, "analytics") || !strings.Contains(sql, "LIMIT 20") {
			t.Fatal(sql, err)
		}
		if _, err = s.metadataQuery(operations.MetadataSpec{Object: "tables", Limit: 5, Target: operations.ObjectRef{Catalog: "foreign"}}); err == nil {
			t.Fatal("foreign warehouse accepted")
		}
	}
}
