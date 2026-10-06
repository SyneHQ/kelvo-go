package nativereader

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
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
	"github.com/apache/arrow-go/v18/arrow/array"
)

func readerSpec(engine, url string) adapter.ConnectionSpec {
	s := adapter.ConnectionSpec{Engine: engine, URL: url, TenantID: "tenant", ConnectionID: "saved", Revision: "current", Database: "analytics", Username: "fixture-user", Password: "fixture-password", Options: map[string]string{}}
	switch engine {
	case "elasticsearch":
		s.Options["authentication"] = "Basic"
	case "trino", "presto":
		s.Schema = "public"
		s.Options = map[string]string{"catalog": s.Database, "schema": s.Schema}
	case "spanner":
		s.Username = ""
		s.Password = ""
		s.Token = "fixture-token"
		s.Options = map[string]string{"project": "project", "instance": "instance", "database": s.Database}
	case "ignite":
		s.Options["cache_name"] = s.Database
	case "athena":
		s.Options = map[string]string{"region": "us-east-1", "workgroup": "reports", "database": s.Database, "output_location": "s3://results/queries/"}
	case "dynamodb":
		s.Options["region"] = "us-east-1"
		s.Token = "fixture-session-token"
	case "cosmosdb":
		s.Username = ""
		s.Password = ""
		s.Token = base64.StdEncoding.EncodeToString(make([]byte, 64))
		s.Schema = "records"
		s.Options = map[string]string{"database": s.Database, "container": s.Schema, "auth": "master_key"}
	}
	return s
}

type captureSink struct {
	rows      int64
	ints      []int64
	documents [][]byte
}

func (s *captureSink) Schema(*arrow.Schema) error { return nil }
func (s *captureSink) Write(b arrow.RecordBatch) error {
	s.rows += b.NumRows()
	switch col := b.Column(0).(type) {
	case *array.Int64:
		for i := 0; i < col.Len(); i++ {
			s.ints = append(s.ints, col.Value(i))
		}
	case *array.Binary:
		for i := 0; i < col.Len(); i++ {
			s.documents = append(s.documents, append([]byte(nil), col.Value(i)...))
		}
	default:
		return adapter.ErrInvalid
	}
	return nil
}

// Real TLS/protocol fixtures verify the credential boundary, exact Arrow values
// and source cleanup. They are not live provider-account acceptance.
func TestRequestOwnedNativeReadersUseOnlyCurrentCredentials(t *testing.T) {
	for _, engine := range []string{"elasticsearch", "trino", "presto", "spanner", "ignite", "athena", "dynamodb", "cosmosdb"} {
		t.Run(engine, func(t *testing.T) {
			var calls, cleanup atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				switch engine {
				case "elasticsearch":
					user, password, ok := r.BasicAuth()
					if !ok || user != "fixture-user" || password != "fixture-password" {
						t.Error("lost basic credentials")
					}
					if r.URL.Path != "/_sql" {
						t.Error("wrong SQL endpoint")
					}
					fmt.Fprint(w, `{"columns":[{"name":"id","type":"long"}],"rows":[[9007199254740993]]}`)
				case "trino", "presto":
					prefix := "X-Trino-"
					if engine == "presto" {
						prefix = "X-Presto-"
					}
					user, password, ok := r.BasicAuth()
					if !ok || user != "fixture-user" || password != "fixture-password" || r.Header.Get(prefix+"Catalog") != "analytics" || r.Header.Get(prefix+"Schema") != "public" {
						t.Error("lost basic credentials or namespace")
					}
					fmt.Fprint(w, `{"id":"query-1","columns":[{"name":"id","type":"bigint"}],"data":[[9007199254740993]]}`)
				case "spanner":
					if r.Header.Get("Authorization") != "Bearer fixture-token" {
						t.Error("lost tenant token")
					}
					switch {
					case r.Method == "DELETE":
						cleanup.Add(1)
						fmt.Fprint(w, `{}`)
					case strings.HasSuffix(r.URL.Path, "/sessions"):
						fmt.Fprint(w, `{"name":"projects/project/instances/instance/databases/analytics/sessions/session1"}`)
					case strings.HasSuffix(r.URL.Path, ":executeSql"):
						fmt.Fprint(w, `{"metadata":{"rowType":{"fields":[{"name":"id","type":{"code":"INT64"}}]}},"rows":[["9007199254740993"]]}`)
					default:
						t.Error("unexpected Spanner path")
						w.WriteHeader(400)
					}
				case "ignite":
					_ = r.ParseForm()
					if r.Form.Get("ignite.login") != "fixture-user" || r.Form.Get("ignite.password") != "fixture-password" || r.Form.Get("cacheName") != "analytics" {
						t.Error("lost Ignite private credentials/cache")
					}
					fmt.Fprint(w, `{"successStatus":0,"error":null,"response":{"fieldsMetadata":[{"fieldName":"id","fieldTypeName":"java.lang.Long"}],"items":[[9007199254740993]],"last":true,"queryId":1}}`)
				case "athena":
					if !strings.Contains(r.Header.Get("Authorization"), "Credential=fixture-user/") {
						t.Error("AWS signing ignored explicit access key")
					}
					switch r.Header.Get("X-Amz-Target") {
					case "AmazonAthena.StartQueryExecution":
						fmt.Fprint(w, `{"QueryExecutionId":"execution-1"}`)
					case "AmazonAthena.GetQueryExecution":
						fmt.Fprint(w, `{"QueryExecution":{"QueryExecutionId":"execution-1","Status":{"State":"SUCCEEDED"}}}`)
					case "AmazonAthena.GetQueryResults":
						fmt.Fprint(w, `{"ResultSet":{"ResultSetMetadata":{"ColumnInfo":[{"Name":"id","Type":"bigint"}]},"Rows":[{"Data":[{"VarCharValue":"id"}]},{"Data":[{"VarCharValue":"9007199254740993"}]}]}}`)
					default:
						t.Error("unexpected Athena action")
						w.WriteHeader(400)
					}
				case "dynamodb":
					if !strings.Contains(r.Header.Get("Authorization"), "Credential=fixture-user/") || r.Header.Get("X-Amz-Security-Token") != "fixture-session-token" {
						t.Error("AWS current credentials lost")
					}
					if r.Header.Get("X-Amz-Target") != "DynamoDB_20120810.ExecuteStatement" {
						t.Error("wrong DynamoDB action")
					}
					fmt.Fprint(w, `{"Items":[{"id":{"N":"9007199254740993"}}]}`)
				case "cosmosdb":
					if r.URL.Path != "/dbs/analytics/colls/records/docs" || r.Header.Get("Authorization") == "" {
						t.Error("wrong Cosmos resource or auth")
					}
					w.Header().Set("x-ms-request-charge", "1")
					fmt.Fprint(w, `{"_count":1,"Documents":[{"id":9007199254740993}]}`)
				}
			}))
			defer server.Close()
			t.Setenv("AWS_ACCESS_KEY_ID", "ambient-must-not-be-used")
			t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/absent/ambient.json")
			spec := readerSpec(engine, server.URL)
			spec.Options["tls_ca_pem"] = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
			s, err := Open(context.Background(), spec, adapter.ProcessLimits{MemoryMB: 64})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			sql := "SELECT id FROM records"
			if engine == "cosmosdb" {
				sql = "SELECT * FROM c"
			}
			sink := &captureSink{}
			stats, err := s.Query(context.Background(), adapter.Query{Statement: sql, MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 1}, sink)
			if err != nil || stats.Rows != 1 || sink.rows != 1 {
				t.Fatalf("rows=%d error=%v", stats.Rows, err)
			}
			if engine == "dynamodb" || engine == "cosmosdb" {
				if len(sink.documents) != 1 || !strings.Contains(string(sink.documents[0]), "9007199254740993") {
					t.Fatal("document precision lost")
				}
			} else if len(sink.ints) != 1 || sink.ints[0] != 9007199254740993 {
				t.Fatal("integer precision lost")
			}
			if engine == "spanner" && cleanup.Load() != 1 {
				t.Fatal("session cleanup missing")
			}
			before := calls.Load()
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = s.Query(cancelled, adapter.Query{Statement: sql, MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 1}, discardSink{})
			if err == nil || calls.Load() != before {
				t.Fatal("cancelled request reached source")
			}
			s.tls = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: x509.NewCertPool()}
			if _, err = s.Query(context.Background(), adapter.Query{Statement: sql, MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 1}, discardSink{}); err == nil {
				t.Fatal("untrusted TLS accepted")
			}
			if err != nil && (strings.Contains(err.Error(), "fixture-password") || strings.Contains(err.Error(), "fixture-session-token")) {
				t.Fatal("source error exposed credentials")
			}
			if calls.Load() != before {
				t.Fatal("TLS failure reached authenticated handler")
			}
		})
	}
}

func TestNativeReaderMetadataBoundsAndNamespace(t *testing.T) {
	for _, engine := range []string{"trino", "presto", "spanner", "athena", "exasol", "ignite"} {
		s := &Session{spec: adapter.ConnectionSpec{Engine: engine, Database: "analytics"}}
		sql, err := s.metadataQuery(operations.MetadataSpec{Object: "tables", Limit: 7})
		if err != nil || !strings.Contains(sql, "LIMIT 7 OFFSET 0") {
			t.Fatal(engine, sql, err)
		}
		for _, spec := range []operations.MetadataSpec{{Object: "tables", Limit: 7, Target: operations.ObjectRef{Catalog: "foreign"}}, {Object: "tables", Limit: 7, Cursor: "-1"}, {Object: "roles", Limit: 7}} {
			if _, err = s.metadataQuery(spec); err == nil {
				t.Fatal(engine, "unsupported metadata accepted")
			}
		}
	}
}

func TestDynamoDBConnectionTestUsesBoundedMetadata(t *testing.T) {
	var called atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if r.Header.Get("X-Amz-Target") != "DynamoDB_20120810.DescribeTable" || body["TableName"] != "analytics" {
			t.Error("wrong test target")
		}
		fmt.Fprint(w, `{"Table":{"TableName":"analytics"}}`)
	}))
	defer server.Close()
	spec := readerSpec("dynamodb", server.URL)
	spec.Options["tls_ca_pem"] = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	s, err := Open(context.Background(), spec, adapter.ProcessLimits{MemoryMB: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Test(context.Background()); err != nil || called.Load() != 1 {
		t.Fatal("connection test failed", err)
	}
}
