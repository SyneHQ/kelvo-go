package nativereader

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/gorilla/websocket"
)

// Optional VM artifact export lets the separate driver-free API consume the
// actual Arrow output of these protocol fixtures without importing an adapter.
func verifyMetadataCapture(t *testing.T, s *Session, object string, out *metadataCapture) {
	t.Helper()
	if err := out.close(); err != nil {
		t.Fatal(err)
	}
	if out.schema == nil || len(out.rows) == 0 {
		t.Fatal("metadata schema/row missing")
	}
	row := map[string]any{}
	for i, field := range out.schema.Fields() {
		row[field.Name] = out.rows[0][i]
	}
	if object != "columns" && s.spec.Engine != "arrow_flight" && row["catalog"] != s.spec.Database {
		t.Fatalf("catalog escaped saved database: %+v", row)
	}
	if object == "schemas" || object == "tables" || object == "columns" {
		if _, ok := row["schema_name"].(string); !ok {
			t.Fatalf("missing schema: %+v", row)
		}
	}
	if object == "columns" {
		if row["name"] == nil || row["table_name"] == nil || row["type"] == nil {
			t.Fatal("missing column identity")
		}
		if row["representation"] == nil && (row["position"] == nil || row["nullable"] == nil) {
			t.Fatal("relational guarantees lost")
		}
		if row["representation"] != nil && (row["position"] != nil || row["nullable"] != nil) {
			t.Fatal("invented document guarantees")
		}
	}
	if dir := os.Getenv("KELVO_NATIVE_METADATA_CAPTURE_DIR"); dir != "" {
		stem := s.spec.Engine + "-" + object
		if err := os.WriteFile(filepath.Join(dir, stem+".arrow"), out.wire.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		info := map[string]any{"engine": s.spec.Engine, "object": object, "database": s.spec.Database, "schema": row["schema_name"]}
		if s.spec.Engine == "arrow_flight" && object != "databases" {
			info["targetCatalog"] = "analytics"
		}
		encoded, _ := json.Marshal(info)
		if err := os.WriteFile(filepath.Join(dir, stem+".json"), encoded, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func metadataFixtureRow(engine, object string) ([]string, []any) {
	schema := "public"
	switch engine {
	case "athena", "bigquery", "exasol":
		schema = "analytics"
	case "ignite":
		schema = "PUBLIC"
	case "spanner":
		schema = ""
	}
	switch object {
	case "databases":
		return []string{"catalog"}, []any{"analytics"}
	case "schemas":
		return []string{"catalog", "schema_name"}, []any{"analytics", schema}
	case "tables":
		return []string{"catalog", "schema_name", "name", "type"}, []any{"analytics", schema, "records", "BASE TABLE"}
	default:
		nullable := any("YES")
		if engine == "exasol" {
			nullable = true
		}
		return []string{"schema_name", "table_name", "name", "type", "position", "nullable"}, []any{schema, "records", "amount", "DECIMAL", int64(2), nullable}
	}
}

func checkMetadataSQL(t *testing.T, engine, object, sql string) {
	t.Helper()
	lower := strings.ToLower(sql)
	if !strings.Contains(lower, "limit 5 offset 0") || (!strings.Contains(sql, "analytics") && !(engine == "spanner" && object == "columns")) {
		t.Error("metadata query lost bounds/namespace")
	}
	if object == "tables" || object == "schemas" || object == "databases" {
		if !strings.Contains(lower, " as catalog") {
			t.Error("metadata omitted catalog projection")
		}
		if (engine == "bigquery" || engine == "athena") && !strings.Contains(lower, "select 'analytics' as catalog") {
			t.Error("provider catalog replaced saved database")
		}
	}
}

func TestNativeSQLMetadataProtocolEnvelopes(t *testing.T) {
	for _, engine := range []string{"trino", "presto", "athena", "spanner", "ignite", "bigquery", "snowflake", "exasol"} {
		for _, object := range []string{"databases", "schemas", "tables", "columns"} {
			if engine == "ignite" && object == "columns" {
				continue
			} // No invented SYS view contract.
			t.Run(engine+"/"+object, func(t *testing.T) {
				fields, row := metadataFixtureRow(engine, object)
				var mu sync.Mutex
				var job map[string]any
				var handler http.HandlerFunc
				if engine == "exasol" {
					handler = exasolMetadataHandler(t, object, fields, row)
				} else {
					handler = func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						encode := func(value any) {
							if err := json.NewEncoder(w).Encode(value); err != nil {
								t.Error(err)
							}
						}
						switch engine {
						case "trino", "presto":
							text, _ := io.ReadAll(r.Body)
							checkMetadataSQL(t, engine, object, string(text))
							columns := []map[string]any{}
							for i, name := range fields {
								typ := "varchar"
								if i == 4 && object == "columns" {
									typ = "bigint"
								}
								columns = append(columns, map[string]any{"name": name, "type": typ})
							}
							encode(map[string]any{"id": "metadata-1", "columns": columns, "data": [][]any{row}})
						case "ignite":
							r.ParseForm()
							checkMetadataSQL(t, engine, object, r.Form.Get("qry"))
							columns := []map[string]any{}
							for _, name := range fields {
								columns = append(columns, map[string]any{"fieldName": strings.ToUpper(name), "fieldTypeName": "java.lang.String"})
							}
							encode(map[string]any{"successStatus": 0, "error": nil, "response": map[string]any{"fieldsMetadata": columns, "items": [][]any{row}, "last": true, "queryId": 1}})
						case "athena":
							switch r.Header.Get("X-Amz-Target") {
							case "AmazonAthena.StartQueryExecution":
								var body struct{ QueryString string }
								json.NewDecoder(r.Body).Decode(&body)
								checkMetadataSQL(t, engine, object, body.QueryString)
								encode(map[string]any{"QueryExecutionId": "metadata-1"})
							case "AmazonAthena.GetQueryExecution":
								encode(map[string]any{"QueryExecution": map[string]any{"QueryExecutionId": "metadata-1", "Status": map[string]any{"State": "SUCCEEDED"}}})
							case "AmazonAthena.GetQueryResults":
								columns := []map[string]any{}
								header, data := []map[string]any{}, []map[string]any{}
								for i, name := range fields {
									typ := "varchar"
									if name == "position" {
										typ = "bigint"
									}
									columns = append(columns, map[string]any{"Name": name, "Type": typ})
									header = append(header, map[string]any{"VarCharValue": name})
									data = append(data, map[string]any{"VarCharValue": fmt.Sprint(row[i])})
								}
								encode(map[string]any{"ResultSet": map[string]any{"ResultSetMetadata": map[string]any{"ColumnInfo": columns}, "Rows": []map[string]any{{"Data": header}, {"Data": data}}}})
							default:
								t.Error("unexpected Athena call")
								w.WriteHeader(400)
							}
						case "spanner":
							if !strings.Contains(r.URL.Path, "/projects/project/instances/instance/databases/analytics/") {
								t.Error("Spanner metadata escaped saved database endpoint")
							}
							switch {
							case r.Method == "DELETE":
								encode(map[string]any{})
							case strings.HasSuffix(r.URL.Path, "/sessions"):
								encode(map[string]any{"name": "projects/project/instances/instance/databases/analytics/sessions/meta"})
							case strings.HasSuffix(r.URL.Path, ":executeSql"):
								var body struct{ SQL string }
								json.NewDecoder(r.Body).Decode(&body)
								checkMetadataSQL(t, engine, object, body.SQL)
								columns := []map[string]any{}
								values := append([]any(nil), row...)
								for i, name := range fields {
									typ := "STRING"
									if name == "position" {
										typ = "INT64"
										values[i] = fmt.Sprint(values[i])
									}
									columns = append(columns, map[string]any{"name": name, "type": map[string]string{"code": typ}})
								}
								encode(map[string]any{"metadata": map[string]any{"rowType": map[string]any{"fields": columns}}, "rows": [][]any{values}})
							default:
								t.Error("unexpected Spanner call")
								w.WriteHeader(400)
							}
						case "bigquery":
							if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/jobs") {
								var body map[string]any
								json.NewDecoder(r.Body).Decode(&body)
								cfg := body["configuration"].(map[string]any)["query"].(map[string]any)
								checkMetadataSQL(t, engine, object, cfg["query"].(string))
								mu.Lock()
								job = body["jobReference"].(map[string]any)
								current := job
								mu.Unlock()
								encode(map[string]any{"jobReference": current, "status": map[string]any{}})
								return
							}
							mu.Lock()
							current := job
							mu.Unlock()
							columns := []map[string]any{}
							values := []map[string]any{}
							for i, name := range fields {
								typ := "STRING"
								if name == "position" {
									typ = "INTEGER"
								}
								columns = append(columns, map[string]any{"name": name, "type": typ, "mode": "NULLABLE"})
								values = append(values, map[string]any{"v": fmt.Sprint(row[i])})
							}
							encode(map[string]any{"jobReference": current, "jobComplete": true, "schema": map[string]any{"fields": columns}, "rows": []map[string]any{{"f": values}}, "totalRows": "1"})
						case "snowflake":
							var body struct{ Statement string }
							json.NewDecoder(r.Body).Decode(&body)
							checkMetadataSQL(t, engine, object, body.Statement)
							columns := []map[string]any{}
							values := append([]any(nil), row...)
							for i, name := range fields {
								typ := "text"
								if name == "position" {
									typ = "fixed"
									values[i] = fmt.Sprint(values[i])
								}
								columns = append(columns, map[string]any{"name": strings.ToUpper(name), "type": typ, "precision": 18, "scale": 0})
							}
							encode(map[string]any{"statementHandle": "metadata-1", "code": "090001", "data": [][]any{values}, "resultSetMetaData": map[string]any{"numRows": 1, "format": "jsonv2", "rowType": columns, "partitionInfo": []map[string]int{{"rowCount": 1}}}})
						}
					}
				}
				server := httptest.NewTLSServer(handler)
				defer server.Close()
				spec := readerSpec(engine, server.URL)
				if engine == "exasol" {
					spec.URL = "wss" + strings.TrimPrefix(server.URL, "https")
					spec.Options["schema"] = spec.Database
				}
				var s *Session
				var err error
				if engine == "bigquery" || engine == "snowflake" {
					// Fixed public origins are validated elsewhere; this unit test
					// substitutes only a trusted fixture origin after admission.
					spec.Username, spec.Password, spec.Token = "", "", "fixture-token"
					if engine == "bigquery" {
						spec.Options = map[string]string{"project": "project", "dataset": "analytics", "location": "US"}
					} else {
						spec.Schema = "public"
						spec.Options = map[string]string{"database": "analytics", "schema": "public", "token_type": "OAUTH"}
					}
					roots := x509.NewCertPool()
					roots.AddCert(server.Certificate())
					s = &Session{spec: spec, source: catalog.Source{ID: "operation_source", Type: engine, Options: spec.Options}, tls: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, memoryMB: 64}
				} else {
					spec.Options["tls_ca_pem"] = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
					s, err = Open(context.Background(), spec, adapter.ProcessLimits{MemoryMB: 64})
				}
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				out := &metadataCapture{}
				defer out.close()
				stats, err := s.Inspect(context.Background(), operations.MetadataSpec{Object: object, Limit: 5}, adapter.Limits{MaxRows: 5, MaxBytes: 1 << 20, BatchRows: 2}, out)
				if err != nil || stats.Rows != 1 {
					t.Fatalf("Inspect failed: %+v %v", stats, err)
				}
				verifyMetadataCapture(t, s, object, out)
			})
		}
	}
}

func exasolMetadataHandler(t *testing.T, object string, fields []string, row []any) http.HandlerFunc {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		reply := func(value any) { conn.WriteJSON(map[string]any{"status": "ok", "responseData": value}) }
		for {
			var in map[string]any
			if conn.ReadJSON(&in) != nil {
				return
			}
			switch in["command"] {
			case "login":
				reply(map[string]any{"publicKeyModulus": hex.EncodeToString(key.N.Bytes()), "publicKeyExponent": "10001"})
			case nil:
				encoded, _ := in["password"].(string)
				encrypted, _ := base64.StdEncoding.DecodeString(encoded)
				plain, err := rsa.DecryptPKCS1v15(rand.Reader, key, encrypted)
				if err != nil || string(plain) != "fixture-password" || in["username"] != "fixture-user" {
					t.Error("current Exasol credentials lost")
				}
				reply(map[string]any{"sessionId": 1, "protocolVersion": 2})
			case "getAttributes":
				conn.WriteJSON(map[string]any{"status": "ok", "attributes": map[string]any{"autocommit": false, "timestampUtcEnabled": true}})
			case "execute":
				sql, _ := in["sqlText"].(string)
				if sql == "ROLLBACK" {
					reply(map[string]any{"numResults": 1, "results": []any{map[string]any{"resultType": "rowCount", "rowCount": 0}}})
					continue
				}
				checkMetadataSQL(t, "exasol", object, sql)
				columns := []map[string]any{}
				data := [][]any{}
				for i, name := range fields {
					typ := map[string]any{"type": "VARCHAR", "size": 128}
					if name == "position" {
						typ = map[string]any{"type": "DECIMAL", "precision": 18, "scale": 0}
					}
					if name == "nullable" {
						typ = map[string]any{"type": "BOOLEAN"}
					}
					columns = append(columns, map[string]any{"name": strings.ToUpper(name), "dataType": typ})
					data = append(data, []any{row[i]})
				}
				reply(map[string]any{"numResults": 1, "results": []any{map[string]any{"resultType": "resultSet", "resultSet": map[string]any{"numColumns": len(fields), "numRows": 1, "numRowsInMessage": 1, "columns": columns, "data": data}}}})
			case "disconnect", "closeResultSet":
				conn.WriteJSON(map[string]any{"status": "ok"})
			default:
				t.Error("unexpected Exasol protocol request")
				return
			}
		}
	}
}
