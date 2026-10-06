package cloudsql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestCloudSQLBoundParametersStayOutsideSQL(t *testing.T) {
	for _, engine := range []string{"d1", "databricks"} {
		t.Run(engine, func(t *testing.T) {
			var calls atomic.Int32
			s := fixtureSession(t, engine, func(w http.ResponseWriter, r *http.Request) {
				var body map[string]json.RawMessage
				if json.NewDecoder(r.Body).Decode(&body) != nil {
					t.Error("invalid body")
				}
				calls.Add(1)
				key := "sql"
				if engine == "databricks" {
					key = "statement"
				}
				if strings.Contains(string(body[key]), "dangerous") {
					t.Error("parameter interpolated")
				}
				var sql string
				_ = json.Unmarshal(body[key], &sql)
				read := strings.HasPrefix(sql, "SELECT")
				if engine == "d1" {
					if string(body["params"]) != `["dangerous' --",null]` {
						t.Error(string(body["params"]))
					}
					if read {
						fmt.Fprint(w, `{"success":true,"result":[{"success":true,"results":{"columns":["id"],"rows":[[1]]}}]}`)
					} else {
						fmt.Fprint(w, `{"success":true,"result":[{"success":true,"meta":{"changes":1}}]}`)
					}
				} else {
					if !strings.Contains(sql, ":p1") || !strings.Contains(sql, ":p2") || string(body["parameters"]) != `[{"name":"p1","type":"STRING","value":"dangerous' --"},{"name":"p2","type":"STRING"}]` {
						t.Error("incorrect Databricks bindings", sql, string(body["parameters"]))
					}
					if read {
						fmt.Fprint(w, `{"statement_id":"query-1","status":{"state":"SUCCEEDED"},"manifest":{"format":"JSON_ARRAY","schema":{"columns":[{"name":"id","type_name":"BIGINT"}]},"total_row_count":1,"total_chunk_count":1},"result":{"row_count":1,"data_array":[["1"]]}}`)
					} else {
						fmt.Fprint(w, `{"statement_id":"write-1","status":{"state":"SUCCEEDED"}}`)
					}
				}
			})
			p := []operations.Parameter{{Type: "string", Value: json.RawMessage(`"dangerous' --"`)}, {Type: "null", Value: json.RawMessage(`null`)}}
			if _, err := s.Query(context.Background(), adapter.Query{Statement: "SELECT 1 WHERE ? IS NOT ?", Parameters: p, MaxRows: 10, MaxBytes: 65536, BatchRows: 1}, &intSink{}); err != nil {
				t.Fatal(err)
			}
			r, err := s.Execute(context.Background(), adapter.Change{Statements: []string{"UPDATE events SET marker=? WHERE marker IS NOT ?"}, Parameters: [][]operations.Parameter{p}})
			if err != nil || r.Outcome != "succeeded" || calls.Load() != 2 {
				t.Fatal(r, err, calls.Load())
			}
			invalid := []operations.Parameter{{Type: "json", Value: json.RawMessage(`{}`)}}
			r, err = s.Execute(context.Background(), adapter.Change{Statements: []string{"UPDATE events SET n=1", "UPDATE events SET n=?"}, Parameters: [][]operations.Parameter{nil, invalid}})
			if err == nil || r.Attempted != 0 || calls.Load() != 2 {
				t.Fatal("invalid later binding allowed earlier effect", r, err)
			}
		})
	}
}
