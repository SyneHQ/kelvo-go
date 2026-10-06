// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package spanner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestChangeExplicitCommitAndUncertainResponse(t *testing.T) {
	for _, scenario := range []string{"success", "cleanup_failure", "unknown_count", "lost_execute", "lost_commit", "invalid_commit", "foreign_session", "invalid_transaction"} {
		t.Run(scenario, func(t *testing.T) {
			var creates, executes, commits, rollbacks, deletes atomic.Int32
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if r.Method != http.MethodDelete && json.NewDecoder(r.Body).Decode(&body) != nil {
					t.Error("invalid request body")
				}
				switch {
				case r.URL.Path == "/v1/"+database+"/sessions":
					creates.Add(1)
					name := session
					if scenario == "foreign_session" {
						name = "projects/foreign/instances/instance/databases/db/sessions/s1"
					}
					fmt.Fprintf(w, `{"name":%q}`, name)
				case r.URL.Path == "/v1/"+session+":beginTransaction":
					if _, ok := body["options"].(map[string]any)["readWrite"]; !ok {
						t.Error("not a read-write transaction")
					}
					id := "dHgtMQ=="
					if scenario == "invalid_transaction" {
						id = "invalid!"
					}
					fmt.Fprintf(w, `{"id":%q}`, id)
				case r.URL.Path == "/v1/"+session+":executeSql":
					executes.Add(1)
					if body["sql"] != "UPDATE events SET active=true" || body["seqno"] != "1" || body["transaction"].(map[string]any)["id"] != "dHgtMQ==" {
						t.Error("wrong transaction binding")
					}
					if scenario == "lost_execute" {
						w.Header().Set("Content-Length", "10000")
						fmt.Fprint(w, `{`)
						return
					}
					count := "9007199254740993"
					if scenario == "unknown_count" {
						count = "bad"
					}
					fmt.Fprintf(w, `{"stats":{"rowCountExact":%q}}`, count)
				case r.URL.Path == "/v1/"+session+":commit":
					commits.Add(1)
					if body["transactionId"] != "dHgtMQ==" {
						t.Error("commit escaped transaction")
					}
					if scenario == "lost_commit" {
						w.Header().Set("Content-Length", "10000")
						fmt.Fprint(w, `{`)
						return
					}
					if scenario == "invalid_commit" {
						fmt.Fprint(w, `{}`)
						return
					}
					fmt.Fprint(w, `{"commitTimestamp":"2026-01-02T03:04:05.123456789Z"}`)
				case r.URL.Path == "/v1/"+session+":rollback":
					rollbacks.Add(1)
					if body["transactionId"] != "dHgtMQ==" {
						t.Error("rollback escaped transaction")
					}
					fmt.Fprint(w, `{}`)
				case r.Method == http.MethodDelete && r.URL.Path == "/v1/"+session:
					deletes.Add(1)
					if scenario == "cleanup_failure" {
						w.WriteHeader(503)
					}
					fmt.Fprint(w, `{}`)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(400)
				}
			})
			count, err := e.ApplyStatement(context.Background(), "UPDATE events SET active=true")
			wantSuccess := scenario == "success" || scenario == "cleanup_failure" || scenario == "unknown_count"
			if (err == nil) != wantSuccess || creates.Load() != 1 || executes.Load() > 1 || commits.Load() > 1 {
				t.Fatalf("err=%v creates=%d executes=%d commits=%d", err, creates.Load(), executes.Load(), commits.Load())
			}
			if wantSuccess && scenario != "unknown_count" && (count == nil || *count != 9007199254740993) {
				t.Fatal("lost exact committed count")
			}
			if scenario == "unknown_count" && count != nil {
				t.Fatal("invented count")
			}
			wantRollback := int32(0)
			if scenario == "lost_execute" || scenario == "lost_commit" || scenario == "invalid_commit" {
				wantRollback = 1
			}
			wantDelete := int32(1)
			if scenario == "foreign_session" {
				wantDelete = 0
			}
			if rollbacks.Load() != wantRollback || deletes.Load() != wantDelete {
				t.Fatalf("rollback=%d deletes=%d", rollbacks.Load(), deletes.Load())
			}
		})
	}
}

func TestChangeDDLTracksOnlyClientNamedOperation(t *testing.T) {
	for _, scenario := range []string{"success", "changed_identity", "failed", "cancelled", "lost_submit"} {
		t.Run(scenario, func(t *testing.T) {
			var name string
			var submits, polls, cancels atomic.Int32
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPatch {
					submits.Add(1)
					var body struct {
						Statements []string `json:"statements"`
						ID         string   `json:"operationId"`
					}
					if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Statements) != 1 || body.Statements[0] != "CREATE TABLE events (id INT64) PRIMARY KEY(id)" || !strings.HasPrefix(body.ID, "kelvo_") || r.URL.Path != "/v1/"+database+"/ddl" {
						t.Error("incorrect DDL submission")
					}
					name = database + "/operations/" + body.ID
					if scenario == "lost_submit" {
						w.Header().Set("Content-Length", "10000")
						fmt.Fprint(w, `{`)
						return
					}
					fmt.Fprintf(w, `{"name":%q}`, name)
					return
				}
				if strings.HasSuffix(r.URL.Path, ":cancel") {
					cancels.Add(1)
					if r.URL.Path != "/v1/"+name+":cancel" {
						t.Error("cancel escaped operation")
					}
					fmt.Fprint(w, `{}`)
					return
				}
				polls.Add(1)
				if r.Method != http.MethodGet || r.URL.Path != "/v1/"+name {
					t.Error("poll escaped operation")
				}
				response := changeOperation{Name: name, Done: true}
				if scenario == "changed_identity" {
					response.Name = database + "/operations/other"
				}
				if scenario == "failed" {
					response.Error = map[string]string{"message": "private source details"}
				}
				json.NewEncoder(w).Encode(response)
			})
			if scenario == "cancelled" {
				e.limits.Timeout = 30 * time.Millisecond
			}
			count, err := e.ApplyStatement(context.Background(), "CREATE TABLE events (id INT64) PRIMARY KEY(id)")
			if (err == nil) != (scenario == "success") || count != nil || submits.Load() != 1 || polls.Load() > 1 {
				t.Fatalf("count=%v err=%v submits=%d polls=%d", count, err, submits.Load(), polls.Load())
			}
			wantCancel := int32(1)
			if scenario == "success" || scenario == "lost_submit" {
				wantCancel = 0
			}
			if cancels.Load() != wantCancel {
				t.Fatalf("cancels=%d", cancels.Load())
			}
		})
	}
}
