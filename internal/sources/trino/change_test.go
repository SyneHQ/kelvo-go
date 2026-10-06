package trino

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestResolvedStatementWaitsForAcknowledgementWithoutReplay(t *testing.T) {
	for _, kind := range []string{"trino", "presto"} {
		t.Run(kind, func(t *testing.T) {
			for _, scenario := range []string{"ack", "ddl", "lost-reply", "wrong-handle", "foreign-next", "missing-ack", "negative-count"} {
				t.Run(scenario, func(t *testing.T) {
					var writes, reads atomic.Int32
					e := setup(t, kind, func(w http.ResponseWriter, r *http.Request) {
						if r.Method == http.MethodDelete {
							w.WriteHeader(204)
							return
						}
						if r.Method == http.MethodPost {
							writes.Add(1)
							if scenario == "lost-reply" {
								conn, _, _ := w.(http.Hijacker).Hijack()
								conn.Close()
								return
							}
							next := nextURL(r, kind, 1)
							if scenario == "foreign-next" {
								next = "https://unselected.invalid/v1/statement/executing/query_1/slug_1/1"
							}
							page(w, map[string]any{"id": "query_1", "nextUri": next})
							return
						}
						reads.Add(1)
						id := "query_1"
						if scenario == "wrong-handle" {
							id = "other"
						}
						response := map[string]any{"id": id, "updateType": "UPDATE", "updateCount": json.Number("9007199254740993")}
						if scenario == "missing-ack" {
							delete(response, "updateType")
							delete(response, "updateCount")
						}
						if scenario == "negative-count" {
							response["updateCount"] = -2
						}
						if scenario == "ddl" {
							response["updateType"] = "CREATE TABLE"
							delete(response, "updateCount")
						}
						page(w, response)
					})
					count, err := e.ApplyStatement(context.Background(), "UPDATE events SET amount=1")
					if writes.Load() != 1 {
						t.Fatal("statement repeated", writes.Load())
					}
					switch scenario {
					case "ack":
						if err != nil || count == nil || *count != 9007199254740993 {
							t.Fatal(count, err)
						}
					case "ddl":
						if err != nil || count != nil {
							t.Fatal(count, err)
						}
					default:
						if err == nil {
							t.Fatal("unacknowledged update accepted")
						}
					}
					if scenario == "foreign-next" && reads.Load() != 0 {
						t.Fatal("foreign pagination fetched")
					}
				})
			}
		})
	}
}
