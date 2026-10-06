// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package athena

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

func TestChangeAcknowledgementNoReuseOrReplay(t *testing.T) {
	for _, scenario := range []string{"success", "changed_identity", "failed", "lost_submit", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			var submits, polls, cancels atomic.Int32
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if json.NewDecoder(r.Body).Decode(&body) != nil {
					t.Error("invalid request JSON")
				}
				switch r.Header.Get("X-Amz-Target") {
				case "AmazonAthena.StartQueryExecution":
					submits.Add(1)
					if body["QueryString"] != "INSERT INTO facts VALUES (1)" || body["WorkGroup"] != "analytics" || len(fmt.Sprint(body["ClientRequestToken"])) != 64 || body["QueryExecutionContext"].(map[string]any)["Database"] != "sales" || body["ResultConfiguration"].(map[string]any)["OutputLocation"] != "s3://test-query-results/prefix/" || body["ResultReuseConfiguration"].(map[string]any)["ResultReuseByAgeConfiguration"].(map[string]any)["Enabled"] != false {
						t.Error("unsafe Athena submission")
					}
					if scenario == "lost_submit" {
						w.Header().Set("Content-Length", "10000")
						fmt.Fprint(w, `{`)
						return
					}
					fmt.Fprint(w, `{"QueryExecutionId":"write-1"}`)
				case "AmazonAthena.GetQueryExecution":
					polls.Add(1)
					if body["QueryExecutionId"] != "write-1" {
						t.Error("poll escaped query")
					}
					id, state := "write-1", "SUCCEEDED"
					if scenario == "changed_identity" {
						id = "other"
					}
					if scenario == "failed" {
						state = "FAILED"
					}
					if scenario == "cancelled" {
						state = "RUNNING"
					}
					fmt.Fprintf(w, `{"QueryExecution":{"QueryExecutionId":%q,"Status":{"State":%q}}}`, id, state)
				case "AmazonAthena.StopQueryExecution":
					cancels.Add(1)
					if body["QueryExecutionId"] != "write-1" {
						t.Error("cancel escaped query")
					}
					fmt.Fprint(w, `{}`)
				default:
					t.Error("unexpected action")
					w.WriteHeader(400)
				}
			})
			if scenario == "cancelled" {
				e.l.Timeout = 30 * time.Millisecond
			}
			count, err := e.ApplyStatement(context.Background(), "INSERT INTO facts VALUES (1)")
			if (err == nil) != (scenario == "success") || count != nil || submits.Load() != 1 {
				t.Fatalf("count=%v err=%v submits=%d", count, err, submits.Load())
			}
			wantCancel := int32(1)
			if scenario == "success" || scenario == "lost_submit" {
				wantCancel = 0
			}
			if cancels.Load() != wantCancel || polls.Load() > 1 {
				t.Fatalf("polls=%d cancels=%d", polls.Load(), cancels.Load())
			}
		})
	}
}

func TestChangeEncodedBudgetBeforeDispatch(t *testing.T) {
	var calls atomic.Int32
	e := setup(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) })
	for _, sql := range []string{"", "INSERT INTO facts VALUES ('" + strings.Repeat("<", 50000) + "')", strings.Repeat("x", 256<<10)} {
		if e.ValidateStatement(sql) == nil {
			t.Fatal("accepted oversized encoded request")
		}
		if _, err := e.ApplyStatement(context.Background(), sql); err == nil {
			t.Fatal("oversized request succeeded")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid statement reached source")
	}
}
