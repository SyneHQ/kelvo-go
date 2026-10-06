// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package snowflake

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

func TestChangeAcknowledgementAndNoReplay(t *testing.T) {
	for _, scenario := range []string{"success", "changed_handle", "failed", "lost_submit", "cancelled", "multiple"} {
		t.Run(scenario, func(t *testing.T) {
			var submits, polls, cancels atomic.Int32
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/cancel") {
					cancels.Add(1)
					if r.URL.Path != endpoint+"/write-1/cancel" {
						t.Error("cancel escaped original handle")
					}
					fmt.Fprint(w, `{}`)
					return
				}
				if r.Method == http.MethodPost {
					submits.Add(1)
					var body struct {
						SQL        string            `json:"statement"`
						Parameters map[string]string `json:"parameters"`
					}
					if json.NewDecoder(r.Body).Decode(&body) != nil || body.SQL != "INSERT INTO events VALUES (1)" || body.Parameters["MULTI_STATEMENT_COUNT"] != "1" || body.Parameters["AUTOCOMMIT"] != "true" || r.URL.Query().Get("requestId") == "" || r.URL.Query().Get("async") != "true" {
						t.Error("unsafe write submission")
					}
					if scenario == "lost_submit" {
						w.Header().Set("Content-Length", "10000")
						fmt.Fprint(w, `{`)
						return
					}
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprint(w, `{"statementHandle":"write-1","code":"333334"}`)
					return
				}
				polls.Add(1)
				if r.Method != http.MethodGet || r.URL.Path != endpoint+"/write-1" {
					t.Error("poll escaped original handle")
				}
				switch scenario {
				case "changed_handle":
					fmt.Fprint(w, `{"statementHandle":"other","code":"090001"}`)
				case "failed":
					fmt.Fprint(w, `{"statementHandle":"write-1","code":"000001"}`)
				case "multiple":
					fmt.Fprint(w, `{"statementHandle":"write-1","code":"090001","statementHandles":["other"]}`)
				default:
					fmt.Fprint(w, `{"statementHandle":"write-1","code":"090001"}`)
				}
			})
			if scenario == "cancelled" {
				e.limits.Timeout = 30 * time.Millisecond
			}
			count, err := e.ApplyStatement(context.Background(), "INSERT INTO events VALUES (1)")
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
