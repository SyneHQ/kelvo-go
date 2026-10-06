// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package bigquery

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

func TestChangeBoundJobAndExactAcknowledgement(t *testing.T) {
	for _, scenario := range []string{"success", "ddl", "changed_identity", "failed", "lost_submit", "cancelled", "invalid_count"} {
		t.Run(scenario, func(t *testing.T) {
			var ref jobRef
			var submits, polls, cancels atomic.Int32
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/cancel") {
					cancels.Add(1)
					if !strings.HasSuffix(r.URL.Path, "/jobs/"+ref.ID+"/cancel") || r.URL.Query().Get("location") != "us" {
						t.Error("cancel escaped original job")
					}
					fmt.Fprint(w, `{}`)
					return
				}
				if r.Method == http.MethodPost {
					submits.Add(1)
					var body struct {
						Ref    jobRef `json:"jobReference"`
						Config struct {
							Query struct {
								SQL    string `json:"query"`
								Legacy bool   `json:"useLegacySql"`
								Billed string `json:"maximumBytesBilled"`
							} `json:"query"`
							Timeout string `json:"jobTimeoutMs"`
						} `json:"configuration"`
					}
					if json.NewDecoder(r.Body).Decode(&body) != nil || body.Config.Query.SQL != "UPDATE events SET active=true" || body.Config.Query.Legacy || body.Config.Query.Billed != "1000000" || body.Config.Timeout == "" || body.Ref.Project != "test-project" || body.Ref.Location != "us" || !strings.HasPrefix(body.Ref.ID, "kelvo_") {
						t.Error("unsafe job submission")
					}
					ref = body.Ref
					if scenario == "lost_submit" {
						w.Header().Set("Content-Length", "10000")
						fmt.Fprint(w, `{`)
						return
					}
					json.NewEncoder(w).Encode(map[string]any{"jobReference": ref, "status": map[string]string{"state": "RUNNING"}})
					return
				}
				polls.Add(1)
				if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/jobs/"+ref.ID) || r.URL.Query().Get("location") != "us" {
					t.Error("poll escaped original job")
				}
				response := changeJob{Ref: ref}
				response.Status.State = "DONE"
				response.Statistics.Query.Affected = "9007199254740993"
				switch scenario {
				case "ddl":
					response.Statistics.Query.Affected = ""
				case "changed_identity":
					response.Ref.Project = "foreign-project"
				case "failed":
					response.Status.Error = map[string]string{"message": "private source details"}
				case "invalid_count":
					response.Statistics.Query.Affected = "1.5"
				}
				json.NewEncoder(w).Encode(response)
			})
			if scenario == "cancelled" {
				e.limits.Timeout = 30 * time.Millisecond
			}
			count, err := e.ApplyStatement(context.Background(), "UPDATE events SET active=true")
			wantSuccess := scenario == "success" || scenario == "ddl" || scenario == "invalid_count"
			if (err == nil) != wantSuccess || submits.Load() != 1 {
				t.Fatalf("err=%v submits=%d", err, submits.Load())
			}
			if scenario == "success" && (count == nil || *count != 9007199254740993) {
				t.Fatal("affected count lost precision")
			}
			if scenario == "ddl" && count != nil {
				t.Fatal("invented DDL count")
			}
			if err != nil && strings.Contains(err.Error(), "private source details") {
				t.Fatal("source error leaked")
			}
			wantCancel := int32(1)
			if wantSuccess {
				wantCancel = 0
			}
			if cancels.Load() != wantCancel || polls.Load() > 1 {
				t.Fatalf("polls=%d cancels=%d", polls.Load(), cancels.Load())
			}
		})
	}
}
