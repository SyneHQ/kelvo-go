// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/SYNEHQ/kelvo-go/ingestion"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestInternalJobCleanupIsBoundBeforeSourceResolution(t *testing.T) {
	for _, kind := range []string{"ingestion", "watcher", "user", "api_key", "scheduled_job", ""} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			failed := errors.New("cleanup receipt unavailable")
			sink := &operationResultSink{}
			bindOperationCleanup(sink, operations.GrantClaims{Authorization: operations.Authorization{Kind: kind}}, func(actual context.Context) error {
				calls++
				if actual != ctx {
					t.Error("cleanup context replaced")
				}
				return failed
			})
			if kind == "ingestion" || kind == "watcher" {
				if err := sink.OperationCleaned(ctx); !errors.Is(err, failed) || calls != 1 {
					t.Fatal("internal job cleanup acknowledgement lost", err, calls)
				}
			} else if err := sink.OperationCleaned(ctx); err != nil || calls != 0 {
				t.Fatal("ordinary operation notified internal job custody", err, calls)
			}
		})
	}
}

func TestOperationIngestionRunRequiresExactAuthenticatedRun(t *testing.T) {
	batch := ingestion.Batch{ID: "batch-a", RunID: "run-a", ObservedAt: "2026-10-06T00:00:00Z",
		Records: []ingestion.Record{{ID: "row-a", Payload: json.RawMessage(`{"value":1}`)}}, Checkpoint: json.RawMessage(`{"cursor":1}`)}
	payload, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	request := operations.Request{Kind: operations.IngestionCommit}
	claims := operations.GrantClaims{Authorization: operations.Authorization{Kind: "ingestion", Ingestion: &operations.IngestionAuthority{RunID: "run-a"}}}
	if err := authorizeOperationIngestionRun(claims, request, payload); err != nil {
		t.Fatal("matching authenticated run rejected", err)
	}
	claims.Authorization.Ingestion.RunID = "another-run"
	if authorizeOperationIngestionRun(claims, request, payload) == nil {
		t.Fatal("one ingestion run authorized another batch")
	}
	claims.Authorization.Ingestion.RunID = "run-a"
	if authorizeOperationIngestionRun(claims, request, []byte(`{"run_id":"run-a"}`)) == nil {
		t.Fatal("malformed batch accepted before credential resolution")
	}
	claims.Authorization.Ingestion = nil
	if authorizeOperationIngestionRun(claims, request, payload) == nil {
		t.Fatal("missing ingestion proof accepted")
	}
}
