// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestOperationClientValidatesOrderedBatchReceiptsAtSubmissionAndPolling(t *testing.T) {
	for _, poll := range []bool{false, true} {
		for _, corrupt := range []bool{false, true} {
			request := operationFixtureRequest()
			request.Spec.Statement.SQL = ""
			request.Spec.Statement.Batch = &operations.BatchSpec{Statements: []operations.BoundStatement{{SQL: "UPDATE samples SET active=true WHERE id=1"}, {SQL: "DELETE FROM samples WHERE id=2"}}}
			completed := operationFixtureStatus(t, request, string(operations.Completed))
			for i, statement := range request.Spec.Statement.Batch.Statements {
				digest, err := operations.StatementDigest(statement)
				if err != nil {
					t.Fatal(err)
				}
				completed.Receipt.Steps = append(completed.Receipt.Steps, operations.StepReceipt{Index: i, SHA256: digest, Effect: operations.EffectCommitted})
			}
			if corrupt {
				completed.Receipt.Steps[1].SHA256 = completed.Receipt.Steps[0].SHA256
			}
			var submissions atomic.Int64
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					submissions.Add(1)
					if poll {
						writeOperationStatus(w, operationFixtureStatus(t, request, "queued"), http.StatusCreated)
						return
					}
				}
				writeOperationStatus(w, completed, http.StatusOK)
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			grant := operationFixtureGrant(t, request)
			var status operations.Response
			var err error
			if poll {
				status, err = c.ExecuteOperation(context.Background(), request, Authority{OperationGrant: grant})
			} else {
				status, err = c.SubmitOperation(context.Background(), request, Authority{OperationGrant: grant})
			}
			if corrupt {
				assertOperationUncertain(t, err, completed.ID, completed.RequestSHA256)
			} else if err != nil || status.Receipt == nil || len(status.Receipt.Steps) != 2 {
				t.Fatalf("valid batch rejected: %+v %v", status, err)
			}
			if submissions.Load() != 1 {
				t.Fatal("batch was replayed")
			}
		}
	}
}
