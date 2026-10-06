//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/operationrun"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestOperationWatchUploadRequiresWatchAuthorityAndRunningCustody(t *testing.T) {
	f, claims, payload := newOperationUploadFixture(t)
	claims.Format = "watch_checkpoint_v1"
	token, err := operations.SignInputUploadGrant(claims, f.key)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/operation-inputs", bytes.NewReader(payload)).WithContext(f.context)
	r.Header.Set(operationInputGrantHeader, token)
	r.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	f.g.serveOperationUpload(w, r, "team-a")
	if w.Code != http.StatusForbidden {
		t.Fatal("ingestion-only principal uploaded a watch checkpoint", w.Code)
	}
	principal := f.policy.Access.Principals["api"]
	principal.Operations = []operations.Kind{operations.WatchAck}
	f.policy.Access.Principals["api"] = principal
	authority, _ := authorityForPrincipal(f.policy, "api")
	f.context = context.WithValue(f.context, jobAuthorityKey{}, authority)
	ref := uploadOperationInput(t, f, claims, payload)
	f.request = operations.Request{Version: operations.Version, Kind: operations.WatchAck, Connection: f.request.Connection, IdempotencyKey: "watch-ack-1",
		Spec: operations.Spec{Watch: &operations.WatchSpec{ID: "watch-a", Generation: "generation-a", Target: operations.ObjectRef{Name: "events"}, Mode: "poll", Checkpoint: &ref, SinkReceiptSHA256: strings.Repeat("a", 64)}}}
	f.claims.Operation = f.request.Kind
	f.claims.RequestSHA256, err = operations.Digest(f.request)
	if err != nil {
		t.Fatal(err)
	}
	f.grant, err = operations.SignGrant(f.claims, f.key)
	if err != nil {
		t.Fatal(err)
	}
	response := f.submit(t)
	scope := operationScope(f.claims)
	binding := ledger.Binding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}
	if _, err := f.state.store.Claim(f.context, scope, response.ID, binding); err != nil {
		t.Fatal(err)
	}
	running, err := f.state.store.Start(f.context, scope, response.ID, binding)
	if err != nil {
		t.Fatal(err)
	}
	serverFiles, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	workerFiles, _, _ := tlsFiles(t, WorkerIdentity("team-a", "worker-a"), ca, key)
	tlsConfig, err := OperationInputTLS(serverFiles)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(f.g.OperationInputs())
	server.TLS = tlsConfig
	server.StartTLS()
	defer server.Close()
	client, err := newOperationInputClient(server.URL, workerFiles, "team-a", operationrun.Config{WorkerID: "worker-a", Owner: binding.Owner, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	client.transport.TLSClientConfig.ServerName = "gateway.test"
	defer client.close()
	got, err := client.loadBulk(f.context, running.Record, f.request)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("admitted watch lost sealed checkpoint", err)
	}
	// A reference cannot be relabelled to another operation's format even when
	// its digest, owner and byte length are unchanged.
	changed := ref
	changed.Format = "ingestion_batch_v1"
	if _, err := f.state.inputs.Load(f.context, operationBulkIdentity(scope, changed), changed); err == nil {
		t.Fatal("watch checkpoint reused as an ingestion batch")
	}
}
