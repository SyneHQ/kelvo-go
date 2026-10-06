//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/ingestion"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type operationIngestionLive struct {
	ctx                  context.Context
	client               *http.Client
	url, token, database string
	key                  ed25519.PrivateKey
	sign                 func(operations.Request, string) string
	submit               func(operations.Request, string) operations.Response
	await                func(string, string, operations.Request) operations.Response
	call                 func(string, string, string, *operations.Request) (int, []byte, error)
}

func runOperationIngestionLive(t *testing.T, h operationIngestionLive) {
	t.Helper()
	connection := operations.ConnectionRef{ID: "postgres-fixture", Database: h.database, Schema: "kelvo_ingestion_fixture"}
	scope := operations.IngestionScope{SourceID: "ingestion-fixture", Stream: "events", Binding: strings.Repeat("c", 64)}
	request := operations.Request{Version: operations.Version, Kind: operations.IngestionInstall, Connection: connection, IdempotencyKey: "install-ingestion-fixture", Spec: operations.Spec{Ingestion: &operations.IngestionSpec{Scope: scope}}}
	run := func(r operations.Request) (operations.Response, string) {
		grant := h.sign(r, "customer-a")
		result := h.await(h.submit(r, grant).ID, grant, r)
		return result, grant
	}
	download := func(result operations.Response, grant string) []byte {
		t.Helper()
		if result.Receipt == nil || result.Receipt.Outcome != operations.Completed || result.Receipt.Result == nil {
			t.Fatalf("ingestion result unavailable: %#v", result.Receipt)
		}
		status, raw, err := h.call(http.MethodGet, "/v1/operations/"+result.ID+"/results", grant, nil)
		hash := sha256.Sum256(raw)
		if err != nil || status != 200 || int64(len(raw)) != result.Receipt.Result.Bytes || hex.EncodeToString(hash[:]) != result.Receipt.Result.SHA256 {
			t.Fatal("ingestion result custody failed", status, err)
		}
		reader, err := ipc.NewReader(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Release()
		if !reader.Next() || reader.RecordBatch().NumRows() != 1 || reader.RecordBatch().NumCols() != 1 || reader.Schema().Field(0).Name != "ingestion" {
			t.Fatal("invalid ingestion result schema")
		}
		column, ok := reader.RecordBatch().Column(0).(*array.Binary)
		if !ok || column.IsNull(0) {
			t.Fatal("invalid ingestion result type")
		}
		value := append([]byte(nil), column.Value(0)...)
		if reader.Next() || reader.Err() != nil {
			t.Fatal("unexpected ingestion result rows")
		}
		return value
	}
	installed, _ := run(request)
	if installed.Receipt == nil || installed.Receipt.Outcome != operations.Completed || installed.Receipt.Effect != operations.EffectCommitted {
		t.Fatalf("ingestion install failed: %#v", installed.Receipt)
	}
	request.Kind, request.IdempotencyKey = operations.IngestionState, ""
	stateResult, stateGrant := run(request)
	empty, err := ingestion.ParseState(download(stateResult, stateGrant))
	if err != nil || empty.Sequence != 0 || empty.LastReceipt != nil {
		t.Fatal("new destination has unexpected state", err)
	}
	batch := ingestion.Batch{ID: "fixture-batch-1", RunID: "fixture-run-1", ObservedAt: "2026-10-06T00:00:00Z",
		Records: []ingestion.Record{{ID: "row-1", Payload: json.RawMessage(`{"amount":12345678901234567890.123,"label":"exact"}`)}}, Checkpoint: json.RawMessage(`{"cursor":"page-1"}`)}
	upload := func(batch ingestion.Batch) operations.InputRef {
		t.Helper()
		raw, err := json.Marshal(batch)
		if err != nil {
			t.Fatal(err)
		}
		return h.upload(t, connection.ID, "ingestion_batch_v1", raw)
	}
	ref := upload(batch)
	request.Kind, request.IdempotencyKey = operations.IngestionCommit, "ingestion-first-commit"
	request.Spec.Ingestion = &operations.IngestionSpec{Scope: scope, BatchID: batch.ID, Input: &ref}
	committed, commitGrant := run(request)
	first, err := ingestion.ParseReceipt(download(committed, commitGrant))
	if err != nil || first.ValidateBatch(batch) != nil || committed.Receipt.Effect != operations.EffectCommitted {
		t.Fatal("ingestion commit receipt mismatch", err)
	}
	duplicate := h.submit(request, commitGrant)
	if duplicate.ID != committed.ID {
		t.Fatal("same operation acquired a second identity")
	}
	// A new source attempt with the identical batch must recover its original
	// transactional receipt, including the original commit timestamp.
	request.IdempotencyKey = "ingestion-reconcile-same-batch"
	retried, retryGrant := run(request)
	again, err := ingestion.ParseReceipt(download(retried, retryGrant))
	if err != nil || again != first {
		t.Fatal("source retry duplicated or changed ingestion receipt", err)
	}
	batch.Records[0].Payload = json.RawMessage(`{"amount":0}`)
	changedRef := upload(batch)
	request.IdempotencyKey = "ingestion-reject-changed-batch"
	request.Spec.Ingestion.Input = &changedRef
	conflict, _ := run(request)
	if conflict.Receipt == nil || conflict.Receipt.Outcome != operations.Failed || conflict.Receipt.Effect != operations.EffectNone || conflict.Receipt.ErrorCode != "CONFLICT" {
		t.Fatalf("changed batch was not rejected: %#v", conflict.Receipt)
	}
	request.Kind, request.IdempotencyKey = operations.IngestionState, ""
	request.Spec.Ingestion = &operations.IngestionSpec{Scope: scope}
	stateResult, stateGrant = run(request)
	state, err := ingestion.ParseState(download(stateResult, stateGrant))
	if err != nil || state.Sequence != 1 || state.LastReceipt == nil || *state.LastReceipt != first || !bytes.Equal(bytes.ReplaceAll(state.Checkpoint, []byte(" "), nil), []byte(`{"cursor":"page-1"}`)) {
		t.Fatal("checkpoint advanced without a matching receipt", err)
	}
}

func (h operationIngestionLive) upload(t *testing.T, connectionID, format string, raw []byte) operations.InputRef {
	t.Helper()
	hash := sha256.Sum256(raw)
	now := time.Now()
	claims := operations.InputUploadClaims{Version: operations.InputUploadVersion, Issuer: "fixture-gateway", Audience: "operations", ClusterTenant: "a", ServicePrincipal: "api", AppTeam: "customer-a",
		Subject: operations.Subject{Kind: "api_key", ID: "fixture-key"}, ID: "ingestion-upload", IssuedAt: now.Unix(), ExpiresAt: now.Add(120 * time.Second).Unix(),
		ConnectionID: connectionID, Format: format, SHA256: hex.EncodeToString(hash[:]), Bytes: int64(len(raw))}
	grant, err := operations.SignInputUploadGrant(claims, h.key)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(h.ctx, http.MethodPost, h.url+"/v1/operation-inputs", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.GetBody = nil
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set(operationInputGrantHeader, grant)
	req.Header.Set("Content-Type", "application/octet-stream")
	response, err := h.client.Do(req)
	if err != nil {
		t.Fatal("input upload failed")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	var ref operations.InputRef
	if err != nil || response.StatusCode != http.StatusCreated || operations.DecodeStrict(body, &ref, 4096) != nil || ref.Validate() != nil || ref.SHA256 != claims.SHA256 || ref.Bytes != claims.Bytes || ref.Format != claims.Format {
		t.Fatal("sealed input response invalid", response.StatusCode)
	}
	return ref
}
