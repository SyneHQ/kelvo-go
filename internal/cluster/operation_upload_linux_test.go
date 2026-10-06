//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/operationrun"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func newOperationUploadFixture(t *testing.T) (operationHTTPFixture, operations.InputUploadClaims, []byte) {
	t.Helper()
	f := newOperationHTTPFixture(t)
	principal := f.policy.Access.Principals["api"]
	principal.Operations = append(principal.Operations, operations.IngestionCommit)
	f.policy.Access.Principals["api"] = principal
	authority, _ := authorityForPrincipal(f.policy, "api")
	f.context = context.WithValue(f.context, jobAuthorityKey{}, authority)
	payload := []byte(`{"version":1,"fixture":"sealed source input"}`)
	sum := sha256.Sum256(payload)
	c := f.claims
	claims := operations.InputUploadClaims{Version: operations.InputUploadVersion, Issuer: c.Issuer, Audience: c.Audience,
		ClusterTenant: c.ClusterTenant, ServicePrincipal: c.ServicePrincipal, AppTeam: c.AppTeam, Subject: c.Subject,
		ID: "upload-1", IssuedAt: c.IssuedAt, ExpiresAt: c.ExpiresAt, ConnectionID: c.ConnectionID,
		SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(payload)), Format: "ingestion_batch_v1"}
	return f, claims, payload
}

func uploadOperationInput(t *testing.T, f operationHTTPFixture, claims operations.InputUploadClaims, payload []byte) operations.InputRef {
	t.Helper()
	token, err := operations.SignInputUploadGrant(claims, f.key)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/operation-inputs", bytes.NewReader(payload)).WithContext(f.context)
	r.Header.Set("Content-Type", "application/octet-stream")
	r.Header.Set(operationInputGrantHeader, token)
	w := httptest.NewRecorder()
	f.g.serveOperationUpload(w, r, "team-a")
	var ref operations.InputRef
	if w.Code != http.StatusCreated || operations.DecodeStrict(w.Body.Bytes(), &ref, 4096) != nil || ref.Validate() != nil {
		t.Fatalf("sealed upload failed: %d", w.Code)
	}
	return ref
}

func bindBulkOperation(t *testing.T, f *operationHTTPFixture, ref operations.InputRef) {
	t.Helper()
	f.request = operations.Request{Version: operations.Version, Kind: operations.IngestionCommit, Connection: f.request.Connection,
		IdempotencyKey: "batch-1", Spec: operations.Spec{Ingestion: &operations.IngestionSpec{Scope: operations.IngestionScope{SourceID: "source-a", Stream: "events", Binding: strings.Repeat("a", 64)}, BatchID: "batch-a", Input: &ref}}}
	f.claims.Operation = f.request.Kind
	var err error
	f.claims.RequestSHA256, err = operations.Digest(f.request)
	if err != nil {
		t.Fatal(err)
	}
	f.grant, err = operations.SignGrant(f.claims, f.key)
	if err != nil {
		t.Fatal(err)
	}
}

func TestOperationUploadSealsScopeAndRequiresSeparateExecutionGrant(t *testing.T) {
	f, claims, payload := newOperationUploadFixture(t)
	ref := uploadOperationInput(t, f, claims, payload)
	if ref.SHA256 != claims.SHA256 || ref.Bytes != claims.Bytes || ref.Format != claims.Format {
		t.Fatal("upload reference differs from signed content")
	}
	bound := operationBulkIdentity(operationUploadScope(claims), ref)
	loaded, err := f.state.inputs.Load(f.context, bound, ref)
	if err != nil || !bytes.Equal(loaded, payload) {
		t.Fatal("sealed input differs", err)
	}
	bindBulkOperation(t, &f, ref)
	token, _ := operations.SignInputUploadGrant(claims, f.key)
	body, _ := operations.Encode(f.request)
	if w := f.call(http.MethodPost, "/v1/operations", body, token); w.Code != 403 {
		t.Fatal("upload authority executed operation", w.Code)
	}
	f.submit(t)
	foreign := f.claims
	foreign.AppTeam = "other-customer"
	foreign.ID = "foreign-team"
	grant, _ := operations.SignGrant(foreign, f.key)
	if w := f.call(http.MethodPost, "/v1/operations", body, grant); w.Code != 400 {
		t.Fatal("foreign operation bound another team's bulk data", w.Code)
	}
	for name, mutate := range map[string]func(*ledger.Scope){
		"issuer": func(s *ledger.Scope) { s.Issuer = "other" }, "tenant": func(s *ledger.Scope) { s.ClusterTenant = "other" },
		"principal": func(s *ledger.Scope) { s.ServicePrincipal = "other" }, "team": func(s *ledger.Scope) { s.AppTeam = "other" },
		"subject-kind": func(s *ledger.Scope) { s.SubjectKind = "user" }, "subject": func(s *ledger.Scope) { s.SubjectID = "other" },
		"job": func(s *ledger.Scope) { s.SubjectJobID = "other" }, "connection": func(s *ledger.Scope) { s.ConnectionID = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			scope := operationUploadScope(claims)
			mutate(&scope)
			if _, err := f.state.inputs.Load(f.context, operationBulkIdentity(scope, ref), ref); err == nil {
				t.Fatal("foreign scope read sealed bytes")
			}
		})
	}
}

func TestOperationUploadRejectsInvalidContentAndRevokedPrincipal(t *testing.T) {
	for _, name := range []string{"wrong-hash", "short", "excess", "content-type", "duplicate-content-type", "compressed", "mixed-grants", "revoked", "draining"} {
		t.Run(name, func(t *testing.T) {
			f, claims, payload := newOperationUploadFixture(t)
			if name == "wrong-hash" {
				payload = bytes.Repeat([]byte("x"), len(payload))
			}
			if name == "short" {
				payload = payload[:len(payload)-1]
			}
			if name == "excess" {
				payload = append(payload, 'x')
			}
			token, _ := operations.SignInputUploadGrant(claims, f.key)
			r := httptest.NewRequest(http.MethodPost, "/v1/operation-inputs", bytes.NewReader(payload)).WithContext(f.context)
			r.Header.Set(operationInputGrantHeader, token)
			r.Header.Set("Content-Type", "application/octet-stream")
			if name == "content-type" {
				r.Header.Set("Content-Type", "application/json")
			}
			if name == "duplicate-content-type" {
				r.Header.Add("Content-Type", "application/json")
			}
			if name == "compressed" {
				r.Header.Set("Content-Encoding", "gzip")
			}
			if name == "mixed-grants" {
				r.Header.Set(operationGrantHeader, f.grant)
			}
			if name == "revoked" {
				delete(f.policy.Access.Principals, "api")
			}
			if name == "draining" {
				f.g.BeginDrain()
			}
			w := httptest.NewRecorder()
			f.g.serveOperationUpload(w, r, "team-a")
			if w.Code < 400 {
				t.Fatal("invalid upload accepted")
			}
		})
	}
}

func TestOperationBulkFetchRequiresRunningMTLSCustody(t *testing.T) {
	f, claims, payload := newOperationUploadFixture(t)
	ref := uploadOperationInput(t, f, claims, payload)
	bindBulkOperation(t, &f, ref)
	response := f.submit(t)
	scope := operationScope(f.claims)
	binding := ledger.Binding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}
	assigned, err := f.state.store.Claim(f.context, scope, response.ID, binding)
	if err != nil {
		t.Fatal(err)
	}
	serverFiles, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	workerFiles, _, _ := tlsFiles(t, WorkerIdentity("team-a", "worker-a"), ca, key)
	tlsConfig, err := OperationInputTLS(serverFiles)
	if err != nil {
		t.Fatal(err)
	}
	var corrupt atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mode := corrupt.Load()
		if mode == 0 {
			f.g.OperationInputs().ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		body := payload
		switch mode {
		case 1:
			w.Header().Add("Content-Type", "application/json")
		case 2:
			body = bytes.Repeat([]byte("x"), len(payload))
		case 3:
			body = append(bytes.Clone(payload), 'x')
		case 4:
			w.Header().Set("Content-Encoding", "gzip")
		}
		_, _ = w.Write(body)
	}))
	server.TLS = tlsConfig
	server.StartTLS()
	defer server.Close()
	client, err := newOperationInputClient(server.URL, workerFiles, "team-a", operationrun.Config{WorkerID: "worker-a", Owner: binding.Owner, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	client.transport.TLSClientConfig.ServerName = "gateway.test"
	defer client.close()
	assumed := assigned.Record
	assumed.State = ledger.Running
	if got, err := client.loadBulk(f.context, assumed, f.request); err == nil || len(got) != 0 {
		t.Fatal("assigned worker fetched data before running")
	}
	running, err := f.state.store.Start(f.context, scope, response.ID, binding)
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.loadBulk(f.context, running.Record, f.request)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("running worker did not get exact sealed bytes", err)
	}
	for mode := int32(1); mode <= 4; mode++ {
		corrupt.Store(mode)
		if got, err := client.loadBulk(f.context, running.Record, f.request); err == nil || len(got) != 0 {
			t.Fatal("worker accepted corrupt bulk response", mode)
		}
	}
	corrupt.Store(0)
	body, _ := json.Marshal(operationInputRequest{Scope: scope, Binding: binding})
	r := httptest.NewRequest(http.MethodPost, "/v1/operations/team-a/"+response.ID+"/bulk-input", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.g.OperationInputs().ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("plaintext bulk disclosure", w.Code)
	}
	f.cluster.lease = func(ctx context.Context, _, _ string) (time.Time, error) {
		_, err := f.state.store.Cancel(ctx, scope, response.ID)
		return time.Now().Add(time.Minute), err
	}
	if got, err := client.loadBulk(f.context, running.Record, f.request); err == nil || len(got) != 0 {
		t.Fatal("cancelled worker received sealed bytes")
	}
}
