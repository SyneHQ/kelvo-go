//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	"github.com/SYNEHQ/kelvo-go/internal/operationrun"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type operationMemoryBackend struct {
	mu       sync.Mutex
	values   map[string]ledger.Entry
	revision uint64
}

func (b *operationMemoryBackend) Get(ctx context.Context, key string) (ledger.Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ledger.Entry{}, err
	}
	e, ok := b.values[key]
	if !ok {
		return e, ledger.ErrMissing
	}
	e.Value = bytes.Clone(e.Value)
	return e, nil
}
func (b *operationMemoryBackend) Create(ctx context.Context, key string, data []byte) (uint64, error) {
	return b.write(ctx, key, data, 0)
}
func (b *operationMemoryBackend) Update(ctx context.Context, key string, data []byte, revision uint64) (uint64, error) {
	return b.write(ctx, key, data, revision)
}
func (b *operationMemoryBackend) write(ctx context.Context, key string, data []byte, revision uint64) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if b.values[key].Revision != revision {
		return 0, ledger.ErrRevision
	}
	b.revision++
	b.values[key] = ledger.Entry{Value: bytes.Clone(data), Revision: b.revision}
	return b.revision, nil
}

type operationClusterFixture struct {
	Store
	policy Policy
	lease  func(context.Context, string, string) (time.Time, error)
}

func (s *operationClusterFixture) Policy() Policy { return s.policy }
func (s *operationClusterFixture) WorkerLease(ctx context.Context, id, owner string) (time.Time, error) {
	return s.lease(ctx, id, owner)
}

type operationHTTPFixture struct {
	g       *Gateway
	state   *gatewayOperations
	cluster *operationClusterFixture
	policy  Policy
	request operations.Request
	claims  operations.GrantClaims
	grant   string
	key     ed25519.PrivateKey
	context context.Context
}

func newOperationHTTPFixture(t *testing.T) operationHTTPFixture {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	policy := Policy{TenantID: "team-a", LeaseDuration: 5 * time.Second, Operations: &OperationPolicy{Shards: 8, SlotsPerShard: 8, Retention: time.Hour, ExecutionTimeout: time.Minute},
		Workers: map[string]int{"worker-a": 2}, Access: &PrincipalPolicy{Revision: 1, Principals: map[string]PrincipalGrant{"api": {Kind: "service", Operations: []operations.Kind{operations.StatementExecute},
			DelegatedResolver: &ResolverTrust{Issuer: "gateway", Audience: "operations", URL: "https://resolver.test/internal/kelvo/resolve", PublicKey: base64.StdEncoding.EncodeToString(pub)}}}}}
	store, err := ledger.New(&operationMemoryBackend{values: map[string]ledger.Entry{}}, operationStorePolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := operationinput.Open(operationinput.Config{MaxInputBytes: operations.MaxRequestBytes, Storage: exports.Config{Directory: filepath.Join(t.TempDir(), "inputs"), Tenant: "team-a", MaxEntries: 64, MaxStoredBytes: 128 << 20, MaxTTL: 6 * time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := inputs.Close(); err != nil {
			t.Error(err)
		}
	})
	state := &gatewayOperations{store: store, inputs: inputs}
	cluster := &operationClusterFixture{policy: policy, lease: func(context.Context, string, string) (time.Time, error) { return time.Now().Add(time.Minute), nil }}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g := &Gateway{ctx: ctx, tenants: map[string]gatewayTenant{"team-a": {store: cluster}}, operations: map[string]*gatewayOperations{"team-a": state}, permits: make(chan struct{}, 8)}
	request := operations.Request{Version: 1, Kind: operations.StatementExecute, Connection: operations.ConnectionRef{ID: "saved-a", Database: "example"}, IdempotencyKey: "write-1",
		Spec: operations.Spec{Statement: &operations.StatementSpec{SQL: "UPDATE example SET value = 1", Transaction: operations.TransactionRequired}}}
	digest, err := operations.Digest(request)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	claims := operations.GrantClaims{Version: 2, Issuer: "gateway", Audience: "operations", ClusterTenant: "team-a", ServicePrincipal: "api", AppTeam: "customer-a", Subject: operations.Subject{Kind: "api_key", ID: "key-a"}, ID: "grant-1", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix(), ConnectionID: "saved-a", Operation: request.Kind, RequestSHA256: digest, Authorization: operations.Authorization{Kind: "api_key"}}
	grant, err := operations.SignGrant(claims, key)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := authorityForPrincipal(policy, "api")
	return operationHTTPFixture{g: g, state: state, cluster: cluster, policy: policy, request: request, claims: claims, grant: grant, key: key, context: context.WithValue(ctx, jobAuthorityKey{}, a)}
}
func (f operationHTTPFixture) call(method, path string, body []byte, grant string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, bytes.NewReader(body)).WithContext(f.context)
	r.Header.Set(operationGrantHeader, grant)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.g.serveOperationHTTP(w, r, "team-a", strings.Split(strings.Trim(path, "/"), "/"))
	return w
}
func (f operationHTTPFixture) submit(t *testing.T) operations.Response {
	t.Helper()
	body, err := operations.Encode(f.request)
	if err != nil {
		t.Fatal(err)
	}
	w := f.call(http.MethodPost, "/v1/operations", body, f.grant)
	if w.Code != http.StatusAccepted {
		t.Fatalf("submit %d: %s", w.Code, w.Body.String())
	}
	var response operations.Response
	if operations.DecodeStrict(w.Body.Bytes(), &response, operations.MaxReceiptBytes+4096) != nil || response.Validate() != nil {
		t.Fatal("invalid submit response")
	}
	return response
}

func TestOperationHTTPDuplicateIdentityAndStatusNeverStart(t *testing.T) {
	f := newOperationHTTPFixture(t)
	first := f.submit(t)
	second := f.submit(t)
	if first.ID != second.ID || first.State != ledger.Queued {
		t.Fatal("duplicate created another attempt")
	}
	for range 3 {
		w := f.call(http.MethodGet, "/v1/operations/"+first.ID, nil, f.grant)
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
		state, err := f.state.store.Get(f.context, operationScope(f.claims), first.ID)
		if err != nil || state.Record.State != ledger.Queued {
			t.Fatal("status started operation")
		}
	}
	changed := f.request
	statement := *changed.Spec.Statement
	statement.SQL = "DELETE FROM example"
	changed.Spec.Statement = &statement
	body, _ := operations.Encode(changed)
	if w := f.call(http.MethodPost, "/v1/operations", body, f.grant); w.Code != 403 {
		t.Fatalf("changed body accepted: %d", w.Code)
	}
	claims := f.claims
	claims.RequestSHA256, _ = operations.Digest(changed)
	claims.ID = "grant-2"
	grant, _ := operations.SignGrant(claims, f.key)
	if w := f.call(http.MethodPost, "/v1/operations", body, grant); w.Code != 409 {
		t.Fatalf("idempotency collision accepted: %d", w.Code)
	}
}

func TestOperationHTTPForeignTeamAndReadGrantDenied(t *testing.T) {
	f := newOperationHTTPFixture(t)
	first := f.submit(t)
	claims := f.claims
	claims.AppTeam = "customer-b"
	claims.ID = "other-team"
	grant, _ := operations.SignGrant(claims, f.key)
	if w := f.call(http.MethodGet, "/v1/operations/"+first.ID, nil, grant); w.Code != 404 {
		t.Fatalf("foreign team read %d", w.Code)
	}
	policy := f.policy
	policy.Access = &PrincipalPolicy{Revision: 2, Principals: map[string]PrincipalGrant{}}
	if _, err := authorizeOperationEnvelope(f.context, policy, f.grant); err == nil {
		t.Fatal("revoked principal accepted")
	}
	f.g.BeginDrain()
	body, _ := operations.Encode(f.request)
	if w := f.call(http.MethodPost, "/v1/operations", body, f.grant); w.Code != 503 {
		t.Fatal("draining gateway admitted operation")
	}
}

func TestOperationLeaseRechecksCancellationAfterWorkerLookup(t *testing.T) {
	f := newOperationHTTPFixture(t)
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
	if _, err := currentOperationLease(f.context, f.cluster, f.state.store, running.Record, binding); err != nil {
		t.Fatal(err)
	}
	f.cluster.lease = func(ctx context.Context, _, _ string) (time.Time, error) {
		_, err := f.state.store.Cancel(ctx, scope, response.ID)
		return time.Now().Add(time.Minute), err
	}
	if _, err := currentOperationLease(f.context, f.cluster, f.state.store, running.Record, binding); !errors.Is(err, ledger.ErrConflict) {
		t.Fatalf("cancelled lease allowed: %v", err)
	}
}

func TestOperationInputRequiresRealMTLSAndCurrentAssignedCustody(t *testing.T) {
	f := newOperationHTTPFixture(t)
	response := f.submit(t)
	scope := operationScope(f.claims)
	binding := ledger.Binding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}
	if _, err := f.state.store.Claim(f.context, scope, response.ID, binding); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(operationInputRequest{Scope: scope, Binding: binding})
	path := "/v1/operations/team-a/" + response.ID + "/input"
	untrusted := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	untrusted.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.g.OperationInputs().ServeHTTP(w, untrusted)
	if w.Code != 403 {
		t.Fatalf("plaintext input allowed: %d", w.Code)
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
	inputClient, err := newOperationInputClient(server.URL, workerFiles, "team-a", operationrun.Config{WorkerID: "worker-a", Owner: binding.Owner, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	transport := inputClient.transport
	transport.TLSClientConfig.ServerName = "gateway.test"
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	send := func() (*http.Response, []byte) {
		t.Helper()
		r, _ := http.NewRequest(http.MethodPost, server.URL+path, bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return resp, data
	}
	resp, data := send()
	canonical, _ := operations.Encode(f.request)
	if resp.StatusCode != 200 || !bytes.Equal(data, canonical) {
		t.Fatalf("sealed request delivery failed: %d", resp.StatusCode)
	}
	if _, err := f.state.store.Cancel(f.context, scope, response.ID); err != nil {
		t.Fatal(err)
	}
	resp, _ = send()
	if resp.StatusCode != 409 {
		t.Fatalf("cancelled input disclosed: %d", resp.StatusCode)
	}
}

func TestOperationPolicyCloneDetachesGrantAndResolver(t *testing.T) {
	f := newOperationHTTPFixture(t)
	copy, err := clonePolicy(f.policy)
	if err != nil {
		t.Fatal(err)
	}
	copy.Operations.Retention = 2 * time.Hour
	grant := copy.Access.Principals["api"]
	grant.Operations[0] = operations.NativeExecute
	grant.DelegatedResolver.URL = "https://changed.test/internal/kelvo/resolve"
	if f.policy.Operations.Retention != time.Hour || f.policy.Access.Principals["api"].Operations[0] != operations.StatementExecute || f.policy.Access.Principals["api"].DelegatedResolver.URL != "https://resolver.test/internal/kelvo/resolve" {
		t.Fatal("mutable policy aliases retained")
	}
}
