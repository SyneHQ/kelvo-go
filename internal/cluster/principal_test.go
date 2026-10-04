// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/nats-io/nats.go/jetstream"
	"go.yaml.in/yaml/v3"
)

func principalTestPolicy() Policy {
	p := testPolicy()
	p.Access = &PrincipalPolicy{Revision: 7, Principals: map[string]PrincipalGrant{
		"analyst": {Kind: "user", NativeSources: []string{"sales_native"}, FederatedSources: []string{"sales", "daily_sales"}},
		"reports": {Kind: "service", FederatedSources: []string{"daily_sales"}, AllowLiteralQueries: true},
	}}
	return p
}

func principalKeySet(t *testing.T, revision uint64, a, b map[string][]string) gatewayKeySet {
	t.Helper()
	raw, err := yaml.Marshal(gatewayKeyDocument{Version: 2, Revision: revision, Principals: map[string]map[string][]string{"a": a, "b": b}})
	if err != nil {
		t.Fatal(err)
	}
	set, err := parseGatewayKeys(raw, map[string]bool{"a": true, "b": true}, 1)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestPrincipalGrantsAndPolicyVersionFailClosed(t *testing.T) {
	p := principalTestPolicy()
	if err := ValidatePolicy(p); err != nil {
		t.Fatal(err)
	}
	a, ok := authorityForPrincipal(p, "analyst")
	if !ok || a.PrincipalKind != "user" || len(a.PolicyVersion) != 64 {
		t.Fatal("missing server authority")
	}
	tests := []struct {
		name    string
		r       query.Request
		allowed bool
	}{
		{"native grant", query.Request{Mode: "native", ConnectionID: "sales_native", SQL: "SELECT 1"}, true},
		{"native denied", query.Request{Mode: "native", ConnectionID: "payroll", SQL: "SELECT 1"}, false},
		{"federation grant", query.Request{Mode: "federated", Sources: []string{"sales", "daily_sales"}, SQL: "SELECT * FROM sales"}, true},
		{"unselected secret", query.Request{Mode: "federated", Sources: []string{"sales", "payroll"}, SQL: "SELECT * FROM sales"}, false},
		{"snapshot denied", query.Request{Mode: "federated", Sources: []string{"payroll_snapshot"}, SQL: "SELECT * FROM payroll_snapshot"}, false},
		{"literal denied", query.Request{Mode: "federated", SQL: "SELECT 1"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateJobAuthority(p, &a, tt.r)
			if (err == nil) != tt.allowed {
				t.Fatalf("allowed=%v error=%v", tt.allowed, err)
			}
		})
	}
	request := tests[0].r
	for _, forged := range []*JobAuthority{nil, {}, {PrincipalID: "analyst", PrincipalKind: "service", PolicyVersion: a.PolicyVersion}, {PrincipalID: "reports", PrincipalKind: "user", PolicyVersion: a.PolicyVersion}} {
		if validateJobAuthority(p, forged, request) == nil {
			t.Fatal("forged authority allowed")
		}
	}
	p.Access.Revision++
	if validateJobAuthority(p, &a, request) == nil {
		t.Fatal("old policy still authorized")
	}
	p.Access.Revision--
	p.TenantID = "b"
	if validateJobAuthority(p, &a, request) == nil {
		t.Fatal("authority crossed tenant")
	}
	legacy := testPolicy()
	if validateJobAuthority(legacy, &a, request) == nil {
		t.Fatal("principal job downgraded to legacy")
	}
}

func TestPrincipalPolicyRejectsUnboundedOrAmbiguousGrants(t *testing.T) {
	for _, mutate := range []func(*PrincipalPolicy){
		func(p *PrincipalPolicy) { p.Revision = 0 },
		func(p *PrincipalPolicy) { p.Principals["../user"] = PrincipalGrant{Kind: "user"} },
		func(p *PrincipalPolicy) { p.Principals["analyst"] = PrincipalGrant{Kind: "admin"} },
		func(p *PrincipalPolicy) {
			p.Principals["analyst"] = PrincipalGrant{Kind: "user", NativeSources: []string{"sales", "sales"}}
		},
		func(p *PrincipalPolicy) {
			p.Principals["analyst"] = PrincipalGrant{Kind: "user", FederatedSources: []string{"*"}}
		},
		func(p *PrincipalPolicy) {
			p.Principals["analyst"] = PrincipalGrant{Kind: "user", NativeSources: make([]string, 65)}
		},
	} {
		p := principalTestPolicy()
		mutate(p.Access)
		if ValidatePolicy(p) == nil {
			t.Fatal("invalid grant accepted")
		}
	}
	cfg := GatewayConfig{Tenants: []TenantConfig{{Policy: principalTestPolicy(), TokenEnv: "KELVO_TOKEN"}}}
	if validateGatewayAuthentication(&cfg) == nil {
		t.Fatal("tenant API token accepted with principal access")
	}
}

func TestPrincipalKeysCannotMoveWithinTenantOrAcrossTenants(t *testing.T) {
	a := bareAuthenticator(t)
	initial := principalKeySet(t, 1, map[string][]string{"analyst": {rotationOld}}, map[string][]string{"reports": {rotationOther}})
	if !a.apply(initial, time.Now()) {
		t.Fatal("principal keys rejected")
	}
	tenant, active, ok := a.lookup(rotationOld)
	if !ok || tenant != "a" || active.Value(keyPrincipalID{}) != "analyst" {
		t.Fatal("principal not derived from private key")
	}
	moved := principalKeySet(t, 2, map[string][]string{"reports": {rotationOld}}, map[string][]string{"reports": {rotationOther}})
	if a.apply(moved, time.Now()) || active.Err() == nil {
		t.Fatal("token reassigned principal")
	}
	if !a.apply(initial, time.Now()) {
		t.Fatal("same ownership failed recovery")
	}
	_, active, _ = a.lookup(rotationOld)
	revoked := principalKeySet(t, 3, map[string][]string{"analyst": {}}, map[string][]string{"reports": {rotationOther}})
	if !a.apply(revoked, time.Now()) || active.Err() == nil {
		t.Fatal("revocation did not fence in-flight context")
	}
}

func TestPrincipalKeyDocumentRejectsMixedAndMalformedBindings(t *testing.T) {
	cases := []string{
		"version: 2\nrevision: 1\nprincipals: {a: null, b: {}}\n",
		"version: 2\nrevision: 1\nprincipals: {a: {analyst: null}, b: {}}\n",
		"version: 2\nrevision: 1\nprincipals: {a: {admin/user: []}, b: {}}\n",
		"version: 2\nrevision: 1\nprincipals: {a: {}, b: {}}\ntenants: {a: [], b: []}\n",
		"version: 1\nrevision: 1\nprincipals: {a: {}, b: {}}\ntenants: {a: [], b: []}\n",
		"version: 2\nrevision: 1\nprincipals: {a: {analyst: [short]}, b: {}}\n",
		"version: 2\nrevision: 1\nprincipals: {a: {analyst: [" + rotationOld + "], reports: [" + rotationOld + "]}, b: {}}\n",
	}
	for _, raw := range cases {
		if _, err := parseGatewayKeys([]byte(raw), map[string]bool{"a": true, "b": true}, 1); err == nil {
			t.Fatal("ambiguous principal document accepted")
		}
	}
}

func TestGatewayPrincipalOwnershipCoversStatusCancelAndResults(t *testing.T) {
	p := principalTestPolicy()
	a, _ := authorityForPrincipal(p, "analyst")
	job := Job{ID: "owned", TenantID: "a", Authority: &a, State: Queued, ExpiresAt: time.Now().Add(time.Minute), Request: query.Request{Mode: "federated", Sources: []string{"sales"}, SQL: "SELECT * FROM sales"}}
	st := &gatewayStore{policy: p, jobs: map[string]Snapshot{"owned": {Job: job}}}
	auth := bareAuthenticator(t)
	if !auth.apply(principalKeySet(t, 1, map[string][]string{"analyst": {rotationOld}, "reports": {rotationNew}}, map[string][]string{}), time.Now()) {
		t.Fatal("auth")
	}
	g := &Gateway{tenants: map[string]gatewayTenant{"a": {store: st}}, auth: auth, permits: make(chan struct{}, 2), resultWaiters: make(chan struct{}, 2), ctx: context.Background()}
	for _, tt := range []struct{ method, path string }{{"GET", "/v1/queries/owned"}, {"POST", "/v1/queries/owned/cancel"}, {"GET", "/v1/queries/owned/results"}} {
		r := httptest.NewRequest(tt.method, tt.path, nil)
		r.Header.Set("Authorization", "Bearer "+rotationNew)
		r.Header.Set("X-Kelvo-Principal", "analyst")
		r.Header.Set("X-Kelvo-Tenant", "a")
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != http.StatusNotFound {
			t.Fatalf("foreign %s returned %d", tt.path, w.Code)
		}
	}
	r := httptest.NewRequest("GET", "/v1/queries/owned", nil)
	r.Header.Set("Authorization", "Bearer "+rotationOld)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("owner denied: %d", w.Code)
	}
	for _, body := range []string{
		`{"mode":"native","connection_id":"payroll","sql":"SELECT 1"}`,
		`{"mode":"federated","sources":["sales"],"sql":"SELECT 1","authority":{"principal_id":"analyst"}}`,
		`{"mode":"federated","sources":["sales"],"sql":"SELECT 1","access":{"sources":{}}}`,
		`{"mode":"federated","sources":["sales"],"sql":"SELECT 1","row_column_policy":{"sources":{}}}`,
	} {
		r := httptest.NewRequest("POST", "/v1/queries", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+rotationOld)
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != 400 && w.Code != 403 {
			t.Fatalf("forbidden submission reached store: %d", w.Code)
		}
	}
}

type principalKV struct {
	jetstream.KeyValue
	raw      []byte
	revision uint64
}

func (k *principalKV) Create(_ context.Context, _ string, raw []byte, _ ...jetstream.KVCreateOpt) (uint64, error) {
	k.raw = append([]byte(nil), raw...)
	k.revision = 1
	return 1, nil
}
func (k *principalKV) Get(_ context.Context, _ string) (jetstream.KeyValueEntry, error) {
	return principalEntry{k}, nil
}
func (k *principalKV) Update(_ context.Context, _ string, raw []byte, revision uint64) (uint64, error) {
	if revision != k.revision {
		return 0, jetstream.ErrKeyRevisionMismatch
	}
	k.raw = append([]byte(nil), raw...)
	k.revision++
	return k.revision, nil
}

type principalEntry struct{ *principalKV }

func (e principalEntry) Bucket() string                  { return jobsBucket }
func (e principalEntry) Key() string                     { return "slot.0" }
func (e principalEntry) Value() []byte                   { return append([]byte(nil), e.raw...) }
func (e principalEntry) Revision() uint64                { return e.revision }
func (e principalEntry) Created() time.Time              { return time.Now() }
func (e principalEntry) Delta() uint64                   { return 0 }
func (e principalEntry) Operation() jetstream.KeyValueOp { return jetstream.KeyValuePut }

func TestDurablePrincipalAdmissionAndImmutableCAS(t *testing.T) {
	p := principalTestPolicy()
	a, _ := authorityForPrincipal(p, "analyst")
	kv := &principalKV{}
	st := &NATSStore{policy: p, kv: kv}
	request := query.Request{Mode: "native", ConnectionID: "sales_native", SQL: "SELECT 1"}
	if _, err := st.Submit(context.Background(), request); err == nil || len(kv.raw) != 0 {
		t.Fatal("unauthenticated durable submission")
	}
	ctx := context.WithValue(context.Background(), jobAuthorityKey{}, a)
	s, err := st.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	var recorded Job
	if json.Unmarshal(kv.raw, &recorded) != nil || recorded.Authority == nil || *recorded.Authority != a {
		t.Fatal("authority absent from durable job")
	}
	next := s.Job
	next.State = Failed
	other := a
	other.PrincipalID = "reports"
	next.Authority = &other
	if _, err := st.CompareAndSwap(ctx, s, next); !errors.Is(err, ErrConflict) {
		t.Fatal("CAS changed principal", err)
	}
	next.Authority = nil
	if _, err := st.CompareAndSwap(ctx, s, next); !errors.Is(err, ErrConflict) {
		t.Fatal("CAS removed principal", err)
	}
	next.Authority = &a
	if _, err := st.CompareAndSwap(ctx, s, next); err != nil {
		t.Fatal("valid terminal mutation rejected", err)
	}
}

func TestWorkerRejectsUnboundPrincipalJobBeforeAssignment(t *testing.T) {
	p := principalTestPolicy()
	st := &nodeTestStore{p: p, jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}
	executor := &heldExecutor{started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	n, err := newNode(NodeConfig{Policy: p, WorkerID: "a1"}, st, executor)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	st.add(id)
	waitFor(t, func() bool { s, _ := st.Get(context.Background(), id); return s.Job.State == Failed })
	if executor.calls.Load() != 0 {
		t.Fatal("unauthorized job reached executor")
	}
}

func TestPrincipalContextCannotBeCreatedByJSONOrHeaders(t *testing.T) {
	// A raw token hash and user-supplied context/header strings do not satisfy the
	// private server-side context key used by the durable submission boundary.
	p := principalTestPolicy()
	ctx := context.WithValue(context.Background(), "principal_id", "analyst")
	if _, err := submissionAuthority(ctx, p, query.Request{Mode: "native", ConnectionID: "sales_native", SQL: "SELECT 1"}); err == nil {
		t.Fatal("untrusted principal accepted")
	}
}

func TestPrincipalRetiredTokensRemainBoundedAndCannotBeReassigned(t *testing.T) {
	a := bareAuthenticator(t)
	first := principalKeySet(t, 1, map[string][]string{"analyst": {rotationOld}}, map[string][]string{})
	if !a.apply(first, time.Now()) {
		t.Fatal("initial")
	}
	removed := principalKeySet(t, 2, map[string][]string{}, map[string][]string{})
	if !a.apply(removed, time.Now()) {
		t.Fatal("removed")
	}
	moved := principalKeySet(t, 3, map[string][]string{"reports": {rotationOld}}, map[string][]string{})
	if a.apply(moved, time.Now()) {
		t.Fatal("retired token reassigned to another principal")
	}
	for index := 0; len(a.bindings) < gatewayMaxBindings; index++ {
		a.bindings[sha256.Sum256([]byte(fmt.Sprintf("retired-%d", index)))] = "a\x00analyst"
	}
	fresh := principalKeySet(t, 4, map[string][]string{"analyst": {rotationNew}}, map[string][]string{})
	if a.apply(fresh, time.Now()) || len(a.bindings) != gatewayMaxBindings {
		t.Fatal("binding history evicted or grew beyond limit")
	}
	restored := principalKeySet(t, 4, map[string][]string{"analyst": {rotationOld}}, map[string][]string{})
	if !a.apply(restored, time.Now()) {
		t.Fatal("known binding could not recover at capacity")
	}
}

func TestGatewayPrincipalRevocationAtCommitWithholdsEOSWithoutCallback(t *testing.T) {
	p := principalTestPolicy()
	principal, _ := authorityForPrincipal(p, "analyst")
	auth := bareAuthenticator(t)
	if !auth.apply(principalKeySet(t, 1, map[string][]string{"analyst": {rotationOld}}, map[string][]string{}), time.Now()) {
		t.Fatal("initial")
	}
	_, key, _ := auth.lookup(rotationOld)
	encoded, stats := compressedRelayFixture(t, "none")
	store := &compressionRelayStore{gatewayStore: gatewayStore{policy: p}, snapshot: Snapshot{Revision: 1, Job: Job{
		ID: "principal-result", TenantID: "a", Authority: &principal, State: Assigned, Owner: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", WorkerID: "worker-one",
		CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute), Request: query.Request{Mode: "federated", Sources: []string{"sales"}, SQL: "SELECT * FROM sales"},
	}}}
	revoked := principalKeySet(t, 2, map[string][]string{"analyst": {}}, map[string][]string{})
	store.beforeCommit = func() {
		if !auth.apply(revoked, time.Now()) {
			t.Error("revoke")
		}
	}
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !store.resultReady(r.Header.Get("X-Kelvo-Claim"), stats) {
			t.Error("claim")
			return
		}
		_, _ = w.Write(encoded)
	}))
	defer upstream.Close()
	endpoint, _ := url.Parse(upstream.URL)
	tenant := gatewayTenant{store: store, workers: map[string]workerEndpoint{"worker-one": {url: endpoint, client: upstream.Client()}}}
	ctx := context.WithValue(context.Background(), jobAuthorityKey{}, principal)
	ctx = context.WithValue(ctx, keyAuthorizationContext{}, keyAuthorization{auth, key})
	// Deliberately omit AfterFunc: this makes delayed callback scheduling
	// deterministic and requires the direct authority check to withhold EOS.
	output := httptest.NewRecorder()
	aborted := false
	func() {
		defer func() {
			if v := recover(); v != nil {
				if v != http.ErrAbortHandler {
					t.Fatalf("panic: %v", v)
				}
				aborted = true
			}
		}()
		(&Gateway{}).results(output, httptest.NewRequest("GET", "/v1/queries/principal-result/results", nil).WithContext(ctx), tenant, "principal-result", nil)
	}()
	if !aborted || ctx.Err() != nil || key.Err() == nil || bytes.HasSuffix(output.Body.Bytes(), encoded[len(encoded)-8:]) {
		t.Fatal("revoked key released final EOS or relied on callback")
	}
}

func TestPrincipalFinalCheckObservesDocumentExpiryWithoutTimer(t *testing.T) {
	auth := bareAuthenticator(t)
	if !auth.apply(principalKeySet(t, 1, map[string][]string{"analyst": {rotationOld}}, map[string][]string{}), time.Now()) {
		t.Fatal("initial")
	}
	_, key, _ := auth.lookup(rotationOld)
	ctx := context.WithValue(context.Background(), keyAuthorizationContext{}, keyAuthorization{auth, key})
	auth.mu.Lock()
	auth.validUntil = time.Now().Add(-time.Second)
	auth.mu.Unlock()
	if ctx.Err() != nil || key.Err() != nil || requestAuthorityErr(ctx) == nil {
		t.Fatal("completion accepted expired authority without running timer")
	}
}
