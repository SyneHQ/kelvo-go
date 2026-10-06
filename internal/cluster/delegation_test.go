// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func delegatedTestGrant(t *testing.T) (Policy, ed25519.PrivateKey, delegation.Claims, query.Request, JobAuthority) {
	t.Helper()
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)) // Public deterministic fixture, never a deployment key.
	p := testPolicy()
	p.Access = &PrincipalPolicy{Revision: 1, Principals: map[string]PrincipalGrant{"bridge": {Kind: "service", DelegatedResolver: &ResolverTrust{Issuer: "api-resolver", Audience: "analytics", URL: "https://resolver.test/internal/kelvo/resolve", PublicKey: base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey))}}}}
	r := query.Request{Mode: "native", ConnectionID: "source_1", SQL: "SELECT 2"}
	digest, err := delegation.QueryDigest(r)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	c := delegation.Claims{Version: 1, Issuer: "api-resolver", Audience: "analytics", ClusterTenant: "a", ServicePrincipal: "bridge", AppTeam: "team-a", Subject: delegation.Subject{Kind: "user", ID: "user-a"}, ID: strings.Repeat("a", 48), IssuedAt: now - 1, ExpiresAt: now + 120, QuerySHA256: digest, Sources: []delegation.Source{{Alias: "source_1", ConnectionID: "saved-a"}}}
	r.Delegation, err = delegation.Sign(c, key)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := authorityForPrincipal(p, "bridge")
	return p, key, c, r, a
}
func delegatedRequestContext(a JobAuthority, token string) context.Context {
	ctx := context.WithValue(context.Background(), jobAuthorityKey{}, a)
	return context.WithValue(ctx, delegationHeaderKey{}, token)
}

func TestDelegatedAdmissionRequiresMatchingHeaderAndExplicitServiceTrust(t *testing.T) {
	p, _, _, r, a := delegatedTestGrant(t)
	if err := ValidatePolicy(p); err != nil {
		t.Fatal(err)
	}
	ctx := delegatedRequestContext(a, r.Delegation)
	bound, err := submissionAuthority(ctx, p, r)
	if err != nil || bound.DelegationSHA256 != delegation.Digest(r.Delegation) || bound.ApplicationTeam != "team-a" || bound.SubjectID != "user-a" {
		t.Fatal("delegated authority was not bound", err)
	}
	if err := validateJobAuthority(p, bound, r); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []context.Context{context.Background(), context.WithValue(context.Background(), jobAuthorityKey{}, a), delegatedRequestContext(a, "other")} {
		if _, err := submissionAuthority(bad, p, r); err == nil {
			t.Fatal("missing or mismatched grant header was accepted")
		}
	}
	static := r
	static.Delegation = ""
	if _, err := submissionAuthority(ctx, p, static); err == nil {
		t.Fatal("delegated context reached static path")
	}
	if err := validateJobAuthority(testPolicy(), nil, r); err == nil {
		t.Fatal("legacy policy bypassed delegated authorization")
	}
	p.Access.Revision++
	if err := validateJobAuthority(p, bound, r); err == nil {
		t.Fatal("policy rotation preserved old grant authority")
	}
	p.Access.Revision--
	g := p.Access.Principals["bridge"]
	g.DelegatedResolver = nil
	p.Access.Principals["bridge"] = g
	if _, err := submissionAuthority(ctx, p, r); err == nil {
		t.Fatal("missing resolver policy was accepted")
	}
}

func TestSharedServiceCannotAccessAnotherApplicationGrant(t *testing.T) {
	p, key, c, r, a := delegatedTestGrant(t)
	bound, err := submissionAuthority(delegatedRequestContext(a, r.Delegation), p, r)
	if err != nil {
		t.Fatal(err)
	}
	job := Job{ID: "owned", TenantID: p.TenantID, Authority: bound, State: Queued, ExpiresAt: time.Now().Add(time.Minute), Request: r}
	st := &gatewayStore{policy: p, jobs: map[string]Snapshot{"owned": {Job: job, Revision: 1}}}
	auth := bareAuthenticator(t)
	if !auth.apply(principalKeySet(t, 1, map[string][]string{"bridge": {rotationOld}}, map[string][]string{}), time.Now()) {
		t.Fatal("auth")
	}
	g := &Gateway{tenants: map[string]gatewayTenant{"a": {store: st}}, auth: auth, permits: make(chan struct{}, 2), resultWaiters: make(chan struct{}, 2), ctx: context.Background()}
	for _, mutate := range []func(*delegation.Claims){func(c *delegation.Claims) { c.AppTeam = "team-b" }, func(c *delegation.Claims) { c.Subject.ID = "user-b" }, func(c *delegation.Claims) { c.ID = strings.Repeat("b", 48) }} {
		other := c
		mutate(&other)
		token, err := delegation.Sign(other, key)
		if err != nil {
			t.Fatal(err)
		}
		for _, route := range []struct{ method, path string }{{"GET", "/v1/queries/owned"}, {"POST", "/v1/queries/owned/cancel"}, {"GET", "/v1/queries/owned/results"}} {
			req := httptest.NewRequest(route.method, route.path, nil)
			req.Header.Set("Authorization", "Bearer "+rotationOld)
			req.Header.Set(delegation.Header, token)
			w := httptest.NewRecorder()
			g.ServeHTTP(w, req)
			if w.Code != 404 {
				t.Fatalf("foreign scope %s returned %d", route.path, w.Code)
			}
		}
	}
	for _, token := range []string{"", r.Delegation} {
		req := httptest.NewRequest("GET", "/v1/queries/owned", nil)
		req.Header.Set("Authorization", "Bearer "+rotationOld)
		if token != "" {
			req.Header.Set(delegation.Header, token)
		}
		w := httptest.NewRecorder()
		g.ServeHTTP(w, req)
		expected := 404
		if token != "" {
			expected = 200
		}
		if w.Code != expected {
			t.Fatalf("grant ownership=%d wanted %d", w.Code, expected)
		}
	}
}

func TestExpiredDelegationPermitsOnlyBriefCancellationCleanup(t *testing.T) {
	p, key, c, r, a := delegatedTestGrant(t)
	c.IssuedAt = time.Now().Unix() - 61
	c.ExpiresAt = time.Now().Unix() - 1
	var err error
	r.Delegation, err = delegation.Sign(c, key)
	if err != nil {
		t.Fatal(err)
	}
	bound, _, err := delegatedAuthority(p, a, r, time.Unix(c.IssuedAt+1, 0))
	if err != nil {
		t.Fatal(err)
	}
	st := &gatewayStore{policy: p, jobs: map[string]Snapshot{"owned": {Job: Job{ID: "owned", TenantID: p.TenantID, Authority: &bound, Request: r, State: Queued, ExpiresAt: time.Now().Add(time.Minute)}}}}
	for _, route := range []struct {
		method, path string
		allowed      bool
	}{{"GET", "/v1/queries/owned", false}, {"GET", "/v1/queries/owned/results", false}, {"POST", "/v1/queries/owned/connection-lease", false}, {"POST", "/v1/queries/owned/cancel", true}} {
		req := httptest.NewRequest(route.method, route.path, nil).WithContext(context.WithValue(context.Background(), jobAuthorityKey{}, a))
		req.Header.Set(delegation.Header, r.Delegation)
		ctx, cancel, err := delegatedHTTPContext(req, p)
		if cancel != nil {
			defer cancel()
		}
		if (err == nil) != route.allowed {
			t.Fatalf("expiry on %s allowed=%v err=%v", route.path, route.allowed, err)
		}
		if err == nil {
			if _, err := principalSnapshot(ctx, gatewayTenant{store: st}, "owned"); err != nil {
				t.Fatal("cleanup cannot see its own query", err)
			}
		}
	}
	c.ExpiresAt = time.Now().Unix() - 6
	r.Delegation, _ = delegation.Sign(c, key)
	req := httptest.NewRequest("POST", "/v1/queries/owned/cancel", nil).WithContext(context.WithValue(context.Background(), jobAuthorityKey{}, a))
	req.Header.Set(delegation.Header, r.Delegation)
	_, cancel, err := delegatedHTTPContext(req, p)
	cancel()
	if err == nil {
		t.Fatal("cleanup grace exceeded five seconds")
	}
}

type delegatedLeaseStore struct {
	*gatewayStore
	until  time.Time
	err    error
	onRead func()
}

func (s *delegatedLeaseStore) WorkerLease(context.Context, string, string) (time.Time, error) {
	if s.onRead != nil {
		s.onRead()
	}
	return s.until, s.err
}

func TestConnectionLeaseRequiresLiveAssignmentAndWorkerCustody(t *testing.T) {
	p, _, _, r, a := delegatedTestGrant(t)
	bound, err := submissionAuthority(delegatedRequestContext(a, r.Delegation), p, r)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	job := Job{ID: "owned", TenantID: p.TenantID, Authority: bound, State: Running, ExpiresAt: now.Add(time.Minute), HeartbeatAt: now, WorkerID: "a1", Owner: strings.Repeat("b", 32), Claim: strings.Repeat("c", 32), Request: r}
	for name, modify := range map[string]func(*delegatedLeaseStore, *connectionLeaseRequest){
		"valid":           func(*delegatedLeaseStore, *connectionLeaseRequest) {},
		"worker mismatch": func(_ *delegatedLeaseStore, b *connectionLeaseRequest) { b.WorkerID = "other" },
		"owner mismatch":  func(_ *delegatedLeaseStore, b *connectionLeaseRequest) { b.Owner = strings.Repeat("d", 32) },
		"claim mismatch":  func(_ *delegatedLeaseStore, b *connectionLeaseRequest) { b.Claim = strings.Repeat("d", 32) },
		"stale heartbeat": func(s *delegatedLeaseStore, _ *connectionLeaseRequest) {
			j := s.jobs["owned"]
			j.Job.HeartbeatAt = now.Add(-time.Minute)
			s.jobs["owned"] = j
		},
		"expired worker":     func(s *delegatedLeaseStore, _ *connectionLeaseRequest) { s.until = now.Add(-time.Second) },
		"worker unavailable": func(s *delegatedLeaseStore, _ *connectionLeaseRequest) { s.err = ErrConflict },
		"cancel during custody read": func(s *delegatedLeaseStore, _ *connectionLeaseRequest) {
			s.onRead = func() { j := s.jobs["owned"]; j.Job.State = Cancelled; s.jobs["owned"] = j }
		},
	} {
		t.Run(name, func(t *testing.T) {
			st := &delegatedLeaseStore{gatewayStore: &gatewayStore{policy: p, jobs: map[string]Snapshot{"owned": {Job: job, Revision: 1}}}, until: time.Now().Add(p.LeaseDuration)}
			body := connectionLeaseRequest{WorkerID: job.WorkerID, Owner: job.Owner, Claim: job.Claim}
			modify(st, &body)
			raw, _ := json.Marshal(body)
			req := httptest.NewRequest("POST", "/v1/queries/owned/connection-lease", strings.NewReader(string(raw))).WithContext(delegatedRequestContext(a, r.Delegation))
			w := httptest.NewRecorder()
			(&Gateway{}).connectionLease(w, req, gatewayTenant{store: st}, "owned")
			if (w.Code == 200) != (name == "valid") {
				t.Fatalf("lease=%d body=%s", w.Code, w.Body.String())
			}
			if name == "valid" {
				var receipt struct {
					ValidUntil int64 `json:"valid_until"`
				}
				if json.Unmarshal(w.Body.Bytes(), &receipt) != nil || receipt.ValidUntil <= time.Now().Unix() || receipt.ValidUntil > time.Now().Add(5*time.Second).Unix() {
					t.Fatal("lease receipt outside proof lifetime")
				}
			}
			if strings.Contains(w.Body.String(), r.Delegation) || strings.Contains(w.Body.String(), job.Owner) || strings.Contains(w.Body.String(), job.Claim) {
				t.Fatal("lease response exposed authority material")
			}
		})
	}
}

func TestDelegatedDurableAuthorityCannotChangeScope(t *testing.T) {
	p, _, _, r, a := delegatedTestGrant(t)
	kv := &principalKV{}
	store := &NATSStore{policy: p, kv: kv}
	ctx := delegatedRequestContext(a, r.Delegation)
	s, err := store.Submit(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if s.Job.Authority == nil || s.Job.Authority.ApplicationTeam != "team-a" {
		t.Fatal("scope was not durable")
	}
	for _, mutate := range []func(*Job){func(j *Job) { a := *j.Authority; a.ApplicationTeam = "team-b"; j.Authority = &a }, func(j *Job) { a := *j.Authority; a.DelegationSHA256 = ""; j.Authority = &a }, func(j *Job) { j.Request.Delegation = "" }} {
		next := s.Job
		next.State = Failed
		mutate(&next)
		if _, err := store.CompareAndSwap(ctx, s, next); !errors.Is(err, ErrConflict) {
			t.Fatal("durable scope mutation accepted", err)
		}
	}
}

type delegatedCapture struct {
	executions chan delegation.Execution
	deadlines  chan time.Time
}

func (e *delegatedCapture) Execute(ctx context.Context, r query.Request, _ query.Sink) (query.Stats, error) {
	if r.SQL == "SELECT 1" {
		return query.Stats{}, nil
	}
	value, ok := delegation.ExecutionFromContext(ctx)
	if !ok {
		return query.Stats{}, errors.New("delegation context missing")
	}
	deadline, _ := ctx.Deadline()
	e.executions <- value
	e.deadlines <- deadline
	return query.Stats{}, query.NewError("QUERY_FAILED", "fixture completed")
}

func TestNodeDerivesDelegatedProofFromItsRunningAssignment(t *testing.T) {
	p, _, claims, r, a := delegatedTestGrant(t)
	authority, err := submissionAuthority(delegatedRequestContext(a, r.Delegation), p, r)
	if err != nil {
		t.Fatal(err)
	}
	st := &nodeTestStore{p: p, jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}
	executor := &delegatedCapture{executions: make(chan delegation.Execution, 1), deadlines: make(chan time.Time, 1)}
	n, err := newNode(NodeConfig{Policy: p, WorkerID: "a1"}, st, executor)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	st.mu.Lock()
	st.jobs[id] = Snapshot{Revision: 1, Job: Job{ID: id, TenantID: p.TenantID, Authority: authority, Request: r, State: Queued, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute)}}
	st.mu.Unlock()
	if err := st.Enqueue(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { s, _ := st.Get(context.Background(), id); return s.Job.State == Assigned })
	s, _ := st.Get(context.Background(), id)
	claimed := s.Job
	claimed.State = Claimed
	claimed.Claim = strings.Repeat("b", 32)
	if _, err := st.CompareAndSwap(context.Background(), s, claimed); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(GatewayIdentity)
	cert := &x509.Certificate{URIs: []*url.URL{u}}
	req := httptest.NewRequest("GET", "/internal/queries/"+id+"/results", nil)
	req.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}
	req.Header.Set("X-Kelvo-Claim", claimed.Claim)
	w := httptest.NewRecorder()
	n.ServeHTTP(w, req)
	select {
	case execution := <-executor.executions:
		if execution.Token != r.Delegation || execution.Claims.AppTeam != "team-a" || execution.Binding.JobID != id || execution.Binding.WorkerID != "a1" || execution.Binding.Owner != claimed.Owner || execution.Binding.Claim != claimed.Claim {
			t.Fatal("worker received caller-controlled or incomplete assignment")
		}
		deadline := <-executor.deadlines
		if deadline.IsZero() || deadline.After(time.Unix(claims.ExpiresAt, 0)) {
			t.Fatal("worker authority outlived grant")
		}
	default:
		t.Fatalf("node never passed delegated authority: %d", w.Code)
	}
}
