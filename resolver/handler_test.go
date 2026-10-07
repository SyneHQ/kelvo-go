// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package resolver

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
)

type leaseFixture struct {
	query     func(context.Context, string, string, Binding) (time.Time, error)
	operation func(context.Context, string, string, Binding) (time.Time, error)
}

func (l leaseFixture) ValidateConnectionLease(c context.Context, id, grant string, b Binding) (time.Time, error) {
	if l.query == nil {
		return time.Time{}, ErrInvalid
	}
	return l.query(c, id, grant, b)
}
func (l leaseFixture) ValidateOperationLease(c context.Context, id, grant string, b Binding) (time.Time, error) {
	if l.operation == nil {
		return time.Time{}, ErrInvalid
	}
	return l.operation(c, id, grant, b)
}

type handlerFixture struct {
	config    Config
	query     QueryRequest
	operation OperationRequest
	tls       *tls.ConnectionState
	key       ed25519.PrivateKey
}

func newFixture(t *testing.T) *handlerFixture {
	t.Helper()
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{42}, ed25519.SeedSize))
	trust := delegation.Trust{Issuer: "application", Audience: "kelvo", ClusterTenant: "cluster", ServicePrincipal: "service", PublicKey: key.Public().(ed25519.PublicKey)}
	f := &handlerFixture{key: key}
	f.query = QueryRequest{Query: query.Request{Mode: "native", SQL: "SELECT 1", ConnectionID: "source_1"}, JobID: "query-1", WorkerID: "worker", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}
	digest, err := delegation.QueryDigest(f.query.Query)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	claims := delegation.Claims{Version: 1, Issuer: trust.Issuer, Audience: trust.Audience, ClusterTenant: trust.ClusterTenant, ServicePrincipal: trust.ServicePrincipal, AppTeam: "team-a", Subject: delegation.Subject{Kind: "user", ID: "user-a"}, ID: strings.Repeat("c", 48), IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix(), QuerySHA256: digest, Sources: []delegation.Source{{Alias: "source_1", ConnectionID: "saved-a", Database: "app"}}}
	f.query.Delegation, err = delegation.Sign(claims, key)
	if err != nil {
		t.Fatal(err)
	}
	f.operation = OperationRequest{Operation: operations.Request{Version: 1, Kind: operations.QueryRead, Connection: operations.ConnectionRef{ID: "saved-a", Database: "app"}, IdempotencyKey: "read-1", Spec: operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT 1"}}}, OperationID: "operation-1", WorkerID: f.query.WorkerID, Owner: f.query.Owner, Claim: f.query.Claim}
	f.signOperation(t)
	identity, _ := url.Parse("spiffe://kelvo/tenant/cluster/worker/worker")
	leaf := &x509.Certificate{Raw: []byte("test-leaf"), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), URIs: []*url.URL{identity}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	root := &x509.Certificate{Raw: []byte("test-root"), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true}
	f.tls = &tls.ConnectionState{HandshakeComplete: true, Version: tls.VersionTLS13, PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf, root}}}
	f.config = Config{QueryTrust: []delegation.Trust{trust}, OperationTrust: []operations.GrantTrust{{Issuer: trust.Issuer, Audience: trust.Audience, ClusterTenant: trust.ClusterTenant, ServicePrincipal: trust.ServicePrincipal, PublicKey: trust.PublicKey}}, Leases: leaseFixture{
		query: func(_ context.Context, id, token string, b Binding) (time.Time, error) {
			if id != f.query.JobID || token != f.query.Delegation || b != f.query.Binding() {
				return time.Time{}, ErrInvalid
			}
			return time.Now().Add(5 * time.Second), nil
		},
		operation: func(_ context.Context, id, token string, b Binding) (time.Time, error) {
			if id != f.operation.OperationID || token != f.operation.Grant || b != f.operation.Binding() {
				return time.Time{}, ErrInvalid
			}
			return time.Now().Add(5 * time.Second), nil
		},
	}}
	f.config.AuthorizeQuery = func(_ context.Context, _ QueryRequest, c delegation.Claims) (Authorization, error) {
		if c.AppTeam != "team-a" || c.Sources[0].ConnectionID != "saved-a" {
			return Authorization{}, ErrInvalid
		}
		return fixtureAuthorization(), nil
	}
	f.config.ResolveQuery = func(context.Context, QueryRequest, delegation.Claims, Authorization) (QueryMaterial, error) {
		return QueryMaterial{Sources: []Source{fixtureSource()}, Secrets: fixtureSecrets(), ValidUntil: fixtureAuthorization().ValidUntil}, nil
	}
	f.config.AuthorizeOperation = func(_ context.Context, _ OperationRequest, c operations.GrantClaims) (Authorization, error) {
		if c.AppTeam != "team-a" || c.ConnectionID != "saved-a" {
			return Authorization{}, ErrInvalid
		}
		return fixtureAuthorization(), nil
	}
	f.config.ResolveOperation = func(context.Context, OperationRequest, operations.GrantClaims, Authorization) (OperationMaterial, error) {
		return OperationMaterial{Source: fixtureSource(), Secrets: fixtureSecrets(), ValidUntil: fixtureAuthorization().ValidUntil}, nil
	}
	return f
}

func (f *handlerFixture) signOperation(t *testing.T) {
	t.Helper()
	digest, err := operations.Digest(f.operation.Operation)
	if err != nil {
		t.Fatal(err)
	}
	a := operations.Authorization{Kind: "read"}
	if f.operation.Operation.Kind.Mutating() {
		a.Kind = "trusted_app"
	}
	c := operations.GrantClaims{Version: operations.GrantVersion, Issuer: "application", Audience: "kelvo", ClusterTenant: "cluster", ServicePrincipal: "service", AppTeam: "team-a", Subject: operations.Subject{Kind: "user", ID: "user-a"}, ID: "grant-1", IssuedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Minute).Unix(), ConnectionID: f.operation.Operation.Connection.ID, Operation: f.operation.Operation.Kind, RequestSHA256: digest, Authorization: a}
	f.operation.Grant, err = operations.SignGrant(c, f.key)
	if err != nil {
		t.Fatal(err)
	}
}
func fixtureAuthorization() Authorization {
	return Authorization{Revision: strings.Repeat("d", 64), ValidUntil: time.Now().Add(time.Minute)}
}
func fixtureSource() Source {
	return Source{ID: "source_1", Type: "postgres", DSNEnv: "KELVO_SOURCE_REQUEST_0_DSN"}
}
func fixtureSecrets() map[string]string {
	return map[string]string{"KELVO_SOURCE_REQUEST_0_DSN": "private-fixture-value"}
}

func (f *handlerFixture) request(t *testing.T, path string, body any) *http.Request {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	r.TLS = f.tls
	r.Header.Set("Content-Type", "application/json")
	return r
}
func serveFixture(t *testing.T, c Config, r *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	h, err := NewHandler(c)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestHandlerReturnsRequestBoundShortCredentialResponses(t *testing.T) {
	f := newFixture(t)
	w := serveFixture(t, f.config, f.request(t, QueryPath, f.query))
	var q QueryResponse
	if w.Code != http.StatusOK || operations.DecodeStrict(w.Body.Bytes(), &q, MaxResponseBytes) != nil || q.Version != Version || q.DelegationSHA256 != delegation.Digest(f.query.Delegation) || q.ValidUntil <= time.Now().Unix() || q.ValidUntil > time.Now().Unix()+5 || q.Secrets[fixtureSource().DSNEnv] == "" {
		t.Fatalf("invalid query response: status %d", w.Code)
	}
	w = serveFixture(t, f.config, f.request(t, OperationPath, f.operation))
	var op OperationResponse
	digest, _ := operations.Digest(f.operation.Operation)
	if w.Code != http.StatusOK || operations.DecodeStrict(w.Body.Bytes(), &op, MaxResponseBytes) != nil || op.GrantSHA256 != operations.GrantDigest(f.operation.Grant) || op.RequestSHA256 != digest || op.SourceRevision != fixtureAuthorization().Revision {
		t.Fatalf("invalid operation response: status %d", w.Code)
	}
}

func TestHandlerRejectsMalformedRequestsBeforeSecrets(t *testing.T) {
	for name, change := range map[string]func(*http.Request){
		"unsigned body": func(r *http.Request) {
			raw := strings.Replace(readBody(r), "SELECT 1", "SELECT 2", 1)
			r.Body = io.NopCloser(strings.NewReader(raw))
		},
		"duplicate key": func(r *http.Request) {
			raw := strings.Replace(readBody(r), `"job_id":"query-1"`, `"job_id":"query-1","JOB_ID":"query-1"`, 1)
			r.Body = io.NopCloser(strings.NewReader(raw))
		},
		"unknown field": func(r *http.Request) {
			raw := strings.TrimSuffix(readBody(r), "}") + `,"source_url":"https://foreign.invalid"}`
			r.Body = io.NopCloser(strings.NewReader(raw))
		},
		"trailing value": func(r *http.Request) { r.Body = io.NopCloser(strings.NewReader(readBody(r) + `{}`)) },
		"oversized": func(r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader(strings.Repeat(" ", MaxQueryRequestBytes+1)))
		},
		"wrong media":     func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") },
		"duplicate media": func(r *http.Request) { r.Header.Add("Content-Type", "application/json") },
		"encoded body":    func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") },
		"missing TLS":     func(r *http.Request) { r.TLS = nil },
		"proxy identity": func(r *http.Request) {
			r.TLS = nil
			r.Header.Set("X-Forwarded-Client-Cert", "spiffe://kelvo/tenant/cluster/worker/worker")
		},
		"TLS downgrade": func(r *http.Request) { r.TLS.Version = tls.VersionTLS12 },
		"wrong worker": func(r *http.Request) {
			u, _ := url.Parse("spiffe://kelvo/tenant/cluster/worker/other")
			r.TLS.PeerCertificates[0].URIs = []*url.URL{u}
		},
		"wrong tenant": func(r *http.Request) {
			u, _ := url.Parse("spiffe://kelvo/tenant/other/worker/worker")
			r.TLS.PeerCertificates[0].URIs = []*url.URL{u}
		},
		"expired chain":    func(r *http.Request) { r.TLS.VerifiedChains[0][1].NotAfter = time.Now().Add(-time.Second) },
		"unverified chain": func(r *http.Request) { r.TLS.VerifiedChains = nil },
		"wrong EKU": func(r *http.Request) {
			r.TLS.PeerCertificates[0].ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		},
		"query URL":    func(r *http.Request) { r.URL.RawQuery = "retarget=1" },
		"encoded path": func(r *http.Request) { r.URL.RawPath = "/internal/kelvo/%72esolve" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			called := false
			f.config.ResolveQuery = func(context.Context, QueryRequest, delegation.Claims, Authorization) (QueryMaterial, error) {
				called = true
				return QueryMaterial{}, nil
			}
			r := f.request(t, QueryPath, f.query)
			change(r)
			w := serveFixture(t, f.config, r)
			if w.Code < 400 || called || strings.Contains(w.Body.String(), "private-fixture-value") {
				t.Fatal("invalid request reached credentials")
			}
		})
	}
}

func readBody(r *http.Request) string { b, _ := io.ReadAll(r.Body); return string(b) }

func TestHandlerRejectsCustodyAndAuthorizationRaces(t *testing.T) {
	for _, scenario := range []string{"initial lease", "final lease", "long lease", "revoked authorization", "changed revision", "expired certificate", "ambient secret", "extra secret", "wrong alias", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			leases, auths, loads := 0, 0, 0
			l := f.config.Leases.(leaseFixture)
			l.query = func(context.Context, string, string, Binding) (time.Time, error) {
				leases++
				if scenario == "initial lease" || scenario == "final lease" && leases > 1 {
					return time.Time{}, ErrInvalid
				}
				if scenario == "long lease" {
					return time.Now().Add(time.Minute), nil
				}
				return time.Now().Add(5 * time.Second), nil
			}
			f.config.Leases = l
			f.config.AuthorizeQuery = func(context.Context, QueryRequest, delegation.Claims) (Authorization, error) {
				auths++
				a := fixtureAuthorization()
				if scenario == "revoked authorization" && auths > 1 {
					return a, ErrInvalid
				}
				if scenario == "changed revision" && auths > 1 {
					a.Revision = strings.Repeat("e", 64)
				}
				return a, nil
			}
			r := f.request(t, QueryPath, f.query)
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			r = r.WithContext(ctx)
			f.config.ResolveQuery = func(context.Context, QueryRequest, delegation.Claims, Authorization) (QueryMaterial, error) {
				loads++
				m := QueryMaterial{Sources: []Source{fixtureSource()}, Secrets: fixtureSecrets(), ValidUntil: fixtureAuthorization().ValidUntil}
				switch scenario {
				case "ambient secret":
					m.Sources[0].DSNEnv = "DATABASE_URL"
				case "extra secret":
					m.Secrets["OTHER"] = "private-fixture-value"
				case "wrong alias":
					m.Sources[0].ID = "other"
				case "expired certificate":
					f.tls.PeerCertificates[0].NotAfter = time.Now().Add(-time.Second)
				case "cancelled":
					cancel()
				}
				return m, nil
			}
			w := serveFixture(t, f.config, r)
			if w.Code < 400 || strings.Contains(w.Body.String(), "private-fixture-value") {
				t.Fatal("race released credentials")
			}
			if (scenario == "initial lease" || scenario == "long lease") && loads != 0 {
				t.Fatal("credentials loaded without valid lease")
			}
		})
	}
}

func TestOperationChangedScopeCannotReachCredentials(t *testing.T) {
	for _, change := range []func(*OperationRequest){func(r *OperationRequest) { r.Operation.Connection.Database = "foreign" }, func(r *OperationRequest) { r.Operation.Connection.Schema = "foreign" }, func(r *OperationRequest) { r.Operation.Spec.Query.SQL = "SELECT 2" }, func(r *OperationRequest) { r.Claim = strings.Repeat("f", 32) }} {
		f := newFixture(t)
		input := clone(f.operation)
		change(&input)
		called := false
		f.config.ResolveOperation = func(context.Context, OperationRequest, operations.GrantClaims, Authorization) (OperationMaterial, error) {
			called = true
			return OperationMaterial{}, nil
		}
		w := serveFixture(t, f.config, f.request(t, OperationPath, input))
		if w.Code < 400 || called {
			t.Fatal("changed operation reached credentials")
		}
	}
}

func TestHandlerSnapshotsTrustAndHookInputs(t *testing.T) {
	f := newFixture(t)
	f.config.ResolveQuery = func(_ context.Context, _ QueryRequest, c delegation.Claims, _ Authorization) (QueryMaterial, error) {
		c.Sources[0].Alias = "changed"
		return QueryMaterial{Sources: []Source{fixtureSource()}, Secrets: fixtureSecrets(), ValidUntil: fixtureAuthorization().ValidUntil}, nil
	}
	h, err := NewHandler(f.config)
	if err != nil {
		t.Fatal(err)
	}
	clear(f.config.QueryTrust[0].PublicKey)
	f.config.QueryTrust[0].Issuer = "changed"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, f.request(t, QueryPath, f.query))
	if w.Code != http.StatusOK {
		t.Fatal("caller mutation changed frozen handler authority")
	}
}

func TestHandlerDefaultsFailClosed(t *testing.T) {
	if _, err := NewHandler(Config{}); err == nil {
		t.Fatal("missing custody verifier accepted")
	}
	f := newFixture(t)
	f.config.ResolveQuery = nil
	w := serveFixture(t, f.config, f.request(t, QueryPath, f.query))
	if w.Code != http.StatusForbidden {
		t.Fatal("missing query hook did not fail closed")
	}
	f.config.AuthorizeOperation = nil
	w = serveFixture(t, f.config, f.request(t, OperationPath, f.operation))
	if w.Code != http.StatusForbidden {
		t.Fatal("missing operation authority did not fail closed")
	}
}

func TestCredentialExpiryCannotExtendResponseAuthority(t *testing.T) {
	for _, endpoint := range []string{QueryPath, OperationPath} {
		for _, scenario := range []string{"missing", "expired", "short", "long"} {
			t.Run(endpoint+"/"+scenario, func(t *testing.T) {
				f := newFixture(t)
				var expires time.Time
				switch scenario {
				case "expired":
					expires = time.Now().Add(-time.Second)
				case "short":
					expires = time.Now().Add(2 * time.Second)
				case "long":
					expires = time.Now().Add(time.Hour)
				}
				f.config.ResolveQuery = func(context.Context, QueryRequest, delegation.Claims, Authorization) (QueryMaterial, error) {
					return QueryMaterial{Sources: []Source{fixtureSource()}, Secrets: fixtureSecrets(), ValidUntil: expires}, nil
				}
				f.config.ResolveOperation = func(context.Context, OperationRequest, operations.GrantClaims, Authorization) (OperationMaterial, error) {
					return OperationMaterial{Source: fixtureSource(), Secrets: fixtureSecrets(), ValidUntil: expires}, nil
				}
				var input any = f.query
				if endpoint == OperationPath {
					input = f.operation
				}
				w := serveFixture(t, f.config, f.request(t, endpoint, input))
				if scenario == "missing" || scenario == "expired" {
					if w.Code < 400 || strings.Contains(w.Body.String(), "private-fixture-value") {
						t.Fatal("expired credentials were released")
					}
					return
				}
				var response struct {
					ValidUntil int64 `json:"valid_until"`
				}
				if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.ValidUntil > expires.Unix() || response.ValidUntil > time.Now().Unix()+5 {
					t.Fatal("credential expiry extended response authority")
				}
			})
		}
	}
}

func TestCompletionRequiresExactRetainedCustodyAndWorker(t *testing.T) {
	for _, scenario := range []string{"valid", "changed grant", "wrong tenant", "missing state", "expired state"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			digest, _ := operations.Digest(f.operation.Operation)
			input := CompletionRequest{Version: Version, OperationID: f.operation.OperationID, RequestSHA256: digest, GrantSHA256: operations.GrantDigest(f.operation.Grant), WorkerID: f.operation.WorkerID, Owner: f.operation.Owner, Claim: f.operation.Claim}
			f.config.AuthorizeCompletion = func(context.Context, CompletionRequest) (CompletionAuthority, error) {
				a := CompletionAuthority{Request: input, ClusterTenant: "cluster", ValidUntil: time.Now().Add(time.Minute)}
				switch scenario {
				case "changed grant":
					a.Request.GrantSHA256 = strings.Repeat("f", 64)
				case "wrong tenant":
					a.ClusterTenant = "other"
				case "missing state":
					return a, errors.New("missing")
				case "expired state":
					a.ValidUntil = time.Now().Add(-time.Second)
				}
				return a, nil
			}
			completed := false
			f.config.CompleteOperation = func(context.Context, CompletionAuthority) error { completed = true; return nil }
			w := serveFixture(t, f.config, f.request(t, CompletionPath, input))
			if scenario == "valid" {
				if w.Code != http.StatusNoContent || !completed {
					t.Fatal("valid cleanup refused")
				}
			} else if w.Code < 400 || completed {
				t.Fatal("unbound cleanup released custody")
			}
		})
	}
}

func TestCompletionKeepsAdmissionWhileCredentialHooksAreBlocked(t *testing.T) {
	f := newFixture(t)
	f.config.MaxConcurrent = 1
	started, unblock := make(chan struct{}), make(chan struct{})
	f.config.ResolveQuery = func(context.Context, QueryRequest, delegation.Claims, Authorization) (QueryMaterial, error) {
		close(started)
		<-unblock
		return QueryMaterial{}, ErrInvalid
	}
	digest, _ := operations.Digest(f.operation.Operation)
	input := CompletionRequest{Version: Version, OperationID: f.operation.OperationID, RequestSHA256: digest, GrantSHA256: operations.GrantDigest(f.operation.Grant), WorkerID: f.operation.WorkerID, Owner: f.operation.Owner, Claim: f.operation.Claim}
	f.config.AuthorizeCompletion = func(context.Context, CompletionRequest) (CompletionAuthority, error) {
		return CompletionAuthority{Request: input, ClusterTenant: "cluster", ValidUntil: time.Now().Add(time.Minute)}, nil
	}
	f.config.CompleteOperation = func(context.Context, CompletionAuthority) error { return nil }
	h, err := NewHandler(f.config)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	defer func() {
		close(unblock)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("released credential hook did not return")
		}
	}()
	request := f.request(t, QueryPath, f.query)
	go func() { h.ServeHTTP(httptest.NewRecorder(), request); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("credential hook did not start")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, f.request(t, CompletionPath, input))
	if w.Code != http.StatusNoContent {
		t.Fatal("ordinary resolution starved physical cleanup")
	}
	if len(h.slots) != 1 {
		t.Fatal("blocked hook lost admission")
	}
}
