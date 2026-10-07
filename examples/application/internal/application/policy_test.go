// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package application

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
	"github.com/SYNEHQ/kelvo-go/resolver"
	"go.yaml.in/yaml/v3"
)

func saveYAML(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := yaml.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func policyFixture(t *testing.T) (Store, Policy) {
	t.Helper()
	dir := t.TempDir()
	p := Policy{Version: 1, Subjects: map[string]Subject{
		"alice": {Kind: "user", Team: "team-a", Connections: map[string]string{"saved-a": "write"}},
		"bob":   {Kind: "user", Team: "team-b", Connections: map[string]string{"saved-b": "read"}},
	}, Connections: map[string]Connection{}}
	for _, team := range []string{"a", "b"} {
		id := "saved-" + team
		p.Connections[id] = Connection{Team: "team-" + team, Type: "postgres", Database: "example", Revision: "1", CredentialsFile: id + ".yaml", ReadSQL: "SELECT id, value FROM app_" + team + " WHERE id=$1", WriteSQL: "INSERT INTO app_" + team + "(id,value) VALUES($1,$2)", CancelSQL: "SELECT pg_sleep(30)"}
		// Synthetic markers exercise fresh lookup; no real credential is a fixture.
		if err := os.WriteFile(filepath.Join(dir, id+".secret"), []byte("synthetic-source-"+team+"-v1"), 0600); err != nil {
			t.Fatal(err)
		}
		saveYAML(t, filepath.Join(dir, id+".yaml"), credentialReference{Revision: "1", DSNFile: id + ".secret"})
	}
	s := Store{Config: Config{Version: 1, PolicyFile: filepath.Join(dir, "policy.yaml")}}
	saveYAML(t, s.Config.PolicyFile, p)
	return s, p
}

func TestTwoTeamsAndSourceOwnershipFailClosed(t *testing.T) {
	s, p := policyFixture(t)
	ctx := context.Background()
	for _, test := range []struct {
		team, subject, id string
		write, allowed    bool
	}{
		{"team-a", "alice", "saved-a", true, true}, {"team-b", "bob", "saved-b", false, true},
		{"team-b", "bob", "saved-b", true, false}, {"team-a", "alice", "saved-b", false, false},
		{"team-b", "alice", "saved-a", false, false}, {"team-a", "unknown", "saved-a", false, false},
	} {
		_, _, err := s.Select(ctx, test.team, test.subject, "user", test.id, test.write)
		if (err == nil) != test.allowed {
			t.Fatalf("unexpected authorization decision for %s/%s", test.team, test.subject)
		}
	}
	u := p.Subjects["alice"]
	u.Connections["saved-b"] = "write"
	p.Subjects["alice"] = u
	saveYAML(t, s.Config.PolicyFile, p)
	if _, _, err := s.Select(ctx, "team-a", "alice", "user", "saved-b", true); err == nil {
		t.Fatal("membership bypassed source ownership")
	}
}

func TestCredentialsAreFreshAndRevisionBound(t *testing.T) {
	s, p := policyFixture(t)
	ctx := context.Background()
	connection, before, err := s.Select(ctx, "team-a", "alice", "user", "saved-a", false)
	if err != nil {
		t.Fatal(err)
	}
	_, secrets, err := s.material(ctx, connection)
	if err != nil || secrets["KELVO_SOURCE_REQUEST_0_DSN"] != "synthetic-source-a-v1" {
		t.Fatal("initial credential lookup failed")
	}
	dir := filepath.Dir(s.Config.PolicyFile)
	if err := os.WriteFile(filepath.Join(dir, "saved-a-v2.secret"), []byte("synthetic-source-a-v2"), 0600); err != nil {
		t.Fatal(err)
	}
	saveYAML(t, filepath.Join(dir, "saved-a-v2.yaml"), credentialReference{Revision: "2", DSNFile: "saved-a-v2.secret"})
	connection.CredentialsFile = "saved-a-v2.yaml"
	if _, _, err := s.material(ctx, connection); err == nil {
		t.Fatal("mixed credential revisions were accepted")
	}
	connection.Revision = "2"
	p.Connections["saved-a"] = connection
	saveYAML(t, s.Config.PolicyFile, p)
	_, after, err := s.Select(ctx, "team-a", "alice", "user", "saved-a", false)
	if err != nil || before.Revision == after.Revision {
		t.Fatal("credential rotation did not revoke prior authorization")
	}
	_, secrets, err = s.material(ctx, connection)
	if err != nil || secrets["KELVO_SOURCE_REQUEST_0_DSN"] != "synthetic-source-a-v2" {
		t.Fatal("new credentials were not fetched")
	}
	request := resolver.OperationRequest{Operation: operations.Request{Kind: operations.QueryRead, Connection: operations.ConnectionRef{ID: "saved-a"}}}
	claims := operations.GrantClaims{AppTeam: "team-a", Subject: operations.Subject{Kind: "user", ID: "alice"}}
	if _, err := s.ResolveOperation(ctx, request, claims, before); err == nil {
		t.Fatal("stale authorization loaded revised credentials")
	}
	delete(p.Subjects, "alice")
	saveYAML(t, s.Config.PolicyFile, p)
	if _, err := s.ResolveOperation(ctx, request, claims, after); err == nil {
		t.Fatal("revoked membership loaded credentials")
	}
}

type testLease struct{ token string }

func (l testLease) ValidateConnectionLease(_ context.Context, id, token string, binding resolver.Binding) (time.Time, error) {
	if id != "query-1" || token != l.token || binding.WorkerID != "worker-a" || binding.Validate() != nil {
		return time.Time{}, resolver.ErrInvalid
	}
	return time.Now().Add(4 * time.Second), nil
}
func (testLease) ValidateOperationLease(context.Context, string, string, resolver.Binding) (time.Time, error) {
	return time.Time{}, resolver.ErrInvalid
}

func TestHandlerRejectsChangedAndExpiredGrantsBeforeCredentials(t *testing.T) {
	s, p := policyFixture(t)
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s.Config.Identity = Identity{Issuer: "example-app", Audience: "analytics", ClusterTenant: "demo", ServicePrincipal: "app", PublicKeyEnv: "EXAMPLE_TEST_PUBLIC_KEY"}
	t.Setenv(s.Config.Identity.PublicKeyEnv, base64.StdEncoding.EncodeToString(pub))
	r := query.Request{Mode: "native", ConnectionID: "source_1", SQL: p.Connections["saved-a"].ReadSQL, Parameters: []query.Parameter{{Type: "string", Value: json.RawMessage(`"row-1"`)}}}
	digest, err := delegation.QueryDigest(r)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	claims := delegation.Claims{Version: 1, Issuer: "example-app", Audience: "analytics", ClusterTenant: "demo", ServicePrincipal: "app", AppTeam: "team-a", Subject: delegation.Subject{Kind: "user", ID: "alice"}, ID: "0123456789abcdef0123456789abcdef0123456789abcdef", IssuedAt: now.Unix(), ExpiresAt: now.Unix() + 240, QuerySHA256: digest, Sources: []delegation.Source{{Alias: "source_1", ConnectionID: "saved-a", Database: "example"}}}
	token, err := delegation.Sign(claims, key)
	if err != nil {
		t.Fatal(err)
	}
	identity, _ := url.Parse("spiffe://kelvo/tenant/demo/worker/worker-a")
	leaf := &x509.Certificate{NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), URIs: []*url.URL{identity}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	for _, test := range []struct {
		name              string
		change            func(*resolver.QueryRequest)
		expired           bool
		foreignConnection bool
		missingPolicy     bool
		want              int
	}{
		{name: "current", want: http.StatusOK},
		{name: "changed SQL", change: func(r *resolver.QueryRequest) { r.Query.SQL += " LIMIT 1" }, want: http.StatusForbidden},
		{name: "expired", expired: true, want: http.StatusForbidden},
		{name: "foreign saved connection", foreignConnection: true, want: http.StatusForbidden},
		{name: "unavailable policy", missingPolicy: true, want: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := resolver.QueryRequest{Delegation: token, Query: r, JobID: "query-1", WorkerID: "worker-a", Owner: "0123456789abcdef0123456789abcdef", Claim: "abcdef0123456789abcdef0123456789"}
			if test.change != nil {
				test.change(&input)
			}
			if test.expired {
				expired := claims
				expired.IssuedAt = now.Unix() - 300
				expired.ExpiresAt = now.Unix() - 1
				input.Delegation, err = delegation.Sign(expired, key)
				if err != nil {
					t.Fatal(err)
				}
			}
			if test.foreignConnection {
				foreign := claims
				foreign.Sources = []delegation.Source{{Alias: "source_1", ConnectionID: "saved-b", Database: "example"}}
				input.Delegation, err = delegation.Sign(foreign, key)
				if err != nil {
					t.Fatal(err)
				}
			}
			store := s
			if test.missingPolicy {
				store.Config.PolicyFile = filepath.Join(t.TempDir(), "absent-policy.yaml")
			}
			auditPath := filepath.Join(t.TempDir(), "audit.json")
			store.Audit, err = NewAudit(auditPath)
			if err != nil {
				t.Fatal(err)
			}
			handler, err := store.Handler(testLease{token: input.Delegation})
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(input)
			request := httptest.NewRequest(http.MethodPost, resolver.QueryPath, bytes.NewReader(raw))
			request.Header.Set("Content-Type", "application/json")
			// Unit-test transport seam; the executable uses a real verifying mTLS listener.
			request.TLS = &tls.ConnectionState{HandshakeComplete: true, Version: tls.VersionTLS13, PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status %d, want %d", response.Code, test.want)
			}
			raw, err = os.ReadFile(auditPath)
			if err != nil {
				t.Fatal(err)
			}
			var counts AuditCounts
			if err := json.Unmarshal(raw, &counts); err != nil {
				t.Fatal(err)
			}
			if test.want == http.StatusForbidden && counts.CredentialLookups != 0 {
				t.Fatal("invalid grant reached credential lookup")
			}
			if test.foreignConnection && counts.QueryPolicyDenied != 1 {
				t.Fatal("explicit policy denial was not recorded")
			}
			if test.missingPolicy && (counts.QueryPolicyDenied != 0 || counts.QueryAuthorizationFailed != 1) {
				t.Fatal("unavailable policy was misreported as an explicit denial")
			}
		})
	}
}
