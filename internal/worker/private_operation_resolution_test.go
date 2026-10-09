// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
	"github.com/SYNEHQ/kelvo-go/sourceproof"
)

type privateResolutionFixture struct {
	policy       *privateOperationPolicy
	executor     *Executor
	record       ledger.Record
	request      operations.Request
	calls        atomic.Int32
	change       func(*resolver.OperationResponse)
	claimsChange func(*sourceproof.Scope)
}

func newPrivateResolutionFixture(t *testing.T) *privateResolutionFixture {
	t.Helper()
	f := &privateResolutionFixture{}
	record, request, response := operationResolverFixture(t)
	now := time.Now()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	trust := operations.GrantTrust{Issuer: "gateway", Audience: "kelvo", ClusterTenant: "shared", ServicePrincipal: "api", PublicKey: public}
	claims := operations.GrantClaims{Version: operations.GrantVersion, Issuer: trust.Issuer, Audience: trust.Audience, ClusterTenant: trust.ClusterTenant, ServicePrincipal: trust.ServicePrincipal, AppTeam: record.Scope.AppTeam, Subject: operations.Subject{Kind: record.Scope.SubjectKind, ID: record.Scope.SubjectID}, ID: "grant-private-fixture", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix(), ConnectionID: record.Scope.ConnectionID, Operation: request.Kind, RequestSHA256: record.RequestSHA256, Authorization: operations.Authorization{Kind: "api_key"}}
	record.AuthorityToken, err = operations.SignGrant(claims, key)
	if err != nil {
		t.Fatal(err)
	}
	record.AuthoritySHA256 = operations.GrantDigest(record.AuthorityToken)
	record.AuthorityUntil = time.Unix(claims.ExpiresAt, 0)
	record.ExecuteBefore = record.AuthorityUntil
	f.record, f.request = record, request
	_, tlsKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, tlsKey.Public(), tlsKey)
	if err != nil {
		t.Fatal(err)
	}
	identity := "spiffe://kelvo/tenant/shared/worker/worker"
	uri, _ := url.Parse(identity)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{uri}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, tlsKey.Public(), tlsKey)
	if err != nil {
		t.Fatal(err)
	}
	certificateHash := sha256.Sum256(der)
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(caCert)
	parsedDSN, _ := url.Parse(connectionFixtureDSN)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || r.URL.Path != resolver.OperationPath {
			t.Error("resolver request lacked verified transport")
			w.WriteHeader(403)
			return
		}
		wire := resolver.OperationResponse{Version: resolver.Version, GrantSHA256: record.AuthoritySHA256, RequestSHA256: record.RequestSHA256, SourceRevision: response.SourceRevision, ValidUntil: time.Now().Add(4 * time.Second).Unix(), Source: resolver.Source{ID: "source_1", Type: "postgres", DSNEnv: response.Source.DSNEnv}, Secrets: map[string]string{response.Source.DSNEnv: connectionFixtureDSN}}
		scope := sourceproof.Scope{Issuer: trust.Issuer, Audience: trust.Audience, ClusterTenant: trust.ClusterTenant, ServicePrincipal: trust.ServicePrincipal, Tenant: record.Scope.AppTeam, Source: record.Scope.ConnectionID, SourceRevision: wire.SourceRevision, Authority: parsedDSN.Host, RouteID: "private-route", TokenID: "12345678-1234-1234-1234-123456789abc", BindingVersion: 1, Kind: "operation", ExecutionID: record.ID, GrantSHA256: record.AuthoritySHA256, Worker: record.Binding.WorkerID, Owner: record.Binding.Owner, Claim: record.Binding.Claim, WorkerIdentity: identity, WorkerCertSHA256: hex.EncodeToString(certificateHash[:])}
		if f.claimsChange != nil {
			f.claimsChange(&scope)
		}
		proof, err := sourceproof.Sign(key, scope, record.AuthorityToken, time.Now(), time.Unix(wire.ValidUntil, 0))
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		wire.PrivateSource = &proof
		if f.change != nil {
			f.change(&wire)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(wire)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	t.Cleanup(server.Close)
	directory := t.TempDir()
	write := func(name string, data []byte) string {
		path := filepath.Join(directory, name)
		if os.WriteFile(path, data, 0600) != nil {
			t.Fatal("fixture TLS write failed")
		}
		return path
	}
	encodedKey, _ := x509.MarshalPKCS8PrivateKey(tlsKey)
	config := ConnectionResolverConfig{URL: server.URL + resolver.QueryPath, Timeout: time.Second, MaxConcurrent: 2, CAFile: write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})), CertFile: write("worker.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), KeyFile: write("worker-key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encodedKey}))}
	connection, err := NewConnectionResolver(config, identity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(connection.Close)
	f.executor = &Executor{Limits: query.DefaultLimits(), connectionResolvers: map[string]*ConnectionResolver{trust.Issuer: connection}}
	f.policy, err = newPrivateOperationPolicy(connection, trust, public, "private-route", record.Binding.WorkerID, identity, der)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestPrivateOperationResolutionVerifiesGrantAndKeepsProofInParent(t *testing.T) {
	f := newPrivateResolutionFixture(t)
	selection, err := f.policy.resolve(context.Background(), f.executor, f.record, f.request, nil)
	if err != nil {
		t.Fatal(err)
	}
	if selection.input.Source.Engine != "postgresql" || selection.scope.TokenID == "" || selection.scope.BindingVersion != 1 || selection.proof.Grant != f.record.AuthorityToken || !privateInputScope(selection.input, selection.binding) {
		t.Fatal("private resolution lost verified selection")
	}
	raw, err := adapter.EncodeProcessRequest(selection.input)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), selection.proof.Token) || strings.Contains(string(raw), selection.proof.Grant) || strings.Contains(string(raw), "private_source") {
		t.Fatal("parent proof entered child input")
	}
	if _, err := f.executor.ResolveOperationSource(context.Background(), f.record, f.request); err == nil {
		t.Fatal("ordinary path accepted private source")
	}
}

func TestPrivateOperationResolutionRejectsChangedAuthorityBeforeRequest(t *testing.T) {
	changes := map[string]func(*privateResolutionFixture){
		"signature": func(f *privateResolutionFixture) {
			f.record.AuthorityToken += "x"
			f.record.AuthoritySHA256 = operations.GrantDigest(f.record.AuthorityToken)
		},
		"subject":          func(f *privateResolutionFixture) { f.record.Scope.SubjectID = "other" },
		"worker":           func(f *privateResolutionFixture) { f.record.Binding.WorkerID = "other" },
		"request":          func(f *privateResolutionFixture) { f.request.Spec.Statement.SQL = "DELETE FROM analytics" },
		"grant expiry":     func(f *privateResolutionFixture) { f.record.AuthorityUntil = f.record.AuthorityUntil.Add(time.Second) },
		"execution expiry": func(f *privateResolutionFixture) { f.record.ExecuteBefore = time.Now().Add(-time.Second) },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			f := newPrivateResolutionFixture(t)
			change(f)
			if _, err := f.policy.resolve(context.Background(), f.executor, f.record, f.request, nil); err == nil || f.calls.Load() != 0 {
				t.Fatal("unverified authority reached resolver", err, f.calls.Load())
			}
		})
	}
}

func TestPrivateOperationResolutionRejectsChangedProofAndSource(t *testing.T) {
	for _, field := range []string{"tenant", "route", "worker", "certificate", "revision", "authority", "owner", "claim", "missing", "source", "dsn"} {
		t.Run(field, func(t *testing.T) {
			f := newPrivateResolutionFixture(t)
			f.claimsChange = func(s *sourceproof.Scope) {
				switch field {
				case "tenant":
					s.Tenant = "other"
				case "route":
					s.RouteID = "other"
				case "worker":
					s.Worker = "other"
				case "certificate":
					s.WorkerCertSHA256 = strings.Repeat("d", 64)
				case "revision":
					s.SourceRevision = strings.Repeat("d", 64)
				case "authority":
					s.Authority = "other.invalid:5432"
				case "owner":
					s.Owner = strings.Repeat("c", 32)
				case "claim":
					s.Claim = strings.Repeat("c", 32)
				}
			}
			f.change = func(w *resolver.OperationResponse) {
				switch field {
				case "missing":
					w.PrivateSource = nil
				case "source":
					w.Source.Type = "redis"
				case "dsn":
					w.Secrets[w.Source.DSNEnv] = strings.Replace(connectionFixtureDSN, "/analytics?", "/other?", 1)
				}
			}
			if _, err := f.policy.resolve(context.Background(), f.executor, f.record, f.request, nil); err == nil {
				t.Fatal("changed private source accepted")
			}
		})
	}
}

func TestPrivateOperationPolicyPinsActualResolverCertificate(t *testing.T) {
	f := newPrivateResolutionFixture(t)
	wrong := append([]byte(nil), f.policy.certificate...)
	wrong[len(wrong)-1] ^= 1
	if _, err := newPrivateOperationPolicy(f.policy.resolver, f.policy.trust, f.policy.proofKey, f.policy.route, f.policy.worker, f.policy.identity, wrong); err == nil {
		t.Fatal("mismatched resolver and broker worker certificate accepted")
	}
}
