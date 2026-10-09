//go:build linux

package worker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/rabbitconnect"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
	"github.com/SYNEHQ/kelvo-go/sourceproof"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type budgetFixtureIssuer func(context.Context, rabbitconnect.IssueRequest) (string, error)

func (f budgetFixtureIssuer) Issue(ctx context.Context, r rabbitconnect.IssueRequest) (string, error) {
	return f(ctx, r)
}

// This opt-in test uses the source-matched test executable as the parent and
// the pinned real adapter as its contained child. Authority services are local
// fixtures. PostgreSQL, child IPC, proof verification and Arrow are real paths.
// It does not qualify the deployed node CLI or DBAPI/KMS/Rabbit services.
func TestContainedPrivateBudgetRealAdapter(t *testing.T) {
	get := func(name string) string { return os.Getenv("KELVO_TEST_PRIVATE_BUDGET_" + name) }
	for _, name := range []string{"ADAPTER", "ADAPTER_SHA256", "POSTGRES_DSN", "SOURCE_CA_FILE", "SOURCE_SHA256"} {
		if get(name) == "" {
			t.Skip("explicit disposable private-budget fixture required")
		}
	}
	dsn, err := url.Parse(get("POSTGRES_DSN"))
	if err != nil || (dsn.Scheme != "postgres" && dsn.Scheme != "postgresql") || net.ParseIP(dsn.Hostname()) == nil || !net.ParseIP(dsn.Hostname()).IsLoopback() || !strings.HasPrefix(strings.TrimPrefix(dsn.Path, "/"), "kelvo_budget_") || dsn.Query().Get("sslmode") != "verify-full" {
		t.Fatal("private-budget source must be a scoped loopback PostgreSQL TLS fixture")
	}
	ca, err := os.ReadFile(get("SOURCE_CA_FILE"))
	if err != nil || len(ca) > 64<<10 {
		t.Fatal("private-budget source CA unavailable")
	}
	if !operations.ValidDigest(get("ADAPTER_SHA256")) || !operations.ValidDigest(get("SOURCE_SHA256")) {
		t.Fatal("private-budget artifact hashes are required")
	}
	parent, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	hashFile := func(path string) string {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal("private-budget artifact unavailable")
		}
		defer f.Close()
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			t.Fatal("private-budget artifact hash failed")
		}
		return hex.EncodeToString(h.Sum(nil))
	}
	if hashFile(get("ADAPTER")) != get("ADAPTER_SHA256") {
		t.Fatal("private-budget adapter hash changed")
	}
	for _, mode := range []string{"slow_refresh", "cancel_refresh", "expired_authority", "transport_stall"} {
		t.Run(mode, func(t *testing.T) {
			executor, manager, pool := containedExecutor(t)
			op, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			request := operations.Request{Version: 1, Kind: operations.QueryRead, Connection: operations.ConnectionRef{ID: "saved-a", Database: strings.TrimPrefix(dsn.Path, "/"), Schema: "public"}, Spec: operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT 4242::bigint AS sentinel"}}}
			var refreshCancelled atomic.Bool
			refreshStopped := make(chan struct{})
			f := newPrivateResolutionFixtureWithOptions(t, privateResolutionFixtureOptions{Timeout: 4 * time.Second, Request: &request, DSN: get("POSTGRES_DSN"), SourceOptions: map[string]string{"tls_ca_pem": string(ca)}, BeforeResponse: func(ctx context.Context, call int32) error {
				if call != 2 {
					return nil
				}
				if mode == "cancel_refresh" {
					defer close(refreshStopped)
					cancel()
					select {
					case <-ctx.Done():
						refreshCancelled.Store(true)
						return ctx.Err()
					case <-time.After(time.Second):
						return errors.New("resolver cancellation did not arrive")
					}
				}
				if mode == "slow_refresh" {
					timer := time.NewTimer(2300 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}})
			f.change = func(wire *resolver.OperationResponse) {
				if pool.Snapshot().Active != 1 || manager.Status().Active != 1 {
					t.Error("private credentials resolved before resource admission")
				}
				if mode == "expired_authority" && f.calls.Load() == 2 {
					wire.ValidUntil = time.Now().Add(-time.Second).Unix()
				}
			}
			executor.connectionResolvers = f.executor.connectionResolvers
			runtime := privateRuntimeFixture(t, f)
			proxy, issuer, configuration := newBudgetFixtureProxy(t, f, dsn.Host, mode == "transport_stall")
			runtime.opener = func(scope sourceproof.Scope, refresh func(context.Context) (sourceproof.Envelope, error)) (transportbroker.Opener, error) {
				return rabbitconnect.NewWithSourceProof(configuration, issuer, rabbitconnect.SourceProofConfig{RefreshTimeout: 4 * time.Second, PublicKey: f.policy.proofKey, Scope: scope, Refresh: refresh})
			}
			var sentinel bool
			sink := &workerTestSink{write: func(batch arrow.RecordBatch) error {
				if batch.NumRows() != 1 || batch.NumCols() != 1 {
					return errors.New("private-budget result shape differs")
				}
				column, ok := batch.Column(0).(*array.Int64)
				if !ok || column.IsNull(0) || column.Value(0) != 4242 {
					return errors.New("private-budget sentinel differs")
				}
				sentinel = true
				return nil
			}}
			started := time.Now()
			receipt, runErr := executor.executePrivateOperation(op, runtime, OperationProcessConfig{Binary: get("ADAPTER"), SHA256: get("ADAPTER_SHA256")}, f.record, f.request, nil, sink)
			elapsed := time.Since(started)
			if f.calls.Load() != 2 {
				t.Fatalf("private-budget resolver calls: got %d, want 2", f.calls.Load())
			}
			if mode == "slow_refresh" {
				if runErr != nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectNone || !sentinel || sink.rows != 1 || proxy.issued.Load() != 1 || proxy.dials.Load() != 1 || elapsed < 2300*time.Millisecond {
					t.Fatal("slow private refresh did not produce the exact real-adapter result", receipt.Outcome, runErr)
				}
			} else {
				if runErr == nil || sentinel || proxy.dials.Load() != 0 {
					t.Fatal("denied private open reached the source or returned rows")
				}
				if mode == "transport_stall" {
					if proxy.issued.Load() != 1 || proxy.connects.Load() != 1 || elapsed < 1800*time.Millisecond || elapsed > 6*time.Second {
						t.Fatal("transport stall did not retain the short setup budget")
					}
				} else if proxy.issued.Load() != 0 || proxy.connects.Load() != 0 {
					t.Fatal("cancelled or expired authority reached ticket issuance")
				}
			}
			if mode == "cancel_refresh" {
				select {
				case <-refreshStopped:
				case <-time.After(time.Second):
					t.Fatal("cancelled resolver handler did not join")
				}
				if !refreshCancelled.Load() {
					t.Fatal("operation cancellation did not reach the resolver")
				}
			}
			proxy.close(t)
			if pool.Snapshot().Active != 0 || manager.Status().Active != 0 || runtime.broker.Snapshot().Sessions != 0 || len(runtime.entries) != 0 || runtime.pending != 0 {
				t.Fatal("private-budget operation retained process or transport custody")
			}
			configBytes, _ := json.Marshal(struct {
				Mode, DSN, SourceCA string
				RefreshMS, SetupMS  int
			}{mode, get("POSTGRES_DSN"), string(ca), 4000, 2000})
			configHash := sha256.Sum256(configBytes)
			clear(configBytes)
			observation, _ := json.Marshal(map[string]any{"mode": mode, "source_sha256": get("SOURCE_SHA256"), "parent_test_sha256": hashFile(parent), "adapter_sha256": hashFile(get("ADAPTER")), "launcher_sha256": hashFile(os.Getenv("KELVO_TEST_SANDBOX")), "config_sha256": hex.EncodeToString(configHash[:]), "resolver_calls": f.calls.Load(), "issuer_calls": proxy.issued.Load(), "proxy_connects": proxy.connects.Load(), "proxy_source_dials": proxy.dials.Load(), "elapsed_ms": elapsed.Milliseconds(), "cleanup_confirmed": true, "authority_services": "synthetic", "source": "real_postgresql"})
			t.Log("PRIVATE_BUDGET_RECEIPT " + string(observation))
		})
	}
}

type budgetFixtureProxy struct {
	server                  *httptest.Server
	issued, connects, dials atomic.Int32
	mu                      sync.Mutex
	tokens                  map[string]string
	connections             map[net.Conn]struct{}
	wait                    sync.WaitGroup
	once                    sync.Once
}

func (p *budgetFixtureProxy) close(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		p.mu.Lock()
		for conn := range p.connections {
			_ = conn.Close()
		}
		p.mu.Unlock()
		p.server.Close()
		finished := make(chan struct{})
		go func() { p.wait.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Fatal("private-budget proxy cleanup did not finish")
		}
	})
}

func newBudgetFixtureProxy(t *testing.T, f *privateResolutionFixture, source string, stall bool) (*budgetFixtureProxy, rabbitconnect.Issuer, rabbitconnect.Config) {
	t.Helper()
	public, signing, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p := &budgetFixtureProxy{tokens: map[string]string{}, connections: map[net.Conn]struct{}{}}
	p.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		p.connects.Add(1)
		p.mu.Lock()
		authority, ok := p.tokens[request.Header.Get("Proxy-Authorization")]
		delete(p.tokens, request.Header.Get("Proxy-Authorization"))
		p.mu.Unlock()
		if request.Method != http.MethodConnect || !ok || authority != source || request.Host != source {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if stall {
			timer := time.NewTimer(2500 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-request.Context().Done():
			}
			return
		}
		client, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		p.wait.Add(1)
		defer p.wait.Done()
		defer client.Close()
		p.mu.Lock()
		p.connections[client] = struct{}{}
		p.mu.Unlock()
		defer func() { p.mu.Lock(); delete(p.connections, client); p.mu.Unlock() }()
		p.dials.Add(1)
		target, err := (&net.Dialer{Timeout: time.Second}).DialContext(request.Context(), "tcp", source)
		if err != nil {
			return
		}
		defer target.Close()
		_ = target.SetDeadline(time.Now().Add(10 * time.Second))
		_ = client.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err = io.WriteString(client, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		copied := make(chan struct{})
		go func() { _, _ = io.Copy(target, buffered); _ = target.Close(); close(copied) }()
		_, _ = io.Copy(client, target)
		_ = client.Close()
		_ = target.Close()
		<-copied
	}))
	p.server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert, VerifyConnection: func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) != 1 || !bytes.Equal(state.PeerCertificates[0].Raw, f.policy.certificate) {
			return errors.New("private-budget worker certificate differs")
		}
		return nil
	}}
	p.server.StartTLS()
	t.Cleanup(func() { p.close(t) })
	pair := f.policy.resolver.client.Transport.(*http.Transport).TLSClientConfig.Certificates[0]
	privateDER, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	proxyURL, _ := url.Parse(p.server.URL)
	config := rabbitconnect.Config{ProxyAddress: proxyURL.Host, ProxyServerName: proxyURL.Hostname(), RootCAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.server.Certificate().Raw}), ClientCertificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.policy.certificate}), ClientKeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}), WorkerIdentity: f.policy.identity, Issuer: f.policy.trust.Issuer, Audience: f.policy.trust.Audience, ClusterTenant: f.policy.trust.ClusterTenant, ServicePrincipal: f.policy.trust.ServicePrincipal, IssuerPublicKey: public, SetupTimeout: 2 * time.Second}
	issuer := budgetFixtureIssuer(func(ctx context.Context, request rabbitconnect.IssueRequest) (string, error) {
		if ctx.Err() != nil || request.PrivateSource == nil {
			return "", errors.New("private-budget issue has no current proof")
		}
		b, e := request.Binding, request.Binding.Execution
		proof, err := sourceproof.Verify(f.policy.proofKey, *request.PrivateSource, sourceproof.Scope{Issuer: b.Issuer, Audience: b.Audience, ClusterTenant: b.ClusterTenant, ServicePrincipal: b.ServicePrincipal, Tenant: b.Tenant, Source: b.Source, SourceRevision: b.SourceRevision, Authority: b.Authority, RouteID: request.RouteID, TokenID: request.TokenID, BindingVersion: request.BindingVersion, Kind: e.Kind, ExecutionID: e.ID, GrantSHA256: e.GrantSHA256, Worker: e.Worker, Owner: e.Owner, Claim: e.Claim, WorkerIdentity: request.WorkerIdentity, WorkerCertSHA256: request.WorkerCertSHA256}, time.Now())
		if err != nil {
			return "", err
		}
		p.issued.Add(1)
		now := time.Now().Unix()
		claims := map[string]any{"version": 1, "iss": b.Issuer, "aud": b.Audience, "jti": request.OpenID, "iat": now, "exp": min(now+3, proof.ExpiresAt), "session_expires_at": b.ExpiresAt.Unix(), "cluster_tenant": b.ClusterTenant, "service_principal": b.ServicePrincipal, "tenant": b.Tenant, "source": b.Source, "source_revision": b.SourceRevision, "token_id": request.TokenID, "token_generation": strings.Repeat("e", 64), "tunnel_id": strings.Repeat("f", 64), "control_owner": strings.Repeat("a", 64), "authority": b.Authority, "worker_identity": request.WorkerIdentity, "worker_cert_sha256": request.WorkerCertSHA256, "execution": map[string]any{"kind": e.Kind, "id": e.ID, "grant_sha256": e.GrantSHA256, "worker_id": e.Worker, "owner": e.Owner, "claim": e.Claim}}
		header, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "rabbit-connect+jwt", "kid": b.Issuer})
		body, _ := json.Marshal(claims)
		encoded := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body)
		token := encoded + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(signing, []byte(encoded)))
		p.mu.Lock()
		p.tokens["Bearer "+token] = b.Authority
		p.mu.Unlock()
		return token, nil
	})
	return p, issuer, config
}
