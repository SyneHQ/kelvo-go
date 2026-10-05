//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/authfence"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/testutil/authoritybroker"
)

func gatewayAuthorityAttempt(t *testing.T) authfence.Attempt {
	t.Helper()
	start := time.Now()
	attempt, err := authfence.NewAttempt(start, start.Add(authfence.MaxAttemptDuration))
	if err != nil {
		t.Fatal(err)
	}
	return attempt
}

func gatewayAuthorityFixtureClient(t *testing.T, endpoint authoritybroker.Endpoint, scope authfence.Scope) *authfence.Client {
	t.Helper()
	client, err := authfence.NewClient(authfence.Config{URL: endpoint.URL, CAFile: endpoint.CAFile,
		CertFile: endpoint.CertFile, KeyFile: endpoint.KeyFile, Username: endpoint.Username,
		PasswordEnv: endpoint.PasswordEnv, Scope: scope, ReplicaID: authfence.ControlReplicaID})
	if err != nil {
		t.Fatal("invalid fixture control references", err)
	}
	return client
}

func gatewayAuthorityCloseClient(t *testing.T, client *authfence.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatal("authority control cleanup failed", err)
	}
	select {
	case <-client.Quiesced():
	default:
		t.Fatal("authority control retained work")
	}
}

// A retry cannot establish who performed a prior uncertain mutation. Only a
// separate, fresh control witness is accepted as proof of the current record.
func gatewayAuthoritySetCurrent(t *testing.T, fixture *authoritybroker.Fixture, scope authfence.Scope, raw []byte, revision uint64) authfence.Snapshot {
	t.Helper()
	digest := sha256.Sum256(raw)
	document, err := authfence.NewDocument(revision, hex.EncodeToString(digest[:]))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		role := "control"
		if revision == 1 {
			role = "initializer"
		}
		client := gatewayAuthorityFixtureClient(t, fixture.Config(0, role), scope)
		var mutation authfence.MutationResult
		if revision == 1 {
			mutation, err = client.Initialize(ctx, gatewayAuthorityAttempt(t), document)
		} else {
			mutation, err = client.Advance(ctx, gatewayAuthorityAttempt(t), revision-1, document)
		}
		gatewayAuthorityCloseClient(t, client)
		if errors.Is(err, authfence.ErrDenied) || errors.Is(err, authfence.ErrInvalid) {
			t.Fatal("fixture control operation rejected its scope or role", err)
		}
		client = gatewayAuthorityFixtureClient(t, fixture.Config(0, "control"), scope)
		current, err := client.Check(ctx, gatewayAuthorityAttempt(t), document)
		gatewayAuthorityCloseClient(t, client)
		t.Logf("AUTHORITY_CONTROL revision=%d mutation=%s check=%s verified=%t", revision, mutation.Outcome(), current.Outcome(), err == nil)
		if err == nil && current.Outcome() == authfence.OutcomeCurrentVerified && current.Snapshot().Record().Document().Equal(document) {
			return current.Snapshot()
		}
		select {
		case <-ctx.Done():
		case <-time.After(40 * time.Millisecond):
		}
	}
	t.Fatal("authority did not converge to the exact fixture document")
	return authfence.Snapshot{}
}

func gatewayAuthorityWriteKeys(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(path+".next", raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".next", path); err != nil {
		t.Fatal(err)
	}
}

type authorityGatewayFixture struct {
	config GatewayConfig
	stores map[string]Store
	gate   *Gateway
}

func newAuthorityGatewayFixture(t *testing.T, endpoint authoritybroker.Endpoint, scope authfence.Scope, replica string, raw []byte) *authorityGatewayFixture {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	workerTLS, _ := exportIntegrationTLS(t)
	config := GatewayConfig{Listen: "127.0.0.1:0", WorkerTLS: workerTLS, MaxQueries: 16, MaxConcurrent: 2, MaxHTTPRequests: 4,
		Authentication: &GatewayAuthenticationConfig{KeysFile: filepath.Join(root, "keys.yml"), ReloadInterval: time.Second,
			State: &GatewayAuthenticationStateConfig{Scope: replica, Directory: filepath.Join(root, "auth-state")},
			Authority: &GatewayKeyAuthorityConfig{Scope: scope.ID(), ReplicaID: replica, Gateways: scope.Gateways(),
				NATS: NATSConfig{URL: endpoint.URL, CAFile: endpoint.CAFile, CertFile: endpoint.CertFile,
					KeyFile: endpoint.KeyFile, Username: endpoint.Username, PasswordEnv: endpoint.PasswordEnv}}}}
	stores := make(map[string]Store)
	for _, tenant := range scope.Tenants() {
		policy := principalTestPolicy()
		policy.TenantID = tenant
		binding := keyAuthorityBinding(scope)
		policy.Access.KeyAuthority = &binding
		config.Tenants = append(config.Tenants, TenantConfig{Policy: policy,
			Workers: []Endpoint{{ID: "a1", URL: "https://127.0.0.1:1"}}})
		authority, ok := authorityForPrincipal(policy, "reports")
		if !ok {
			t.Fatal("missing fixture principal")
		}
		// Job state is an in-memory fixture; the gateway, private key/state
		// readers and authority protocol use their production implementations.
		store := &admissionStore{gatewayStore: gatewayStore{policy: policy}, jobs: map[string]Snapshot{}}
		id := "owned-" + tenant
		store.jobs[id] = Snapshot{Revision: 1, Job: Job{ID: id, TenantID: tenant, State: Queued,
			Authority: &authority, Request: query.Request{Mode: "federated", SQL: "SELECT 1"},
			CreatedAt: time.Now(), ExpiresAt: time.Now().Add(3 * time.Minute)}}
		stores[tenant] = store
	}
	gatewayAuthorityWriteKeys(t, config.Authentication.KeysFile, raw)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := InitializeGatewayAuthState(ctx, config); err != nil {
		t.Fatal("fixture local history initialization failed", err)
	}
	f := &authorityGatewayFixture{config: config, stores: stores}
	f.start(t)
	t.Cleanup(func() {
		if f.gate != nil {
			if err := f.gate.Close(); err != nil {
				t.Error("gateway retained cleanup", err)
			}
		}
	})
	return f
}

func (f *authorityGatewayFixture) start(t *testing.T) {
	t.Helper()
	gateway, err := NewGateway(f.config, f.stores)
	if err != nil {
		t.Fatal("gateway authority startup failed", err)
	}
	f.gate = gateway
}

func gatewayAuthorityHTTP(gateway *Gateway, token, path string) int {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.Header.Set("Authorization", "Bearer "+token)
	output := httptest.NewRecorder()
	gateway.ServeHTTP(output, request)
	return output.Code
}

func gatewayAuthorityWait(t *testing.T, limit time.Duration, ready func() bool) {
	t.Helper()
	end := time.Now().Add(limit)
	for time.Now().Before(end) {
		if ready() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("gateway authority condition did not become true within its fixture budget")
}

func TestGatewayAuthorityR3(t *testing.T) {
	authoritybroker.Require(t)
	for _, version := range authoritybroker.Versions() {
		t.Run(version, func(t *testing.T) {
			fixture := authoritybroker.Start(t, version)
			scope, err := authfence.NewScope("gateway-fleet", []string{"a", "b"}, []string{"gateway-a", "gateway-b"})
			if err != nil {
				t.Fatal(err)
			}
			initial := principalStateDocument(t, 1, map[string][]string{"reports": {rotationOld, rotationNew}}, map[string][]string{"reports": {rotationOther}})
			gatewayAuthoritySetCurrent(t, fixture, scope, initial, 1)
			first := newAuthorityGatewayFixture(t, fixture.GatewayConfig(0), scope, "gateway-a", initial)
			second := newAuthorityGatewayFixture(t, fixture.GatewayConfig(1), scope, "gateway-b", initial)
			statePath := filepath.Join(first.config.Authentication.State.Directory, "state.yml")
			oldState, err := os.ReadFile(statePath)
			if err != nil {
				t.Fatal(err)
			}
			if !t.Run("TenantHandlesAndWriterOwnership", func(t *testing.T) {
				for _, f := range []*authorityGatewayFixture{first, second} {
					if gatewayAuthorityHTTP(f.gate, rotationOld, "/v1/queries/owned-a") != 200 ||
						gatewayAuthorityHTTP(f.gate, rotationOther, "/v1/queries/owned-b") != 200 ||
						gatewayAuthorityHTTP(f.gate, rotationOld, "/v1/queries/owned-b") != 404 ||
						gatewayAuthorityHTTP(f.gate, rotationOther, "/v1/queries/owned-a") != 404 {
						t.Fatal("verified keys did not preserve tenant handle boundaries")
					}
					duplicate, err := NewGateway(f.config, f.stores)
					if duplicate != nil {
						_ = duplicate.Close()
						t.Fatal("duplicate gateway acquired an active state writer")
					}
					if !errors.Is(err, errGatewayAuthUnavailable) {
						t.Fatal("duplicate writer failure was not redacted")
					}
				}
			}) {
				return
			}
			current := principalStateDocument(t, 2, map[string][]string{"reports": {rotationNew}}, map[string][]string{"reports": {rotationOther}})
			if !t.Run("RotationFencesStaleGateway", func(t *testing.T) {
				_, oldContext, ok := second.gate.auth.lookup(rotationOld)
				if !ok {
					t.Fatal("old key missing before rotation")
				}
				gatewayAuthoritySetCurrent(t, fixture, scope, current, 2)
				rotated := time.Now()
				gatewayAuthorityWriteKeys(t, first.config.Authentication.KeysFile, current)
				gatewayAuthorityWait(t, gatewayAuthorityLease+500*time.Millisecond, func() bool { return !second.gate.auth.active(oldContext) })
				if time.Since(rotated) > gatewayAuthorityLease+500*time.Millisecond {
					t.Fatal("stale gateway exceeded the rotation lease bound")
				}
				gatewayAuthorityWait(t, 5*time.Second, func() bool {
					first.gate.auth.mu.Lock()
					defer first.gate.auth.mu.Unlock()
					return first.gate.auth.revision == 2 && time.Now().Before(first.gate.auth.validUntil)
				})
				if gatewayAuthorityHTTP(second.gate, rotationOld, "/v1/queries/owned-a") != 401 ||
					gatewayAuthorityHTTP(first.gate, rotationOld, "/v1/queries/owned-a") != 401 ||
					gatewayAuthorityHTTP(first.gate, rotationNew, "/v1/queries/owned-a") != 200 {
					t.Fatal("stale or removed keys survived shared authority rotation")
				}
				gatewayAuthorityWriteKeys(t, second.config.Authentication.KeysFile, current)
				gatewayAuthorityWait(t, 5*time.Second, func() bool { return gatewayAuthorityHTTP(second.gate, rotationNew, "/v1/queries/owned-a") == 200 })
			}) {
				return
			}
			if !t.Run("PartitionExpiresLocalLease", func(t *testing.T) {
				_, active, ok := first.gate.auth.lookup(rotationNew)
				if !ok {
					t.Fatal("current key missing before partition")
				}
				// Exercise a blocked Arrow delivery through the real gateway.
				// Upstream bytes and job state are fixtures; no source query runs.
				store := first.stores["a"].(*admissionStore)
				authority, _ := authorityForPrincipal(store.Policy(), "reports")
				store.mu.Lock()
				store.jobs["active"] = Snapshot{Revision: 1, Job: Job{ID: "active", TenantID: "a", State: Assigned,
					WorkerID: "a1", Owner: "owned", Authority: &authority,
					Request: query.Request{Mode: "federated", SQL: "SELECT 1"}, ExpiresAt: time.Now().Add(time.Minute)}}
				store.mu.Unlock()
				encoded, _ := compressedRelayFixture(t, "none")
				upstreamDone := make(chan struct{})
				upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(upstreamDone)
					_, _ = w.Write(encoded[:100])
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				}))
				defer upstream.Close()
				endpoint, _ := url.Parse(upstream.URL)
				first.gate.tenants["a"].workers["a1"] = workerEndpoint{url: endpoint, client: upstream.Client()}
				requestContext, cancel := context.WithCancel(context.Background())
				defer cancel()
				output := &firstRelayWrite{ResponseRecorder: httptest.NewRecorder(), started: make(chan struct{})}
				finished := make(chan any, 1)
				joined := false
				defer func() {
					cancel()
					if !joined {
						select {
						case <-finished:
						case <-time.After(3 * time.Second):
							t.Error("relay goroutine retained after fixture cancellation")
						}
					}
				}()
				go func() {
					defer func() { finished <- recover() }()
					r := httptest.NewRequest(http.MethodGet, "/v1/queries/active/results", nil).WithContext(requestContext)
					r.Header.Set("Authorization", "Bearer "+rotationNew)
					first.gate.ServeHTTP(output, r)
				}()
				select {
				case <-output.started:
				case <-time.After(2 * time.Second):
					t.Fatal("Arrow relay did not reach active delivery")
				}
				cut := time.Now()
				if err := fixture.GatewayProxy(0).Block(); err != nil {
					t.Fatal("gateway partition failed", err)
				}
				gatewayAuthorityWait(t, gatewayAuthorityLease+500*time.Millisecond, func() bool { return !first.gate.auth.active(active) })
				if time.Since(cut) > gatewayAuthorityLease+500*time.Millisecond ||
					gatewayAuthorityHTTP(first.gate, rotationNew, "/v1/queries/owned-a") != 401 ||
					gatewayAuthorityHTTP(second.gate, rotationNew, "/v1/queries/owned-a") != 200 {
					t.Fatal("partition did not isolate expired authority")
				}
				select {
				case result := <-finished:
					joined = true
					if result != http.ErrAbortHandler || output.Header().Get("Kelvo-Result-Completion") != "durable-eos-v1" ||
						strings.HasSuffix(output.Body.String(), string([]byte{255, 255, 255, 255, 0, 0, 0, 0})) {
						t.Fatal("expired authority emitted a successful Arrow completion")
					}
				case <-time.After(time.Second):
					t.Fatal("expired authority left Arrow delivery active")
				}
				job, err := store.Get(context.Background(), "active")
				if err != nil || job.Job.State != Failed {
					t.Fatal("expired Arrow relay did not fail its query state")
				}
				select {
				case <-upstreamDone:
				case <-time.After(time.Second):
					t.Fatal("expired authority left upstream delivery active")
				}
				if err := fixture.GatewayProxy(0).Unblock(); err != nil {
					t.Fatal(err)
				}
				gatewayAuthorityWait(t, 5*time.Second, func() bool { return gatewayAuthorityHTTP(first.gate, rotationNew, "/v1/queries/owned-a") == 200 })
				if first.gate.auth.active(active) {
					t.Fatal("recovery revived the expired request context")
				}
			}) {
				return
			}
			if !t.Run("RestoredLocalStateCannotAdmitStaleKeys", func(t *testing.T) {
				if err := first.gate.Close(); err != nil {
					t.Fatal(err)
				}
				first.gate = nil
				if err := os.WriteFile(statePath, oldState, 0600); err != nil {
					t.Fatal(err)
				}
				gatewayAuthorityWriteKeys(t, first.config.Authentication.KeysFile, initial)
				stale, err := NewGateway(first.config, first.stores)
				if stale != nil {
					_ = stale.Close()
					t.Fatal("restored stale history reactivated old keys")
				}
				if !errors.Is(err, errGatewayAuthUnavailable) {
					t.Fatal("stale startup failure was not redacted")
				}
				gatewayAuthorityWriteKeys(t, first.config.Authentication.KeysFile, current)
				first.start(t)
				if gatewayAuthorityHTTP(first.gate, rotationOld, "/v1/queries/owned-a") != 401 || gatewayAuthorityHTTP(first.gate, rotationNew, "/v1/queries/owned-a") != 200 {
					t.Fatal("current broker state did not fence restored local history")
				}
			}) {
				return
			}
			if !t.Run("ProofPublication", func(t *testing.T) { gatewayAuthorityPublicationCases(t, first) }) {
				return
			}
			t.Run("Cleanup", func(t *testing.T) {
				for _, f := range []*authorityGatewayFixture{first, second} {
					if err := f.gate.Close(); err != nil {
						t.Fatal("gateway cleanup failed", err)
					}
					f.gate = nil
				}
				if err := fixture.Close(); err != nil {
					t.Fatal("broker fixture cleanup failed", err)
				}
				if _, complete := fixture.Receipt(); !complete {
					t.Fatal("missing terminal broker ownership receipt")
				}
			})
		})
	}
}
