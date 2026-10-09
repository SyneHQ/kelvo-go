//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/childipc"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/rabbitconnect"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
	"github.com/SYNEHQ/kelvo-go/sourceproof"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

func privateRuntimeFixture(t *testing.T, f *privateResolutionFixture) *privateOperationRuntime {
	t.Helper()
	p := f.policy
	pair := p.resolver.client.Transport.(*http.Transport).TLSClientConfig.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.certificate})
	encodedKey := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	proxy := rabbitconnect.Config{ProxyAddress: "proxy.invalid:14443", ProxyServerName: "proxy.invalid", RootCAPEM: certificate, ClientCertificatePEM: certificate, ClientKeyPEM: encodedKey, WorkerIdentity: p.identity, Issuer: p.trust.Issuer, Audience: p.trust.Audience, ClusterTenant: p.trust.ClusterTenant, ServicePrincipal: p.trust.ServicePrincipal, IssuerPublicKey: p.proofKey}
	issuer := rabbitconnect.HTTPIssuerConfig{Endpoint: "https://issuer.invalid" + transportissuer.Path, RootCAPEM: certificate, ClientCertificatePEM: certificate, ClientKeyPEM: encodedKey, WorkerIdentity: p.identity, MaxInFlight: 2}
	r, err := newPrivateOperationRuntime(context.Background(), p, proxy, issuer, transportbroker.Limits{MaxSessions: 1, MaxDataConnections: 2, MaxDataPerSession: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.close(ctx); err != nil {
			t.Error(err)
		}
	})
	return r
}

func TestPrivateRuntimeFreshProofAndImmutableRequestOnEveryOpen(t *testing.T) {
	for _, change := range []string{"none", "credentials", "binding"} {
		t.Run(change, func(t *testing.T) {
			f := newPrivateResolutionFixture(t)
			r := privateRuntimeFixture(t, f)
			var opened atomic.Int32
			r.opener = func(scope sourceproof.Scope, refresh func(context.Context) (sourceproof.Envelope, error)) (transportbroker.Opener, error) {
				return privateChannelOpener(func(ctx context.Context, _ transportbroker.OpenRequest) (net.Conn, error) {
					envelope, err := refresh(ctx)
					if err != nil {
						return nil, err
					}
					if _, err := sourceproof.Verify(f.policy.proofKey, envelope, scope, time.Now()); err != nil {
						return nil, err
					}
					opened.Add(1)
					left, right := net.Pipe()
					right.Close()
					return left, nil
				}), nil
			}
			sql, cancelSQL := context.WithCancel(context.Background())
			input, channel, err := r.prepare(sql, f.executor, f.record, f.request, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := channel.close(ctx); err != nil {
					t.Error(err)
				}
			}()
			cancelSQL()
			// The caller can mutate its own request after admission. The retained grant
			// still verifies against the private copy on each physical open.
			f.request.Spec.Statement.SQL = "DELETE FROM other"
			first, err := channel.session.DataDialer().DialContext(context.Background(), "tcp", channel.binding.Authority)
			if err != nil {
				t.Fatal("SQL cancellation or caller mutation changed parent custody", err)
			}
			first.Close()
			if f.calls.Load() != 2 || opened.Load() != 1 || input.Source.Engine != "postgresql" {
				t.Fatal("first physical open did not refresh source")
			}
			if change == "credentials" {
				f.change = func(w *resolver.OperationResponse) {
					w.Secrets[w.Source.DSNEnv] = strings.Replace(w.Secrets[w.Source.DSNEnv], "fixture-password", "rotated-password", 1)
				}
			}
			if change == "binding" {
				f.claimsChange = func(s *sourceproof.Scope) { s.BindingVersion++ }
			}
			second, err := channel.session.DataDialer().DialContext(context.Background(), "tcp", channel.binding.Authority)
			if change == "none" {
				if err != nil {
					t.Fatal(err)
				}
				second.Close()
				if opened.Load() != 2 {
					t.Fatal("second physical open missing")
				}
			} else if err == nil || second != nil || opened.Load() != 1 {
				t.Fatal("changed source opened under old admission")
			}
			if f.calls.Load() != 3 {
				t.Fatal("physical reopen reused proof", f.calls.Load())
			}
		})
	}
}

func TestPrivateRuntimeCapacityAndConstructorFailureKeepOwnedCleanup(t *testing.T) {
	f := newPrivateResolutionFixture(t)
	r := privateRuntimeFixture(t, f)
	r.opener = func(sourceproof.Scope, func(context.Context) (sourceproof.Envelope, error)) (transportbroker.Opener, error) {
		return privateChannelOpener(func(context.Context, transportbroker.OpenRequest) (net.Conn, error) { return nil, adapter.ErrInvalid }), nil
	}
	r.channel = func(context.Context, adapter.ProcessRequest, *transportbroker.Session, transportbroker.Binding, int, func()) (*privateOperationChannel, error) {
		return nil, errors.New("fixture IPC setup failed")
	}
	_, cleanup, err := r.prepare(context.Background(), f.executor, f.record, f.request, nil)
	if err == nil || cleanup == nil || r.broker.Snapshot().Sessions != 1 || len(r.entries) != 1 {
		t.Fatal("constructor failure lost admitted session custody")
	}
	calls := f.calls.Load()
	if _, other, err := r.prepare(context.Background(), f.executor, f.record, f.request, nil); !errors.Is(err, transportbroker.ErrCapacity) || other != nil || f.calls.Load() != calls {
		t.Fatal("full runtime allowed another resolver request")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cleanup.close(ctx); err != nil {
		t.Fatal(err)
	}
	if r.broker.Snapshot().Sessions != 0 || len(r.entries) != 0 || r.pending != 0 {
		t.Fatal("failed constructor leaked session after cleanup")
	}
	// The old cleanup callback cannot remove a later reservation for the same binding.
	_, next, err := r.prepare(context.Background(), f.executor, f.record, f.request, nil)
	if err == nil || next == nil {
		t.Fatal("new admitted attempt missing")
	}
	if err := cleanup.close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(r.entries) != 1 || r.broker.Snapshot().Sessions != 1 {
		t.Fatal("stale cleanup removed newer session")
	}
	if err := next.close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPrivateRuntimeRejectsShutdownBeforeResolverUse(t *testing.T) {
	f := newPrivateResolutionFixture(t)
	r := privateRuntimeFixture(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, channel, err := r.prepare(context.Background(), f.executor, f.record, f.request, nil); err == nil || channel != nil || f.calls.Load() != 0 {
		t.Fatal("draining runtime admitted source")
	}
}

func TestPrivateRuntimeShutdownCancelsPendingResolution(t *testing.T) {
	f := newPrivateResolutionFixture(t)
	r := privateRuntimeFixture(t, f)
	entered, resume := make(chan struct{}), make(chan struct{})
	f.change = func(*resolver.OperationResponse) { close(entered); <-resume }
	result := make(chan error, 1)
	go func() {
		_, channel, err := r.prepare(context.Background(), f.executor, f.record, f.request, nil)
		if channel != nil {
			err = errors.New("shutdown returned a source channel")
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(resume)
		t.Fatal("resolver did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := r.close(ctx)
	close(resume)
	if err != nil {
		t.Fatal("shutdown did not join pending resolution", err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled resolution succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("pending resolver retained execution")
	}
	if r.pending != 0 || len(r.entries) != 0 || r.broker.Snapshot().Sessions != 0 {
		t.Fatal("shutdown lost pending admission")
	}
}

func TestPrivateRuntimeShutdownWaitsForOperationCleanup(t *testing.T) {
	f := newPrivateResolutionFixture(t)
	r := privateRuntimeFixture(t, f)
	r.opener = func(sourceproof.Scope, func(context.Context) (sourceproof.Envelope, error)) (transportbroker.Opener, error) {
		return privateChannelOpener(func(context.Context, transportbroker.OpenRequest) (net.Conn, error) { return nil, adapter.ErrInvalid }), nil
	}
	_, channel, err := r.prepare(context.Background(), f.executor, f.record, f.request, nil)
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if r.close(short) == nil {
		cancel()
		t.Fatal("runtime shutdown skipped operation cleanup owner")
	}
	cancel()
	if len(r.entries) != 1 {
		t.Fatal("shutdown released unjoined operation owner")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := channel.close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestContainedPrivateRuntimeResolvesAfterAdmission(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "IPC construction failed"}[broken], func(t *testing.T) {
			f := newPrivateResolutionFixture(t)
			executor, manager, pool := containedExecutor(t)
			executor.connectionResolvers = f.executor.connectionResolvers
			f.executor = executor
			runtimes, err := NewPrivateOperations(executor, f.policy.worker, f.policy.identity, 1, privateStartupFixture(t, f))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := runtimes.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			r := runtimes.runtimes[f.policy.trust.ServicePrincipal]
			var opens atomic.Int32
			f.change = func(*resolver.OperationResponse) {
				if pool.Snapshot().Active != 1 || manager.Status().Active != 1 {
					t.Error("credentials resolved before containment admission")
				}
			}
			r.opener = func(scope sourceproof.Scope, refresh func(context.Context) (sourceproof.Envelope, error)) (transportbroker.Opener, error) {
				return privateChannelOpener(func(ctx context.Context, _ transportbroker.OpenRequest) (net.Conn, error) {
					proof, err := refresh(ctx)
					if err != nil {
						return nil, err
					}
					if _, err := sourceproof.Verify(f.policy.proofKey, proof, scope, time.Now()); err != nil {
						return nil, err
					}
					opens.Add(1)
					left, right := net.Pipe()
					go func() {
						defer right.Close()
						right.SetDeadline(time.Now().Add(time.Second))
						var ping [4]byte
						if _, err := io.ReadFull(right, ping[:]); err == nil {
							right.Write([]byte("pong"))
						}
					}()
					return left, nil
				}), nil
			}
			if broken {
				r.channel = func(context.Context, adapter.ProcessRequest, *transportbroker.Session, transportbroker.Binding, int, func()) (*privateOperationChannel, error) {
					return nil, adapter.ErrInvalid
				}
			}
			receipt, err := executor.ExecuteDelegatedOperation(context.Background(), runtimes, operationExecutable(t), f.record.ID, f.record.RequestSHA256,
				func(context.Context) (OperationSourceRequest, error) {
					if pool.Snapshot().Active != 1 || manager.Status().Active != 1 {
						t.Error("input preparation ran before containment admission")
					}
					return OperationSourceRequest{Record: f.record, Request: f.request, MaxResultBytes: 4096}, nil
				}, nil)
			if broken {
				if err == nil || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone || opens.Load() != 0 {
					t.Fatal("failed IPC setup executed source", receipt, err)
				}
			} else if err != nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectCommitted || opens.Load() != 1 {
				t.Fatal("private runtime execution failed", receipt, err, opens.Load())
			}
			if pool.Snapshot().Active != 0 || manager.Status().Active != 0 || r.broker.Snapshot().Sessions != 0 || len(r.entries) != 0 || r.pending != 0 {
				t.Fatal("private runtime did not join parent custody")
			}
		})
	}
}

func TestPrivateRuntimeDataOpenUsesOperationContextAndConfiguredBudget(t *testing.T) {
	f := newPrivateResolutionFixture(t)
	r := privateRuntimeFixture(t, f)
	started, joined := make(chan struct{}), make(chan struct{})
	var remaining time.Duration
	r.opener = func(sourceproof.Scope, func(context.Context) (sourceproof.Envelope, error)) (transportbroker.Opener, error) {
		return privateChannelOpener(func(ctx context.Context, _ transportbroker.OpenRequest) (net.Conn, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				return nil, errors.New("data open lost operation deadline")
			}
			remaining = time.Until(deadline)
			close(started)
			<-ctx.Done()
			close(joined)
			return nil, ctx.Err()
		}), nil
	}
	op, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, channel, err := r.prepare(op, f.executor, f.record, f.request, nil)
	if err != nil {
		t.Fatal(err)
	}
	expected := (f.policy.resolver.client.Timeout + rabbitconnect.MaxSetupTime).Milliseconds()
	if channel.dataOpenTimeoutMS != expected {
		t.Fatalf("IPC budget %d != configured %d", channel.dataOpenTimeoutMS, expected)
	}
	file := channel.file
	channel.file = nil
	client, err := childipc.NewClientWithDataOpenTimeout(file, channel.binding.Authority, channel.dataOpenTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	channel.start(os.Getpid())
	result := make(chan error, 1)
	go func() {
		conn, err := client.DialContext(context.Background(), "tcp", channel.binding.Authority)
		if conn != nil {
			conn.Close()
		}
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("parent opener did not start")
	}
	if remaining <= 0 || remaining > time.Second {
		t.Fatal("operation deadline not propagated", remaining)
	}
	cancel()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("operation cancellation did not stop parent opener")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled open succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("child remained blocked")
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := channel.close(ctx); err != nil {
		t.Fatal(err)
	}
	if r.life.Err() != nil {
		t.Fatal("operation cancellation revoked cleanup authority")
	}
	if state := r.broker.Snapshot(); state.Sessions != 0 || state.Opening != 0 || state.DataConnections != 0 || state.CleanupFailed {
		t.Fatal("cleanup retained unexpected broker ownership", state)
	}
}
