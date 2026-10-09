//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

func privateStartupFixture(t *testing.T, f *privateResolutionFixture) map[string]PrivateOperationBinding {
	t.Helper()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.policy.certificate}), 0600); err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(f.policy.proofKey)
	c := PrivateOperationConfig{RouteID: f.policy.route, ProxyAddress: "proxy.invalid:14443", ProxyServerName: "proxy.invalid", ProxyCAFile: ca, ProofPublicKey: key, TicketPublicKey: key, MaxSessions: 1, MaxDataConnections: 2, MaxDataPerSession: 2}
	return map[string]PrivateOperationBinding{f.policy.trust.ServicePrincipal: {Config: c, Trust: f.policy.trust, ResolverCAFile: ca}}
}

func TestPrivateStartupRejectsUnboundConfiguration(t *testing.T) {
	for _, mode := range []string{"principal", "worker", "identity", "sessions", "root", "proof", "ticket", "route", "proxy", "capacity"} {
		t.Run(mode, func(t *testing.T) {
			f := newPrivateResolutionFixture(t)
			bindings := privateStartupFixture(t, f)
			principal := f.policy.trust.ServicePrincipal
			b := bindings[principal]
			worker, identity := f.policy.worker, f.policy.identity
			switch mode {
			case "principal":
				b.Trust.ServicePrincipal = "other"
			case "worker":
				worker = "other"
			case "identity":
				identity = "spiffe://kelvo/tenant/shared/worker/other"
			case "sessions":
				b.Config.MaxSessions = 2
			case "root":
				b.Config.ProxyCAFile = "/missing-private-ca"
			case "proof":
				b.Config.ProofPublicKey = "invalid"
			case "ticket":
				b.Config.TicketPublicKey = "invalid"
			case "route":
				b.Config.RouteID = "../route"
			case "proxy":
				b.Config.ProxyAddress = "https://proxy.invalid"
			case "capacity":
				b.Config.MaxDataPerSession = 3
			}
			bindings[principal] = b
			if runtime, err := NewPrivateOperations(f.executor, worker, identity, 1, bindings); err == nil {
				runtime.Close(context.Background())
				t.Fatal("invalid startup configuration was accepted")
			}
			if f.calls.Load() != 0 {
				t.Fatal("startup fetched source credentials")
			}
		})
	}
}

func TestPrivateStartupSelectsAuthenticatedTransportAndJoins(t *testing.T) {
	for _, mode := range []string{"private", "public", "bad-proof", "wrong-tenant", "wrong-source", "wrong-worker"} {
		t.Run(mode, func(t *testing.T) {
			f := newPrivateResolutionFixture(t)
			switch mode {
			case "public":
				f.change = func(r *resolver.OperationResponse) { r.PrivateSource = nil }
			case "bad-proof":
				f.change = func(r *resolver.OperationResponse) { r.PrivateSource.Token = "invalid" }
			case "wrong-tenant":
				f.record.Scope.AppTeam = "other"
			case "wrong-source":
				f.record.Scope.ConnectionID = "other"
			case "wrong-worker":
				f.record.Binding.WorkerID = "other"
			}
			bindings := privateStartupFixture(t, f)
			runtimes, err := NewPrivateOperations(f.executor, f.policy.worker, f.policy.identity, 1, bindings)
			if err != nil {
				t.Fatal(err)
			}
			// Mutating operator input after construction cannot replace the registry.
			delete(bindings, f.policy.trust.ServicePrincipal)
			r := runtimes.runtimes[f.policy.trust.ServicePrincipal]
			input, channel, err := r.prepareMode(context.Background(), f.executor, f.record, f.request, nil, true, 4096)
			if mode == "private" || mode == "public" {
				if err != nil || input.Limits.MaxBytes != 4096 {
					t.Fatal("valid resolution failed", err)
				}
				if (channel != nil) != (mode == "private") {
					t.Fatal("wrong transport selected")
				}
				if channel != nil {
					if err := channel.close(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
			} else if err == nil || channel != nil {
				t.Fatal("invalid proof or scope entered source runtime")
			}
			stop, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := runtimes.Close(stop); err != nil {
				t.Fatal(err)
			}
			if r.broker.Snapshot().Sessions != 0 || len(r.entries) != 0 || r.pending != 0 {
				t.Fatal("startup runtime leaked custody")
			}
			if _, _, err := r.prepareMode(context.Background(), f.executor, f.record, f.request, nil, true, 4096); err != transportbroker.ErrClosed {
				t.Fatal("closed runtime admitted credentials", err)
			}
		})
	}
}

func TestPrivateStartupRejectsSpecialTrustFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.pipe")
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := readPrivateCA(path); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO trust file accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("FIFO trust file blocked startup")
	}
}
