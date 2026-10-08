//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	"github.com/SYNEHQ/kelvo-go/internal/operationrun"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type operationTestPrepared struct {
	run func(context.Context) (operations.Receipt, error)
}

func (p *operationTestPrepared) Execute(ctx context.Context) (operations.Receipt, error) {
	return p.run(ctx)
}
func (*operationTestPrepared) Close() error { return nil }

func operationWorkerFixture(t *testing.T) operationHTTPFixture {
	t.Helper()
	f := newOperationHTTPFixture(t)
	f.policy.MaxQueries, f.policy.Replicas, f.policy.Limits = 8, 1, query.DefaultLimits()
	f.policy.JobTTL = f.policy.Limits.Timeout + 2*f.policy.LeaseDuration
	f.cluster.policy = f.policy
	if err := ValidatePolicy(f.policy); err != nil {
		t.Fatal(err)
	}
	return f
}
func operationJournal(t *testing.T, pending int) *ServiceAudit {
	t.Helper()
	config := serviceAuditConfig(t)
	config.MaxPending = pending
	journal, err := OpenServiceAudit(config, "worker", []string{"team-a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := journal.CloseBounded(); err != nil {
			t.Error(err)
		}
	})
	return journal
}
func operationTLSClient(t *testing.T, handler http.Handler) (*operationInputClient, string, TLSConfig) {
	t.Helper()
	serverFiles, ca, key := tlsFiles(t, GatewayIdentity, nil, nil)
	workerFiles, _, _ := tlsFiles(t, WorkerIdentity("team-a", "worker-a"), ca, key)
	tlsConfig, err := OperationInputTLS(serverFiles)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = tlsConfig
	server.StartTLS()
	t.Cleanup(server.Close)
	client, err := newOperationInputClient(server.URL, workerFiles, "team-a", operationrun.Config{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	client.transport.TLSClientConfig.ServerName = "gateway.test" // Certificate fixture DNS SAN.
	t.Cleanup(client.close)
	return client, server.URL, workerFiles
}
func operationAssigned(t *testing.T, f operationHTTPFixture) ledger.Record {
	t.Helper()
	response := f.submit(t)
	binding := ledger.Binding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32)}
	record, err := f.state.store.Claim(f.context, operationScope(f.claims), response.ID, binding)
	if err != nil {
		t.Fatal(err)
	}
	return record.Record
}

func TestOperationWorkerAutonomousTLSInputAndAudit(t *testing.T) {
	f := operationWorkerFixture(t)
	journal := operationJournal(t, 4)
	_, endpoint, workerTLS := operationTLSClient(t, f.g.OperationInputs())
	var calls atomic.Int32
	worker, err := NewOperationWorker(f.policy, f.cluster, f.state.store, OperationWorkerConfig{Runtime: operationrun.Config{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Concurrency: 2, PollInterval: 10 * time.Millisecond}, InputURL: endpoint, TLS: workerTLS}, journal,
		func(_ context.Context, record ledger.Record, request operations.Request, binding ledger.Binding) (operationrun.Prepared, error) {
			if digest, _ := operations.Digest(request); digest != record.RequestSHA256 {
				return nil, operations.ErrInvalid
			}
			return &operationTestPrepared{run: func(ctx context.Context) (operations.Receipt, error) {
				if _, err := f.state.store.Current(ctx, record.Scope, record.ID, binding); err != nil {
					return operations.Receipt{}, err
				}
				calls.Add(1)
				return operations.Receipt{Outcome: operations.Completed, Effect: operations.EffectCommitted}, errors.New("response delivery failed after commit")
			}}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	worker.input.transport.TLSClientConfig.ServerName = "gateway.test"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := worker.Close(ctx); !errors.Is(err, operationrun.ErrDelivery) {
			t.Error(err)
		}
	})
	response := f.submit(t)
	if err := worker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var result ledger.Record
	waitFor(t, func() bool {
		got, err := f.state.store.Get(f.context, operationScope(f.claims), response.ID)
		if err != nil {
			return false
		}
		result = got.Record
		return result.Terminal()
	})
	if err := worker.Drain(context.Background()); !errors.Is(err, operationrun.ErrDelivery) {
		t.Fatal(err)
	}
	if worker.Ready() || !worker.runtime.Joined() {
		t.Fatal("joined delivery failure remained ready")
	}
	if result.State != string(operations.Completed) || calls.Load() != 1 {
		t.Fatal("autonomous execution failed", result.State, calls.Load())
	}
	events := serviceEvents(t, journal)
	if len(events) != 1 || events[0].Kind != audit.OperationExecution || events[0].Outcome != audit.Succeeded || events[0].Binding.PrincipalID != "api" {
		t.Fatal("source receipt not audited", events)
	}
	// A joined operation failure must not skip another accepted query's drain.
	dispatchDone := make(chan struct{})
	close(dispatchDone)
	node := &Node{operations: &nodeOperations{worker: worker}, dispatchCancel: func() {}, dispatchDone: dispatchDone,
		jobs: map[string]*reservation{"accepted": {}}}
	deadline, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err = node.Drain(deadline)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, operationrun.ErrDelivery) {
		t.Fatal("operation failure bypassed accepted query drain", err)
	}
	delete(node.jobs, "accepted")
	if err := node.Drain(context.Background()); !errors.Is(err, operationrun.ErrDelivery) {
		t.Fatal("drain discarded the settled lifecycle failure", err)
	}
	// The completed runtime may release its result store and its exclusive
	// storage lock even though it reports a result-delivery error.
	directory := t.TempDir()
	custody, err := exports.OpenCustody(context.Background(), directory, f.policy.TenantID, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	defer custody.Close()
	results, err := operationinput.Open(operationinput.Config{MaxInputBytes: 1 << 20, Storage: exports.Config{
		Directory: custody.DataDirectory(), Tenant: f.policy.TenantID, MaxEntries: 2, MaxStoredBytes: 2 << 20, MaxTTL: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	defer results.Close()
	node.operations.custody, node.operations.results = custody, results
	if err := node.operations.close(context.Background()); !errors.Is(err, operationrun.ErrDelivery) {
		t.Fatal("storage shutdown discarded the settled lifecycle failure", err)
	}
	reopened, err := exports.OpenCustody(context.Background(), directory, f.policy.TenantID, "worker-a")
	if err != nil {
		t.Fatal("joined operation leaked its storage ownership", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	current, err := f.state.store.Get(f.context, operationScope(f.claims), response.ID)
	if err != nil || current.Record.State != string(operations.Completed) || current.Record.Receipt.Effect != operations.EffectCommitted {
		t.Fatal("shutdown rewrote the confirmed source effect", err)
	}
}

func TestOperationWorkerInputRejectsChangedBodiesAndRedirects(t *testing.T) {
	for _, mode := range []string{"valid", "changed", "oversized", "redirect", "encoding", "content_type"} {
		t.Run(mode, func(t *testing.T) {
			f := operationWorkerFixture(t)
			record := operationAssigned(t, f)
			canonical, _ := operations.Encode(f.request)
			var calls atomic.Int32
			client, _, _ := operationTLSClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, err := io.ReadAll(io.LimitReader(r.Body, 8193))
				if err != nil {
					t.Error(err)
				}
				var input operationInputRequest
				if operations.DecodeStrict(body, &input, 8192) != nil || input.Scope != record.Scope || input.Binding != record.Binding || r.Method != http.MethodPost {
					t.Error("input request was not custody-bound")
				}
				w.Header().Set("Content-Type", "application/json")
				switch mode {
				case "changed":
					_, _ = io.WriteString(w, strings.ReplaceAll(string(canonical), "UPDATE", "DELETE"))
				case "oversized":
					_, _ = w.Write(append(canonical, ' '))
				case "redirect":
					w.Header().Set("Location", "/redirected")
					w.WriteHeader(http.StatusTemporaryRedirect)
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
					_, _ = w.Write(canonical)
				case "content_type":
					w.Header().Set("Content-Type", "text/plain")
					_, _ = w.Write(canonical)
				default:
					_, _ = w.Write(canonical)
				}
			}))
			request, err := client.load(context.Background(), record)
			if mode == "valid" {
				if err != nil || request.Spec.Statement.SQL != f.request.Spec.Statement.SQL {
					t.Fatal("valid input failed", err)
				}
			} else if err == nil {
				t.Fatal("untrusted input accepted")
			}
			if calls.Load() != 1 {
				t.Fatal("input retried or redirect followed", calls.Load())
			}
		})
	}
}

func TestOperationWorkerAuthorizationBindsEveryCustodyLayer(t *testing.T) {
	for _, mode := range []string{"valid", "request", "grant", "scope", "binding", "worker_lease", "cancel_during_lease", "policy", "certificate"} {
		t.Run(mode, func(t *testing.T) {
			f := operationWorkerFixture(t)
			record := operationAssigned(t, f)
			policy, err := clonePolicy(f.policy)
			if err != nil {
				t.Fatal(err)
			}
			worker := &OperationWorker{policy: policy, store: f.cluster, ledger: f.state.store, input: &operationInputClient{worker: "worker-a", owner: record.Binding.Owner, identityUntil: time.Now().Add(time.Minute)}}
			request, _ := operations.Clone(f.request)
			binding := record.Binding
			switch mode {
			case "request":
				request.Spec.Statement.SQL = "DELETE FROM example"
			case "grant":
				record.AuthorityToken += "changed"
			case "scope":
				record.Scope.AppTeam = "other-team"
			case "binding":
				binding.Claim = strings.Repeat("c", 32)
			case "worker_lease":
				f.cluster.lease = func(context.Context, string, string) (time.Time, error) { return time.Now().Add(-time.Second), nil }
			case "cancel_during_lease":
				f.cluster.lease = func(ctx context.Context, _, _ string) (time.Time, error) {
					_, err := f.state.store.Cancel(ctx, record.Scope, record.ID)
					return time.Now().Add(time.Minute), err
				}
			case "policy":
				f.cluster.policy.MaxQueries++
			case "certificate":
				worker.input.identityUntil = time.Now().Add(-time.Second)
			}
			err = worker.authorize(context.Background(), record, &request, binding)
			if (mode == "valid") != (err == nil) {
				t.Fatal("authorization boundary", mode, err)
			}
		})
	}
}

func TestOperationWorkerAuditCapacityRejectsBeforeDriver(t *testing.T) {
	f := operationWorkerFixture(t)
	record := operationAssigned(t, f)
	journal := operationJournal(t, 1)
	if _, err := f.state.store.Start(f.context, record.Scope, record.ID, record.Binding); err != nil {
		t.Fatal(err)
	}
	authority, _ := authorityForPrincipal(f.policy, "api")
	blocker, err := journal.begin(context.Background(), f.policy.TenantID, &authority, audit.OperationExecution)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.abort(context.Background())
	var calls atomic.Int32
	worker := &OperationWorker{policy: f.policy, store: f.cluster, ledger: f.state.store, audit: journal, input: &operationInputClient{worker: "worker-a", owner: record.Binding.Owner, identityUntil: time.Now().Add(time.Minute)}}
	p := &operationAuditedExecution{worker: worker, record: record, request: f.request, binding: record.Binding, delegate: &operationTestPrepared{run: func(context.Context) (operations.Receipt, error) { calls.Add(1); return operations.Receipt{}, nil }}}
	receipt, err := p.Execute(context.Background())
	if err == nil || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone || calls.Load() != 0 {
		t.Fatal("audit exhaustion reached source", receipt, err)
	}
	if err := blocker.complete(nil); err != nil {
		t.Fatal(err)
	}
}

func TestOperationWorkerRequiresExplicitTLSAndWorkerCertificate(t *testing.T) {
	for _, endpoint := range []string{"http://gateway.test", "https://gateway.test/path", "https://user@gateway.test", "https://gateway.test?token=value"} {
		if _, err := newOperationInputClient(endpoint, TLSConfig{}, "team-a", operationrun.Config{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Concurrency: 1}); err == nil {
			t.Fatal("unsafe endpoint accepted", endpoint)
		}
	}
	gatewayFiles, _, _ := tlsFiles(t, GatewayIdentity, nil, nil)
	if _, err := newOperationInputClient("https://gateway.test", gatewayFiles, "team-a", operationrun.Config{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Concurrency: 1}); err == nil {
		t.Fatal("gateway identity used as worker")
	}
}
