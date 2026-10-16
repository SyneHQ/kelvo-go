// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapter/frame"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

func readWorkerExecutor(t *testing.T, fs *fakeSet) (*Executor, *WarmPoolManager) {
	t.Helper()
	t.Setenv("KELVO_TEST_READ_DSN", "postgres://reader:secret@db.internal:5432/appdb?sslmode=verify-full")
	cfg := DefaultWarmPoolConfig()
	cfg.Enabled = true
	mgr, err := NewWarmPoolManager(cfg, fs.launcher())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Drain(context.Background()) })
	mgr.Adapter = OperationProcessConfig{Binary: "/fake/kelvo-adapter-go"}
	limits := query.DefaultLimits()
	limits.MaxBytes = 64 << 20
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "app", Type: "postgres", DSNEnv: "KELVO_TEST_READ_DSN"}}}, limits)
	if err != nil {
		t.Fatal(err)
	}
	e.WarmPool = mgr
	return e, mgr
}

// Native reads and inspections reach the read worker as QueryRead and
// MetadataInspect requests on one shared worker, with the source's database
// and credentials and no per-read reservation.
func TestReadWorkerServesNativeReadsAndInspections(t *testing.T) {
	fs := &fakeSet{}
	var seen []adapter.ProcessRequest
	fs.serve = func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type == frame.Request {
				var r adapter.ProcessRequest
				_ = json.Unmarshal(f.Payload, &r)
				fs.mu.Lock()
				seen = append(seen, r)
				fs.mu.Unlock()
				replyOK(p.stdoutW, f.Stream, f.Payload)
			}
		})
	}
	e, mgr := readWorkerExecutor(t, fs)
	read := query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT $1", Parameters: []query.Parameter{{Type: "int64", Value: json.RawMessage(`"7"`)}}}
	inspect := query.Request{Mode: "native", ConnectionID: "app", Inspect: &operations.MetadataSpec{Object: "columns", Target: operations.ObjectRef{Schema: "public", Name: "events"}, Limit: 100}}
	for _, r := range []query.Request{read, inspect} {
		sink := &workerTestSink{}
		stats, err := e.Execute(context.Background(), r, sink)
		// The client checks the job's rows and batches against what it decoded.
		if err != nil || stats.Rows != 1 || sink.rows != 1 || stats.Batches != int64(sink.batches) || stats.Batches != 1 {
			t.Fatal(stats, err, sink.rows)
		}
	}
	if len(fs.procs) != 1 || mgr.metrics.Hits.Load() != 1 || len(seen) != 2 {
		t.Fatalf("procs=%d hits=%d requests=%d", len(fs.procs), mgr.metrics.Hits.Load(), len(seen))
	}
	q, m := seen[0], seen[1]
	if q.Request.Kind != operations.QueryRead || q.Request.Spec.Query.SQL != "SELECT $1" || len(q.Request.Spec.Query.Parameters) != 1 ||
		m.Request.Kind != operations.MetadataInspect || m.Request.Spec.Metadata.Target.Name != "events" ||
		q.Source.Engine != "postgres" || q.Source.Database != "appdb" || q.Source.DSN == "" || q.Source.Revision != m.Source.Revision || q.Source.TenantID != "static" {
		t.Fatalf("unexpected requests %+v %+v", q.Request, m.Request)
	}
}

// Without a read worker an inspection is refused (it has no one-shot query
// child); a source or limits the adapter would refuse never reach it.
func TestReadWorkerEligibility(t *testing.T) {
	fs := &fakeSet{serve: func(p *fakeWorkerProcess) { serveFake(p, nil) }}
	e, _ := readWorkerExecutor(t, fs)
	inspect := query.Request{Mode: "native", ConnectionID: "app", Inspect: &operations.MetadataSpec{Object: "schemas", Limit: 10}}

	off := *e
	off.WarmPool = nil
	_, err := off.Execute(context.Background(), inspect, &workerTestSink{})
	var public *query.Error
	if !errors.As(err, &public) || public.Code != "UNAVAILABLE" {
		t.Fatal("inspection without a read worker", err)
	}
	big := *e
	big.Limits.MaxBytes = 128 << 20
	if _, err := big.Execute(context.Background(), inspect, &workerTestSink{}); !errors.As(err, &public) || public.Code != "UNAVAILABLE" {
		t.Fatal("limits above the adapter's were routed", err)
	}
	if len(fs.procs) != 0 {
		t.Fatal("ineligible requests launched a worker")
	}

	for dsn, want := range map[string]string{
		"postgres://u:p@h:5432/db?sslmode=verify-full":            "db",
		"postgresql://u:p@h/db?sslmode=disable&connect_timeout=5": "db",
		"postgres://u:p@h/db":                                     "",
		"postgres://u:p@h/db?sslmode=require":                     "",
		"postgres://u@h/db?sslmode=disable":                       "",
		"postgres://u:p@h/db?sslmode=disable&application_name=x":  "",
		"mysql://u:p@h/db?sslmode=disable":                        "",
		"postgres://u:p@h/?sslmode=disable":                       "",
	} {
		got, ok := readWorkerDatabase(dsn, "")
		if got != want || ok != (want != "") {
			t.Fatal(dsn, got, ok)
		}
	}
	if _, ok := readWorkerDatabase("postgres://u:p@h/db?sslmode=disable", "other"); ok {
		t.Fatal("database mismatch accepted")
	}
	if readWorkerSourceShape(catalog.Source{Type: "postgres", DSNEnv: "X", Options: map[string]string{"search_path": "x"}}) ||
		readWorkerSourceShape(catalog.Source{Type: "mysql", DSNEnv: "X"}) || !readWorkerSourceShape(catalog.Source{Type: "postgres", DSNEnv: "X"}) {
		t.Fatal("source shape gate")
	}
}

// A deadline under a second is sent to the worker with millisecond precision
// instead of being refused or sent to the one-shot path.
func TestReadWorkerSubSecondDeadline(t *testing.T) {
	fs := &fakeSet{}
	var seen []adapter.ProcessRequest
	fs.serve = func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type == frame.Request {
				var r adapter.ProcessRequest
				_ = json.Unmarshal(f.Payload, &r)
				fs.mu.Lock()
				seen = append(seen, r)
				fs.mu.Unlock()
				replyOK(p.stdoutW, f.Stream, f.Payload)
			}
		})
	}
	e, _ := readWorkerExecutor(t, fs)
	// End the deadline inside the current whole second, at least 500ms away:
	// rounded down to seconds it would already have passed.
	for time.Now().Nanosecond() > 400_000_000 {
		time.Sleep(10 * time.Millisecond)
	}
	now := time.Now()
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(now.Unix(), 950_000_000))
	defer cancel()
	if _, err := e.Execute(ctx, query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}, &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(seen) != 1 || seen[0].Limits.TimeoutMS < 1 || seen[0].Limits.TimeoutMS > 950 || seen[0].ExpiresAt != now.Unix()+1 {
		t.Fatalf("sub-second deadline not sent to the worker: %+v", seen)
	}
}

// Inspections a read worker cannot take go to the previous path, a one-shot
// adapter operation, never UNAVAILABLE or the native query child.
func TestInspectionFallsBackWhenReadWorkerCannotTakeIt(t *testing.T) {
	fs := &fakeSet{serve: func(p *fakeWorkerProcess) { serveFake(p, nil) }}
	e, mgr := readWorkerExecutor(t, fs)
	e.OperationAdapter = mgr.Adapter
	inspect := query.Request{Mode: "native", ConnectionID: "app", Inspect: &operations.MetadataSpec{Object: "schemas", Limit: 10}}

	// Node limits above the adapter's keep reads off read workers; the
	// inspection still runs, on the operation path.
	big := *e
	big.Limits.MaxBytes = 256 << 20
	if _, err := big.Execute(context.Background(), inspect, &workerTestSink{}); err != nil {
		t.Fatal("inspection refused when the read worker could not take it:", err)
	}
	// Under a caller's custody it must not fall into the native child with empty SQL.
	custody, _ := containment.NewCustody(func() {})
	ctx := containment.WithCustody(context.Background(), custody)
	if _, err := e.Execute(ctx, inspect, &workerTestSink{}); err != nil {
		t.Fatal("inspection under custody:", err)
	}
	// Without any adapter there is nothing to fall back to.
	none := *e
	none.WarmPool, none.OperationAdapter = nil, OperationProcessConfig{}
	if _, err := none.Execute(context.Background(), inspect, &workerTestSink{}); err == nil || !strings.Contains(err.Error(), "operation adapter") {
		t.Fatal("inspection without an adapter:", err)
	}
}

// Apps send one alias for every connection: workers are keyed by the
// delegation's saved connection, so two connections behind the same alias get
// separate workers and the adapter sees the saved connection.
func TestReadWorkerKeyedBySavedConnection(t *testing.T) {
	fs := &fakeSet{}
	var seen []adapter.ProcessRequest
	fs.serve = func(p *fakeWorkerProcess) {
		var mu sync.Mutex
		serveFake(p, func(f frame.Frame) {
			if f.Type == frame.Request {
				var r adapter.ProcessRequest
				_ = json.Unmarshal(f.Payload, &r)
				fs.mu.Lock()
				seen = append(seen, r)
				fs.mu.Unlock()
				mu.Lock()
				replyOK(p.stdoutW, f.Stream, f.Payload)
				mu.Unlock()
			}
		})
	}
	e, _ := readWorkerExecutor(t, fs)
	resolverClient := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		var body resolver.QueryRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		writeConnectionResponse(t, w, connectionResolution{Version: 1, DelegationSHA256: delegation.Digest(body.Delegation), ValidUntil: time.Now().Add(30 * time.Second).Unix(),
			Sources: []catalog.Source{{ID: "selected", Type: "postgres", DSNEnv: "KELVO_SOURCE_REQUEST_0_DSN"}},
			Secrets: map[string]string{"KELVO_SOURCE_REQUEST_0_DSN": connectionFixtureDSN}})
	})
	e, err := e.WithConnectionResolvers(map[string]*ConnectionResolver{"gateway": resolverClient})
	if err != nil {
		t.Fatal(err)
	}
	read := func(saved string) {
		r := query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT 1", Delegation: "grant-" + saved}
		digest, _ := delegation.QueryDigest(r)
		claims := delegation.Claims{Issuer: "gateway", ClusterTenant: "tenant", AppTeam: "team-a", QuerySHA256: digest, ExpiresAt: time.Now().Add(time.Minute).Unix(),
			Sources: []delegation.Source{{Alias: "selected", ConnectionID: saved, Database: "analytics"}}}
		ctx := delegation.WithExecution(context.Background(), claims, r.Delegation, delegation.ExecutionBinding{JobID: "job", WorkerID: "worker", Owner: "owner", Claim: "claim"})
		if _, err := e.Execute(ctx, r, &workerTestSink{}); err != nil {
			t.Fatal(saved, err)
		}
	}
	read("saved-a")
	read("saved-b")
	read("saved-a")
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.procs) != 2 || len(seen) != 3 || seen[0].Source.ConnectionID != "saved-a" || seen[1].Source.ConnectionID != "saved-b" || seen[0].Request.Connection.ID != "saved-a" {
		t.Fatalf("workers keyed by alias: procs=%d first=%q second=%q", len(fs.procs), seen[0].Source.ConnectionID, seen[1].Source.ConnectionID)
	}
}

// A burst on one connection queues or scales out; no read fails. Each worker
// serves four reads at a time (the adapter's source pool), so most of the
// fifty wait for a slot, some longer than a credential window: the window is
// stamped when a read is sent, not when it queued. A key at max_in_flight
// launches a second worker (max_per_key 2).
func TestReadWorkerBurstOnOneConnectionNeverFails(t *testing.T) {
	credentialWindowMax.Store(int64(time.Second))
	defer credentialWindowMax.Store(0)
	fs := &fakeSet{}
	fs.serve = func(p *fakeWorkerProcess) {
		source := make(chan struct{}, 4)
		var write sync.Mutex // one frame at a time, as the adapter's writer does
		serveFake(p, func(f frame.Frame) {
			if f.Type != frame.Request {
				return
			}
			go func() {
				source <- struct{}{}
				time.Sleep(250 * time.Millisecond)
				<-source
				write.Lock()
				defer write.Unlock()
				replyOK(p.stdoutW, f.Stream, f.Payload)
			}()
		})
	}
	e, mgr := readWorkerExecutor(t, fs)
	const readers = 50
	var wg sync.WaitGroup
	var failed atomic.Int32
	var firstErr atomic.Value
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := query.Request{Mode: "native", ConnectionID: "app", SQL: fmt.Sprintf("SELECT %d", i)}
			if _, err := e.Execute(context.Background(), r, &workerTestSink{}); err != nil {
				failed.Add(1)
				firstErr.CompareAndSwap(nil, err.Error())
			}
		}()
	}
	wg.Wait()
	if failed.Load() != 0 || mgr.metrics.Fallbacks.Load() != 0 {
		t.Fatal("burst reads failed:", failed.Load(), firstErr.Load(), "fallbacks", mgr.metrics.Fallbacks.Load())
	}
	if mgr.metrics.Spawns.Load() != 2 {
		t.Fatal("a key at max_in_flight did not scale out to max_per_key workers:", mgr.metrics.Spawns.Load())
	}
}
