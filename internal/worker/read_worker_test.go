// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapter/frame"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
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
		if err != nil || stats.Rows != 1 || sink.rows != 1 {
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
