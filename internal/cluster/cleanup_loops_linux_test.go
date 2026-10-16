//go:build linux

package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/operationrun"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// cleanupScript fails with a timeout for the first timeouts calls, then with a
// real I/O error. It reports how many calls ran before the loop ended.
type cleanupScript struct {
	timeouts int32
	calls    atomic.Int32
	gate     chan struct{} // holds the first real error until the timeouts were observed
}

func (c *cleanupScript) run(context.Context) error {
	if c.calls.Add(1) <= c.timeouts {
		return context.DeadlineExceeded
	}
	<-c.gate
	return errors.New("disk I/O error")
}

// drive runs loop in a goroutine, asserts timeouts alone never trip fault, and
// that the following real error does.
func (c *cleanupScript) drive(t *testing.T, loop func(), fault func() bool) {
	t.Helper()
	done := make(chan struct{})
	go func() { loop(); close(done) }()
	// The loop keeps ticking through every timeout and reaches the next call.
	waitFor(t, func() bool { return c.calls.Load() > c.timeouts })
	if fault() {
		t.Fatal("a cleanup timeout tripped the fault handling")
	}
	close(c.gate)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a real cleanup error did not end the loop")
	}
	if c.calls.Load() != c.timeouts+1 || !fault() {
		t.Fatalf("calls=%d fault=%v", c.calls.Load(), fault())
	}
}

func cleanupTestRuntime(t *testing.T) *operationrun.Runtime {
	t.Helper()
	f := operationWorkerFixture(t)
	fail := func() error { return errOperationConfig }
	rt, err := operationrun.New(f.state.store, operationrun.Config{WorkerID: "worker-a", Owner: "0123456789abcdef0123456789abcdef", Concurrency: 1, PollInterval: 20 * time.Millisecond}, operationrun.Hooks{
		Load:      func(context.Context, ledger.Record) (operations.Request, error) { return operations.Request{}, fail() },
		Authorize: func(context.Context, ledger.Record, operations.Request, ledger.Binding) error { return fail() },
		Prepare: func(context.Context, ledger.Record, operations.Request, ledger.Binding) (operationrun.Prepared, error) {
			return nil, fail()
		},
		AuditAdmission: func(context.Context, ledger.Record, operations.Request) error { return fail() },
	})
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func TestNodeCleanupLoopRetriesTimeoutsAndDrainsOnRealFault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dctx, dcancel := context.WithCancel(ctx)
	n := &Node{ctx: ctx, cancel: cancel, dispatchCancel: dcancel, cfg: NodeConfig{WorkerID: "a1"},
		operations: &nodeOperations{worker: &OperationWorker{runtime: cleanupTestRuntime(t)}}}
	_ = dctx
	s := &cleanupScript{timeouts: 3, gate: make(chan struct{})}
	s.drive(t, func() { n.cleanupResultsEvery(10*time.Millisecond, s.run) }, n.operations.failed.Load)
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.draining {
		t.Fatal("a real fault did not drain the node")
	}
}

func TestApplicationMaintainRetriesTimeoutsAndDrainsOnRealFault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &Application{ctx: ctx, runtime: cleanupTestRuntime(t)}
	s := &cleanupScript{timeouts: 3, gate: make(chan struct{})}
	s.drive(t, func() { a.maintainEvery(10*time.Millisecond, s.run) }, a.failed.Load)
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.draining {
		t.Fatal("a real fault did not drain the application")
	}
}

func TestExportCleanupLoopRetriesTimeoutsAndStopsOnRealFault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dctx, dcancel := context.WithCancel(ctx)
	r := &ExportRuntime{ctx: ctx, cancel: cancel, dispatchCtx: dctx, dispatchCancel: dcancel,
		cfg: NodeConfig{Exports: &ExportNodeConfig{CleanupInterval: 10 * time.Millisecond}}}
	s := &cleanupScript{timeouts: 3, gate: make(chan struct{})}
	s.drive(t, func() { r.cleanupEvery(s.run) }, func() bool { return ctx.Err() != nil })
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failure == nil {
		t.Fatal("a real fault did not record the export runtime failure")
	}
}
