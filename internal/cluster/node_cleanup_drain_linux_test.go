//go:build linux

package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	"github.com/SYNEHQ/kelvo-go/internal/operationrun"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
	"golang.org/x/sys/unix"
)

// A result-store cleanup that merely times out (another holder of the store's
// root lock, a slow scan under load) must not drain the node's query dispatch:
// the node would keep heartbeating while every Queued job waited forever (#240).
func TestNodeResultCleanupTimeoutDoesNotStopQueryDispatch(t *testing.T) {
	f := operationWorkerFixture(t)
	p := testPolicy()
	s := &nodeTestStore{p: p, jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}
	n, err := newNode(NodeConfig{Policy: p, WorkerID: "a1"}, s, probeExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { n.operations = nil; n.Close() }()

	dir := filepath.Join(t.TempDir(), "results")
	results, err := operationinput.Open(operationinput.Config{MaxInputBytes: 1 << 20, Storage: exports.Config{Directory: dir, Tenant: "team-a", MaxEntries: 64, MaxStoredBytes: 16 << 20, MaxTTL: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	defer results.Close()
	runtime, err := operationrun.New(f.state.store, operationrun.Config{WorkerID: "worker-a", Owner: n.owner, Concurrency: 1, PollInterval: 20 * time.Millisecond}, operationrun.Hooks{
		Load: func(context.Context, ledger.Record) (operations.Request, error) {
			return operations.Request{}, errOperationConfig
		},
		Authorize: func(context.Context, ledger.Record, operations.Request, ledger.Binding) error {
			return errOperationConfig
		},
		Prepare: func(context.Context, ledger.Record, operations.Request, ledger.Binding) (operationrun.Prepared, error) {
			return nil, errOperationConfig
		},
		AuditAdmission: func(context.Context, ledger.Record, operations.Request) error { return errOperationConfig },
	})
	if err != nil {
		t.Fatal(err)
	}
	n.operations = &nodeOperations{results: results, worker: &OperationWorker{runtime: runtime}}

	// Hold the store's root lock past the cleanup's 5 s budget.
	lock, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	n.wg.Add(1)
	go n.cleanupOperationResults()
	time.Sleep(7 * time.Second)

	const id = "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	s.add(id)
	waitFor(t, func() bool { v, _ := s.Get(context.Background(), id); return v.Job.State == Assigned })
}
