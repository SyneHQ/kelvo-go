// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/httpstream"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
)

type reservation struct {
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	once          sync.Once
	started       bool          // guarded by Node.mu; a permit remains held until Execute exits
	leaseMutation chan struct{} // initialized under Node.mu; renewal and ResultReady only
	last          *Snapshot     // guarded by Node.mu; this node's latest write of the job, tried before a read
}

type Node struct {
	operations            *nodeOperations
	obs                   *readObserver
	closeOperationRuntime func()
	exports               *ExportRuntime
	audit                 *ServiceAudit
	ownAudit              bool
	closeErr              error
	cfg                   NodeConfig
	store                 Store
	executor              query.Executor
	owner                 string
	ctx                   context.Context
	cancel                context.CancelFunc
	dispatchCtx           context.Context
	dispatchCancel        context.CancelFunc
	dispatchDone          chan struct{}
	leaseFailure          chan struct{}
	leaseFailureCause     CoordinationFailure // guarded by mu; fixed after fencing
	draining              bool                // guarded by mu
	failed                atomic.Bool         // fault drain: stop renewing the lease
	permits               chan struct{}
	mu                    sync.Mutex
	jobs                  map[string]*reservation
	wg                    sync.WaitGroup
	streams               sync.WaitGroup
	once                  sync.Once
}

func randomToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// NewNode is deliberately Linux-only. Cluster execution must use the native
// pre-runtime sandbox launcher, not an unsandboxed subprocess fallback.
func NewNode(cfg NodeConfig, store Store, executor *worker.Executor) (*Node, error) {
	if runtime.GOOS != "linux" || executor == nil || executor.SandboxPath == "" {
		return nil, errors.New("cluster nodes require the Linux sandbox launcher")
	}
	policy, err := clonePolicy(cfg.Policy)
	if err != nil {
		return nil, err
	}
	cfg.Policy = policy
	if cfg.Policy.Access != nil {
		for _, grant := range cfg.Policy.Access.Principals {
			if resolver := grant.DelegatedResolver; resolver != nil && !executor.HasConnectionResolver(resolver.Issuer, resolver.URL) {
				return nil, errors.New("delegated worker resolver does not match tenant policy")
			}
		}
	}
	executor, err = bindCatalogExecutor(cfg.Policy, executor)
	if err != nil {
		return nil, err
	}
	if executor.Config.Acceleration != nil && executor.Config.Acceleration.TenantID != cfg.Policy.TenantID {
		return nil, errors.New("acceleration tenant must match worker tenant")
	}
	if err := ValidateExportCatalog(cfg, executor.Config); err != nil {
		return nil, err
	}
	if err := executor.ValidateObjectRuntime(); err != nil {
		return nil, err
	}
	return newNode(cfg, store, executor)
}

func newNode(cfg NodeConfig, store Store, executor query.Executor) (*Node, error) {
	if cfg.Operations != nil {
		copy := *cfg.Operations
		copy.Adapter = cfg.Operations.Adapter.Clone()
		cfg.Operations = &copy
	}
	policy, err := clonePolicy(cfg.Policy)
	if err != nil {
		return nil, err
	}
	cfg.Policy = policy
	if err := validateNodeAudit(cfg); err != nil {
		return nil, err
	}
	if err := ValidatePolicy(cfg.Policy); err != nil {
		return nil, err
	}
	if err := validateNodeExports(cfg); err != nil {
		return nil, err
	}
	if err := validateNodeOperations(cfg); err != nil {
		return nil, err
	}
	if store == nil || executor == nil || !reflect.DeepEqual(store.Policy(), cfg.Policy) || cfg.Policy.Workers[cfg.WorkerID] < 1 {
		return nil, errors.New("invalid tenant worker configuration")
	}
	owner, err := randomToken()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{cfg: cfg, store: store, executor: executor, owner: owner, ctx: ctx, cancel: cancel, permits: make(chan struct{}, cfg.Policy.Workers[cfg.WorkerID]), jobs: map[string]*reservation{}, leaseFailure: make(chan struct{})}

	if cfg.RuntimeAudit != nil {
		if !cfg.RuntimeAudit.matches(cfg.Audit, "worker", []string{cfg.Policy.TenantID}) {
			cancel()
			return nil, audit.ErrInvalid
		}
		n.audit = cfg.RuntimeAudit
	} else {
		n.audit, err = OpenServiceAudit(cfg.Audit, "worker", []string{cfg.Policy.TenantID})
		if err != nil {
			cancel()
			return nil, err
		}
		n.ownAudit = true
	}
	started := false
	defer func() {
		if !started && n.closeOperationRuntime != nil {
			n.closeOperationRuntime()
		}
		if !started && n.ownAudit {
			_ = n.audit.CloseBounded()
		}
	}()
	if cfg.Operations != nil {
		prepare, stop := context.WithTimeout(ctx, 30*time.Second)
		prepared, closeRuntime, err := cfg.Operations.Adapter.PrepareRuntime(prepare)
		stop()
		if err != nil {
			cancel()
			return nil, err
		}
		n.cfg.Operations.Adapter = prepared
		n.closeOperationRuntime = closeRuntime
	}
	n.dispatchCtx, n.dispatchCancel = context.WithCancel(ctx)
	n.dispatchDone = make(chan struct{})
	probe, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()

	probeAudit, err := n.audit.beginService(probe, cfg.Policy, audit.QueryExecution)
	if err != nil {
		cancel()
		return nil, err
	}
	defer probeAudit.abort(probe)
	_, err = executor.Execute(probe, query.Request{Mode: "federated", SQL: "SELECT 1"}, discardSink{})
	err = errors.Join(err, probeAudit.complete(err))
	if err != nil {
		cancel()
		return nil, errors.New("sandboxed worker startup probe failed")
	}
	if err = store.ClaimWorker(probe, cfg.WorkerID, owner); err != nil {
		cancel()
		if failure, ok := CoordinationDiagnostic(err); ok {
			return nil, &coordinationError{message: "worker identity is already active or its store is unavailable", failure: failure}
		}
		return nil, errors.New("worker identity is already active or its store is unavailable")
	}
	if cfg.Exports != nil {
		jobs := cfg.RuntimeExportStore
		if jobs == nil {
			base, ok := store.(*NATSStore)
			if !ok {
				cancel()
				return nil, errors.New("exports require a retained tenant job store")
			}
			jobs, err = OpenExportStore(probe, base, false)
			if err != nil {
				cancel()
				return nil, err
			}
		}
		native, ok := executor.(*worker.Executor)
		if !ok {
			cancel()
			return nil, errors.New("exports require the sandboxed worker executor")
		}
		exportConfig := cfg
		exportConfig.RuntimeAudit = n.audit
		n.exports, err = NewExportRuntime(exportConfig, jobs, native, owner)
		if err != nil {
			cancel()
			return nil, err
		}
		context.AfterFunc(n.ctx, n.exports.Stop)
	}
	if err := n.initOperations(probe); err != nil {
		cancel()
		if n.exports != nil {
			_ = n.exports.Close()
		}
		return nil, err
	}
	if n.operations != nil {
		if err := n.operations.worker.Start(n.ctx); err != nil {
			cancel()
			_ = n.operations.close(context.Background())
			if n.exports != nil {
				_ = n.exports.Close()
			}
			return nil, err
		}
		n.wg.Add(1)
		go n.cleanupOperationResults()
	}
	n.obs = newReadObserver(cfg.Observability)
	if cfg.Observability != nil && cfg.Observability.StallDump != nil {
		n.obs.stall = newStallDetector(*cfg.Observability.StallDump, n.obs)
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.obs.stall.run(n.ctx) }()
	}
	n.wg.Add(3)
	go n.dispatch()
	go n.heartbeat()
	go n.dispatchWatchdog()
	started = true
	return n, nil
}

type discardSink struct{}

func (discardSink) Schema(*arrow.Schema) error    { return nil }
func (discardSink) Write(arrow.RecordBatch) error { return nil }

func (n *Node) dispatch() {
	defer n.wg.Done()
	defer close(n.dispatchDone)
	for {
		if !n.audit.Ready() {
			select {
			case <-n.dispatchCtx.Done():
				return
			case <-time.After(200 * time.Millisecond):
				continue
			}
		}
		select {
		case <-n.dispatchCtx.Done():
			return
		case n.permits <- struct{}{}:
		}
		d, err := n.store.Next(n.dispatchCtx)
		if err != nil {
			<-n.permits
			if n.dispatchCtx.Err() != nil {
				return
			}
			if !errors.Is(err, ErrNoJob) {
				select {
				case <-n.dispatchCtx.Done():
					return
				case <-time.After(200 * time.Millisecond):
				}
			}
			continue
		}
		if !n.audit.Ready() {
			_ = d.Retry(n.ctx)
			<-n.permits
			continue
		}
		s, err := n.store.Get(n.ctx, d.ID())
		if err != nil || s.Job.State != Queued || !time.Now().Before(s.Job.ExpiresAt) {
			if errors.Is(err, ErrNotFound) || err == nil {
				_ = d.Ack(n.ctx)
			} else {
				_ = d.Retry(n.ctx)
			}
			<-n.permits
			continue
		}
		if err := validateJobAuthority(n.cfg.Policy, s.Job.Authority, s.Job.Request); err != nil {
			next := s.Job
			next.State, next.Error = Failed, query.PublicError(err)
			if _, updateErr := n.store.CompareAndSwap(n.ctx, s, next); updateErr == nil || errors.Is(updateErr, ErrConflict) {
				_ = d.Ack(n.ctx)
			} else {
				_ = d.Retry(n.ctx)
			}
			<-n.permits
			continue
		}
		span := n.cfg.RuntimeTracing.Start(jobTrace(s.Job.Trace), tracing.ClusterDispatch)
		ctx, cancel := context.WithDeadline(n.ctx, s.Job.ExpiresAt)
		r := &reservation{ctx: ctx, cancel: cancel, done: make(chan struct{})}
		n.mu.Lock()
		if n.draining {
			n.mu.Unlock()
			span.End(telemetry.OutcomeCanceled)
			cancel()
			_ = d.Retry(n.ctx)
			<-n.permits
			return
		}
		n.jobs[s.Job.ID] = r
		n.mu.Unlock()
		j := s.Job
		j.State = Assigned
		j.WorkerID = n.cfg.WorkerID
		j.Owner = n.owner
		j.HeartbeatAt = time.Now().UTC()
		if _, err = n.store.CompareAndSwap(n.ctx, s, j); err != nil {
			outcome := telemetry.OutcomeError
			finishClusterTrace(span, n.ctx, &outcome)
			n.release(s.Job.ID, r)
			if errors.Is(err, ErrConflict) {
				_ = d.Ack(n.ctx)
			} else {
				_ = d.Retry(n.ctx)
			}
			continue
		}
		span.End(telemetry.OutcomeSuccess)
		// An ACK failure may redeliver the envelope. Its durable state is already
		// ASSIGNED, so another node must acknowledge it without another execution.
		_ = d.Ack(n.ctx)
		n.wg.Add(1)
		go n.watch(s.Job.ID, r)
	}
}

func (n *Node) release(id string, r *reservation) {
	r.once.Do(func() {
		close(r.done)
		r.cancel()
		n.mu.Lock()
		if n.jobs[id] == r {
			delete(n.jobs, id)
		}
		n.mu.Unlock()
		<-n.permits
	})
}

func (n *Node) stopReservation(id string, r *reservation) {
	r.cancel()
	n.mu.Lock()
	started := r.started
	n.mu.Unlock()
	if !started {
		n.release(id, r)
	}
}

func (n *Node) watch(id string, r *reservation) {
	defer n.wg.Done()
	tick := time.NewTicker(min(n.cfg.Policy.LeaseDuration/3, time.Second))
	defer tick.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-r.ctx.Done():
			select {
			case <-r.done:
				return
			default:
			}
			n.finish(id, Failed, query.Stats{}, query.NewError("WORKER_LOST", "Worker execution ended before completion"))
			n.stopReservation(id, r)
			return
		case <-tick.C:
			ctx, stop := context.WithTimeout(n.ctx, n.cfg.Policy.LeaseDuration/3)
			err := n.renewLease(ctx, id)
			stop()
			if err != nil {
				// A completed handler may release its reservation while this
				// renewal is still returning. Its durable result remains for the
				// gateway to commit; do not turn that handoff into worker loss.
				select {
				case <-r.done:
					return
				default:
				}
				n.finish(id, Failed, query.Stats{}, query.NewError("WORKER_LOST", "Worker lease could not be renewed"))
				n.stopReservation(id, r)
				return
			}
		}
	}
}

var errLeaseCurrent = errors.New("worker lease does not need renewal")

// Poll durable cancellation at the existing cadence, but do not rewrite a fresh
// lease. Claim and completion transitions also renew HeartbeatAt atomically.
func (n *Node) renewLease(ctx context.Context, id string) error {
	unlock, err := n.lockLeaseMutation(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	err = n.mutate(ctx, id, func(j *Job) error {
		if j.Terminal() {
			return ErrConflict
		}
		now := time.Now().UTC()
		age := now.Sub(j.HeartbeatAt)
		if age >= 0 && age < n.cfg.Policy.LeaseDuration/3 {
			return errLeaseCurrent
		}
		j.HeartbeatAt = now
		return nil
	})
	if errors.Is(err, errLeaseCurrent) {
		return nil
	}
	return err
}

// The local renewal loop must not race its own final result publication. This
// gate intentionally excludes cancellation and failure, which retain authority
// to fence either operation through the durable CAS and terminal-state checks.
func (n *Node) lockLeaseMutation(ctx context.Context, id string) (func(), error) {
	n.mu.Lock()
	r := n.jobs[id]
	if r == nil {
		n.mu.Unlock()
		return nil, ErrConflict
	}
	if r.leaseMutation == nil {
		r.leaseMutation = make(chan struct{}, 1)
	}
	gate := r.leaseMutation
	n.mu.Unlock()
	select {
	case gate <- struct{}{}:
		unlock := func() { <-gate }
		if err := ctx.Err(); err != nil {
			unlock()
			return nil, err
		}
		n.mu.Lock()
		current := n.jobs[id] == r
		n.mu.Unlock()
		if !current {
			unlock()
			return nil, ErrConflict
		}
		select {
		case <-r.done:
			unlock()
			return nil, ErrConflict
		default:
		}
		if r.ctx != nil && r.ctx.Err() != nil {
			unlock()
			return nil, r.ctx.Err()
		}
		return unlock, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// dispatchWatchdog fails the node if dispatch exits while it is not draining:
// the node would otherwise stay registered and claim nothing. It logs slot
// accounting for the diagnosis.
// ponytail: Store has no queue depth, so "idle with queued jobs" is not
// detectable; add a depth probe to Store if that case shows up.
func (n *Node) dispatchWatchdog() {
	defer n.wg.Done()
	select {
	case <-n.ctx.Done():
	case <-n.dispatchDone:
		n.mu.Lock()
		draining, jobs := n.draining, len(n.jobs)
		n.mu.Unlock()
		if !draining && n.ctx.Err() == nil {
			log.Printf("kelvo: node %s dispatch exited while not draining (permits %d/%d, jobs %d); failing node", n.cfg.WorkerID, len(n.permits), cap(n.permits), jobs)
			n.Fail()
		}
	}
}

func (n *Node) heartbeat() {
	defer n.wg.Done()
	tick := time.NewTicker(n.cfg.Policy.LeaseDuration / 3)
	defer tick.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-tick.C:
			// A node that drained on a fault claims nothing; let its lease
			// lapse so the gateway routes elsewhere. Graceful drain keeps renewing.
			if n.failed.Load() {
				continue
			}
			ctx, stop := context.WithTimeout(n.ctx, n.cfg.Policy.LeaseDuration/3)
			err := n.store.HeartbeatWorker(ctx, n.cfg.WorkerID, n.owner)
			stop()
			if err != nil {
				n.mu.Lock()
				// Close cancels the same context under mu. A renewal interrupted
				// by ordinary shutdown must not become a permanent lease fault.
				if n.ctx.Err() == nil {
					failure, ok := CoordinationDiagnostic(err)
					if !ok {
						failure = coordinationUnavailable("worker_renew", err).failure
					}
					n.leaseFailureCause = failure
					n.cancel()
					close(n.leaseFailure)
				}
				n.mu.Unlock()
				return
			}
		}
	}
}

// LeaseFailure closes after the first failed worker-identity renewal permanently
// fences this owner. A closed channel preserves a failure that occurs before a
// supervisor subscribes. Normal drain/Close never signals it. The node cannot
// reactivate itself; its supervisor must start a new owner after safe shutdown.
func (n *Node) LeaseFailure() <-chan struct{} { return n.leaseFailure }

// LeaseFailureCause returns a copy of the first permanent renewal failure.
// It is zero before failure; normal shutdown does not manufacture a cause.
func (n *Node) LeaseFailureCause() CoordinationFailure {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaseFailureCause
}

func (n *Node) mutate(ctx context.Context, id string, fn func(*Job) error) error {
	return n.mutateFrom(ctx, id, fn, false)
}

// mutateFrom is mutate; with fromLastWrite the first attempt starts from this
// node's own latest write of the job, if it has one. When nobody has written
// since, the update succeeds without reading the job first. A newer revision (a
// cancellation, the gateway's claim) fails that update with a conflict and the
// attempt repeats from a fresh read, exactly as before. The stale-hint cost
// is bounded: at most one wasted update per call (the hint is used once, then
// cleared), after which every attempt reads first. Only the result
// publication asks for it: everything else keeps reading first.
func (n *Node) mutateFrom(ctx context.Context, id string, fn func(*Job) error, fromLastWrite bool) error {
	var hint *Snapshot
	if fromLastWrite {
		hint = n.lastWrite(id)
	}
	for range 8 {
		var s Snapshot
		if hint != nil {
			s, hint = *hint, nil
		} else {
			var err error
			if s, err = n.store.Get(ctx, id); err != nil {
				return err
			}
		}
		if s.Job.WorkerID != n.cfg.WorkerID || s.Job.Owner != n.owner {
			return ErrConflict
		}
		j := s.Job
		if err := fn(&j); err != nil {
			return err
		}
		written, err := n.store.CompareAndSwap(ctx, s, j)
		if err == nil {
			n.rememberWrite(id, written)
		}
		if !errors.Is(err, ErrConflict) {
			return err
		}
	}
	return ErrConflict
}

func (n *Node) lastWrite(id string) *Snapshot {
	n.mu.Lock()
	defer n.mu.Unlock()
	if r := n.jobs[id]; r != nil && r.last != nil {
		s := *r.last
		r.last = nil
		return &s
	}
	return nil
}

func (n *Node) rememberWrite(id string, s Snapshot) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if r := n.jobs[id]; r != nil && !s.Job.Terminal() {
		r.last = &s
	}
}

func (n *Node) finish(id, state string, stats query.Stats, result error) error {
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if state == ResultReady {
		unlock, err := n.lockLeaseMutation(ctx, id)
		if err != nil {
			return err
		}
		defer unlock()
	}
	return n.mutateFrom(ctx, id, func(j *Job) error {
		if j.Terminal() {
			return ErrConflict
		}
		j.State = state
		j.Stats = stats
		if result != nil {
			j.Error = query.PublicError(result)
		}
		return nil
	}, state == ResultReady)
}

// BeginDrain stops new reservations while existing jobs retain their leases and
// remain available through the result and cancellation endpoints.
func (n *Node) BeginDrain() {
	n.mu.Lock()
	n.draining = true
	n.dispatchCancel()
	n.mu.Unlock()
	if n.exports != nil {
		n.exports.BeginDrain()
	}
	if n.operations != nil {
		n.operations.worker.BeginDrain()
	}
}

// Fail drains the node because it is unhealthy and stops renewing its lease,
// unlike BeginDrain, which keeps it registered while in-flight work finishes.
func (n *Node) Fail() {
	n.failed.Store(true)
	n.BeginDrain()
}

// Drain waits for reservations, including unclaimed results, to finish. The
// caller must invoke Close after the grace period to cancel remaining work.
func (n *Node) Drain(ctx context.Context) error {
	n.BeginDrain()
	var resultErr error
	if n.operations != nil {
		if err := n.operations.worker.Drain(ctx); err != nil {
			if !n.operations.worker.runtime.Joined() {
				return err
			}
			resultErr = err
		}
	}
	select {
	case <-n.dispatchDone:
	case <-ctx.Done():
		return errors.Join(resultErr, ctx.Err())
	}
	if n.exports != nil {
		if err := n.exports.Drain(ctx); err != nil {
			return errors.Join(resultErr, err)
		}
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		n.mu.Lock()
		empty := len(n.jobs) == 0
		n.mu.Unlock()
		if empty {
			return resultErr
		}
		select {
		case <-ctx.Done():
			return errors.Join(resultErr, ctx.Err())
		case <-tick.C:
		}
	}
}

func (n *Node) Close() error {
	n.once.Do(func() {
		n.BeginDrain()
		n.mu.Lock()
		n.cancel()
		n.mu.Unlock()
		n.wg.Wait()
		n.streams.Wait()
		n.closeErr = errors.Join(n.closeErr, n.operations.close(context.Background()))
		if n.closeOperationRuntime != nil {
			n.closeOperationRuntime()
		}
		if n.exports != nil {
			n.closeErr = errors.Join(n.closeErr, n.exports.Close())
		}
		if n.ownAudit {
			n.closeErr = errors.Join(n.closeErr, n.audit.CloseBounded())
		}
		_ = n.store.Close()
	})
	return n.closeErr
}

func (n *Node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 || r.TLS.PeerCertificates[0] == nil || !hasURI(r.TLS.PeerCertificates[0], GatewayIdentity) {
		n.audit.authenticationDenied()
		http.Error(w, "mutual TLS gateway identity required", http.StatusUnauthorized)
		return
	}
	if r.URL.Path == "/sources" && n.cfg.RuntimeSourceHealth != nil {
		n.serveSourceHealth(w, r)
		return
	}
	if r.URL.Path == "/datasets" && n.cfg.RuntimeDatasets != nil {
		n.cfg.RuntimeDatasets.ServeHTTP(w, r)
		return
	}
	if r.URL.Path == "/metrics" && n.cfg.RuntimeMetrics != nil {
		n.cfg.RuntimeMetrics.ServeHTTP(w, r)
		if r.Method == http.MethodGet {
			n.writeReadAdapterMetrics(w)
			n.obs.writeMetrics(w)
		}
		return
	}
	if r.URL.Path == "/resources" && n.cfg.RuntimeResources != nil {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		// Export only aggregate configured reservations, never process secrets
		// or tenant/query identifiers. Snapshot releases its lock before I/O.
		_ = json.NewEncoder(w).Encode(n.cfg.RuntimeResources.Snapshot())
		return
	}
	if r.URL.Path == "/history" && n.cfg.RuntimeHistory != nil {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(n.cfg.RuntimeHistory.Entries())
		return
	}
	if n.ctx.Err() != nil {
		http.Error(w, "worker unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/health" {
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/ready" {
		n.mu.Lock()
		ready := !n.draining && n.ctx.Err() == nil && n.audit.Ready()
		n.mu.Unlock()
		if ready && n.exports != nil {
			ready = n.exports.Ready()
		}
		if ready && n.operations != nil {
			ready = !n.operations.failed.Load() && n.operations.worker.Ready()
		}
		if native, ok := n.executor.(*worker.Executor); ready && ok {
			ready = (native.ResourcePool == nil || !native.ResourcePool.Snapshot().Draining) && (native.Containment == nil || native.Containment.Err() == nil)
		}
		if ready && !n.cfg.RuntimeDatasets.hasRequired(n.cfg.RequiredDatasets) {
			ready = false
		}
		if ready && n.cfg.RuntimeDatasets != nil {
			ready = n.cfg.RuntimeDatasets.Ready(r.Context())
		}
		if !ready {
			http.Error(w, "worker unavailable", http.StatusServiceUnavailable)
		} else {
			_, _ = w.Write([]byte("ready\n"))
		}
		return
	}
	if n.exports != nil && n.exports.ServeHTTP(w, r) {
		return
	}
	if n.serveOperationResult(w, r) {
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 4 || parts[0] != "internal" || parts[1] != "queries" {
		http.NotFound(w, r)
		return
	}
	id := parts[2]
	n.mu.Lock()
	reserved := n.jobs[id]
	n.mu.Unlock()
	if reserved == nil {
		http.NotFound(w, r)
		return
	}
	if parts[3] == "cancel" && r.Method == http.MethodPost {
		op, err := n.audit.beginGateway(r.Context(), n.cfg.Policy, audit.QueryCancel)
		if err != nil {
			auditBeginHTTPError(w, err, "audit storage unavailable")
			return
		}
		defer op.abort(r.Context())
		err = n.finish(id, Cancelled, query.Stats{}, query.NewError("CANCELLED", "Query cancelled"))
		reserved.cancel()
		if auditErr := op.complete(err); auditErr != nil {
			http.Error(w, "audit storage unavailable", http.StatusServiceUnavailable)
			return
		}
		if err != nil {
			http.Error(w, "query cancellation state unavailable", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if parts[3] != "results" || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	n.mu.Lock()
	if n.ctx.Err() != nil || reserved.ctx.Err() != nil || reserved.started || n.jobs[id] != reserved {
		n.mu.Unlock()
		http.Error(w, "query is unavailable or already consumed", http.StatusConflict)
		return
	}
	reserved.started = true
	n.streams.Add(1)
	n.mu.Unlock()
	defer n.streams.Done()
	// A refusal for want of an audit reservation leaves the job exactly as it
	// was, claimable again, so it keeps its reservation here.
	retryable := false
	defer func() {
		if !retryable {
			n.release(id, reserved)
		}
	}()
	claim := r.Header.Get("X-Kelvo-Claim")
	// Reserve the audit slot BEFORE consuming the claim or changing the job. A
	// full queue then answers 429 with nothing changed, so the retry it invites
	// can succeed. (If the job cannot be read or is not claimable, the claim
	// path below fails it exactly as before.)
	var op *auditOperation
	reservedAudit := false
	if pre, getErr := n.store.Get(r.Context(), id); getErr == nil && pre.Job.State == Claimed && len(claim) == 32 && subtle.ConstantTimeCompare([]byte(pre.Job.Claim), []byte(claim)) == 1 {
		var auditErr error
		op, auditErr = n.audit.begin(r.Context(), n.cfg.Policy.TenantID, pre.Job.Authority, audit.QueryExecution)
		if auditErr != nil {
			if errors.Is(auditErr, audit.ErrBusy) {
				n.mu.Lock()
				reserved.started = false
				n.mu.Unlock()
				retryable = true
			} else {
				_ = n.finish(id, Failed, query.Stats{}, query.NewError("UNAVAILABLE", "Audit storage unavailable"))
			}
			auditBeginHTTPError(w, auditErr, "audit storage unavailable")
			return
		}
		reservedAudit = true
	}
	var request query.Request
	var authority *JobAuthority
	var carrier tracing.Carrier
	var delegatedClaims delegation.Claims
	var executionBinding delegation.ExecutionBinding
	err := n.mutate(r.Context(), id, func(j *Job) error {
		if j.State != Claimed || len(claim) != 32 || subtle.ConstantTimeCompare([]byte(j.Claim), []byte(claim)) != 1 {
			return ErrConflict
		}
		if err := validateJobAuthority(n.cfg.Policy, j.Authority, j.Request); err != nil {
			return err
		}
		if err := query.ValidateRequest(j.Request); err != nil {
			return err
		}
		j.State = Running
		j.HeartbeatAt = time.Now().UTC()
		request = j.Request
		authority = j.Authority
		carrier = jobTrace(j.Trace)
		if request.Delegation != "" {
			_, claims, err := delegatedAuthority(n.cfg.Policy, *authority, request, time.Now())
			if err != nil {
				return err
			}
			delegatedClaims = claims
			executionBinding = delegation.ExecutionBinding{JobID: j.ID, WorkerID: j.WorkerID, Owner: j.Owner, Claim: j.Claim}
		}
		return nil
	})
	if err != nil {
		if reservedAudit {
			op.abort(r.Context()) // the reservation was for an execution that never started
		}
		_ = n.finish(id, Failed, query.Stats{}, query.NewError("QUERY_FAILED", "Query claim could not start"))
		http.Error(w, "query is unavailable or already consumed", http.StatusConflict)
		return
	}
	executionDeadline := time.Now().Add(n.cfg.Policy.Limits.Timeout)
	if request.Delegation != "" {
		executionDeadline = minTime(executionDeadline, time.Unix(delegatedClaims.ExpiresAt, 0))
	}
	ctx, cancel := context.WithDeadline(reserved.ctx, executionDeadline)
	ctx = tracing.WithCarrier(ctx, carrier)
	if authority != nil {
		ctx = context.WithValue(ctx, jobAuthorityKey{}, *authority)
	}
	stopClient := context.AfterFunc(r.Context(), cancel)
	defer func() { stopClient(); cancel() }()
	var completion containment.Completion
	ctx = containment.WithCompletion(ctx, &completion)
	defer completion.Complete()
	if !reservedAudit {
		// The job could not be read beforehand: reserve now, as before. A refusal
		// here has already consumed the claim, so it fails the job.
		var auditErr error
		op, auditErr = n.audit.begin(ctx, n.cfg.Policy.TenantID, authority, audit.QueryExecution)
		if auditErr != nil {
			if errors.Is(auditErr, audit.ErrBusy) {
				_ = n.finish(id, Failed, query.Stats{}, query.NewError("RESOURCE_EXHAUSTED", "Audit queue full"))
			} else {
				_ = n.finish(id, Failed, query.Stats{}, query.NewError("UNAVAILABLE", "Audit storage unavailable"))
			}
			auditBeginHTTPError(w, auditErr, "audit storage unavailable")
			return
		}
	}
	defer op.abort(ctx)
	deadline, _ := ctx.Deadline()
	stopWrites := httpstream.WatchWriteDeadline(ctx, w, deadline)
	defer stopWrites()
	tail := &arrowEOSTail{w: w}
	sink := &nodeSink{w: w, sink: worker.NewIPCSink(tail, n.cfg.Policy.Limits)}
	defer sink.sink.Abort()
	executionStarted := time.Now()
	n.obs.begin()
	var stats query.Stats
	executionCtx, err := executionAuthorityContext(ctx, n.cfg.Policy, authority, request)
	if err == nil {
		if request.Delegation != "" {
			executionCtx = delegation.WithExecution(executionCtx, delegatedClaims, request.Delegation, executionBinding)
		}
		stats, err = n.executor.Execute(executionCtx, request, sink)
	}
	if err == nil {
		err = sink.sink.Finish()
	}
	if err == nil && !tail.validEOS() {
		err = query.NewError("QUERY_FAILED", "Incomplete Arrow stream")
	}
	stats.WireBytes = sink.sink.EncodedBytes()
	if err == nil {
		err = ctx.Err()
	}
	err = errors.Join(err, op.complete(err))
	if err == nil {
		err = n.finish(id, ResultReady, stats, nil)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = tail.FlushEOS()
	}
	if err != nil {
		_ = n.finish(id, Failed, stats, err)
	}
	recordNodeHistory(n.cfg.RuntimeHistory, id, executionStarted, err)
	n.obs.end(n.cfg.Policy.TenantID, id, request, time.Since(executionStarted), stats, err)
	if err != nil {
		if sink.started {
			panic(http.ErrAbortHandler)
		}
		w.Header().Set("Content-Type", "application/json")
		// Only a refusal for capacity is retryable: it carries Retry-After. A limit
		// the read hit (rows, bytes, memory) never does.
		if after, capacity := worker.CapacityRetryAfter(err); capacity {
			w.Header().Set("Retry-After", strconv.Itoa(max(1, int((after+time.Second-1)/time.Second))))
		}
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": query.PublicError(err)})
	}
}

type nodeSink struct {
	w       http.ResponseWriter
	sink    *worker.IPCSink
	started bool
}

func (s *nodeSink) Schema(schema *arrow.Schema) error {
	if schema == nil || s.started {
		return query.NewError("QUERY_FAILED", "Invalid result schema")
	}
	if err := s.sink.Schema(schema); err != nil {
		return err
	}
	s.w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	s.w.Header().Set("Cache-Control", "no-store")
	s.w.WriteHeader(http.StatusOK)
	s.started = true
	return nil
}
func (s *nodeSink) Write(b arrow.RecordBatch) error { return s.sink.Write(b) }
