// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
)

type reservation struct {
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
	started bool // guarded by Node.mu; a permit remains held until Execute exits
}

type Node struct {
	cfg            NodeConfig
	store          Store
	executor       query.Executor
	owner          string
	ctx            context.Context
	cancel         context.CancelFunc
	dispatchCtx    context.Context
	dispatchCancel context.CancelFunc
	dispatchDone   chan struct{}
	draining       bool // guarded by mu
	permits        chan struct{}
	mu             sync.Mutex
	jobs           map[string]*reservation
	wg             sync.WaitGroup
	streams        sync.WaitGroup
	once           sync.Once
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
	if executor.Config.Acceleration != nil && executor.Config.Acceleration.TenantID != cfg.Policy.TenantID {
		return nil, errors.New("acceleration tenant must match worker tenant")
	}
	return newNode(cfg, store, executor)
}

func newNode(cfg NodeConfig, store Store, executor query.Executor) (*Node, error) {
	if err := ValidatePolicy(cfg.Policy); err != nil {
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
	n := &Node{cfg: cfg, store: store, executor: executor, owner: owner, ctx: ctx, cancel: cancel, permits: make(chan struct{}, cfg.Policy.Workers[cfg.WorkerID]), jobs: map[string]*reservation{}}
	n.dispatchCtx, n.dispatchCancel = context.WithCancel(ctx)
	n.dispatchDone = make(chan struct{})
	probe, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	if _, err = executor.Execute(probe, query.Request{Mode: "federated", SQL: "SELECT 1"}, discardSink{}); err != nil {
		cancel()
		return nil, errors.New("sandboxed worker startup probe failed")
	}
	if err = store.ClaimWorker(probe, cfg.WorkerID, owner); err != nil {
		cancel()
		return nil, errors.New("worker identity is already active or its store is unavailable")
	}
	n.wg.Add(2)
	go n.dispatch()
	go n.heartbeat()
	return n, nil
}

type discardSink struct{}

func (discardSink) Schema(*arrow.Schema) error    { return nil }
func (discardSink) Write(arrow.RecordBatch) error { return nil }

func (n *Node) dispatch() {
	defer n.wg.Done()
	defer close(n.dispatchDone)
	for {
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
		ctx, cancel := context.WithDeadline(n.ctx, s.Job.ExpiresAt)
		r := &reservation{ctx: ctx, cancel: cancel, done: make(chan struct{})}
		n.mu.Lock()
		if n.draining {
			n.mu.Unlock()
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
			n.release(s.Job.ID, r)
			if errors.Is(err, ErrConflict) {
				_ = d.Ack(n.ctx)
			} else {
				_ = d.Retry(n.ctx)
			}
			continue
		}
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
			err := n.mutate(ctx, id, func(j *Job) error {
				if j.Terminal() {
					return ErrConflict
				}
				j.HeartbeatAt = time.Now().UTC()
				return nil
			})
			stop()
			if err != nil {
				n.finish(id, Failed, query.Stats{}, query.NewError("WORKER_LOST", "Worker lease could not be renewed"))
				n.stopReservation(id, r)
				return
			}
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
			ctx, stop := context.WithTimeout(n.ctx, n.cfg.Policy.LeaseDuration/3)
			err := n.store.HeartbeatWorker(ctx, n.cfg.WorkerID, n.owner)
			stop()
			if err != nil {
				n.cancel()
				return
			}
		}
	}
}

func (n *Node) mutate(ctx context.Context, id string, fn func(*Job) error) error {
	for range 8 {
		s, err := n.store.Get(ctx, id)
		if err != nil {
			return err
		}
		if s.Job.WorkerID != n.cfg.WorkerID || s.Job.Owner != n.owner {
			return ErrConflict
		}
		j := s.Job
		if err := fn(&j); err != nil {
			return err
		}
		_, err = n.store.CompareAndSwap(ctx, s, j)
		if !errors.Is(err, ErrConflict) {
			return err
		}
	}
	return ErrConflict
}

func (n *Node) finish(id, state string, stats query.Stats, result error) error {
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	return n.mutate(ctx, id, func(j *Job) error {
		if j.Terminal() {
			return ErrConflict
		}
		j.State = state
		j.Stats = stats
		if result != nil {
			j.Error = query.PublicError(result)
		}
		return nil
	})
}

// BeginDrain stops new reservations while existing jobs retain their leases and
// remain available through the result and cancellation endpoints.
func (n *Node) BeginDrain() {
	n.mu.Lock()
	n.draining = true
	n.dispatchCancel()
	n.mu.Unlock()
}

// Drain waits for reservations, including unclaimed results, to finish. The
// caller must invoke Close after the grace period to cancel remaining work.
func (n *Node) Drain(ctx context.Context) error {
	n.BeginDrain()
	select {
	case <-n.dispatchDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		n.mu.Lock()
		empty := len(n.jobs) == 0
		n.mu.Unlock()
		if empty {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
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
		_ = n.store.Close()
	})
	return nil
}

func (n *Node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 || r.TLS.PeerCertificates[0] == nil || !hasURI(r.TLS.PeerCertificates[0], GatewayIdentity) {
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
		ready := !n.draining && n.ctx.Err() == nil
		n.mu.Unlock()
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
		_ = n.finish(id, Cancelled, query.Stats{}, query.NewError("CANCELLED", "Query cancelled"))
		reserved.cancel()
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
	defer n.release(id, reserved)
	claim := r.Header.Get("X-Kelvo-Claim")
	var request query.Request
	var authority *JobAuthority
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
		return nil
	})
	if err != nil {
		_ = n.finish(id, Failed, query.Stats{}, query.NewError("QUERY_FAILED", "Query claim could not start"))
		http.Error(w, "query is unavailable or already consumed", http.StatusConflict)
		return
	}
	ctx, cancel := context.WithTimeout(reserved.ctx, n.cfg.Policy.Limits.Timeout)
	if authority != nil {
		ctx = context.WithValue(ctx, jobAuthorityKey{}, *authority)
	}
	stopClient := context.AfterFunc(r.Context(), cancel)
	defer func() { stopClient(); cancel() }()
	controller := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	_ = controller.SetWriteDeadline(deadline)
	done := make(chan struct{})
	stopWrite := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			_ = controller.SetWriteDeadline(time.Now())
		case <-stopWrite:
		}
	}()
	defer func() { close(stopWrite); <-done; _ = controller.SetWriteDeadline(time.Time{}) }()
	sink := &nodeSink{w: w, sink: worker.NewIPCSink(w, n.cfg.Policy.Limits)}
	defer sink.sink.Abort()
	executionStarted := time.Now()
	stats, err := n.executor.Execute(ctx, request, sink)
	if err == nil {
		err = sink.sink.Finish()
	}
	stats.WireBytes = sink.sink.EncodedBytes()
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = n.finish(id, ResultReady, stats, nil)
	} else {
		_ = n.finish(id, Failed, stats, err)
	}
	recordNodeHistory(n.cfg.RuntimeHistory, id, executionStarted, err)
	if err != nil {
		if sink.started {
			panic(http.ErrAbortHandler)
		}
		w.Header().Set("Content-Type", "application/json")
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
