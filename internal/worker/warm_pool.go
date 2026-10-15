// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapter/frame"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// WarmPoolConfig configures the warm worker pool for small read-only operations.
type WarmPoolConfig struct {
	Enabled        bool          `yaml:"enabled"`
	IdleTTL        time.Duration `yaml:"idle_ttl"`          // default 30m
	MaxWorkers     int           `yaml:"max_workers"`       // default 4
	MaxPerKey      int           `yaml:"max_per_key"`       // default 1
	QueueDepth     int           `yaml:"queue_depth"`       // default 32
	QueueTimeout   time.Duration `yaml:"queue_timeout"`     // default 10s
	WorkerMemoryMB int           `yaml:"worker_memory_mb"`  // default 64
}

// errWarmUnavailable means no warm worker could be obtained and nothing was
// sent to one. The caller runs the operation on the one-shot path instead.
var errWarmUnavailable = errors.New("warm_pool: no warm worker available")

func DefaultWarmPoolConfig() WarmPoolConfig {
	return WarmPoolConfig{
		Enabled:        false,
		IdleTTL:        30 * time.Minute,
		MaxWorkers:     4,
		MaxPerKey:      1,
		QueueDepth:     32,
		QueueTimeout:   10 * time.Second,
		WorkerMemoryMB: 64,
	}
}

func (c WarmPoolConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.IdleTTL < 10*time.Millisecond || c.IdleTTL > 24*time.Hour {
		return errors.New("warm_pool: idle_ttl must be between 10ms and 24h")
	}
	if c.MaxWorkers < 1 || c.MaxWorkers > 256 {
		return errors.New("warm_pool: max_workers must be between 1 and 256")
	}
	if c.MaxPerKey < 1 || c.MaxPerKey > c.MaxWorkers {
		return errors.New("warm_pool: max_per_key must be between 1 and max_workers")
	}
	if c.QueueDepth < 1 || c.QueueDepth > 1024 {
		return errors.New("warm_pool: queue_depth must be between 1 and 1024")
	}
	if c.QueueTimeout < 10*time.Millisecond || c.QueueTimeout > time.Minute {
		return errors.New("warm_pool: queue_timeout must be between 10ms and 1m")
	}
	if c.WorkerMemoryMB < 16 || c.WorkerMemoryMB > 1024 {
		return errors.New("warm_pool: worker_memory_mb must be between 16 and 1024")
	}
	return nil
}

// WarmPoolMetrics records observability counters for the warm pool.
type WarmPoolMetrics struct {
	Hits           atomic.Int64
	Misses         atomic.Int64
	Spawns         atomic.Int64
	QueueWaitMS    atomic.Int64
	EvictionsTTL   atomic.Int64
	EvictionsRev   atomic.Int64
	EvictionsErr   atomic.Int64
	EvictionsDrain atomic.Int64
	Fallbacks      atomic.Int64
}

// WorkerProcess represents an interactive worker process with framed IO.
type WorkerProcess interface {
	Stdin() io.Writer
	Stdout() io.Reader
	Kill() error
	Wait() error
}

// WorkerLauncher spawns a long-lived adapter serve process.
type WorkerLauncher interface {
	Launch(ctx context.Context, e *Executor, cfg OperationProcessConfig, limits query.Limits) (WorkerProcess, func() error, error)
}

type warmWorkerEntry struct {
	id                    string
	key                   string
	tenantID              string
	appTeam               string
	connectionID          string
	revision              string
	credentialsValidUntil int64
	lastActive            time.Time
	proc                  WorkerProcess
	cleanup               func() error
	busy                  bool
	sw                    *frame.SyncWriter
	nextStream            uint32 // only touched by the single in-flight request
}

type queueWaiter struct {
	key    string
	ready  chan *warmWorkerEntry
	ctx    context.Context
	cancel context.CancelFunc
}

// WarmPoolManager orchestrates warm workers.
type WarmPoolManager struct {
	cfg      WarmPoolConfig
	metrics  WarmPoolMetrics
	launcher WorkerLauncher

	mu      sync.Mutex
	workers map[string]*warmWorkerEntry // id -> entry
	queue   []*queueWaiter
	closed  bool
	stopRpr chan struct{}
}

// NewWarmPoolManager creates a new pool manager.
func NewWarmPoolManager(cfg WarmPoolConfig, launcher WorkerLauncher) (*WarmPoolManager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if launcher == nil {
		launcher = &defaultWorkerLauncher{}
	}
	mgr := &WarmPoolManager{
		cfg:      cfg,
		launcher: launcher,
		workers:  make(map[string]*warmWorkerEntry),
		stopRpr:  make(chan struct{}),
	}
	if cfg.Enabled {
		go mgr.reapLoop()
	}
	return mgr, nil
}

// Metrics returns the pool metrics.
func (m *WarmPoolManager) Metrics() *WarmPoolMetrics {
	return &m.metrics
}

// Config returns the pool configuration.
func (m *WarmPoolManager) Config() WarmPoolConfig {
	return m.cfg
}

// IsEligible checks if a request qualifies for execution on a warm worker.
func (m *WarmPoolManager) IsEligible(input adapter.ProcessRequest) bool {
	if !m.cfg.Enabled {
		return false
	}
	if input.Request.Kind != operations.QueryRead && input.Request.Kind != operations.MetadataInspect {
		return false
	}
	if input.SourceFile != nil || input.Runtime != nil || input.PrivateTransport != nil {
		return false
	}
	if adapter.NativeReader(input.Source.Engine) || input.Source.Engine == "google_sheets" || adapter.CloudSQL(input.Source.Engine) {
		return false
	}
	return true
}

func (m *WarmPoolManager) reapLoop() {
	interval := m.cfg.IdleTTL / 10
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	} else if interval > 5*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopRpr:
			return
		case <-ticker.C:
			m.reapExpired()
		}
	}
}

func (m *WarmPoolManager) reapExpired() {
	m.mu.Lock()
	now := time.Now()
	var toEvict []*warmWorkerEntry

	for id, w := range m.workers {
		if !w.busy {
			// Source credentials are valid for seconds, far shorter than the idle
			// TTL. An idle worker is not evicted when they lapse: it can do nothing
			// until a request with a fresh, re-authorized grant for the same
			// revision reaches it.
			if now.Sub(w.lastActive) >= m.cfg.IdleTTL {
				delete(m.workers, id)
				toEvict = append(toEvict, w)
				m.metrics.EvictionsTTL.Add(1)
			}
		}
	}
	m.mu.Unlock()

	for _, w := range toEvict {
		m.killWorker(w)
	}
}

func (m *WarmPoolManager) killWorker(w *warmWorkerEntry) {
	if w.proc != nil {
		// Kill first: a Close write blocks forever on a worker that stopped reading.
		_ = w.proc.Kill()
		_ = frame.WriteClose(w.proc.Stdin())
		_ = w.proc.Wait()
	}
	if w.cleanup != nil {
		_ = w.cleanup()
	}
}

func (m *WarmPoolManager) evictStaleRevisionsLocked(tenantID, appTeam, connectionID, currentRevision string) {
	var toEvict []*warmWorkerEntry
	for id, w := range m.workers {
		if !w.busy && w.tenantID == tenantID && w.appTeam == appTeam && w.connectionID == connectionID && w.revision != currentRevision {
			delete(m.workers, id)
			toEvict = append(toEvict, w)
			m.metrics.EvictionsRev.Add(1)
		}
	}
	go func() {
		for _, w := range toEvict {
			m.killWorker(w)
		}
	}()
}

// Acquire gets an idle worker or spawns a new one, queueing if capacity is reached.
func (m *WarmPoolManager) Acquire(ctx context.Context, e *Executor, cfg OperationProcessConfig, input adapter.ProcessRequest) (*warmWorkerEntry, func(error), error) {
	key := fmt.Sprintf("%s\x00%s\x00%s\x00%s", input.Source.TenantID, input.AppTeam, input.Source.ConnectionID, input.Source.Revision)
	now := time.Now()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, nil, errors.New("warm_pool: pool is closed")
	}

	// Evict any idle workers for this connection with older revisions
	m.evictStaleRevisionsLocked(input.Source.TenantID, input.AppTeam, input.Source.ConnectionID, input.Source.Revision)

	// Look for existing idle worker for key
	for _, w := range m.workers {
		if !w.busy && w.key == key {
			// This request was resolved and authorized on its own; the worker
			// now serves under its credential window, not the first request's.
			w.busy = true
			w.lastActive = now
			w.credentialsValidUntil = input.CredentialsValidUntil
			m.metrics.Hits.Add(1)
			m.mu.Unlock()

			release := func(err error) {
				m.releaseWorker(w, err)
			}
			return w, release, nil
		}
	}

	// Cache miss
	m.metrics.Misses.Add(1)

	// Check if we can spawn a new worker
	keyCount := 0
	for _, w := range m.workers {
		if w.key == key {
			keyCount++
		}
	}

	if len(m.workers) < m.cfg.MaxWorkers && keyCount < m.cfg.MaxPerKey {
		// Reserve worker ID
		workerID := fmt.Sprintf("worker-%d", time.Now().UnixNano())
		entry := &warmWorkerEntry{
			id:                    workerID,
			key:                   key,
			tenantID:              input.Source.TenantID,
			appTeam:               input.AppTeam,
			connectionID:          input.Source.ConnectionID,
			revision:              input.Source.Revision,
			credentialsValidUntil: input.CredentialsValidUntil,
			lastActive:            now,
			busy:                  true,
		}
		m.workers[workerID] = entry
		m.mu.Unlock()

		// Launch process outside mutex
		limits := e.Limits
		limits.MemoryMB = m.cfg.WorkerMemoryMB
		proc, cleanup, err := m.launcher.Launch(ctx, e, cfg, limits)
		if err != nil {
			m.mu.Lock()
			delete(m.workers, workerID)
			m.mu.Unlock()
			return nil, nil, err
		}

		entry.proc = proc
		entry.cleanup = cleanup
		entry.sw = frame.NewSyncWriter(proc.Stdin())
		if err := helloHandshake(ctx, proc); err != nil {
			// A worker that cannot speak v2 is a launch failure: fall back to one-shot.
			_ = proc.Kill()
			_ = proc.Wait()
			if cleanup != nil {
				_ = cleanup()
			}
			m.mu.Lock()
			delete(m.workers, workerID)
			m.mu.Unlock()
			return nil, nil, err
		}

		// Count only workers that actually started; refused launches are fallbacks.
		m.metrics.Spawns.Add(1)

		release := func(execErr error) {
			m.releaseWorker(entry, execErr)
		}
		return entry, release, nil
	}

	// Must enqueue
	if len(m.queue) >= m.cfg.QueueDepth {
		m.mu.Unlock()
		return nil, nil, query.NewError("RESOURCE_EXHAUSTED", "Warm worker pool queue depth exceeded")
	}

	waiterCtx, cancelWaiter := context.WithTimeout(ctx, m.cfg.QueueTimeout)
	waiter := &queueWaiter{
		key:    key,
		ready:  make(chan *warmWorkerEntry, 1),
		ctx:    waiterCtx,
		cancel: cancelWaiter,
	}
	m.queue = append(m.queue, waiter)
	m.mu.Unlock()

	startWait := time.Now()
	select {
	case <-waiterCtx.Done():
		cancelWaiter()
		m.mu.Lock()
		m.removeWaiterLocked(waiter)
		m.mu.Unlock()
		m.metrics.QueueWaitMS.Add(time.Since(startWait).Milliseconds())
		if errors.Is(waiterCtx.Err(), context.DeadlineExceeded) {
			return nil, nil, query.NewError("RESOURCE_EXHAUSTED", "Warm worker pool queue wait timed out")
		}
		return nil, nil, waiterCtx.Err()

	case w := <-waiter.ready:
		cancelWaiter()
		m.metrics.QueueWaitMS.Add(time.Since(startWait).Milliseconds())
		release := func(execErr error) {
			m.releaseWorker(w, execErr)
		}
		return w, release, nil
	}
}

func (m *WarmPoolManager) removeWaiterLocked(waiter *queueWaiter) {
	for i, q := range m.queue {
		if q == waiter {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			break
		}
	}
}

func (m *WarmPoolManager) releaseWorker(w *warmWorkerEntry, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err != nil || m.closed {
		// On ANY failure, evict and kill worker to prevent connection poisoning
		delete(m.workers, w.id)
		if m.closed {
			m.metrics.EvictionsDrain.Add(1)
		} else {
			m.metrics.EvictionsErr.Add(1)
		}
		go m.killWorker(w)
		return
	}

	w.lastActive = time.Now()
	w.busy = false

	// Check if there is a waiter for this key
	for i, waiter := range m.queue {
		if waiter.key == w.key && waiter.ctx.Err() == nil {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			w.busy = true
			waiter.ready <- w
			return
		}
	}
}

// Execute runs an operation on a warm worker, streaming results to sink.
func (m *WarmPoolManager) Execute(ctx context.Context, e *Executor, cfg OperationProcessConfig, input adapter.ProcessRequest, sink query.Sink) (operations.Receipt, error) {
	w, release, err := m.Acquire(ctx, e, cfg, input)
	if err != nil {
		if ctx.Err() != nil {
			return rejectedOperation(input, "CANCELLED"), ctx.Err()
		}
		// Launch failure, a full pool or a queue timeout: nothing reached a
		// worker, so the one-shot path can still run this operation safely.
		m.metrics.Fallbacks.Add(1)
		return operations.Receipt{}, errWarmUnavailable
	}

	receipt, execErr, keep := m.executeOnWorker(ctx, e, w, input, sink)
	if keep {
		// The stream ended with its own terminal frame: the worker is healthy.
		release(nil)
	} else {
		release(execErr)
	}
	return receipt, execErr
}

const helloTimeout = 5 * time.Second

// cancelGrace is a var so tests can shorten it.
var cancelGrace = 2 * time.Second

// helloHandshake sends Hello and requires a matching Hello back. A stuck
// worker is abandoned on ctx/timeout; the caller kills it, which unblocks the goroutine.
func helloHandshake(ctx context.Context, proc WorkerProcess) error {
	ctx, cancel := context.WithTimeout(ctx, helloTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		if err := frame.WriteHello(proc.Stdin()); err != nil {
			done <- err
			return
		}
		f, err := frame.Read(proc.Stdout())
		if err == nil {
			err = frame.CheckHello(f)
		}
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// executeOnWorker runs one request on its own stream. keep reports whether the
// worker is still healthy: true only when the stream ended with a terminal frame
// and nothing about the exchange was a protocol, limit, receipt or I/O failure.
func (m *WarmPoolManager) executeOnWorker(ctx context.Context, e *Executor, w *warmWorkerEntry, input adapter.ProcessRequest, sink query.Sink) (_ operations.Receipt, _ error, keep bool) {
	proc := w.proc
	raw, err := adapter.EncodeProcessRequest(input)
	if err != nil {
		return rejectedOperation(input, "INVALID_ARGUMENT"), operationFailure("INVALID_ARGUMENT"), true
	}

	w.nextStream++
	stream := w.nextStream
	if err := w.sw.Write(frame.Request, stream, raw); err != nil {
		return uncertainOperation(input), operationFailure("QUERY_FAILED"), false
	}

	// On caller cancellation tell the worker, then give the stream cancelGrace
	// to end; past that the worker is killed, which also unblocks the reader.
	stop := make(chan struct{})
	var graceFired atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-stop:
			return
		case <-ctx.Done():
		}
		t := time.AfterFunc(cancelGrace, func() { _ = proc.Kill() })
		defer func() {
			if !t.Stop() {
				graceFired.Store(true) // the kill ran or is running
			}
		}()
		_ = w.sw.Write(frame.Cancel, stream, nil)
		<-stop
	}()
	defer func() {
		close(stop)
		wg.Wait()
		if graceFired.Load() {
			keep = false
		}
	}()

	hasher := sha256.New()
	reader := &framedResultReader{
		r:        proc.Stdout(),
		stream:   stream,
		hasher:   hasher,
		maxBytes: input.Limits.MaxBytes,
	}

	limits := e.Limits
	limits.MaxRows = input.Limits.MaxRows
	limits.MaxBytes = input.Limits.MaxBytes

	observed, readErr := e.readIPC(ctx, reader, limits, sink)
	if ctx.Err() != nil && !reader.finished {
		// readIPC may bail on ctx before the terminal frame; drain it.
		_, _ = io.Copy(io.Discard, reader)
	}
	if ctx.Err() != nil {
		// Only a Receipt keeps the worker; a Completed one was not cross-checked here.
		return uncertainOperation(input), ctx.Err(), reader.receipt != nil && reader.receipt.Outcome != operations.Completed
	}
	if readErr != nil || reader.errFrame != nil || reader.receipt == nil {
		// Only a non-Completed Receipt keeps the worker. A terminal Error means
		// the adapter is exiting (fatal condition), so the worker is not reusable.
		if reader.receipt != nil && reader.receipt.Outcome != operations.Completed && reader.errFrame == nil {
			return *reader.receipt, nil, true
		}
		return uncertainOperation(input), operationFailure("QUERY_FAILED"), false
	}

	receipt := *reader.receipt
	wireDigest := hex.EncodeToString(hasher.Sum(nil))

	// Validate receipt against observed results
	if receipt.Outcome == operations.Completed {
		ref := receipt.Result
		if ref == nil || ref.ID != input.OperationID || ref.Format != "arrow_ipc" || ref.Rows != observed.Rows || ref.Bytes != observed.WireBytes || ref.SHA256 != wireDigest {
			return uncertainOperation(input), operationFailure("QUERY_FAILED"), false
		}
	}

	return receipt, nil, true
}

// Drain gracefully stops all workers in the pool.
func (m *WarmPoolManager) Drain(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	close(m.stopRpr)

	// Cancel all queue waiters
	for _, q := range m.queue {
		q.cancel()
	}
	m.queue = nil

	workers := make([]*warmWorkerEntry, 0, len(m.workers))
	for id, w := range m.workers {
		delete(m.workers, id)
		m.metrics.EvictionsDrain.Add(1)
		workers = append(workers, w)
	}
	m.mu.Unlock()

	for _, w := range workers {
		m.killWorker(w)
	}
	return nil
}

type framedResultReader struct {
	r          io.Reader
	stream     uint32
	remoteErr  bool // stream ended with a worker Error terminal frame
	curr       []byte
	hasher     hash.Hash
	receipt    *operations.Receipt
	errFrame   error
	finished   bool
	totalBytes int64
	maxBytes   int64
}

func (fr *framedResultReader) Read(p []byte) (int, error) {
	if len(fr.curr) > 0 {
		n := copy(p, fr.curr)
		fr.curr = fr.curr[n:]
		return n, nil
	}
	if fr.finished {
		return 0, io.EOF
	}

	for {
		f, err := frame.Read(fr.r)
		if err != nil {
			fr.finished = true
			fr.errFrame = err
			return 0, err
		}
		fType, payload := f.Type, f.Payload
		if fType == frame.Pong {
			continue
		}
		if f.Stream != fr.stream {
			fr.finished = true
			fr.errFrame = fmt.Errorf("frame %v for stream %d, expected %d", fType, f.Stream, fr.stream)
			return 0, io.EOF
		}

		switch fType {
		case frame.Result:
			if int64(len(payload)) > fr.maxBytes-fr.totalBytes {
				fr.finished = true
				fr.errFrame = adapter.ErrLimit
				return 0, adapter.ErrLimit
			}
			fr.totalBytes += int64(len(payload))
			if fr.hasher != nil {
				fr.hasher.Write(payload)
			}
			n := copy(p, payload)
			fr.curr = payload[n:]
			return n, nil

		case frame.Receipt:
			var receipt operations.Receipt
			if err := json.Unmarshal(payload, &receipt); err != nil {
				fr.errFrame = err
			} else {
				fr.receipt = &receipt
			}
			fr.finished = true
			return 0, io.EOF

		case frame.Error:
			fr.errFrame = errors.New(string(payload))
			fr.remoteErr = true
			fr.finished = true
			return 0, io.EOF

		default:
			fr.errFrame = fmt.Errorf("unexpected frame type: %v", fType)
			fr.finished = true
			return 0, io.EOF
		}
	}
}

type defaultWorkerLauncher struct{}
