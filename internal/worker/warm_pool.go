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
	"github.com/apache/arrow-go/v18/arrow"
)

// WarmPoolConfig configures the warm worker pool for small read-only operations.
type WarmPoolConfig struct {
	Enabled     bool          `yaml:"enabled"`
	IdleTTL     time.Duration `yaml:"idle_ttl"`      // default 30m
	MaxWorkers  int           `yaml:"max_workers"`   // default 4
	MaxPerKey   int           `yaml:"max_per_key"`   // default 1
	MaxInFlight int           `yaml:"max_in_flight"` // default 16, reads in flight per worker
	// StreamBufferMB caps the unread result bytes the node buffers per stream
	// (default 8). A slow client reading a large result then fails fast with
	// RESOURCE_EXHAUSTED instead of holding node memory for it.
	StreamBufferMB int           `yaml:"stream_buffer_mb"`
	QueueDepth     int           `yaml:"queue_depth"`      // default 64, reads waiting for a full worker
	QueueTimeout   time.Duration `yaml:"queue_timeout"`    // default 10s
	WorkerMemoryMB int           `yaml:"worker_memory_mb"` // default 64
}

// errWarmUnavailable means no warm worker could be obtained and nothing was
// sent to one. The caller runs the operation on the one-shot path instead.
var errWarmUnavailable = errors.New("warm_pool: no warm worker available")

func DefaultWarmPoolConfig() WarmPoolConfig {
	return WarmPoolConfig{
		Enabled:     false,
		IdleTTL:     30 * time.Minute,
		MaxWorkers:  4,
		MaxPerKey:   1,
		MaxInFlight: 16,
		// An explorer page opens ~50 reads at once: 16 in flight plus 64
		// waiting absorbs it without a refusal.
		QueueDepth:     64,
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
	// The adapter serves at most 64 streams; past that it answers busy, a
	// terminal Error that would kill the worker.
	if c.StreamBufferMB < 0 || c.StreamBufferMB > 256 {
		return errors.New("warm_pool: stream_buffer_mb must be between 0 and 256")
	}
	if c.MaxInFlight < 0 || c.MaxInFlight > 64 {
		return errors.New("warm_pool: max_in_flight must be between 1 and 64")
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

// maxInFlight treats an unset max_in_flight as the default, so configs written
// before it existed keep validating.
func (c WarmPoolConfig) maxInFlight() int {
	if c.MaxInFlight == 0 {
		return 16
	}
	return c.MaxInFlight
}

// streamBuffer is the per-stream buffer cap in bytes.
func (c WarmPoolConfig) streamBuffer() int64 {
	if c.StreamBufferMB == 0 {
		return 8 << 20
	}
	return int64(c.StreamBufferMB) << 20
}

// StreamBuffers is the node memory one worker may buffer for its streams:
// max_in_flight × stream_buffer_mb.
func (c WarmPoolConfig) StreamBuffers() int64 { return int64(c.maxInFlight()) * c.streamBuffer() }

// FitsOverhead reports whether a worker's stream buffers fit the per-job
// overhead its reservation carries for parent-side allocations.
func (c WarmPoolConfig) FitsOverhead(overheadBytes int64) error {
	if c.StreamBuffers() > overheadBytes {
		return errors.New("warm_pool: max_in_flight × stream_buffer_mb of stream buffers exceeds the per-job memory overhead")
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
	SlowConsumers  atomic.Int64 // streams failed for falling 8 MiB behind
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
	inFlight              int           // guarded by the manager's mu
	ready                 chan struct{} // closed once the launch finished
	launchErr             error         // set before ready is closed
	sw                    *frame.SyncWriter
	nextStream            atomic.Uint32
	killOnce              sync.Once

	// The demux reader owns stdout and routes frames to streams by ID.
	smu     sync.Mutex
	streams map[uint32]*streamQueue // nil once the demux reader has stopped
}

// WarmPoolManager orchestrates warm workers.
type WarmPoolManager struct {
	cfg      WarmPoolConfig
	metrics  WarmPoolMetrics
	launcher WorkerLauncher
	// Adapter is the pinned adapter binary for reads routed from the query
	// path; operations pass their own.
	Adapter OperationProcessConfig

	mu      sync.Mutex
	workers map[string]*warmWorkerEntry // id -> entry
	waiting int                         // requests waiting for a slot on a full worker
	changed chan struct{}               // closed and replaced whenever a slot frees
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
		changed:  make(chan struct{}),
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
		if w.inFlight == 0 {
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

// killWorker is idempotent: a worker may be killed by its demux reader, a
// failed request and drain at once.
func (m *WarmPoolManager) killWorker(w *warmWorkerEntry) {
	w.killOnce.Do(func() {
		if w.proc != nil {
			// Kill first: a Close write blocks forever on a worker that stopped reading.
			_ = w.proc.Kill()
			_ = frame.WriteClose(w.proc.Stdin())
			_ = w.proc.Wait()
		}
		if w.cleanup != nil {
			_ = w.cleanup()
		}
	})
}

// evictLocked removes w and kills it in the background. Only the first
// eviction of a worker is counted.
func (m *WarmPoolManager) evictLocked(w *warmWorkerEntry, counter *atomic.Int64) {
	if m.workers[w.id] == w {
		delete(m.workers, w.id)
		counter.Add(1)
	}
	m.broadcastLocked()
	go m.killWorker(w)
}

func (m *WarmPoolManager) broadcastLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}

func (m *WarmPoolManager) evictStaleRevisionsLocked(tenantID, appTeam, connectionID, currentRevision string) {
	for _, w := range m.workers {
		if w.inFlight == 0 && w.tenantID == tenantID && w.appTeam == appTeam && w.connectionID == connectionID && w.revision != currentRevision {
			m.evictLocked(w, &m.metrics.EvictionsRev)
		}
	}
}

// errWorkerBusy is a full worker that stayed full for queue_timeout. It is not
// a fallback: fifty one-shot children is the collapse the pool exists to avoid.
var errWorkerBusy = query.NewError("RESOURCE_EXHAUSTED", "Read worker is busy; retry after 1s")

// Acquire takes a stream slot on a warm worker for input's key, launching one
// when the key has none. A key whose workers are all full waits up to
// queue_timeout for a slot, then gets errWorkerBusy. errWarmUnavailable means
// no worker could be obtained and nothing was sent.
func (m *WarmPoolManager) Acquire(ctx context.Context, e *Executor, cfg OperationProcessConfig, input adapter.ProcessRequest) (*warmWorkerEntry, func(error), error) {
	key := fmt.Sprintf("%s\x00%s\x00%s\x00%s", input.Source.TenantID, input.AppTeam, input.Source.ConnectionID, input.Source.Revision)
	var deadline <-chan time.Time
	var startWait time.Time
	missed, waiting := false, false
	defer func() {
		if waiting {
			m.mu.Lock()
			m.waiting--
			m.mu.Unlock()
			m.metrics.QueueWaitMS.Add(time.Since(startWait).Milliseconds())
		}
	}()

	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, nil, errWarmUnavailable
		}
		m.evictStaleRevisionsLocked(input.Source.TenantID, input.AppTeam, input.Source.ConnectionID, input.Source.Revision)

		// The least loaded worker for this key with a free slot.
		var best *warmWorkerEntry
		keyCount := 0
		for _, w := range m.workers {
			if w.key != key {
				continue
			}
			keyCount++
			if w.inFlight < m.cfg.maxInFlight() && (best == nil || w.inFlight < best.inFlight) {
				best = w
			}
		}
		if best != nil {
			best.inFlight++
			best.lastActive = time.Now()
			// This request was resolved and authorized on its own; the worker
			// now serves under its credential window, not the first request's.
			best.credentialsValidUntil = input.CredentialsValidUntil
			m.metrics.Hits.Add(1)
			m.mu.Unlock()
			// A worker still launching is shared: wait for its handshake.
			select {
			case <-best.ready:
			case <-ctx.Done():
				m.releaseWorker(best, nil)
				return nil, nil, ctx.Err()
			}
			if best.launchErr != nil {
				m.releaseWorker(best, nil)
				return nil, nil, errWarmUnavailable
			}
			return best, func(err error) { m.releaseWorker(best, err) }, nil
		}
		if !missed {
			missed = true
			m.metrics.Misses.Add(1)
		}

		if keyCount < m.cfg.MaxPerKey && len(m.workers) < m.cfg.MaxWorkers {
			entry := &warmWorkerEntry{
				id:                    fmt.Sprintf("worker-%d", time.Now().UnixNano()),
				key:                   key,
				tenantID:              input.Source.TenantID,
				appTeam:               input.AppTeam,
				connectionID:          input.Source.ConnectionID,
				revision:              input.Source.Revision,
				credentialsValidUntil: input.CredentialsValidUntil,
				lastActive:            time.Now(),
				inFlight:              1,
				ready:                 make(chan struct{}),
			}
			m.workers[entry.id] = entry
			m.mu.Unlock()
			// The launch belongs to the pool, not to this caller: if this
			// caller leaves, others sharing the launching worker still get it.
			go func() { _ = m.launch(e, cfg, entry) }()
			select {
			case <-entry.ready:
			case <-ctx.Done():
				m.releaseWorker(entry, nil)
				return nil, nil, ctx.Err()
			}
			if entry.launchErr != nil {
				m.releaseWorker(entry, nil)
				return nil, nil, entry.launchErr
			}
			return entry, func(err error) { m.releaseWorker(entry, err) }, nil
		}

		if keyCount == 0 {
			// Every slot belongs to other connections: run one-shot.
			m.mu.Unlock()
			return nil, nil, errWarmUnavailable
		}

		// This key's workers are all full: wait a bounded time for a slot.
		if !waiting {
			if m.waiting >= m.cfg.QueueDepth {
				m.mu.Unlock()
				return nil, nil, query.NewError("RESOURCE_EXHAUSTED", "Warm worker pool queue depth exceeded; retry after 1s")
			}
			m.waiting++
			waiting = true
			startWait = time.Now()
			timer := time.NewTimer(m.cfg.QueueTimeout)
			defer timer.Stop()
			deadline = timer.C
		}
		changed := m.changed
		m.mu.Unlock()
		select {
		case <-changed:
		case <-deadline:
			return nil, nil, errWorkerBusy
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

// launch starts entry's process, handshakes and starts its demux reader. On
// failure the entry is removed and every request sharing it falls back.
func (m *WarmPoolManager) launch(e *Executor, cfg OperationProcessConfig, entry *warmWorkerEntry) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), launchTimeout)
	defer cancel()
	defer func() {
		if err != nil {
			m.mu.Lock()
			entry.launchErr = err
			if m.workers[entry.id] == entry {
				delete(m.workers, entry.id)
			}
			m.broadcastLocked()
			m.mu.Unlock()
		}
		close(entry.ready)
	}()
	limits := e.Limits
	limits.MemoryMB = m.cfg.WorkerMemoryMB
	proc, cleanup, err := m.launcher.Launch(ctx, e, cfg, limits)
	if err != nil {
		return err
	}
	entry.proc = proc
	entry.cleanup = cleanup
	entry.sw = frame.NewSyncWriter(proc.Stdin())
	if err := helloHandshake(ctx, proc); err != nil {
		// A worker that cannot speak v2 is a launch failure: fall back to one-shot.
		m.killWorker(entry)
		return err
	}
	entry.streams = make(map[uint32]*streamQueue)
	go m.demux(entry)
	// Count only workers that actually started; refused launches are fallbacks.
	m.metrics.Spawns.Add(1)
	return nil
}

// demux is the only reader of a worker's stdout. It routes each frame to its
// stream's queue and never blocks on a slow consumer. A frame for an unknown
// stream or a read failure ends the worker and
// fails every stream still in flight on it.
func (m *WarmPoolManager) demux(w *warmWorkerEntry) {
	var cause error
	for cause == nil {
		f, err := frame.Read(w.proc.Stdout())
		if err != nil {
			cause = err
			break
		}
		if f.Type == frame.Pong {
			continue
		}
		w.smu.Lock()
		q := w.streams[f.Stream]
		if q != nil && (f.Type == frame.Receipt || f.Type == frame.Error) {
			// A later frame on this stream is unknown, which kills the worker.
			delete(w.streams, f.Stream)
		}
		w.smu.Unlock()
		if q == nil {
			cause = fmt.Errorf("frame %v for unknown stream %d", f.Type, f.Stream)
			break
		}
		q.push(f)
	}
	w.smu.Lock()
	streams := w.streams
	w.streams = nil
	w.smu.Unlock()
	for _, q := range streams {
		q.close(cause)
	}
	m.mu.Lock()
	m.evictLocked(w, &m.metrics.EvictionsErr)
	m.mu.Unlock()
}

// open registers a stream before its Request is written. It fails only when
// the demux reader has stopped, before anything was sent. onSlow runs once,
// off the demux goroutine, if the stream's consumer falls behind.
func (w *warmWorkerEntry) open(capBytes int64, onSlow func(), slowCount *atomic.Int64) (uint32, *streamQueue, bool) {
	stream := w.nextStream.Add(1)
	q := &streamQueue{notify: make(chan struct{}, 1), done: make(chan struct{}), capBytes: capBytes, onSlow: onSlow, slowCount: slowCount}
	w.smu.Lock()
	defer w.smu.Unlock()
	if w.streams == nil {
		return 0, nil, false
	}
	w.streams[stream] = q
	return stream, q, true
}

// A stream's queue holds at most stream_buffer_mb of unread Result bytes. A
// consumer that falls further behind fails with errSlowConsumer instead of
// growing node memory or blocking the demux reader.

var errSlowConsumer = query.NewError("RESOURCE_EXHAUSTED", "Read result consumer is too slow (slow consumer)")

// streamQueue buffers one stream's frames between the demux reader and the
// request reading them. push never blocks. An abandoned stream drops its
// Result frames but still records its terminal frame, so the worker is kept
// only if the stream ends properly.
type streamQueue struct {
	mu        sync.Mutex
	frames    []frame.Frame
	queued    int64        // unread Result payload bytes
	capBytes  int64        // stream_buffer_mb
	terminal  *frame.Frame // the Receipt or Error that ended the stream
	err       error        // the worker stopped before a terminal frame
	slow      bool
	abandoned bool
	done      chan struct{} // closed on the terminal frame or worker stop
	notify    chan struct{}
	onSlow    func()
	slowCount *atomic.Int64
}

func (q *streamQueue) push(f frame.Frame) {
	q.mu.Lock()
	switch {
	case f.Type == frame.Receipt || f.Type == frame.Error:
		if q.terminal == nil && q.err == nil {
			q.terminal = &f
			if !q.abandoned {
				q.frames = append(q.frames, f)
			}
			close(q.done)
		}
	case q.abandoned:
	case q.queued+int64(len(f.Payload)) > q.capBytes:
		q.slow, q.abandoned, q.frames, q.queued = true, true, nil, 0
		if q.slowCount != nil {
			q.slowCount.Add(1)
		}
		if q.onSlow != nil {
			go q.onSlow()
		}
	default:
		q.frames = append(q.frames, f)
		q.queued += int64(len(f.Payload))
	}
	q.mu.Unlock()
	q.wake()
}

func (q *streamQueue) wake() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// close records that the worker stopped; frames already queued stay readable.
func (q *streamQueue) close(err error) {
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	q.mu.Lock()
	if q.terminal == nil && q.err == nil {
		q.err = err
		close(q.done)
	}
	q.mu.Unlock()
	q.wake()
}

// abandon drops whatever is queued and every later Result frame.
func (q *streamQueue) abandon() {
	q.mu.Lock()
	q.abandoned, q.frames, q.queued = true, nil, 0
	q.mu.Unlock()
	q.wake()
}

func (q *streamQueue) next() (frame.Frame, error) {
	for {
		q.mu.Lock()
		if len(q.frames) > 0 {
			f := q.frames[0]
			q.frames[0] = frame.Frame{}
			q.frames = q.frames[1:]
			if f.Type == frame.Result {
				q.queued -= int64(len(f.Payload))
			}
			q.mu.Unlock()
			return f, nil
		}
		slow, abandoned, err := q.slow, q.abandoned, q.err
		q.mu.Unlock()
		switch {
		case slow:
			return frame.Frame{}, errSlowConsumer
		case err != nil:
			return frame.Frame{}, err
		case abandoned:
			return frame.Frame{}, io.ErrUnexpectedEOF
		}
		<-q.notify
	}
}

// settled reports how a stream ended: its terminal frame, or nil if the
// worker stopped first.
func (q *streamQueue) settled() *frame.Frame {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.terminal
}

func (m *WarmPoolManager) releaseWorker(w *warmWorkerEntry, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w.inFlight--
	w.lastActive = time.Now()
	if m.closed {
		m.evictLocked(w, &m.metrics.EvictionsDrain)
		return
	}
	if err != nil {
		// On ANY failure, evict and kill worker to prevent connection poisoning.
		// Other streams in flight on it fail with it.
		m.evictLocked(w, &m.metrics.EvictionsErr)
		return
	}
	m.broadcastLocked()
}

// Execute runs an operation on a warm worker, streaming results to sink.
func (m *WarmPoolManager) Execute(ctx context.Context, e *Executor, cfg OperationProcessConfig, input adapter.ProcessRequest, sink query.Sink) (operations.Receipt, error) {
	w, release, err := m.Acquire(ctx, e, cfg, input)
	if err != nil {
		if ctx.Err() != nil {
			return rejectedOperation(input, "CANCELLED"), ctx.Err()
		}
		var public *query.Error
		if errors.As(err, &public) && public.Code == "RESOURCE_EXHAUSTED" {
			return rejectedOperation(input, "RESOURCE_EXHAUSTED"), err
		}
		// Launch failure or no slot for this connection: nothing reached a
		// worker, so the one-shot path can still run this operation safely.
		m.metrics.Fallbacks.Add(1)
		return operations.Receipt{}, errWarmUnavailable
	}

	receipt, execErr, keep, sent := m.executeOnWorker(ctx, e, w, input, sink)
	switch {
	case !sent:
		// The worker died between Acquire and our write: nothing was sent.
		release(errWarmUnavailable)
		m.metrics.Fallbacks.Add(1)
		return operations.Receipt{}, errWarmUnavailable
	case keep:
		// The stream ended with its own terminal frame: the worker is healthy.
		release(nil)
	default:
		release(execErr)
	}
	return receipt, execErr
}

// helloTimeout bounds the handshake; a var so tests can shorten it.
var helloTimeout = 5 * time.Second

// launchTimeout bounds one worker launch, reservation wait and Hello
// included. It is a var so tests can shorten it.
var launchTimeout = 30 * time.Second

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
// worker is still healthy; sent is false only when nothing was written
// because the worker had died.
//
// A local failure (the caller cancelled, the sink failed, a node-side row or
// byte limit, a slow consumer) only abandons this stream: the worker is told
// to cancel it, its frames are dropped, and the worker is kept if the stream
// still ends with a Receipt within cancelGrace. The worker is killed for a
// protocol violation, a terminal Error, a receipt mismatch, an I/O error, or an
// abandoned stream that does not end in time.
func (m *WarmPoolManager) executeOnWorker(ctx context.Context, e *Executor, w *warmWorkerEntry, input adapter.ProcessRequest, sink query.Sink) (_ operations.Receipt, _ error, keep, sent bool) {
	raw, err := adapter.EncodeProcessRequest(input)
	if err != nil {
		return rejectedOperation(input, "INVALID_ARGUMENT"), operationFailure("INVALID_ARGUMENT"), true, true
	}
	var streamID atomic.Uint32
	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			// A worker that stopped reading cannot block this request: the
			// grace timer below kills it, which ends the write.
			go func() { _ = w.sw.Write(frame.Cancel, streamID.Load(), nil) }()
		})
	}
	stream, queue, ok := w.open(m.cfg.streamBuffer(), cancel, &m.metrics.SlowConsumers)
	streamID.Store(stream)
	if !ok {
		return operations.Receipt{}, errWarmUnavailable, false, false
	}
	if err := w.sw.Write(frame.Request, stream, raw); err != nil {
		return uncertainOperation(input), operationFailure("QUERY_FAILED"), false, true
	}
	stopWatch := context.AfterFunc(ctx, func() {
		cancel()
		queue.abandon() // unblocks the reader
	})
	defer stopWatch()

	hasher := sha256.New()
	reader := &framedResultReader{next: queue.next, hasher: hasher, maxBytes: input.Limits.MaxBytes}
	watched := &watchedSink{Sink: sink}
	limits := e.Limits
	limits.MaxRows = input.Limits.MaxRows
	limits.MaxBytes = input.Limits.MaxBytes
	observed, readErr := e.readIPC(ctx, reader, limits, watched)

	if reader.finished && reader.terminal {
		// The stream ended on its own terminal frame.
		if reader.remoteErr {
			return uncertainOperation(input), operationFailure("QUERY_FAILED"), false, true
		}
		if reader.receipt == nil {
			return uncertainOperation(input), operationFailure("QUERY_FAILED"), false, true // unparsable receipt
		}
		receipt := *reader.receipt
		if receipt.Outcome != operations.Completed {
			return receipt, nil, true, true
		}
		if readErr == nil {
			ref, wireDigest := receipt.Result, hex.EncodeToString(hasher.Sum(nil))
			if ref == nil || ref.ID != input.OperationID || ref.Format != "arrow_ipc" || ref.Rows != observed.Rows || ref.Bytes != observed.WireBytes || ref.SHA256 != wireDigest {
				return uncertainOperation(input), operationFailure("QUERY_FAILED"), false, true
			}
			return receipt, nil, true, true
		}
		// The whole stream arrived but this caller could not take it.
		if local := localReadFailure(ctx, watched, reader, readErr); local != nil {
			return uncertainOperation(input), local, true, true
		}
		return uncertainOperation(input), operationFailure("QUERY_FAILED"), false, true
	}

	local := localReadFailure(ctx, watched, reader, readErr)
	if local == nil {
		// The worker stopped, broke the protocol or sent invalid Arrow data.
		return uncertainOperation(input), operationFailure("QUERY_FAILED"), false, true
	}
	// Abandon only this stream and wait for its terminal frame.
	cancel()
	queue.abandon()
	timer := time.NewTimer(cancelGrace)
	defer timer.Stop()
	select {
	case <-queue.done:
	case <-timer.C:
		_ = w.proc.Kill() // the demux reader then fails every stream on it
		return uncertainOperation(input), local, false, true
	}
	terminal := queue.settled()
	// After a cancel any Receipt, even a Completed one, is a valid end.
	return uncertainOperation(input), local, terminal != nil && terminal.Type == frame.Receipt, true
}

// localReadFailure returns the caller-side cause of a failed read, or nil when
// the failure came from the worker.
func localReadFailure(ctx context.Context, sink *watchedSink, reader *framedResultReader, readErr error) error {
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.Is(reader.errFrame, errSlowConsumer) || errors.Is(readErr, errSlowConsumer):
		return errSlowConsumer
	case sink.failed:
		return readErr
	case errors.Is(reader.errFrame, adapter.ErrLimit):
		return query.NewError("RESOURCE_EXHAUSTED", "Worker result exceeds byte limit")
	}
	var public *query.Error
	if errors.As(readErr, &public) && public.Code == "RESOURCE_EXHAUSTED" {
		return readErr // a node-side row or byte limit
	}
	return nil
}

// watchedSink records whether the caller's sink refused a write.
type watchedSink struct {
	query.Sink
	failed bool
}

func (s *watchedSink) Schema(schema *arrow.Schema) error {
	err := s.Sink.Schema(schema)
	s.failed = s.failed || err != nil
	return err
}

func (s *watchedSink) Write(batch arrow.RecordBatch) error {
	err := s.Sink.Write(batch)
	s.failed = s.failed || err != nil
	return err
}

// Drain gracefully stops all workers in the pool.
func (m *WarmPoolManager) Drain(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	close(m.stopRpr)
	m.broadcastLocked()

	workers := make([]*warmWorkerEntry, 0, len(m.workers))
	for id, w := range m.workers {
		delete(m.workers, id)
		m.metrics.EvictionsDrain.Add(1)
		workers = append(workers, w)
	}
	m.mu.Unlock()

	for _, w := range workers {
		<-w.ready
		m.killWorker(w)
	}
	return nil
}

// framedResultReader exposes one stream's Result payloads as a byte stream and
// records its terminal frame.
type framedResultReader struct {
	next       func() (frame.Frame, error)
	terminal   bool // the stream's own Receipt or Error was read
	remoteErr  bool // that terminal frame was an Error
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

	f, err := fr.next()
	if err != nil {
		fr.finished = true
		fr.errFrame = err
		return 0, err
	}
	switch f.Type {
	case frame.Result:
		if int64(len(f.Payload)) > fr.maxBytes-fr.totalBytes {
			fr.finished = true
			fr.errFrame = adapter.ErrLimit
			return 0, adapter.ErrLimit
		}
		fr.totalBytes += int64(len(f.Payload))
		if fr.hasher != nil {
			fr.hasher.Write(f.Payload)
		}
		n := copy(p, f.Payload)
		fr.curr = f.Payload[n:]
		return n, nil

	case frame.Receipt:
		fr.terminal = true
		var receipt operations.Receipt
		if err := json.Unmarshal(f.Payload, &receipt); err != nil {
			fr.errFrame = err
		} else {
			fr.receipt = &receipt
		}
		fr.finished = true
		return 0, io.EOF

	case frame.Error:
		fr.terminal, fr.remoteErr = true, true
		fr.errFrame = errors.New(string(f.Payload))
		fr.finished = true
		return 0, io.EOF

	default:
		fr.errFrame = fmt.Errorf("unexpected frame type: %v", f.Type)
		fr.finished = true
		return 0, io.EOF
	}
}

type defaultWorkerLauncher struct{}
