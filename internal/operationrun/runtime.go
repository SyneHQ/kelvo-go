// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package operationrun executes retained operations independently of HTTP reads.
package operationrun

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	api "github.com/SYNEHQ/kelvo-go/operations"
)

var ErrInvalid = errors.New("invalid operation runtime configuration")
var ErrClosed = errors.New("operation runtime closed")
var ErrCompletion = errors.New("operation receipt persistence could not be verified")
var ErrCleanup = errors.New("operation resource cleanup did not complete")
var ErrDelivery = errors.New("confirmed operation result or audit completion failed")

type Config struct {
	WorkerID     string
	Owner        string
	Concurrency  int
	PollInterval time.Duration
	// ScanShards bounds storage reads per pass. Zero uses sixteen shards.
	ScanShards int
}

// Hooks must honor context cancellation, be safe across concurrent operations,
// and not log request data or credentials. Authorize verifies the current grant,
// independent worker lease, and operation binding; Record is not proof of them.
// Prepare builds a closure and checks capabilities. Actual credential resolution
// and source Open/dispatch belong inside Execute, after durable Start succeeds.
type Hooks struct {
	Load           func(context.Context, ledger.Record) (api.Request, error)
	Authorize      func(context.Context, ledger.Record, api.Request, ledger.Binding) error
	Prepare        func(context.Context, ledger.Record, api.Request, ledger.Binding) (Prepared, error)
	AuditAdmission func(context.Context, ledger.Record, api.Request) error
}

// Prepared executes once. A valid confirmed receipt is retained even if Execute
// also reports a result-delivery error. An absent/invalid mutation receipt means
// outcome_unknown, never an assumed rollback. Close must release resources.
type Prepared interface {
	Execute(context.Context) (api.Receipt, error)
	Close() error
}

type Runtime struct {
	store *ledger.Store
	cfg   Config
	hooks Hooks
	mu    sync.Mutex
	// Once started, the dispatcher alone adds work. Drain stops that producer
	// before waiting for workers, so Wait never races with a fresh Add.
	started, draining bool
	failure           error
	cancel            context.CancelFunc
	stop              chan struct{}
	dispatchDone      chan struct{}
	done              chan struct{}
	wake              chan struct{}
	active            map[string]struct{}
	wg                sync.WaitGroup
	cursor            int
}

func New(store *ledger.Store, cfg Config, hooks Hooks) (*Runtime, error) {
	if store == nil || cfg.Concurrency < 1 || cfg.Concurrency > 256 || cfg.PollInterval < 10*time.Millisecond || cfg.PollInterval > time.Minute ||
		(ledger.Binding{WorkerID: cfg.WorkerID, Owner: cfg.Owner, Claim: "00000000000000000000000000000000"}).Validate() != nil ||
		hooks.Load == nil || hooks.Authorize == nil || hooks.Prepare == nil || hooks.AuditAdmission == nil || cfg.ScanShards < 0 || cfg.ScanShards > 256 {
		return nil, ErrInvalid
	}
	if cfg.ScanShards == 0 {
		cfg.ScanShards = 16
	}
	return &Runtime{store: store, cfg: cfg, hooks: hooks, stop: make(chan struct{}), dispatchDone: make(chan struct{}), done: make(chan struct{}), wake: make(chan struct{}, 1), active: map[string]struct{}{}}, nil
}

// Start launches one bounded recovery scanner. GET/status/result handlers never
// execute operations; a lost wakeup is recovered from the durable queued shard.
func (r *Runtime) Start(parent context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.draining {
		return ErrClosed
	}
	if r.started {
		return ErrInvalid
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	ctx, cancel := context.WithCancel(parent)
	r.cancel, r.started = cancel, true
	go r.dispatch(ctx)
	go func() {
		<-r.dispatchDone
		r.wg.Wait()
		close(r.done)
	}()
	return nil
}

// Wake is an optional best-effort latency hint. It carries no authority or SQL.
func (r *Runtime) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Runtime) BeginDrain() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.draining {
		return
	}
	r.draining = true
	close(r.stop)
	if !r.started {
		close(r.dispatchDone)
		close(r.done)
	}
}

// Drain stops admission and waits for accepted work without cancelling it.
func (r *Runtime) Drain(ctx context.Context) error {
	r.BeginDrain()
	select {
	case <-r.done:
		return r.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Err reports lifecycle failures without exposing storage, query or driver data.
// A failure stops new dispatch; it does not change an operation's source effect.
func (r *Runtime) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failure
}

// Joined reports that no dispatcher or accepted operation can still use the
// runtime's borrowed clients. A lifecycle error can remain after this is true.
func (r *Runtime) Joined() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

func (r *Runtime) fail(cause error) {
	r.mu.Lock()
	if !errors.Is(r.failure, cause) {
		r.failure = errors.Join(r.failure, cause)
	}
	r.mu.Unlock()
	r.BeginDrain()
}

// Close cancels in-flight work and waits within the caller's deadline. Drivers
// that ignore cancellation retain their slot; they are never silently retried.
func (r *Runtime) Close(ctx context.Context) error {
	r.BeginDrain()
	r.mu.Lock()
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Unlock()
	return r.Drain(ctx)
}

func (r *Runtime) dispatch(ctx context.Context) {
	defer close(r.dispatchDone)
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		default:
		}
		r.scan(ctx)
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case <-ticker.C:
		case <-r.wake:
		}
	}
}

func (r *Runtime) scan(ctx context.Context) {
	count := min(r.cfg.ScanShards, r.store.Policy().Shards)
	for range count {
		r.mu.Lock()
		full := r.draining || len(r.active) >= r.cfg.Concurrency
		r.mu.Unlock()
		if full || ctx.Err() != nil {
			return
		}
		shard := r.cursor
		r.cursor = (r.cursor + 1) % r.store.Policy().Shards
		records, err := r.store.QueuedShard(ctx, shard)
		if err != nil {
			continue
		}
		for _, record := range records {
			r.mu.Lock()
			_, exists := r.active[record.ID]
			if r.draining || len(r.active) >= r.cfg.Concurrency {
				r.mu.Unlock()
				return
			}
			if exists {
				r.mu.Unlock()
				continue
			}
			r.active[record.ID] = struct{}{}
			r.wg.Add(1)
			r.mu.Unlock()
			go func() {
				defer r.wg.Done()
				defer func() {
					r.mu.Lock()
					delete(r.active, record.ID)
					r.mu.Unlock()
				}()
				r.run(ctx, record)
			}()
		}
	}
}

func (r *Runtime) hookContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, min(r.store.Policy().LeaseDuration/3, r.store.Policy().StorageTimeout))
}

func (r *Runtime) run(parent context.Context, queued ledger.Record) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return
	}
	binding := ledger.Binding{WorkerID: r.cfg.WorkerID, Owner: r.cfg.Owner, Claim: hex.EncodeToString(nonce[:])}
	claimed, err := r.store.Claim(parent, queued.Scope, queued.ID, binding)
	if err != nil {
		return // Unknown claim acknowledgements never authorize preparation/start.
	}
	record := claimed.Record
	ctx, cancel := context.WithDeadline(parent, record.ExecuteBefore)
	defer cancel()
	started := false
	var prepared Prepared
	// Hook panics do not crash other tenants or leave a successful dispatch
	// eligible for retry. Diagnostics are deliberately not copied to receipts.
	defer func() {
		if recover() != nil {
			if started {
				r.complete(record, binding, r.uncertain(record, "SOURCE_FAILED"))
			} else {
				r.reject(record, binding, "UNAVAILABLE")
			}
		}
		// Close follows the durable completion attempt, including on source
		// panic. A cleanup panic must not replace a confirmed source receipt.
		if prepared != nil && closePrepared(prepared) != nil {
			r.fail(ErrCleanup)
		}
	}()
	hookCtx, hookCancel := r.hookContext(ctx)
	request, err := r.hooks.Load(hookCtx, record)
	if err == nil {
		err = hookCtx.Err()
	}
	hookCancel()
	if err != nil {
		r.reject(record, binding, "UNAVAILABLE")
		return
	}
	ref, _, err := api.SealRequest(request, record.RequestRef.ID)
	if err != nil || ref != record.RequestRef || request.Kind != record.Kind || request.Connection.ID != record.Scope.ConnectionID {
		r.reject(record, binding, "INVALID_ARGUMENT")
		return
	}
	digest, err := api.Digest(request)
	if err != nil || digest != record.RequestSHA256 {
		r.reject(record, binding, "INVALID_ARGUMENT")
		return
	}
	request, _ = api.Clone(request)
	if r.authorize(ctx, record, request, binding) != nil {
		r.reject(record, binding, "PERMISSION_DENIED")
		return
	}
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go r.heartbeat(heartbeatCtx, cancel, heartbeatDone, record, request, binding)
	defer func() { stopHeartbeat(); <-heartbeatDone }()
	hookCtx, hookCancel = r.hookContext(ctx)
	prepared, err = r.hooks.Prepare(hookCtx, record, request, binding)
	if err == nil {
		err = hookCtx.Err()
	}
	hookCancel()
	if err != nil || prepared == nil {
		code := "UNAVAILABLE"
		if errors.Is(err, api.ErrUnsupported) {
			code = "UNSUPPORTED"
		}
		r.reject(record, binding, code)
		return
	}
	if request.Kind.Mutating() {
		hookCtx, hookCancel = r.hookContext(ctx)
		err = r.hooks.AuditAdmission(hookCtx, record, request)
		if err == nil {
			err = hookCtx.Err()
		}
		hookCancel()
		if err != nil {
			r.reject(record, binding, "UNAVAILABLE")
			return
		}
	}
	if r.authorize(ctx, record, request, binding) != nil {
		r.reject(record, binding, "PERMISSION_DENIED")
		return
	}
	if _, err := r.store.Start(ctx, record.Scope, record.ID, binding); err != nil {
		return // Durable Start acknowledgement is the only source dispatch gate.
	}
	started = true
	if ctx.Err() != nil {
		r.complete(record, binding, r.uncertain(record, "CANCELLED"))
		return
	}
	receipt, executeErr := prepared.Execute(ctx)
	// A source-confirmed receipt survives a later transport/result error. Missing
	// confirmation remains unknown for mutations, including context deadlines.
	if receipt.Version == 0 {
		receipt.Version = api.Version
	}
	if receipt.OperationID == "" {
		receipt.OperationID = record.ID
	}
	if receipt.RequestSHA256 == "" {
		receipt.RequestSHA256 = record.RequestSHA256
	}
	if receipt.Validate() != nil || receipt.OperationID != record.ID || receipt.RequestSHA256 != record.RequestSHA256 || receipt.Outcome == api.CancelledBeforeStart ||
		(!record.Kind.Mutating() && receipt.Effect != api.EffectNone) || (record.Kind.Mutating() && receipt.Outcome == api.Completed && receipt.Effect != api.EffectCommitted) {
		receipt = r.uncertain(record, "SOURCE_FAILED")
	}
	r.complete(record, binding, receipt)
	if receipt.Outcome == api.Completed && executeErr != nil {
		r.fail(ErrDelivery)
	}
}

func closePrepared(prepared Prepared) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrCleanup
		}
	}()
	return prepared.Close()
}

func (r *Runtime) authorize(ctx context.Context, record ledger.Record, request api.Request, binding ledger.Binding) error {
	bounded, cancel := r.hookContext(ctx)
	defer cancel()
	if bounded.Err() != nil {
		return bounded.Err()
	}
	err := r.hooks.Authorize(bounded, record, request, binding)
	if err == nil {
		err = bounded.Err()
	}
	return err
}

func (r *Runtime) heartbeat(ctx context.Context, cancel context.CancelFunc, done chan struct{}, record ledger.Record, request api.Request, binding ledger.Binding) {
	defer close(done)
	defer func() {
		if recover() != nil {
			cancel()
		}
	}()
	ticker := time.NewTicker(r.store.Policy().LeaseDuration / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.authorize(ctx, record, request, binding) != nil {
				cancel()
				return
			}
			snapshot, err := r.store.Renew(ctx, record.Scope, record.ID, binding)
			if err != nil {
				cancel()
				return
			}
			record = snapshot.Record
		}
	}
}

func (r *Runtime) reject(record ledger.Record, binding ledger.Binding, code string) {
	ctx, cancel := context.WithTimeout(context.Background(), r.store.Policy().StorageTimeout)
	defer cancel()
	_, _ = r.store.RejectBeforeStart(ctx, record.Scope, record.ID, binding, code)
}

func (r *Runtime) complete(record ledger.Record, binding ledger.Binding, receipt api.Receipt) {
	ctx, cancel := context.WithTimeout(context.Background(), r.store.Policy().StorageTimeout)
	_, completionErr := r.store.Complete(ctx, record.Scope, record.ID, binding, receipt)
	cancel()
	if completionErr == nil {
		return
	}
	// A lost acknowledgement or concurrent cancellation can already have
	// settled this attempt. Read back once; never redispatch or overwrite it.
	readback, stopReadback := context.WithTimeout(context.Background(), r.store.Policy().StorageTimeout)
	defer stopReadback()
	current, err := r.store.Get(readback, record.Scope, record.ID)
	if err == nil && current.Record.Binding == binding && current.Record.RequestSHA256 == record.RequestSHA256 && current.Record.Terminal() {
		return
	}
	r.fail(ErrCompletion)
}

func (r *Runtime) uncertain(record ledger.Record, code string) api.Receipt {
	receipt := api.Receipt{Version: api.Version, OperationID: record.ID, RequestSHA256: record.RequestSHA256, Outcome: api.Failed, Effect: api.EffectNone, ErrorCode: code}
	if record.Kind.Mutating() {
		receipt.Outcome, receipt.Effect, receipt.ErrorCode = api.OutcomeUnknown, api.EffectUnknown, "OUTCOME_UNKNOWN"
	}
	return receipt
}
