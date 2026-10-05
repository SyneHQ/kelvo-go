// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
)

type exportReservation struct {
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
	started bool // protected by ExportRuntime.mu
}

// ExportRuntime owns one worker's retained filesystem and independent bounded
// dispatch loop. It does not publish Ready, replay SQL, or adopt abandoned jobs.
type ExportRuntime struct {
	cfg                NodeConfig
	store              ExportStore
	executor           query.Executor
	owner              string
	storage            *exports.Store
	custody            *exports.Custody
	pool               *admission.Pool
	audit              *ServiceAudit
	ownAudit           bool
	ctx                context.Context
	cancel             context.CancelFunc
	dispatchCtx        context.Context
	dispatchCancel     context.CancelFunc
	dispatchDone       chan struct{}
	permits, downloads chan struct{}
	mu                 sync.Mutex
	jobs               map[string]*exportReservation
	draining           bool
	failure            error
	wg, handlers       sync.WaitGroup
	once               sync.Once
	closeErr           error
}

func NewExportRuntime(cfg NodeConfig, store ExportStore, executor *worker.Executor, owner string) (*ExportRuntime, error) {
	if cfg.Exports == nil || cfg.Policy.Exports == nil {
		return nil, ErrExportDisabled
	}
	policy, err := clonePolicy(cfg.Policy)
	if err != nil {
		return nil, err
	}
	cfg.Policy = policy
	if executor == nil || cfg.SandboxPath == "" || executor.SandboxPath != cfg.SandboxPath || executor.ResourcePool == nil || cfg.Resources == nil || store == nil || !reflect.DeepEqual(cfg.Policy, store.Policy()) || !validOwner(owner) || cfg.Policy.Workers[cfg.WorkerID] < 1 {
		return nil, exports.ErrInvalid
	}
	executor, err = bindCatalogExecutor(cfg.Policy, executor)
	if err != nil {
		return nil, err
	}
	if err := executor.ValidateObjectRuntime(); err != nil {
		return nil, err
	}
	if err := validateNodeExports(cfg); err != nil {
		return nil, err
	}
	if err := ValidateExportCatalog(cfg, executor.Config); err != nil {
		return nil, err
	}
	if !executor.ScratchRoot.MatchesDirectory(cfg.ScratchDirectory) || (len(cfg.Policy.SourceQuotas) != 0 && executor.SourceAdmission == nil) {
		return nil, exports.ErrInvalid
	}
	if (cfg.Containment == nil) != (executor.Containment == nil) {
		return nil, exports.ErrInvalid
	}
	if cfg.Containment != nil && (cfg.Containment.Validate(cfg.Resources, cfg.Policy.Limits) != nil ||
		executor.ContainmentBudget != cfg.Containment.Budget || !executor.Containment.MatchesConfig(cfg.Containment.Config)) {
		return nil, exports.ErrInvalid
	}
	expectedPool, err := cfg.Resources.NewPool()
	if err != nil || !reflect.DeepEqual(expectedPool.Snapshot().Limits, executor.ResourcePool.Snapshot().Limits) || executor.ResourcePool.Snapshot().Draining {
		return nil, exports.ErrInvalid
	}
	copyExecutor := *executor
	// This operation's export-class custody includes publication and cleanup.
	// The child executor must not acquire an interactive reservation as well.
	copyExecutor.ResourcePool = nil
	copyExecutor.Limits = cfg.Policy.Limits
	copyExecutor.ResourceOverheadBytes = cfg.Resources.OverheadMB << 20
	return newExportRuntime(cfg, store, &copyExecutor, executor.ResourcePool, owner)
}

func newExportRuntime(cfg NodeConfig, store ExportStore, executor query.Executor, pool *admission.Pool, owner string) (*ExportRuntime, error) {
	policy, err := clonePolicy(cfg.Policy)
	if err != nil {
		return nil, err
	}
	cfg.Policy = policy
	if cfg.Exports == nil || cfg.Policy.Exports == nil || store == nil || executor == nil || pool == nil || cfg.Resources == nil || cfg.Exports.MaxConcurrent < 1 || cfg.Exports.MaxDownloads < 1 || cfg.Exports.DownloadMemoryMB < 1 || cfg.Exports.CleanupInterval <= 0 || cfg.Exports.CleanupMaxRemovals < 1 || !reflect.DeepEqual(cfg.Policy, store.Policy()) {
		return nil, exports.ErrInvalid
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &ExportRuntime{cfg: cfg, store: store, executor: executor, pool: pool, owner: owner, ctx: ctx, cancel: cancel, dispatchDone: make(chan struct{}), permits: make(chan struct{}, cfg.Exports.MaxConcurrent), downloads: make(chan struct{}, cfg.Exports.MaxDownloads), jobs: make(map[string]*exportReservation)}
	r.dispatchCtx, r.dispatchCancel = context.WithCancel(ctx)
	probe, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	r.custody, err = exports.OpenCustody(probe, cfg.Exports.Directory, cfg.Policy.TenantID, cfg.WorkerID)
	if err != nil {
		cancel()
		return nil, err
	}
	r.storage, err = exports.Open(exports.Config{Directory: r.custody.DataDirectory(), Tenant: cfg.Policy.TenantID, MaxEntries: cfg.Exports.MaxEntries, MaxStoredBytes: cfg.Exports.MaxStoredBytes, MaxTTL: cfg.Policy.Exports.MaxTTL})
	if err != nil {
		cancel()
		return nil, errors.Join(err, r.custody.Close())
	}
	if cfg.RuntimeAudit != nil {
		if !cfg.RuntimeAudit.matches(cfg.Audit, "worker", []string{cfg.Policy.TenantID}) {
			err = audit.ErrInvalid
		} else {
			r.audit = cfg.RuntimeAudit
		}
	} else {
		r.audit, err = OpenServiceAudit(cfg.Audit, "worker", []string{cfg.Policy.TenantID})
		r.ownAudit = err == nil
	}
	if err != nil {
		cancel()
		return nil, errors.Join(err, r.storage.Close(), r.custody.Close())
	}
	// Startup never scans broker jobs for executable work: only fresh Queued
	// deliveries can be assigned. Existing Ready reads use the retained root ID.
	r.wg.Add(2)
	go r.dispatchExports()
	go r.cleanupExports()
	return r, nil
}

func (r *ExportRuntime) Ready() bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.draining && r.failure == nil && r.ctx.Err() == nil && r.audit.Ready()
}

func (r *ExportRuntime) BeginDrain() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.draining = true
	r.dispatchCancel()
	r.mu.Unlock()
}

// Stop is permanent and is also invoked when the owning node loses its lease.
func (r *ExportRuntime) Stop() {
	if r == nil {
		return
	}
	r.BeginDrain()
	r.cancel()
}

func (r *ExportRuntime) Drain(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.BeginDrain()
	select {
	case <-r.dispatchDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		r.mu.Lock()
		empty := len(r.jobs) == 0 && len(r.downloads) == 0
		r.mu.Unlock()
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

func (r *ExportRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		r.Stop()
		r.wg.Wait()
		r.handlers.Wait()
		// A busy store keeps custody, preventing another process from taking
		// ownership while leaked readers or writers still retain live leases.
		err := r.storage.Close()
		if err == nil {
			err = r.custody.Close()
		}
		if r.ownAudit {
			err = errors.Join(err, r.audit.CloseBounded())
		}
		r.mu.Lock()
		r.closeErr = errors.Join(r.failure, err)
		r.mu.Unlock()
	})
	return r.closeErr
}

func (r *ExportRuntime) storageFailure(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.failure = errors.Join(r.failure, err)
	r.mu.Unlock()
	r.Stop()
}

func (r *ExportRuntime) cleanupExports() {
	defer r.wg.Done()
	tick := time.NewTicker(r.cfg.Exports.CleanupInterval)
	defer tick.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-tick.C:
		}
		ctx, stop := context.WithTimeout(r.ctx, min(r.cfg.Exports.CleanupInterval, 5*time.Second))
		_, err := r.storage.Cleanup(ctx, r.cfg.Exports.CleanupMaxRemovals)
		stop()
		if err != nil && r.ctx.Err() == nil {
			r.storageFailure(err)
			return
		}
	}
}

func (r *ExportRuntime) releaseExport(id string, reservation *exportReservation) {
	reservation.once.Do(func() {
		close(reservation.done)
		reservation.cancel()
		r.mu.Lock()
		if r.jobs[id] == reservation {
			delete(r.jobs, id)
		}
		r.mu.Unlock()
		<-r.permits
	})
}

func (r *ExportRuntime) stopExport(id string, reservation *exportReservation) {
	reservation.cancel()
	r.mu.Lock()
	started := reservation.started
	r.mu.Unlock()
	if !started {
		r.releaseExport(id, reservation)
	}
}

func (r *ExportRuntime) ownedExport(ctx context.Context, id string) (ExportSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ExportSnapshot{}, err
	}
	s, err := r.store.GetExport(ctx, id)
	if err != nil {
		return ExportSnapshot{}, err
	}
	if validateExportJob(r.cfg.Policy, s.Job) != nil || s.Job.WorkerID != r.cfg.WorkerID || s.Job.WorkerOwner != r.owner {
		return ExportSnapshot{}, ErrExportConflict
	}
	return s, nil
}

func (r *ExportRuntime) updateExport(ctx context.Context, id string, mutate func(*ExportJob) error) (ExportSnapshot, error) {
	for range 8 {
		s, err := r.ownedExport(ctx, id)
		if err != nil {
			return ExportSnapshot{}, err
		}
		next := s.Job
		if err = mutate(&next); err != nil {
			return ExportSnapshot{}, err
		}
		result, err := r.store.CompareAndSwapExport(ctx, s, next)
		if !errors.Is(err, ErrExportConflict) {
			return result, err
		}
	}
	return ExportSnapshot{}, ErrExportConflict
}

func (r *ExportRuntime) failExport(id string, cause error, uncertain bool) error {
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	_, err := r.updateExport(ctx, id, func(j *ExportJob) error {
		if !exportActive(*j) {
			return ErrExportConflict
		}
		j.State = ExportFailed
		if uncertain && j.Local != nil && j.Receipt == nil {
			j.State = ExportPublicationUncertain
		}
		j.Error = query.PublicError(cause)
		return nil
	})
	if errors.Is(err, ErrExportConflict) || errors.Is(err, ErrExportNotFound) {
		return nil
	}
	return err
}

func exportLive(j ExportJob) error {
	now := time.Now()
	if !now.Before(j.ExpiresAt) || !now.Before(j.AuthorityUntil) {
		return context.DeadlineExceeded
	}
	if j.State != ExportStored && !j.ExecutionDeadline.IsZero() && !now.Before(j.ExecutionDeadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func (r *ExportRuntime) RunExport(parent context.Context, claimed ExportSnapshot) (result ExportRunResult, resultErr error) {
	j := claimed.Job
	if j.State != ExportClaimed || j.WorkerID != r.cfg.WorkerID || j.WorkerOwner != r.owner || validateExportJob(r.cfg.Policy, j) != nil {
		return result, ErrExportConflict
	}
	r.mu.Lock()
	reservation := r.jobs[j.ID]
	if r.ctx.Err() != nil || reservation == nil || reservation.started || reservation.ctx.Err() != nil {
		r.mu.Unlock()
		return result, ErrExportConflict
	}
	reservation.started = true
	r.handlers.Add(1)
	r.mu.Unlock()
	defer r.handlers.Done()
	defer r.releaseExport(j.ID, reservation)
	ctx, cancel := context.WithCancel(reservation.ctx)
	stopClient := context.AfterFunc(parent, cancel)
	defer func() { stopClient(); cancel() }()
	if err := parent.Err(); err != nil {
		return result, err
	}
	running, err := r.updateExport(ctx, j.ID, func(next *ExportJob) error {
		if next.State != ExportClaimed || next.Claim != j.Claim || next.Local != nil || next.Receipt != nil {
			return ErrExportConflict
		}
		if err := exportLive(*next); err != nil {
			return err
		}
		next.State = ExportRunning
		return nil
	})
	if err != nil {
		return result, errors.Join(err, r.failExport(j.ID, err, false))
	}
	j = running.Job
	ctx, stopDeadline := context.WithDeadline(ctx, j.ExecutionDeadline)
	defer stopDeadline()
	op, err := r.audit.begin(ctx, j.TenantID, &j.Authority.Principal, audit.ExportExecution)
	if err != nil {
		return result, errors.Join(err, r.failExport(j.ID, err, false))
	}
	defer op.abort(ctx)
	reservationBudget, err := r.pool.Acquire(ctx, admission.Request{Class: admission.ClassExport, MemoryBytes: (int64(j.Spec.QueryLimits.MemoryMB) + r.cfg.Resources.OverheadMB) << 20, ScratchBytes: int64(j.Spec.QueryLimits.MaxTempMB) << 20})
	if err != nil {
		return result, errors.Join(err, r.failExport(j.ID, err, false))
	}
	processCustody, _ := containment.NewCustody(reservationBudget.Release)
	defer processCustody.Complete()
	ctx = containment.WithCustody(ctx, processCustody)
	identity, err := exportIdentity(r.cfg.Policy, j.Authority)
	if err != nil {
		return result, errors.Join(err, r.failExport(j.ID, err, false))
	}
	w, err := r.storage.Reserve(ctx, exports.Request{Identity: identity, ExpiresAt: j.ExpiresAt, Limits: j.Spec.StorageLimits})
	if err != nil {
		return result, errors.Join(err, r.failExport(j.ID, err, errors.Is(err, exports.ErrPublicationUncertain)))
	}
	// Close errors remain part of the operation. Close cancels unpublished
	// writers; a committed/uncertain writer has already released its lease.
	defer func() {
		closeErr := w.Close()
		resultErr = errors.Join(resultErr, closeErr)
		if closeErr != nil {
			r.storageFailure(closeErr)
		}
	}()
	locator := ExportLocator{StorageID: r.custody.ID(), ExportID: w.ID(), Fence: w.Fence()}
	_, err = r.updateExport(ctx, j.ID, func(next *ExportJob) error {
		if next.State != ExportRunning || next.Claim != j.Claim || next.Local != nil {
			return ErrExportConflict
		}
		if err := exportLive(*next); err != nil {
			return err
		}
		next.Local = &locator
		return nil
	})
	if err != nil {
		return result, errors.Join(err, r.failExport(j.ID, err, false))
	}
	executionCtx, err := executionAuthorityContext(ctx, r.cfg.Policy, &j.Authority.Principal, j.Request)
	sink := &exportSink{ctx: ctx, writer: w, identity: identity}
	var stats query.Stats
	if err == nil {
		stats, err = r.executor.Execute(executionCtx, j.Request, sink)
	}
	if err == nil && !sink.bound {
		err = query.NewError("QUERY_FAILED", "Export result schema is missing")
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		current, e := r.ownedExport(ctx, j.ID)
		err = e
		if err == nil && (current.Job.State != ExportRunning || current.Job.Local == nil || *current.Job.Local != locator) {
			err = ErrExportConflict
		}
		if err == nil {
			err = exportLive(current.Job)
		}
	}
	err = errors.Join(err, op.complete(err))
	if err != nil {
		uncertain := errors.Is(err, exports.ErrPublicationUncertain)
		if uncertain {
			r.storageFailure(err)
		}
		return result, errors.Join(err, r.failExport(j.ID, err, uncertain))
	}
	manifest, err := w.Commit(ctx, identity)
	if err != nil {
		uncertain := errors.Is(err, exports.ErrPublicationUncertain)
		if uncertain {
			r.storageFailure(err)
		}
		return result, errors.Join(err, r.failExport(j.ID, err, uncertain))
	}
	receipt, err := sealExportReceipt(locator, manifest)
	if err != nil {
		return result, errors.Join(err, r.failExport(j.ID, err, true))
	}
	stats.WireBytes = manifest.EncodedBytes
	_, err = r.updateExport(ctx, j.ID, func(next *ExportJob) error {
		if next.State != ExportRunning || next.Local == nil || *next.Local != locator || next.Receipt != nil {
			return ErrExportConflict
		}
		if err := exportLive(*next); err != nil {
			return err
		}
		next.State, next.Receipt, next.Stats = ExportStored, &receipt, stats
		return nil
	})
	if err != nil {
		return result, errors.Join(err, r.failExport(j.ID, err, true))
	}
	return ExportRunResult{Receipt: receipt, Stats: stats}, nil
}

type exportSink struct {
	ctx      context.Context
	writer   *exports.Writer
	identity exports.Identity
	bound    bool
}

func (s *exportSink) Schema(schema *arrow.Schema) error {
	if s.bound || schema == nil {
		return query.NewError("QUERY_FAILED", "Invalid export result schema")
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if err := s.writer.BindSchema(s.ctx, s.identity, schema); err != nil {
		return err
	}
	s.bound = true
	return nil
}
func (s *exportSink) Write(batch arrow.RecordBatch) error {
	if !s.bound {
		return query.NewError("QUERY_FAILED", "Export result schema is missing")
	}
	return s.writer.Write(s.ctx, batch)
}
