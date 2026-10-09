// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	"github.com/SYNEHQ/kelvo-go/internal/operationrun"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// Application owns one durable operation ledger. It never stores credentials.
// The process lock replaces distributed worker ownership in this local mode.
type Application struct {
	cfg                         ApplicationConfig
	trust                       operations.GrantTrust
	token, owner                string
	backend                     *operationstore.SQLiteBackend
	ledger                      *operationstore.Store
	inputs, results             *operationinput.Store
	inputCustody, resultCustody *exports.Custody
	runtime                     *operationrun.Runtime
	executor                    *worker.Executor
	audit                       *ServiceAudit
	ctx                         context.Context
	cancel                      context.CancelFunc
	maintenance                 chan struct{}
	httpSlots                   chan struct{}
	mu                          sync.Mutex
	draining, closed            bool
	httpUsers                   sync.WaitGroup
	failed                      atomic.Bool
	started                     atomic.Bool
}

func NewApplication(ctx context.Context, cfg ApplicationConfig, token string, executor *worker.Executor) (_ *Application, resultErr error) {
	if ctx == nil || cfg.Validate() != nil || len(token) < 32 || len(token) > 4096 || executor == nil || executor.ResourcePool == nil || executor.ScratchRoot == nil || executor.Containment == nil ||
		!executor.HasConnectionResolver(cfg.Issuer, cfg.Resolver.URL) || executor.Limits != cfg.Limits || executor.SandboxPath != cfg.SandboxPath {
		return nil, errApplicationConfig
	}
	var owner [16]byte
	if _, err := rand.Read(owner[:]); err != nil {
		return nil, operationstore.ErrUnavailable
	}
	a := &Application{cfg: cfg, token: token, owner: hex.EncodeToString(owner[:]), executor: executor, maintenance: make(chan struct{}), httpSlots: make(chan struct{}, cfg.MaxHTTPRequests)}
	a.ctx, a.cancel = context.WithCancel(context.WithoutCancel(ctx))
	a.trust, _ = cfg.trust()
	defer func() {
		if resultErr != nil {
			a.cancel()
			_ = a.closeStorage()
		}
	}()
	var err error
	a.backend, err = operationstore.OpenSQLite(ctx, filepath.Join(cfg.StateDirectory, "ledger"), cfg.operationPolicy(), cfg.authorityFingerprint())
	if err != nil {
		return nil, err
	}
	a.ledger, err = operationstore.New(a.backend, cfg.operationPolicy())
	if err != nil {
		return nil, err
	}
	a.inputCustody, err = exports.OpenCustody(ctx, filepath.Join(cfg.StateDirectory, "requests"), cfg.InstanceID, "application")
	if err != nil {
		return nil, err
	}
	a.inputs, err = operationinput.Open(operationinput.Config{MaxInputBytes: operations.MaxRequestBytes, Storage: exports.Config{Directory: a.inputCustody.DataDirectory(), Tenant: cfg.InstanceID, MaxEntries: cfg.MaxRetained * 2, MaxStoredBytes: int64(cfg.MaxRetained) * operations.MaxRequestBytes * 4, MaxTTL: 6 * time.Minute}})
	if err != nil {
		return nil, err
	}
	a.resultCustody, err = exports.OpenCustody(ctx, filepath.Join(cfg.StateDirectory, "results"), cfg.InstanceID, "application")
	if err != nil {
		return nil, err
	}
	a.results, err = operationinput.Open(operationinput.Config{MaxInputBytes: cfg.MaxResultBytes, Storage: exports.Config{Directory: a.resultCustody.DataDirectory(), Tenant: cfg.InstanceID, MaxEntries: cfg.MaxRetained, MaxStoredBytes: cfg.MaxStoredBytes, MaxTTL: cfg.Retention + time.Minute}})
	if err != nil {
		return nil, err
	}
	a.audit, err = OpenServiceAudit(cfg.auditConfig(), "worker", []string{cfg.InstanceID})
	if err != nil {
		return nil, err
	}
	a.runtime, err = operationrun.New(a.ledger, operationrun.Config{WorkerID: "application", Owner: a.owner, Concurrency: cfg.MaxConcurrent, PollInterval: 100 * time.Millisecond}, operationrun.Hooks{
		Load: a.loadRequest, Authorize: a.authorize,
		Prepare: func(ctx context.Context, record operationstore.Record, request operations.Request, binding operationstore.Binding) (operationrun.Prepared, error) {
			if a.failed.Load() || ctx.Err() != nil {
				return nil, operationstore.ErrUnavailable
			}
			return &preparedApplicationOperation{app: a, record: record, request: request}, nil
		},
		AuditAdmission: func(ctx context.Context, record operationstore.Record, request operations.Request) error {
			if ctx.Err() != nil || !a.audit.Ready() {
				return audit.ErrUnavailable
			}
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	go a.maintain()
	return a, nil
}

// Start follows listener readiness. Resolver callbacks must reach the local
// lease endpoint before a retained queued operation can begin.
func (a *Application) Start() error {
	if err := a.runtime.Start(a.ctx); err != nil {
		return err
	}
	a.started.Store(true)
	return nil
}

func (a *Application) beginAudit(ctx context.Context, kind audit.Kind) (*auditOperation, error) {
	return a.audit.begin(ctx, a.cfg.InstanceID, &JobAuthority{PrincipalID: a.cfg.ServicePrincipal, PrincipalKind: "service", PolicyVersion: a.cfg.authorityFingerprint()}, kind)
}

func (a *Application) loadRequest(ctx context.Context, record operationstore.Record) (operations.Request, error) {
	raw, err := a.inputs.Load(ctx, operationInputIdentity(record.Scope, record.AuthoritySHA256), record.RequestRef)
	if err != nil {
		return operations.Request{}, err
	}
	defer clear(raw)
	return operations.ParseRequest(raw)
}

func (a *Application) authorize(ctx context.Context, record operationstore.Record, request operations.Request, binding operationstore.Binding) error {
	if ctx.Err() != nil || a.ctx.Err() != nil || a.failed.Load() || binding.WorkerID != "application" || binding.Owner != a.owner || record.Binding != binding || operations.GrantDigest(record.AuthorityToken) != record.AuthoritySHA256 {
		return operationstore.ErrConflict
	}
	claims, err := operations.VerifyGrant(record.AuthorityToken, a.trust, request, time.Now())
	if err != nil || !a.cfg.allows(claims) || operationScope(claims) != record.Scope || claims.RequestSHA256 != record.RequestSHA256 || claims.Operation != record.Kind || record.AuthorityUntil.After(time.Unix(claims.ExpiresAt, 0)) {
		return operationstore.ErrConflict
	}
	current, err := a.ledger.Get(ctx, record.Scope, record.ID)
	if err != nil {
		return err
	}
	got := current.Record
	if (got.State != operationstore.Assigned && got.State != operationstore.Running) || got.Binding != binding || got.RequestRef != record.RequestRef || got.RequestSHA256 != record.RequestSHA256 || got.AuthoritySHA256 != record.AuthoritySHA256 || !time.Now().Before(got.LeaseUntil) || !time.Now().Before(got.ExecuteBefore) {
		return operationstore.ErrConflict
	}
	return ctx.Err()
}

func (a *Application) maintain() {
	defer close(a.maintenance)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
			_, err := a.inputs.Cleanup(ctx, 32)
			if err == nil {
				_, err = a.results.Cleanup(ctx, 32)
			}
			cancel()
			if err != nil && a.ctx.Err() == nil {
				a.failed.Store(true)
				a.BeginDrain()
				return
			}
		}
	}
}

func (a *Application) BeginDrain() {
	a.mu.Lock()
	a.draining = true
	a.mu.Unlock()
	a.runtime.BeginDrain()
}
func (a *Application) Drain(ctx context.Context) error { a.BeginDrain(); return a.runtime.Drain(ctx) }

// Close retains storage custody when a runtime or HTTP handler has not joined.
func (a *Application) Close(ctx context.Context) error {
	a.BeginDrain()
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
	a.cancel()
	err := a.runtime.Close(ctx)
	if !a.runtime.Joined() {
		return err
	}
	done := make(chan struct{})
	go func() { a.httpUsers.Wait(); <-a.maintenance; close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
	return errors.Join(err, a.closeStorage())
}

func (a *Application) closeStorage() error {
	var err error
	if a.audit != nil {
		err = errors.Join(err, a.audit.CloseBounded())
	}
	if a.results != nil {
		if e := a.results.Close(); e != nil {
			return errors.Join(err, e)
		}
	}
	if a.inputs != nil {
		if e := a.inputs.Close(); e != nil {
			return errors.Join(err, e)
		}
	}
	if a.resultCustody != nil {
		err = errors.Join(err, a.resultCustody.Close())
	}
	if a.inputCustody != nil {
		err = errors.Join(err, a.inputCustody.Close())
	}
	if a.backend != nil {
		err = errors.Join(err, a.backend.Close())
	}
	return err
}

type preparedApplicationOperation struct {
	app        *Application
	record     operationstore.Record
	request    operations.Request
	completion containment.Completion
	sink       *operationResultSink
}

func (p *preparedApplicationOperation) Close() error {
	if p.sink != nil {
		if err := p.sink.abort(); err != nil {
			if errors.Is(err, operationinput.ErrCleanup) || errors.Is(err, operationrun.ErrCleanup) {
				p.app.failed.Store(true)
				p.app.executor.Containment.QuarantineOperation()
				return operationrun.ErrCleanup
			}
			p.completion.Complete()
			return err
		}
	}
	p.completion.Complete()
	return nil
}

func (p *preparedApplicationOperation) Execute(ctx context.Context) (receipt operations.Receipt, resultErr error) {
	rejected := func(code string) operations.Receipt {
		return operations.Receipt{Version: operations.Version, OperationID: p.record.ID, RequestSHA256: p.record.RequestSHA256, Outcome: operations.Rejected, Effect: operations.EffectNone, ErrorCode: code}
	}
	op, err := p.app.beginAudit(ctx, audit.OperationExecution)
	if err != nil {
		return rejected("UNAVAILABLE"), err
	}
	defer op.abort(ctx)
	defer func() {
		outcome, category := audit.Unknown, audit.Interrupted
		if receipt.Validate() == nil && receipt.OperationID == p.record.ID && receipt.RequestSHA256 == p.record.RequestSHA256 {
			switch receipt.Outcome {
			case operations.Completed:
				outcome, category = audit.Succeeded, audit.None
			case operations.Rejected:
				outcome, category = audit.Denied, audit.InvalidRequest
			case operations.Failed:
				outcome, category = audit.Failed, audit.Internal
			case operations.CancelledBeforeStart:
				outcome, category = audit.Cancelled, audit.Cancellation
			}
		}
		resultErr = errors.Join(resultErr, op.finish(outcome, category))
	}()
	ctx = containment.WithCompletion(ctx, &p.completion)
	sink := &operationResultSink{mutating: p.request.Kind.Mutating()}
	p.sink = sink
	capacityExceeded := false
	receipt, resultErr = p.app.executor.ExecuteResolvedOperation(ctx, p.app.cfg.Adapter, p.record.ID, p.record.RequestSHA256, func(admitted context.Context) (adapter.ProcessRequest, error) {
		current, err := p.app.ledger.Current(admitted, p.record.Scope, p.record.ID, p.record.Binding)
		if err != nil {
			return adapter.ProcessRequest{}, err
		}
		record := current.Record
		if err = p.app.authorize(admitted, record, p.request, record.Binding); err != nil {
			return adapter.ProcessRequest{}, err
		}
		limits := p.app.cfg.Limits
		limits.MaxBytes = min(limits.MaxBytes, p.app.cfg.MaxResultBytes)
		if p.request.Kind == operations.MetadataInspect {
			limits.MaxBytes = min(limits.MaxBytes, p.app.cfg.MaxMetadataBytes)
		}
		// Only reads and metadata produce Arrow in the supported relational
		// protocol. Connection checks and statements need no result reservation.
		if p.request.Kind == operations.QueryRead || p.request.Kind == operations.MetadataInspect {
			// The worker cancels its inner context before result finalization.
			// Keep storage under the outer operation context until retention.
			sink.stream, err = p.app.results.BeginStreamLimit(ctx, operationInputIdentity(record.Scope, record.AuthoritySHA256), record.RetainUntil, operationinput.ArrowIPC, limits.MaxBytes)
			if err != nil {
				capacityExceeded = errors.Is(err, exports.ErrLimit)
				return adapter.ProcessRequest{}, err
			}
			sink.inner = worker.NewIPCSink(sink.stream, limits)
		}
		// The application may bind custody even if its response is interrupted.
		// Report physical cleanup after every attempted credential resolution.
		sink.cleaned = func(ctx context.Context) error { return p.app.executor.CompleteOperationCleanup(ctx, record) }
		input, err := p.app.executor.ResolveOperationSource(admitted, record, p.request)
		if err != nil {
			return adapter.ProcessRequest{}, err
		}
		switch input.Source.Engine {
		case "postgres", "postgresql", "mysql":
		default:
			return adapter.ProcessRequest{}, operations.ErrUnsupported
		}
		input.Limits.MaxBytes = min(input.Limits.MaxBytes, limits.MaxBytes)
		return input, nil
	}, sink)
	if capacityExceeded && receipt.Outcome == operations.Rejected && receipt.Effect == operations.EffectNone {
		receipt.ErrorCode = "RESOURCE_EXHAUSTED"
	}
	return receipt, resultErr
}
