// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	"github.com/SYNEHQ/kelvo-go/internal/operationrun"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

type nodeOperations struct {
	private   *worker.PrivateOperations
	worker    *OperationWorker
	ledger    *operationstore.Store
	results   *operationinput.Store
	custody   *exports.Custody
	executor  *worker.Executor
	downloads chan struct{}
	failed    atomic.Bool
}

func (n *Node) initOperations(ctx context.Context) (resultErr error) {
	if n.cfg.Operations == nil {
		return nil
	}
	native, ok := n.executor.(*worker.Executor)
	base, stored := n.store.(*NATSStore)
	if !ok || !stored || native.ResourcePool == nil || native.ScratchRoot == nil || native.Containment == nil {
		return errOperationConfig
	}
	if native.Config.Acceleration != nil && overlappingExportPaths(n.cfg.Operations.Results.Directory, native.Config.Acceleration.Directory) {
		return errOperationConfig
	}
	c := n.cfg.Operations
	state := &nodeOperations{executor: native, downloads: make(chan struct{}, c.MaxDownloads)}
	defer func() {
		if resultErr != nil {
			_ = state.close(context.Background())
		}
	}()
	var err error
	bindings := make(map[string]worker.PrivateOperationBinding, len(c.PrivateSources))
	for principal, config := range c.PrivateSources {
		trust, trustErr := operationTrust(n.cfg.Policy, principal)
		resolver, exists := n.cfg.ConnectionResolvers[trust.Issuer]
		if trustErr != nil || !exists {
			return errOperationConfig
		}
		bindings[principal] = worker.PrivateOperationBinding{Config: config, Trust: trust, ResolverCAFile: resolver.CAFile}
	}
	state.private, err = worker.NewPrivateOperations(native, n.cfg.WorkerID, WorkerIdentity(n.cfg.Policy.TenantID, n.cfg.WorkerID), c.MaxConcurrent, bindings)
	if err != nil {
		return err
	}
	state.ledger, err = base.OpenOperations(ctx, false)
	if err != nil {
		return err
	}
	state.custody, err = exports.OpenCustody(ctx, c.Results.Directory, n.cfg.Policy.TenantID, n.cfg.WorkerID)
	if err != nil {
		return err
	}
	state.results, err = operationinput.Open(operationinput.Config{MaxInputBytes: c.MaxResultBytes, Storage: exports.Config{
		Directory: state.custody.DataDirectory(), Tenant: n.cfg.Policy.TenantID, MaxEntries: c.Results.MaxEntries,
		MaxStoredBytes: c.Results.MaxStoredBytes, MaxTTL: n.cfg.Policy.Operations.Retention}})
	if err != nil {
		return err
	}
	state.worker, err = NewOperationWorker(n.cfg.Policy, n.store, state.ledger, OperationWorkerConfig{
		InputURL: c.InputURL, TLS: c.TLS, Runtime: operationrun.Config{WorkerID: n.cfg.WorkerID, Owner: n.owner,
			Concurrency: c.MaxConcurrent, PollInterval: c.PollInterval}}, n.audit,
		func(ctx context.Context, record operationstore.Record, request operations.Request, binding operationstore.Binding) (operationrun.Prepared, error) {
			if state.failed.Load() || ctx.Err() != nil {
				return nil, operationstore.ErrUnavailable
			}
			return &preparedNodeOperation{node: n, state: state, record: record, request: request}, nil
		})
	if err != nil {
		return err
	}
	n.operations = state
	return nil
}

func (s *nodeOperations) close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	var resultErr error
	if s.worker != nil {
		if err := s.worker.Close(ctx); err != nil {
			if !s.worker.runtime.Joined() {
				return err
			}
			resultErr = err
		}
	}
	if err := s.private.Close(ctx); err != nil {
		return errors.Join(resultErr, err)
	}
	if s.results != nil {
		if err := s.results.Close(); err != nil {
			return errors.Join(resultErr, err)
		}
	}
	if s.custody != nil {
		return errors.Join(resultErr, s.custody.Close())
	}
	return resultErr
}

func (n *Node) cleanupOperationResults() {
	defer n.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			ctx, stop := context.WithTimeout(n.ctx, 5*time.Second)
			_, err := n.operations.results.Cleanup(ctx, 32)
			stop()
			if err != nil && n.ctx.Err() == nil {
				n.operations.failed.Store(true)
				n.BeginDrain()
				return
			}
		}
	}
}

type preparedNodeOperation struct {
	node       *Node
	state      *nodeOperations
	record     operationstore.Record
	request    operations.Request
	completion containment.Completion
	sink       *operationResultSink
}

func (p *preparedNodeOperation) Close() error {
	if p.sink != nil {
		if err := p.sink.abort(); err != nil {
			if errors.Is(err, operationinput.ErrCleanup) || errors.Is(err, operationrun.ErrCleanup) {
				p.state.failed.Store(true)
				p.state.executor.Containment.QuarantineOperation()
				return operationrun.ErrCleanup // Retain capacity while cleanup is uncertain.
			}
			// A settled result error is distinct from physical cleanup failure.
			p.completion.Complete()
			return err
		}
	}
	p.completion.Complete()
	return nil
}

func (p *preparedNodeOperation) Execute(ctx context.Context) (operations.Receipt, error) {
	ctx = containment.WithCompletion(ctx, &p.completion)
	ctx = worker.WithOperationFilePublisher(ctx, func(admitted context.Context, input adapter.ProcessRequest, candidate filesnapshot.Descriptor, source io.Reader) (bool, error) {
		if input.SourceFile == nil {
			return false, operationstore.ErrConflict
		}
		publication := filesnapshot.Publication{Version: 1, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256, Original: *input.SourceFile, Replacement: candidate}
		return p.state.executor.PublishOperationFile(admitted, p.record, p.request, input.Source.Revision, publication, source)
	})
	ctx = worker.WithOperationFileResolver(ctx, func(admitted context.Context, input adapter.ProcessRequest, destination io.Writer) (int64, error) {
		if input.SourceFile == nil {
			return 0, operationstore.ErrConflict
		}
		return p.state.executor.FetchOperationFile(admitted, p.record, p.request, input.Source.Revision, *input.SourceFile, destination)
	})
	sink := &operationResultSink{mutating: p.request.Kind.Mutating()}
	p.sink = sink
	var payload []byte
	defer func() { clear(payload) }()
	return p.state.executor.ExecuteDelegatedOperation(ctx, p.state.private, p.node.cfg.Operations.Adapter, p.record.ID, p.record.RequestSHA256,
		func(admitted context.Context) (worker.OperationSourceRequest, error) {
			current, err := p.state.ledger.Get(admitted, p.record.Scope, p.record.ID)
			if err != nil {
				return worker.OperationSourceRequest{}, err
			}
			record := current.Record
			if record.State != operationstore.Running || record.Binding != p.record.Binding || record.RequestSHA256 != p.record.RequestSHA256 || record.AuthoritySHA256 != p.record.AuthoritySHA256 {
				return worker.OperationSourceRequest{}, operationstore.ErrConflict
			}
			trust, err := operationTrust(p.node.cfg.Policy, record.Scope.ServicePrincipal)
			if err != nil {
				return worker.OperationSourceRequest{}, err
			}
			claims, err := operations.VerifyGrant(record.AuthorityToken, trust, p.request, time.Now())
			if err != nil || operationScope(claims) != record.Scope {
				return worker.OperationSourceRequest{}, operationstore.ErrConflict
			}
			bindOperationCleanup(sink, claims, func(ctx context.Context) error {
				return p.state.executor.CompleteOperationCleanup(ctx, record)
			})
			// Reserve result disk before credentials or customer connections. The
			// runner already holds shared process/memory/scratch admission here.
			sink.stream, err = p.state.results.BeginStream(ctx, operationInputIdentity(record.Scope, record.AuthoritySHA256), record.RetainUntil, operationinput.ArrowIPC)
			if err != nil {
				return worker.OperationSourceRequest{}, err
			}
			limits := p.node.cfg.Policy.Limits
			limits.MaxBytes = min(limits.MaxBytes, p.node.cfg.Operations.MaxResultBytes)
			sink.inner = worker.NewIPCSink(sink.stream, limits)
			payload, err = p.state.worker.input.loadBulk(admitted, record, p.request)
			if err != nil {
				return worker.OperationSourceRequest{}, err
			}
			if err := p.state.worker.authorize(admitted, record, &p.request, record.Binding); err != nil {
				return worker.OperationSourceRequest{}, err
			}
			if err := authorizeOperationIngestionRun(claims, p.request, payload); err != nil {
				return worker.OperationSourceRequest{}, err
			}
			return worker.OperationSourceRequest{Record: record, Request: p.request, Payload: payload, MaxResultBytes: limits.MaxBytes, CleanupLedger: p.state.ledger}, nil
		}, sink)
}

func (sink *operationResultSink) FinalizeOperation(receipt operations.Receipt, executionErr error) (result operations.Receipt, resultErr error) {
	defer func() {
		resultErr = errors.Join(resultErr, sink.abort())
		if resultErr != nil && !sink.mutating && result.Outcome == operations.Completed {
			result.Outcome, result.Effect, result.ErrorCode = operations.Failed, operations.EffectNone, "SOURCE_FAILED"
			result.Result = nil
		}
	}()
	if receipt.Outcome != operations.Completed {
		return receipt, executionErr
	}
	// Child result references describe its pipe, never retained storage. Only a
	// fully validated pipe and committed local stream may become a public ref.
	receipt.Result = nil
	if executionErr == nil && sink.seen {
		var ref operations.InputRef
		if executionErr = sink.inner.Finish(); executionErr == nil {
			ref, executionErr = sink.stream.Commit()
		}
		if executionErr == nil {
			receipt.Result = &operations.ResultRef{ID: ref.ID, SHA256: ref.SHA256, Bytes: ref.Bytes, Rows: sink.rows, Format: "arrow_ipc"}
		}
	}
	if executionErr != nil && !sink.mutating {
		receipt.Outcome, receipt.Effect, receipt.ErrorCode = operations.Failed, operations.EffectNone, "SOURCE_FAILED"
	}
	// A confirmed write remains committed even if retaining/delivering its
	// optional result failed. Its source statement is never replayed.
	return receipt, executionErr
}

type operationResultSink struct {
	mutating bool
	inner    *worker.IPCSink
	stream   *operationinput.StreamWriter
	seen     bool
	rows     int64
	cleaned  func(context.Context) error
	aborted  bool
	abortErr error
}

func (s *operationResultSink) OperationCleaned(ctx context.Context) error {
	if s.cleaned == nil {
		return nil
	}
	return s.cleaned(ctx)
}

func (s *operationResultSink) Schema(schema *arrow.Schema) error {
	if s.inner == nil {
		return operationstore.ErrUnavailable
	}
	if err := s.inner.Schema(schema); err != nil {
		return err
	}
	s.seen = true
	return nil
}
func (s *operationResultSink) Write(batch arrow.RecordBatch) error {
	if s.inner == nil {
		return operationstore.ErrUnavailable
	}
	if err := s.inner.Write(batch); err != nil {
		return err
	}
	s.rows += batch.NumRows()
	return nil
}
func (s *operationResultSink) abort() error {
	if s.aborted {
		return s.abortErr
	}
	s.aborted = true
	// A cleanup panic leaves the cached failure intact for Prepared.Close.
	// Recovery must not reinterpret an interrupted cleanup as successful.
	s.abortErr = operationrun.ErrCleanup
	if s.inner != nil {
		s.inner.Abort()
	}
	if s.stream != nil {
		s.abortErr = s.stream.Close()
	} else {
		s.abortErr = nil
	}
	return s.abortErr
}

var _ operationrun.Prepared = (*preparedNodeOperation)(nil)
var _ query.Sink = (*operationResultSink)(nil)
