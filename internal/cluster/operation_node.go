// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
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
	if s.worker != nil {
		if err := s.worker.Close(ctx); err != nil {
			return err
		}
	}
	if s.results != nil {
		if err := s.results.Close(); err != nil {
			return err
		}
	}
	if s.custody != nil {
		return s.custody.Close()
	}
	return nil
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
	node    *Node
	state   *nodeOperations
	record  operationstore.Record
	request operations.Request
}

func (p *preparedNodeOperation) Close() error { return nil }

func (p *preparedNodeOperation) Execute(ctx context.Context) (operations.Receipt, error) {
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
	defer sink.abort()
	var payload []byte
	defer func() { clear(payload) }()
	return p.state.executor.ExecuteResolvedOperation(ctx, p.node.cfg.Operations.Adapter, p.record.ID, p.record.RequestSHA256,
		func(admitted context.Context) (adapter.ProcessRequest, error) {
			current, err := p.state.ledger.Get(admitted, p.record.Scope, p.record.ID)
			if err != nil {
				return adapter.ProcessRequest{}, err
			}
			record := current.Record
			if record.State != operationstore.Running || record.Binding != p.record.Binding || record.RequestSHA256 != p.record.RequestSHA256 || record.AuthoritySHA256 != p.record.AuthoritySHA256 {
				return adapter.ProcessRequest{}, operationstore.ErrConflict
			}
			trust, err := operationTrust(p.node.cfg.Policy, record.Scope.ServicePrincipal)
			if err != nil {
				return adapter.ProcessRequest{}, err
			}
			claims, err := operations.VerifyGrant(record.AuthorityToken, trust, p.request, time.Now())
			if err != nil || operationScope(claims) != record.Scope {
				return adapter.ProcessRequest{}, operationstore.ErrConflict
			}
			bindOperationCleanup(sink, claims, func(ctx context.Context) error {
				return p.state.executor.CompleteOperationCleanup(ctx, record)
			})
			// Reserve result disk before credentials or customer connections. The
			// runner already holds shared process/memory/scratch admission here.
			sink.stream, err = p.state.results.BeginStream(ctx, operationInputIdentity(record.Scope, record.AuthoritySHA256), record.RetainUntil, operationinput.ArrowIPC)
			if err != nil {
				return adapter.ProcessRequest{}, err
			}
			limits := p.node.cfg.Policy.Limits
			limits.MaxBytes = min(limits.MaxBytes, p.node.cfg.Operations.MaxResultBytes)
			sink.inner = worker.NewIPCSink(sink.stream, limits)
			payload, err = p.state.worker.input.loadBulk(admitted, record, p.request)
			if err != nil {
				return adapter.ProcessRequest{}, err
			}
			if err := p.state.worker.authorize(admitted, record, &p.request, record.Binding); err != nil {
				return adapter.ProcessRequest{}, err
			}
			if err := authorizeOperationIngestionRun(claims, p.request, payload); err != nil {
				return adapter.ProcessRequest{}, err
			}
			input, err := p.state.executor.ResolveOperationSourceWithInput(admitted, record, p.request, payload)
			if err == nil {
				input.Limits.MaxBytes = min(input.Limits.MaxBytes, limits.MaxBytes)
			}
			return input, err
		}, sink)
}

func (sink *operationResultSink) FinalizeOperation(receipt operations.Receipt, executionErr error) (operations.Receipt, error) {
	defer sink.abort()
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
	if s.inner != nil {
		s.inner.Abort()
	}
	if s.stream != nil {
		return s.stream.Close()
	}
	return nil
}

var _ operationrun.Prepared = (*preparedNodeOperation)(nil)
var _ query.Sink = (*operationResultSink)(nil)
