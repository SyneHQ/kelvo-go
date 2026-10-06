// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

const maximumOperationBinaryBytes = 1 << 30

type OperationProcessConfig struct {
	Binary       string               `yaml:"binary"`
	SHA256       string               `yaml:"sha256"`
	JDBC         *OperationJDBCConfig `yaml:"jdbc,omitempty"`
	preparedJDBC *operationJDBCRuntime
}

// OperationFinalizer commits or aborts a retained result while resource custody
// is still held. Process and scratch cleanup have run before this call.
type OperationFinalizer interface {
	FinalizeOperation(operations.Receipt, error) (operations.Receipt, error)
}

// OperationCleanupObserver records physical cleanup only. It is called after
// the cgroup is proven empty and scratch is removed, including failed source
// executions. It must never interpret this callback as a commit or rollback.
type OperationCleanupObserver interface {
	OperationCleaned(context.Context) error
}

type operationCleanupState struct{ prepared, process, scratch bool }

func (state operationCleanupState) notify(sink query.Sink) error {
	if !state.prepared || !state.process || !state.scratch {
		return nil
	}
	observer, ok := sink.(OperationCleanupObserver)
	if !ok {
		return nil
	}
	// Cleanup may follow cancellation or lease expiry. Only this bounded
	// acknowledgement receives an independent context, never source execution.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return observer.OperationCleaned(ctx)
}

// ValidateOperationProcessConfig verifies an operator-owned, pinned executable.
// Execution repeats verification on the descriptor actually passed to fd 5.
func ValidateOperationProcessConfig(cfg OperationProcessConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	file, _, err := openOperationBinary(ctx, cfg)
	if file != nil {
		_ = file.Close()
	}
	if err != nil {
		return err
	}
	runtime, err := openOperationJDBC(ctx, cfg.JDBC)
	if runtime != nil {
		runtime.close()
	}
	return err
}

func (cfg OperationProcessConfig) Validate() error { return ValidateOperationProcessConfig(cfg) }

func operationFailure(code string) error {
	return query.NewError(code, "Operation worker could not complete its bounded execution")
}

func rejectedOperation(input adapter.ProcessRequest, code string) operations.Receipt {
	r := operations.Receipt{Version: operations.Version, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256,
		Outcome: operations.Rejected, Effect: operations.EffectNone, ErrorCode: code}
	if r.Validate() != nil {
		return operations.Receipt{}
	}
	return r
}

func uncertainOperation(input adapter.ProcessRequest) operations.Receipt {
	r := operations.Receipt{Version: operations.Version, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256,
		Outcome: operations.Failed, Effect: operations.EffectNone, ErrorCode: "SOURCE_FAILED"}
	if input.Request.Kind.Mutating() {
		r.Outcome, r.Effect, r.ErrorCode = operations.OutcomeUnknown, operations.EffectUnknown, "OUTCOME_UNKNOWN"
	}
	return r
}

// ExecuteOperation runs an optional adapter once inside the same process-tree,
// scratch and resource boundaries used for analytical workers. Source secrets
// cross only stdin. A receipt is evidence of source effects, not delivery.
func (e *Executor) ExecuteOperation(parent context.Context, cfg OperationProcessConfig, input adapter.ProcessRequest, sink query.Sink) (receipt operations.Receipt, resultErr error) {
	if input.ValidateAt(time.Now()) != nil {
		return rejectedOperation(input, "INVALID_ARGUMENT"), operationFailure("INVALID_ARGUMENT")
	}
	return e.ExecuteResolvedOperation(parent, cfg, input.OperationID, input.RequestSHA256, func(context.Context) (adapter.ProcessRequest, error) { return input, nil }, sink)
}

// ExecuteResolvedOperation reserves process resources before obtaining fresh
// credentials. The public identity binds pre-dispatch failures even when the
// resolver returns no credentials or request.
// The callback is called once; neither credential resolution nor execution is
// retried here.
func (e *Executor) ExecuteResolvedOperation(parent context.Context, cfg OperationProcessConfig, operationID, requestSHA256 string, resolve func(context.Context) (adapter.ProcessRequest, error), sink query.Sink) (receipt operations.Receipt, resultErr error) {
	input := adapter.ProcessRequest{OperationID: operationID, RequestSHA256: requestSHA256}
	receipt = rejectedOperation(input, "INVALID_ARGUMENT")
	var custody *containment.Custody
	var cleanup operationCleanupState
	defer func() {
		if custody != nil {
			defer custody.Complete()
		}
		if finalizer, ok := sink.(OperationFinalizer); ok {
			receipt, resultErr = finalizer.FinalizeOperation(receipt, resultErr)
		}
		resultErr = errors.Join(resultErr, cleanup.notify(sink))
	}()
	if parent == nil || resolve == nil || e == nil || e.Limits.Validate() != nil || !operations.ValidID(operationID) || !operations.ValidDigest(requestSHA256) {
		return receipt, operationFailure("INVALID_ARGUMENT")
	}
	if e.ScratchRoot == nil || e.SandboxPath == "" || e.ResourcePool == nil || e.Containment == nil || containment.FromContext(parent) != nil ||
		!e.ContainmentBudget.FitsOverhead(int64(e.Limits.MemoryMB), e.ResourceOverheadBytes>>20) {
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("CONFIGURATION_ERROR")
	}
	if e.Containment.Err() != nil {
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("RESOURCE_EXHAUSTED")
	}
	if err := validateOperationLauncher(e.SandboxPath); err != nil {
		return rejectedOperation(input, "UNAVAILABLE"), err
	}
	ctx, cancel := context.WithTimeout(parent, e.Limits.Timeout)
	defer cancel()
	if ctx.Err() != nil {
		return rejectedOperation(input, "CANCELLED"), operationFailure("CANCELLED")
	}
	memoryBytes := int64(e.Limits.MemoryMB) << 20
	if e.ResourceOverheadBytes < 0 || e.ResourceOverheadBytes > math.MaxInt64-memoryBytes {
		return receipt, operationFailure("CONFIGURATION_ERROR")
	}
	reservation, err := e.ResourcePool.Acquire(ctx, admission.Request{MemoryBytes: memoryBytes + e.ResourceOverheadBytes, ScratchBytes: int64(e.Limits.MaxTempMB) << 20})
	if err != nil {
		return rejectedOperation(input, "RESOURCE_EXHAUSTED"), operationFailure("RESOURCE_EXHAUSTED")
	}
	custody, _ = containment.NewCustody(reservation.Release)
	workspace, err := newScratchWorkspace(e.ScratchRoot)
	if err != nil {
		return rejectedOperation(input, "RESOURCE_EXHAUSTED"), operationFailure("RESOURCE_EXHAUSTED")
	}
	holdScratch, err := custody.Hold()
	if err != nil {
		if cleanupErr := workspace.cleanup(); cleanupErr != nil {
			e.Containment.QuarantineOperation()
			reportScratchCleanupFailure(cleanupErr)
		}
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("UNAVAILABLE")
	}
	defer func() {
		if cleanupErr := workspace.cleanup(); cleanupErr != nil {
			e.Containment.QuarantineOperation()
			reportScratchCleanupFailure(cleanupErr)
			resultErr = errors.Join(resultErr, operationFailure("RESOURCE_EXHAUSTED"))
			return // Keep scratch custody until ownership is proven empty.
		}
		holdScratch()
		cleanup.scratch = true
	}()
	binary, identity, err := openOperationBinary(ctx, cfg)
	if err != nil {
		return rejectedOperation(input, "UNAVAILABLE"), err
	}
	defer binary.Close()
	// Cluster nodes pin the optional runtime at startup. Direct SDK callers
	// may prepare it too; the fallback verifies it before obtaining credentials.
	jdbc := cfg.preparedJDBC
	if jdbc == nil {
		jdbc, err = openOperationJDBC(ctx, cfg.JDBC)
		if err != nil {
			return rejectedOperation(input, "UNAVAILABLE"), err
		}
		if jdbc != nil {
			defer jdbc.close()
		}
	}
	operationDirectory := workspace.path
	args, err := SandboxCommand(cfg.Binary, workspace.path, catalog.Config{}, e.Limits)
	if err != nil {
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("CONFIGURATION_ERROR")
	}
	// The launcher grants and executes the inherited inode, not the mutable
	// pathname whose content was originally selected by operator configuration.
	args[len(args)-2] = "/proc/self/fd/5"
	processLimits, err := e.ContainmentBudget.ProcessLimits(int64(e.Limits.MemoryMB), e.Limits.Threads)
	if err != nil {
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("CONFIGURATION_ERROR")
	}
	holdProcess, err := custody.Hold()
	if err != nil {
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("UNAVAILABLE")
	}
	job, err := e.Containment.Prepare(processLimits, holdProcess)
	if err != nil {
		holdProcess()
		return rejectedOperation(input, "RESOURCE_EXHAUSTED"), operationFailure("RESOURCE_EXHAUSTED")
	}
	cleanup.prepared = true
	// Even Start failure has one cleanup owner. Finish retains reservations on
	// cgroup uncertainty; workspace cleanup separately retains scratch custody.
	defer func() {
		if _, err := job.Finish(context.Background()); err != nil {
			resultErr = errors.Join(resultErr, operationFailure("RESOURCE_EXHAUSTED"))
		} else {
			cleanup.process = true
		}
	}()
	if ctx.Err() != nil {
		return rejectedOperation(input, "CANCELLED"), operationFailure("CANCELLED")
	}
	resolved, err := resolve(ctx)
	if err != nil {
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("UNAVAILABLE")
	}
	if resolved.OperationID != operationID || resolved.RequestSHA256 != requestSHA256 {
		return rejectedOperation(input, "INVALID_ARGUMENT"), operationFailure("INVALID_ARGUMENT")
	}
	input = resolved
	// Resolvers never choose runtime descriptors or executable configuration.
	if input.Runtime != nil {
		return rejectedOperation(input, "INVALID_ARGUMENT"), operationFailure("INVALID_ARGUMENT")
	}
	var jdbcFiles []*os.File
	if adapter.JDBCProfile(input.Source.Engine) {
		if jdbc == nil {
			return rejectedOperation(input, "UNSUPPORTED"), operationFailure("UNSUPPORTED")
		}
		if jdbc.binding != jdbcConfigDigest(cfg.JDBC) {
			return rejectedOperation(input, "UNAVAILABLE"), operationFailure("CONFIGURATION_ERROR")
		}
		input.Runtime, jdbcFiles, err = jdbc.selectProfile(input.Source.Engine, input.Limits.MemoryMB)
		if err != nil {
			return rejectedOperation(input, "UNAVAILABLE"), err
		}
		args = append([]string{"--operation-jdbc", fmt.Sprint(len(input.Runtime.JARFDs))}, args...)
	}
	arrowResult := input.Request.Kind == operations.QueryRead || input.Request.Kind == operations.MetadataInspect || input.Request.Kind == operations.NativeRead || input.Request.Kind == operations.WatchRead || input.Request.Kind == operations.IngestionState || input.Request.Kind == operations.IngestionCommit || input.Request.Kind == operations.MigrationStatus || input.Request.Kind == operations.MigrationApply || (input.Request.Kind == operations.NativeExecute && input.Request.Spec.Native != nil && input.Request.Spec.Native.ReturnResult)
	fileResult := input.SourceFile != nil && input.Request.Kind == operations.StatementExecute
	if input.ValidateAt(time.Now()) != nil || input.Limits.MaxRows > e.Limits.MaxRows || input.Limits.MaxBytes > e.Limits.MaxBytes || input.Limits.TimeoutMS > e.Limits.Timeout.Milliseconds() || input.Limits.MemoryMB > e.Limits.MemoryMB || input.Limits.Threads > e.Limits.Threads || input.Limits.MaxTempMB > e.Limits.MaxTempMB || (arrowResult && sink == nil) {
		return rejectedOperation(input, "INVALID_ARGUMENT"), operationFailure("INVALID_ARGUMENT")
	}
	var sourceFile *os.File
	var sourceIdentity operationBinaryIdentity
	if input.SourceFile != nil {
		var until int64
		sourceFile, sourceIdentity, until, err = prepareOperationSourceFile(ctx, workspace, input, int64(e.Limits.MaxTempMB)<<20)
		if err != nil {
			return rejectedOperation(input, "UNAVAILABLE"), operationFailure("UNAVAILABLE")
		}
		defer sourceFile.Close()
		// Keep the snapshot inode outside the child's writable directory tree.
		operationDirectory = filepath.Join(workspace.path, "run")
		if os.Mkdir(operationDirectory, 0700) != nil {
			return rejectedOperation(input, "UNAVAILABLE"), operationFailure("UNAVAILABLE")
		}
		for i := 0; i+1 < len(args); i++ {
			if args[i] == "--write" {
				args[i+1] = operationDirectory
				break
			}
		}
		input.CredentialsValidUntil = min(until, input.ExpiresAt)
		if input.ValidateAt(time.Now()) != nil {
			return rejectedOperation(input, "UNAVAILABLE"), operationFailure("UNAVAILABLE")
		}
	}
	deadline := time.Unix(input.ExpiresAt, 0)
	if until := time.Now().Add(time.Duration(input.Limits.TimeoutMS) * time.Millisecond); until.Before(deadline) {
		deadline = until
	}
	runCtx, stopRun := context.WithDeadline(ctx, deadline)
	defer stopRun()
	ctx = runCtx
	payload, err := adapter.EncodeProcessRequest(input)
	if err != nil {
		return rejectedOperation(input, "INVALID_ARGUMENT"), operationFailure("INVALID_ARGUMENT")
	}
	defer clear(payload)
	command := exec.CommandContext(ctx, e.SandboxPath, args...)
	configureProcess(command)
	command.Dir = operationDirectory
	command.Env = operationEnvironment(operationDirectory, e.Limits.Threads)
	command.Stderr = io.Discard
	command.WaitDelay = 3 * time.Second
	if workspace.lease == nil {
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("CONFIGURATION_ERROR")
	}
	readReceipt, writeReceipt, err := os.Pipe()
	if err != nil {
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("UNAVAILABLE")
	}
	defer readReceipt.Close()
	defer writeReceipt.Close()
	command.ExtraFiles = []*os.File{writeReceipt}
	workspace.attach(command)
	command.ExtraFiles = append(command.ExtraFiles, binary) // fd3 receipt, fd4 scratch lease, fd5 executable.
	if sourceFile != nil {
		command.ExtraFiles = append(command.ExtraFiles, sourceFile)
	} // fd6 immutable source snapshot.
	if jdbcFiles != nil {
		command.ExtraFiles = append(command.ExtraFiles, nil)          // fd6 reserved for snapshots.
		command.ExtraFiles = append(command.ExtraFiles, jdbcFiles...) // fd7 Java, fd8+ JARs, final JRE directory.
	}
	command.Stdin = &operationPayloadReader{ctx: ctx, raw: payload}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("UNAVAILABLE")
	}
	defer stdout.Close()
	if err := operationBinaryCurrent(binary, identity); err != nil || (sourceFile != nil && operationBinaryCurrent(sourceFile, sourceIdentity) != nil) || (jdbcFiles != nil && jdbc.current() != nil) || ctx.Err() != nil || input.CredentialsValidUntil <= time.Now().Unix() {
		if child, ok := command.Stdout.(*os.File); ok {
			_ = child.Close()
		}
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("UNAVAILABLE")
	}
	if err := job.Start(command); err != nil {
		if child, ok := command.Stdout.(*os.File); ok {
			_ = child.Close()
		}
		return rejectedOperation(input, "UNAVAILABLE"), operationFailure("UNAVAILABLE")
	}
	// Past Start, unknown source effects must never become a no-effect error.
	receipt = uncertainOperation(input)
	_ = writeReceipt.Close()
	received := make(chan operationReceiptRead, 1)
	go func() {
		read := readOperationReceipt(readReceipt)
		if read.err != nil {
			cancel()
		}
		received <- read
	}()
	stopClose := context.AfterFunc(ctx, func() { _ = stdout.Close() })
	defer stopClose()
	var readErr error
	var observed query.Stats
	hasher := sha256.New()
	var candidate *os.File
	var candidateBytes int64
	if fileResult {
		candidate, readErr = os.CreateTemp(workspace.path, ".file-candidate-")
		if readErr == nil {
			defer candidate.Close()
			defer os.Remove(candidate.Name())
			maximum := min(int64(50<<20), int64(e.Limits.MaxTempMB)<<20)
			candidateBytes, readErr = io.CopyBuffer(io.MultiWriter(candidate, hasher), io.LimitReader(stdout, maximum+1), make([]byte, 64<<10))
			if candidateBytes > maximum {
				readErr = operationFailure("RESOURCE_EXHAUSTED")
			}
		}
	} else if arrowResult {
		limits := e.Limits
		limits.MaxRows = input.Limits.MaxRows
		limits.MaxBytes = input.Limits.MaxBytes
		observed, readErr = readWorkerIPC(ctx, io.TeeReader(stdout, hasher), limits, sink)
	} else {
		var extra [1]byte
		n, err := stdout.Read(extra[:])
		if n != 0 || !errors.Is(err, io.EOF) {
			readErr = operationFailure("QUERY_FAILED")
		}
	}
	if readErr != nil {
		cancel()
	}
	// Keep the leader unreaped until its descendants receive final SIGKILL.
	cleanupErr := finishProcess(command, ctx.Err() != nil)
	waitErr := command.Wait()
	// A receipt reader cannot hold admission indefinitely if a descendant kept
	// fd3 open despite process-group cleanup. Cgroup cleanup runs in the defer.
	_ = readReceipt.SetReadDeadline(time.Now().Add(3 * time.Second))
	var read operationReceiptRead
	select {
	case read = <-received:
	case <-time.After(3 * time.Second):
		_ = readReceipt.Close()
		read = <-received
	}
	defer clear(read.raw)
	if errors.Is(cleanupErr, os.ErrProcessDone) {
		cleanupErr = nil
	}
	if readErr != nil || waitErr != nil || cleanupErr != nil || read.err != nil || ctx.Err() != nil {
		resultErr = operationFailure("QUERY_FAILED")
	}
	if fileResult {
		return finishFileOperation(ctx, input, read, candidate, candidateBytes, hex.EncodeToString(hasher.Sum(nil)), readErr == nil && waitErr == nil && cleanupErr == nil && ctx.Err() == nil)
	}
	receipt, receiptErr := operationProcessReceipt(input, read, arrowResult, observed, hex.EncodeToString(hasher.Sum(nil)), readErr == nil && waitErr == nil && ctx.Err() == nil)
	if receiptErr != nil {
		resultErr = operationFailure("QUERY_FAILED")
	}
	return receipt, resultErr
}

func operationEnvironment(directory string, threads int) []string {
	return []string{"PATH=/usr/bin:/bin", "HOME=" + directory, "TMPDIR=" + directory, "GOMAXPROCS=" + fmt.Sprint(threads), "KELVO_OPERATION_PROCESS=1"}
}

type operationPayloadReader struct {
	ctx    context.Context
	raw    []byte
	offset int
}

func (r *operationPayloadReader) Read(out []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.offset == len(r.raw) {
		return 0, io.EOF
	}
	n := copy(out, r.raw[r.offset:])
	r.offset += n
	return n, nil
}

type operationReceiptRead struct {
	raw []byte
	err error
}

func readOperationReceipt(reader io.Reader) operationReceiptRead {
	raw, err := io.ReadAll(io.LimitReader(reader, operations.MaxReceiptBytes+1))
	if len(raw) > operations.MaxReceiptBytes {
		clear(raw)
		return operationReceiptRead{err: operationFailure("QUERY_FAILED")}
	}
	return operationReceiptRead{raw: raw, err: err}
}

func operationProcessReceipt(input adapter.ProcessRequest, read operationReceiptRead, arrowResult bool, observed query.Stats, wireDigest string, delivered bool) (operations.Receipt, error) {
	unknown := uncertainOperation(input)
	var receipt operations.Receipt
	if read.err != nil || operations.DecodeStrict(read.raw, &receipt, operations.MaxReceiptBytes) != nil || receipt.Validate() != nil || receipt.Version != operations.Version || receipt.OperationID != input.OperationID || receipt.RequestSHA256 != input.RequestSHA256 ||
		(!input.Request.Kind.Mutating() && receipt.Effect != operations.EffectNone) || (input.Request.Kind.Mutating() && receipt.Outcome == operations.Completed && receipt.Effect != operations.EffectCommitted) || operations.ValidateStatementReceipt(input.Request, receipt) != nil {
		return unknown, operationFailure("QUERY_FAILED")
	}
	// The durable operation is already running, but the adapter can still prove
	// it stopped before the selected database operation was dispatched.
	if receipt.Outcome == operations.CancelledBeforeStart {
		receipt.Outcome = operations.Rejected
	}
	if receipt.Outcome == operations.Completed && arrowResult {
		ref := receipt.Result
		if !delivered || ref == nil || ref.ID != input.OperationID || ref.Format != "arrow_ipc" || ref.Rows != observed.Rows || ref.Bytes != observed.WireBytes || ref.SHA256 != wireDigest {
			if input.Request.Kind.Mutating() && receipt.Effect == operations.EffectCommitted {
				receipt.Result = nil
				return receipt, operationFailure("QUERY_FAILED")
			}
			return unknown, operationFailure("QUERY_FAILED")
		}
	} else if receipt.Result != nil {
		// Preserve positive mutation evidence while discarding an unowned result.
		if input.Request.Kind.Mutating() {
			receipt.Result = nil
			return receipt, operationFailure("QUERY_FAILED")
		}
		return unknown, operationFailure("QUERY_FAILED")
	}
	return receipt, nil
}

func validateOperationLauncher(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return operationFailure("CONFIGURATION_ERROR")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return operationFailure("CONFIGURATION_ERROR")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0111 == 0 || info.Mode()&0022 != 0 {
		return operationFailure("CONFIGURATION_ERROR")
	}
	return nil
}
