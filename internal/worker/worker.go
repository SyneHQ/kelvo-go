// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package worker executes each query in a disposable subprocess. This is process
// lifetime isolation, not an OS filesystem/network sandbox.
package worker

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

type Input struct {
	Access  *access.Policy `json:"access,omitempty"`
	Config  catalog.Config `json:"config"`
	Limits  query.Limits   `json:"limits"`
	Request query.Request  `json:"request"`
	// Parent and child use the same executable. Older strict child decoders do
	// not accept this field; it is not an independent rolling wire protocol.
	TimingVersion uint8 `json:"timing_version,omitempty"`
}

// ExecutionContext validates the trusted envelope before any child engine is
// opened. Parent checks are repeated because worker stdin is a separate boundary.
func (in Input) ExecutionContext(ctx context.Context) (context.Context, error) {
	if in.Access != nil {
		var err error
		ctx, err = access.WithPolicy(ctx, *in.Access)
		if err != nil {
			return nil, err
		}
	}
	if err := access.ValidateResolvedRequest(ctx, in.Config, in.Request); err != nil {
		return nil, err
	}
	return ctx, nil
}

type Outcome struct {
	Stats           query.Stats            `json:"stats"`
	Error           *query.Error           `json:"error,omitempty"`
	Timing          *telemetry.ChildTiming `json:"timing,omitempty"`
	timingMalformed bool
}
type Executor struct {
	// Bound definitions are private; exported Config is a detached inspection
	// copy and cannot retarget queries or copied export executors after binding.
	catalogBinding *boundCatalog
	// ObjectRuntime is parent-only and shared by copies, including exports.
	ObjectRuntime     *acceleration.ObjectRuntime
	Containment       *containment.Manager
	ContainmentBudget containment.Budget
	Tracing           *tracing.Recorder
	Config            catalog.Config
	Limits            query.Limits
	Binary            string
	SandboxPath       string
	// ScratchRoot optionally owns private crash-recoverable query workspaces.
	ScratchRoot *ScratchRoot
	// ResourcePool must be shared by query and refresh executors on this process.
	ResourcePool *admission.Pool
	// ResourceOverheadBytes reserves memory beyond DuckDB's managed memory limit.
	ResourceOverheadBytes int64
	Metrics               *telemetry.Registry
	SourceHealth          *telemetry.SourceHealth
	SourceAdmission       SourceAdmitter
	Secrets               SecretResolver
}

type boundCatalog struct {
	config catalog.Config
	digest string
}

type catalogAuthorityKey struct{}

// WithCatalogAuthority carries authenticated execution authority independently
// of row policies. Empty explicitly means legacy-unbound; absence is reserved
// for trusted operator work such as a worker's startup probe.
func WithCatalogAuthority(ctx context.Context, expected string) (context.Context, error) {
	if ctx == nil || (expected != "" && !validCatalogFingerprint(expected)) {
		return nil, query.NewError("CONFIGURATION_ERROR", "Invalid catalog authority")
	}
	return context.WithValue(ctx, catalogAuthorityKey{}, expected), nil
}

func validCatalogFingerprint(expected string) bool {
	if len(expected) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(expected)
	return err == nil && hex.EncodeToString(decoded) == expected
}

// WithCatalogBinding returns a separate executor pinned to approved operator
// definitions. Rebinding checks the effective private catalog, never a caller's
// subsequently mutated public Config. Source secrets are not resolved here.
func (e *Executor) WithCatalogBinding(expected string) (*Executor, error) {
	bad := query.NewError("CONFIGURATION_ERROR", "Worker catalog authority does not match")
	if e == nil || !validCatalogFingerprint(expected) {
		return nil, bad
	}
	config := e.Config
	if e.catalogBinding != nil {
		if e.catalogBinding.digest != expected {
			return nil, bad
		}
		config = e.catalogBinding.config
	}
	private, digest, err := catalog.AuthoritySnapshot(config)
	if err != nil || digest != expected {
		return nil, bad
	}
	public, _, err := catalog.AuthoritySnapshot(private)
	if err != nil {
		return nil, bad
	}
	bound := *e
	bound.Config = public
	bound.catalogBinding = &boundCatalog{config: private, digest: digest}
	return &bound, nil
}

func New(c catalog.Config, l query.Limits) (*Executor, error) {
	exe, e := os.Executable()
	if e != nil {
		return nil, e
	}
	return &Executor{Config: c, Limits: l, Binary: exe}, nil
}

// Fits bounded snapshot and federation statistics together; arbitrary stderr
// still cannot grow with query data or an unbounded native error message.
const maxOutcomeBytes = 64 << 10

type boundedBuffer struct{ bytes.Buffer }

func (b *boundedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	left := maxOutcomeBytes - b.Len()
	if left > 0 {
		if len(p) > left {
			p = p[:left]
		}
		_, _ = b.Buffer.Write(p)
	}
	return n, nil
}

func (e *Executor) Execute(ctx context.Context, r query.Request, sink query.Sink) (stats query.Stats, resultErr error) {
	if expected, present := ctx.Value(catalogAuthorityKey{}).(string); present {
		actual := ""
		if e.catalogBinding != nil {
			actual = e.catalogBinding.digest
		}
		if actual != expected {
			return stats, query.NewError("PERMISSION_DENIED", "Query catalog authority does not match")
		}
	}
	if e.catalogBinding != nil {
		bound := *e
		bound.Config = e.catalogBinding.config
		return bound.execute(ctx, r, sink)
	}
	return e.execute(ctx, r, sink)
}

func (e *Executor) execute(ctx context.Context, r query.Request, sink query.Sink) (stats query.Stats, resultErr error) {
	parent := ctx
	start := time.Now()
	var admissionWait time.Duration
	phases := newExecutionPhases(start, e.Metrics != nil || e.Tracing != nil)
	defer recordExecutionPhases(e.Metrics, ctx, start, &admissionWait, &resultErr, &phases, e.Tracing)
	if err := e.Limits.Validate(); err != nil {
		return stats, err
	}
	if sink == nil {
		return stats, query.NewError("INVALID_ARGUMENT", "Result sink is required")
	}
	if r.Mode == "" {
		r.Mode = "federated"
	}
	if err := query.ValidateRequest(r); err != nil {
		return stats, err
	}
	if err := access.ValidateRequest(ctx, e.Config, r); err != nil {
		return stats, err
	}
	if err := e.ValidateObjectRuntime(); err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(ctx, e.Limits.Timeout)
	defer cancel()

	custody := containment.FromContext(ctx)
	if e.Containment != nil {
		if e.Containment.Err() != nil {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Worker process containment is unavailable")
		}
		if !e.ContainmentBudget.FitsOverhead(int64(e.Limits.MemoryMB), e.ResourceOverheadBytes>>20) || e.SandboxPath == "" || e.ScratchRoot == nil || (e.ResourcePool == nil && custody == nil) {
			return stats, query.NewError("CONFIGURATION_ERROR", "Contained workers require sandbox, managed scratch, and separate native and parent reservations")
		}
	}
	if custody != nil && e.ResourcePool != nil {
		return stats, admission.ErrInvalid
	}
	if e.ResourcePool != nil {
		memoryBytes := int64(e.Limits.MemoryMB) << 20
		if e.ResourceOverheadBytes < 0 || e.ResourceOverheadBytes > math.MaxInt64-memoryBytes {
			return stats, admission.ErrInvalid
		}
		phases.enter(telemetry.PhaseNodeAdmission)
		waitStart := time.Now()
		reservation, err := e.ResourcePool.Acquire(ctx, admission.Request{
			MemoryBytes:  memoryBytes + e.ResourceOverheadBytes,
			ScratchBytes: int64(e.Limits.MaxTempMB) << 20,
		})
		admissionWait = time.Since(waitStart)
		if err != nil {
			return stats, err
		}
		// Registered before snapshot leases, temp files and child cleanup so those
		// resources are released before a competing job can take this reservation.
		custody, _ = containment.NewCustody(reservation.Release)
		defer custody.Complete()
	}
	executionCtx := ctx // retain the deadline even if a quota child cancels first
	if e.SourceAdmission != nil {
		phases.enter(telemetry.PhaseSourceAdmission)
	}
	quotaStarted := time.Now()
	quotaCtx, releaseQuota, quotaErr := e.acquireSourceQuota(ctx, r)
	admissionWait += time.Since(quotaStarted)
	if quotaErr != nil {
		return stats, quotaErr
	}
	defer releaseQuota()
	ctx = quotaCtx
	phases.enter(telemetry.PhasePrepare)
	resolution, err := acceleration.ResolveWithRuntime(ctx, e.Config, r, e.ObjectRuntime)
	if err != nil {
		return stats, err
	}
	defer func() {
		cleanup, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCleanup()
		if err := resolution.Close(cleanup); err != nil {
			resultErr = errors.Join(resultErr, query.NewError("DATASET_UNAVAILABLE", "Snapshot ownership or cleanup did not complete"))
		}
	}()
	ctx = resolution.Context()
	sources, versions := resolution.Sources, resolution.Versions
	cfg := catalog.Config{Sources: sources, ExtensionDirectory: e.Config.ExtensionDirectory}
	if err := access.ValidateResolvedRequest(ctx, cfg, r); err != nil {
		return stats, err
	}
	snapshotScratch, err := resolution.HoldConsumer()
	if err != nil {
		return stats, err
	}
	workspace, err := newScratchWorkspace(e.ScratchRoot)
	if err != nil {
		snapshotScratch() // No child or snapshot data was admitted to this workspace.
		return stats, query.NewError("RESOURCE_EXHAUSTED", "Worker scratch workspace is unavailable")
	}
	dir := workspace.path
	releaseScratch := func() {}
	if e.Containment != nil {
		releaseScratch, err = custody.Hold()
		if err != nil {
			if cleanupErr := workspace.cleanup(); cleanupErr == nil {
				snapshotScratch()
			} else {
				e.Containment.QuarantineOperation()
				reportScratchCleanupFailure(cleanupErr)
			}
			return stats, err
		}
	}
	defer func() {
		if err := workspace.cleanup(); err != nil {
			if e.Containment != nil {
				e.Containment.QuarantineOperation()
			}
			reportScratchCleanupFailure(err)
			if resultErr == nil {
				resultErr = query.NewError("RESOURCE_EXHAUSTED", "Worker scratch cleanup failed")
			}
			return // Retain scratch custody while owned files remain uncertain.
		}
		releaseScratch()
		snapshotScratch()
	}()
	input := Input{Config: cfg, Limits: e.Limits, Request: r}
	if e.Metrics != nil {
		input.TimingVersion = telemetry.ChildTimingVersion
	}
	if policy, restricted := access.PolicyFromContext(ctx); restricted {
		input.Access = &policy
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return stats, err
	}
	command, args := e.Binary, []string{"worker"}
	if e.SandboxPath != "" {
		args, err = SandboxCommand(e.Binary, dir, cfg, e.Limits)
		if err != nil {
			return stats, err
		}
		command = e.SandboxPath
	}
	cmd := exec.CommandContext(ctx, command, args...)
	configureProcess(cmd)
	workspace.attach(cmd)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "TMPDIR=" + dir, "GOMAXPROCS=" + fmt.Sprint(e.Limits.Threads)}
	// Only explicitly configured secrets enter the query process.
	seen := map[string]bool{}
	for _, s := range sources {
		names, err := sourceEnvironmentNames(s)
		if err != nil {
			return stats, err
		}
		for _, key := range names {
			if key != "" && !seen[key] {
				if err := catalog.ValidateEnvironment(key); err != nil {
					return stats, query.NewError("CONFIGURATION_ERROR", "Source environment reference is not permitted")
				}
				v, ok, secretErr := e.resolveSecret(ctx, key)
				if secretErr != nil {
					return stats, secretErr
				}
				if !ok {
					return stats, query.NewError("CONFIGURATION_ERROR", "A source environment variable is missing")
				}
				cmd.Env = append(cmd.Env, key+"="+v)
				seen[key] = true
			}
		}
	}
	cmd.Stdin = bytes.NewReader(payload)
	var stderr boundedBuffer
	cmd.Stderr = &stderr
	cmd.WaitDelay = 3 * time.Second
	var processJob *containment.Job
	processFinished := false
	if e.Containment != nil {
		processLimits, prepErr := e.ContainmentBudget.ProcessLimits(int64(e.Limits.MemoryMB), e.Limits.Threads)
		if prepErr != nil {
			return stats, prepErr
		}
		token, prepErr := custody.Hold()
		if prepErr != nil {
			return stats, prepErr
		}
		snapshotProcess, prepErr := resolution.HoldConsumer()
		if prepErr != nil {
			token()
			return stats, prepErr
		}
		processJob, prepErr = e.Containment.Prepare(processLimits, func() {
			token()
			snapshotProcess()
		})
		if prepErr != nil {
			token()
			snapshotProcess() // Prepare never admits a child when it returns an error.
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Worker process containment is unavailable")
		}
		defer func() {
			if !processFinished {
				if _, err := processJob.Finish(context.Background()); err != nil {
					resultErr = query.NewError("RESOURCE_EXHAUSTED", "Worker process cleanup is uncertain")
				}
			}
		}()
	}
	// Allocate pipes only once every fallible preparation step is complete.
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return stats, err
	}
	phases.enter(telemetry.PhaseExecutionDelivery)
	var childStarted time.Time
	if e.Metrics != nil {
		childStarted = time.Now()
	}
	if processJob != nil {
		err = processJob.Start(cmd)
	} else {
		err = cmd.Start()
	}
	if err != nil {
		_ = stdout.Close()
		if childPipe, ok := cmd.Stdout.(*os.File); ok {
			_ = childPipe.Close()
		}
		return stats, err
	}
	stopClose := context.AfterFunc(ctx, func() { _ = stdout.Close() })
	defer stopClose()
	observed, readErr := readWorkerIPC(ctx, stdout, e.Limits, phases.sink(sink))
	phases.enter(telemetry.PhaseCleanup)
	if readErr != nil {
		cancel()
	}
	// On Linux the process-group ID stays pinned by the unreaped child. Kill
	// any descendants before Wait reaps it, including children that closed the
	// output pipe and would otherwise survive a successful leader exit.
	cleanupErr := finishProcess(cmd, ctx.Err() != nil)
	waitErr := cmd.Wait()
	var childBound time.Duration
	if e.Metrics != nil {
		childBound = time.Since(childStarted)
	}
	if processJob != nil {
		_, containedErr := processJob.Finish(context.Background())
		processFinished = true
		if containedErr != nil {
			cleanupErr = containedErr
		}
	}
	var outcome Outcome
	decodeErr := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &outcome)
	if e.Metrics != nil {
		terminated := cmd.ProcessState != nil && cmd.ProcessState.ExitCode() == -1
		recordChildTiming(e.Metrics, ctx, outcome, decodeErr, terminated, childBound)
	}
	stats = outcome.Stats
	stats.Accelerations = versions
	stats.Rows, stats.Bytes, stats.Batches, stats.WireBytes = observed.Rows, observed.Bytes, observed.Batches, observed.WireBytes
	stats.DurationNS = time.Since(start).Nanoseconds()
	resultErr = workerResultError(parent, executionCtx, ctx, readErr, waitErr, decodeErr, cleanupErr, outcome.Error)
	recordSourceHealth(e.SourceHealth, r, outcome.Error, resultErr, decodeErr == nil)
	return stats, resultErr
}

// ValidateObjectRuntime rejects an uncontained or retargeted protected catalog
// before snapshot I/O. Standalone callers must supply the same node resources.
func (e *Executor) ValidateObjectRuntime() error {
	if !acceleration.ProtectedObjects(e.Config) {
		return nil
	}
	if e.ObjectRuntime == nil || e.Containment == nil || e.SandboxPath == "" || e.ScratchRoot == nil {
		return query.NewError("CONFIGURATION_ERROR", "Protected snapshots require a contained node with managed scratch and an object runtime")
	}
	if err := e.ObjectRuntime.Match(e.Config); err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Protected snapshot runtime does not match the worker catalog")
	}
	return nil
}

// Decide success only after the subprocess and its descendants are cleaned up.
// A source lease can be lost after the final IPC context check; its child
// context does not cancel the original caller or an enclosing refresh writer.
func workerResultError(parent, executionCtx, ctx context.Context, readErr, waitErr, decodeErr, cleanupErr error, outcomeError *query.Error) error {
	if parent.Err() != nil {
		return parent.Err()
	}
	if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
		return executionCtx.Err()
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ctx.Err()
	}
	if readErr != nil {
		if decodeErr == nil && outcomeError != nil {
			return outcomeError
		}
		if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) {
			return readErr
		}
		// Preserve typed sink limits. Other subprocess/IPC errors stay sanitized.
		var qe *query.Error
		if errors.As(readErr, &qe) {
			return qe
		}
		return query.NewError("QUERY_FAILED", "Query worker did not complete an Arrow result")
	}
	if waitErr != nil || decodeErr != nil || (cleanupErr != nil && !errors.Is(cleanupErr, os.ErrProcessDone)) {
		return query.NewError("QUERY_FAILED", "Query worker failed")
	}
	if outcomeError != nil {
		return outcomeError
	}
	if ctx.Err() != nil {
		// Local read failures have already returned above. Do not expose an
		// arbitrary coordination cause or mistake lost ownership for success.
		return query.NewError("UNAVAILABLE", "Query source admission ended before completion")
	}
	return nil
}

// The parent retains cloud reader and writer credentials. Query children only
// receive a range capability with no provider identity or upstream object URL.
func sourceEnvironmentNames(source catalog.Source) ([]string, error) {
	if err := source.ValidateObjectSnapshot(); err != nil {
		return nil, query.NewError("CONFIGURATION_ERROR", "Invalid isolated object snapshot")
	}
	if err := source.ValidateObjectRanges(); err != nil {
		return nil, query.NewError("CONFIGURATION_ERROR", "Invalid isolated object range capabilities")
	}
	if source.Ranges != nil {
		return nil, nil
	}
	if source.Object != nil {
		return nil, query.NewError("CONFIGURATION_ERROR", "Cloud object credentials cannot enter the query worker")
	}
	if source.Range == nil {
		return []string{source.DSNEnv, source.URLEnv, source.UsernameEnv, source.PasswordEnv, source.TokenEnv}, nil
	}
	if source.Range.Validate() != nil || source.Type != "parquet" || source.Path != source.Range.URL ||
		source.Federation != nil || source.Adapter != "" || source.DSNEnv != "" || source.URLEnv != "" || source.UsernameEnv != "" ||
		source.PasswordEnv != "" || source.TokenEnv != "" || len(source.Options) != 0 {
		return nil, query.NewError("CONFIGURATION_ERROR", "Invalid isolated object range capability")
	}
	return nil, nil
}

// IPCSink borrows each batch until Write returns. Finish emits EOS only on success.
type IPCSink struct {
	out          io.Writer
	writer       *ipc.Writer
	limits       query.Limits
	rows         int64
	decodedBytes int64
	counter      *countWriter
	err          error
	closed       bool
}

func NewIPCSink(out io.Writer, l query.Limits) *IPCSink { return &IPCSink{out: out, limits: l} }
func (s *IPCSink) Schema(schema *arrow.Schema) error {
	if s.err != nil {
		return s.err
	}
	if s.closed {
		return query.NewError("INTERNAL", "Result stream is closed")
	}
	if schema == nil {
		return s.fail(query.NewError("INTERNAL", "Missing result schema"))
	}
	if s.writer != nil {
		return s.fail(query.NewError("INTERNAL", "Result schema repeated"))
	}
	options, err := query.ResultIPCOptions(s.limits.ResultCompression)
	if err != nil {
		return s.fail(err)
	}
	s.counter = &countWriter{w: s.out, max: s.limits.MaxBytes}
	s.writer = ipc.NewWriter(s.counter, append(options, ipc.WithSchema(schema))...)
	return nil
}
func (s *IPCSink) Write(b arrow.RecordBatch) error {
	if s.err != nil {
		return s.err
	}
	if s.closed {
		return query.NewError("INTERNAL", "Result stream is closed")
	}
	if s.writer == nil {
		return s.fail(query.NewError("INTERNAL", "Missing result schema"))
	}
	if b == nil || b.NumRows() < 0 {
		return s.fail(query.NewError("INTERNAL", "Invalid result batch"))
	}
	if b.NumRows() > s.limits.MaxRows-s.rows {
		return s.fail(query.NewError("RESOURCE_EXHAUSTED", "Result row limit exceeded"))
	}
	// Bound decoded buffers before compression; a small encoded result must not
	// allow a highly compressible batch to bypass the logical result byte cap.
	size := arrowutil.TotalRecordSize(b)
	if size < 0 || size > s.limits.MaxBytes-s.decodedBytes {
		return s.fail(query.NewError("RESOURCE_EXHAUSTED", "Result byte limit exceeded"))
	}
	if err := s.writer.Write(b); err != nil {
		return s.fail(err)
	}
	s.rows += b.NumRows()
	s.decodedBytes += size
	return nil
}
func (s *IPCSink) Finish() error {
	if s.err != nil {
		return s.err
	}
	if s.closed {
		return nil
	}
	if s.writer == nil {
		return s.fail(query.NewError("INTERNAL", "Missing result schema"))
	}
	if err := s.writer.Close(); err != nil {
		return s.fail(err)
	}
	s.closed = true
	return nil
}

// Abort releases writer state after an executor fails outside a sink method.
// It never emits EOS and is safe to defer alongside a successful Finish.
func (s *IPCSink) Abort() {
	if s.closed {
		return
	}
	_ = s.fail(query.NewError("CANCELLED", "Result stream aborted"))
}

func (s *IPCSink) fail(err error) error {
	if s.err == nil {
		s.err = err
		if !s.closed && s.writer != nil {
			// Release Arrow's retained dictionaries without publishing EOS or
			// counting bytes from cleanup after an incomplete result.
			s.counter.discard = true
			_ = s.writer.Close()
		}
		s.closed = true
	}
	return s.err
}
func (s *IPCSink) EncodedBytes() int64 {
	if s.counter == nil {
		return 0
	}
	return s.counter.n
}

type countWriter struct {
	w       io.Writer
	max, n  int64
	discard bool
}

func (w *countWriter) Write(p []byte) (int, error) {
	if w.discard {
		return len(p), nil
	}
	if int64(len(p)) > w.max-w.n {
		return 0, query.NewError("RESOURCE_EXHAUSTED", "Encoded result byte limit exceeded")
	}
	n, e := w.w.Write(p)
	w.n += int64(n)
	return n, e
}

// ResolveOutput prevents an incomplete query from replacing an existing export.
func ResolveOutput(path string) (*os.File, string, error) {
	dir := filepath.Dir(path)
	f, e := os.CreateTemp(dir, ".kelvo-result-*")
	if e != nil {
		return nil, "", e
	}
	return f, f.Name(), nil
}
