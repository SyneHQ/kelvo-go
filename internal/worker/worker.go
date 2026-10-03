// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package worker executes each query in a disposable subprocess. This is process
// lifetime isolation, not an OS filesystem/network sandbox.
package worker

import (
	"bytes"
	"context"
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
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

type Input struct {
	Config  catalog.Config `json:"config"`
	Limits  query.Limits   `json:"limits"`
	Request query.Request  `json:"request"`
}
type Outcome struct {
	Stats query.Stats  `json:"stats"`
	Error *query.Error `json:"error,omitempty"`
}
type Executor struct {
	Tracing     *tracing.Recorder
	Config      catalog.Config
	Limits      query.Limits
	Binary      string
	SandboxPath string
	// ResourcePool must be shared by query and refresh executors on this process.
	ResourcePool *admission.Pool
	// ResourceOverheadBytes reserves memory beyond DuckDB's managed memory limit.
	ResourceOverheadBytes int64
	Metrics               *telemetry.Registry
	SourceAdmission       SourceAdmitter
	Secrets               SecretResolver
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
	parent := ctx
	start := time.Now()
	var admissionWait time.Duration
	defer recordExecution(e.Metrics, ctx, start, &admissionWait, &resultErr, e.Tracing)
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
	ctx, cancel := context.WithTimeout(ctx, e.Limits.Timeout)
	defer cancel()
	if e.ResourcePool != nil {
		memoryBytes := int64(e.Limits.MemoryMB) << 20
		if e.ResourceOverheadBytes < 0 || e.ResourceOverheadBytes > math.MaxInt64-memoryBytes {
			return stats, admission.ErrInvalid
		}
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
		defer reservation.Release()
	}
	quotaStarted := time.Now()
	quotaCtx, releaseQuota, quotaErr := e.acquireSourceQuota(ctx, r)
	admissionWait += time.Since(quotaStarted)
	if quotaErr != nil {
		return stats, quotaErr
	}
	defer releaseQuota()
	ctx = quotaCtx
	sources, versions, release, err := acceleration.Resolve(ctx, e.Config, r)
	if err != nil {
		return stats, err
	}
	defer release()
	dir, err := os.MkdirTemp("", "kelvo-worker-")
	if err != nil {
		return stats, err
	}
	defer os.RemoveAll(dir)
	cfg := catalog.Config{Sources: sources, ExtensionDirectory: e.Config.ExtensionDirectory}
	payload, err := json.Marshal(Input{cfg, e.Limits, r})
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
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return stats, err
	}
	if err = cmd.Start(); err != nil {
		return stats, err
	}
	stopClose := context.AfterFunc(ctx, func() { _ = stdout.Close() })
	defer stopClose()
	observed, readErr := readWorkerIPC(ctx, stdout, e.Limits, sink)
	if readErr != nil {
		cancel()
	}
	// On Linux the process-group ID stays pinned by the unreaped child. Kill
	// any descendants before Wait reaps it, including children that closed the
	// output pipe and would otherwise survive a successful leader exit.
	cleanupErr := finishProcess(cmd, ctx.Err() != nil)
	waitErr := cmd.Wait()
	var outcome Outcome
	decodeErr := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &outcome)
	stats = outcome.Stats
	stats.Accelerations = versions
	stats.Rows, stats.Bytes, stats.Batches, stats.WireBytes = observed.Rows, observed.Bytes, observed.Batches, observed.WireBytes
	stats.DurationNS = time.Since(start).Nanoseconds()
	if parent.Err() != nil {
		return stats, parent.Err()
	}
	if ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return stats, ctx.Err()
	}
	if readErr != nil {
		if decodeErr == nil && outcome.Error != nil {
			return stats, outcome.Error
		}
		if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) {
			return stats, readErr
		}
		// Preserve typed sink limits. Other subprocess/IPC errors stay sanitized.
		var qe *query.Error
		if errors.As(readErr, &qe) {
			return stats, qe
		}
		return stats, query.NewError("QUERY_FAILED", "Query worker did not complete an Arrow result")
	}
	if waitErr != nil || decodeErr != nil || (cleanupErr != nil && !errors.Is(cleanupErr, os.ErrProcessDone)) {
		return stats, query.NewError("QUERY_FAILED", "Query worker failed")
	}
	if outcome.Error != nil {
		return stats, outcome.Error
	}
	return stats, nil
}

// The parent retains cloud reader and writer credentials. Query children only
// receive a range capability with no provider identity or upstream object URL.
func sourceEnvironmentNames(source catalog.Source) ([]string, error) {
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
