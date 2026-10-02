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
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
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
	Config      catalog.Config
	Limits      query.Limits
	Binary      string
	SandboxPath string
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

func (e *Executor) Execute(ctx context.Context, r query.Request, sink query.Sink) (query.Stats, error) {
	parent := ctx
	start := time.Now()
	var stats query.Stats
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
				v, ok := os.LookupEnv(key)
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
	out     io.Writer
	writer  *ipc.Writer
	limits  query.Limits
	rows    int64
	counter *countWriter
}

func NewIPCSink(out io.Writer, l query.Limits) *IPCSink { return &IPCSink{out: out, limits: l} }
func (s *IPCSink) Schema(schema *arrow.Schema) error {
	if s.writer != nil {
		return query.NewError("INTERNAL", "Result schema repeated")
	}
	s.counter = &countWriter{w: s.out, max: s.limits.MaxBytes}
	s.writer = ipc.NewWriter(s.counter, ipc.WithSchema(schema))
	return nil
}
func (s *IPCSink) Write(b arrow.RecordBatch) error {
	if s.writer == nil {
		return query.NewError("INTERNAL", "Missing result schema")
	}
	if b.NumRows() > s.limits.MaxRows-s.rows {
		return query.NewError("RESOURCE_EXHAUSTED", "Result row limit exceeded")
	}
	s.rows += b.NumRows()
	return s.writer.Write(b)
}
func (s *IPCSink) Finish() error {
	if s.writer == nil {
		return query.NewError("INTERNAL", "Missing result schema")
	}
	return s.writer.Close()
}
func (s *IPCSink) EncodedBytes() int64 {
	if s.counter == nil {
		return 0
	}
	return s.counter.n
}

type countWriter struct {
	w      io.Writer
	max, n int64
}

func (w *countWriter) Write(p []byte) (int, error) {
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
