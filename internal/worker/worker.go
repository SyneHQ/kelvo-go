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
	Config catalog.Config
	Limits query.Limits
	Binary string
}

func New(c catalog.Config, l query.Limits) (*Executor, error) {
	exe, e := os.Executable()
	if e != nil {
		return nil, e
	}
	return &Executor{Config: c, Limits: l, Binary: exe}, nil
}

type boundedBuffer struct{ bytes.Buffer }

func (b *boundedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	left := 16384 - b.Len()
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
	ids := r.Sources
	if r.Mode == "native" {
		if r.ConnectionID == "" || len(r.Sources) > 0 {
			return stats, query.NewError("INVALID_ARGUMENT", "Native queries require one connection_id")
		}
		ids = []string{r.ConnectionID}
	} else if r.Mode != "" && r.Mode != "federated" {
		return stats, query.NewError("INVALID_ARGUMENT", "Unknown query mode")
	}
	sources, err := e.Config.Select(ids)
	if err != nil {
		return stats, query.NewError("INVALID_ARGUMENT", "Unknown or duplicate source")
	}
	if _, err = r.Values(); err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(ctx, e.Limits.Timeout)
	defer cancel()
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
	cmd := exec.CommandContext(ctx, e.Binary, "worker")
	configureProcess(cmd)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "TMPDIR=" + dir, "GOMAXPROCS=" + fmt.Sprint(e.Limits.Threads)}
	// Only explicitly configured secrets enter the query process.
	seen := map[string]bool{}
	for _, s := range sources {
		for _, key := range []string{s.DSNEnv, s.URLEnv, s.UsernameEnv, s.PasswordEnv} {
			if key != "" && !seen[key] {
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
	reader, readErr := ipc.NewReader(stdout)
	if readErr == nil {
		readErr = sink.Schema(reader.Schema())
		for readErr == nil && reader.Next() {
			if ctx.Err() != nil {
				readErr = ctx.Err()
				break
			}
			batch := reader.RecordBatch()
			readErr = sink.Write(batch)
		}
		if readErr == nil {
			readErr = reader.Err()
		}
		reader.Release()
	}
	if readErr != nil {
		cancel()
	}
	waitErr := cmd.Wait()
	var outcome Outcome
	decodeErr := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &outcome)
	stats = outcome.Stats
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
	if waitErr != nil || decodeErr != nil {
		return stats, query.NewError("QUERY_FAILED", "Query worker failed")
	}
	if outcome.Error != nil {
		return stats, outcome.Error
	}
	return stats, nil
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
