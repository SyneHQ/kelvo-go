// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
)

func TestCommandIOChild(t *testing.T) {
	mode := os.Getenv("KELVO_TEST_COMMAND_IO")
	if mode == "" {
		return
	}
	if mode == "hold-input" {
		_, _ = os.Stdout.Write([]byte("R"))
		time.Sleep(10 * time.Second)
		os.Exit(0)
	}
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
	if err != nil {
		os.Exit(2)
	}
	if mode == "flood-diagnostic" {
		_, _ = os.Stderr.Write(bytes.Repeat([]byte("x"), 2*maxOutcomeBytes))
	} else if mode == "outcome" {
		_, _ = os.Stderr.Write([]byte(`{"stats":{"rows":17}}`))
	} else if mode == "echo" {
		_, _ = os.Stdout.Write(raw)
		_, _ = os.Stderr.Write([]byte("diagnostic-complete"))
	} else {
		os.Exit(3)
	}
	os.Exit(0)
}

func commandIOFixture(t *testing.T, diagnostic io.Writer) (*commandIO, *exec.Cmd, *containment.Custody, *admission.Pool, *atomic.Int32) {
	t.Helper()
	pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 1024,
		Classes: map[admission.Class]admission.ClassLimits{admission.ClassExport: {MaxConcurrent: 1, MemoryBytes: 1024}}})
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := pool.Acquire(context.Background(), admission.Request{MemoryBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	custody, _ := containment.NewCustody(reservation.Release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCommandIOChild$")
	command.Env = append(os.Environ(), "KELVO_TEST_COMMAND_IO=echo")
	drained := &atomic.Int32{}
	payload := []byte("private-input-envelope")
	pipes, err := newCommandIO(ctx, command, payload, diagnostic, custody, func() { drained.Add(1); pool.Drain() })
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	clear(payload) // The command must own its input after constructor return.
	t.Cleanup(func() {
		cancel()
		if command.Process != nil && command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
		_ = pipes.Finish(context.Background())
		custody.Complete()
	})
	return pipes, command, custody, pool, drained
}

func TestCommandIOConcreteFilesAndCompletionCustody(t *testing.T) {
	var diagnostic bytes.Buffer
	pipes, command, custody, pool, _ := commandIOFixture(t, &diagnostic)
	for _, endpoint := range []any{command.Stdin, command.Stdout, command.Stderr} {
		if _, ok := endpoint.(*os.File); !ok {
			t.Fatal("process launch retained an implicit copy endpoint")
		}
	}
	if err := pipes.Start(command.Start); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := pipes.Consume(func(reader io.Reader) error { _, err := io.Copy(&output, reader); return err }); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := pipes.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if output.String() != "private-input-envelope" || diagnostic.String() != "diagnostic-complete" {
		t.Fatal("explicit command I/O changed the input or output")
	}
	if pool.Snapshot().Active != 1 {
		t.Fatal("I/O completion released the outer publication owner")
	}
	custody.Complete()
	if pool.Snapshot().Active != 0 {
		t.Fatal("completed command retained admission")
	}
	if err := pipes.Consume(func(io.Reader) error { return nil }); !errors.Is(err, containment.ErrInvalid) {
		t.Fatal("finished output admitted another consumer", err)
	}
}

type heldDiagnostic struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	target  io.Writer
}

func (w *heldDiagnostic) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	if w.target != nil {
		return w.target.Write(p)
	}
	return len(p), nil
}

func TestCommandIODoesNotDecodeUnjoinedDiagnostics(t *testing.T) {
	var diagnostic boundedBuffer
	writer := &heldDiagnostic{entered: make(chan struct{}), release: make(chan struct{}), target: &diagnostic}
	defer func() {
		select {
		case <-writer.release:
		default:
			close(writer.release)
		}
	}()
	pipes, command, _, _, _ := commandIOFixture(t, writer)
	command.Env = append(os.Environ(), "KELVO_TEST_COMMAND_IO=outcome")
	if err := pipes.Start(command.Start); err != nil {
		t.Fatal(err)
	}
	if err := pipes.Consume(func(reader io.Reader) error { _, err := io.Copy(io.Discard, reader); return err }); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("native outcome did not reach the held diagnostic writer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := pipes.Finish(ctx); !errors.Is(err, containment.ErrQuarantined) {
		t.Fatal("held diagnostic writer escaped quarantine", err)
	}
	if _, err := decodeCommandOutcome(pipes, &diagnostic); !errors.Is(err, containment.ErrQuarantined) {
		t.Fatal("outcome decoding read an unfinished diagnostic buffer", err)
	}
	close(writer.release)
	if err := pipes.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if outcome, err := decodeCommandOutcome(pipes, &diagnostic); err != nil || outcome.Stats.Rows != 17 {
		t.Fatal("joined outcome was not decoded", outcome, err)
	}
}

func TestCommandIOBlockedDiagnosticKeepsAllClassesReserved(t *testing.T) {
	writer := &heldDiagnostic{entered: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		select {
		case <-writer.release:
		default:
			close(writer.release)
		}
	}()
	pipes, command, custody, pool, drained := commandIOFixture(t, writer)
	if err := pipes.Start(command.Start); err != nil {
		t.Fatal(err)
	}
	if err := pipes.Consume(func(reader io.Reader) error { _, err := io.Copy(io.Discard, reader); return err }); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("native diagnostic did not reach the blocked parent writer")
	}
	custody.Complete()
	for _, class := range []admission.Class{admission.ClassInteractive, admission.ClassRefresh, admission.ClassExport} {
		if reservation, err := pool.TryAcquire(admission.Request{Class: class, MemoryBytes: 1}); !errors.Is(err, admission.ErrBusy) || reservation != nil {
			t.Fatal("blocked I/O freed capacity for another workload class", class, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := pipes.Finish(ctx); !errors.Is(err, containment.ErrQuarantined) || time.Since(started) > 250*time.Millisecond {
		t.Fatal("diagnostic writer bypassed the cleanup deadline", err)
	}
	if pool.Snapshot().Active != 1 || !pool.Snapshot().Draining || drained.Load() == 0 {
		t.Fatal("uncertain I/O lost custody or left admission open")
	}
	close(writer.release)
	if err := pipes.Finish(context.Background()); err != nil || pool.Snapshot().Active != 0 || !pool.Snapshot().Draining {
		t.Fatal("late diagnostic completion did not release existing custody while retaining drain", err)
	}
}

func TestCommandIOBlockedConsumerRetainsCustodyAfterNativeExit(t *testing.T) {
	pipes, command, custody, pool, _ := commandIOFixture(t, io.Discard)
	if err := pipes.Start(command.Start); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	consumed := make(chan error, 1)
	go func() {
		consumed <- pipes.Consume(func(reader io.Reader) error {
			_, err := io.Copy(io.Discard, reader)
			close(entered)
			<-release
			return err
		})
	}()
	select {
	case <-entered:
	case <-pipes.ctx.Done():
		t.Fatal("result consumer did not reach its boundary before the child deadline")
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	custody.Complete()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := pipes.Finish(ctx); !errors.Is(err, containment.ErrQuarantined) || pool.Snapshot().Active != 1 {
		t.Fatal("native exit released a blocked result consumer", err)
	}
	close(release)
	if err := <-consumed; err != nil {
		t.Fatal(err)
	}
	if err := pipes.Finish(context.Background()); err != nil || pool.Snapshot().Active != 0 {
		t.Fatal("late consumer completion retained finished I/O custody", err)
	}
}

func TestCommandIODiagnosticFloodStaysBounded(t *testing.T) {
	var diagnostic boundedBuffer
	pipes, command, _, _, _ := commandIOFixture(t, &diagnostic)
	command.Env = append(os.Environ(), "KELVO_TEST_COMMAND_IO=flood-diagnostic")
	if err := pipes.Start(command.Start); err != nil {
		t.Fatal(err)
	}
	if err := pipes.Consume(func(reader io.Reader) error { _, err := io.Copy(io.Discard, reader); return err }); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := pipes.Finish(context.Background()); err != nil || diagnostic.Len() != maxOutcomeBytes {
		t.Fatal("native diagnostic input bypassed the bounded writer", err, diagnostic.Len())
	}
}

func TestCommandIOCancellationStopsBlockedInput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCommandIOChild$")
	command.Env = append(os.Environ(), "KELVO_TEST_COMMAND_IO=hold-input")
	var released atomic.Int32
	custody, _ := containment.NewCustody(func() { released.Add(1) })
	pipes, err := newCommandIO(ctx, command, bytes.Repeat([]byte("private"), 1<<17), io.Discard, custody, func() {})
	if err != nil {
		t.Fatal(err)
	}
	defer pipes.Finish(context.Background())
	if err := pipes.Start(command.Start); err != nil {
		t.Fatal(err)
	}
	if err := pipes.Consume(func(reader io.Reader) error {
		var ready [1]byte
		_, err := io.ReadFull(reader, ready[:])
		if ready[0] != 'R' {
			return io.ErrUnexpectedEOF
		}
		return err
	}); err != nil {
		cancel()
		_ = command.Wait()
		t.Fatal(err)
	}
	cancel()
	if err := command.Wait(); err == nil {
		t.Fatal("cancelled child continued executing")
	}
	custody.Complete()
	if err := pipes.Finish(context.Background()); err != nil || released.Load() != 1 || len(pipes.input) != 0 || pipes.callbacks != 0 {
		t.Fatal("cancelled input retained its bytes or resource ownership", err)
	}
}

func TestCommandIOLaunchAndCancellationKeepOwnership(t *testing.T) {
	for _, mode := range []string{"in-flight", "cancelled-before-start", "unused"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var released atomic.Int32
			custody, _ := containment.NewCustody(func() { released.Add(1) })
			command := exec.Command("unused")
			pipes, err := newCommandIO(ctx, command, []byte("secret"), io.Discard, custody, func() {})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "in-flight" {
				entered, resume := make(chan struct{}), make(chan struct{})
				started := make(chan error, 1)
				go func() { started <- pipes.Start(func() error { close(entered); <-resume; return io.ErrUnexpectedEOF }) }()
				<-entered
				custody.Complete()
				deadline, stop := context.WithTimeout(ctx, 20*time.Millisecond)
				err = pipes.Finish(deadline)
				stop()
				if !errors.Is(err, containment.ErrQuarantined) || released.Load() != 0 {
					close(resume)
					t.Fatal("unfinished native launch lost its I/O custody", err)
				}
				close(resume)
				if err := <-started; !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatal(err)
				}
			} else if mode == "cancelled-before-start" {
				cancel()
				called := false
				if err := pipes.Start(func() error { called = true; return nil }); !errors.Is(err, context.Canceled) || called {
					t.Fatal("cancelled launch delivered private input", err)
				}
			}
			custody.Complete()
			if err := pipes.Finish(context.Background()); err != nil || released.Load() != 1 || len(pipes.input) != 0 {
				t.Fatal("resolved command retained files, input or admission", err)
			}
		})
	}
}
