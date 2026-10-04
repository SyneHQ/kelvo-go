//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

const objectWriterWait = 3 * time.Second

// These gates deliberately ignore cancellation. A canceled request is not
// evidence that its factory, provider call, or client cleanup has returned.
type objectWriterGate struct {
	entered chan struct{}
	release chan struct{}
	enter   sync.Once
	leave   sync.Once
}

func (g *objectWriterGate) block() {
	g.enter.Do(func() { close(g.entered) })
	<-g.release
}

func (g *objectWriterGate) unblock() { g.leave.Do(func() { close(g.release) }) }

type objectWriterClient struct {
	objectstore.Client
	payloadGate *objectWriterGate
	closeGate   *objectWriterGate
	closes      atomic.Int32
	closed      atomic.Int32
}

func (c *objectWriterClient) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
	if c.payloadGate != nil && strings.HasSuffix(key, ".parquet") {
		c.payloadGate.block()
	}
	return c.Client.Put(ctx, key, body, size, digest, condition)
}

func (c *objectWriterClient) Close() {
	c.closes.Add(1)
	if c.closeGate != nil {
		c.closeGate.block()
	}
	c.Client.Close()
	c.closed.Add(1)
}

type objectWriterFixture struct {
	backend  *objectBackend
	objects  *fakeSnapshotObjects
	reader   *objectWriterClient
	gates    []*objectWriterGate
	writers  []interface{ Abort() error }
	finished []<-chan struct{}
}

func newObjectWriterFixture(t *testing.T) *objectWriterFixture {
	t.Helper()
	f := &objectWriterFixture{objects: newFakeSnapshotObjects()}
	f.reader = &objectWriterClient{Client: f.objects}
	var err error
	f.backend, err = newObjectBackend(testObjectConfig(t), f.reader)
	if err != nil {
		t.Fatal(err)
	}
	// Renewal timing is unrelated to these ownership handoffs.
	f.backend.renewInterval = time.Hour
	t.Cleanup(func() {
		for _, gate := range f.gates {
			gate.unblock()
		}
		done := make(chan struct{})
		go func() {
			for _, writer := range f.writers {
				_ = writer.Abort()
			}
			_ = f.backend.Close()
			close(done)
		}()
		for _, finished := range append(f.finished, done) {
			select {
			case <-finished:
			case <-time.After(objectWriterWait):
				t.Error("object writer fixture did not join its released work")
			}
		}
	})
	return f
}

func (f *objectWriterFixture) gate() *objectWriterGate {
	gate := &objectWriterGate{entered: make(chan struct{}), release: make(chan struct{})}
	f.gates = append(f.gates, gate)
	return gate
}

func (f *objectWriterFixture) begin(t *testing.T, dataset string) RefreshWriter {
	t.Helper()
	writer, err := f.backend.Begin(context.Background(), dataset)
	if err != nil {
		t.Fatal(err)
	}
	f.writers = append(f.writers, writer)
	return writer
}

type objectWriterFuture[T any] struct {
	done  chan struct{}
	value T
}

func startObjectWriterOperation[T any](f *objectWriterFixture, operation func() T) *objectWriterFuture[T] {
	future := &objectWriterFuture[T]{done: make(chan struct{})}
	f.finished = append(f.finished, future.done)
	go func() {
		future.value = operation()
		close(future.done)
	}()
	return future
}

func awaitObjectWriterSignal(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(objectWriterWait):
		t.Fatal("timed out waiting for " + label)
	}
}

func awaitObjectWriterOperation[T any](t *testing.T, future *objectWriterFuture[T], label string) T {
	t.Helper()
	awaitObjectWriterSignal(t, future.done, label)
	return future.value
}

func objectWriterStillPending[T any](t *testing.T, future *objectWriterFuture[T], label string) {
	t.Helper()
	select {
	case <-future.done:
		t.Fatal(label + " returned before its blocked work finished")
	default:
	}
}

func objectWriterReaderOpen(t *testing.T, f *objectWriterFixture) {
	t.Helper()
	if f.reader.closes.Load() != 0 {
		t.Fatal("backend closed its shared reader before writer cleanup joined")
	}
}

type objectWriterBeginResult struct {
	returnedWriter bool
	err            error
}

func startObjectWriterBegin(f *objectWriterFixture, dataset string) *objectWriterFuture[objectWriterBeginResult] {
	return startObjectWriterOperation(f, func() objectWriterBeginResult {
		writer, err := f.backend.Begin(context.Background(), dataset)
		result := objectWriterBeginResult{returnedWriter: writer != nil, err: err}
		// Also release an incorrectly admitted writer when a regression fails.
		if writer != nil {
			_ = writer.Abort()
		}
		return result
	})
}

func TestObjectWriterLifetimeCloseJoinsPendingFactoryAndLateOwnedClient(t *testing.T) {
	f := newObjectWriterFixture(t)
	anchor := f.begin(t, "anchor")
	factory, cleanup := f.gate(), f.gate()
	client := &objectWriterClient{Client: f.objects, closeGate: cleanup}
	f.backend.writeClient = func() (objectstore.Client, bool, error) {
		factory.block()
		return client, true, nil
	}
	begin := startObjectWriterBegin(f, "events")
	awaitObjectWriterSignal(t, factory.entered, "pending writer factory")
	closing := startObjectWriterOperation(f, f.backend.Close)
	awaitObjectWriterSignal(t, anchor.Context().Done(), "backend drain cancellation")
	if err := anchor.Abort(); err != nil {
		t.Fatal(err)
	}
	objectWriterStillPending(t, closing, "backend Close with pending factory")
	objectWriterReaderOpen(t, f)
	factory.unblock()
	awaitObjectWriterSignal(t, cleanup.entered, "late owned client cleanup")
	objectWriterStillPending(t, begin, "canceled Begin")
	objectWriterStillPending(t, closing, "backend Close with late cleanup")
	objectWriterReaderOpen(t, f)
	cleanup.unblock()
	result := awaitObjectWriterOperation(t, begin, "canceled Begin cleanup")
	if result.returnedWriter || !errors.Is(result.err, errBackendClosed) {
		t.Fatalf("late factory admitted a writer after drain: writer=%t err=%v", result.returnedWriter, result.err)
	}
	if err := awaitObjectWriterOperation(t, closing, "backend Close"); err != nil {
		t.Fatal(err)
	}
	if client.closes.Load() != 1 || client.closed.Load() != 1 || f.reader.closes.Load() != 1 {
		t.Fatal("late writer and shared reader cleanup were not once-only and joined")
	}
}

func TestObjectWriterLifetimeFactoryErrorHonorsPartialClientOwnership(t *testing.T) {
	for _, owned := range []bool{false, true} {
		name := "shared"
		if owned {
			name = "owned"
		}
		t.Run(name, func(t *testing.T) {
			f := newObjectWriterFixture(t)
			anchor := f.begin(t, "anchor")
			factoryErr := errors.New("fixture returned a partial client")
			cleanup := f.gate()
			client := f.reader
			if owned {
				client = &objectWriterClient{Client: f.objects, closeGate: cleanup}
			}
			f.backend.writeClient = func() (objectstore.Client, bool, error) { return client, owned, factoryErr }
			begin := startObjectWriterBegin(f, "events")
			if owned {
				awaitObjectWriterSignal(t, cleanup.entered, "partial owned client cleanup")
				objectWriterStillPending(t, begin, "failed Begin with partial client")
			} else {
				result := awaitObjectWriterOperation(t, begin, "shared-client factory failure")
				if result.returnedWriter || !errors.Is(result.err, factoryErr) {
					t.Fatalf("factory failure lost its result: %+v", result)
				}
			}
			objectWriterReaderOpen(t, f)
			closing := startObjectWriterOperation(f, f.backend.Close)
			awaitObjectWriterSignal(t, anchor.Context().Done(), "backend drain cancellation")
			if err := anchor.Abort(); err != nil {
				t.Fatal(err)
			}
			if owned {
				objectWriterStillPending(t, closing, "backend Close with failed factory cleanup")
				objectWriterReaderOpen(t, f)
				cleanup.unblock()
				result := awaitObjectWriterOperation(t, begin, "partial-client factory failure")
				if result.returnedWriter || !errors.Is(result.err, factoryErr) {
					t.Fatalf("factory failure lost its result: %+v", result)
				}
			}
			if err := awaitObjectWriterOperation(t, closing, "backend Close"); err != nil {
				t.Fatal(err)
			}
			if client.closes.Load() != 1 || client.closed.Load() != 1 || f.reader.closes.Load() != 1 {
				t.Fatal("factory failure leaked or double-closed client ownership")
			}
		})
	}
}

func TestObjectWriterLifetimeDrainWaitsForSharedSingleAndMultipartBorrowers(t *testing.T) {
	f := newObjectWriterFixture(t)
	single := f.begin(t, "events")
	multipart, err := f.backend.BeginMultipart(context.Background(), "other", remoteMultipartOptions())
	if err != nil {
		t.Fatal(err)
	}
	f.writers = append(f.writers, multipart)
	part, err := multipart.NewPart()
	if err != nil {
		t.Fatal(err)
	}
	file := single.File()
	closing := startObjectWriterOperation(f, f.backend.Close)
	for _, writer := range []interface{ Context() context.Context }{single, multipart} {
		awaitObjectWriterSignal(t, writer.Context().Done(), "borrowed writer cancellation")
		if !errors.Is(context.Cause(writer.Context()), errBackendClosed) {
			t.Fatal("writer did not retain backend drain cause")
		}
	}
	if _, err := file.Stat(); err != nil {
		t.Fatal("drain closed the caller's single-file borrow", err)
	}
	if _, err := part.Stat(); err != nil {
		t.Fatal("drain closed the caller's multipart borrow", err)
	}
	if writer, err := f.backend.Begin(context.Background(), "late"); writer != nil || !errors.Is(err, errBackendClosed) {
		if writer != nil {
			_ = writer.Abort()
		}
		t.Fatal("backend admitted a new writer during drain", err)
	}
	objectWriterReaderOpen(t, f)
	if err := single.Abort(); err != nil {
		t.Fatal(err)
	}
	objectWriterStillPending(t, closing, "backend Close with remaining multipart borrow")
	objectWriterReaderOpen(t, f)
	if err := multipart.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := awaitObjectWriterOperation(t, closing, "joined backend Close"); err != nil {
		t.Fatal(err)
	}
	for _, borrowed := range []*os.File{file, part} {
		if _, err := borrowed.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("writer Abort did not release the borrowed file", err)
		}
	}
	if f.reader.closes.Load() != 1 || f.reader.closed.Load() != 1 {
		t.Fatal("shared client was not closed exactly once after both writers")
	}
}

type objectWriterCommitResult struct {
	snapshot Snapshot
	err      error
}

func TestObjectWriterLifetimeCommitAbortAndDrainPreservePreviousGeneration(t *testing.T) {
	f := newObjectWriterFixture(t)
	previous := writeObjectSnapshot(t, f.backend, "PAR1previous generationPAR1")
	payload := f.gate()
	client := &objectWriterClient{Client: f.objects, payloadGate: payload}
	f.backend.writeClient = func() (objectstore.Client, bool, error) { return client, true, nil }
	writer := f.begin(t, "events")
	if _, err := io.WriteString(writer.File(), "PAR1canceled generationPAR1"); err != nil {
		t.Fatal(err)
	}
	commit := startObjectWriterOperation(f, func() objectWriterCommitResult {
		snapshot, err := writer.Commit("new-config", 3)
		return objectWriterCommitResult{snapshot, err}
	})
	awaitObjectWriterSignal(t, payload.entered, "blocked payload upload")
	closing := startObjectWriterOperation(f, f.backend.Close)
	awaitObjectWriterSignal(t, writer.Context().Done(), "commit cancellation")
	abortStarted := make(chan struct{})
	abort := startObjectWriterOperation(f, func() error {
		close(abortStarted)
		return writer.Abort()
	})
	awaitObjectWriterSignal(t, abortStarted, "concurrent Abort admission")
	objectWriterStillPending(t, commit, "Commit with outstanding upload")
	objectWriterStillPending(t, abort, "Abort while Commit owns its upload")
	objectWriterStillPending(t, closing, "backend Close with outstanding upload")
	objectWriterReaderOpen(t, f)
	payload.unblock()
	result := awaitObjectWriterOperation(t, commit, "canceled Commit")
	if !errors.Is(result.err, errBackendClosed) || result.snapshot.Generation != "" {
		t.Fatalf("canceled upload published success: %+v", result)
	}
	if err := awaitObjectWriterOperation(t, abort, "concurrent Abort"); err != nil {
		t.Fatal(err)
	}
	if err := awaitObjectWriterOperation(t, closing, "backend Close"); err != nil {
		t.Fatal(err)
	}
	observer := testObjectBackend(t, f.objects)
	current, err := verifyRawObjectFixture(observer, "events")
	if err != nil || current.Generation != previous.Generation || current.SHA256 != previous.SHA256 {
		t.Fatalf("canceled refresh damaged the committed generation: %+v %v", current, err)
	}
	if client.closes.Load() != 1 || f.reader.closes.Load() != 1 {
		t.Fatal("Commit/Abort/drain did not share once-only client cleanup")
	}
}

func TestObjectWriterLifetimePublishedCommitSurvivesDrainDuringClientCleanup(t *testing.T) {
	f := newObjectWriterFixture(t)
	previous := writeObjectSnapshot(t, f.backend, "PAR1previous generationPAR1")
	cleanup := f.gate()
	client := &objectWriterClient{Client: f.objects, closeGate: cleanup}
	f.backend.writeClient = func() (objectstore.Client, bool, error) { return client, true, nil }
	writer := f.begin(t, "events")
	if _, err := io.WriteString(writer.File(), "PAR1published generationPAR1"); err != nil {
		t.Fatal(err)
	}
	commit := startObjectWriterOperation(f, func() objectWriterCommitResult {
		snapshot, err := writer.Commit("new-config", 3)
		return objectWriterCommitResult{snapshot, err}
	})
	awaitObjectWriterSignal(t, cleanup.entered, "published writer client cleanup")
	observer := testObjectBackend(t, f.objects)
	published, err := verifyRawObjectFixture(observer, "events")
	if err != nil || published.Generation == previous.Generation {
		t.Fatal("fixture did not publish its new generation before cleanup", err)
	}
	closing := startObjectWriterOperation(f, f.backend.Close)
	awaitObjectWriterSignal(t, writer.Context().Done(), "backend drain during completed publication")
	abortStarted := make(chan struct{})
	abort := startObjectWriterOperation(f, func() error {
		close(abortStarted)
		return writer.Abort()
	})
	awaitObjectWriterSignal(t, abortStarted, "concurrent Abort admission")
	objectWriterStillPending(t, commit, "published Commit with blocked cleanup")
	objectWriterStillPending(t, abort, "Abort while Commit owns client cleanup")
	objectWriterStillPending(t, closing, "backend Close with blocked cleanup")
	objectWriterReaderOpen(t, f)
	cleanup.unblock()
	result := awaitObjectWriterOperation(t, commit, "committed writer cleanup")
	if result.err != nil || result.snapshot.Generation != published.Generation {
		t.Fatalf("drain replaced completed publication with a failure: %+v", result)
	}
	if err := awaitObjectWriterOperation(t, abort, "concurrent Abort"); err != nil {
		t.Fatal(err)
	}
	if err := awaitObjectWriterOperation(t, closing, "backend Close"); err != nil {
		t.Fatal(err)
	}
	current, err := verifyRawObjectFixture(observer, "events")
	if err != nil || current.Generation != published.Generation || current.SHA256 != published.SHA256 {
		t.Fatalf("drain changed the already-published generation: %+v %v", current, err)
	}
	if client.closes.Load() != 1 || client.closed.Load() != 1 || f.reader.closes.Load() != 1 {
		t.Fatal("publication cleanup did not retain once-only client ownership")
	}
}
