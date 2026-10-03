// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package httpstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type deadlineWriter struct {
	*httptest.ResponseRecorder
	mu        sync.Mutex
	deadlines []time.Time
	entered   chan struct{}
	release   chan struct{}
	block     bool
}

func (w *deadlineWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.deadlines = append(w.deadlines, deadline)
	block := w.block && !deadline.IsZero() && !deadline.After(time.Now().Add(time.Millisecond))
	w.block = false
	w.mu.Unlock()
	if block {
		close(w.entered)
		<-w.release
	}
	return nil
}

func (w *deadlineWriter) snapshot() []time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Time(nil), w.deadlines...)
}

func TestCompletionRestoresDeadlineAndStopsBeforeWriterRelease(t *testing.T) {
	w := &deadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deadline := time.Now().Add(time.Minute)
	stop := WatchWriteDeadline(ctx, w, deadline)
	stop()
	stop()
	values := w.snapshot()
	if len(values) != 2 || !values[0].Equal(deadline) || !values[1].IsZero() {
		t.Fatal("normal completion did not restore exactly once")
	}
	cancel()
	time.Sleep(2 * cancellationInterval)
	if len(w.snapshot()) != len(values) {
		t.Fatal("watcher used writer after return")
	}
}

func TestCancellationReassertsExpiredDeadlineAndJoins(t *testing.T) {
	w := &deadlineWriter{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	stop := WatchWriteDeadline(ctx, w, time.Now().Add(time.Minute))
	defer stop()
	w.mu.Lock()
	w.block = true
	w.mu.Unlock()
	cancel()
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not set deadline")
	}
	// A close implementation may replace the expired deadline while finishing
	// its own write. The next enforcement must expire that new deadline too.
	if err := w.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	close(w.release)
	deadline := time.Now().Add(time.Second)
	for {
		values := w.snapshot()
		if len(values) >= 4 && !values[len(values)-1].After(time.Now()) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replacement deadline was not expired")
		}
		time.Sleep(time.Millisecond)
	}
	stop()
	values := w.snapshot()
	for _, value := range values {
		if value.IsZero() {
			t.Fatal("cancelled response deadline was cleared")
		}
	}
	time.Sleep(2 * cancellationInterval)
	if len(w.snapshot()) != len(values) {
		t.Fatal("cancelled watcher retained writer")
	}
}

func TestStopJoinsAnInFlightDeadlineUpdate(t *testing.T) {
	w := &deadlineWriter{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	stop := WatchWriteDeadline(ctx, w, time.Now().Add(time.Minute))
	w.mu.Lock()
	w.block = true
	w.mu.Unlock()
	cancel()
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("watcher did not enter")
	}
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
		t.Fatal("stop returned while writer method remained active")
	case <-time.After(10 * time.Millisecond):
	}
	close(w.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop did not join")
	}
}

func TestUnsupportedWriterDoesNotStartEnforcement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := WatchWriteDeadline(ctx, httptest.NewRecorder(), time.Now().Add(time.Minute))
	cancel()
	stop()
	stop()
	if err := http.NewResponseController(httptest.NewRecorder()).SetWriteDeadline(time.Now()); err == nil {
		t.Fatal("fixture unexpectedly implements deadlines")
	}
}
