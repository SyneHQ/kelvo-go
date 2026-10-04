// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package httpstream

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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

func TestCompletionPreservesDeadlineAndStopsBeforeWriterRelease(t *testing.T) {
	w := &deadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deadline := time.Now().Add(time.Minute)
	stop := WatchWriteDeadline(ctx, w, deadline)
	stop()
	stop()
	values := w.snapshot()
	if len(values) != 1 || !values[0].Equal(deadline) {
		t.Fatal("normal completion changed the deadline before server flush")
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

// A small response remains in net/http's buffer until after ServeHTTP returns.
// Observe the deadline at the actual TLS socket write from finishRequest;
// clearing it in handler cleanup would silently remove this last-write bound.
type bufferedTLSWriteState struct {
	handlerReturned    atomic.Bool
	writesAfterHandler atomic.Int32
	unboundedWrites    atomic.Int32
}

type bufferedTLSListener struct {
	net.Listener
	state *bufferedTLSWriteState
}

func (l *bufferedTLSListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &bufferedTLSConn{Conn: connection, state: l.state}, nil
}

type bufferedTLSConn struct {
	net.Conn
	state    *bufferedTLSWriteState
	mu       sync.Mutex
	deadline time.Time
}

func (c *bufferedTLSConn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.deadline = deadline
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(deadline)
}

func (c *bufferedTLSConn) Write(value []byte) (int, error) {
	if c.state.handlerReturned.Load() {
		c.mu.Lock()
		deadline := c.deadline
		c.mu.Unlock()
		c.state.writesAfterHandler.Add(1)
		if deadline.IsZero() {
			c.state.unboundedWrites.Add(1)
		}
	}
	return c.Conn.Write(value)
}

func TestTLSBufferedCompletionRetainsDeadlineThroughServerFlush(t *testing.T) {
	var state bufferedTLSWriteState
	const payload = "small buffered final response"
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stop := WatchWriteDeadline(r.Context(), w, time.Now().Add(3*time.Second))
		defer func() { stop(); state.handlerReturned.Store(true) }()
		_, _ = io.WriteString(w, payload)
	}))
	server.Listener = &bufferedTLSListener{Listener: server.Listener, state: &state}
	server.StartTLS()
	defer server.Close()
	client := server.Client()
	client.Timeout = 3 * time.Second
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != payload || response.ProtoMajor != 1 {
		t.Fatal("buffered HTTP/1 TLS response failed", response.Proto, err)
	}
	if state.writesAfterHandler.Load() == 0 {
		t.Fatal("fixture did not observe post-handler socket flush")
	}
	if state.unboundedWrites.Load() != 0 {
		t.Fatal("handler cleanup cleared the final TLS flush deadline")
	}
}
