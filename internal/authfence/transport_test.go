// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type protocolConn struct {
	mu           sync.Mutex
	deadlines    [3]time.Time
	writes       atomic.Int32
	closes       atomic.Int32
	closeEntered chan struct{}
	closeRelease <-chan struct{}
	closeErr     error
	deadlineErr  error
}

func (c *protocolConn) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *protocolConn) Write(p []byte) (int, error) { c.writes.Add(1); return len(p), nil }
func (c *protocolConn) Close() error {
	c.closes.Add(1)
	if c.closeEntered != nil {
		close(c.closeEntered)
	}
	if c.closeRelease != nil {
		<-c.closeRelease
	}
	return c.closeErr
}
func (*protocolConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10000}
}
func (*protocolConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4222}
}
func (c *protocolConn) deadline(index int, value time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadlines[index] = value
	return c.deadlineErr
}
func (c *protocolConn) SetDeadline(value time.Time) error      { return c.deadline(0, value) }
func (c *protocolConn) SetReadDeadline(value time.Time) error  { return c.deadline(1, value) }
func (c *protocolConn) SetWriteDeadline(value time.Time) error { return c.deadline(2, value) }

func protocolWait(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("fixture synchronization timed out")
	}
}

func TestFenceTransportClampsEveryDeadline(t *testing.T) {
	deadline := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	raw := &protocolConn{}
	conn := &ownedConn{Conn: raw, ctx: ctx, deadline: deadline, done: make(chan struct{})}
	for _, value := range []time.Time{{}, deadline.Add(time.Hour), deadline.Add(-time.Millisecond)} {
		for _, set := range []func(time.Time) error{conn.SetDeadline, conn.SetReadDeadline, conn.SetWriteDeadline} {
			if err := set(value); err != nil {
				t.Fatal(err)
			}
		}
		want := value
		if value.IsZero() || value.After(deadline) {
			want = deadline
		}
		raw.mu.Lock()
		got := raw.deadlines
		raw.mu.Unlock()
		for _, actual := range got {
			if !actual.Equal(want) {
				t.Fatal("deadline extended or reset")
			}
		}
	}
	if _, err := conn.Write([]byte("fresh")); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := conn.Write([]byte("expired")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if raw.writes.Load() != 1 {
		t.Fatal("canceled write reached transport")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFenceTransportSealsBeforeBlockedClose(t *testing.T) {
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	raw := &protocolConn{closeEntered: entered, closeRelease: release}
	conn := &ownedConn{Conn: raw, ctx: context.Background(), deadline: time.Now().Add(time.Second), done: make(chan struct{})}
	go func() { _ = conn.Close(); close(finished) }()
	protocolWait(t, entered)
	if _, err := conn.Write([]byte("late")); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	if raw.writes.Load() != 0 {
		t.Fatal("write reached a closing transport")
	}
	select {
	case <-conn.done:
		t.Fatal("close completed before underlying close")
	default:
	}
	close(release)
	protocolWait(t, finished)
	if err := conn.Close(); err != nil || raw.closes.Load() != 1 {
		t.Fatal("transport closed more than once")
	}
}

func TestFenceGenerationRejectsOtherEndpointsAndRedial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	generation := newGeneration(ctx, "127.0.0.1:4222")
	raw := &protocolConn{}
	var calls atomic.Int32
	generation.dial = func(context.Context, string, string) (net.Conn, error) { calls.Add(1); return raw, nil }
	for _, endpoint := range [][2]string{{"udp", "127.0.0.1:4222"}, {"tcp", "127.0.0.1:4223"}} {
		if _, err := generation.Dial(endpoint[0], endpoint[1]); !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("foreign endpoint was dialed")
	}
	if _, err := generation.Dial("tcp", "127.0.0.1:4222"); err != nil {
		t.Fatal(err)
	}
	if _, err := generation.Dial("tcp", "127.0.0.1:4222"); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("dial retried")
	}
	if err := generation.finish(); err != nil || raw.closes.Load() != 1 {
		t.Fatal(err)
	}
}

func TestFenceGenerationRetainsLatePartialConnection(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "late", true: "partial-error"}[partial], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			generation := newGeneration(ctx, "127.0.0.1:4222")
			entered, release, closing, closed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			underlying := errors.New("fixture close uncertainty")
			raw := &protocolConn{closeEntered: closing, closeRelease: closed, closeErr: underlying}
			generation.dial = func(context.Context, string, string) (net.Conn, error) {
				close(entered)
				<-release
				if partial {
					return raw, errors.New("fixture partial dial")
				}
				return raw, nil
			}
			result := make(chan error, 1)
			go func() {
				conn, err := generation.Dial("tcp", "127.0.0.1:4222")
				if conn != nil {
					t.Error("late connection escaped custody")
				}
				result <- err
			}()
			protocolWait(t, entered)
			if !partial {
				cancel()
			}
			close(release)
			protocolWait(t, closing)
			select {
			case <-result:
				t.Fatal("dial custody ended before partial close")
			default:
			}
			close(closed)
			if err := <-result; !errors.Is(err, ErrUnavailable) {
				t.Fatal(err)
			}
			if err := generation.finish(); !errors.Is(err, underlying) {
				t.Fatal("close uncertainty lost", err)
			}
			if raw.closes.Load() != 1 {
				t.Fatal("partial connection closed more than once")
			}
		})
	}
}

func TestFenceGenerationFinishJoinsCancellationClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	generation := newGeneration(ctx, "127.0.0.1:4222")
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	raw := &protocolConn{closeEntered: entered, closeRelease: release}
	generation.dial = func(context.Context, string, string) (net.Conn, error) { return raw, nil }
	if _, err := generation.Dial("tcp", "127.0.0.1:4222"); err != nil {
		t.Fatal(err)
	}
	cancel()
	protocolWait(t, entered)
	go func() { _ = generation.finish(); close(finished) }()
	select {
	case <-finished:
		t.Fatal("finish abandoned cancellation callback")
	default:
	}
	close(release)
	protocolWait(t, finished)
	protocolWait(t, generation.callbackDone)
	if raw.closes.Load() != 1 {
		t.Fatal("callback and cleanup double-closed connection")
	}
}

type protocolWriteBarrierConn struct {
	net.Conn
	returned chan error
	release  <-chan struct{}
}

func (c *protocolWriteBarrierConn) Write(data []byte) (int, error) {
	n, err := c.Conn.Write(data)
	c.returned <- err
	<-c.release
	return n, err
}

func TestFenceCancellationInterruptsEnteredWriteAndRetainsCustody(t *testing.T) {
	client, _ := NewClient(protocolConfig(t, "east"))
	transport, peer := net.Pipe()
	defer peer.Close()
	release := make(chan struct{})
	raw := &protocolWriteBarrierConn{Conn: transport, returned: make(chan error, 1), release: release}
	client.open = func(op *operation, _ Config) (wireSession, error) {
		generation := newGeneration(op.ctx, "127.0.0.1:4222")
		generation.dial = func(context.Context, string, string) (net.Conn, error) { return raw, nil }
		conn, err := generation.Dial("tcp", "127.0.0.1:4222")
		return &protocolSession{call: func(string, []byte, nats.Header) ([]byte, error) {
			_, writeErr := conn.Write([]byte("a request larger than the one-byte peer read"))
			return nil, writeErr
		}, onClose: generation.finish}, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.Read(ctx, protocolAttempt(t)); result <- err }()
	// Consume exactly one byte. This proves Write has entered net.Pipe and
	// still has bytes outstanding when cancellation aborts the connection.
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var first [1]byte
	if n, err := peer.Read(first[:]); err != nil || n != 1 {
		t.Fatal("write never entered the transport", err)
	}
	cancel()
	select {
	case err := <-raw.returned:
		if err == nil {
			t.Fatal("blocked write completed without cancellation error")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation failed to interrupt entered write")
	}
	if err := <-result; !errors.Is(err, ErrExpired) {
		t.Fatal(err)
	}
	if _, err := client.Read(context.Background(), protocolAttempt(t)); !errors.Is(err, ErrBusy) {
		t.Fatal("write completion abandoned", err)
	}
	closed, stop := context.WithCancel(context.Background())
	stop()
	if err := client.Close(closed); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	select {
	case <-client.Quiesced():
		t.Fatal("quiesced before write completion joined")
	default:
	}
	close(release)
	protocolWait(t, client.Quiesced())
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFenceReplyRequiresExactLiteralInbox(t *testing.T) {
	inbox := "_INBOX.kelvo-authfence.east.0123456789abcdef"
	valid := &nats.Msg{Subject: inbox, Data: []byte(`{"stream":"KELVO_AUTHORITY","seq":18}`)}
	raw, err := correlatedReply(inbox, valid)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] = '!'
	if valid.Data[0] != '{' {
		t.Fatal("reply bytes were borrowed")
	}
	for _, change := range []func(*nats.Msg){
		func(m *nats.Msg) { m.Subject = inbox + ".old" },
		func(m *nats.Msg) { m.Subject = "_INBOX.kelvo-authfence.west.other" },
		func(m *nats.Msg) { m.Reply = "unexpected.reply" },
		func(m *nats.Msg) { m.Header = nats.Header{"Status": []string{"503"}} },
		func(m *nats.Msg) { m.Header = nats.Header{"X-Unexpected": []string{"value"}} },
		func(m *nats.Msg) { m.Data = make([]byte, maxResponseBytes+1) },
	} {
		bad := *valid
		change(&bad)
		if _, err := correlatedReply(inbox, &bad); !errors.Is(err, ErrUnavailable) {
			t.Fatal("uncorrelated reply accepted")
		}
	}
	if _, err := correlatedReply(inbox, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}
