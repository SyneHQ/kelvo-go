// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
//go:build linux

package childipc

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type dialFunc func(context.Context, string, string) (net.Conn, error)

func (f dialFunc) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return f(ctx, network, address)
}

func echoDialer(t *testing.T) (dialFunc, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	var calls atomic.Int32
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(conn, conn); conn.Close() }()
		}
	}()
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		calls.Add(1)
		if network != "tcp" || address != "source.private:5432" {
			return nil, ErrChannel
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}, &calls
}

func closeServer(t *testing.T, s *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestInheritedChildHelper(t *testing.T) {
	if os.Getenv("KELVO_PRIVATE_IPC_HELPER") != "1" {
		return
	}
	client, err := NewClient(os.NewFile(3, "inherited-private-source"), "source.private:5432")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := client.DialContext(ctx, "tcp", "source.private:5432")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if conn.RemoteAddr().String() != "source.private:5432" {
		t.Fatal("source authority lost")
	}
	want := strings.Repeat("private-stream-fixture", 8192)
	done := make(chan error, 1)
	go func() { _, err := io.WriteString(conn, want); done <- err }()
	raw := make([]byte, len(want))
	if _, err := io.ReadFull(conn, raw); err != nil {
		t.Fatal(err)
	}
	if string(raw) != want || <-done != nil {
		t.Fatal("stream was corrupted")
	}
}

func TestInheritedChannelAuthenticatesActualChild(t *testing.T) {
	dialer, calls := echoDialer(t)
	s, file, err := NewPair(context.Background(), "source.private:5432", dialer, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestInheritedChildHelper$", "-test.timeout=5s")
	command.Env = append(os.Environ(), "KELVO_PRIVATE_IPC_HELPER=1")
	command.ExtraFiles = []*os.File{file}
	if err := command.Start(); err != nil {
		file.Close()
		closeServer(t, s)
		t.Fatal(err)
	}
	file.Close()
	served := make(chan error, 1)
	go func() { served <- s.Serve(command.Process.Pid) }()
	if err := command.Wait(); err != nil {
		closeServer(t, s)
		t.Fatal(err)
	}
	closeServer(t, s)
	<-served
	if calls.Load() != 1 {
		t.Fatal("unexpected physical connection count")
	}
}

func TestChannelRejectsWrongCallerAndSource(t *testing.T) {
	for _, wrongPID := range []bool{false, true} {
		t.Run(map[bool]string{false: "source", true: "caller"}[wrongPID], func(t *testing.T) {
			dialer, calls := echoDialer(t)
			s, file, err := NewPair(context.Background(), "source.private:5432", dialer, nil, 1)
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(file, "source.private:5432")
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			pid := os.Getpid()
			if wrongPID {
				pid++
			}
			go s.Serve(pid)
			address := "other.private:5432"
			if wrongPID {
				address = "source.private:5432"
			}
			if conn, err := client.DialContext(context.Background(), "tcp", address); err == nil {
				conn.Close()
				t.Fatal("unadmitted scope accepted")
			}
			closeServer(t, s)
			if calls.Load() != 0 {
				t.Fatal("denied request opened a source")
			}
		})
	}
}

func TestChannelBoundsStreamsAndJoinsOnClose(t *testing.T) {
	dialer, calls := echoDialer(t)
	s, file, err := NewPair(context.Background(), "source.private:5432", dialer, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(file, "source.private:5432")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	go s.Serve(os.Getpid())
	conn, err := client.DialContext(context.Background(), "tcp", "source.private:5432")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if other, err := client.DialContext(context.Background(), "tcp", "source.private:5432"); err == nil {
		other.Close()
		t.Fatal("capacity exceeded")
	}
	closeServer(t, s)
	if calls.Load() != 1 {
		t.Fatal("capacity check occurred after source opening")
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("closed execution retained source stream")
	}
}

func TestChannelCancellationRequiresExplicitParentCapability(t *testing.T) {
	dialer, calls := echoDialer(t)
	s, file, err := NewPair(context.Background(), "source.private:5432", dialer, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(file, "source.private:5432")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	go s.Serve(os.Getpid())
	if conn, err := client.DialCancellation(context.Background(), "tcp", "source.private:5432"); err == nil {
		conn.Close()
		t.Fatal("cancellation silently used data capacity")
	}
	closeServer(t, s)
	if calls.Load() != 0 {
		t.Fatal("unsupported cancellation opened data connection")
	}
}

type delayedClose struct {
	net.Conn
	release chan struct{}
	started chan struct{}
}

func (c *delayedClose) Close() error { close(c.started); <-c.release; return c.Conn.Close() }

func TestCloseDeadlineRetainsSingleCleanupOwner(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	blocked := &delayedClose{Conn: left, release: make(chan struct{}), started: make(chan struct{})}
	dialer := dialFunc(func(context.Context, string, string) (net.Conn, error) { return blocked, nil })
	s, file, err := NewPair(context.Background(), "source.private:5432", dialer, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(file, "source.private:5432")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	go s.Serve(os.Getpid())
	conn, err := client.DialContext(context.Background(), "tcp", "source.private:5432")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Close(ctx); err == nil {
		t.Fatal("unconfirmed close reported complete")
	}
	<-blocked.started
	close(blocked.release)
	closeServer(t, s)
}

func TestInvalidAuthorityConsumesInheritedFile(t *testing.T) {
	dialer, _ := echoDialer(t)
	s, file, err := NewPair(context.Background(), "source.private:5432", dialer, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if client, err := NewClient(file, "invalid"); err == nil {
		client.Close()
		t.Fatal("invalid authority accepted")
	}
	if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("rejected inherited descriptor remains open")
	}
	closeServer(t, s)
}

func TestChannelRejectsMalformedRequestsBeforeDial(t *testing.T) {
	for _, tc := range []struct {
		name   string
		data   []byte
		rights bool
	}{
		{"short", []byte{1}, false}, {"oversized", make([]byte, 4096), false},
		{"version", []byte{2, 1}, false}, {"purpose", []byte{1, 3}, false},
		{"descriptor", []byte{1, 1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dialer, calls := echoDialer(t)
			s, file, err := NewPair(context.Background(), "source.private:5432", dialer, nil, 1)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := net.FileConn(file)
			file.Close()
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			control := conn.(*net.UnixConn)
			served := make(chan error, 1)
			go func() { served <- s.Serve(os.Getpid()) }()
			var rights []byte
			var peer *os.File
			if tc.rights {
				read, write, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				defer read.Close()
				peer = write
				rights = unix.UnixRights(int(write.Fd()))
				// The receiver must close the delivered descriptor on rejection.
				if err := read.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				defer func() {
					var b [1]byte
					if _, err := read.Read(b[:]); err != io.EOF {
						t.Errorf("received descriptor leaked: %v", err)
					}
				}()
			}
			if _, _, err := control.WriteMsgUnix(tc.data, rights, nil); err != nil {
				t.Fatal(err)
			}
			if peer != nil {
				peer.Close()
			}
			select {
			case err := <-served:
				if err == nil {
					t.Fatal("malformed message accepted")
				}
			case <-time.After(time.Second):
				t.Fatal("malformed message stalled admission")
			}
			closeServer(t, s)
			if calls.Load() != 0 {
				t.Fatal("malformed request opened source connection")
			}
		})
	}
}

type failedClose struct{ net.Conn }

func (c *failedClose) Close() error { c.Conn.Close(); return errors.New("fixture close failure") }
func TestCloseReportsPhysicalCleanupFailure(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	dialer := dialFunc(func(context.Context, string, string) (net.Conn, error) { return &failedClose{left}, nil })
	s, file, err := NewPair(context.Background(), "source.private:5432", dialer, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(file, "source.private:5432")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	go s.Serve(os.Getpid())
	conn, err := client.DialContext(context.Background(), "tcp", "source.private:5432")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); !errors.Is(err, ErrChannel) {
		t.Fatalf("cleanup failure not retained: %v", err)
	}
	if err := s.Close(ctx); !errors.Is(err, ErrChannel) {
		t.Fatalf("repeated cleanup lost failure: %v", err)
	}
}

func TestDataOpenBudgetExceedsLegacyLimit(t *testing.T) {
	dial, _ := echoDialer(t)
	delayed := dialFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		timer := time.NewTimer(2200 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return dial(ctx, network, address)
	})
	s, file, err := NewPair(context.Background(), "source.private:5432", delayed, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeServer(t, s)
	op, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = s.ConfigureDataOpen(op, 4*time.Second); err != nil {
		t.Fatal(err)
	}
	client, err := NewClientWithDataOpenTimeout(file, "source.private:5432", 4000)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	go s.Serve(os.Getpid())
	conn, err := client.DialContext(op, "tcp", "source.private:5432")
	if err != nil {
		t.Fatal("descriptor wait retained old two-second deadline", err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 2)
	if _, err = io.ReadFull(conn, body); err != nil || string(body) != "ok" {
		t.Fatal(err)
	}
}

func TestDataOpenCancellationJoinsResolverWithoutCancellingCustody(t *testing.T) {
	for _, mode := range []string{"operation_cancel", "operation_deadline", "child_cancel_then_cleanup"} {
		t.Run(mode, func(t *testing.T) {
			entered, joined := make(chan struct{}), make(chan struct{})
			delayed := dialFunc(func(ctx context.Context, _, _ string) (net.Conn, error) {
				close(entered)
				<-ctx.Done()
				close(joined)
				return nil, ctx.Err()
			})
			life, cancelLife := context.WithCancel(context.Background())
			defer cancelLife()
			op, cancelOp := context.WithCancel(context.Background())
			defer cancelOp()
			if mode == "operation_deadline" {
				var stop context.CancelFunc
				op, stop = context.WithTimeout(op, 80*time.Millisecond)
				defer stop()
			}
			s, file, err := NewPair(life, "source.private:5432", delayed, nil, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.ConfigureDataOpen(op, 10*time.Second); err != nil {
				t.Fatal(err)
			}
			client, err := NewClientWithDataOpenTimeout(file, "source.private:5432", 10000)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			go s.Serve(os.Getpid())
			child, cancelChild := context.WithCancel(context.Background())
			defer cancelChild()
			result := make(chan error, 1)
			go func() {
				conn, err := client.DialContext(child, "tcp", "source.private:5432")
				if conn != nil {
					conn.Close()
				}
				result <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("resolver not entered")
			}
			if mode == "operation_cancel" {
				cancelOp()
			}
			if mode == "child_cancel_then_cleanup" {
				cancelChild()
				select {
				case err := <-result:
					if err == nil {
						t.Fatal("child cancellation succeeded")
					}
				case <-time.After(time.Second):
					t.Fatal("child did not stop")
				}
				// The worker calls Close after the child exits; that call must cancel and join resolution.
				closeServer(t, s)
			}
			select {
			case <-joined:
			case <-time.After(time.Second):
				t.Fatal("resolver outlived cancellation/cleanup")
			}
			if mode != "child_cancel_then_cleanup" {
				select {
				case err := <-result:
					if err == nil {
						t.Fatal("cancelled open succeeded")
					}
				case <-time.After(time.Second):
					t.Fatal("child did not observe failure")
				}
				closeServer(t, s)
			}
			if life.Err() != nil {
				t.Fatal("data cancellation revoked runtime cleanup lifetime")
			}
		})
	}
}

func TestDataOpenTimeoutDefaultsAndIntegerBounds(t *testing.T) {
	dial, _ := echoDialer(t)
	for _, timeout := range []int64{-1, 32001, 1 << 62} {
		s, file, err := NewPair(context.Background(), "source.private:5432", dial, nil, 1)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := NewClientWithDataOpenTimeout(file, "source.private:5432", timeout); err == nil {
			t.Fatal("invalid milliseconds accepted")
		}
		closeServer(t, s)
	}
	s, file, err := NewPair(context.Background(), "source.private:5432", dial, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeServer(t, s)
	client, err := NewClientWithDataOpenTimeout(file, "source.private:5432", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.dataOpenTimeout != 2*time.Second || s.dataOpenTimeout != 2*time.Second {
		t.Fatal("ordinary open default changed")
	}
}
