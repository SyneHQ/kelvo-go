// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
//go:build linux

package childipc

import (
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
