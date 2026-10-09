// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
//go:build linux

// Package childipc gives one admitted child an inherited, source-scoped dial
// channel. The parent retains TLS, tickets, authority and physical cleanup.
package childipc

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"golang.org/x/sys/unix"
)

const setupLimit = 2 * time.Second
const ancillaryBytes = 2048

var ErrChannel = errors.New("private source child channel is unavailable")

type Server struct {
	ctx                                       context.Context
	cancel                                    context.CancelFunc
	control                                   *net.UnixConn
	data, cancellation                        transportbroker.Dialer
	authority                                 string
	maximum                                   int
	mu                                        sync.Mutex
	started, closed                           bool
	streams                                   map[*bridge]struct{}
	workers                                   sync.WaitGroup
	done                                      chan struct{}
	finish                                    sync.Once
	shutdown                                  sync.Once
	closedPhysical                            chan struct{}
	cleanupFailed                             bool
	postgresCleanup                           adapter.PostgresCleanup
	dataOpened, registered, aborted, observed bool
	cleanupHandle                             string
}

// NewPair creates no listener or socket pathname. The caller passes only the
// returned file to the admitted child and closes its copy immediately after
// Start. Serve must use the actual process ID reported by that Start.
// ctx must remain valid until execution cleanup completes. Do not use the
// SQL request context: cancellation can require a new physical connection.
func NewPair(ctx context.Context, authority string, data, cancellation transportbroker.Dialer, maxStreams int) (*Server, *os.File, error) {
	if ctx == nil || ctx.Err() != nil || transportbroker.ValidateAuthority(authority) != nil || data == nil || maxStreams < 1 || maxStreams > 64 {
		return nil, nil, ErrChannel
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, ErrChannel
	}
	if unix.SetsockoptInt(fds[0], unix.SOL_SOCKET, unix.SO_PASSCRED, 1) != nil {
		unix.Close(fds[0])
		unix.Close(fds[1])
		return nil, nil, ErrChannel
	}
	parent := os.NewFile(uintptr(fds[0]), "private-source-parent")
	child := os.NewFile(uintptr(fds[1]), "private-source-child")
	conn, err := net.FileConn(parent)
	parent.Close()
	if err != nil {
		child.Close()
		return nil, nil, ErrChannel
	}
	control, ok := conn.(*net.UnixConn)
	if !ok {
		conn.Close()
		child.Close()
		return nil, nil, ErrChannel
	}
	lifetime, cancel := context.WithCancel(ctx)
	s := &Server{ctx: lifetime, cancel: cancel, control: control, data: data, cancellation: cancellation, authority: authority, maximum: maxStreams, streams: map[*bridge]struct{}{}, done: make(chan struct{}), closedPhysical: make(chan struct{})}
	return s, child, nil
}

// Serve authenticates each request with kernel-provided SCM_CREDENTIALS. It
// accepts no route, source, endpoint, grant or descriptor from the child.
// Closing the control channel cancels all outstanding source connections.
func (s *Server) Serve(childPID int) error {
	if s == nil || childPID < 1 {
		return ErrChannel
	}
	s.mu.Lock()
	if s.started || s.closed {
		s.mu.Unlock()
		return ErrChannel
	}
	s.started = true
	s.mu.Unlock()
	stop := context.AfterFunc(s.ctx, func() { s.closeConnections() })
	defer stop()
	defer func() {
		s.closeConnections()
		s.workers.Wait()
		<-s.closedPhysical
		s.finish.Do(func() { close(s.done) })
	}()
	for {
		var data [512]byte
		oob := make([]byte, ancillaryBytes)
		n, on, flags, _, err := s.control.ReadMsgUnix(data[:], oob)
		credentials, rights, parseErr := parseControl(oob[:on])
		for _, fd := range rights {
			unix.Close(fd)
		}
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			return ErrChannel
		}
		if parseErr != nil || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || n < 2 || data[0] != 1 || len(rights) != 0 || credentials == nil || int(credentials.Pid) != childPID {
			return ErrChannel
		}
		if data[1] >= 3 {
			err := s.servePostgresCleanup(data[:n])
			clear(data[:])
			if err != nil {
				return err
			}
			continue
		}
		if n != 2 || s.postgresCleanup != nil && (data[1] != 1 || s.dataOpened) {
			return ErrChannel
		}
		var dialer transportbroker.Dialer
		switch data[1] {
		case 1:
			dialer = s.data
		case 2:
			dialer = s.cancellation
		default:
			return ErrChannel
		}
		if dialer == nil {
			return ErrChannel
		}
		s.mu.Lock()
		full := len(s.streams) >= s.maximum || s.closed
		s.mu.Unlock()
		if full {
			return ErrChannel
		}
		ctx, cancel := context.WithTimeout(s.ctx, setupLimit)
		remote, err := dialer.DialContext(ctx, "tcp", s.authority)
		cancel()
		if err != nil || remote == nil {
			if remote != nil {
				s.recordCleanup(remote.Close())
			}
			return ErrChannel
		}
		stream, child, err := newBridge(remote)
		if err != nil {
			s.recordCleanup(remote.Close())
			return ErrChannel
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			child.Close()
			stream.close()
			s.recordCleanup(stream.err)
			return ErrChannel
		}
		s.streams[stream] = struct{}{}
		s.workers.Add(1)
		s.mu.Unlock()
		if s.control.SetWriteDeadline(time.Now().Add(setupLimit)) != nil {
			child.Close()
			stream.close()
			s.remove(stream)
			return ErrChannel
		}
		written, _, err := s.control.WriteMsgUnix([]byte{1, 0}, unix.UnixRights(int(child.Fd())), nil)
		child.Close()
		if err != nil || written != 2 {
			stream.close()
			s.remove(stream)
			return ErrChannel
		}
		s.dataOpened = true
		go func() { stream.run(); s.remove(stream) }()
	}
}

func (s *Server) recordCleanup(err error) {
	if err != nil {
		s.mu.Lock()
		s.cleanupFailed = true
		s.mu.Unlock()
	}
}

func (s *Server) remove(b *bridge) {
	s.mu.Lock()
	if b.err != nil {
		s.cleanupFailed = true
	}
	delete(s.streams, b)
	s.mu.Unlock()
	s.workers.Done()
}
func (s *Server) closeConnections() {
	s.cancel()
	s.control.Close()
	s.shutdown.Do(func() {
		s.mu.Lock()
		s.closed = true
		streams := make([]*bridge, 0, len(s.streams))
		for b := range s.streams {
			streams = append(streams, b)
		}
		s.mu.Unlock()
		go func() {
			defer close(s.closedPhysical)
			for _, b := range streams {
				b.close()
			}
		}()
	})
}

// Close joins control and stream owners. The caller must retain execution
// custody if the deadline expires before this method confirms completion.
func (s *Server) Close(ctx context.Context) error {
	if s == nil || ctx == nil {
		return ErrChannel
	}
	s.closeConnections()
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		s.finish.Do(func() { go func() { <-s.closedPhysical; close(s.done) }() })
	}
	select {
	case <-s.done:
		s.mu.Lock()
		failed := s.cleanupFailed
		s.mu.Unlock()
		if failed {
			return ErrChannel
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type bridge struct {
	local  *net.UnixConn
	remote net.Conn
	once   sync.Once
	err    error
}

func newBridge(remote net.Conn) (*bridge, *os.File, error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, ErrChannel
	}
	parent := os.NewFile(uintptr(fds[0]), "private-stream-parent")
	child := os.NewFile(uintptr(fds[1]), "private-stream-child")
	conn, err := net.FileConn(parent)
	parent.Close()
	if err != nil {
		child.Close()
		return nil, nil, ErrChannel
	}
	local, ok := conn.(*net.UnixConn)
	if !ok {
		conn.Close()
		child.Close()
		return nil, nil, ErrChannel
	}
	return &bridge{local: local, remote: remote}, child, nil
}
func (b *bridge) close() { b.once.Do(func() { b.local.Close(); b.err = b.remote.Close() }) }
func (b *bridge) run() {
	finished := make(chan struct{}, 1)
	go func() {
		_, err := io.Copy(b.remote, b.local)
		if writer, ok := b.remote.(interface{ CloseWrite() error }); err == nil && ok {
			if writer.CloseWrite() != nil {
				b.close()
			}
		} else {
			b.close()
		}
		finished <- struct{}{}
	}()
	_, err := io.Copy(b.local, b.remote)
	if err != nil || b.local.CloseWrite() != nil {
		b.close()
	}
	<-finished
	b.close()
}

func parseControl(raw []byte) (*unix.Ucred, []int, error) {
	messages, err := unix.ParseSocketControlMessage(raw)
	if err != nil {
		return nil, nil, ErrChannel
	}
	var credentials *unix.Ucred
	var rights []int
	var invalid bool
	for _, message := range messages {
		if message.Header.Level != unix.SOL_SOCKET {
			invalid = true
			continue
		}
		switch message.Header.Type {
		case unix.SCM_CREDENTIALS:
			if credentials != nil {
				invalid = true
			}
			value, err := unix.ParseUnixCredentials(&message)
			if err != nil {
				invalid = true
			} else {
				credentials = value
			}
		case unix.SCM_RIGHTS:
			values, err := unix.ParseUnixRights(&message)
			if err != nil {
				invalid = true
			}
			rights = append(rights, values...)
		default:
			invalid = true
		}
	}
	if invalid {
		return credentials, rights, ErrChannel
	}
	return credentials, rights, nil
}

type Client struct {
	control   *net.UnixConn
	authority string
	slot      chan struct{}
	once      sync.Once
	done      chan struct{}
}

// NewClient consumes the inherited descriptor. The child receives no parent
// credentials. A failed/cancelled exchange permanently closes this channel.
func NewClient(file *os.File, authority string) (*Client, error) {
	if file == nil {
		return nil, ErrChannel
	}
	if transportbroker.ValidateAuthority(authority) != nil {
		file.Close()
		return nil, ErrChannel
	}
	conn, err := net.FileConn(file)
	file.Close()
	if err != nil {
		return nil, ErrChannel
	}
	control, ok := conn.(*net.UnixConn)
	if !ok {
		conn.Close()
		return nil, ErrChannel
	}
	return &Client{control: control, authority: authority, slot: make(chan struct{}, 1), done: make(chan struct{})}, nil
}
func (c *Client) Close() error {
	if c == nil {
		return ErrChannel
	}
	c.once.Do(func() { c.control.Close(); close(c.done) })
	return nil
}
func (c *Client) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return c.dial(ctx, network, address, 1)
}
func (c *Client) DialCancellation(ctx context.Context, network, address string) (net.Conn, error) {
	return c.dial(ctx, network, address, 2)
}
func (c *Client) dial(ctx context.Context, network, address string, purpose byte) (net.Conn, error) {
	if c == nil || ctx == nil || network != "tcp" || address != c.authority {
		return nil, ErrChannel
	}
	select {
	case c.slot <- struct{}{}:
		defer func() { <-c.slot }()
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrChannel
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	deadline := time.Now().Add(setupLimit)
	if until, ok := ctx.Deadline(); ok && until.Before(deadline) {
		deadline = until
	}
	if c.control.SetDeadline(deadline) != nil {
		c.Close()
		return nil, ErrChannel
	}
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if n, _, err := c.control.WriteMsgUnix([]byte{1, purpose}, nil, nil); err != nil || n != 2 {
		c.Close()
		return nil, ErrChannel
	}
	var data [3]byte
	oob := make([]byte, ancillaryBytes)
	n, on, flags, _, err := c.control.ReadMsgUnix(data[:], oob)
	credentials, rights, parseErr := parseControl(oob[:on])
	if err != nil || parseErr != nil || credentials != nil || n != 2 || data[0] != 1 || data[1] != 0 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || len(rights) != 1 || ctx.Err() != nil {
		for _, fd := range rights {
			unix.Close(fd)
		}
		c.Close()
		return nil, ErrChannel
	}
	unix.CloseOnExec(rights[0])
	file := os.NewFile(uintptr(rights[0]), "private-source-stream")
	conn, err := net.FileConn(file)
	file.Close()
	if err != nil {
		c.Close()
		return nil, ErrChannel
	}
	if c.control.SetDeadline(time.Time{}) != nil {
		conn.Close()
		c.Close()
		return nil, ErrChannel
	}
	return &sourceConn{conn: conn, authority: c.authority}, nil
}

type sourceConn struct {
	conn      net.Conn
	authority string
}
type sourceAddress string

func (sourceAddress) Network() string                    { return "tcp" }
func (a sourceAddress) String() string                   { return string(a) }
func (c *sourceConn) Read(b []byte) (int, error)         { return c.conn.Read(b) }
func (c *sourceConn) Write(b []byte) (int, error)        { return c.conn.Write(b) }
func (c *sourceConn) Close() error                       { return c.conn.Close() }
func (c *sourceConn) LocalAddr() net.Addr                { return c.conn.LocalAddr() }
func (c *sourceConn) RemoteAddr() net.Addr               { return sourceAddress(c.authority) }
func (c *sourceConn) SetDeadline(t time.Time) error      { return c.conn.SetDeadline(t) }
func (c *sourceConn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *sourceConn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }
func (c *sourceConn) CloseWrite() error {
	if conn, ok := c.conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return ErrChannel
}
