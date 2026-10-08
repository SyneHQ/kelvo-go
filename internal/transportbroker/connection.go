// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package transportbroker

import (
	"net"
	"sync"
	"time"
)

type connection struct {
	conn    net.Conn
	self    *connection
	session *Session
	purpose Purpose
	once    sync.Once
	closed  chan struct{}
	err     error
}

// PostgreSQL cancellation derives its destination from RemoteAddr. Expose the
// admitted database authority, never the proxy's address. This is routing
// identity only; the driver must still verify the source TLS hostname itself.
func (c *connection) RemoteAddr() net.Addr {
	if c == nil || c.self != c {
		return nil
	}
	return sourceAddress(c.session.binding.Authority)
}

type sourceAddress string

func (sourceAddress) Network() string  { return "tcp" }
func (a sourceAddress) String() string { return string(a) }

func (c *connection) Close() error {
	if c == nil || c.self != c {
		return ErrInvalid
	}
	c.once.Do(func() {
		defer close(c.closed)
		err := closeConnection(c.conn)
		b := c.session.broker
		b.mu.Lock()
		defer b.mu.Unlock()
		if err != nil {
			c.err = ErrCleanup
			b.state.CleanupFailed, b.state.Draining = true, true
			return
		}
		delete(c.session.connections, c)
		c.session.releaseLocked(c.purpose)
	})
	<-c.closed
	return c.err
}

func closeConnection(conn net.Conn) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrCleanup
		}
	}()
	return conn.Close()
}

func (c *connection) CloseWrite() error {
	if c == nil || c.self != c {
		return ErrInvalid
	}
	if conn, ok := c.conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return ErrUnsupported
}

func (c *connection) CloseRead() error {
	if c == nil || c.self != c {
		return ErrInvalid
	}
	if conn, ok := c.conn.(interface{ CloseRead() error }); ok {
		return conn.CloseRead()
	}
	return ErrUnsupported
}

// Keep the accepted physical socket private. An exported embedded net.Conn
// would let a caller replace the target that Close confirms to accounting.
func (c *connection) Read(p []byte) (int, error) {
	if c == nil || c.self != c {
		return 0, ErrInvalid
	}
	return c.conn.Read(p)
}
func (c *connection) Write(p []byte) (int, error) {
	if c == nil || c.self != c {
		return 0, ErrInvalid
	}
	return c.conn.Write(p)
}
func (c *connection) LocalAddr() net.Addr {
	if c == nil || c.self != c {
		return nil
	}
	return c.conn.LocalAddr()
}
func (c *connection) SetDeadline(t time.Time) error {
	if c == nil || c.self != c {
		return ErrInvalid
	}
	return c.conn.SetDeadline(t)
}
func (c *connection) SetReadDeadline(t time.Time) error {
	if c == nil || c.self != c {
		return ErrInvalid
	}
	return c.conn.SetReadDeadline(t)
}
func (c *connection) SetWriteDeadline(t time.Time) error {
	if c == nil || c.self != c {
		return ErrInvalid
	}
	return c.conn.SetWriteDeadline(t)
}
