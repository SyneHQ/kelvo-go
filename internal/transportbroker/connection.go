// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package transportbroker

import (
	"net"
	"sync"
)

type connection struct {
	net.Conn
	session *Session
	purpose Purpose
	once    sync.Once
	closed  chan struct{}
	err     error
}

// PostgreSQL cancellation derives its destination from RemoteAddr. Expose the
// admitted database authority, never the proxy's address. This is routing
// identity only; the driver must still verify the source TLS hostname itself.
func (c *connection) RemoteAddr() net.Addr { return sourceAddress(c.session.binding.Authority) }

type sourceAddress string

func (sourceAddress) Network() string  { return "tcp" }
func (a sourceAddress) String() string { return string(a) }

func (c *connection) Close() error {
	c.once.Do(func() {
		defer close(c.closed)
		err := closeConnection(c.Conn)
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
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return ErrUnsupported
}

func (c *connection) CloseRead() error {
	if conn, ok := c.Conn.(interface{ CloseRead() error }); ok {
		return conn.CloseRead()
	}
	return ErrUnsupported
}
