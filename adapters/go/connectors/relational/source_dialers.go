// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package relational

import (
	"context"
	"database/sql/driver"
	"net"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/stdlib"
)

type sourceDialFunc func(context.Context, string, string) (net.Conn, error)
type cancellationPurpose struct{}
type dataPurpose struct{}
type dataScope struct{ tenant, connection, revision, authority string }

func dialScope(c adapter.Connection) dataScope {
	return dataScope{c.TenantID, c.ConnectionID, c.Revision, net.JoinHostPort(c.Host, strconv.Itoa(c.Port))}
}

type sourceConnector struct {
	driver.Connector
	scope dataScope
}

func (c sourceConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.Connector.Connect(context.WithValue(ctx, dataPurpose{}, c.scope))
}
func sourcePostgresConnector(config *pgx.ConnConfig, c adapter.Connection) driver.Connector {
	return sourceConnector{Connector: stdlib.GetConnector(*config), scope: dialScope(c)}
}

func validSourceDialers(c adapter.Connection) bool {
	if c.DialContext == nil {
		return c.DialCancellation == nil && c.PostgresCleanup == nil
	}
	return c.Engine == "mysql" && c.PostgresCleanup == nil || c.Engine == "postgresql" && ((c.DialCancellation != nil) != (c.PostgresCleanup != nil))
}

// The driver must retain the original source address for TLS and cancellation.
// The hook owns the physical transport. Neither DNS nor direct dialing is a fallback.
func scopedSourceDialer(c adapter.Connection, dial sourceDialFunc) sourceDialFunc {
	authority := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if ctx == nil || network != "tcp" || address != authority || dial == nil {
			return nil, adapter.ErrInvalid
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := dial(ctx, network, address)
		if err != nil || conn == nil || ctx.Err() != nil {
			if conn != nil {
				conn.Close()
			}
			if err != nil {
				return nil, err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, adapter.ErrInvalid
		}
		return &sourceConnection{Conn: conn, authority: sourceAddress(authority)}, nil
	}
}

type sourceAddress string

func (a sourceAddress) Network() string { return "tcp" }
func (a sourceAddress) String() string  { return string(a) }

type sourceConnection struct {
	net.Conn
	authority sourceAddress
}

func (c *sourceConnection) RemoteAddr() net.Addr { return c.authority }

func configurePostgresSourceDialers(config *pgx.ConnConfig, c adapter.Connection) {
	data := scopedSourceDialer(c, c.DialContext)
	cancel := scopedSourceDialer(c, c.DialCancellation)
	config.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
		if ctx == nil || host != c.Host {
			return nil, adapter.ErrInvalid
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return []string{c.Host}, nil
	}
	config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if ctx != nil && ctx.Value(dataPurpose{}) != nil && ctx.Value(dataPurpose{}) != dialScope(c) {
			return nil, adapter.ErrInvalid
		}
		if ctx != nil && ctx.Value(cancellationPurpose{}) != true && ctx.Value(dataPurpose{}) == dialScope(c) {
			return data(ctx, network, address)
		}
		// pgx also cancels from asyncClose with a new context. Only the
		// connector can mark a data open; all other opens get cancellation
		// authority. A denied cancellation never retries the data hook.
		return cancel(ctx, network, address)
	}
	if c.PostgresCleanup != nil {
		configureTypedPostgresCleanup(config, c.PostgresCleanup)
		return
	}
	config.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &sourceCancellationWatcher{conn: conn}
	}
}

// Match pgx's bounded cancellation lifecycle, with an explicit channel purpose.
// Unwatch waits for cancellation before the pool can reuse the connection.
type sourceCancellationWatcher struct {
	conn *pgconn.PgConn
	stop context.CancelFunc
	done chan struct{}
}

func (h *sourceCancellationWatcher) HandleCancel(context.Context) {
	deadline := time.Now().Add(500 * time.Millisecond)
	h.conn.Conn().SetDeadline(deadline)
	ctx, stop := context.WithDeadline(context.Background(), deadline)
	h.stop, h.done = stop, make(chan struct{})
	go func() {
		defer close(h.done)
		defer stop()
		h.conn.CancelRequest(context.WithValue(ctx, cancellationPurpose{}, true))
		// pgx keeps this interval because the server can acknowledge the cancel
		// connection before it delivers cancellation to the query connection.
		time.Sleep(100 * time.Millisecond)
	}()
}
func (h *sourceCancellationWatcher) HandleUnwatchAfterCancel() {
	h.stop()
	<-h.done
	h.conn.Conn().SetDeadline(time.Time{})
}
