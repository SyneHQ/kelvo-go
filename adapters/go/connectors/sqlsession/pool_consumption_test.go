package sqlsession

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type ownedPoolConnector struct {
	opening, release                   chan struct{}
	closes, execs, commits, afterClose atomic.Int32
}

func (c *ownedPoolConnector) Connect(ctx context.Context) (driver.Conn, error) {
	close(c.opening)
	select {
	case <-c.release:
		return &ownedPoolConn{state: c}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (c *ownedPoolConnector) Driver() driver.Driver { return ownedPoolDriver{} }

type ownedPoolDriver struct{}

func (ownedPoolDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("unexpected driver open")
}

type ownedPoolConn struct {
	state  *ownedPoolConnector
	closed atomic.Bool
}

func (c *ownedPoolConn) used() {
	if c.closed.Load() {
		c.state.afterClose.Add(1)
	}
}
func (c *ownedPoolConn) Prepare(string) (driver.Stmt, error) {
	c.used()
	return nil, errors.New("unexpected prepare")
}
func (c *ownedPoolConn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return errors.New("closed twice")
	}
	c.state.closes.Add(1)
	return nil
}
func (c *ownedPoolConn) Begin() (driver.Tx, error) { c.used(); return ownedPoolTx{c}, nil }
func (c *ownedPoolConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c.Begin()
}
func (c *ownedPoolConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.used()
	c.state.execs.Add(1)
	return driver.RowsAffected(1), nil
}

type ownedPoolTx struct{ conn *ownedPoolConn }

func (t ownedPoolTx) Commit() error   { t.conn.used(); t.conn.state.commits.Add(1); return nil }
func (t ownedPoolTx) Rollback() error { t.conn.used(); return nil }

func TestOperationConsumesPoolBeforeQueuedBorrowerCanReuseSession(t *testing.T) {
	state := &ownedPoolConnector{opening: make(chan struct{}), release: make(chan struct{})}
	pool := sql.OpenDB(state)
	pool.SetMaxOpenConns(1)
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		result, err := Execute(ctx, pool, "postgresql", []string{"UPDATE items SET n=1"}, Options{Transaction: true})
		if err == nil && result.Outcome != Succeeded {
			err = errors.New("held operation did not succeed")
		}
		completed <- err
	}()
	select {
	case <-state.opening:
	case <-ctx.Done():
		t.Fatal("operation did not acquire source")
	}
	borrower := make(chan error, 1)
	go func() {
		conn, err := pool.Conn(ctx)
		if conn != nil {
			conn.Close()
		}
		borrower <- err
	}()
	for pool.Stats().WaitCount == 0 && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("borrower did not queue")
	}
	close(state.release)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	if err := <-borrower; err == nil {
		t.Fatal("queued borrower received consumed session")
	}
	if conn, err := pool.Conn(context.Background()); err == nil {
		conn.Close()
		t.Fatal("new borrower received consumed session")
	}
	if state.execs.Load() != 1 || state.commits.Load() != 1 || state.closes.Load() != 1 || state.afterClose.Load() != 0 {
		t.Fatal("held operation or physical cleanup failed")
	}
}
