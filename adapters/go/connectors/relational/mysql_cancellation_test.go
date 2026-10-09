package relational

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type cancelFixture struct {
	id      int64
	entered chan string
	release chan struct{}
	closes  atomic.Int32
	fail    bool
}
type cancelConnector struct {
	f       *cancelFixture
	control bool
}

func (c cancelConnector) Driver() driver.Driver                        { return cancelDriver{c} }
func (c cancelConnector) Connect(context.Context) (driver.Conn, error) { return &cancelConn{c}, nil }

type cancelDriver struct{ c cancelConnector }

func (d cancelDriver) Open(string) (driver.Conn, error) { return d.c.Connect(context.Background()) }

type cancelConn struct{ c cancelConnector }

func (c *cancelConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *cancelConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c *cancelConn) Close() error              { c.c.f.closes.Add(1); return nil }
func (c *cancelConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if q != "SELECT CONNECTION_ID()" || c.c.control {
		return nil, errors.New("unexpected query")
	}
	return &cancelIDRows{id: c.c.f.id}, nil
}
func (c *cancelConn) ExecContext(ctx context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	if !c.c.control {
		return nil, errors.New("unexpected exec")
	}
	c.c.f.entered <- q
	select {
	case <-c.c.f.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if c.c.f.fail {
		return nil, errors.New("fixture transport failure")
	}
	return driver.RowsAffected(0), nil
}

type cancelIDRows struct {
	id   int64
	read bool
}

func (r *cancelIDRows) Columns() []string { return []string{"CONNECTION_ID()"} }
func (r *cancelIDRows) Close() error      { return nil }
func (r *cancelIDRows) Next(v []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	v[0] = r.id
	return nil
}

func TestMySQLCancellationJoinsControlBeforeDataContext(t *testing.T) {
	f := &cancelFixture{id: 173, entered: make(chan string, 1), release: make(chan struct{})}
	pool := sql.OpenDB(cancelConnector{f: f})
	defer pool.Close()
	data, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state, err := beginMySQLReadCancellation(ctx, data, func(context.Context) (*sql.DB, error) { return sql.OpenDB(cancelConnector{f: f, control: true}), nil })
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case q := <-f.entered:
		if q != "KILL QUERY 173" {
			t.Fatal(q)
		}
	case <-time.After(time.Second):
		t.Fatal("control connection did not start")
	}
	if state.context.Err() != nil || f.closes.Load() != 0 {
		t.Fatal("data cancellation or connection close preceded KILL completion")
	}
	finished := make(chan error, 1)
	go func() { finished <- state.finish() }()
	select {
	case <-finished:
		t.Fatal("finish did not join control")
	case <-time.After(20 * time.Millisecond):
	}
	close(f.release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if state.context.Err() == nil || f.closes.Load() != 1 {
		t.Fatal("control connection was not closed before data cancellation")
	}
	if err := state.finish(); err != nil {
		t.Fatal(err)
	}
}
func TestMySQLCancellationSuccessDoesNotOpenControl(t *testing.T) {
	f := &cancelFixture{id: 42}
	pool := sql.OpenDB(cancelConnector{f: f})
	defer pool.Close()
	data, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	state, err := beginMySQLReadCancellation(context.Background(), data, func(context.Context) (*sql.DB, error) {
		t.Error("successful query opened control")
		return nil, errors.New("unexpected")
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.finish(); err != nil {
		t.Fatal(err)
	}
}
func TestMySQLCancellationFailureRemainsUnconfirmed(t *testing.T) {
	f := &cancelFixture{id: 42, entered: make(chan string, 1), release: make(chan struct{}), fail: true}
	close(f.release)
	pool := sql.OpenDB(cancelConnector{f: f})
	defer pool.Close()
	data, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state, err := beginMySQLReadCancellation(ctx, data, func(context.Context) (*sql.DB, error) { return sql.OpenDB(cancelConnector{f: f, control: true}), nil })
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := state.finish(); !errors.Is(err, errMySQLCancellation) {
		t.Fatal("unconfirmed KILL was reported as successful", err)
	}
}
func TestMySQLCancellationRejectsInvalidServerID(t *testing.T) {
	for _, id := range []int64{0, -1, 1 << 32} {
		f := &cancelFixture{id: id}
		pool := sql.OpenDB(cancelConnector{f: f})
		data, err := pool.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, err = beginMySQLReadCancellation(context.Background(), data, func(context.Context) (*sql.DB, error) { t.Error("invalid ID opened control"); return nil, nil })
		data.Close()
		pool.Close()
		if err == nil {
			t.Fatal("invalid server ID accepted", id)
		}
	}
}
