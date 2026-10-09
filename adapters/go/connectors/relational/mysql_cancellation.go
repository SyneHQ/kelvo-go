package relational

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	mysqldriver "github.com/go-sql-driver/mysql"
)

var errMySQLCancellation = errors.New("MySQL server cancellation was not confirmed")

// The caller must keep the data connection pinned until finish returns. The
// control connection uses the same source account and a separate reserved dialer.
// This path is dormant unless trusted composition supplies DialCancellation.
func mysqlCancellationPool(c adapter.Connection, config *mysqldriver.Config) func(context.Context) (*sql.DB, error) {
	if c.Engine != "mysql" || c.DialContext == nil || c.DialCancellation == nil {
		return nil
	}
	cancelConfig := config.Clone()
	cancelConfig.DialFunc = scopedSourceDialer(c, c.DialCancellation)
	cancelConfig.Timeout = time.Second
	cancelConfig.ReadTimeout = time.Second
	cancelConfig.WriteTimeout = time.Second
	return func(ctx context.Context) (*sql.DB, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		connector, err := mysqldriver.NewConnector(cancelConfig.Clone())
		if err != nil {
			return nil, adapter.ErrInvalid
		}
		pool := sql.OpenDB(connector)
		pool.SetMaxOpenConns(1)
		pool.SetMaxIdleConns(0)
		return pool, nil
	}
}

type mysqlReadCancellation struct {
	context context.Context
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
	err     error
}

func beginMySQLReadCancellation(ctx context.Context, data *sql.Conn, open func(context.Context) (*sql.DB, error)) (*mysqlReadCancellation, error) {
	if open == nil {
		return &mysqlReadCancellation{context: ctx}, nil
	}
	// Only the authenticated server can select the numeric target. No statement,
	// source ID or caller parameter can supply a connection ID.
	var id uint64
	if err := data.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id); err != nil {
		return nil, err
	}
	if id == 0 || id > 1<<32-1 {
		return nil, adapter.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	work, cancel := context.WithCancel(context.WithoutCancel(ctx))
	state := &mysqlReadCancellation{context: work, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(state.done)
		defer cancel()
		select {
		case <-ctx.Done():
		case <-state.stop:
		}
		if ctx.Err() == nil {
			return
		}
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer stop()
		pool, err := open(cleanup)
		if err == nil && pool != nil {
			_, err = pool.ExecContext(cleanup, "KILL QUERY "+strconv.FormatUint(id, 10))
			err = errors.Join(err, pool.Close())
		} else if err == nil {
			err = adapter.ErrInvalid
		}
		if err != nil {
			state.err = errMySQLCancellation
		}
		// Cancel the driver context only after KILL has returned and the control
		// connection has closed. Closing data first would permit connection-ID reuse.
	}()
	return state, nil
}

func (s *mysqlReadCancellation) finish() error {
	if s.stop == nil {
		return nil
	}
	s.once.Do(func() { close(s.stop) })
	<-s.done
	return s.err
}
