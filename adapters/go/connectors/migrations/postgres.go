// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package migrations adapts golang-migrate to an authorized, cancellable session.
package migrations

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

type PostgreSQL struct {
	DB                *sql.DB
	Database          string
	Schema            string
	transactionStatus func(*sql.Conn) (byte, error)
}

func quote(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }

func (p PostgreSQL) connection(ctx context.Context) (*postgresDriver, error) {
	if ctx == nil || p.DB == nil || p.Database == "" {
		return nil, migration.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := p.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	var db, schema string
	if err := conn.QueryRowContext(ctx, "SELECT current_database(), current_schema()").Scan(&db, &schema); err != nil {
		_ = discardConnection(conn)
		return nil, err
	}
	if db != p.Database || schema == "" || p.Schema != "" && schema != p.Schema {
		_ = discardConnection(conn)
		return nil, migration.ErrInvalid
	}
	lock, err := database.GenerateAdvisoryLockId(db, schema, migration.Table)
	if err != nil {
		_ = discardConnection(conn)
		return nil, err
	}
	status := p.transactionStatus
	if status == nil {
		status = postgresTransactionStatus
	}
	d := &postgresDriver{ctx: ctx, conn: conn, table: quote(schema) + "." + quote(migration.Table), lockID: lock, transactionStatus: status}
	if state, err := status(conn); err != nil || state != 'I' {
		_ = d.Close()
		return nil, migration.ErrInvalid
	}
	return d, nil
}

func postgresTransactionStatus(conn *sql.Conn) (byte, error) {
	var status byte
	err := conn.Raw(func(raw any) error {
		c, ok := raw.(*stdlib.Conn)
		if !ok || c.Conn() == nil {
			return migration.ErrInvalid
		}
		status = c.Conn().PgConn().TxStatus()
		return nil
	})
	return status, err
}

func discardConnection(conn *sql.Conn) error {
	if conn == nil {
		return nil
	}
	err := conn.Raw(func(any) error { return driver.ErrBadConn })
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		err = nil
	}
	closed := conn.Close()
	if errors.Is(closed, sql.ErrConnDone) {
		closed = nil
	}
	return errors.Join(err, closed)
}

// Status does not instantiate a library database driver: some library
// constructors install schema_migrations even for a version lookup.
func (p PostgreSQL) Status(ctx context.Context) (migration.State, error) {
	d, err := p.connection(ctx)
	if err != nil {
		return migration.State{}, err
	}
	defer d.Close()
	tx, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return migration.State{}, err
	}
	defer tx.Rollback()
	v, dirty, err := readVersion(ctx, tx, d.table)
	return migration.State{Version: int64(v), Dirty: dirty}, err
}

func (p PostgreSQL) Apply(ctx context.Context, plan migration.Plan) (migration.Result, error) {
	result := migration.Result{Version: migration.Version, From: plan.Expected, To: plan.Expected, FilesApplied: []string{}, Effect: operations.EffectNone}
	if plan.Validate() != nil {
		return result, migration.ErrInvalid
	}
	d, err := p.connection(ctx)
	if err != nil {
		return result, err
	}
	defer d.Close()
	d.plan = &plan
	return applyPlan(plan, "postgres", d, &d.outcome)
}

type postgresDriver struct {
	ctx           context.Context
	conn          *sql.Conn
	table, lockID string
	plan          *migration.Plan
	locked        bool
	outcome
	transactionStatus func(*sql.Conn) (byte, error)
}

var _ database.Driver = (*postgresDriver)(nil)

func (*postgresDriver) Open(string) (database.Driver, error) { return nil, migration.ErrInvalid }
func (*postgresDriver) Drop() error                          { return migration.ErrInvalid }
func (d *postgresDriver) Close() error {
	if d.conn == nil {
		return nil
	}
	// Roll back explicit transactions before unlocking: PostgreSQL rejects
	// unlock queries inside an aborted transaction. Always physically discard
	// the session, including failed unlocks and caller-supplied reusable pools.
	d.rollbackOpenTransaction()
	var unlockErr error
	if d.locked {
		unlockErr = d.Unlock()
	}
	err := discardConnection(d.conn)
	d.conn = nil
	return errors.Join(unlockErr, err)
}

func (d *postgresDriver) rollbackOpenTransaction() {
	if d.conn == nil || d.transactionStatus == nil {
		return
	}
	status, err := d.transactionStatus(d.conn)
	if err != nil || status == 'I' {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = d.conn.ExecContext(ctx, "ROLLBACK")
}

func (d *postgresDriver) requireIdle() error {
	status, err := d.transactionStatus(d.conn)
	if err == nil && status == 'I' {
		return nil
	}
	d.uncertain = true
	d.rollbackOpenTransaction()
	return migration.ErrOutcomeUnknown
}

const lockHeldSQL = `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted AND classid::bigint=(($1::bigint >> 32) & 4294967295) AND objid::bigint=($1::bigint & 4294967295) AND objsubid=1)`

func (d *postgresDriver) requireLock() error {
	var held bool
	if !d.locked {
		return database.ErrNotLocked
	}
	if err := d.conn.QueryRowContext(d.ctx, lockHeldSQL, d.lockID).Scan(&held); err != nil || !held {
		d.uncertain = true
		return migration.ErrOutcomeUnknown
	}
	return nil
}

func (d *postgresDriver) Lock() (resultErr error) {
	if d.locked || d.plan == nil {
		return database.ErrLocked
	}
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for !d.locked {
		if err := d.conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", d.lockID).Scan(&d.locked); err != nil {
			return err
		}
		if !d.locked {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
	}
	defer func() {
		if resultErr != nil {
			_ = d.Unlock()
		}
	}()
	v, dirty, err := d.Version()
	if err != nil {
		return err
	}
	if (migration.State{Version: int64(v), Dirty: dirty}) != d.plan.Expected {
		return migration.ErrConflict
	}
	if dirty && d.plan.Direction != "force" {
		return migration.ErrDirty
	}
	// Initialization is an explicit side effect of apply, after the version
	// precondition. Preserve the standard table so existing histories still work.
	var exists bool
	if err := d.conn.QueryRowContext(d.ctx, "SELECT to_regclass($1) IS NOT NULL", d.table).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if _, err := d.conn.ExecContext(d.ctx, "CREATE TABLE "+d.table+" (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL)"); err != nil {
			d.uncertain = true
			return err
		}
		d.changed = true
	}
	return nil
}

func (d *postgresDriver) Unlock() error {
	if !d.locked {
		return database.ErrNotLocked
	}
	// Read the final state while still owning the advisory lock. A later
	// migration must never change the receipt of this invocation.
	stateErr := d.requireLock()
	if stateErr == nil {
		var version int
		var dirty bool
		version, dirty, stateErr = d.Version()
		if stateErr == nil {
			d.finalState = migration.State{Version: int64(version), Dirty: dirty}
			d.finalRead = true
		}
	}
	if stateErr != nil && d.changed {
		d.uncertain = true
	}
	// Cleanup is bounded even when execution was cancelled. The private pool
	// closes immediately after this invocation; sessions never cross operations.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var unlocked bool
	err := d.conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", d.lockID).Scan(&unlocked)
	d.locked = false
	if err != nil {
		return errors.Join(stateErr, err)
	}
	if !unlocked {
		return database.ErrNotLocked
	}
	return stateErr
}

func (d *postgresDriver) Version() (int, bool, error) {
	return readVersion(d.ctx, d.conn, d.table)
}

type versionQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func readVersion(ctx context.Context, source versionQueryer, table string) (int, bool, error) {
	exists, err := postgresHistory(ctx, source, table)
	if err != nil || !exists {
		return -1, false, err
	}
	rows, err := source.QueryContext(ctx, "SELECT version, dirty FROM "+table+" LIMIT 2")
	if err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code == "42P01" {
			return -1, false, nil
		}
		return -1, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return -1, false, rows.Err()
	}
	var state migration.State
	if err := rows.Scan(&state.Version, &state.Dirty); err != nil {
		return -1, false, err
	}
	if rows.Next() || state.Validate() != nil {
		return -1, false, migration.ErrInvalid
	}
	return int(state.Version), state.Dirty, rows.Err()
}

func (d *postgresDriver) Run(input io.Reader) error {
	if !d.locked {
		return database.ErrNotLocked
	}
	if err := d.requireIdle(); err != nil {
		return err
	}
	if err := d.requireLock(); err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(input, migration.MaxFileBytes+1))
	if err != nil || len(raw) > migration.MaxFileBytes {
		return migration.ErrInvalid
	}
	defer clear(raw)
	if strings.TrimSpace(string(raw)) == "" {
		return nil
	}
	if _, err := d.conn.ExecContext(d.ctx, string(raw)); err != nil {
		// A script may contain explicit commits. An error is not proof that
		// earlier statements had no effects, even on a transactional DDL backend.
		d.uncertain = true
		d.rollbackOpenTransaction()
		return err
	}
	if err := d.requireIdle(); err != nil {
		return err
	}
	if err := d.requireLock(); err != nil {
		return err
	}
	d.changed = true
	return nil
}

func (d *postgresDriver) SetVersion(version int, dirty bool) error {
	if !d.locked || (migration.State{Version: int64(version)}).Validate() != nil {
		return migration.ErrInvalid
	}
	if err := d.requireIdle(); err != nil {
		return err
	}
	if err := d.requireLock(); err != nil {
		return err
	}
	tx, err := d.conn.BeginTx(d.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if exists, err := postgresHistory(d.ctx, tx, d.table); err != nil || !exists {
		if err == nil {
			err = migration.ErrInvalid
		}
		return err
	}
	if _, err := tx.ExecContext(d.ctx, "DELETE FROM "+d.table); err != nil {
		return err
	}
	if version >= 0 || dirty {
		if _, err := tx.ExecContext(d.ctx, "INSERT INTO "+d.table+" (version, dirty) VALUES ($1, $2)", version, dirty); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		d.uncertain = true
		return err
	}
	d.changed = true
	return nil
}
