// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package migrations

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/golang-migrate/migrate/v4/database"
)

// Relational supports databases with session-scoped migration locks. MySQL's
// apply pool must explicitly enable multiple statements; query pools must not.
type Relational struct {
	DB       *sql.DB
	Engine   string
	Database string
	Schema   string
}

func (p Relational) connection(ctx context.Context) (*relationalDriver, error) {
	if ctx == nil || p.DB == nil || p.Database == "" || (p.Engine != "mysql" && p.Engine != "mariadb" && p.Engine != "sqlserver") {
		return nil, migration.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := p.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	d := &relationalDriver{ctx: ctx, conn: conn, engine: p.Engine, database: p.Database, schema: p.Schema}
	if d.engine == "sqlserver" {
		var db, schema string
		err = conn.QueryRowContext(ctx, "SELECT DB_NAME(), SCHEMA_NAME()").Scan(&db, &schema)
		if err == nil && (db != p.Database || schema == "" || p.Schema != "" && schema != p.Schema) {
			err = migration.ErrInvalid
		}
		d.schema = schema
		d.table = bracket(schema) + "." + bracket(migration.Table)
		d.lockID, _ = database.GenerateAdvisoryLockId(p.Database, schema)
	} else {
		if p.Schema != "" && p.Schema != p.Database {
			err = migration.ErrInvalid
		}
		d.schema = p.Database
		d.table = backtick(p.Database) + "." + backtick(migration.Table)
		d.lockID, _ = database.GenerateAdvisoryLockId(p.Database + ":" + migration.Table)
	}
	if err == nil {
		err = d.idle(ctx)
	}
	if err != nil {
		_ = discardConnection(conn)
		return nil, err
	}
	return d, nil
}

func (p Relational) Status(ctx context.Context) (migration.State, error) {
	d, err := p.connection(ctx)
	if err != nil {
		return migration.State{}, err
	}
	defer d.Close()
	// SQL Server has no read-only transaction mode. Provenance checks below
	// permit only an ordinary table with two primitive, non-computed columns.
	tx, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: d.engine != "sqlserver"})
	if err != nil {
		return migration.State{}, err
	}
	defer tx.Rollback()
	exists, err := d.historyTable(ctx, tx)
	if err != nil || !exists {
		return migration.State{Version: -1}, err
	}
	v, dirty, err := d.readVersion(ctx, tx)
	return migration.State{Version: int64(v), Dirty: dirty}, err
}

func (p Relational) Apply(ctx context.Context, plan migration.Plan) (migration.Result, error) {
	if err := plan.Validate(); err != nil {
		return initialResult(plan), err
	}
	d, err := p.connection(ctx)
	if err != nil {
		return initialResult(plan), err
	}
	defer d.Close()
	d.plan = &plan
	return applyPlan(plan, p.Engine, d, &d.outcome)
}

func backtick(value string) string { return "`" + strings.ReplaceAll(value, "`", "``") + "`" }
func bracket(value string) string  { return "[" + strings.ReplaceAll(value, "]", "]]") + "]" }

type relationalDriver struct {
	ctx                                     context.Context
	conn                                    *sql.Conn
	engine, database, schema, table, lockID string
	plan                                    *migration.Plan
	locked                                  bool
	outcome
}

var _ database.Driver = (*relationalDriver)(nil)

func (*relationalDriver) Open(string) (database.Driver, error) { return nil, migration.ErrInvalid }
func (*relationalDriver) Drop() error                          { return migration.ErrInvalid }

// idle never starts or commits a transaction. MySQL/MariaDB reject SET
// TRANSACTION during an active transaction, including an empty BEGIN which
// information_schema.innodb_trx would miss. Autocommit must remain enabled.
func (d *relationalDriver) idle(ctx context.Context) error {
	var db string
	if d.engine == "sqlserver" {
		// XACT_STATE may be 1 inside the autocommit SELECT itself.
		// @@TRANCOUNT identifies a user transaction; negative state is unsafe.
		var count, state, implicit int
		if err := d.conn.QueryRowContext(ctx, "SELECT DB_NAME(), @@TRANCOUNT, XACT_STATE(), CASE WHEN (2 & @@OPTIONS) = 2 THEN 1 ELSE 0 END").Scan(&db, &count, &state, &implicit); err != nil {
			return err
		}
		if db != d.database || count != 0 || state < 0 || state > 1 || implicit != 0 {
			return migration.ErrInvalid
		}
		// A script's SET ROWCOUNT must not truncate subsequent catalog checks,
		// version reads, or history deletes on this same session.
		_, err := d.conn.ExecContext(ctx, "SET ROWCOUNT 0")
		return err
	}
	var autocommit bool
	if err := d.conn.QueryRowContext(ctx, "SELECT DATABASE(), @@session.autocommit").Scan(&db, &autocommit); err != nil {
		return err
	}
	if db != d.database || !autocommit {
		return migration.ErrInvalid
	}
	_, err := d.conn.ExecContext(ctx, "SET TRANSACTION READ WRITE")
	return err
}

func (d *relationalDriver) requireIdle() error {
	if err := d.idle(d.ctx); err != nil {
		d.uncertain = true
		d.rollback()
		return migration.ErrOutcomeUnknown
	}
	return nil
}

func (d *relationalDriver) rollback() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	query := "ROLLBACK"
	if d.engine == "sqlserver" {
		query = "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION"
	}
	_, _ = d.conn.ExecContext(ctx, query)
}

func (d *relationalDriver) Close() error {
	if d.conn == nil {
		return nil
	}
	// Always rollback before releasing the lock, even when cancellation made
	// transaction-state inspection impossible. Never return this session to a pool.
	d.rollback()
	var unlockErr error
	if d.locked {
		unlockErr = d.Unlock()
	}
	err := discardConnection(d.conn)
	d.conn = nil
	return errors.Join(unlockErr, err)
}

const sqlServerLock = "DECLARE @r int; EXEC @r = sys.sp_getapplock @Resource=@p1, @LockMode='Exclusive', @LockOwner='Session', @LockTimeout=0; SELECT @r"
const sqlServerUnlock = "DECLARE @r int; EXEC @r = sys.sp_releaseapplock @Resource=@p1, @LockOwner='Session'; SELECT @r"
const sqlServerHeld = "SELECT CASE WHEN APPLOCK_MODE('public', @p1, 'Session') = 'Exclusive' THEN 1 ELSE 0 END"

func (d *relationalDriver) lockOnce(ctx context.Context) (bool, error) {
	if d.engine != "sqlserver" {
		var acquired sql.NullInt64
		err := d.conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", d.lockID).Scan(&acquired)
		if err == nil && !acquired.Valid {
			err = migration.ErrInvalid
		}
		return acquired.Valid && acquired.Int64 == 1, err
	}
	var code int
	err := d.conn.QueryRowContext(ctx, sqlServerLock, d.lockID).Scan(&code)
	if err == nil && code < -1 {
		err = migration.ErrInvalid
	}
	return code >= 0, err
}

func (d *relationalDriver) requireLock() error {
	if !d.locked {
		return database.ErrNotLocked
	}
	query := "SELECT COALESCE(IS_USED_LOCK(?) = CONNECTION_ID(), 0)"
	if d.engine == "sqlserver" {
		query = sqlServerHeld
	}
	var held bool
	if err := d.conn.QueryRowContext(d.ctx, query, d.lockID).Scan(&held); err != nil || !held {
		d.uncertain = true
		return migration.ErrOutcomeUnknown
	}
	return nil
}

func (d *relationalDriver) Lock() (resultErr error) {
	if d.locked || d.plan == nil {
		return database.ErrLocked
	}
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for !d.locked {
		var err error
		d.locked, err = d.lockOnce(ctx)
		if err != nil {
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
	exists, err := d.historyTable(d.ctx, d.conn)
	if err != nil || exists {
		return err
	}
	ddl := "CREATE TABLE " + d.table + " (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL) ENGINE=InnoDB"
	if d.engine == "sqlserver" {
		ddl = "CREATE TABLE " + d.table + " (version bigint NOT NULL PRIMARY KEY, dirty bit NOT NULL)"
	}
	if _, err := d.conn.ExecContext(d.ctx, ddl); err != nil {
		d.uncertain = true
		return err
	}
	d.changed = true
	return nil
}

func (d *relationalDriver) Unlock() error {
	if !d.locked {
		return database.ErrNotLocked
	}
	stateErr := d.requireLock()
	if stateErr == nil {
		var v int
		var dirty bool
		v, dirty, stateErr = d.Version()
		if stateErr == nil {
			d.finalState, d.finalRead = migration.State{Version: int64(v), Dirty: dirty}, true
		}
	}
	if stateErr != nil && d.changed {
		d.uncertain = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	query := "SELECT RELEASE_LOCK(?)"
	if d.engine == "sqlserver" {
		query = sqlServerUnlock
	}
	var code sql.NullInt64
	err := d.conn.QueryRowContext(ctx, query, d.lockID).Scan(&code)
	d.locked = false
	if err == nil && (!code.Valid || d.engine == "sqlserver" && code.Int64 < 0 || d.engine != "sqlserver" && code.Int64 != 1) {
		err = database.ErrNotLocked
	}
	return errors.Join(stateErr, err)
}

func (d *relationalDriver) Version() (int, bool, error) {
	exists, err := d.historyTable(d.ctx, d.conn)
	if err != nil || !exists {
		return -1, false, err
	}
	return d.readVersion(d.ctx, d.conn)
}

func (d *relationalDriver) readVersion(ctx context.Context, conn versionQueryer) (int, bool, error) {
	query := "SELECT version, dirty FROM " + d.table + " LIMIT 2"
	if d.engine == "sqlserver" {
		query = "SELECT TOP (2) version, dirty FROM " + d.table
	}
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
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

func (d *relationalDriver) Run(input io.Reader) error {
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
		d.uncertain = true
		d.rollback()
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

func (d *relationalDriver) SetVersion(version int, dirty bool) error {
	if !d.locked || (migration.State{Version: int64(version)}).Validate() != nil {
		return migration.ErrInvalid
	}
	if err := d.requireIdle(); err != nil {
		return err
	}
	if err := d.requireLock(); err != nil {
		return err
	}
	exists, err := d.historyTable(d.ctx, d.conn)
	if err != nil || !exists {
		d.uncertain = true
		return migration.ErrOutcomeUnknown
	}
	tx, err := d.conn.BeginTx(d.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(d.ctx, "DELETE FROM "+d.table); err != nil {
		return err
	}
	if version >= 0 || dirty {
		query := "INSERT INTO " + d.table + " (version, dirty) VALUES (?, ?)"
		if d.engine == "sqlserver" {
			query = "INSERT INTO " + d.table + " (version, dirty) VALUES (@p1, @p2)"
		}
		if _, err := tx.ExecContext(d.ctx, query, version, dirty); err != nil {
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
