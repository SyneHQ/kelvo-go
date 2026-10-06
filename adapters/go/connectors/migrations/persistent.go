// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package migrations

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/golang-migrate/migrate/v4/database"
)

const persistentLockTable = "kelvo_migration_lock"

// PersistentSQL uses a durable, non-expiring source lock. A crashed or uncertain
// migration requires operator reconciliation; it never frees a lock while a
// CockroachDB background schema job may still be running. Redshift's primary
// keys are not enforced, so acquisition also holds its table lock in a transaction.
type PersistentSQL struct {
	DB                       *sql.DB
	Engine, Database, Schema string
	transactionStatus        func(*sql.Conn) (byte, error)
}

func (p PersistentSQL) connection(ctx context.Context) (*persistentDriver, error) {
	if ctx == nil || p.DB == nil || p.Database == "" || (p.Engine != "cockroachdb" && p.Engine != "redshift") {
		return nil, migration.ErrInvalid
	}
	conn, err := p.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	status := p.transactionStatus
	if status == nil {
		status = postgresTransactionStatus
	}
	d := &persistentDriver{ctx: ctx, conn: conn, engine: p.Engine, transactionStatus: status}
	var db string
	if err = conn.QueryRowContext(ctx, "SELECT current_database(), current_schema()").Scan(&db, &d.schema); err == nil && (db != p.Database || d.schema == "" || p.Schema != "" && p.Schema != d.schema) {
		err = migration.ErrInvalid
	}
	if err == nil {
		err = d.idle()
	}
	if err != nil {
		_ = discardConnection(conn)
		return nil, err
	}
	d.table = quote(d.schema) + "." + quote(migration.Table)
	d.lockTable = quote(d.schema) + "." + quote(persistentLockTable)
	return d, nil
}

func (p PersistentSQL) Status(ctx context.Context) (migration.State, error) {
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
	return d.state(ctx, tx)
}

func (p PersistentSQL) Apply(ctx context.Context, plan migration.Plan) (migration.Result, error) {
	if plan.Validate() != nil {
		return initialResult(plan), migration.ErrInvalid
	}
	d, err := p.connection(ctx)
	if err != nil {
		return initialResult(plan), err
	}
	defer d.Close()
	d.plan = &plan
	return applyPlan(plan, p.Engine, d, &d.outcome)
}

type persistentDriver struct {
	ctx                                     context.Context
	conn                                    *sql.Conn
	engine, schema, table, lockTable, owner string
	transactionStatus                       func(*sql.Conn) (byte, error)
	plan                                    *migration.Plan
	locked                                  bool
	outcome
}

var _ database.Driver = (*persistentDriver)(nil)

func (*persistentDriver) Open(string) (database.Driver, error) { return nil, migration.ErrInvalid }
func (*persistentDriver) Drop() error                          { return migration.ErrInvalid }

func (d *persistentDriver) idle() error {
	state, err := d.transactionStatus(d.conn)
	if err != nil || state != 'I' {
		return migration.ErrInvalid
	}
	return nil
}
func (d *persistentDriver) requireIdle() error {
	if d.idle() != nil {
		d.uncertain = true
		return migration.ErrOutcomeUnknown
	}
	return nil
}
func (d *persistentDriver) Close() error {
	if d.conn == nil {
		return nil
	}
	state, err := d.transactionStatus(d.conn)
	if err != nil || state != 'I' {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _ = d.conn.ExecContext(ctx, "ROLLBACK")
		cancel()
	}
	var unlock error
	if d.locked && !d.uncertain {
		unlock = d.Unlock()
	}
	err = discardConnection(d.conn)
	d.conn = nil
	return errors.Join(unlock, err)
}

const warehouseTableSQL = `SELECT t.table_type,c.column_name,c.data_type,c.is_nullable,c.column_default FROM information_schema.tables t LEFT JOIN information_schema.columns c ON c.table_catalog=t.table_catalog AND c.table_schema=t.table_schema AND c.table_name=t.table_name WHERE t.table_catalog=current_database() AND t.table_schema=$1 AND t.table_name=$2 ORDER BY c.ordinal_position LIMIT 3`

func (d *persistentDriver) tableShape(ctx context.Context, q versionQueryer, name string) (bool, error) {
	rows, err := q.QueryContext(ctx, warehouseTableSQL, d.schema, name)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var kind string
		var column, datatype, nullable, defaultValue sql.NullString
		if err := rows.Scan(&kind, &column, &datatype, &nullable, &defaultValue); err != nil {
			return false, err
		}
		if kind != "BASE TABLE" || !column.Valid || !datatype.Valid || !nullable.Valid || nullable.String != "NO" || defaultValue.Valid || seen[column.String] {
			return false, migration.ErrInvalid
		}
		valid := false
		if name == migration.Table {
			valid = column.String == "version" && datatype.String == "bigint" || column.String == "dirty" && datatype.String == "boolean"
		} else if name == persistentLockTable {
			valid = column.String == "lock_id" && datatype.String == "bigint" || column.String == "owner" && (datatype.String == "character varying" || datatype.String == "text" || datatype.String == "string")
		}
		if !valid {
			return false, migration.ErrInvalid
		}
		seen[column.String] = true
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(seen) == 0 {
		return false, nil
	}
	if len(seen) != 2 {
		return false, migration.ErrInvalid
	}
	// Redshift does not implement triggers. CockroachDB does; source histories
	// and lock tables must not execute user trigger code while being read/written.
	if d.engine == "cockroachdb" {
		if err := rows.Close(); err != nil {
			return false, err
		}
		r, err := q.QueryContext(ctx, `SELECT count(*) FROM information_schema.triggers WHERE event_object_schema=$1 AND event_object_table=$2`, d.schema, name)
		if err != nil {
			return false, err
		}
		defer r.Close()
		var count int
		if !r.Next() || r.Scan(&count) != nil || count != 0 || r.Next() {
			return false, migration.ErrInvalid
		}
		if err := r.Err(); err != nil {
			return false, err
		}
	}
	return true, nil
}
func (d *persistentDriver) state(ctx context.Context, q versionQueryer) (migration.State, error) {
	exists, err := d.tableShape(ctx, q, migration.Table)
	if err != nil || !exists {
		return migration.State{Version: -1}, err
	}
	rows, err := q.QueryContext(ctx, "SELECT version, dirty FROM "+d.table+" LIMIT 2")
	if err != nil {
		return migration.State{}, err
	}
	defer rows.Close()
	state := migration.State{Version: -1}
	if !rows.Next() {
		return state, rows.Err()
	}
	if err := rows.Scan(&state.Version, &state.Dirty); err != nil {
		return migration.State{}, err
	}
	if rows.Next() || state.Validate() != nil {
		return migration.State{}, migration.ErrInvalid
	}
	return state, rows.Err()
}
func (d *persistentDriver) Version() (int, bool, error) {
	state, err := d.state(d.ctx, d.conn)
	return int(state.Version), state.Dirty, err
}

func (d *persistentDriver) Lock() (resultErr error) {
	if d.locked || d.plan == nil {
		return database.ErrLocked
	}
	// Fail stale requests before installing coordination metadata. The same
	// condition is checked again after source-wide lock acquisition.
	state, err := d.state(d.ctx, d.conn)
	if err != nil {
		return err
	}
	if state != d.plan.Expected {
		return migration.ErrConflict
	}
	if state.Dirty && d.plan.Direction != "force" {
		return migration.ErrDirty
	}
	exists, err := d.tableShape(d.ctx, d.conn, persistentLockTable)
	if err != nil {
		return err
	}
	if !exists {
		_, err = d.conn.ExecContext(d.ctx, "CREATE TABLE "+d.lockTable+" (lock_id bigint NOT NULL PRIMARY KEY, owner varchar(64) NOT NULL)")
		if err != nil {
			d.uncertain = true
			return err
		}
		d.changed = true
	}
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return err
	}
	d.owner = hex.EncodeToString(token)
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()
	tx, err := d.conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if d.engine == "redshift" {
		if _, err = tx.ExecContext(ctx, "LOCK TABLE "+d.lockTable); err != nil {
			return err
		}
	}
	if exists, err = d.tableShape(ctx, tx, persistentLockTable); err != nil || !exists {
		if err == nil {
			err = migration.ErrInvalid
		}
		return err
	}
	owner, err := d.readOwner(ctx, tx)
	if err != nil {
		return err
	}
	if owner != "" {
		return database.ErrLocked
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO "+d.lockTable+" (lock_id,owner) VALUES (1,$1)", d.owner); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		d.uncertain = true
		return err
	}
	d.locked = true
	defer func() {
		if resultErr != nil && !d.uncertain {
			_ = d.Unlock()
		}
	}()
	if err = d.requireLock(); err != nil {
		return err
	}
	state, err = d.state(d.ctx, d.conn)
	if err != nil {
		return err
	}
	if state != d.plan.Expected {
		return migration.ErrConflict
	}
	if state.Dirty && d.plan.Direction != "force" {
		return migration.ErrDirty
	}
	exists, err = d.tableShape(d.ctx, d.conn, migration.Table)
	if err != nil {
		return err
	}
	if !exists {
		if _, err = d.conn.ExecContext(d.ctx, "CREATE TABLE "+d.table+" (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL)"); err != nil {
			d.uncertain = true
			return err
		}
		d.changed = true
	}
	return nil
}

func (d *persistentDriver) readOwner(ctx context.Context, q versionQueryer) (string, error) {
	rows, err := q.QueryContext(ctx, "SELECT lock_id,owner FROM "+d.lockTable+" LIMIT 2")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", rows.Err()
	}
	var id int64
	var owner string
	if rows.Scan(&id, &owner) != nil || id != 1 || len(owner) != 64 || rows.Next() {
		return "", migration.ErrInvalid
	}
	if _, err := hex.DecodeString(owner); err != nil {
		return "", migration.ErrInvalid
	}
	return owner, rows.Err()
}
func (d *persistentDriver) requireLock() error {
	if !d.locked {
		return database.ErrNotLocked
	}
	exists, err := d.tableShape(d.ctx, d.conn, persistentLockTable)
	if err == nil && exists {
		var owner string
		owner, err = d.readOwner(d.ctx, d.conn)
		if owner != d.owner {
			err = database.ErrNotLocked
		}
	} else if err == nil {
		err = database.ErrNotLocked
	}
	if err != nil {
		d.uncertain = true
		return migration.ErrOutcomeUnknown
	}
	return nil
}
func (d *persistentDriver) Unlock() error {
	if !d.locked {
		return database.ErrNotLocked
	}
	if d.uncertain {
		return migration.ErrOutcomeUnknown
	}
	if err := d.requireLock(); err != nil {
		return err
	}
	state, err := d.state(d.ctx, d.conn)
	if err != nil {
		d.uncertain = true
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := d.conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		d.uncertain = true
		return err
	}
	defer tx.Rollback()
	if d.engine == "redshift" {
		if _, err = tx.ExecContext(ctx, "LOCK TABLE "+d.lockTable); err != nil {
			d.uncertain = true
			return err
		}
	}
	result, err := tx.ExecContext(ctx, "DELETE FROM "+d.lockTable+" WHERE lock_id=1 AND owner=$1", d.owner)
	if err == nil {
		var count int64
		count, err = result.RowsAffected()
		if err == nil && count != 1 {
			err = database.ErrNotLocked
		}
	}
	if err != nil {
		d.uncertain = true
		return err
	}
	if err = tx.Commit(); err != nil {
		d.uncertain = true
		return err
	}
	d.locked = false
	d.finalRead = true
	d.finalState = state
	return nil
}
func (d *persistentDriver) Run(input io.Reader) error {
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
	if _, err = d.conn.ExecContext(d.ctx, string(raw)); err != nil {
		d.uncertain = true
		return err
	}
	d.changed = true
	if err := d.requireIdle(); err != nil {
		return err
	}
	return d.requireLock()
}
func (d *persistentDriver) SetVersion(version int, dirty bool) error {
	if (migration.State{Version: int64(version)}).Validate() != nil {
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
	exists, err := d.tableShape(d.ctx, tx, migration.Table)
	if err != nil || !exists {
		if err == nil {
			err = migration.ErrInvalid
		}
		return err
	}
	if _, err = tx.ExecContext(d.ctx, "DELETE FROM "+d.table); err != nil {
		return err
	}
	if version >= 0 || dirty {
		if _, err = tx.ExecContext(d.ctx, "INSERT INTO "+d.table+" (version,dirty) VALUES ($1,$2)", version, dirty); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		d.uncertain = true
		return err
	}
	d.changed = true
	return nil
}
