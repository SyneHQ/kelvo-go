// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package migrations

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type relationalFixture struct {
	p             Relational
	m             sqlmock.Sqlmock
	table, schema string
}

func newRelationalFixture(t *testing.T, engine string) relationalFixture {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	f := relationalFixture{p: Relational{DB: db, Engine: engine, Database: "app", Schema: "app"}, m: mock, schema: "app", table: "`app`.`schema_migrations`"}
	if engine == "sqlserver" {
		f.p.Schema, f.schema, f.table = "dbo", "dbo", "[dbo].[schema_migrations]"
	}
	return f
}

func (f relationalFixture) idle() {
	if f.p.Engine == "sqlserver" {
		f.m.ExpectQuery("SELECT DB_NAME(), @@TRANCOUNT, XACT_STATE(), CASE WHEN (2 & @@OPTIONS) = 2 THEN 1 ELSE 0 END").WillReturnRows(sqlmock.NewRows([]string{"db", "count", "state", "implicit"}).AddRow("app", 0, 1, 0))
		f.m.ExpectExec("SET ROWCOUNT 0").WillReturnResult(sqlmock.NewResult(0, 0))
	} else {
		f.m.ExpectQuery("SELECT DATABASE(), @@session.autocommit").WillReturnRows(sqlmock.NewRows([]string{"db", "autocommit"}).AddRow("app", true))
		f.m.ExpectExec("SET TRANSACTION READ WRITE").WillReturnResult(sqlmock.NewResult(0, 0))
	}
}

func (f relationalFixture) identity() {
	if f.p.Engine == "sqlserver" {
		f.m.ExpectQuery("SELECT DB_NAME(), SCHEMA_NAME()").WillReturnRows(sqlmock.NewRows([]string{"db", "schema"}).AddRow("app", "dbo"))
	}
	f.idle()
}

func (f relationalFixture) history(exists bool) {
	if f.p.Engine == "sqlserver" {
		rows := sqlmock.NewRows([]string{"kind", "name", "system", "user", "nullable", "computed", "generated"})
		if exists {
			rows.AddRow("U ", "version", 127, 127, false, false, 0).AddRow("U ", "dirty", 104, 104, false, false, 0)
		}
		f.m.ExpectQuery(sqlServerHistorySQL).WithArgs(f.schema, migration.Table).WillReturnRows(rows)
	} else {
		rows := sqlmock.NewRows([]string{"kind", "engine", "name", "type", "nullable", "extra", "generated"})
		if exists {
			rows.AddRow("BASE TABLE", "InnoDB", "version", "bigint", "NO", "", "").AddRow("BASE TABLE", "InnoDB", "dirty", "tinyint", "NO", "", "")
		}
		f.m.ExpectQuery(mysqlHistorySQL).WithArgs(f.schema, migration.Table).WillReturnRows(rows)
	}
	if exists {
		query := mysqlTriggersSQL
		if f.p.Engine == "sqlserver" {
			query = sqlServerTriggersSQL
		}
		f.m.ExpectQuery(query).WithArgs(f.schema, migration.Table).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	}
}

func (f relationalFixture) version(v int64, dirty bool) {
	f.history(true)
	f.readVersion(v, dirty)
}

func (f relationalFixture) readVersion(v int64, dirty bool) {
	query := "SELECT version, dirty FROM " + f.table + " LIMIT 2"
	if f.p.Engine == "sqlserver" {
		query = "SELECT TOP (2) version, dirty FROM " + f.table
	}
	f.m.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).AddRow(v, dirty))
}

func (f relationalFixture) lock() {
	query, code := "SELECT GET_LOCK(?, 0)", 1
	if f.p.Engine == "sqlserver" {
		query, code = sqlServerLock, 0
	}
	f.m.ExpectQuery(query).WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"code"}).AddRow(code))
}

func (f relationalFixture) held(held bool) {
	query := "SELECT COALESCE(IS_USED_LOCK(?) = CONNECTION_ID(), 0)"
	if f.p.Engine == "sqlserver" {
		query = sqlServerHeld
	}
	f.m.ExpectQuery(query).WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"held"}).AddRow(held))
}

func (f relationalFixture) unlock() {
	query, code := "SELECT RELEASE_LOCK(?)", 1
	if f.p.Engine == "sqlserver" {
		query, code = sqlServerUnlock, 0
	}
	f.m.ExpectQuery(query).WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"code"}).AddRow(code))
}

func (f relationalFixture) final(v int64, dirty bool) { f.held(true); f.version(v, dirty); f.unlock() }
func (f relationalFixture) rollback() {
	query := "ROLLBACK"
	if f.p.Engine == "sqlserver" {
		query = "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION"
	}
	f.m.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 0))
}

func (f relationalFixture) set(v int, dirty bool, commitErr error) {
	f.idle()
	f.held(true)
	f.history(true)
	f.m.ExpectBegin()
	f.m.ExpectExec("DELETE FROM " + f.table).WillReturnResult(sqlmock.NewResult(0, 1))
	if v >= 0 || dirty {
		query := "INSERT INTO " + f.table + " (version, dirty) VALUES (?, ?)"
		if f.p.Engine == "sqlserver" {
			query = "INSERT INTO " + f.table + " (version, dirty) VALUES (@p1, @p2)"
		}
		f.m.ExpectExec(query).WithArgs(v, dirty).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	if commitErr == nil {
		f.m.ExpectCommit()
	} else {
		f.m.ExpectCommit().WillReturnError(commitErr)
	}
}

func (f relationalFixture) prepared() {
	f.identity()
	f.lock()
	f.version(1, false)
	f.history(true)
	f.version(1, false)
}
func (f relationalFixture) check(t *testing.T) {
	t.Helper()
	if err := f.m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if f.p.DB.Stats().OpenConnections != 0 {
		t.Fatal("migration session returned to reusable pool")
	}
}

func TestRelationalStatusHasNoInitializationSideEffects(t *testing.T) {
	for _, engine := range []string{"mysql", "mariadb", "sqlserver"} {
		t.Run(engine, func(t *testing.T) {
			f := newRelationalFixture(t, engine)
			f.identity()
			f.m.ExpectBegin()
			f.history(false)
			f.m.ExpectRollback()
			f.rollback()
			state, err := f.p.Status(context.Background())
			if err != nil || state != (migration.State{Version: -1}) {
				t.Fatal(state, err)
			}
			f.check(t)
		})
	}
}

func TestRelationalApplyRetainsReceiptUntilUnlock(t *testing.T) {
	for _, engine := range []string{"mysql", "mariadb", "sqlserver"} {
		t.Run(engine, func(t *testing.T) {
			f := newRelationalFixture(t, engine)
			f.prepared()
			f.set(2, true, nil)
			f.idle()
			f.held(true)
			f.m.ExpectExec("CREATE TABLE orders(id bigint)").WillReturnResult(sqlmock.NewResult(0, 0))
			f.idle()
			f.held(true)
			f.set(2, false, nil)
			f.final(2, false)
			f.rollback()
			result, err := f.p.Apply(context.Background(), testPlan())
			if err != nil || result.Effect != operations.EffectCommitted || result.To.Version != 2 || len(result.FilesApplied) != 1 {
				t.Fatal(result, err)
			}
			f.check(t)
		})
	}
}

func TestRelationalApplyRejectsStaleVersionBeforeDDL(t *testing.T) {
	for _, engine := range []string{"mysql", "mariadb", "sqlserver"} {
		t.Run(engine, func(t *testing.T) {
			f := newRelationalFixture(t, engine)
			f.identity()
			f.lock()
			f.version(3, false)
			f.final(3, false)
			f.rollback()
			result, err := f.p.Apply(context.Background(), testPlan())
			if !errors.Is(err, migration.ErrConflict) || result.Effect != operations.EffectNone {
				t.Fatal(result, err)
			}
			f.check(t)
		})
	}
}

func TestRelationalLostCommitRemainsUnknown(t *testing.T) {
	for _, engine := range []string{"mysql", "mariadb", "sqlserver"} {
		t.Run(engine, func(t *testing.T) {
			f := newRelationalFixture(t, engine)
			f.prepared()
			f.set(2, true, errors.New("lost response"))
			f.final(2, true)
			f.rollback()
			result, err := f.p.Apply(context.Background(), testPlan())
			if !errors.Is(err, migration.ErrOutcomeUnknown) || result.Effect != operations.EffectUnknown {
				t.Fatal(result, err)
			}
			f.check(t)
		})
	}
}

func TestRelationalOpenTransactionCannotBecomeCleanHistory(t *testing.T) {
	for _, engine := range []string{"mysql", "mariadb", "sqlserver"} {
		t.Run(engine, func(t *testing.T) {
			f := newRelationalFixture(t, engine)
			f.prepared()
			f.set(2, true, nil)
			f.idle()
			f.held(true)
			f.m.ExpectExec("CREATE TABLE orders(id bigint)").WillReturnResult(sqlmock.NewResult(0, 0))
			if engine == "sqlserver" {
				f.m.ExpectQuery("SELECT DB_NAME(), @@TRANCOUNT, XACT_STATE(), CASE WHEN (2 & @@OPTIONS) = 2 THEN 1 ELSE 0 END").WillReturnRows(sqlmock.NewRows([]string{"db", "count", "state", "implicit"}).AddRow("app", 1, 1, 0))
			} else {
				f.m.ExpectQuery("SELECT DATABASE(), @@session.autocommit").WillReturnRows(sqlmock.NewRows([]string{"db", "autocommit"}).AddRow("app", true))
				f.m.ExpectExec("SET TRANSACTION READ WRITE").WillReturnError(errors.New("transaction characteristics cannot change in transaction"))
			}
			f.rollback()
			f.final(2, true)
			f.rollback()
			result, err := f.p.Apply(context.Background(), testPlan())
			if !errors.Is(err, migration.ErrOutcomeUnknown) || result.Effect != operations.EffectUnknown {
				t.Fatal(result, err)
			}
			f.check(t)
		})
	}
}

func TestRelationalScriptCannotReleaseLockThenCleanHistory(t *testing.T) {
	for _, engine := range []string{"mysql", "mariadb", "sqlserver"} {
		t.Run(engine, func(t *testing.T) {
			f := newRelationalFixture(t, engine)
			f.prepared()
			f.set(2, true, nil)
			f.idle()
			f.held(true)
			f.m.ExpectExec("CREATE TABLE orders(id bigint)").WillReturnResult(sqlmock.NewResult(0, 0))
			f.idle()
			f.held(false)
			f.held(false)
			f.unlock()
			f.rollback()
			result, err := f.p.Apply(context.Background(), testPlan())
			if !errors.Is(err, migration.ErrOutcomeUnknown) || result.Effect != operations.EffectUnknown {
				t.Fatal(result, err)
			}
			f.check(t)
		})
	}
}

func TestMySQLStatusRejectsNontransactionalOrGeneratedHistory(t *testing.T) {
	for _, kind := range []string{"myisam", "view", "generated"} {
		t.Run(kind, func(t *testing.T) {
			f := newRelationalFixture(t, "mysql")
			f.identity()
			f.m.ExpectBegin()
			engine, tabletype, extra := "InnoDB", "BASE TABLE", ""
			if kind == "myisam" {
				engine = "MyISAM"
			}
			if kind == "view" {
				tabletype = "VIEW"
			}
			if kind == "generated" {
				extra = "VIRTUAL GENERATED"
			}
			f.m.ExpectQuery(mysqlHistorySQL).WithArgs(f.schema, migration.Table).WillReturnRows(sqlmock.NewRows([]string{"kind", "engine", "name", "type", "nullable", "extra", "generated"}).AddRow(tabletype, engine, "version", "bigint", "NO", extra, ""))
			f.m.ExpectRollback()
			f.rollback()
			if _, err := f.p.Status(context.Background()); !errors.Is(err, migration.ErrInvalid) {
				t.Fatal(err)
			}
			f.check(t)
		})
	}
}

func TestSQLServerStatusRejectsViewsAndComputedHistory(t *testing.T) {
	for _, kind := range []string{"view", "computed", "alias", "synonym"} {
		t.Run(kind, func(t *testing.T) {
			f := newRelationalFixture(t, "sqlserver")
			f.identity()
			f.m.ExpectBegin()
			object, computed, user := "U", false, 127
			if kind == "view" {
				object = "V"
			}
			if kind == "synonym" {
				object = "SN"
			}
			if kind == "computed" {
				computed = true
			}
			if kind == "alias" {
				user = 256
			}
			f.m.ExpectQuery(sqlServerHistorySQL).WithArgs(f.schema, migration.Table).WillReturnRows(sqlmock.NewRows([]string{"kind", "name", "system", "user", "nullable", "computed", "generated"}).AddRow(object, "version", 127, user, false, computed, 0))
			f.m.ExpectRollback()
			f.rollback()
			if _, err := f.p.Status(context.Background()); !errors.Is(err, migration.ErrInvalid) {
				t.Fatal(err)
			}
			f.check(t)
		})
	}
}

func TestRelationalRejectsUnsupportedIdentityAndCanceledContext(t *testing.T) {
	f := newRelationalFixture(t, "mysql")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.p.Status(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	f.p.Engine = "redshift"
	if _, err := f.p.Status(context.Background()); !errors.Is(err, migration.ErrInvalid) {
		t.Fatal(err)
	}
	if err := f.m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLServerIdleRejectsDoomedAndImplicitTransactions(t *testing.T) {
	for _, state := range []struct{ count, state, implicit int }{{0, -1, 0}, {0, 2, 0}, {1, 1, 0}, {0, 1, 1}} {
		f := newRelationalFixture(t, "sqlserver")
		f.m.ExpectQuery("SELECT DB_NAME(), SCHEMA_NAME()").WillReturnRows(sqlmock.NewRows([]string{"db", "schema"}).AddRow("app", "dbo"))
		f.m.ExpectQuery("SELECT DB_NAME(), @@TRANCOUNT, XACT_STATE(), CASE WHEN (2 & @@OPTIONS) = 2 THEN 1 ELSE 0 END").WillReturnRows(sqlmock.NewRows([]string{"db", "count", "state", "implicit"}).AddRow("app", state.count, state.state, state.implicit))
		if _, err := f.p.Status(context.Background()); !errors.Is(err, migration.ErrInvalid) {
			t.Fatal(err)
		}
		if err := f.m.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}
