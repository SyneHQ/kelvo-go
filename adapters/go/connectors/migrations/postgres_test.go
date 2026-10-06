// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package migrations

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/jackc/pgx/v5/pgconn"
)

const versionSQL = `SELECT version, dirty FROM "public"."schema_migrations" LIMIT 2`
const table = `"public"."schema_migrations"`

func fixture(t *testing.T) (PostgreSQL, sqlmock.Sqlmock) {
	t.Helper()
	db, m, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return PostgreSQL{DB: db, Database: "app", Schema: "public", transactionStatus: func(*sql.Conn) (byte, error) { return 'I', nil }}, m
}
func expectIdentity(m sqlmock.Sqlmock) {
	m.ExpectQuery("SELECT current_database(), current_schema()").WillReturnRows(sqlmock.NewRows([]string{"database", "schema"}).AddRow("app", "public"))
}
func expectPGHistory(m sqlmock.Sqlmock, exists bool) {
	rows := sqlmock.NewRows([]string{"kind", "rls", "rules", "name", "type", "notnull", "generated", "identity", "triggers", "inheritance"})
	if exists {
		rows.AddRow("r", false, false, "version", 20, true, "", "", false, false).AddRow("r", false, false, "dirty", 16, true, "", "", false, false)
	}
	m.ExpectQuery(postgresHistorySQL).WithArgs(table).WillReturnRows(rows)
}
func expectVersion(m sqlmock.Sqlmock, v int64, dirty bool) {
	expectPGHistory(m, true)
	m.ExpectQuery(versionSQL).WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).AddRow(v, dirty))
}
func expectLock(m sqlmock.Sqlmock) {
	m.ExpectQuery("SELECT pg_try_advisory_lock($1)").WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(true))
}
func expectUnlock(m sqlmock.Sqlmock) {
	m.ExpectQuery("SELECT pg_advisory_unlock($1)").WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"unlocked"}).AddRow(true))
}
func expectHeld(m sqlmock.Sqlmock, held bool) {
	m.ExpectQuery(lockHeldSQL).WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"held"}).AddRow(held))
}
func expectFinal(m sqlmock.Sqlmock, v int64, dirty bool) {
	expectHeld(m, true)
	expectVersion(m, v, dirty)
	expectUnlock(m)
}
func expectSet(m sqlmock.Sqlmock, v int, dirty bool, commitErr error) {
	expectHeld(m, true)
	m.ExpectBegin()
	expectPGHistory(m, true)
	m.ExpectExec("DELETE FROM " + table).WillReturnResult(sqlmock.NewResult(0, 1))
	if v >= 0 || dirty {
		m.ExpectExec("INSERT INTO "+table+" (version, dirty) VALUES ($1, $2)").WithArgs(v, dirty).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	if commitErr == nil {
		m.ExpectCommit()
	} else {
		m.ExpectCommit().WillReturnError(commitErr)
	}
}
func testPlan() migration.Plan {
	return migration.Plan{Version: migration.Version, Expected: migration.State{Version: 1}, Direction: "up", Files: []migration.File{{Name: "1_old.up.sql", Content: "SELECT 1"}, {Name: "2_orders.up.sql", Content: "CREATE TABLE orders(id bigint)"}}}
}

func TestStatusOnFreshDatabaseNeverInstallsOrLocks(t *testing.T) {
	p, m := fixture(t)
	expectIdentity(m)
	m.ExpectBegin()
	expectPGHistory(m, false)
	m.ExpectRollback()
	state, err := p.Status(context.Background())
	if err != nil || state != (migration.State{Version: -1}) {
		t.Fatal(state, err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if p.DB.Stats().OpenConnections != 0 {
		t.Fatal("status session returned to reusable pool")
	}
}

func TestScriptLeavingTransactionOpenCannotBecomeCleanVersion(t *testing.T) {
	p, m := fixture(t)
	calls := 0
	p.transactionStatus = func(*sql.Conn) (byte, error) {
		calls++
		if calls == 4 || calls == 5 {
			return 'T', nil
		}
		return 'I', nil
	}
	plan := testPlan()
	plan.Files[1].Content = "BEGIN; CREATE TABLE orders(id bigint)"
	expectIdentity(m)
	expectLock(m)
	expectVersion(m, 1, false)
	m.ExpectQuery("SELECT to_regclass($1) IS NOT NULL").WithArgs(table).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	expectVersion(m, 1, false)
	expectSet(m, 2, true, nil)
	expectHeld(m, true)
	m.ExpectExec(plan.Files[1].Content).WillReturnResult(sqlmock.NewResult(0, 0))
	m.ExpectExec("ROLLBACK").WillReturnResult(sqlmock.NewResult(0, 0))
	expectFinal(m, 2, true)
	result, err := p.Apply(context.Background(), plan)
	if !errors.Is(err, migration.ErrOutcomeUnknown) || result.Effect != operations.EffectUnknown {
		t.Fatal("open transaction reported committed", result, err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal("open script reached clean SetVersion", err)
	}
	if p.DB.Stats().OpenConnections != 0 {
		t.Fatal("transaction session was returned to pool")
	}
}

func TestVersionUpdateRejectsNonidleTransactionBeforeHistoryWrite(t *testing.T) {
	p, m := fixture(t)
	calls := 0
	p.transactionStatus = func(*sql.Conn) (byte, error) {
		calls++
		if calls == 2 || calls == 3 {
			return 'E', nil
		}
		return 'I', nil
	}
	expectIdentity(m)
	expectLock(m)
	expectVersion(m, 1, false)
	m.ExpectQuery("SELECT to_regclass($1) IS NOT NULL").WithArgs(table).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	expectVersion(m, 1, false)
	m.ExpectExec("ROLLBACK").WillReturnResult(sqlmock.NewResult(0, 0))
	expectFinal(m, 1, false)
	result, err := p.Apply(context.Background(), testPlan())
	if !errors.Is(err, migration.ErrOutcomeUnknown) || result.Effect != operations.EffectUnknown {
		t.Fatal(result, err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal("nonidle session wrote history", err)
	}
}

func TestMigrationRequiresRealTransactionStateInProduction(t *testing.T) {
	p, m := fixture(t)
	p.transactionStatus = nil
	expectIdentity(m)
	if _, err := p.Status(context.Background()); !errors.Is(err, migration.ErrInvalid) {
		t.Fatal("uninspectable transaction state was accepted")
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if p.DB.Stats().OpenConnections != 0 {
		t.Fatal("uninspectable session retained")
	}
}

func TestScriptCannotDropMigrationLockThenMarkHistoryClean(t *testing.T) {
	p, m := fixture(t)
	plan := testPlan()
	plan.Files[1].Content = "SELECT pg_advisory_unlock_all()"
	expectIdentity(m)
	expectLock(m)
	expectVersion(m, 1, false)
	m.ExpectQuery("SELECT to_regclass($1) IS NOT NULL").WithArgs(table).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	expectVersion(m, 1, false)
	expectSet(m, 2, true, nil)
	expectHeld(m, true)
	m.ExpectExec(plan.Files[1].Content).WillReturnResult(sqlmock.NewResult(0, 0))
	expectHeld(m, false)
	// No final version receipt may be captured after losing the lock.
	expectHeld(m, false)
	m.ExpectQuery("SELECT pg_advisory_unlock($1)").WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"unlocked"}).AddRow(false))
	result, err := p.Apply(context.Background(), plan)
	if !errors.Is(err, migration.ErrOutcomeUnknown) || result.Effect != operations.EffectUnknown {
		t.Fatal("lost-lock script reported a settled outcome", result, err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal("lost lock allowed history write/final receipt", err)
	}
}

func TestStatusRejectsCorruptMultipleVersionRows(t *testing.T) {
	p, m := fixture(t)
	expectIdentity(m)
	m.ExpectBegin()
	expectPGHistory(m, true)
	m.ExpectQuery(versionSQL).WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).AddRow(1, false).AddRow(2, true))
	m.ExpectRollback()
	if _, err := p.Status(context.Background()); err == nil {
		t.Fatal("ambiguous history accepted")
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStatusPropagatesReadOnlyViewWriteRejection(t *testing.T) {
	p, m := fixture(t)
	expectIdentity(m)
	m.ExpectBegin()
	expectPGHistory(m, true)
	m.ExpectQuery(versionSQL).WillReturnError(&pgconn.PgError{Code: "25006", Message: "cannot execute INSERT in a read-only transaction"})
	m.ExpectRollback()
	if _, err := p.Status(context.Background()); err == nil {
		t.Fatal("write-capable history view accepted")
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if p.DB.Stats().OpenConnections != 0 {
		t.Fatal("failed status session retained")
	}
}

func TestApplyChecksExpectedStateBeforeAnyDDL(t *testing.T) {
	p, m := fixture(t)
	expectIdentity(m)
	expectLock(m)
	expectVersion(m, 2, false)
	expectFinal(m, 2, false)
	result, err := p.Apply(context.Background(), testPlan())
	if !errors.Is(err, migration.ErrConflict) || result.Effect != operations.EffectNone {
		t.Fatal(result, err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestApplyKeepsFinalReceiptInsideLock(t *testing.T) {
	p, m := fixture(t)
	expectIdentity(m)
	expectLock(m)
	expectVersion(m, 1, false)
	m.ExpectQuery("SELECT to_regclass($1) IS NOT NULL").WithArgs(table).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	expectVersion(m, 1, false)
	expectSet(m, 2, true, nil)
	expectHeld(m, true)
	m.ExpectExec("CREATE TABLE orders(id bigint)").WillReturnResult(sqlmock.NewResult(0, 0))
	expectHeld(m, true)
	expectSet(m, 2, false, nil)
	expectFinal(m, 2, false)
	result, err := p.Apply(context.Background(), testPlan())
	if err != nil || result.Effect != operations.EffectCommitted || result.To.Version != 2 || len(result.FilesApplied) != 1 || result.FilesApplied[0] != "2_orders.up.sql" {
		t.Fatal(result, err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal("query occurred after releasing migration lock", err)
	}
}

func TestLostVersionCommitRemainsUnknown(t *testing.T) {
	p, m := fixture(t)
	expectIdentity(m)
	expectLock(m)
	expectVersion(m, 1, false)
	m.ExpectQuery("SELECT to_regclass($1) IS NOT NULL").WithArgs(table).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	expectVersion(m, 1, false)
	expectSet(m, 2, true, errors.New("lost response"))
	expectFinal(m, 2, true)
	result, err := p.Apply(context.Background(), testPlan())
	if !errors.Is(err, migration.ErrOutcomeUnknown) || result.Effect != operations.EffectUnknown {
		t.Fatal(result, err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestScriptFailureDoesNotInventRollback(t *testing.T) {
	p, m := fixture(t)
	expectIdentity(m)
	expectLock(m)
	expectVersion(m, 1, false)
	m.ExpectQuery("SELECT to_regclass($1) IS NOT NULL").WithArgs(table).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	expectVersion(m, 1, false)
	expectSet(m, 2, true, nil)
	expectHeld(m, true)
	m.ExpectExec("CREATE TABLE orders(id bigint)").WillReturnError(errors.New("source interrupted"))
	expectFinal(m, 2, true)
	result, err := p.Apply(context.Background(), testPlan())
	if !errors.Is(err, migration.ErrOutcomeUnknown) || result.Effect != operations.EffectUnknown {
		t.Fatal(result, err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStatusRejectsHistoryViewBeforeEvaluatingIt(t *testing.T) {
	p, m := fixture(t)
	expectIdentity(m)
	m.ExpectBegin()
	m.ExpectQuery(postgresHistorySQL).WithArgs(table).WillReturnRows(sqlmock.NewRows([]string{"kind", "rls", "rules", "name", "type", "notnull", "generated", "identity", "triggers", "inheritance"}).AddRow("v", false, true, "version", 20, true, "", "", false, false))
	m.ExpectRollback()
	if _, err := p.Status(context.Background()); !errors.Is(err, migration.ErrInvalid) {
		t.Fatal(err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal("view evaluated", err)
	}
}
