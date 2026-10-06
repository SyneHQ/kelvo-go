// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package watchers

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SYNEHQ/kelvo-go/watch"
)

var mysqlFixtureColumns = []mysqlColumn{{Name: "id", Kind: "bigint"}, {Name: "amount", Kind: "decimal"}}

func mysqlFixture(t *testing.T) (MySQL, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return MySQL{DB: db, Scope: watch.Scope{TeamID: "team", ConnectionID: "saved", Database: "app", Schema: "app", Table: "orders", ID: "watcher", Generation: "generation"}}, mock
}

func expectMySQLOpen(m MySQL, mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT DATABASE(),CURRENT_USER()`).WillReturnRows(sqlmock.NewRows([]string{"database", "user"}).AddRow(m.Scope.Database, "watcher@%"))
	mock.ExpectQuery(`SELECT GET_LOCK(?,5)`).WithArgs(objectName(m.Scope.ID)).WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(1))
}

func expectMySQLClose(m MySQL, mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT RELEASE_LOCK(?)`).WithArgs(objectName(m.Scope.ID)).WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
}

func expectMySQLTable(m MySQL, mock sqlmock.Sqlmock, suffix, comment string) {
	mock.ExpectQuery(`SELECT ENGINE,TABLE_TYPE,TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`).WithArgs(m.Scope.Schema, objectName(m.Scope.ID)+suffix).WillReturnRows(sqlmock.NewRows([]string{"engine", "kind", "comment"}).AddRow("InnoDB", "BASE TABLE", comment))
}

func expectMySQLState(m MySQL, mock sqlmock.Sqlmock, state string) {
	query := mock.ExpectQuery(`SELECT scope_key,state,source_signature,source_columns FROM ` + m.name("_state") + ` WHERE generation=?`).WithArgs(m.Scope.Generation)
	if state == "" {
		query.WillReturnError(sql.ErrNoRows)
	} else {
		query.WillReturnRows(sqlmock.NewRows([]string{"key", "state", "signature", "columns"}).AddRow(m.Scope.Key(), state, mysqlSignature(mysqlFixtureColumns), mysqlColumnsJSON(mysqlFixtureColumns)))
	}
}

func expectMySQLSource(m MySQL, mock sqlmock.Sqlmock, columns []mysqlColumn) {
	mock.ExpectQuery(`SELECT ENGINE,TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`).WithArgs(m.Scope.Schema, m.Scope.Table).WillReturnRows(sqlmock.NewRows([]string{"engine", "kind"}).AddRow("InnoDB", "BASE TABLE"))
	rows := sqlmock.NewRows([]string{"name", "kind"})
	for _, column := range columns {
		rows.AddRow(column.Name, column.Kind)
	}
	mock.ExpectQuery(`SELECT COLUMN_NAME,DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`).WithArgs(m.Scope.Schema, m.Scope.Table).WillReturnRows(rows)
}

func expectMySQLTrigger(m MySQL, mock sqlmock.Sqlmock, operation string, exists bool) {
	query := mock.ExpectQuery(`SELECT EVENT_OBJECT_TABLE,EVENT_MANIPULATION,ACTION_TIMING,ACTION_STATEMENT,DEFINER FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=? AND TRIGGER_NAME=?`).WithArgs(m.Scope.Schema, objectName(m.Scope.ID)+"_"+strings.ToLower(operation))
	if exists {
		query.WillReturnRows(sqlmock.NewRows([]string{"table", "event", "timing", "body", "definer"}).AddRow(m.Scope.Table, operation, "AFTER", m.triggerBody(operation, mysqlFixtureColumns), "watcher@%"))
	} else {
		query.WillReturnError(sql.ErrNoRows)
	}
}

func expectMySQLOutbox(m MySQL, mock sqlmock.Sqlmock) {
	signature := mysqlSignature(mysqlFixtureColumns)
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS ` + m.name("_events") + ` (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,operation VARCHAR(6) NOT NULL,row_data JSON NULL,old_row_data JSON NULL,created_at DATETIME(6) NOT NULL) ENGINE=InnoDB COMMENT='` + signature + `'`).WillReturnResult(sqlmock.NewResult(0, 0))
	expectMySQLTable(m, mock, "_events", signature)
}

func TestMySQLPartialInstallationResumesOnlyItsGeneration(t *testing.T) {
	m, mock := mysqlFixture(t)
	for attempt := 0; attempt < 2; attempt++ {
		expectMySQLOpen(m, mock)
		expectMySQLTable(m, mock, "_state", mysqlStateComment)
		state := ""
		if attempt == 1 {
			state = "installing"
		}
		expectMySQLState(m, mock, state)
		expectMySQLSource(m, mock, mysqlFixtureColumns)
		if attempt == 0 {
			mock.ExpectQuery(`SELECT EXISTS(SELECT 1 FROM ` + m.name("_state") + ` WHERE state<>'retired')`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
			mock.ExpectExec(`INSERT INTO `+m.name("_state")+` (generation,scope_key,state,source_signature,source_columns) VALUES(?,?,'installing',?,?)`).WithArgs(m.Scope.Generation, m.Scope.Key(), mysqlSignature(mysqlFixtureColumns), mysqlColumnsJSON(mysqlFixtureColumns)).WillReturnResult(sqlmock.NewResult(0, 1))
		}
		expectMySQLOutbox(m, mock)
		for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			exists := attempt == 1 && operation == "INSERT"
			expectMySQLTrigger(m, mock, operation, exists)
			if exists {
				continue
			}
			create := mock.ExpectExec(`CREATE TRIGGER ` + m.name("_"+strings.ToLower(operation)) + ` AFTER ` + operation + ` ON ` + m.table() + ` FOR EACH ROW ` + m.triggerBody(operation, mysqlFixtureColumns))
			if attempt == 0 && operation == "UPDATE" {
				create.WillReturnError(errors.New("connection lost after first trigger"))
				break
			}
			create.WillReturnResult(sqlmock.NewResult(0, 0))
		}
		if attempt == 1 {
			mock.ExpectExec(`UPDATE ` + m.name("_state") + ` SET state='active' WHERE generation=? AND state='installing'`).WithArgs(m.Scope.Generation).WillReturnResult(sqlmock.NewResult(0, 1))
		}
		expectMySQLClose(m, mock)
		err := m.Install(context.Background())
		if attempt == 0 && !errors.Is(err, watch.ErrOutcomeUnknown) || attempt == 1 && err != nil {
			t.Fatal("partial DDL lifecycle lost", attempt, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMySQLPartialRemovalResumesWithoutReinstalling(t *testing.T) {
	m, mock := mysqlFixture(t)
	for attempt := 0; attempt < 2; attempt++ {
		expectMySQLOpen(m, mock)
		expectMySQLTable(m, mock, "_state", mysqlStateComment)
		state := "active"
		if attempt == 1 {
			state = "removing"
		}
		expectMySQLState(m, mock, state)
		expectMySQLTable(m, mock, "_events", mysqlSignature(mysqlFixtureColumns))
		for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			expectMySQLTrigger(m, mock, operation, attempt == 0 || operation != "INSERT")
		}
		if attempt == 0 {
			mock.ExpectExec(`UPDATE ` + m.name("_state") + ` SET state='removing' WHERE generation=? AND state IN ('active','installing')`).WithArgs(m.Scope.Generation).WillReturnResult(sqlmock.NewResult(0, 1))
		}
		for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			drop := mock.ExpectExec(`DROP TRIGGER IF EXISTS ` + m.name("_"+strings.ToLower(operation)))
			if attempt == 0 && operation == "UPDATE" {
				drop.WillReturnError(errors.New("connection lost after first drop"))
				break
			}
			drop.WillReturnResult(sqlmock.NewResult(0, 0))
		}
		if attempt == 1 {
			mock.ExpectExec(`DROP TABLE IF EXISTS ` + m.name("_events")).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(`UPDATE ` + m.name("_state") + ` SET state='retired' WHERE generation=? AND state='removing'`).WithArgs(m.Scope.Generation).WillReturnResult(sqlmock.NewResult(0, 1))
		}
		expectMySQLClose(m, mock)
		err := m.Remove(context.Background())
		if attempt == 0 && !errors.Is(err, watch.ErrOutcomeUnknown) || attempt == 1 && err != nil {
			t.Fatal("partial removal lifecycle lost", attempt, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMySQLRetiredGenerationCannotReviveOrRemoveReplacement(t *testing.T) {
	for _, action := range []string{"install", "remove"} {
		t.Run(action, func(t *testing.T) {
			m, mock := mysqlFixture(t)
			expectMySQLOpen(m, mock)
			expectMySQLTable(m, mock, "_state", mysqlStateComment)
			expectMySQLState(m, mock, "retired")
			expectMySQLClose(m, mock)
			var err error
			if action == "install" {
				err = m.Install(context.Background())
				if !errors.Is(err, watch.ErrConflict) {
					t.Fatal("retired generation reinstalled", err)
				}
			} else if err = m.Remove(context.Background()); err != nil {
				t.Fatal("retired cleanup changed the replacement", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMySQLDifferentGenerationCannotAdoptPartialInstallation(t *testing.T) {
	m, mock := mysqlFixture(t)
	expectMySQLOpen(m, mock)
	expectMySQLTable(m, mock, "_state", mysqlStateComment)
	expectMySQLState(m, mock, "")
	expectMySQLSource(m, mock, mysqlFixtureColumns)
	mock.ExpectQuery(`SELECT EXISTS(SELECT 1 FROM ` + m.name("_state") + ` WHERE state<>'retired')`).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	expectMySQLClose(m, mock)
	if err := m.Install(context.Background()); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("another generation adopted unfinished objects", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLInstalledSchemaCannotSilentlyChange(t *testing.T) {
	m, mock := mysqlFixture(t)
	expectMySQLOpen(m, mock)
	expectMySQLTable(m, mock, "_state", mysqlStateComment)
	expectMySQLState(m, mock, "active")
	expectMySQLSource(m, mock, append(append([]mysqlColumn(nil), mysqlFixtureColumns...), mysqlColumn{Name: "added", Kind: "text"}))
	expectMySQLClose(m, mock)
	if err := m.Install(context.Background()); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("stale trigger definition accepted", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLUnlockFailureRetiresPhysicalSession(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   any
		err     error
		discard bool
	}{{"released", int64(1), nil, false}, {"not_owned", int64(0), nil, true}, {"missing", nil, nil, true}, {"failure", nil, errors.New("unlock failed"), true}} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			query := mock.ExpectQuery(`SELECT RELEASE_LOCK(?)`).WithArgs("lock")
			if tc.err != nil {
				query.WillReturnError(tc.err)
			} else {
				query.WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(tc.value))
			}
			if tc.discard {
				mock.ExpectClose()
			}
			releaseMySQLLock(conn, "lock")
			err = conn.PingContext(context.Background())
			if tc.discard && !errors.Is(err, sql.ErrConnDone) || !tc.discard && err != nil {
				t.Fatal("lock escaped through pool reuse", err)
			}
			_ = conn.Close()
			if !tc.discard {
				mock.ExpectClose()
			}
			_ = db.Close()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
