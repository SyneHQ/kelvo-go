// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package watchers

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SYNEHQ/kelvo-go/watch"
)

func TestPostgreSQLRejectsMongoResumeBeforeSourceActivity(t *testing.T) {
	p, m := pgFixture(t)
	batch, err := watch.NewResumeBatch(p.Scope, nil, base64.StdEncoding.EncodeToString([]byte("from")), base64.StdEncoding.EncodeToString([]byte("to")))
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(p.Ack(context.Background(), *batch.Checkpoint, strings.Repeat("a", 64)), watch.ErrInvalid) {
		t.Fatal("Mongo checkpoint accepted by SQL watcher")
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func pgFixture(t *testing.T) (PostgreSQL, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return PostgreSQL{DB: db, Scope: watch.Scope{TeamID: "team", ConnectionID: "saved", Database: "app", Schema: "public", Table: "orders", ID: "watcher", Generation: "generation"}}, mock
}

func expectBegin(p PostgreSQL, m sqlmock.Sqlmock) {
	m.ExpectBegin()
	m.ExpectQuery(`SELECT current_database()`).WillReturnRows(sqlmock.NewRows([]string{"database"}).AddRow(p.Scope.Database))
	for _, sql := range []string{`SET LOCAL statement_timeout = '20s'`, `SET LOCAL lock_timeout = '5s'`, `SET LOCAL standard_conforming_strings = on`} {
		m.ExpectExec(sql).WillReturnResult(sqlmock.NewResult(0, 0))
	}
	m.ExpectExec(`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`).WithArgs(p.name("")).WillReturnResult(sqlmock.NewResult(0, 1))
}
func expectGeneration(p PostgreSQL, m sqlmock.Sqlmock, state string) {
	m.ExpectQuery(`SELECT scope_key,state FROM ` + p.name("_state") + ` WHERE generation=$1`).WithArgs(p.Scope.Generation).WillReturnRows(sqlmock.NewRows([]string{"key", "state"}).AddRow(p.Scope.Key(), state))
}

func TestAckChecksEachEventAndRollsBackOnChangedPayload(t *testing.T) {
	p, m := pgFixture(t)
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	events := []watch.Event{{ID: "1", Operation: "INSERT", Data: json.RawMessage(`{"id":1}`), OldData: json.RawMessage(`null`), Timestamp: now}, {ID: "3", Operation: "INSERT", Data: json.RawMessage(`{"id":3}`), OldData: json.RawMessage(`null`), Timestamp: now}}
	batch, err := watch.NewBatch(p.Scope, events)
	if err != nil {
		t.Fatal(err)
	}
	expectBegin(p, m)
	expectGeneration(p, m, "active")
	selectSQL := p.selectEvents() + ` WHERE id=$2 FOR UPDATE`
	m.ExpectQuery(selectSQL).WithArgs(watch.MaxBatchBytes, int64(1)).WillReturnRows(sqlmock.NewRows([]string{"id", "op", "size", "data", "old", "at"}).AddRow(1, "INSERT", 8, []byte(`{"id":1}`), nil, now))
	m.ExpectExec(`DELETE FROM ` + p.name("_events") + ` WHERE id=$1`).WithArgs(int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectQuery(selectSQL).WithArgs(watch.MaxBatchBytes, int64(3)).WillReturnRows(sqlmock.NewRows([]string{"id", "op", "size", "data", "old", "at"}).AddRow(3, "INSERT", 8, []byte(`{"id":4}`), nil, now))
	m.ExpectRollback()
	if !errors.Is(p.Ack(context.Background(), *batch.Checkpoint, strings.Repeat("a", 64)), watch.ErrConflict) {
		t.Fatal("changed payload acknowledged")
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAckDoesNotDeleteUnlistedLateCommitAndIsRetrySafe(t *testing.T) {
	p, m := pgFixture(t)
	checkpoint := watch.Checkpoint{Version: watch.Version, ScopeSHA256: p.Scope.Key(), Generation: p.Scope.Generation, Entries: []watch.Entry{{ID: "3", SHA256: strings.Repeat("a", 64)}}}
	expectBegin(p, m)
	expectGeneration(p, m, "active")
	m.ExpectQuery(p.selectEvents()+` WHERE id=$2 FOR UPDATE`).WithArgs(watch.MaxBatchBytes, int64(3)).WillReturnError(sql.ErrNoRows)
	m.ExpectCommit()
	if err := p.Ack(context.Background(), checkpoint, strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal("ack touched IDs outside checkpoint", err)
	}
}

func TestRetiredGenerationCannotReadOrAck(t *testing.T) {
	p, m := pgFixture(t)
	expectBegin(p, m)
	expectGeneration(p, m, "retired")
	m.ExpectRollback()
	if _, err := p.Read(context.Background(), 10, 1, 4096); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("retired reader accepted", err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOversizedEventStaysAtSource(t *testing.T) {
	p, m := pgFixture(t)
	expectBegin(p, m)
	expectGeneration(p, m, "active")
	m.ExpectQuery(p.selectEvents()+` ORDER BY id LIMIT $2`).WithArgs(int64(1024), 1).WillReturnRows(sqlmock.NewRows([]string{"id", "op", "size", "data", "old", "at"}).AddRow(1, "INSERT", 2048, nil, nil, time.Now().UTC()))
	m.ExpectRollback()
	if _, err := p.Read(context.Background(), 1, 1, 1024); !errors.Is(err, watch.ErrLimit) {
		t.Fatal("oversized event skipped", err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownAckCommitIsNeverReportedAsRollback(t *testing.T) {
	p, m := pgFixture(t)
	checkpoint := watch.Checkpoint{Version: watch.Version, ScopeSHA256: p.Scope.Key(), Generation: p.Scope.Generation, Entries: []watch.Entry{{ID: "3", SHA256: strings.Repeat("a", 64)}}}
	expectBegin(p, m)
	expectGeneration(p, m, "active")
	m.ExpectQuery(p.selectEvents()+` WHERE id=$2 FOR UPDATE`).WithArgs(watch.MaxBatchBytes, int64(3)).WillReturnError(sql.ErrNoRows)
	m.ExpectCommit().WillReturnError(errors.New("lost commit response"))
	if err := p.Ack(context.Background(), checkpoint, strings.Repeat("b", 64)); !errors.Is(err, watch.ErrOutcomeUnknown) {
		t.Fatal("ambiguous commit lost", err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
