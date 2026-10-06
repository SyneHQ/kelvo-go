// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package watchers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SYNEHQ/kelvo-go/watch"
)

func expectMySQLActive(m MySQL, mock sqlmock.Sqlmock) {
	expectMySQLTable(m, mock, "_state", mysqlStateComment)
	expectMySQLState(m, mock, "active")
	expectMySQLTable(m, mock, "_events", mysqlSignature(mysqlFixtureColumns))
}

func TestMySQLAckChecksPayloadAndRollsBackWholeCheckpoint(t *testing.T) {
	m, mock := mysqlFixture(t)
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	events := []watch.Event{{ID: "1", Operation: "INSERT", Data: json.RawMessage(`{"id":1}`), OldData: json.RawMessage(`null`), Timestamp: now}, {ID: "3", Operation: "INSERT", Data: json.RawMessage(`{"id":3}`), OldData: json.RawMessage(`null`), Timestamp: now}}
	batch, err := watch.NewBatch(m.Scope, events)
	if err != nil {
		t.Fatal(err)
	}
	expectMySQLOpen(m, mock)
	mock.ExpectBegin()
	expectMySQLActive(m, mock)
	query := m.selectEvents() + ` WHERE id=CAST(? AS UNSIGNED) FOR UPDATE`
	mock.ExpectQuery(query).WithArgs(watch.MaxBatchBytes, watch.MaxBatchBytes, "1").WillReturnRows(sqlmock.NewRows([]string{"id", "op", "size", "data", "old", "at"}).AddRow(1, "INSERT", 8, []byte(`{"id":1}`), nil, "2026-10-06T00:00:00.000000Z"))
	mock.ExpectExec(`DELETE FROM ` + m.name("_events") + ` WHERE id=CAST(? AS UNSIGNED)`).WithArgs("1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(query).WithArgs(watch.MaxBatchBytes, watch.MaxBatchBytes, "3").WillReturnRows(sqlmock.NewRows([]string{"id", "op", "size", "data", "old", "at"}).AddRow(3, "INSERT", 8, []byte(`{"id":4}`), nil, "2026-10-06T00:00:00.000000Z"))
	mock.ExpectRollback()
	expectMySQLClose(m, mock)
	if err := m.Ack(context.Background(), *batch.Checkpoint, strings.Repeat("a", 64)); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("changed payload acknowledged", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLAckTouchesOnlyExplicitIDsAndRetainsUnknownCommit(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "duplicate", true: "lost_commit"}[unknown], func(t *testing.T) {
			m, mock := mysqlFixture(t)
			checkpoint := watch.Checkpoint{Version: watch.Version, ScopeSHA256: m.Scope.Key(), Generation: m.Scope.Generation, Entries: []watch.Entry{{ID: "18446744073709551615", SHA256: strings.Repeat("a", 64)}}}
			expectMySQLOpen(m, mock)
			mock.ExpectBegin()
			expectMySQLActive(m, mock)
			mock.ExpectQuery(m.selectEvents()+` WHERE id=CAST(? AS UNSIGNED) FOR UPDATE`).WithArgs(watch.MaxBatchBytes, watch.MaxBatchBytes, "18446744073709551615").WillReturnError(sql.ErrNoRows)
			commit := mock.ExpectCommit()
			if unknown {
				commit.WillReturnError(errors.New("lost commit response"))
			}
			expectMySQLClose(m, mock)
			err := m.Ack(context.Background(), checkpoint, strings.Repeat("b", 64))
			if unknown && !errors.Is(err, watch.ErrOutcomeUnknown) || !unknown && err != nil {
				t.Fatal("ack result changed", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("ack used a high-water mark or replayed", err)
			}
		})
	}
}

func TestMySQLReadRetainsOversizedEventAtSource(t *testing.T) {
	m, mock := mysqlFixture(t)
	expectMySQLOpen(m, mock)
	mock.ExpectBegin()
	expectMySQLActive(m, mock)
	mock.ExpectQuery(m.selectEvents()+` ORDER BY id LIMIT ?`).WithArgs(int64(1024), int64(1024), 1).WillReturnRows(sqlmock.NewRows([]string{"id", "op", "size", "data", "old", "at"}).AddRow(1, "INSERT", 2048, nil, nil, "2026-10-06T00:00:00.000000Z"))
	mock.ExpectRollback()
	expectMySQLClose(m, mock)
	if _, err := m.Read(context.Background(), 1, 1, 1024); !errors.Is(err, watch.ErrLimit) {
		t.Fatal("oversized event skipped", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLReadPreservesUnsignedIDsAndDecimalJSON(t *testing.T) {
	m, mock := mysqlFixture(t)
	expectMySQLOpen(m, mock)
	mock.ExpectBegin()
	expectMySQLActive(m, mock)
	data := []byte(`{"amount":12345678901234567890.123}`)
	mock.ExpectQuery(m.selectEvents()+` ORDER BY id LIMIT ?`).WithArgs(int64(4096), int64(4096), 1).WillReturnRows(sqlmock.NewRows([]string{"id", "op", "size", "data", "old", "at"}).AddRow("18446744073709551615", "INSERT", len(data), data, nil, "2026-10-06T00:00:00.000001Z"))
	mock.ExpectRollback()
	expectMySQLClose(m, mock)
	batch, err := m.Read(context.Background(), 1, 1, 4096)
	if err != nil || len(batch.Events) != 1 || batch.Events[0].ID != "18446744073709551615" || string(batch.Events[0].Data) != string(data) || batch.Events[0].Timestamp.Nanosecond() != 1000 {
		t.Fatal("source values rounded", batch, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLRejectsProviderResumeBeforeDatabaseAccess(t *testing.T) {
	m, mock := mysqlFixture(t)
	checkpoint := watch.Checkpoint{Version: watch.Version, ScopeSHA256: m.Scope.Key(), Generation: m.Scope.Generation, Resume: &watch.Resume{From: "YQ==", To: "Yg=="}}
	if err := m.Ack(context.Background(), checkpoint, strings.Repeat("a", 64)); !errors.Is(err, watch.ErrInvalid) {
		t.Fatal("provider checkpoint reached SQL", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
