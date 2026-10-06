// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	wire "github.com/SYNEHQ/kelvo-go/ingestion"
)

func mockStore(t *testing.T) (Store, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	return Store{DB: db, Scope: fixtureScope()}, mock
}

func expectBegin(mock sqlmock.Sqlmock, store Store) {
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT current_database\(\)`).WillReturnRows(sqlmock.NewRows([]string{"database"}).AddRow(store.Scope.Database))
	mock.ExpectExec(`SET LOCAL statement_timeout`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`SET LOCAL lock_timeout`).WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectCommitBatch(t *testing.T, store Store, mock sqlmock.Sqlmock) {
	t.Helper()
	expectBegin(mock, store)
	mock.ExpectQuery(`SELECT sequence FROM .* FOR UPDATE`).WithArgs(store.Scope.Key()).WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(0))
	mock.ExpectQuery(`SELECT batch_id,digest,sequence,record_count,committed_at`).WithArgs(store.Scope.Key(), "batch-1").WillReturnRows(sqlmock.NewRows([]string{"id", "digest", "sequence", "records", "at"}))
	mock.ExpectQuery(`SELECT clock_timestamp\(\)`).WillReturnRows(sqlmock.NewRows([]string{"at"}).AddRow(time.Unix(1800000000, 0).UTC()))
	for range 3 {
		mock.ExpectExec(`INSERT INTO`).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec(`UPDATE .* SET sequence=`).WillReturnResult(sqlmock.NewResult(0, 1))
}

func TestLostCommitAcknowledgementIsUnknown(t *testing.T) {
	store, mock := mockStore(t)
	expectCommitBatch(t, store, mock)
	mock.ExpectCommit().WillReturnError(errors.New("connection lost after dispatch"))
	receipt, err := store.Commit(context.Background(), fixtureBatch())
	if !errors.Is(err, wire.ErrOutcomeUnknown) || receipt.BatchID != "" {
		t.Fatalf("ambiguous commit claimed success: %v", err)
	}
}

func TestCommittedBatchReturnsExactRetryReceiptWithoutAnotherCommit(t *testing.T) {
	store, mock := mockStore(t)
	batch := fixtureBatch()
	hash, _ := batch.Digest()
	wanted := wire.Receipt{BatchID: batch.ID, Digest: hash, Sequence: 1, Records: len(batch.Records), CommittedAt: time.Unix(1800000000, 123000).UTC()}
	expectBegin(mock, store)
	mock.ExpectQuery(`SELECT sequence FROM .* FOR UPDATE`).WithArgs(store.Scope.Key()).WillReturnRows(sqlmock.NewRows([]string{"sequence"}).AddRow(99))
	mock.ExpectQuery(`SELECT batch_id,digest,sequence,record_count,committed_at`).WithArgs(store.Scope.Key(), batch.ID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "digest", "sequence", "records", "at"}).AddRow(wanted.BatchID, wanted.Digest, wanted.Sequence, wanted.Records, wanted.CommittedAt))
	mock.ExpectRollback()
	got, err := store.Commit(context.Background(), batch)
	if err != nil || got != wanted {
		t.Fatalf("retry receipt changed: %v", err)
	}
}

func TestInvalidBatchNeverOpensTransaction(t *testing.T) {
	store, _ := mockStore(t)
	batch := fixtureBatch()
	batch.Records = append(batch.Records, batch.Records[0])
	if _, err := store.Commit(context.Background(), batch); !errors.Is(err, wire.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestStateCannotInstallOrCommit(t *testing.T) {
	store, mock := mockStore(t)
	expectBegin(mock, store)
	mock.ExpectQuery(`SELECT sequence,`).WithArgs(store.Scope.Key()).WillReturnRows(sqlmock.NewRows([]string{"sequence", "checkpoint", "receipt"}).AddRow(0, []byte(`{}`), nil))
	mock.ExpectRollback()
	state, err := store.State(context.Background())
	if err != nil || state.Sequence != 0 || state.LastReceipt != nil {
		t.Fatal(err)
	}
}

func TestStateRejectsCorruptReceipt(t *testing.T) {
	store, mock := mockStore(t)
	expectBegin(mock, store)
	mock.ExpectQuery(`SELECT sequence,`).WillReturnRows(sqlmock.NewRows([]string{"sequence", "checkpoint", "receipt"}).AddRow(2, []byte(`{}`), nil))
	mock.ExpectRollback()
	if _, err := store.State(context.Background()); !errors.Is(err, wire.ErrInvalid) {
		t.Fatal("corrupt committed state accepted", err)
	}
}

func TestInstallCommitFailureIsUnknown(t *testing.T) {
	store, mock := mockStore(t)
	expectBegin(mock, store)
	mock.ExpectExec(`SELECT pg_advisory_xact_lock`).WillReturnResult(sqlmock.NewResult(0, 0))
	for range 5 {
		mock.ExpectExec(`CREATE `).WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectExec(`INSERT INTO`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errors.New("commit acknowledgement lost"))
	if err := store.Install(context.Background()); !errors.Is(err, wire.ErrOutcomeUnknown) {
		t.Fatal("install claimed definite failure", err)
	}
}
