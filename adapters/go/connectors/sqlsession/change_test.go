package sqlsession

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestStatementValidation(t *testing.T) {
	for _, statement := range []string{
		"COMMIT", "/* comment */ SET ROLE admin", "UPDATE t SET a=1; DROP TABLE t",
		"/*!50000 COMMIT */", "UPDATE other.t SET a=1", `UPDATE "other"."t" SET a=1`,
		"UPDATE t SET a='unterminated", "UPDATE t SET a=$$body$$", "CALL commit_changes()",
		"UPDATE t SET a='\\'; COMMIT --'", "UPDATE t SET a=1;;", "UPDATE t SET a=1 # hidden",
	} {
		if err := Validate("mysql", []string{statement}, Options{Transaction: true}); err == nil {
			t.Errorf("accepted %q", statement)
		}
	}
	for _, statement := range []string{"UPDATE t SET a='a;b';", "/* comment */ UPDATE t SET a='it''s valid'", "DELETE FROM t WHERE id=?"} {
		if err := Validate("postgresql", []string{statement}, Options{}); err != nil {
			t.Errorf("rejected %q: %v", statement, err)
		}
	}
	if Validate("clickhouse", []string{"INSERT INTO t VALUES (1)"}, Options{Transaction: true}) == nil {
		t.Fatal("ClickHouse transaction accepted")
	}
}

func TestRollbackAndSessionDiscard(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`SET ROLE "reviewer"`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE items").WithArgs(int64(9007199254740993)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO items").WillReturnError(errors.New("constraint violation"))
	mock.ExpectRollback()
	mock.ExpectClose()
	result, err := ExecuteStatements(context.Background(), db, "postgresql", []Statement{
		{SQL: "UPDATE items SET amount=$1", Parameters: []any{int64(9007199254740993)}},
		{SQL: "INSERT INTO items(id) VALUES(1)"},
	}, Options{Transaction: true, Role: "reviewer"})
	if err == nil || result.Outcome != Failed || result.Completed != 0 || result.AffectedRows != nil {
		t.Fatalf("rollback result: %+v %v", result, err)
	}
	if result.Attempted != 2 {
		t.Fatal("rollback lost attempted statement count")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitFailureIsUnknown(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE items").WillReturnResult(sqlmock.NewResult(0, 7))
	mock.ExpectCommit().WillReturnError(errors.New("connection reset"))
	mock.ExpectClose()
	result, err := Execute(context.Background(), db, "postgresql", []string{"UPDATE items SET active=true"}, Options{Transaction: true})
	if err == nil || result.Outcome != Unknown || result.Completed != 0 || result.AffectedRows != nil {
		t.Fatalf("commit uncertainty lost: %+v %v", result, err)
	}
	if result.Attempted != 1 {
		t.Fatal("commit uncertainty lost attempted count")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAutocommitFailurePreservesCompletedStatementsWithoutRetry(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec("UPDATE first").WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectExec("UPDATE second").WillReturnError(errors.New("connection reset"))
	mock.ExpectClose()
	result, err := Execute(context.Background(), db, "postgresql", []string{"UPDATE first SET n=1", "UPDATE second SET n=2"}, Options{})
	if err == nil || result.Outcome != Unknown || result.Completed != 1 || result.AffectedRows != nil {
		t.Fatalf("autocommit uncertainty lost: %+v %v", result, err)
	}
	if result.Attempted != 2 {
		t.Fatal("autocommit uncertainty lost attempted count")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitReportsRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE items").WillReturnResult(sqlmock.NewResult(0, 7))
	mock.ExpectCommit()
	mock.ExpectClose()
	result, err := Execute(context.Background(), db, "postgresql", []string{"UPDATE items SET active=true"}, Options{Transaction: true})
	if err != nil || result.Outcome != Succeeded || result.Completed != 1 || result.AffectedRows == nil || *result.AffectedRows != 7 {
		t.Fatalf("commit result: %+v %v", result, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
