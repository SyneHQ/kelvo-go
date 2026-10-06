package sqlsession

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMySQLRequiredTransactionChecksEveryRelation(t *testing.T) {
	for _, engine := range []string{"mysql", "mariadb"} {
		t.Run(engine, func(t *testing.T) {
			for _, statement := range []string{
				"UPDATE local_table a, other_db.nontransactional b SET b.n=b.n+1 WHERE a.id=b.id",
				"UPDATE local_table a, `other_db`.`nontransactional` b SET b.n=1 WHERE a.id=b.id",
				"DELETE b FROM local_table a, other_db.nontransactional b WHERE a.id=b.id",
				"INSERT INTO local_table SELECT * FROM other_db.nontransactional",
				"UPDATE local_table SET n=(SELECT MAX(n) FROM other_db.nontransactional)",
				"UPDATE local_table SET n=other_db.side_effect()",
				"MERGE INTO local_table USING another ON local_table.id=another.id WHEN MATCHED THEN DELETE",
			} {
				// Every slot is checked before the SQL pool is touched. An invalid
				// later statement must reject an otherwise valid first mutation.
				result, err := Execute(context.Background(), nil, engine, []string{"UPDATE local_table SET n=1", statement}, Options{Transaction: true})
				if !errors.Is(err, errMySQLTransaction) || result.Outcome != Failed || result.Attempted != 0 || result.Completed != 0 {
					t.Errorf("relation escaped preflight: %q: %+v %v", statement, result, err)
				}
			}
			for _, statement := range []string{
				"UPDATE local_table SET n=? WHERE id=?",
				"UPDATE local_table a, another b SET b.n=b.n+1 WHERE a.id=b.id",
				"UPDATE `local_table` a JOIN `another` b ON a.id=b.id SET a.n=b.n",
				"INSERT INTO local_table (id,n) VALUES (?,?)",
				"INSERT INTO local_table SELECT * FROM another",
				"DELETE FROM local_table WHERE id=?",
				"UPDATE local_table SET n=(SELECT MAX(n) FROM another)",
			} {
				if err := Validate(engine, []string{statement}, Options{Transaction: true}); err != nil {
					t.Errorf("unqualified DML rejected: %q: %v", statement, err)
				}
			}
		})
	}
}

func TestMySQLRollbackCannotConfirmImplicitEffectsAbsent(t *testing.T) {
	for _, engine := range []string{"mysql", "mariadb"} {
		t.Run(engine, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			mock.ExpectBegin()
			// A native trigger may have written a nontransactional table before
			// the later error. SQL rollback acknowledgement does not cover it.
			mock.ExpectExec("UPDATE items").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("INSERT INTO items").WillReturnError(errors.New("constraint violation"))
			mock.ExpectRollback()
			mock.ExpectClose()
			result, err := Execute(context.Background(), db, engine, []string{"UPDATE items SET n=1", "INSERT INTO items(id) VALUES(1)"}, Options{Transaction: true})
			if err == nil || result.Outcome != Unknown || result.Attempted != 2 || result.Completed != 0 || result.AffectedRows != nil {
				t.Fatalf("rollback invented an effect-free result: %+v %v", result, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
