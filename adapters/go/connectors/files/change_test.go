//go:build cgo

package files

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func fileChange(transaction bool, statements ...string) adapter.Change {
	return adapter.Change{Statements: statements, Parameters: make([][]operations.Parameter, len(statements)), Transaction: transaction}
}
func TestSQLiteChangesPrepareImageAndLeaveSourceUntouched(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		t.Run(map[bool]string{false: "autocommit", true: "transaction"}[transaction], func(t *testing.T) {
			s := sqliteFixture(t)
			var output bytes.Buffer
			result, candidate, err := s.PrepareFileChange(context.Background(), fileChange(transaction, "UPDATE trips SET fare='new' WHERE id=2", "INSERT INTO trips(id,fare) VALUES(4,'four')"), &output)
			if err != nil || result.Completed != 2 || result.Outcome != "succeeded" || candidate == nil {
				t.Fatal(result, candidate, err)
			}
			hash := sha256.Sum256(output.Bytes())
			if candidate.SHA256 != hex.EncodeToString(hash[:]) || candidate.Bytes != int64(output.Len()) {
				t.Fatal(candidate)
			}
			path := t.TempDir() + "/candidate.sqlite"
			if err = os.WriteFile(path, output.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			next := openFixture(t, path, "sqlite")
			got := &capture{}
			if _, err = next.Query(context.Background(), testQuery("SELECT fare FROM trips WHERE id=2"), got); err != nil || got.rows[0][0] != "new" {
				t.Fatal(got.rows, err)
			}
			got = &capture{}
			if _, err = s.Query(context.Background(), testQuery("SELECT fare FROM trips WHERE id=2"), got); err != nil || got.rows[0][0] != nil {
				t.Fatal("original mutated", got.rows, err)
			}
		})
	}
}
func TestSQLiteRollbackAndPartialImageAreDistinct(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		t.Run(map[bool]string{false: "partial", true: "rollback"}[transaction], func(t *testing.T) {
			s := sqliteFixture(t)
			var out bytes.Buffer
			result, candidate, err := s.PrepareFileChange(context.Background(), fileChange(transaction, "UPDATE trips SET fare='new' WHERE id=2", "INSERT INTO absent VALUES(1)"), &out)
			if err == nil {
				t.Fatal("invalid statement succeeded")
			}
			if transaction {
				if candidate != nil || out.Len() != 0 || result.Completed != 0 {
					t.Fatal(result, candidate, out.Len())
				}
			} else if candidate == nil || out.Len() == 0 || result.Completed != 1 {
				t.Fatal(result, candidate, out.Len())
			}
		})
	}
}
func TestFileMutationsDenyRuntimeAndFilesystemCommands(t *testing.T) {
	s := sqliteFixture(t)
	for _, statement := range []string{"ATTACH '/tmp/other' AS other", "CREATE TABLE leak AS SELECT load_extension('/tmp/x.so')", "PRAGMA writable_schema=ON", "VACUUM INTO '/tmp/copy'", "UPDATE trips SET id=4; DELETE FROM trips", "CREATE VIRTUAL TABLE lookup USING fts5(value)"} {
		t.Run(statement, func(t *testing.T) {
			if _, candidate, err := s.PrepareFileChange(context.Background(), fileChange(false, statement), io.Discard); err == nil || candidate != nil {
				t.Fatal(candidate, err)
			}
		})
	}
}
