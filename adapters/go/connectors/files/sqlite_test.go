//go:build cgo

package files

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func sqliteFixture(t *testing.T) *Session {
	t.Helper()
	path := t.TempDir() + "/source.sqlite"
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"CREATE TABLE trips(id INTEGER, fare TEXT, payload BLOB)", "INSERT INTO trips VALUES(9007199254740993,'10.000000000000000001',X'00ff'),(2,NULL,X''),(3,'0',NULL)", "CREATE TABLE mixed(value)", "INSERT INTO mixed VALUES(9007199254740993),('text')", "CREATE TABLE dates(value DATETIME)", "INSERT INTO dates VALUES('not-a-date')"} {
		if _, err = db.Exec(q); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	return openFixture(t, path, "sqlite")
}
func TestSQLiteCTEParametersAndExactValues(t *testing.T) {
	s := sqliteFixture(t)
	sink := &capture{}
	q := testQuery("WITH selected AS (SELECT * FROM trips WHERE id>?) SELECT id,fare,payload FROM selected ORDER BY id DESC")
	q.Parameters = []operations.Parameter{jsonParameter("int64", 2)}
	stats, err := s.Query(context.Background(), q, sink)
	if err != nil || stats.Rows != 2 || len(sink.rows) != 2 {
		t.Fatal(stats, sink.rows, err)
	}
	if sink.rows[0][0] != int64(9007199254740993) || sink.rows[0][1] != "10.000000000000000001" {
		t.Fatal(sink.rows)
	}
	if sink.rows[1][2] != nil {
		t.Fatal("NULL lost", sink.rows)
	}
}
func TestSQLiteRejectsWritesEscapesAndAmbiguousTypes(t *testing.T) {
	s := sqliteFixture(t)
	for _, q := range []string{"UPDATE trips SET id=4", "SELECT 1; DELETE FROM trips", "ATTACH '/tmp/other.db' AS other", "PRAGMA query_only=OFF", "SELECT load_extension('/tmp/driver.so')", "SELECT * FROM pragma_database_list()", "SELECT * FROM mixed", "SELECT * FROM dates"} {
		t.Run(q, func(t *testing.T) {
			if _, err := s.Query(context.Background(), testQuery(q), &capture{}); err == nil {
				t.Fatal("unsafe or lossy result accepted")
			}
		})
	}
	sink := &capture{}
	if _, err := s.Query(context.Background(), testQuery("SELECT CAST(value AS TEXT) AS value FROM dates"), sink); err != nil || sink.rows[0][0] != "not-a-date" {
		t.Fatal(sink.rows, err)
	}
}
func TestSQLiteMetadataLimitsAndCancellation(t *testing.T) {
	s := sqliteFixture(t)
	sink := &capture{}
	stats, err := s.Inspect(context.Background(), operations.MetadataSpec{Object: "columns", Target: operations.ObjectRef{Name: "trips"}, Limit: 100}, adapter.Limits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 2}, sink)
	if err != nil || stats.Rows != 3 {
		t.Fatal(stats, err)
	}
	q := testQuery("SELECT id FROM trips")
	q.MaxRows = 1
	if _, err = s.Query(context.Background(), q, &capture{}); !errors.Is(err, adapter.ErrLimit) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err = s.Query(ctx, testQuery("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n"), &capture{}); err == nil {
		t.Fatal("unbounded recursive query survived deadline")
	}
	stop := errors.New("sink stopped")
	if _, err = s.Query(context.Background(), testQuery("SELECT id FROM trips"), &capture{fail: stop}); !errors.Is(err, stop) {
		t.Fatal(err)
	}
}
