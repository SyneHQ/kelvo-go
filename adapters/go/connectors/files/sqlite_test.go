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
	allColumns := &capture{}
	stats, err = s.Inspect(context.Background(), operations.MetadataSpec{Object: "columns", Limit: 100}, adapter.Limits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 2}, allColumns)
	if err != nil || stats.Rows != 5 || len(allColumns.rows) != 5 || allColumns.rows[0][1] != "dates" || allColumns.rows[1][1] != "mixed" || allColumns.rows[2][1] != "trips" {
		t.Fatal("whole-schema column metadata changed", allColumns.rows, err)
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

func TestSQLitePrimaryAndForeignKeyMetadataPreservesCompositeOrder(t *testing.T) {
	path := t.TempDir() + "/keys.sqlite"
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE TABLE parents(region TEXT, number INTEGER, PRIMARY KEY(region,number))`,
		`CREATE TABLE children(id INTEGER PRIMARY KEY, region TEXT, number INTEGER, FOREIGN KEY(region,number) REFERENCES parents)`,
		`CREATE TABLE explicit_child(id INTEGER PRIMARY KEY, parent_number INTEGER, parent_region TEXT, FOREIGN KEY(parent_region,parent_number) REFERENCES parents(region,number))`,
	} {
		if _, err = db.Exec(statement); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	s := openFixture(t, path, "sqlite")
	limits := adapter.Limits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 2}
	primary := &capture{}
	_, err = s.Inspect(context.Background(), operations.MetadataSpec{Object: "primary_keys", Target: operations.ObjectRef{Name: "parents"}, Limit: 100}, limits, primary)
	if err != nil || len(primary.rows) != 2 || primary.rows[0][3] != "region" || primary.rows[1][3] != "number" || primary.rows[0][4] != int64(1) || primary.rows[1][4] != int64(2) {
		t.Fatal("composite primary key changed", primary.rows, err)
	}
	for _, table := range []string{"children", "explicit_child"} {
		foreign := &capture{}
		_, err = s.Inspect(context.Background(), operations.MetadataSpec{Object: "foreign_keys", Target: operations.ObjectRef{Name: table}, Limit: 100}, limits, foreign)
		if err != nil || len(foreign.rows) != 2 || foreign.rows[0][4] != int64(1) || foreign.rows[1][4] != int64(2) || foreign.rows[0][6] != "parents" || foreign.rows[0][7] != "region" || foreign.rows[1][7] != "number" {
			t.Fatal("composite foreign key changed", table, foreign.rows, err)
		}
	}
	page := &capture{}
	_, err = s.Inspect(context.Background(), operations.MetadataSpec{Object: "relationships", Target: operations.ObjectRef{Name: "children"}, Limit: 1, Cursor: "1"}, limits, page)
	if err != nil || len(page.rows) != 1 || page.rows[0][4] != int64(2) {
		t.Fatal("relationship page changed", page.rows, err)
	}
	for _, statement := range []string{`SELECT * FROM pragma_foreign_key_list('children')`, `SELECT * FROM pragma_table_xinfo('parents')`} {
		if _, err := s.Query(context.Background(), testQuery(statement), &capture{}); err == nil {
			t.Fatal("metadata-only PRAGMA accepted in query", statement)
		}
	}
}
