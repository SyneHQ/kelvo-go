//go:build duckdb_arrow

package files

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestCSVAndJSONSnapshotsPreserveExactText(t *testing.T) {
	for _, fixture := range []struct {
		format, body, statement string
		want                    any
	}{
		{"csv", "id,amount\n9007199254740993,10.000000000000000001\n", "SELECT id FROM data", "9007199254740993"},
		{"jsonl", "{\"id\":9007199254740993,\"amount\":10.000000000000000001}\n", "SELECT document FROM data", "{\"id\":9007199254740993,\"amount\":10.000000000000000001}"},
	} {
		t.Run(fixture.format, func(t *testing.T) {
			path := t.TempDir() + "/data." + fixture.format
			if err := os.WriteFile(path, []byte(fixture.body), 0600); err != nil {
				t.Fatal(err)
			}
			s := openFixture(t, path, fixture.format)
			sink := &capture{}
			stats, err := s.Query(context.Background(), testQuery(fixture.statement), sink)
			if err != nil || stats.Rows != 1 || sink.rows[0][0] != fixture.want {
				t.Fatal(stats, sink.rows, err)
			}
			for _, q := range []string{"SELECT * FROM read_csv_auto('/etc/passwd')", "SET enable_external_access=true", "COPY data TO '/tmp/leak.csv'"} {
				if _, err = s.Query(context.Background(), testQuery(q), &capture{}); err == nil {
					t.Fatal("external access accepted", q)
				}
			}
		})
	}
}
func TestDuckDBFileQueriesDefaultCatalogAndMetadata(t *testing.T) {
	path := t.TempDir() + "/source.duckdb"
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"CREATE TABLE trips(id BIGINT,amount DECIMAL(30,18))", "INSERT INTO trips VALUES(9007199254740993,10.000000000000000001)"} {
		if _, err = db.Exec(q); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	s := openFixture(t, path, "duckdb")
	sink := &capture{}
	stats, err := s.Query(context.Background(), testQuery("WITH chosen AS (SELECT * FROM trips) SELECT id FROM chosen"), sink)
	if err != nil || stats.Rows != 1 || sink.rows[0][0] != int64(9007199254740993) {
		t.Fatal(stats, sink.rows, err)
	}
	sink = &capture{}
	stats, err = s.Inspect(context.Background(), operations.MetadataSpec{Object: "tables", Limit: 100}, adapter.Limits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 2}, sink)
	if err != nil || stats.Rows != 1 {
		t.Fatal(stats, sink.rows, err)
	}
	if _, err = s.Query(context.Background(), testQuery("UPDATE trips SET id=1"), &capture{}); err == nil {
		t.Fatal("snapshot mutation accepted")
	}
}

func TestDuckDBFileChangesProduceSeparateImage(t *testing.T) {
	path := t.TempDir() + "/source.duckdb"
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("CREATE TABLE trips(id BIGINT); INSERT INTO trips VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	source := openFixture(t, path, "duckdb")
	var out bytes.Buffer
	result, candidate, err := source.PrepareFileChange(context.Background(), fileChange(true, "UPDATE trips SET id=9007199254740993"), &out)
	if err != nil || candidate == nil || result.Completed != 1 {
		t.Fatal(result, candidate, err)
	}
	nextPath := t.TempDir() + "/next.duckdb"
	if err = os.WriteFile(nextPath, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	next := openFixture(t, nextPath, "duckdb")
	sink := &capture{}
	if _, err = next.Query(context.Background(), testQuery("SELECT id FROM trips"), sink); err != nil || sink.rows[0][0] != int64(9007199254740993) {
		t.Fatal(sink.rows, err)
	}
	sink = &capture{}
	if _, err = source.Query(context.Background(), testQuery("SELECT id FROM trips"), sink); err != nil || sink.rows[0][0] != int64(1) {
		t.Fatal("original changed", sink.rows, err)
	}
}
