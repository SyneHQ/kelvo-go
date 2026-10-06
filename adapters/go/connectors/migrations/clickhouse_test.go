package migrations

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/migration"
)

func TestClickHouseScriptsKeepQuotedSemicolonsAndRejectDistributedDDL(t *testing.T) {
	sql := "CREATE TABLE events(id UInt64,note String) ENGINE=Memory; INSERT INTO events VALUES(1,'a; b'); -- comment;\n SELECT 1"
	parts, err := clickhouseStatements(sql)
	if err != nil || len(parts) != 3 || !strings.Contains(parts[1], "'a; b'") {
		t.Fatal(parts, err)
	}
	for _, bad := range []string{"CREATE TABLE t ON CLUSTER x (id UInt64) ENGINE=Memory", "CREATE TABLE t(id UInt64) ENGINE=Distributed(a,b,c)", "CREATE TABLE t(id UInt64) ENGINE=ReplicatedMergeTree() ORDER BY id", "SET session_id='other'", "DROP TABLE `kelvo_migration_lock`", "INSERT INTO \"schema_migrations\" VALUES(1)", "SELECT * FROM remote('other','db','t')", "SELECT 1 SETTINGS max_execution_time=0", "SELECT 'unfinished", "SELECT 1 /* unclosed", "CREATE TABLE t(id UInt64) ENGINE=\"Distributed\"(a,b,c)"} {
		if _, err := clickhouseStatements(bad); err == nil {
			t.Fatal("unsafe script accepted", bad)
		}
	}
}
func TestClickHouseUncertainRequestKeepsDurableLock(t *testing.T) {
	execs := 0
	d := &clickhouseDriver{ctx: context.Background(), owner: "kelvo-owner:abc", locked: true, source: ClickHouse{Database: "app", Query: func(context.Context, string) ([][]json.RawMessage, error) {
		return [][]json.RawMessage{{json.RawMessage(`"Memory"`), json.RawMessage(`"kelvo-owner:abc"`)}}, nil
	}, Exec: func(context.Context, string) error { execs++; return context.DeadlineExceeded }}}
	if err := d.Run(strings.NewReader("ALTER TABLE events ADD COLUMN note String")); err == nil || !d.uncertain {
		t.Fatal(err)
	}
	if err := d.Unlock(); !errors.Is(err, migration.ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if execs != 1 {
		t.Fatal("unknown mutation retried or lock removed", execs)
	}
}
