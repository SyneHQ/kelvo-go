package relational

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func TestProbePgssLag(t *testing.T) {
	creds := os.Getenv("KELVO_RT_POSTGRESQL")
	if creds == "" {
		t.Skip()
	}
	_, rest, _ := strings.Cut(creds, "@")
	target, _, _ := strings.Cut(rest, "/")
	proxy := newDelayProxy(t, target, 0)
	s := liveSession(t, "postgresql", proxy.addr(), creds)
	admin, _ := sql.Open("pgx", "postgres://"+creds+"?sslmode=disable")
	defer admin.Close()
	admin.Exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
	admin.Exec("SELECT pg_stat_statements_reset()")
	miss := 0
	for i := 1; i <= 200; i++ {
		liveRead(s, context.Background(), "SELECT 42 AS probe_marker")
		var n int64
		admin.QueryRow("SELECT coalesce(sum(calls),0) FROM pg_stat_statements WHERE query LIKE 'SELECT 42 AS probe_marker%'").Scan(&n)
		if n != int64(i) {
			miss++
		}
	}
	t.Logf("pgss behind right after read returned (rollback awaited by liveRead): %d/200", miss)
	miss = 0
	admin.Exec("SELECT pg_stat_statements_reset()")
	for i := 1; i <= 200; i++ {
		sink := &firstRowSink{}
		s.Query(context.Background(), adapterQuery("SELECT 43 AS probe_marker"), sink)
		var n int64
		admin.QueryRow("SELECT coalesce(sum(calls),0) FROM pg_stat_statements WHERE query LIKE 'SELECT 43 AS probe_marker%'").Scan(&n)
		if n != int64(i) {
			miss++
		}
		waitRollbacks()
	}
	t.Logf("pgss behind right after Query returned, rollback in flight: %d/200", miss)
}

func adapterQuery(stmt string) adapter.Query {
	return adapter.Query{Statement: stmt, MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 10}
}
