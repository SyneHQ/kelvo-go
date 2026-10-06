package clickhouse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	native "github.com/SYNEHQ/kelvo-go/internal/sources/clickhouse"
)

func TestMigrationHTTPPinsServerAndRejectsFailover(t *testing.T) {
	const uuid = "11111111-2222-4333-8444-555555555555"
	var session string
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		user, password, ok := r.BasicAuth()
		if !ok || user != "fixture" || password != "test-only" {
			t.Error("private source authentication missing")
		}
		if calls == 1 {
			session = q.Get("session_id")
			if !strings.HasPrefix(session, "kelvo-migration-") || len(session) < 70 || q.Get("session_check") != "" {
				t.Error("invalid initial session")
			}
			fmt.Fprintf(w, `{"data":[[%q,"Atomic"]],"rows":1}`, uuid)
			return
		}
		if q.Get("session_id") != session || q.Get("session_check") != "1" {
			t.Error("request allowed implicit session recreation")
		}
		http.Error(w, "session not found on failover server", http.StatusBadRequest)
	}))
	defer server.Close()
	s := &Session{source: native.ResolvedSource{URL: server.URL, Username: "fixture", Password: "test-only"}, database: "app", client: server.Client(), migrationUUID: uuid}
	runner, err := s.migrationRunner(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Status(context.Background()); err == nil {
		t.Fatal("failed-over session accepted")
	}
	if calls != 2 {
		t.Fatal("source operation retried", calls)
	}
}
func TestMigrationRejectsWrongServerOrDatabaseEngine(t *testing.T) {
	for _, body := range []string{`{"data":[["99999999-2222-4333-8444-555555555555","Atomic"]],"rows":1}`, `{"data":[["11111111-2222-4333-8444-555555555555","Replicated"]],"rows":1}`} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
		s := &Session{source: native.ResolvedSource{URL: server.URL}, database: "app", client: server.Client(), migrationUUID: "11111111-2222-4333-8444-555555555555"}
		if _, err := s.migrationRunner(context.Background()); err == nil {
			t.Fatal("unbound topology accepted")
		}
		server.Close()
	}
}
