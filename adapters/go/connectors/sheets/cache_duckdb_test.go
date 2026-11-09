//go:build duckdb_arrow

package sheets

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

type fakeGoogle struct {
	version    atomic.Value
	driveFails atomic.Bool
	values     atomic.Int32
	drive      atomic.Int32
}

func cachedSessions(t *testing.T, n int) (*fakeGoogle, []*Session, string) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("KELVO_SHEETS_CACHE_DIR", dir)
	t.Setenv("KELVO_SHEETS_CACHE_MAX_MB", "64")
	g := &fakeGoogle{}
	g.version.Store("7")
	sheets := fixtureHandler(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/drive/v3/files/") {
			g.drive.Add(1)
			if r.URL.Path != "/drive/v3/files/saved-sheet" || r.URL.Query().Get("fields") != "version,modifiedTime" || r.Header.Get("Authorization") != "Bearer drive-token" || g.driveFails.Load() {
				w.WriteHeader(403)
				return
			}
			w.Write([]byte(`{"version":"` + g.version.Load().(string) + `","modifiedTime":"2026-11-09T00:00:00Z"}`))
			return
		}
		if strings.Contains(r.URL.Path, "/values/") {
			g.values.Add(1)
		}
		sheets(w, r)
	}))
	t.Cleanup(server.Close)
	sessions := make([]*Session, n)
	for i := range sessions {
		s, err := Open(context.Background(), "saved-sheet", `{"type":"service_account"}`, testLimits())
		if err != nil {
			t.Fatal(err)
		}
		s.origin, s.driveOrigin, s.http = server.URL, server.URL, server.Client()
		s.mint = func(_ context.Context, _, scope string) (string, error) {
			if scope == driveScope {
				return "drive-token", nil
			}
			return "explicit-test-token", nil
		}
		t.Cleanup(func() { s.Close() })
		sessions[i] = s
	}
	return g, sessions, dir
}

func queryTrips(t *testing.T, s *Session) {
	t.Helper()
	sink := &capture{}
	stats, err := s.Query(context.Background(), adapter.Query{Statement: `SELECT id FROM google_sheets.main."Daily trips" ORDER BY id`, MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 10}, sink)
	if err != nil || stats.Rows != 2 || sink.rows[0][0] != "2" {
		t.Fatal(stats, sink.rows, err)
	}
}

func entries(t *testing.T, dir string) []string {
	names, _ := os.ReadDir(dir)
	var out []string
	for _, n := range names {
		if n.IsDir() {
			out = append(out, n.Name())
		}
	}
	return out
}

func TestCacheReusesSnapshotForSameVersion(t *testing.T) {
	g, s, dir := cachedSessions(t, 2)
	queryTrips(t, s[0])
	queryTrips(t, s[1])
	if g.values.Load() != 1 || g.drive.Load() != 2 || len(entries(t, dir)) != 1 {
		t.Fatal(g.values.Load(), g.drive.Load(), entries(t, dir))
	}
}

func TestCacheRebuildsOnNewVersionAndDropsOld(t *testing.T) {
	g, s, dir := cachedSessions(t, 1)
	queryTrips(t, s[0])
	old := entries(t, dir)
	g.version.Store("8")
	queryTrips(t, s[0])
	now := entries(t, dir)
	if g.values.Load() != 2 || len(now) != 1 || now[0] == old[0] {
		t.Fatal(g.values.Load(), old, now)
	}
}

func TestCacheDriveFailureBuildsLive(t *testing.T) {
	g, s, dir := cachedSessions(t, 1)
	queryTrips(t, s[0])
	g.driveFails.Store(true)
	queryTrips(t, s[0])
	if g.values.Load() != 2 || len(entries(t, dir)) != 1 {
		t.Fatal(g.values.Load(), entries(t, dir))
	}
}

func TestCacheSkippedForRawToken(t *testing.T) {
	g, s, dir := cachedSessions(t, 1)
	s[0].credential = "explicit-test-token"
	queryTrips(t, s[0])
	queryTrips(t, s[0])
	if g.values.Load() != 2 || g.drive.Load() != 0 || len(entries(t, dir)) != 0 {
		t.Fatal(g.values.Load(), g.drive.Load(), entries(t, dir))
	}
}

func TestCacheTestRebuildsSameVersion(t *testing.T) {
	g, s, dir := cachedSessions(t, 1)
	queryTrips(t, s[0])
	if err := s[0].Test(context.Background()); err != nil {
		t.Fatal(err)
	}
	queryTrips(t, s[0])
	if g.values.Load() != 2 || len(entries(t, dir)) != 1 {
		t.Fatal(g.values.Load(), entries(t, dir))
	}
}

func TestCacheConcurrentColdQueriesBuildOnce(t *testing.T) {
	g, s, _ := cachedSessions(t, 4)
	var wg sync.WaitGroup
	for _, session := range s {
		wg.Add(1)
		go func() { defer wg.Done(); queryTrips(t, session) }()
	}
	wg.Wait()
	if g.values.Load() != 1 {
		t.Fatal(g.values.Load())
	}
}

func TestCacheUnsetEnvBuildsLiveEveryTime(t *testing.T) {
	g, s, dir := cachedSessions(t, 1)
	t.Setenv("KELVO_SHEETS_CACHE_DIR", "")
	queryTrips(t, s[0])
	queryTrips(t, s[0])
	if g.values.Load() != 2 || g.drive.Load() != 0 || len(entries(t, dir)) != 0 {
		t.Fatal(g.values.Load(), g.drive.Load(), entries(t, dir))
	}
}

func TestCacheIOFailureFallsBackLive(t *testing.T) {
	g, s, dir := cachedSessions(t, 1)
	os.Mkdir(filepath.Join(dir, s[0].cacheKey("7")+".lock"), 0o700) // the lock file cannot be opened
	queryTrips(t, s[0])
	if g.values.Load() != 1 || len(entries(t, dir)) != 1 {
		t.Fatal(g.values.Load(), entries(t, dir))
	}
}

func TestCacheCorruptEntryIsRebuilt(t *testing.T) {
	g, s, dir := cachedSessions(t, 1)
	queryTrips(t, s[0])
	path := filepath.Join(dir, s[0].cacheKey("7"), "snapshot.duckdb")
	raw, _ := os.ReadFile(path)
	raw[len(raw)/2] ^= 0xff
	os.WriteFile(path, raw, 0o600)
	queryTrips(t, s[0])
	queryTrips(t, s[0])
	if g.values.Load() != 2 {
		t.Fatal(g.values.Load())
	}
}
