package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/golang-migrate/migrate/v4/database"
)

func TestClickHouseLiveMigrations(t *testing.T) {
	fd := os.Getenv("KELVO_CLICKHOUSE_MIGRATION_FD")
	if fd == "" {
		t.Skip("isolated ClickHouse TLS migration fixture not configured")
	}
	n, err := strconv.Atoi(fd)
	if err != nil || n < 3 {
		t.Fatal("invalid fixture pipe")
	}
	input := os.NewFile(uintptr(n), "clickhouse-fixture")
	defer input.Close()
	raw, err := io.ReadAll(io.LimitReader(input, 64<<10))
	if err != nil {
		t.Fatal("fixture unavailable")
	}
	defer clear(raw)
	var f struct {
		Host, Username, Password, Database, CA, ServerName, ServerUUID string
		Port                                                           int
	}
	if json.Unmarshal(raw, &f) != nil || f.Database != "kelvo_migration_fixture" {
		t.Fatal("invalid fixture scope")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(f.CA)) {
		t.Fatal("invalid CA")
	}
	c := adapter.Connection{TenantID: "fixture", ConnectionID: "clickhouse", Revision: "one", Engine: "clickhouse", Host: f.Host, Port: f.Port, Namespace: f.Database, Username: f.Username, Password: f.Password, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: f.ServerName}, Options: map[string]string{"migration_server_uuid": f.ServerUUID}}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	base, err := (Driver{}).Open(ctx, c)
	if err != nil {
		t.Fatalf("source unavailable (%T)", err)
	}
	s := base.(*Session)
	defer s.Close()
	state := func(want migration.State) {
		t.Helper()
		got, err := s.MigrationStatus(ctx)
		if err != nil || got != want {
			t.Fatalf("state=%+v want=%+v (%T)", got, want, err)
		}
	}
	exec := func(sql string) {
		t.Helper()
		runner, err := s.migrationRunner(ctx)
		if err == nil {
			err = runner.Exec(ctx, sql)
		}
		if err != nil {
			t.Fatalf("fixture command failed (%T)", err)
		}
	}
	scalar := func(sql string) uint64 {
		t.Helper()
		runner, err := s.migrationRunner(ctx)
		if err != nil {
			t.Fatal("fixture session unavailable")
		}
		rows, err := runner.Query(ctx, sql)
		if err != nil || len(rows) != 1 || len(rows[0]) != 1 {
			t.Fatal("fixture count unavailable")
		}
		text := strings.Trim(string(rows[0][0]), `"`)
		value, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			t.Fatal("fixture count invalid")
		}
		return value
	}
	apply := func(plan migration.Plan, want int64) {
		t.Helper()
		result, err := s.ApplyMigration(ctx, plan)
		if err != nil || result.Effect != operations.EffectCommitted || result.To.Version != want {
			t.Fatalf("migration=%+v (%T)", result, err)
		}
		state(migration.State{Version: want})
	}
	files := []migration.File{{Name: "1_create.up.sql", Content: "CREATE TABLE migration_probe(id UInt64,marker UInt64) ENGINE=Memory"}, {Name: "1_create.down.sql", Content: "DROP TABLE migration_probe SYNC"}, {Name: "2_seed.up.sql", Content: "INSERT INTO migration_probe VALUES(1,10)"}, {Name: "2_seed.down.sql", Content: "TRUNCATE TABLE migration_probe"}, {Name: "3_next.up.sql", Content: "INSERT INTO migration_probe VALUES(2,20)"}, {Name: "3_next.down.sql", Content: "TRUNCATE TABLE migration_probe"}, {Name: "4_fail.up.sql", Content: "INSERT INTO nonexistent_fixture_table VALUES(1)"}, {Name: "4_fail.down.sql", Content: "SELECT 1"}}
	state(migration.State{Version: -1})
	apply(migration.Plan{Version: 1, Expected: migration.State{Version: -1}, Direction: "up", Steps: 2, Files: files}, 2)
	if scalar("SELECT count(*) FROM migration_probe") != 1 {
		t.Fatal("incorrect initial data")
	}
	stale := migration.Plan{Version: 1, Expected: migration.State{Version: 1}, Direction: "up", Steps: 1, Files: files}
	if _, err := s.ApplyMigration(ctx, stale); !errors.Is(err, migration.ErrConflict) {
		t.Fatal("stale request accepted")
	}
	concurrent := migration.Plan{Version: 1, Expected: migration.State{Version: 2}, Direction: "up", Steps: 1, Files: files}
	var wg sync.WaitGroup
	outcomes := make(chan bool, 2)
	for range 2 {
		wg.Go(func() { r, e := s.ApplyMigration(ctx, concurrent); outcomes <- e == nil && r.To.Version == 3 })
	}
	wg.Wait()
	close(outcomes)
	success := 0
	for ok := range outcomes {
		if ok {
			success++
		}
	}
	if success != 1 || scalar("SELECT count(*) FROM migration_probe") != 2 {
		t.Fatal("concurrent migrations duplicated a script", success)
	}
	state(migration.State{Version: 3})
	failed := migration.Plan{Version: 1, Expected: migration.State{Version: 3}, Direction: "up", Steps: 1, Files: files}
	result, err := s.ApplyMigration(ctx, failed)
	if !errors.Is(err, migration.ErrOutcomeUnknown) || result.Effect != operations.EffectUnknown {
		t.Fatal("failed mutation was settled")
	}
	state(migration.State{Version: 4, Dirty: true})
	version := int64(3)
	force := migration.Plan{Version: 1, Expected: migration.State{Version: 4, Dirty: true}, Direction: "force", ForceVersion: &version, Files: []migration.File{}}
	if _, err := s.ApplyMigration(ctx, force); !errors.Is(err, database.ErrLocked) {
		t.Fatal("force stole uncertain lock")
	}
	// Fixture-only recovery: the server rejected a nonexistent table before a
	// background mutation could start. Operators must reconcile real failures.
	exec("DROP TABLE kelvo_migration_lock SYNC")
	apply(force, 3)
	apply(migration.Plan{Version: 1, Expected: migration.State{Version: 3}, Direction: "down", Steps: 3, Files: files}, -1)
	wrong := *s
	wrong.migrationUUID = "99999999-2222-4333-8444-555555555555"
	if _, err := wrong.MigrationStatus(ctx); err == nil {
		t.Fatal("wrong server pin accepted")
	}
	exec("DROP TABLE schema_migrations SYNC")
	exec("CREATE VIEW schema_migrations AS SELECT toInt64(1) AS version,toUInt8(0) AS dirty,toUInt64(1) AS sequence")
	if _, err := s.MigrationStatus(ctx); !errors.Is(err, migration.ErrInvalid) {
		t.Fatal("history view accepted")
	}
	t.Log("verified HTTPS, pinned server UUID/session, Atomic history, concurrent source lock, stale versions, up/down/force, uncertain lock retention and history-view denial")
}
