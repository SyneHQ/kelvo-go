package relational

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
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/golang-migrate/migrate/v4/database"
)

func TestPersistentLiveMigrations(t *testing.T) {
	fd := os.Getenv("KELVO_MIGRATION_LIVE_FD")
	if fd == "" {
		t.Skip("isolated warehouse migration fixture not configured")
	}
	n, err := strconv.Atoi(fd)
	if err != nil || n < 3 {
		t.Fatal("invalid fixture pipe")
	}
	input := os.NewFile(uintptr(n), "warehouse-fixture")
	defer input.Close()
	raw, err := io.ReadAll(io.LimitReader(input, 64<<10))
	if err != nil {
		t.Fatal("fixture unavailable")
	}
	defer clear(raw)
	var f struct {
		Engine, Host, Username, Password, Database, Schema, CA, ServerName string
		Port                                                               int
	}
	if json.Unmarshal(raw, &f) != nil || f.Database != "kelvo_migration_fixture" || (f.Engine != "cockroachdb" && f.Engine != "redshift") {
		t.Fatal("invalid fixture scope")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(f.CA)) {
		t.Fatal("invalid fixture CA")
	}
	c := adapter.Connection{TenantID: "fixture", ConnectionID: "warehouse", Revision: "one", Engine: f.Engine, Host: f.Host, Port: f.Port, Namespace: f.Database, Schema: f.Schema, Username: f.Username, Password: f.Password, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: f.ServerName}}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	base, err := (PGFamily{Engine: f.Engine}).Open(ctx, c)
	if err != nil {
		t.Fatalf("source unavailable (%T)", err)
	}
	s := base.(*familySession)
	defer s.Close()
	state := func(want migration.State) {
		t.Helper()
		got, err := s.MigrationStatus(ctx)
		if err != nil || got != want {
			t.Fatalf("status got=%+v want=%+v (%T)", got, want, err)
		}
	}
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := s.Pool.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("fixture command failed (%T)", err)
		}
	}
	scalar := func(q string) int64 {
		t.Helper()
		var count int64
		if err := s.Pool.QueryRowContext(ctx, q).Scan(&count); err != nil {
			t.Fatalf("fixture count failed (%T)", err)
		}
		return count
	}
	apply := func(p migration.Plan, want int64) {
		t.Helper()
		result, err := s.ApplyMigration(ctx, p)
		if err != nil || result.Effect != operations.EffectCommitted || result.To.Version != want {
			message := ""
			if err != nil {
				message = err.Error()
			}
			for _, secret := range []string{c.Password, c.Username, c.Host} {
				message = strings.ReplaceAll(message, secret, "[redacted]")
			}
			t.Fatalf("apply got=%+v (%T):%.512s", result, err, message)
		}
		state(migration.State{Version: want})
	}
	files := []migration.File{{Name: "1_create.up.sql", Content: "CREATE TABLE migration_probe(id bigint NOT NULL PRIMARY KEY, marker bigint NOT NULL)"}, {Name: "1_create.down.sql", Content: "DROP TABLE migration_probe"}, {Name: "2_seed.up.sql", Content: "INSERT INTO migration_probe VALUES(1,10)"}, {Name: "2_seed.down.sql", Content: "DELETE FROM migration_probe"}, {Name: "3_failed.up.sql", Content: "INSERT INTO nonexistent_fixture_table VALUES(1)"}, {Name: "3_failed.down.sql", Content: "SELECT 1"}}
	state(migration.State{Version: -1})
	apply(migration.Plan{Version: 1, Expected: migration.State{Version: -1}, Direction: "up", Steps: 2, Files: files}, 2)
	if scalar(`SELECT count(*) FROM "public"."kelvo_migration_lock"`) != 0 || scalar("SELECT marker FROM migration_probe WHERE id=1") != 10 {
		t.Fatal("successful migration left lock or wrong data")
	}
	stale := migration.Plan{Version: 1, Expected: migration.State{Version: 1}, Direction: "up", Steps: 1, Files: files}
	if _, err := s.ApplyMigration(ctx, stale); !errors.Is(err, migration.ErrConflict) {
		t.Fatal("stale version accepted")
	}
	owner := strings.Repeat("b", 64)
	exec(`INSERT INTO "public"."kelvo_migration_lock" (lock_id,owner) VALUES(1,$1)`, owner)
	plan := migration.Plan{Version: 1, Expected: migration.State{Version: 2}, Direction: "up", Steps: 1, Files: files}
	if _, err := s.ApplyMigration(ctx, plan); !errors.Is(err, database.ErrLocked) {
		t.Fatalf("cross-worker lock bypassed (%T)", err)
	}
	exec(`DELETE FROM "public"."kelvo_migration_lock" WHERE lock_id=1 AND owner=$1`, owner)
	result, err := s.ApplyMigration(ctx, plan)
	if !errors.Is(err, migration.ErrOutcomeUnknown) || result.Effect != operations.EffectUnknown {
		t.Fatalf("failed script settled (%T)", err)
	}
	state(migration.State{Version: 3, Dirty: true})
	if scalar(`SELECT count(*) FROM "public"."kelvo_migration_lock"`) != 1 {
		t.Fatal("uncertain execution dropped durable lock")
	}
	force := int64(2)
	recovery := migration.Plan{Version: 1, Expected: migration.State{Version: 3, Dirty: true}, Direction: "force", ForceVersion: &force, Files: []migration.File{}}
	if _, err := s.ApplyMigration(ctx, recovery); !errors.Is(err, database.ErrLocked) {
		t.Fatal("force silently stole durable lock")
	}
	// Fixture-only reconciliation: the failed statement addressed an absent
	// table and started no background job. Production recovery is operator-owned.
	exec(`DELETE FROM "public"."kelvo_migration_lock"`)
	apply(recovery, 2)
	apply(migration.Plan{Version: 1, Expected: migration.State{Version: 2}, Direction: "down", Steps: 2, Files: files}, -1)
	exec(`DROP TABLE "public"."schema_migrations"`)
	exec(`CREATE VIEW "public"."schema_migrations" AS SELECT CAST(1 AS bigint) AS version,false AS dirty`)
	if _, err := s.MigrationStatus(ctx); !errors.Is(err, migration.ErrInvalid) {
		t.Fatal("history view accepted")
	}
	t.Log("verified TLS, source-wide durable lock, stale version rejection, up/down/force, uncertain lock retention, manual recovery and history-view denial")
}
