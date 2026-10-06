// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package relational

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
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
	mssql "github.com/microsoft/go-mssqldb"
)

// The disposable fixture supplies credentials through an inherited pipe only.
// The harness owns database creation, container lifetime and final cleanup.
func TestRelationalLiveMigrations(t *testing.T) {
	fd := os.Getenv("KELVO_MIGRATION_LIVE_FD")
	if fd == "" {
		t.Skip("isolated relational migration fixture not configured")
	}
	n, err := strconv.Atoi(fd)
	if err != nil || n < 3 {
		t.Fatal("invalid fixture pipe")
	}
	input := os.NewFile(uintptr(n), "migration-fixture")
	defer input.Close()
	raw, err := io.ReadAll(io.LimitReader(input, 64<<10))
	if err != nil {
		t.Fatal("fixture unavailable")
	}
	defer clear(raw)
	var fixture struct {
		Engine, Host, Username, Password, Database, Schema, CA, ServerName, AdminPassword string
		Port                                                                              int
	}
	if json.Unmarshal(raw, &fixture) != nil || fixture.Database != "kelvo_migration_fixture" {
		t.Fatal("invalid fixture scope")
	}
	var driver *Driver
	switch fixture.Engine {
	case "mysql":
		driver = NewMySQL()
	case "mariadb":
		driver = NewMariaDB()
	case "sqlserver":
		driver = NewSQLServer()
	default:
		t.Fatal("unsupported fixture engine")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(fixture.CA)) {
		t.Fatal("invalid fixture CA")
	}
	connection := adapter.Connection{TenantID: "fixture", ConnectionID: "migration", Revision: "one", Engine: fixture.Engine, Host: fixture.Host, Port: fixture.Port, Namespace: fixture.Database, Schema: fixture.Schema, Username: fixture.Username, Password: fixture.Password, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: fixture.ServerName}}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if fixture.Engine == "sqlserver" && fixture.AdminPassword != "" {
		bootstrapSQLServerMigrationFixture(t, ctx, connection, fixture.AdminPassword)
		fixture.AdminPassword = ""
	}
	open := func() *Session {
		t.Helper()
		session, err := driver.Open(ctx, connection)
		if err != nil {
			message := err.Error()
			for _, secret := range []string{connection.Password, connection.Username, connection.Host, connection.Namespace} {
				if secret != "" {
					message = strings.ReplaceAll(message, secret, "[redacted]")
				}
			}
			t.Fatalf("fixture source open failed (%T): %.512s", err, message)
		}
		return session.(*Session)
	}
	s := open()
	defer s.Close()
	if fixture.Engine == "sqlserver" {
		var db, schema string
		var count, state, implicit int
		err := s.Pool.QueryRowContext(ctx, "SELECT DB_NAME(), SCHEMA_NAME(), @@TRANCOUNT, XACT_STATE(), CASE WHEN (2 & @@OPTIONS) = 2 THEN 1 ELSE 0 END").Scan(&db, &schema, &count, &state, &implicit)
		t.Logf("SQL Server initial identity: database_match=%t schema_match=%t transactions=%d state=%d implicit=%d error_type=%T", db == fixture.Database, schema == fixture.Schema, count, state, implicit, err)
	}

	assertState := func(want migration.State) {
		t.Helper()
		state, err := s.MigrationStatus(ctx)
		if err != nil || state != want {
			message := ""
			if err != nil {
				message = err.Error()
			}
			for _, secret := range []string{connection.Password, connection.Username, connection.Host, connection.Namespace} {
				if secret != "" {
					message = strings.ReplaceAll(message, secret, "[redacted]")
				}
			}
			t.Fatalf("unexpected migration state: got=%+v want=%+v error_type=%T error=%.512s", state, want, err, message)
		}
	}
	scalar := func(query string) int64 {
		t.Helper()
		var value int64
		if err := s.Pool.QueryRowContext(ctx, query).Scan(&value); err != nil {
			t.Fatalf("fixture verification failed (%T)", err)
		}
		return value
	}
	apply := func(plan migration.Plan, want int64) {
		t.Helper()
		result, err := s.ApplyMigration(ctx, plan)
		if err != nil || result.Effect != operations.EffectCommitted || result.To != (migration.State{Version: want}) {
			t.Fatalf("migration did not commit: result=%+v error_type=%T", result, err)
		}
		assertState(migration.State{Version: want})
	}
	files := []migration.File{
		{Name: "1_seed.up.sql", Content: "CREATE TABLE kelvo_migration_probe(id bigint NOT NULL PRIMARY KEY, marker bigint NOT NULL); INSERT INTO kelvo_migration_probe VALUES(1,10)"},
		{Name: "1_seed.down.sql", Content: "DROP TABLE kelvo_migration_probe"},
		{Name: "2_bump.up.sql", Content: "UPDATE kelvo_migration_probe SET marker=marker+1 WHERE id=1"},
		{Name: "2_bump.down.sql", Content: "UPDATE kelvo_migration_probe SET marker=marker-1 WHERE id=1"},
	}
	if fixture.Engine == "sqlserver" {
		files[2].Content = "SET ROWCOUNT 1; " + files[2].Content
	}
	assertState(migration.State{Version: -1})
	if fixture.Engine != "sqlserver" {
		if _, err := s.Pool.ExecContext(ctx, "SELECT 1; SELECT 2"); err == nil {
			t.Fatal("ordinary MySQL pool enabled multiple statements")
		}
	}
	plan := migration.Plan{Version: migration.Version, Expected: migration.State{Version: -1}, Direction: "up", Steps: 1, Files: files}
	apply(plan, 1)
	if scalar("SELECT marker FROM kelvo_migration_probe WHERE id=1") != 10 {
		t.Fatal("multi-statement migration was incomplete")
	}
	result, err := s.ApplyMigration(ctx, plan)
	if !errors.Is(err, migration.ErrConflict) || result.Effect != operations.EffectNone {
		t.Fatal("stale migration was not rejected")
	}
	plan.Expected.Version = 1
	apply(plan, 2)
	if scalar("SELECT marker FROM kelvo_migration_probe WHERE id=1") != 11 {
		t.Fatal("migration replayed")
	}

	// An empty BEGIN must also be detected. innodb_trx cannot prove that this
	// session is idle; the following write must never be committed by SetVersion.
	begin := "START TRANSACTION"
	if fixture.Engine == "sqlserver" {
		begin = "BEGIN TRANSACTION"
	}
	openFiles := append(append([]migration.File(nil), files...), migration.File{Name: "3_open.up.sql", Content: begin + "; INSERT INTO kelvo_migration_probe VALUES(2,999)"})
	result, err = s.ApplyMigration(ctx, migration.Plan{Version: migration.Version, Expected: migration.State{Version: 2}, Direction: "up", Files: openFiles})
	if !errors.Is(err, migration.ErrOutcomeUnknown) || result.Effect != operations.EffectUnknown {
		t.Fatalf("open transaction was settled: result=%+v error_type=%T", result, err)
	}
	assertState(migration.State{Version: 3, Dirty: true})
	if scalar("SELECT COUNT(*) FROM kelvo_migration_probe WHERE id=2") != 0 {
		t.Fatal("version update committed the open script transaction")
	}
	force := int64(2)
	apply(migration.Plan{Version: migration.Version, Expected: migration.State{Version: 3, Dirty: true}, Direction: "force", ForceVersion: &force}, 2)

	// A second session's lock blocks dispatch, and the caller's cancellation
	// stops waiting without source writes or leaking the first session's lock.
	owner := open()
	defer owner.Close()
	conn, err := owner.Pool.Conn(ctx)
	if err != nil {
		t.Fatal("fixture lock connection unavailable")
	}
	defer conn.Close()
	aid, _ := database.GenerateAdvisoryLockId(fixture.Database + ":" + migration.Table)
	lockSQL, unlockSQL := "SELECT GET_LOCK(?, 0)", "SELECT RELEASE_LOCK(?)"
	if fixture.Engine == "sqlserver" {
		aid, _ = database.GenerateAdvisoryLockId(fixture.Database, fixture.Schema)
		lockSQL = "DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource=@p1,@LockMode='Exclusive',@LockOwner='Session',@LockTimeout=0; SELECT @r"
		unlockSQL = "DECLARE @r int; EXEC @r=sys.sp_releaseapplock @Resource=@p1,@LockOwner='Session'; SELECT @r"
	}
	var lockCode int
	if err := conn.QueryRowContext(ctx, lockSQL, aid).Scan(&lockCode); err != nil || fixture.Engine != "sqlserver" && lockCode != 1 || fixture.Engine == "sqlserver" && lockCode < 0 {
		t.Fatal("fixture migration lock unavailable")
	}
	short, stop := context.WithTimeout(ctx, 150*time.Millisecond)
	started := time.Now()
	plan.Expected.Version = 2
	result, err = s.ApplyMigration(short, plan)
	stop()
	if err == nil || result.Effect != operations.EffectNone || time.Since(started) > 3*time.Second {
		t.Fatal("migration lock cancellation did not remain bounded and effect-free")
	}
	if err := conn.QueryRowContext(ctx, unlockSQL, aid).Scan(&lockCode); err != nil {
		t.Fatal("fixture migration lock cleanup failed")
	}
	assertState(migration.State{Version: 2})
	apply(migration.Plan{Version: migration.Version, Expected: migration.State{Version: 2}, Direction: "down", Files: files}, -1)

	// Status must reject a user-supplied history view before evaluating it.
	for _, query := range []string{"DROP TABLE schema_migrations", "CREATE VIEW schema_migrations AS SELECT CAST(1 AS bigint) AS version, CAST(0 AS bit) AS dirty"} {
		if fixture.Engine != "sqlserver" && query[0] == 'C' {
			query = "CREATE VIEW schema_migrations AS SELECT 1 AS version, 0 AS dirty"
		}
		if _, err := s.Pool.ExecContext(ctx, query); err != nil {
			t.Fatalf("fixture view setup failed (%T)", err)
		}
	}
	if _, err := s.MigrationStatus(ctx); err == nil {
		t.Fatal("history view accepted")
	}
	if _, err := s.Pool.ExecContext(ctx, "DROP VIEW schema_migrations"); err != nil {
		t.Fatal("fixture view cleanup failed")
	}
}

func bootstrapSQLServerMigrationFixture(t *testing.T, ctx context.Context, connection adapter.Connection, password string) {
	t.Helper()
	admin := connection
	admin.Username, admin.Password, admin.Namespace, admin.Schema = "sa", password, "master", "dbo"
	config, err := sqlserverConfig(admin)
	if err != nil {
		t.Fatal("invalid SQL Server fixture bootstrap")
	}
	pool := sql.OpenDB(mssql.NewConnectorConfig(config))
	defer pool.Close()
	pool.SetMaxOpenConns(1)
	ready := time.Now().Add(40 * time.Second)
	for pool.PingContext(ctx) != nil {
		if ctx.Err() != nil || time.Now().After(ready) {
			t.Fatal("SQL Server fixture did not become ready")
		}
		time.Sleep(time.Second)
	}
	// Disposable bootstrap credentials arrive through the same inherited pipe.
	// The SA login is disabled before the application account runs any tests.
	for _, query := range []string{
		"CREATE DATABASE [kelvo_migration_fixture]",
		"CREATE LOGIN [kelvo_migration] WITH PASSWORD='" + strings.ReplaceAll(connection.Password, "'", "''") + "', CHECK_POLICY=OFF",
		"USE [kelvo_migration_fixture]; CREATE USER [kelvo_migration] FOR LOGIN [kelvo_migration]; ALTER ROLE db_owner ADD MEMBER [kelvo_migration]",
		"ALTER LOGIN [sa] DISABLE",
	} {
		if _, err := pool.ExecContext(ctx, query); err != nil {
			t.Fatalf("SQL Server fixture bootstrap failed (%T)", err)
		}
	}
}
