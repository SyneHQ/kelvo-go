package migrations

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/golang-migrate/migrate/v4/database"
)

func persistentFixture(t *testing.T, engine string) (*persistentDriver, sqlmock.Sqlmock) {
	t.Helper()
	db, m, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	d := &persistentDriver{ctx: context.Background(), conn: conn, engine: engine, schema: "public", table: table, lockTable: `"public"."kelvo_migration_lock"`, owner: strings.Repeat("a", 64), transactionStatus: func(*sql.Conn) (byte, error) { return 'I', nil }}
	t.Cleanup(func() { _ = conn.Close(); _ = db.Close() })
	return d, m
}
func persistentShape(m sqlmock.Sqlmock, engine, name string, exists bool) {
	rows := sqlmock.NewRows([]string{"kind", "column", "type", "nullable", "default"})
	if exists {
		if name == migration.Table {
			rows.AddRow("BASE TABLE", "version", "bigint", "NO", nil).AddRow("BASE TABLE", "dirty", "boolean", "NO", nil)
		} else {
			rows.AddRow("BASE TABLE", "lock_id", "bigint", "NO", nil).AddRow("BASE TABLE", "owner", "character varying", "NO", nil)
		}
	}
	m.ExpectQuery(warehouseTableSQL).WithArgs("public", name).WillReturnRows(rows)
	if exists && engine == "cockroachdb" {
		m.ExpectQuery(`SELECT count(*) FROM information_schema.triggers WHERE event_object_schema=$1 AND event_object_table=$2`).WithArgs("public", name).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	}
}
func persistentState(m sqlmock.Sqlmock, engine string, version int64, dirty bool) {
	persistentShape(m, engine, migration.Table, true)
	m.ExpectQuery(versionSQL).WillReturnRows(sqlmock.NewRows([]string{"version", "dirty"}).AddRow(version, dirty))
}
func persistentOwner(m sqlmock.Sqlmock, engine, owner string) {
	persistentShape(m, engine, persistentLockTable, true)
	m.ExpectQuery(`SELECT lock_id,owner FROM "public"."kelvo_migration_lock" LIMIT 2`).WillReturnRows(sqlmock.NewRows([]string{"lock_id", "owner"}).AddRow(1, owner))
}

func TestPersistentLockRejectsStaleVersionWithoutCreatingMetadata(t *testing.T) {
	for _, engine := range []string{"cockroachdb", "redshift"} {
		t.Run(engine, func(t *testing.T) {
			d, m := persistentFixture(t, engine)
			p := testPlan()
			d.plan = &p
			persistentState(m, engine, 2, false)
			if err := d.Lock(); !errors.Is(err, migration.ErrConflict) {
				t.Fatal(err)
			}
			if err := m.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestPersistentLockRejectsExistingOwnerAcrossWorkers(t *testing.T) {
	for _, engine := range []string{"cockroachdb", "redshift"} {
		t.Run(engine, func(t *testing.T) {
			d, m := persistentFixture(t, engine)
			p := testPlan()
			d.plan = &p
			persistentState(m, engine, 1, false)
			persistentShape(m, engine, persistentLockTable, true)
			m.ExpectBegin()
			if engine == "redshift" {
				m.ExpectExec(`LOCK TABLE "public"."kelvo_migration_lock"`).WillReturnResult(sqlmock.NewResult(0, 0))
			}
			persistentOwner(m, engine, strings.Repeat("b", 64))
			m.ExpectRollback()
			if err := d.Lock(); !errors.Is(err, database.ErrLocked) {
				t.Fatal(err)
			}
			if err := m.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestPersistentUncertainScriptNeverUnlocksOrRetries(t *testing.T) {
	for _, engine := range []string{"cockroachdb", "redshift"} {
		t.Run(engine, func(t *testing.T) {
			d, m := persistentFixture(t, engine)
			d.locked = true
			persistentOwner(m, engine, d.owner)
			m.ExpectExec("ALTER TABLE orders ADD COLUMN total bigint").WillReturnError(context.DeadlineExceeded)
			if err := d.Run(strings.NewReader("ALTER TABLE orders ADD COLUMN total bigint")); err == nil || !d.uncertain {
				t.Fatal(err)
			}
			if err := d.Unlock(); !errors.Is(err, migration.ErrOutcomeUnknown) {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			if err := m.ExpectationsWereMet(); err != nil {
				t.Fatal("uncertain source was unlocked", err)
			}
		})
	}
}
func TestPersistentReleaseDeletesOnlyItsOwnerAndCapturesStateBeforeRelease(t *testing.T) {
	for _, engine := range []string{"cockroachdb", "redshift"} {
		t.Run(engine, func(t *testing.T) {
			d, m := persistentFixture(t, engine)
			d.locked = true
			persistentOwner(m, engine, d.owner)
			persistentState(m, engine, 2, false)
			m.ExpectBegin()
			if engine == "redshift" {
				m.ExpectExec(`LOCK TABLE "public"."kelvo_migration_lock"`).WillReturnResult(sqlmock.NewResult(0, 0))
			}
			m.ExpectExec(`DELETE FROM "public"."kelvo_migration_lock" WHERE lock_id=1 AND owner=$1`).WithArgs(d.owner).WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectCommit()
			if err := d.Unlock(); err != nil || d.locked || !d.finalRead || d.finalState.Version != 2 {
				t.Fatal(err)
			}
			if err := m.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestPersistentLostOwnershipFailsClosed(t *testing.T) {
	d, m := persistentFixture(t, "redshift")
	d.locked = true
	persistentOwner(m, "redshift", strings.Repeat("b", 64))
	if err := d.requireLock(); !errors.Is(err, migration.ErrOutcomeUnknown) || !d.uncertain {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
