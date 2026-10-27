//go:build duckdb_arrow && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// The rendered bound must select the same rows whatever the engine session
// time zone is: a naive literal against a TIMESTAMPTZ column is read in the
// session zone and would skip (or re-read) hours of rows off UTC.
func TestIncrementalLiteralIgnoresSessionTimeZone(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE z(ts TIMESTAMPTZ); CREATE TABLE n(ts TIMESTAMP);
		INSERT INTO z VALUES ('2026-03-01 10:00:00+00'), ('2026-03-01 12:00:00+00');
		INSERT INTO n VALUES ('2026-03-01 10:00:00'), ('2026-03-01 12:00:00')`); err != nil {
		t.Fatal(err)
	}
	zoned, _ := incrementalLiteral("2026-03-01T12:00:00Z", 0, true)
	naive, _ := incrementalLiteral("2026-03-01T12:00:00Z", 0, false)
	for _, zone := range []string{"UTC", "America/New_York", "Asia/Kolkata"} {
		if _, err := db.Exec("SET TimeZone='" + zone + "'"); err != nil {
			t.Skipf("engine has no time zone support: %v", err)
		}
		for table, literal := range map[string]string{"z": zoned, "n": naive} {
			var n int
			if err := db.QueryRow("SELECT count(*) FROM " + table + " WHERE ts >= " + literal).Scan(&n); err != nil || n != 1 {
				t.Fatalf("%s in %s: %d rows %v (literal %s)", table, zone, n, err, literal)
			}
		}
	}
}
