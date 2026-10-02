// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"crypto/tls"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func clearPG(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "PG") {
			old, ok := os.LookupEnv(name)
			os.Unsetenv(name)
			t.Cleanup(func() {
				if ok {
					os.Setenv(name, old)
				}
			})
		}
	}
}
func TestTLSDSN(t *testing.T) {
	clearPG(t)
	for _, dsn := range []string{"postgres://u:p@db.example/x?sslmode=verify-full", "postgresql://u:p%40ss@[::1]:5432/x?sslmode=verify-full&connect_timeout=5"} {
		if err := validateDSN(dsn); err != nil {
			t.Fatal(err)
		}
	}
	for _, dsn := range []string{
		"postgres://u:p@/x?sslmode=verify-full", "postgres://u:p@db/x?sslmode=disable", "postgres://u:p@db/x?sslmode=require", "postgres://u@db/x?sslmode=verify-full", "postgres://u:@db/x?sslmode=verify-full", "postgres://u:p@db/?sslmode=verify-full",
		"postgres://u:p@db/x?sslmode=disable&sslmode=verify-full", "postgres://u:p@db/x?sslmode=verify-full&host=evil", "postgres://u:p@db/x?sslmode=verify-full&hostaddr=evil", "postgres://u:p@db/x?sslmode=verify-full&user=other", "postgres://u:p@db/x?sslmode=verify-full&password=other", "postgres://u:p@db/x?sslmode=verify-full&options=-csearch_path=other",
		"postgres://u:p@db/x?sslmode=verify-full&sslrootcert=/etc/passwd", "postgres://u:p@db/x?sslmode=verify-full&sslcert=/etc/passwd", "postgres://u:p@db/x?sslmode=verify-full&service=other", "postgres://u:p@db/x?sslmode=verify-full&servicefile=/etc/passwd", "postgres://u:p@db/x?sslmode=verify-full&passfile=/etc/passwd",
		"postgres://u:p@db,x/x?sslmode=verify-full", "postgres://u:p@db:0/x?sslmode=verify-full", "postgres://u:p@db/x?sslmode=verify-full#fragment", "postgres://u:p@db/x?sslmode=verify-full&connect_timeout=0", "postgres://u:p@db/x?sslmode=verify-full&connect_timeout=1&connect_timeout=2",
	} {
		if validateDSN(dsn) == nil {
			t.Fatalf("unsafe DSN accepted")
		}
	}
}
func TestConfigUsesNoAmbientCredentials(t *testing.T) {
	clearPG(t)
	config, err := parseConfig("postgres://explicit:password@db.example/analytics?sslmode=verify-full")
	if err != nil {
		t.Fatal(err)
	}
	if config.Host != "db.example" || config.User != "explicit" || config.Password != "password" || config.Database != "analytics" || config.TLSConfig.MinVersion != tls.VersionTLS12 || config.TLSConfig.InsecureSkipVerify || len(config.TLSConfig.Certificates) != 0 || len(config.Fallbacks) != 0 || config.RuntimeParams["timezone"] != "UTC" {
		t.Fatal("config was not isolated")
	}
	conn := &pgconn.PgConn{}
	handler, ok := config.BuildContextWatcherHandler(conn).(*pgconn.CancelRequestContextWatcherHandler)
	if !ok || handler.Conn != conn || handler.CancelRequestDelay != 0 || handler.DeadlineDelay != 500*time.Millisecond {
		t.Fatal("bounded upstream cancellation handler missing")
	}
	for _, name := range []string{"PGSERVICEFILE", "PGSERVICE", "PGPASSFILE", "PGSSLKEY", "PGSSLCERT", "PGSSLROOTCERT", "PGPASSWORD", "PGUSER", "PGHOST", "PGPORT", "PGOPTIONS"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "forbidden")
			if _, err := parseConfig("postgres://explicit:password@db.example/analytics?sslmode=verify-full"); err == nil {
				t.Fatal("ambient setting accepted")
			}
		})
	}
}
