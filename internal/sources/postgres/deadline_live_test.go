// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/jackc/pgx/v5/pgconn"
)

// Use a dedicated PostgreSQL fixture with verified TLS and a SELECT-only role.
// The test changes only its own session settings and runs rollback-only queries.
func TestLiveStatementDeadlineStopsSourceAndRestoresSession(t *testing.T) {
	dsn := os.Getenv("KELVO_TEST_POSTGRES_DEADLINE_DSN")
	if dsn == "" {
		t.Skip("dedicated PostgreSQL deadline fixture is not configured")
	}
	clearPG(t)
	source := catalog.Source{}
	if ca := os.Getenv("KELVO_TEST_POSTGRES_DEADLINE_CA_PEM"); ca != "" {
		source.Options = map[string]string{"tls_ca_pem": ca}
	}
	db, err := openSourceDB(source, dsn)
	if err != nil {
		t.Fatal("invalid dedicated deadline fixture configuration")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("fixture connection failed (%T)", err)
	}
	defer conn.Close()
	var original string
	var originalPID int64
	if err := conn.QueryRowContext(ctx, "SELECT pg_catalog.current_setting('statement_timeout'), pg_catalog.pg_backend_pid()").Scan(&original, &originalPID); err != nil {
		t.Fatalf("fixture identity read failed (%T)", err)
	}
	defer func() {
		resetCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_, _ = conn.ExecContext(resetCtx, "SELECT pg_catalog.set_config('statement_timeout', $1, false)", original)
	}()
	for _, test := range []struct {
		name, sourceSetting string
		wantMax             int64
	}{
		{"disabled source timeout", "0", 2000},
		{"stricter source timeout", "200", 200},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := conn.ExecContext(ctx, "SELECT pg_catalog.set_config('statement_timeout', $1, false)", test.sourceSetting); err != nil {
				t.Fatalf("fixture session setup failed (%T)", err)
			}
			var before string
			if err := conn.QueryRowContext(ctx, "SELECT pg_catalog.current_setting('statement_timeout')").Scan(&before); err != nil {
				t.Fatalf("fixture timeout read failed (%T)", err)
			}
			tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatalf("fixture transaction failed (%T)", err)
			}
			defer tx.Rollback()
			requestCtx, stop := context.WithTimeout(ctx, 2*time.Second)
			err = configureStatementTimeout(requestCtx, tx)
			stop()
			if err != nil {
				t.Fatalf("source timeout configuration failed (%T)", err)
			}
			var selected int64
			if err := tx.QueryRowContext(ctx, "SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name='statement_timeout'").Scan(&selected); err != nil {
				t.Fatalf("transaction timeout read failed (%T)", err)
			}
			if selected < 1 || selected > test.wantMax || (test.sourceSetting != "0" && selected != test.wantMax) {
				t.Fatalf("source timeout=%d, expected positive and at most %d", selected, test.wantMax)
			}
			// The query uses a longer, live context. Only the source timeout can
			// stop this sleep. The configured request context is already cancelled.
			started := time.Now()
			_, err = tx.ExecContext(ctx, "SELECT pg_sleep(6)")
			var server *pgconn.PgError
			if ctx.Err() != nil || !errors.As(err, &server) || server.Code != "57014" {
				t.Fatalf("expected source timeout with a live client context, got %T", err)
			}
			if time.Since(started) >= 5*time.Second {
				t.Fatal("source timeout did not stop the sleep within the test bound")
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("fixture rollback failed (%T)", err)
			}
			var restored string
			var pid int64
			if err := conn.QueryRowContext(ctx, "SELECT pg_catalog.current_setting('statement_timeout'), pg_catalog.pg_backend_pid()").Scan(&restored, &pid); err != nil {
				t.Fatalf("session restoration read failed (%T)", err)
			}
			if restored != before || pid != originalPID {
				t.Fatal("timeout escaped its transaction or the session was replaced")
			}
		})
	}
}
