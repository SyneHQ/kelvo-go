// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Requires a dedicated TLS PostgreSQL fixture. The reader needs CONNECT and
// visibility of its own pg_stat_activity rows; no schema writes/admin role.
// All three connections use the same role. Supply an optional in-memory CA in
// KELVO_TEST_POSTGRES_CANCEL_CA_PEM; the DSN must retain sslmode=verify-full.
func TestLiveCancellationWaitsForDelayedTLSCancelAndPreservesOtherBackend(t *testing.T) {
	dsn := os.Getenv("KELVO_TEST_POSTGRES_CANCEL_DSN")
	if dsn == "" {
		t.Skip("dedicated PostgreSQL cancellation fixture is not configured")
	}
	clearPG(t)
	source := catalog.Source{}
	if ca := os.Getenv("KELVO_TEST_POSTGRES_CANCEL_CA_PEM"); ca != "" {
		source.Options = map[string]string{"tls_ca_pem": ca}
	}
	base, err := parseSourceConfig(source, dsn)
	if err != nil {
		t.Fatal("invalid dedicated cancellation fixture configuration")
	}
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	connect := func(config *pgx.ConnConfig) *pgx.Conn {
		t.Helper()
		conn, err := pgx.ConnectConfig(ctx, config)
		if err != nil {
			t.Fatalf("fixture connection failed (%T)", err)
		}
		t.Cleanup(func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = conn.Close(closeCtx)
		})
		return conn
	}
	observer := connect(base.Copy())
	unrelated := connect(base.Copy())
	targetConfig := base.Copy()
	dial := targetConfig.DialFunc
	var dials atomic.Int32
	const connectionDelay = 1500 * time.Millisecond
	targetConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if dials.Add(1) > 1 {
			// Delay only a new cancellation connection. Real pgx must then
			// negotiate TLS and deliver the actual PostgreSQL CancelRequest.
			timer := time.NewTimer(connectionDelay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		return dial(ctx, network, address)
	}
	target := connect(targetConfig)
	startSleep := func(conn *pgx.Conn, marker string) (context.CancelFunc, <-chan struct{}, <-chan error) {
		queryCtx, cancel := context.WithCancel(ctx)
		done, result := make(chan struct{}), make(chan error, 1)
		go func() {
			_, err := conn.Exec(queryCtx, "SELECT pg_sleep(30) /* "+marker+" */")
			result <- err
			close(done)
		}()
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(query.WorkerWaitDelay):
				t.Error("fixture query cleanup exceeded bound")
			}
		})
		return cancel, done, result
	}
	marker := fmt.Sprintf("kelvo_cancel_%d", time.Now().UnixNano())
	_, otherDone, _ := startSleep(unrelated, marker+"_unrelated")
	cancelTarget, targetDone, targetResult := startSleep(target, marker+"_target")
	isActive := func(pid uint32) bool {
		t.Helper()
		var active bool
		if err := observer.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND state='active' AND query LIKE $2)", pid, "%"+marker+"%").Scan(&active); err != nil {
			t.Fatalf("source activity observation failed (%T)", err)
		}
		return active
	}
	readyBy := time.Now().Add(5 * time.Second)
	for !isActive(target.PgConn().PID()) || !isActive(unrelated.PgConn().PID()) {
		if time.Now().After(readyBy) {
			t.Fatal("both marked source queries did not become active")
		}
		time.Sleep(20 * time.Millisecond)
	}
	started := time.Now()
	cancelTarget()
	select {
	case <-targetDone:
	case <-time.After(query.WorkerCancellationGrace):
		t.Fatal("target query did not return within cancellation grace")
	}
	err = <-targetResult
	var pgErr *pgconn.PgError
	if !errors.Is(err, context.Canceled) && !(errors.As(err, &pgErr) && pgErr.Code == "57014") {
		t.Fatalf("expected cancellation, got %T", err)
	}
	// Check immediately when the driver returns: a later asynchronous cleanup
	// could hide a too-short cancellation budget, but an exiting worker cannot
	// keep that background retry alive. The old 500ms budget fails here.
	if isActive(target.PgConn().PID()) {
		t.Fatal("driver returned while the marked source query was still running")
	}
	if !isActive(unrelated.PgConn().PID()) {
		t.Fatal("cancelling one connection also stopped the unrelated backend")
	}
	select {
	case <-otherDone:
		t.Fatal("unrelated query unexpectedly completed")
	default:
	}
	if dials.Load() < 2 || time.Since(started) < connectionDelay {
		t.Fatal("test did not exercise the delayed cancellation connection")
	}
	t.Logf("delayed verified-TLS cancellation stopped only its target in %s", time.Since(started))
}
