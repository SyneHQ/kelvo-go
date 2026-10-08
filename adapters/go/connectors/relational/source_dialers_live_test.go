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
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/migration"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/stdlib"
)

// Credentials arrive through a private inherited pipe. The controller owns the
// disposable source, source schema and final cleanup. No external DB is allowed.
func TestSourceDialersLive(t *testing.T) {
	descriptor := os.Getenv("KELVO_SOURCE_DIALERS_LIVE_FD")
	if descriptor == "" {
		t.Skip("isolated source dialer fixture not configured")
	}
	fd, err := strconv.Atoi(descriptor)
	if err != nil {
		t.Fatal("invalid fixture descriptor")
	}
	cleanPostgresEnvironment(t)
	if fd < 3 {
		t.Fatal("invalid fixture descriptor")
	}
	input := os.NewFile(uintptr(fd), "source-dialer-fixture")
	defer input.Close()
	raw, err := io.ReadAll(io.LimitReader(input, 64<<10))
	if err != nil {
		t.Fatal("fixture input unavailable")
	}
	defer clear(raw)
	var f struct {
		Engine, PhysicalIP, Host, Username, Password, Database, CA string
		Port                                                       int
	}
	if json.Unmarshal(raw, &f) != nil || net.ParseIP(f.PhysicalIP) == nil || f.Host != "source.private.invalid" || f.Database != "kelvo_private_dialer_fixture" {
		t.Fatal("invalid disposable fixture scope")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(f.CA)) {
		t.Fatal("fixture CA unavailable")
	}
	var dnsCalls, dataCalls, cancelCalls atomic.Int64
	rejectingResolver := &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
		dnsCalls.Add(1)
		return nil, errors.New("fixture DNS is forbidden")
	}}
	originalResolver := net.DefaultResolver
	net.DefaultResolver = rejectingResolver
	t.Cleanup(func() {
		net.DefaultResolver = originalResolver
		if dnsCalls.Load() != 0 {
			t.Error("driver attempted child DNS resolution")
		}
	})
	authority := net.JoinHostPort(f.Host, strconv.Itoa(f.Port))
	physical := net.JoinHostPort(f.PhysicalIP, strconv.Itoa(f.Port))
	physicalDialer := &net.Dialer{Timeout: time.Second, Resolver: rejectingResolver}
	hook := func(counter *atomic.Int64) sourceDialFunc {
		return func(ctx context.Context, network, address string) (net.Conn, error) {
			if network != "tcp" || address != authority {
				return nil, errors.New("driver changed original authority")
			}
			counter.Add(1)
			return physicalDialer.DialContext(ctx, "tcp", physical)
		}
	}
	c := adapter.Connection{TenantID: "fixture-a", ConnectionID: "private-source", Revision: "one", Engine: f.Engine, Host: f.Host, Port: f.Port, Namespace: f.Database, Username: f.Username, Password: f.Password, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, DialContext: hook(&dataCalls)}
	if f.Engine == "postgresql" {
		c.Schema = "public"
		c.DialCancellation = hook(&cancelCalls)
	} else if f.Engine == "mysql" {
		c.Schema = f.Database
	} else {
		t.Fatal("unsupported live fixture engine")
	}
	ctx, stop := context.WithTimeout(context.Background(), 40*time.Second)
	defer stop()
	driver := NewPostgreSQL()
	if f.Engine == "mysql" {
		driver = NewMySQL()
	}
	open := func(connection adapter.Connection) *Session {
		t.Helper()
		session, err := driver.Open(ctx, connection)
		if err != nil {
			t.Fatalf("source connection failed: %T", err)
		}
		return session.(*Session)
	}
	s := open(c)
	defer s.Close()
	var one int
	if err := s.Pool.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatal("source SELECT failed")
	}
	if dataCalls.Load() != 1 {
		t.Fatal("source SELECT did not reuse its admitted data connection")
	}
	if f.Engine == "postgresql" {
		var secure bool
		if err := s.Pool.QueryRowContext(ctx, "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()").Scan(&secure); err != nil || !secure {
			t.Fatal("PostgreSQL data was not protected by TLS")
		}
	} else {
		var name, cipher string
		if err := s.Pool.QueryRowContext(ctx, "SHOW STATUS LIKE 'Ssl_cipher'").Scan(&name, &cipher); err != nil || cipher == "" {
			t.Fatal("MySQL data was not protected by TLS")
		}
	}
	// The physical endpoint has no certificate authority of its own. A wrong
	// source hostname must fail even though the hook reaches the same source.
	wrong := c
	wrong.TLS = c.TLS.Clone()
	wrong.TLS.ServerName = "wrong.private.invalid"
	if session, err := driver.Open(ctx, wrong); err == nil {
		session.Close()
		t.Fatal("wrong source TLS hostname was accepted")
	}
	if f.Engine == "mysql" {
		config, err := mysqlConfig(c)
		if err != nil {
			t.Fatal("MySQL source configuration failed")
		}
		clone := config.Clone()
		clone.MultiStatements = true
		if clone.DialFunc == nil || clone.TLS.ServerName != f.Host || config.MultiStatements {
			t.Fatal("MySQL clone lost hook or TLS isolation")
		}
		connector, err := mysqldriver.NewConnector(clone)
		if err != nil {
			t.Fatal("MySQL clone connector unavailable")
		}
		pool := sql.OpenDB(connector)
		before := dataCalls.Load()
		if _, err := pool.ExecContext(ctx, "SELECT 1; SELECT 2"); err != nil {
			pool.Close()
			t.Fatal("MySQL clone did not execute admitted multistatements")
		}
		pool.Close()
		if dataCalls.Load() != before+1 {
			t.Fatal("MySQL clone bypassed source hook")
		}
		if _, err := s.Pool.ExecContext(ctx, "SELECT 1; SELECT 2"); err == nil {
			t.Fatal("ordinary MySQL pool enabled multistatements")
		}
		state, err := s.MigrationStatus(ctx)
		if err != nil || state.Version != -1 {
			t.Fatal("fresh MySQL migration state unavailable")
		}
		before = dataCalls.Load()
		plan := migration.Plan{Version: migration.Version, Expected: state, Direction: "up", Steps: 1, Files: []migration.File{{Name: "1_private.up.sql", Content: "CREATE TABLE kelvo_private_probe(id INT PRIMARY KEY, value INT); INSERT INTO kelvo_private_probe VALUES(1,73);"}}}
		result, err := s.ApplyMigration(ctx, plan)
		if err != nil || result.To.Version != 1 {
			t.Fatalf("private MySQL migration failed: %T", err)
		}
		var value int
		if err := s.Pool.QueryRowContext(ctx, "SELECT value FROM kelvo_private_probe WHERE id=1").Scan(&value); err != nil || value != 73 {
			t.Fatal("private MySQL migration statements were incomplete")
		}
		if dataCalls.Load() <= before {
			t.Fatal("migration clone did not use an admitted hook")
		}
		t.Log("MySQL TLS, no child DNS, SELECT, Clone and migration hook gates passed")
		return
	}
	// An observer uses the literal fixture IP and a separate direct fixture
	// connection. It is not a private-source fallback or part of hook counts.
	observerConfig, err := postgresConfig(c)
	if err != nil {
		t.Fatal("observer configuration unavailable")
	}
	observerConfig.Host = f.PhysicalIP
	observerConfig.LookupFunc = func(context.Context, string) ([]string, error) { return []string{f.PhysicalIP}, nil }
	observerConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		return physicalDialer.DialContext(ctx, "tcp", physical)
	}
	observer := stdlib.OpenDB(*observerConfig)
	defer observer.Close()
	active := func(pid int) bool {
		t.Helper()
		var count int
		err := observer.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid=$1", pid).Scan(&count)
		if err != nil {
			t.Fatal("fixture observer query failed")
		}
		return count != 0
	}
	waitGone := func(pid int) {
		t.Helper()
		for end := time.Now().Add(2 * time.Second); time.Now().Before(end); {
			if !active(pid) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("cancelled source backend remained active")
	}
	var pid int
	if err := s.Pool.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal("source backend identity unavailable")
	}
	beforeData, beforeCancel := dataCalls.Load(), cancelCalls.Load()
	short, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	rows, queryErr := s.Pool.QueryContext(short, "SELECT pg_sleep(30)")
	if rows != nil {
		rows.Close()
	}
	cancel()
	if queryErr == nil {
		t.Fatal("PostgreSQL cancellation did not interrupt the query")
	}
	s.Close()
	waitGone(pid)
	if cancelCalls.Load() <= beforeCancel || dataCalls.Load() != beforeData {
		t.Fatal("PostgreSQL cancellation used a data retry or missed cancellation hook")
	}
	// Reject both the explicit watcher and pgx asyncClose cancellation channel.
	// Cleanup uses the observer only after asserting no data fallback occurred.
	denied := c
	denied.DialCancellation = func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" || address != authority {
			return nil, adapter.ErrInvalid
		}
		cancelCalls.Add(1)
		return nil, errors.New("fixture cancellation denied")
	}
	blocked := open(denied)
	defer blocked.Close()
	if err := blocked.Pool.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal("denied-cancel backend identity unavailable")
	}
	beforeData, beforeCancel = dataCalls.Load(), cancelCalls.Load()
	short, cancel = context.WithTimeout(ctx, 150*time.Millisecond)
	rows, queryErr = blocked.Pool.QueryContext(short, "SELECT pg_sleep(30)")
	if rows != nil {
		rows.Close()
	}
	cancel()
	if queryErr == nil {
		t.Fatal("denied cancellation did not bound the client request")
	}
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end) && cancelCalls.Load() < beforeCancel+2; {
		time.Sleep(10 * time.Millisecond)
	}
	if dataCalls.Load() != beforeData || cancelCalls.Load() < beforeCancel+2 {
		t.Fatal("explicit or asyncClose cancellation escaped its hook")
	}
	var requested bool
	if err := observer.QueryRowContext(ctx, "SELECT pg_cancel_backend($1)", pid).Scan(&requested); err != nil {
		t.Fatal("owned fixture backend cleanup failed")
	}
	blocked.Close()
	waitGone(pid)
	if dataCalls.Load() != beforeData {
		t.Fatal("denied cancellation retried a data connection")
	}
	t.Log("PostgreSQL TLS, no child DNS, SELECT, watcher cancellation, asyncClose separation and backend cleanup gates passed")
}
