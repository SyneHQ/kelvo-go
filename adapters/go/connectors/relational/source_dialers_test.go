// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package relational

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func TestSourceDialersPreserveAuthorityAndSeparatePurpose(t *testing.T) {
	cleanPostgresEnvironment(t)
	c := testConnection()
	var dataCalls, cancelCalls int
	denied := errors.New("fixture transport denied")
	c.DialContext = func(context.Context, string, string) (net.Conn, error) { dataCalls++; return nil, denied }
	c.DialCancellation = func(context.Context, string, string) (net.Conn, error) { cancelCalls++; return nil, denied }
	config, err := postgresConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	addresses, err := config.LookupFunc(context.Background(), c.Host)
	if err != nil || len(addresses) != 1 || addresses[0] != c.Host || config.TLSConfig.ServerName != c.Host {
		t.Fatal("original DNS/TLS authority changed")
	}
	if _, err := config.LookupFunc(context.Background(), "other.invalid"); err == nil {
		t.Fatal("different source resolved")
	}
	dataCtx := context.WithValue(context.Background(), dataPurpose{}, dialScope(c))
	for _, ctx := range []context.Context{dataCtx, context.WithValue(dataCtx, cancellationPurpose{}, true), context.Background()} {
		if _, err := config.DialFunc(ctx, "tcp", "db.example:5432"); !errors.Is(err, denied) {
			t.Fatal("transport denial was not retained")
		}
	}
	if dataCalls != 1 || cancelCalls != 2 {
		t.Fatal("data or cancellation used the wrong hook")
	}
	other := c
	other.ConnectionID = "other"
	for _, input := range []struct {
		ctx              context.Context
		network, address string
	}{
		{dataCtx, "unix", "db.example:5432"}, {dataCtx, "tcp", "other.invalid:5432"},
		{context.WithValue(dataCtx, dataPurpose{}, dialScope(other)), "tcp", "db.example:5432"},
	} {
		if _, err := config.DialFunc(input.ctx, input.network, input.address); err == nil {
			t.Fatal("unbound dial accepted")
		}
	}
	if dataCalls != 1 || cancelCalls != 2 {
		t.Fatal("invalid dial reached a hook")
	}
	raw, err := json.Marshal(c)
	if err != nil || strings.Contains(string(raw), "DialContext") || strings.Contains(string(raw), "DialCancellation") {
		t.Fatal("runtime hooks entered serialized connection")
	}
}

func TestSourceDialersRejectPartialAndUnsupportedConfiguration(t *testing.T) {
	hook := func(context.Context, string, string) (net.Conn, error) {
		t.Fatal("invalid configuration opened a connection")
		return nil, adapter.ErrInvalid
	}
	for _, engine := range []string{"postgresql", "mysql", "mariadb", "sqlserver", "redshift"} {
		c := testConnection()
		c.Engine = engine
		c.DialCancellation = hook
		if validConnection(c) {
			t.Fatal("cancellation-only transport accepted")
		}
		c.DialContext = hook
		if validConnection(c) != (engine == "postgresql" || engine == "mysql") {
			t.Fatal("engine transport scope changed")
		}
		c.DialCancellation = nil
		if validConnection(c) != (engine == "mysql") {
			t.Fatal("PostgreSQL accepted a missing cancellation hook")
		}
	}
}

func TestMySQLSourceDialerSurvivesMigrationClone(t *testing.T) {
	c := testConnection()
	c.Engine = "mysql"
	c.Port = 3306
	c.Schema = c.Namespace
	calls := 0
	c.DialContext = func(context.Context, string, string) (net.Conn, error) { calls++; return nil, adapter.ErrInvalid }
	config, err := mysqlConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	clone := config.Clone()
	clone.MultiStatements = true
	if clone.DialFunc == nil || clone.TLS.ServerName != c.Host || config.MultiStatements {
		t.Fatal("clone lost source transport or isolation")
	}
	if _, err := clone.DialFunc(context.Background(), "tcp", "db.example:3306"); err == nil || calls != 1 {
		t.Fatal("clone bypassed the source hook")
	}
}

func TestSourceDialerRetainsAddressAndClosesLateConnections(t *testing.T) {
	c := testConnection()
	left, right := net.Pipe()
	defer right.Close()
	dial := scopedSourceDialer(c, func(context.Context, string, string) (net.Conn, error) { return left, nil })
	conn, err := dial(context.Background(), "tcp", "db.example:5432")
	if err != nil {
		t.Fatal(err)
	}
	if conn.RemoteAddr().Network() != "tcp" || conn.RemoteAddr().String() != "db.example:5432" {
		t.Fatal("cancellation lost original source address")
	}
	conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	left, right = net.Pipe()
	defer right.Close()
	dial = scopedSourceDialer(c, func(context.Context, string, string) (net.Conn, error) { cancel(); return left, nil })
	if _, err := dial(ctx, "tcp", "db.example:5432"); !errors.Is(err, context.Canceled) {
		t.Fatal("late connection escaped cancellation")
	}
	if _, err := right.Write([]byte{1}); err == nil {
		t.Fatal("late connection was not closed")
	}
}
