//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestRabbitNativeMySQLHelper(t *testing.T) {
	if os.Getenv("KELVO_RABBIT_NATIVE_HELPER") != "1" {
		t.Skip("requires the actual Rabbit native fixture coordinator")
	}
	input, err := readNativeHelperInput(os.Stdin)
	if err != nil || !hexValue(input.MySQLPassword, 64) || input.Mode == "cancel" {
		t.Fatal("invalid native MySQL fixture input")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	session, opens := nativeHelperSession(t, ctx, input)
	host, _, _ := net.SplitHostPort(input.Claims.Authority)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(input.SourceCA) {
		t.Fatal("invalid native MySQL source CA")
	}
	config := mysql.NewConfig()
	config.User, config.Passwd, config.DBName = "kelvo_reader", input.MySQLPassword, "rabbit_native"
	config.Net, config.Addr = "tcp", input.Claims.Authority
	config.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ServerName: host, RootCAs: roots}
	config.Timeout, config.ReadTimeout, config.WriteTimeout = 5*time.Second, 30*time.Second, 5*time.Second
	config.DialFunc = session.DataDialer().DialContext
	config.Logger = nativeMySQLLogger{}
	connector, err := mysql.NewConnector(config)
	if err != nil {
		t.Fatal("cannot construct native MySQL connector")
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	err = db.PingContext(ctx)
	switch input.Mode {
	case "denied":
		if err == nil || opens.Load() != 0 {
			t.Fatal("denied scope reached a native MySQL connection")
		}
	case "hostname":
		var hostnameError x509.HostnameError
		if !errors.As(err, &hostnameError) || opens.Load() != 1 {
			t.Fatal("native MySQL did not verify original source hostname")
		}
	case "rows":
		if err != nil {
			t.Fatal("native MySQL connection through Rabbit failed")
		}
		verifyNativeMySQLRows(t, ctx, db)
		if opens.Load() != 1 {
			t.Fatal("native MySQL row stream unexpectedly reopened its source")
		}
	default:
		t.Fatal("unsupported native MySQL qualification mode")
	}
	result := nativeHelperResult{Version: 1, Mode: input.Mode, Opens: opens.Load()}
	if input.Mode == "rows" {
		result.Rows = 100000
	}
	encoded, _ := json.Marshal(result)
	fmt.Println("KELVO_NATIVE_RESULT:" + string(encoded))
}

// Database-driver diagnostics are deliberately not emitted by this fixture;
// its assertions return fixed messages and no source credentials or payloads.
type nativeMySQLLogger struct{}

func (nativeMySQLLogger) Print(...any) {}

func verifyNativeMySQLRows(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var tlsName, tlsVersion string
	if db.QueryRowContext(ctx, `SHOW SESSION STATUS LIKE 'Ssl_version'`).Scan(&tlsName, &tlsVersion) != nil || tlsName != "Ssl_version" || tlsVersion != "TLSv1.3" {
		t.Fatal("native MySQL source did not negotiate TLS 1.3")
	}
	var count, sum int64
	err := db.QueryRowContext(ctx, `WITH grouped AS (SELECT category,count(*) AS n,sum(id) AS s FROM private_connect_rows GROUP BY category)
SELECT sum(n),sum(s) FROM grouped`).Scan(&count, &sum)
	if err != nil || count != 100000 || sum != 5000050000 {
		t.Fatal("native MySQL CTE changed result content")
	}
	// The fixture account has SELECT only. Prove its actual authority without
	// changing rows even if a mistaken grant made this statement admissible.
	_, err = db.ExecContext(ctx, `UPDATE private_connect_rows SET category=category WHERE 1=0`)
	var denied *mysql.MySQLError
	if !errors.As(err, &denied) || denied.Number != 1142 {
		t.Fatal("native MySQL fixture account is not SELECT-only")
	}
	rows, err := db.QueryContext(ctx, `SELECT id,category,note FROM private_connect_rows ORDER BY id`)
	if err != nil {
		t.Fatal("native MySQL row stream failed")
	}
	defer rows.Close()
	var seen int64
	for rows.Next() {
		var id, category int64
		var note sql.NullString
		if rows.Scan(&id, &category, &note) != nil {
			t.Fatal("native MySQL row decode failed")
		}
		seen++
		if id != seen || category != id%17 || note.Valid != (id%13 != 0) || note.Valid && note.String != "row-"+strconv.FormatInt(id, 10) {
			t.Fatal("native MySQL row content changed")
		}
	}
	if rows.Err() != nil || seen != 100000 {
		t.Fatal("native MySQL row stream was incomplete")
	}
}
