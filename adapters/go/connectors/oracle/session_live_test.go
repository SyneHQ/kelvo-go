// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package oracle

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// This check uses the existing isolated OOS fixture and never changes its data.
func TestOracleVerifiedTLSLifecycleLive(t *testing.T) {
	directory := os.Getenv("KELVO_ORACLE_FIXTURE_DIR")
	if directory == "" {
		t.Skip("KELVO_ORACLE_FIXTURE_DIR is not configured")
	}
	raw, err := os.ReadFile(filepath.Join(directory, "credentials.json"))
	if err != nil {
		t.Fatal("fixture credentials are unavailable")
	}
	var credentials map[string]string
	if json.Unmarshal(raw, &credentials) != nil || len(credentials["oracle"]) != 48 {
		t.Fatal("the unchanged 48-byte Oracle fixture password is required")
	}
	pem, err := os.ReadFile(filepath.Join(directory, "tls", "ca.crt"))
	if err != nil {
		t.Fatal("fixture CA is unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("fixture CA is invalid")
	}
	connection := adapter.Connection{Engine: "oracle", TenantID: "fixture", ConnectionID: "oracle-fixture", Revision: "1",
		Host: "127.0.0.1", Port: 52484, Namespace: "FREEPDB1", Schema: "OOS", Username: "OOS", Password: credentials["oracle"],
		TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "127.0.0.1"}}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	opened, err := (Driver{}).Open(ctx, connection)
	if err != nil {
		t.Fatalf("verified long-password connection failed (%T)", err)
	}
	defer opened.Close()
	session := opened.(*Session)
	for round := range 3 {
		sink := &oracleSink{}
		t.Cleanup(sink.close)
		stats, err := session.Query(ctx, adapter.Query{Statement: `SELECT id,balance,created_at,note FROM "OOS"."ACCOUNTS" WHERE id=9007199254740993`, MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 2}, sink)
		if err != nil {
			public := query.PublicError(err)
			t.Fatalf("fixture read %d failed: %s: %s", round, public.Code, public.Message)
		}
		if stats.Rows != 1 || len(sink.records) != 1 || sink.records[0].NumCols() != 4 {
			t.Fatal("fixture row shape changed")
		}
		row := sink.records[0]
		if row.Column(0).ValueStr(0) != "9007199254740993" || row.Column(1).ValueStr(0) != "1234567890123456.12345678" || !row.Column(3).IsNull(0) {
			t.Fatal("fixture integer, decimal, or NULL value changed")
		}
		stamp, ok := row.Column(2).(*array.Timestamp)
		if !ok || int64(stamp.Value(0))%1_000_000_000 != 123456000 {
			t.Fatal("fixture timestamp precision changed")
		}
		if err := session.Test(ctx); err != nil {
			t.Fatalf("replacement connection %d failed (%T)", round, err)
		}
	}
	for _, object := range []string{"tables", "columns", "primary_keys", "foreign_keys"} {
		sink := &oracleSink{}
		t.Cleanup(sink.close)
		stats, err := session.Inspect(ctx, operations.MetadataSpec{Object: object, Target: operations.ObjectRef{Catalog: "FREEPDB1", Schema: "OOS"}, Limit: 100}, adapter.Limits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 10}, sink)
		if err != nil {
			public := query.PublicError(err)
			t.Fatalf("fixture %s metadata failed: %s: %s", object, public.Code, public.Message)
		}
		if stats.Rows == 0 {
			t.Fatalf("fixture %s metadata is empty", object)
		}
	}
	wrongPassword := connection
	wrongPassword.Password = "incorrect-fixture-password"
	if unexpected, err := (Driver{}).Open(ctx, wrongPassword); err == nil {
		_ = unexpected.Close()
		t.Fatal("Oracle accepted an incorrect password")
	}
	wrongName := connection
	wrongName.TLS = connection.TLS.Clone()
	wrongName.TLS.ServerName = "wrong-name.invalid"
	if unexpected, err := (Driver{}).Open(ctx, wrongName); err == nil {
		_ = unexpected.Close()
		t.Fatal("Oracle accepted an incorrect TLS name")
	}
	wrongTrust := connection
	wrongTrust.TLS = connection.TLS.Clone()
	wrongTrust.TLS.RootCAs = x509.NewCertPool()
	if unexpected, err := (Driver{}).Open(ctx, wrongTrust); err == nil {
		_ = unexpected.Close()
		t.Fatal("Oracle accepted an untrusted TLS certificate")
	}
}
