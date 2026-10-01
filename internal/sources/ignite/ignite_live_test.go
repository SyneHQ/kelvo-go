// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package ignite

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func liveEngine(t *testing.T) *Engine {
	t.Helper()
	path := os.Getenv("KELVO_TEST_IGNITE_CONFIG")
	if path == "" {
		t.Skip("run scripts/ignite_acceptance.py for live Ignite 2")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("live fixture configuration unavailable")
	}
	var cfg struct{ URL, Username, Password, CA, Cache string }
	if json.Unmarshal(data, &cfg) != nil {
		t.Fatal("invalid live fixture configuration")
	}
	t.Setenv("KELVO_SOURCE_IGNITE_LIVE_URL", cfg.URL)
	t.Setenv("KELVO_SOURCE_IGNITE_LIVE_USER", cfg.Username)
	t.Setenv("KELVO_SOURCE_IGNITE_LIVE_PASSWORD", cfg.Password)
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "grid", Type: "ignite",
		URLEnv: "KELVO_SOURCE_IGNITE_LIVE_URL", UsernameEnv: "KELVO_SOURCE_IGNITE_LIVE_USER",
		PasswordEnv: "KELVO_SOURCE_IGNITE_LIVE_PASSWORD", Options: map[string]string{"cache_name": cfg.Cache}}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	ca, err := os.ReadFile(cfg.CA)
	if err != nil {
		t.Fatal("fixture CA unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid fixture CA")
	}
	e.client.http.Transport.(*http.Transport).TLSClientConfig.RootCAs = roots
	t.Cleanup(func() { e.Close() })
	return e
}

func liveRequest(sql string) query.Request {
	return query.Request{Mode: "native", ConnectionID: "grid", SQL: sql}
}

func TestLiveIgnitePagination(t *testing.T) {
	e := liveEngine(t)
	s := sink(t)
	stats, err := e.Execute(context.Background(), liveRequest("SELECT id, label FROM KELVO_NATIVE_ACCEPTANCE ORDER BY id"), s)
	if err != nil || stats.Rows != 1205 || s.schema == nil || s.schema.NumFields() != 2 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	rows, nulls := 0, 0
	for _, r := range s.records {
		ids, labels := r.Column(0).(*array.Int64), r.Column(1).(*array.String)
		for i := 0; i < ids.Len(); i++ {
			if ids.IsNull(i) || ids.Value(i) != 9007199254740993+int64(rows) {
				t.Fatalf("integer changed at row %d", rows)
			}
			if labels.IsNull(i) {
				nulls++
			}
			rows++
		}
	}
	if rows != 1205 || nulls != 121 {
		t.Fatalf("rows=%d nulls=%d", rows, nulls)
	}
}

func TestLiveIgniteExactTypes(t *testing.T) {
	e := liveEngine(t)
	s := sink(t)
	sql := "SELECT CAST(1234567890123456789.123456789 AS DECIMAL(28,9)) AS amount, " +
		"TIMESTAMP '2026-10-01 12:30:00.123456789' AS created, DATE '1960-01-02' AS birth, " +
		"X'0001FF' AS payload, CAST(NULL AS BIGINT) AS missing, TRUE AS flag, " +
		"TIME '23:59:59' AS clock, CAST(CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) AS VARCHAR) AS ident"
	stats, err := e.Execute(context.Background(), liveRequest(sql), s)
	if err != nil || stats.Rows != 1 || len(s.records) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	r := s.records[0]
	if r.Column(0).(*array.String).Value(0) != "1234567890123456789.123456789" {
		t.Fatal("decimal changed")
	}
	stamp := r.Column(1).(*array.Timestamp)
	if stamp.Value(0).ToTime(arrow.Nanosecond).Format("2006-01-02 15:04:05.999999999") != "2026-10-01 12:30:00.123456789" || s.schema.Field(1).Type.(*arrow.TimestampType).TimeZone != "" {
		t.Fatal("timestamp changed or acquired timezone")
	}
	if r.Column(2).(*array.Date32).Value(0).ToTime().Format("2006-01-02") != "1960-01-02" {
		t.Fatal("date changed")
	}
	if string(r.Column(3).(*array.Binary).Value(0)) != string([]byte{0, 1, 255}) || !r.Column(4).IsNull(0) || !r.Column(5).(*array.Boolean).Value(0) {
		t.Fatal("binary, NULL, or boolean changed")
	}
	if r.Column(6).(*array.String).Value(0) != "23:59:59" || r.Column(7).(*array.String).Value(0) != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatal("time or UUID changed")
	}
}

func TestLiveIgniteUUIDMetadataRefusal(t *testing.T) {
	e := liveEngine(t)
	s := sink(t)
	_, err := e.Execute(context.Background(), liveRequest("SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) AS ident"), s)
	if err == nil || query.PublicError(err).Code != "UNSUPPORTED" || len(s.records) != 0 || s.schema == nil || s.schema.Field(0).Type.ID() != arrow.BINARY {
		t.Fatalf("UUID/binary ambiguity silently accepted: %v", err)
	}
}

func TestLiveIgniteEmptyResult(t *testing.T) {
	e := liveEngine(t)
	s := sink(t)
	stats, err := e.Execute(context.Background(), liveRequest("SELECT id, label FROM KELVO_NATIVE_ACCEPTANCE WHERE 1=0"), s)
	if err != nil || stats.Rows != 0 || s.schema == nil || s.schema.NumFields() != 2 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}

func TestLiveIgniteFailureCleanup(t *testing.T) {
	e := liveEngine(t)
	e.limits.MaxRows = 10
	_, err := e.Execute(context.Background(), liveRequest("SELECT id FROM KELVO_NATIVE_ACCEPTANCE"), sink(t))
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("row limit err=%v", err)
	}
	e.limits = query.DefaultLimits()
	s := sink(t)
	s.schemaErr = errors.New("fixture rejects schema")
	_, err = e.Execute(context.Background(), liveRequest("SELECT id FROM KELVO_NATIVE_ACCEPTANCE"), s)
	if !errors.Is(err, s.schemaErr) {
		t.Fatalf("sink failure err=%v", err)
	}
	e.client.password = "deliberately-invalid-fixture-password"
	_, err = e.Execute(context.Background(), liveRequest("SELECT id FROM KELVO_NATIVE_ACCEPTANCE"), sink(t))
	if err == nil || query.PublicError(err).Code != "QUERY_FAILED" {
		t.Fatalf("authentication failure err=%v", err)
	}
}
