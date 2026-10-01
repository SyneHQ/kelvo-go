// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package dynamodb

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func liveEngine(t *testing.T, trust bool) *Engine {
	t.Helper()
	path := os.Getenv("KELVO_TEST_DYNAMODB_CONFIG")
	if path == "" {
		t.Skip("run scripts/dynamodb_acceptance.py on the designated VM")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("fixture config unavailable")
	}
	var config struct{ URL, ID, Secret, CA string }
	if json.Unmarshal(raw, &config) != nil {
		t.Fatal("invalid fixture config")
	}
	t.Setenv("KELVO_SOURCE_DDB_LIVE_URL", config.URL)
	t.Setenv("KELVO_SOURCE_DDB_LIVE_ID", config.ID)
	t.Setenv("KELVO_SOURCE_DDB_LIVE_SECRET", config.Secret)
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "live", Type: "dynamodb", URLEnv: "KELVO_SOURCE_DDB_LIVE_URL", UsernameEnv: "KELVO_SOURCE_DDB_LIVE_ID", PasswordEnv: "KELVO_SOURCE_DDB_LIVE_SECRET", Options: map[string]string{"region": "us-east-1"}}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if trust {
		ca, err := os.ReadFile(config.CA)
		if err != nil {
			t.Fatal("fixture CA unavailable")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(ca) {
			t.Fatal("invalid fixture CA")
		}
		// Test-only trust injection keeps production certificate verification intact.
		e.c.HTTP.Transport.(*http.Transport).TLSClientConfig.RootCAs = roots
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}
func liveRequest(sql string) query.Request {
	return query.Request{SQL: sql, Mode: "native", ConnectionID: "live"}
}
func TestLiveDynamoDBTaggedTypesAndExactNumbers(t *testing.T) {
	e := liveEngine(t, true)
	sink := &capture{}
	stats, err := e.Execute(context.Background(), liveRequest(`SELECT * FROM "kelvo_native_types" WHERE pk='typed'`), sink)
	if err != nil || stats.Rows != 1 || len(sink.documents) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	var item map[string]map[string]json.RawMessage
	if json.Unmarshal([]byte(sink.documents[0]), &item) != nil {
		t.Fatal("invalid tagged output")
	}
	for key, tag := range map[string]string{"pk": "S", "large": "N", "decimal": "N", "nil": "NULL", "flag": "BOOL", "binary": "B", "text": "S", "strings": "SS", "numbers": "NS", "binaries": "BS", "list": "L", "map": "M"} {
		if len(item[key]) != 1 || item[key][tag] == nil {
			t.Fatalf("type tag lost for %s", key)
		}
	}
	if string(item["large"]["N"]) != `"9007199254740993"` || string(item["decimal"]["N"]) != `"12345678901234567890123456789.123456789"` || string(item["nil"]["NULL"]) != "true" || string(item["flag"]["BOOL"]) != "false" || string(item["binary"]["B"]) != `"AP8="` || string(item["text"]["S"]) != `""` {
		t.Fatal("exact scalar data lost")
	}
}
func TestLiveDynamoDBPagination(t *testing.T) {
	e := liveEngine(t, true)
	sink := &capture{}
	stats, err := e.Execute(context.Background(), liveRequest(`SELECT * FROM "kelvo_native_pages"`), sink)
	if err != nil || stats.Rows != 1205 || len(sink.documents) != 1205 {
		t.Fatalf("rows=%d stats=%+v err=%v", len(sink.documents), stats, err)
	}
	seen := map[string]bool{}
	for _, document := range sink.documents {
		var item struct {
			PK    struct{ S string } `json:"pk"`
			Large struct{ N string } `json:"large"`
		}
		if json.Unmarshal([]byte(document), &item) != nil || seen[item.PK.S] {
			t.Fatal("invalid or repeated page item")
		}
		seen[item.PK.S] = true
		number, err := strconv.ParseInt(item.Large.N, 10, 64)
		if err != nil || number < 9007199254740993 || number > 9007199254742197 {
			t.Fatal("integer precision lost")
		}
	}
}
func TestLiveDynamoDBEmptyFilteredPages(t *testing.T) {
	e := liveEngine(t, true)
	sink := &capture{}
	stats, err := e.Execute(context.Background(), liveRequest(`SELECT * FROM "kelvo_native_pages" WHERE kind='absent'`), sink)
	if err != nil || stats.Rows != 0 || sink.schema == nil || len(sink.documents) != 0 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}
func TestLiveDynamoDBRestrictionsAndLimits(t *testing.T) {
	e := liveEngine(t, true)
	for _, sql := range []string{`DELETE FROM "kelvo_native_types" WHERE pk='typed'`, `SELECT * FROM "kelvo_native_types"; SELECT * FROM "kelvo_native_pages"`} {
		if _, err := e.Execute(context.Background(), liveRequest(sql), &capture{}); err == nil {
			t.Fatal("write or multiple statements accepted")
		}
	}
	r := liveRequest(`SELECT * FROM "kelvo_native_types" WHERE pk=?`)
	r.Parameters = []query.Parameter{{Type: "string", Value: json.RawMessage(`"typed"`)}}
	if _, err := e.Execute(context.Background(), r, &capture{}); err == nil || query.PublicError(err).Code != "UNSUPPORTED" {
		t.Fatal("parameters were not explicitly rejected")
	}
	r = liveRequest(`SELECT * FROM "kelvo_native_types"`)
	r.ConnectionID = "other"
	if _, err := e.Execute(context.Background(), r, &capture{}); err == nil {
		t.Fatal("source binding ignored")
	}
	e.l.MaxRows = 10
	if _, err := e.Execute(context.Background(), liveRequest(`SELECT * FROM "kelvo_native_pages"`), &capture{}); err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("row limit err=%v", err)
	}
}
func TestLiveDynamoDBRequiresVerifiedTLS(t *testing.T) {
	e := liveEngine(t, false)
	if _, err := e.Execute(context.Background(), liveRequest(`SELECT * FROM "kelvo_native_types"`), &capture{}); err == nil {
		t.Fatal("untrusted fixture certificate accepted")
	}
}
