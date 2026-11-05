// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package clickhouse

import (
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestSourceSchemes(t *testing.T) {
	limits := query.Limits{MaxRows: 10, MaxBytes: 1 << 20, Timeout: time.Second, MemoryMB: 64, Threads: 1, MaxTempMB: 1}
	for _, tc := range []struct {
		name, url  string
		options    map[string]string
		federation bool
		ok         bool
	}{
		{"https", "https://ch.example:8443/?database=db", nil, false, true},
		{"clickhouses", "clickhouses://ch.example:9440?database=db", nil, false, true},
		{"clickhouses_no_port", "clickhouses://ch.example?database=db", nil, false, false},
		{"clickhouses_path", "clickhouses://ch.example:9440/x?database=db", nil, false, false},
		{"clickhouse_plain", "clickhouse://ch.example:9000?database=db", nil, false, false},
		{"clickhouses_compression", "clickhouses://ch.example:9440?database=db", map[string]string{"arrow_compression": "none"}, false, false},
		{"clickhouses_federation", "clickhouses://ch.example:9440?database=db", nil, true, false},
		{"clickhouses_bad_ca", "clickhouses://ch.example:9440?database=db", map[string]string{"tls_ca_pem": "junk"}, false, false},
	} {
		t.Setenv("KELVO_TEST_SCHEME_URL", tc.url)
		source := catalog.Source{ID: "s", Type: "clickhouse", URLEnv: "KELVO_TEST_SCHEME_URL", Options: tc.options}
		if tc.federation {
			source.Federation = &catalog.FederationConfig{}
		}
		_, err := New(catalog.Config{Sources: []catalog.Source{source}}, limits)
		if (err == nil) != tc.ok {
			t.Fatalf("%s: New err=%v", tc.name, err)
		}
		_, err = NewResolved(ResolvedSource{ID: "s", URL: tc.url, Username: "u", Password: "p", Compression: tc.options["arrow_compression"]}, limits)
		if wantResolved := tc.ok || tc.name == "clickhouses_federation" || tc.name == "clickhouses_bad_ca" || tc.name == "clickhouses_compression"; (err == nil) != wantResolved {
			t.Fatalf("%s: NewResolved err=%v", tc.name, err)
		}
	}
	if _, err := NewResolved(ResolvedSource{ID: "s", URL: "clickhouses://ch.example:9440?database=db", Username: "u", Password: "p", Compression: "lz4_frame"}, limits); err == nil {
		t.Fatal("arrow_compression accepted with clickhouses")
	}
}

func TestNativeTypeMapping(t *testing.T) {
	for name, want := range map[string]string{
		"Int32": "int32", "UInt64": "uint64", "Nullable(String)": "utf8", "LowCardinality(Nullable(String))": "utf8",
		"FixedString(4)": "binary", "Date32": "date32", "DateTime": "timestamp[s, tz=UTC]", "DateTime('Asia/Kolkata')": "timestamp[s, tz=Asia/Kolkata]",
		"DateTime64(3)": "timestamp[ms, tz=UTC]", "DateTime64(9, 'UTC')": "timestamp[ns, tz=UTC]", "Decimal(10, 2)": "decimal(10, 2)",
		"Decimal(50, 2)": "utf8", "UUID": "utf8", "Array(Int32)": "utf8", "Bool": "bool",
	} {
		if got, _, _ := nativeType(name); got.String() != want {
			t.Errorf("%s: got %s want %s", name, got, want)
		}
	}
}
