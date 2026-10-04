// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func authorityFixture() Config {
	return Config{ExtensionDirectory: "/registered/extensions", Sources: []Source{
		{ID: "beta", Type: "postgres", DSNEnv: "KELVO_SOURCE_BETA_DSN"},
		{ID: "alpha", Type: "clickhouse", URLEnv: "KELVO_SOURCE_ALPHA_URL", UsernameEnv: "KELVO_SOURCE_ALPHA_USER", PasswordEnv: "KELVO_SOURCE_ALPHA_PASSWORD",
			Options: map[string]string{"arrow_compression": "lz4_frame"}, Federation: &FederationConfig{MaxScanRows: 100, MaxScanBytes: 4096, Tables: []FederationTable{
				{Name: "orders", Database: "reports", Table: "orders"}, {Name: "accounts", Database: "reports", Table: "accounts"},
			}}},
	}, Acceleration: &AccelerationConfig{Directory: "/registered/snapshots", TenantID: "tenant-a", Datasets: []Dataset{
		{ID: "recent", Query: query.Request{Mode: "federated", Sources: []string{"alpha", "beta"}, SQL: "SELECT 1"},
			RefreshInterval: time.Minute, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits(),
			Scan: &SnapshotScanLimits{MaxRows: 10, MaxBytes: 4096}, SchemaEvolution: &SchemaEvolution{AddNullableColumns: true}, Multipart: &MultipartConfig{MaxParts: 2, MaxPartBytes: 1 << 20}},
		{ID: "archive", Query: query.Request{Mode: "native", ConnectionID: "alpha", SQL: "SELECT 2"}, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits()},
	}, ObjectStorage: &ObjectStorage{ObjectLocation: ObjectLocation{Provider: "s3", Endpoint: "https://storage.example.invalid", Bucket: "fixture-bucket", Prefix: "snapshots", Region: "us-east-1"},
		ReadCredentials:  ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_READ_ID", SecretAccessKeyEnv: "KELVO_SOURCE_READ_KEY"},
		WriteCredentials: ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_WRITE_ID", SecretAccessKeyEnv: "KELVO_SOURCE_WRITE_KEY"}}}}
}

func TestAuthorityCanonicalDigestPreservesExecutionAndSnapshotOrdering(t *testing.T) {
	c := authorityFixture()
	snapshot, digest, err := AuthoritySnapshot(c)
	if err != nil || !reflect.DeepEqual(snapshot, c) {
		t.Fatalf("definition order or representation changed: %v", err)
	}
	reordered := authorityFixture()
	slices.Reverse(reordered.Sources)
	slices.Reverse(reordered.Acceleration.Datasets)
	for _, s := range reordered.Sources {
		if s.Federation != nil {
			slices.Reverse(s.Federation.Tables)
		}
	}
	reordered.Sources[1].Type = "postgresql"
	other, err := AuthorityFingerprint(reordered)
	if err != nil || other != digest {
		t.Fatalf("unordered definitions changed authority: %v", err)
	}
	for _, d := range c.Acceleration.Datasets {
		before, err := c.DatasetFingerprint(d.ID)
		after, copiedErr := snapshot.DatasetFingerprint(d.ID)
		if err != nil || copiedErr != nil || before != after {
			t.Fatal("authority binding changed existing snapshot fingerprints", err, copiedErr)
		}
	}
	before := snapshot.Sources[1].Federation.Tables[0].Table
	c.Sources[1].Federation.Tables[0].Table = "replaced"
	c.Sources[1].Options["arrow_compression"] = "none"
	c.Acceleration.Datasets[0].Query.Sources[0] = "replaced"
	c.Acceleration.Datasets[0].Scan.MaxRows = 999
	c.Acceleration.Datasets[0].SchemaEvolution.SafeWidening = true
	c.Acceleration.Datasets[0].Multipart.MaxParts = 9
	c.Acceleration.ObjectStorage.ReadCredentials.AccessKeyIDEnv = "KELVO_SOURCE_REPLACED"
	if snapshot.Sources[1].Federation.Tables[0].Table != before {
		t.Fatal("nested table slice remains aliased")
	}
	if got, err := AuthorityFingerprint(snapshot); err != nil || got != digest {
		t.Fatal("caller mutations changed detached authority", err)
	}
}

func TestAuthorityDigestBindsExecutionAffectingDefinitions(t *testing.T) {
	base, err := AuthorityFingerprint(authorityFixture())
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*Config){
		"source identity":    func(c *Config) { c.Sources[0].ID = "other" },
		"source type":        func(c *Config) { c.Sources[0].Type = "mysql" },
		"adapter":            func(c *Config) { c.Sources[0].Adapter = "dbapi" },
		"source path":        func(c *Config) { c.Sources[0].Path = "/registered/other" },
		"dsn reference":      func(c *Config) { c.Sources[0].DSNEnv = "KELVO_SOURCE_OTHER_DSN" },
		"url reference":      func(c *Config) { c.Sources[1].URLEnv = "KELVO_SOURCE_OTHER_URL" },
		"username reference": func(c *Config) { c.Sources[1].UsernameEnv = "KELVO_SOURCE_OTHER_USER" },
		"password reference": func(c *Config) { c.Sources[1].PasswordEnv = "KELVO_SOURCE_OTHER_PASSWORD" },
		"token reference":    func(c *Config) { c.Sources[1].TokenEnv = "KELVO_SOURCE_OTHER_TOKEN" },
		"options":            func(c *Config) { c.Sources[1].Options["arrow_compression"] = "none" },
		"table alias":        func(c *Config) { c.Sources[1].Federation.Tables[0].Name = "other" },
		"database":           func(c *Config) { c.Sources[1].Federation.Tables[0].Database = "other" },
		"schema":             func(c *Config) { c.Sources[1].Federation.Tables[0].Schema = "other" },
		"table":              func(c *Config) { c.Sources[1].Federation.Tables[0].Table = "other" },
		"source row limit":   func(c *Config) { c.Sources[1].Federation.MaxScanRows++ },
		"source byte limit":  func(c *Config) { c.Sources[1].Federation.MaxScanBytes++ },
		"extensions":         func(c *Config) { c.ExtensionDirectory = "/registered/other" },
		"snapshot root":      func(c *Config) { c.Acceleration.Directory = "/registered/other" },
		"tenant":             func(c *Config) { c.Acceleration.TenantID = "tenant-b" },
		"dataset identity":   func(c *Config) { c.Acceleration.Datasets[0].ID = "other" },
		"dataset sql":        func(c *Config) { c.Acceleration.Datasets[0].Query.SQL = "SELECT 3" },
		"query mode":         func(c *Config) { c.Acceleration.Datasets[0].Query.Mode = "native" },
		"query connection":   func(c *Config) { c.Acceleration.Datasets[0].Query.ConnectionID = "beta" },
		"query source order": func(c *Config) { slices.Reverse(c.Acceleration.Datasets[0].Query.Sources) },
		"query diagnostics":  func(c *Config) { c.Acceleration.Datasets[0].Query.ScanDiagnostics = true },
		"refresh interval":   func(c *Config) { c.Acceleration.Datasets[0].RefreshInterval++ },
		"freshness":          func(c *Config) { c.Acceleration.Datasets[0].MaxAge++ },
		"authorization":      func(c *Config) { c.Acceleration.Datasets[0].AuthorizationVersion = "v2" },
		"scan rows":          func(c *Config) { c.Acceleration.Datasets[0].Scan.MaxRows++ },
		"scan bytes":         func(c *Config) { c.Acceleration.Datasets[0].Scan.MaxBytes++ },
		"nullable evolution": func(c *Config) { c.Acceleration.Datasets[0].SchemaEvolution.AddNullableColumns = false },
		"widening evolution": func(c *Config) { c.Acceleration.Datasets[0].SchemaEvolution.SafeWidening = true },
		"part count":         func(c *Config) { c.Acceleration.Datasets[0].Multipart.MaxParts++ },
		"part bytes":         func(c *Config) { c.Acceleration.Datasets[0].Multipart.MaxPartBytes++ },
		"query max rows":     func(c *Config) { c.Acceleration.Datasets[0].Limits.MaxRows++ },
		"query max bytes":    func(c *Config) { c.Acceleration.Datasets[0].Limits.MaxBytes++ },
		"query timeout":      func(c *Config) { c.Acceleration.Datasets[0].Limits.Timeout++ },
		"query memory":       func(c *Config) { c.Acceleration.Datasets[0].Limits.MemoryMB++ },
		"query threads":      func(c *Config) { c.Acceleration.Datasets[0].Limits.Threads++ },
		"query scratch":      func(c *Config) { c.Acceleration.Datasets[0].Limits.MaxTempMB++ },
		"query compression":  func(c *Config) { c.Acceleration.Datasets[0].Limits.ResultCompression = "lz4_frame" },
		"storage endpoint":   func(c *Config) { c.Acceleration.ObjectStorage.Endpoint = "https://other.example.invalid" },
		"storage bucket":     func(c *Config) { c.Acceleration.ObjectStorage.Bucket = "other-bucket" },
		"storage namespace":  func(c *Config) { c.Acceleration.ObjectStorage.Prefix = "other" },
		"storage region":     func(c *Config) { c.Acceleration.ObjectStorage.Region = "us-east-2" },
		"read credential": func(c *Config) {
			c.Acceleration.ObjectStorage.ReadCredentials.SecretAccessKeyEnv = "KELVO_SOURCE_OTHER_READ"
		},
		"write credential": func(c *Config) {
			c.Acceleration.ObjectStorage.WriteCredentials.SecretAccessKeyEnv = "KELVO_SOURCE_OTHER_WRITE"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := authorityFixture()
			mutate(&c)
			got, err := AuthorityFingerprint(c)
			if err != nil || got == base {
				t.Fatalf("changed definition did not receive a distinct digest: %v", err)
			}
		})
	}
}

func TestAuthorityRejectsCapabilitiesAmbiguityAndUnboundedDefinitions(t *testing.T) {
	cases := map[string]func(*Config){
		"local snapshot":           func(c *Config) { c.Sources[0].LocalSnapshot = &LocalSnapshotRead{} },
		"object snapshot":          func(c *Config) { c.Sources[0].ObjectSnapshot = &ObjectSnapshotRead{} },
		"object read":              func(c *Config) { c.Sources[0].Object = &ObjectRead{} },
		"range":                    func(c *Config) { c.Sources[0].Range = &ObjectRange{} },
		"ranges even empty":        func(c *Config) { c.Sources[0].Ranges = []ObjectRange{} },
		"paths even empty":         func(c *Config) { c.Sources[0].ParquetPaths = []string{} },
		"unresolved runtime alias": func(c *Config) { c.Sources[0].Type = "accelerated" },
		"source count":             func(c *Config) { c.Sources = make([]Source, 257) },
		"duplicate source":         func(c *Config) { c.Sources[0].ID = "alpha" },
		"case source":              func(c *Config) { c.Sources[0].ID = "ALPHA" },
		"dataset count":            func(c *Config) { c.Acceleration.Datasets = make([]Dataset, 65) },
		"dataset and source":       func(c *Config) { c.Acceleration.Datasets[0].ID = "ALPHA" },
		"case datasets":            func(c *Config) { c.Acceleration.Datasets[0].ID = "ARCHIVE" },
		"table count":              func(c *Config) { c.Sources[1].Federation.Tables = make([]FederationTable, 33) },
		"case tables":              func(c *Config) { c.Sources[1].Federation.Tables[0].Name = "ACCOUNTS" },
		"relative path":            func(c *Config) { c.Sources[0].Path = "private-relative-file" },
		"dirty path":               func(c *Config) { c.Sources[0].Path = "/private/../other" },
		"oversized option":         func(c *Config) { c.Sources[1].Options["x"] = strings.Repeat("x", 4097) },
		"invalid utf8":             func(c *Config) { c.Sources[1].Options["x"] = string([]byte{0xff}) },
		"oversized sql":            func(c *Config) { c.Acceleration.Datasets[0].Query.SQL = strings.Repeat("x", 64<<10+1) },
		"unbound parameters":       func(c *Config) { c.Acceleration.Datasets[0].Query.Parameters = []query.Parameter{{Type: "null"}} },
		"ambient secret":           func(c *Config) { c.Sources[0].DSNEnv = "AWS_SECRET_ACCESS_KEY" },
		"inline credential":        func(c *Config) { c.Sources[0].DSNEnv = "private-inline-value" },
		"mongo stage count": func(c *Config) {
			c.Acceleration.Datasets[0].Query.Mongo = &query.MongoRequest{Pipeline: make([]json.RawMessage, 129)}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := authorityFixture()
			mutate(&c)
			_, digest, err := AuthoritySnapshot(c)
			if err == nil || digest != "" || strings.Contains(err.Error(), "private") {
				t.Fatalf("invalid authority accepted or leaked: %v", err)
			}
		})
	}
	// Bounded raw text can still expand beyond the encoded document limit.
	c := Config{}
	for _, id := range []string{"one", "two", "three"} {
		s := Source{ID: id, Type: "clickhouse", Options: map[string]string{}}
		for _, key := range strings.Split("a b c d e f g h i j k l m n o p", " ") {
			s.Options[key] = strings.Repeat("\x00", 4096)
		}
		c.Sources = append(c.Sources, s)
	}
	if _, err := AuthorityFingerprint(c); err == nil {
		t.Fatal("JSON escaping exceeded the authority document bound")
	}
}

func TestAuthorityUsesVersionedDomainAndNeverResolvesSecretValues(t *testing.T) {
	want := sha256.Sum256([]byte("kelvo/catalog-authority/v1\n{\"sources\":[]}"))
	for _, c := range []Config{{}, {Sources: []Source{}}} {
		got, err := AuthorityFingerprint(c)
		if err != nil || got != hex.EncodeToString(want[:]) {
			t.Fatalf("v1 domain or empty definition canonicalization changed: %v", err)
		}
	}
	c := Config{Sources: []Source{{ID: "source", Type: "csv", Path: "/does-not-exist/registered.csv"}}}
	if _, err := AuthorityFingerprint(c); err != nil {
		t.Fatal("fingerprinting attempted file resolution", err)
	}
	c = authorityFixture()
	before, _ := AuthorityFingerprint(c)
	t.Setenv("KELVO_SOURCE_ALPHA_URL", "private-first-secret-value")
	first, err := AuthorityFingerprint(c)
	t.Setenv("KELVO_SOURCE_ALPHA_URL", "private-second-secret-value")
	second, secondErr := AuthorityFingerprint(c)
	if err != nil || secondErr != nil || before != first || first != second {
		t.Fatal("resolved secret values affected definition authority", err, secondErr)
	}
}

func TestAuthorityMongoStagesRemainOrderedAndDetached(t *testing.T) {
	c := authorityFixture()
	c.Acceleration.Datasets[0].Query = query.Request{Mode: "native", ConnectionID: "beta", Mongo: &query.MongoRequest{Collection: "orders", Pipeline: []json.RawMessage{json.RawMessage(`{"$match":{"tenant":7}}`), json.RawMessage(`{"$limit":1}`)}}}
	snapshot, before, err := AuthoritySnapshot(c)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(c.Acceleration.Datasets[0].Query.Mongo.Pipeline)
	after, err := AuthorityFingerprint(c)
	if err != nil || before == after {
		t.Fatal("Mongo stage order was discarded", err)
	}
	c.Acceleration.Datasets[0].Query.Mongo.Pipeline[0][0] = ' '
	if got, err := AuthorityFingerprint(snapshot); err != nil || got != before {
		t.Fatal("raw Mongo bytes remain aliased", err)
	}
}
