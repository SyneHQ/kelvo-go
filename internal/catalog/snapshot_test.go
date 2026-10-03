// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func snapshotSource() Source {
	return Source{ID: "orders_fast", Type: "parquet", Path: "/private/orders.parquet", LocalSnapshot: &LocalSnapshotRead{
		Dataset: "orders_fast", Generation: strings.Repeat("a", 32), SchemaSHA256: strings.Repeat("b", 64),
		Parts: []LocalSnapshotPart{{Rows: 1_000_000, Bytes: 5 << 20, SHA256: strings.Repeat("c", 64)}},
		Scan:  SnapshotScanLimits{MaxRows: 1_000_000, MaxBytes: 256 << 20},
	}}
}

func TestSnapshotReadEnvelopeIsBoundedAndJSONOnly(t *testing.T) {
	source := snapshotSource()
	if err := source.ValidateLocalSnapshot(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Source
	if err := json.Unmarshal(raw, &decoded); err != nil || decoded.ValidateLocalSnapshot() != nil || decoded.LocalSnapshot.Generation != source.LocalSnapshot.Generation {
		t.Fatal("trusted generation did not survive worker envelope", err)
	}
	dir := t.TempDir()
	data := filepath.Join(dir, "data.parquet")
	if err := os.WriteFile(data, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "catalog.yml")
	if err := os.WriteFile(config, []byte("sources:\n  - id: orders_fast\n    type: parquet\n    path: data.parquet\n    local_snapshot: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(config); err == nil {
		t.Fatal("YAML supplied trusted generation provenance")
	}
}

func TestSnapshotReadRejectsConfusedProvenance(t *testing.T) {
	for _, mutate := range []func(*Source){
		func(s *Source) { s.Type = "postgres" },
		func(s *Source) { s.LocalSnapshot.Dataset = "other" },
		func(s *Source) { s.LocalSnapshot.Generation = "invalid" },
		func(s *Source) { s.LocalSnapshot.SchemaSHA256 = "invalid" },
		func(s *Source) { s.LocalSnapshot.Parts[0].SHA256 = "invalid" },
		func(s *Source) { s.LocalSnapshot.Parts[0].Rows = -1 },
		func(s *Source) { s.LocalSnapshot.Parts[0].Bytes = 0 },
		func(s *Source) { s.LocalSnapshot.Parts = append(s.LocalSnapshot.Parts, s.LocalSnapshot.Parts[0]) },
		func(s *Source) { s.LocalSnapshot.Scan.MaxRows = 0 },
		func(s *Source) { s.Path = "relative.parquet" },
		func(s *Source) { s.Path = "/private/*.parquet" },
		func(s *Source) { s.Path = "https://private/data.parquet" },
		func(s *Source) { s.ParquetPaths = []string{s.Path} },
		func(s *Source) { s.TokenEnv = "PRIVATE_TOKEN" },
		func(s *Source) { s.Federation = &FederationConfig{} },
		func(s *Source) { s.Range = &ObjectRange{} },
	} {
		source := snapshotSource()
		mutate(&source)
		if source.ValidateLocalSnapshot() == nil {
			t.Fatal("confused snapshot source accepted")
		}
		if _, err := (Config{Sources: []Source{source}}).Select([]string{source.ID}); err == nil {
			t.Fatal("selection omitted provenance validation")
		}
	}
	source := snapshotSource()
	source.LocalSnapshot.SchemaSHA256 = ""
	if err := source.ValidateLocalSnapshot(); err != nil {
		t.Fatal("schema-less unrestricted legacy generation rejected", err)
	}
	source.ParquetPaths, source.Path = []string{source.Path, "/private/second.parquet"}, ""
	source.LocalSnapshot.Parts = append(source.LocalSnapshot.Parts, source.LocalSnapshot.Parts[0])
	if err := source.ValidateLocalSnapshot(); err != nil {
		t.Fatal("valid multipart generation rejected", err)
	}
	source.ParquetPaths[1] = source.ParquetPaths[0]
	if source.ValidateLocalSnapshot() == nil {
		t.Fatal("duplicate part path accepted")
	}
}

func TestSnapshotScanDefaultsAndExplicitBudgets(t *testing.T) {
	defaults, err := (Dataset{}).EffectiveSnapshotScanLimits()
	if err != nil || defaults.MaxRows != 1_000_000 || defaults.MaxBytes != 256<<20 {
		t.Fatal("snapshot scans lack independent defaults", defaults, err)
	}
	want := SnapshotScanLimits{MaxRows: 5_000_000, MaxBytes: 1 << 30}
	dataset := Dataset{Scan: &want}
	dataset.Limits.MaxRows, dataset.Limits.MaxBytes = 10, 1024
	if actual, err := dataset.EffectiveSnapshotScanLimits(); err != nil || actual != want {
		t.Fatal("output bounds replaced raw scan capacity", actual, err)
	}
	for _, limits := range []SnapshotScanLimits{{MaxRows: -1}, {MaxRows: 100_000_001}, {MaxBytes: -1}, {MaxBytes: 1023}, {MaxBytes: (1 << 40) + 1}} {
		if _, err := limits.Effective(); err == nil {
			t.Fatal("unbounded snapshot scan accepted", limits)
		}
	}
}
