// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func objectSnapshotSource(parts int) Source {
	source := Source{ID: "orders_fast", Type: "parquet", ObjectSnapshot: &ObjectSnapshotRead{
		Dataset: "orders_fast", Generation: strings.Repeat("a", 32), SchemaSHA256: strings.Repeat("b", 64),
		Scan: SnapshotScanLimits{MaxRows: 1_000_000, MaxBytes: 256 << 20},
	}}
	for index := 0; index < parts; index++ {
		target := "http://127.0.0.1:12345/" + strings.Repeat("c", 64) + "/orders_fast"
		if parts > 1 {
			target += fmt.Sprintf("/part-%04d", index)
		}
		part := ObjectSnapshotPart{URL: target, Rows: int64(index + 1), Bytes: 4096 + int64(index), SHA256: strings.Repeat("d", 64)}
		source.ObjectSnapshot.Parts = append(source.ObjectSnapshot.Parts, part)
		capability := ObjectRange{URL: target, Bytes: part.Bytes}
		if parts == 1 {
			source.Path, source.Range = target, &capability
		} else {
			source.Ranges = append(source.Ranges, capability)
		}
	}
	return source
}

func TestObjectSnapshotEnvelopeIsJSONOnlyAndBindsExactCapabilities(t *testing.T) {
	for _, parts := range []int{1, 2, 256} {
		source := objectSnapshotSource(parts)
		if err := source.ValidateObjectSnapshot(); err != nil {
			t.Fatal("valid capability layout rejected", err)
		}
		raw, err := json.Marshal(source)
		if err != nil {
			t.Fatal(err)
		}
		var decoded Source
		if err := json.Unmarshal(raw, &decoded); err != nil || decoded.ValidateObjectSnapshot() != nil || !reflect.DeepEqual(source, decoded) {
			t.Fatal("worker envelope changed trusted provenance", err)
		}
		if _, err := (Config{Sources: []Source{decoded}}).Select([]string{source.ID}); err != nil {
			t.Fatal("selection rejected trusted provenance", err)
		}
	}
	config := filepath.Join(t.TempDir(), "catalog.yml")
	for _, field := range []string{"object_snapshot", "local_snapshot", "object_range", "object_ranges"} {
		if err := os.WriteFile(config, []byte("sources:\n  - id: orders_fast\n    type: parquet\n    path: data.parquet\n    "+field+": {}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(config); err == nil {
			t.Fatal("public YAML supplied trusted worker metadata", field)
		}
	}
}

func TestObjectSnapshotRejectsConfusedProvenance(t *testing.T) {
	for name, change := range map[string]func(*Source){
		"source-type":    func(s *Source) { s.Type = "csv" },
		"source-id":      func(s *Source) { s.ID = "other" },
		"dataset":        func(s *Source) { s.ObjectSnapshot.Dataset = "other" },
		"generation":     func(s *Source) { s.ObjectSnapshot.Generation = "invalid" },
		"schema":         func(s *Source) { s.ObjectSnapshot.SchemaSHA256 = "invalid" },
		"part-digest":    func(s *Source) { s.ObjectSnapshot.Parts[0].SHA256 = "invalid" },
		"negative-rows":  func(s *Source) { s.ObjectSnapshot.Parts[0].Rows = -1 },
		"row-overflow":   func(s *Source) { s.ObjectSnapshot.Parts[0].Rows = 100_000_001 },
		"part-size":      func(s *Source) { s.ObjectSnapshot.Parts[0].Bytes++ },
		"truncated-part": func(s *Source) { s.Range.Bytes = 11; s.ObjectSnapshot.Parts[0].Bytes = 11 },
		"oversized-part": func(s *Source) { s.Range.Bytes = 4<<30 + 1; s.ObjectSnapshot.Parts[0].Bytes = s.Range.Bytes },
		"unbound-url":    func(s *Source) { s.ObjectSnapshot.Parts[0].URL += "/part-0000" },
		"changed-capability": func(s *Source) {
			s.Range.URL = strings.Replace(s.Range.URL, strings.Repeat("c", 64), strings.Repeat("e", 64), 1)
			s.Path = s.Range.URL
		},
		"wrong-url-dataset": func(s *Source) {
			s.Range.URL = strings.Replace(s.Range.URL, "/orders_fast", "/other", 1)
			s.Path, s.ObjectSnapshot.Parts[0].URL = s.Range.URL, s.Range.URL
		},
		"part-as-single": func(s *Source) {
			s.Range.URL += "/part-0000"
			s.Path, s.ObjectSnapshot.Parts[0].URL = s.Range.URL, s.Range.URL
		},
		"remote-url": func(s *Source) {
			s.Range.URL = strings.Replace(s.Range.URL, "127.0.0.1", "example.test", 1)
			s.Path, s.ObjectSnapshot.Parts[0].URL = s.Range.URL, s.Range.URL
		},
		"path":             func(s *Source) { s.Path = "/private/data.parquet" },
		"no-range":         func(s *Source) { s.Range = nil },
		"empty-parts":      func(s *Source) { s.ObjectSnapshot.Parts = nil },
		"extra-part":       func(s *Source) { s.ObjectSnapshot.Parts = append(s.ObjectSnapshot.Parts, s.ObjectSnapshot.Parts[0]) },
		"implicit-budget":  func(s *Source) { s.ObjectSnapshot.Scan.MaxRows = 0 },
		"invalid-budget":   func(s *Source) { s.ObjectSnapshot.Scan.MaxBytes = -1 },
		"local-provenance": func(s *Source) { s.LocalSnapshot = snapshotSource().LocalSnapshot },
		"local-paths":      func(s *Source) { s.ParquetPaths = []string{} },
		"object":           func(s *Source) { s.Object = &ObjectRead{} },
		"mixed-ranges":     func(s *Source) { s.Ranges = []ObjectRange{*s.Range} },
		"credential":       func(s *Source) { s.TokenEnv = "KELVO_SOURCE_TOKEN" },
		"options":          func(s *Source) { s.Options = map[string]string{"option": "value"} },
		"callback":         func(s *Source) { s.Federation = &FederationConfig{} },
		"adapter":          func(s *Source) { s.Adapter = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			source := objectSnapshotSource(1)
			change(&source)
			if source.ValidateObjectSnapshot() == nil {
				t.Fatal("confused provenance accepted")
			}
			if _, err := (Config{Sources: []Source{source}}).Select([]string{source.ID}); err == nil {
				t.Fatal("selection omitted provenance validation")
			}
		})
	}
	local := snapshotSource()
	local.ObjectSnapshot = objectSnapshotSource(1).ObjectSnapshot
	if local.ValidateLocalSnapshot() == nil {
		t.Fatal("local admission ignored conflicting object provenance")
	}
}

func TestObjectSnapshotMultipartRequiresOrderedBoundedParts(t *testing.T) {
	for name, change := range map[string]func(*Source){
		"reorder-descriptor": func(s *Source) {
			s.ObjectSnapshot.Parts[0], s.ObjectSnapshot.Parts[1] = s.ObjectSnapshot.Parts[1], s.ObjectSnapshot.Parts[0]
		},
		"reorder-ranges": func(s *Source) { s.Ranges[0], s.Ranges[1] = s.Ranges[1], s.Ranges[0] },
		"duplicate-range": func(s *Source) {
			s.Ranges[1], s.ObjectSnapshot.Parts[1] = s.Ranges[0], s.ObjectSnapshot.Parts[0]
		},
		"different-capability": func(s *Source) {
			s.Ranges[1].URL = strings.Replace(s.Ranges[1].URL, strings.Repeat("c", 64), strings.Repeat("e", 64), 1)
			s.ObjectSnapshot.Parts[1].URL = s.Ranges[1].URL
		},
		"cumulative-rows": func(s *Source) { s.ObjectSnapshot.Parts[0].Rows = 100_000_000 },
		"schema-less":     func(s *Source) { s.ObjectSnapshot.SchemaSHA256 = "" },
		"empty-ranges":    func(s *Source) { s.Ranges = []ObjectRange{} },
		"missing-part":    func(s *Source) { s.ObjectSnapshot.Parts = s.ObjectSnapshot.Parts[:1] },
		"too-many-parts":  func(s *Source) { *s = objectSnapshotSource(257) },
	} {
		t.Run(name, func(t *testing.T) {
			source := objectSnapshotSource(2)
			change(&source)
			if source.ValidateObjectSnapshot() == nil {
				t.Fatal("confused multipart provenance accepted")
			}
		})
	}
	source := objectSnapshotSource(1)
	source.ObjectSnapshot.SchemaSHA256 = ""
	if err := source.ValidateObjectSnapshot(); err != nil {
		t.Fatal("unrestricted legacy single-object source rejected", err)
	}
	source.ObjectSnapshot = nil
	if err := source.ValidateObjectSnapshot(); err != nil {
		t.Fatal("legacy range source changed", err)
	}
}
