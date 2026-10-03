// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestMultipartConfigurationBoundsAndFingerprint(t *testing.T) {
	base, err := loadAccelerationFixture(t, acceleratedYAML)
	if err != nil {
		t.Fatal(err)
	}
	original, err := base.DatasetFingerprint("orders_fast")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		parts int
		bytes int64
		valid bool
	}{
		{2, 1 << 20, true}, {256, 1 << 20, true}, {1, 1 << 20, false}, {257, 1 << 20, false}, {2, (1 << 20) - 1, false}, {2, (4 << 30) + 1, false}, {2, base.Acceleration.Datasets[0].Limits.MaxBytes + 1, false},
	} {
		t.Run(fmt.Sprintf("%d-%d", tc.parts, tc.bytes), func(t *testing.T) {
			c, err := loadAccelerationFixture(t, acceleratedYAML+fmt.Sprintf("      multipart:\n        max_parts: %d\n        max_part_bytes: %d\n", tc.parts, tc.bytes))
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			if err == nil {
				fp, err := c.DatasetFingerprint("orders_fast")
				if err != nil || fp != original {
					t.Fatal("storage layout changed authorization fingerprint", err)
				}
			}
		})
	}
}

func TestMultipartProcessOnlyPaths(t *testing.T) {
	content := strings.Replace(acceleratedYAML, "    type: clickhouse", "    parquet_paths: [/tmp/private.parquet]\n    type: clickhouse", 1)
	if _, err := loadAccelerationFixture(t, content); err == nil {
		t.Fatal("user YAML accepted process-only paths")
	}
	valid := Source{ID: "data", Type: "parquet", ParquetPaths: []string{"/tmp/part-1.parquet", "/tmp/part'2.parquet"}}
	data, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip Source
	if err := json.Unmarshal(data, &roundtrip); err != nil || roundtrip.ValidateParquetPaths() != nil || len(roundtrip.ParquetPaths) != 2 {
		t.Fatal("process IPC lost paths", err)
	}
	for name, mutate := range map[string]func(*Source){
		"csv": func(s *Source) { s.Type = "csv" }, "mixed": func(s *Source) { s.Path = "/tmp/other.parquet" },
		"remote":     func(s *Source) { s.ParquetPaths = []string{"https://host/file.parquet"} },
		"glob":       func(s *Source) { s.ParquetPaths = []string{"/tmp/*.parquet"} },
		"relative":   func(s *Source) { s.ParquetPaths = []string{"part.parquet"} },
		"traversal":  func(s *Source) { s.ParquetPaths = []string{"/tmp/../etc/passwd"} },
		"duplicate":  func(s *Source) { s.ParquetPaths = []string{"/tmp/a", "/tmp/a"} },
		"empty":      func(s *Source) { s.ParquetPaths = []string{} },
		"credential": func(s *Source) { s.TokenEnv = "KELVO_SOURCE_TOKEN" },
		"too-many":   func(s *Source) { s.ParquetPaths = make([]string, 257) },
	} {
		t.Run(name, func(t *testing.T) {
			s := valid
			mutate(&s)
			if s.ValidateParquetPaths() == nil {
				t.Fatal("accepted invalid paths")
			}
			if _, err := (Config{Sources: []Source{s}}).Select([]string{"data"}); err == nil {
				t.Fatal("selection bypassed validation")
			}
		})
	}
}

func TestMultipartRejectsObjectStorage(t *testing.T) {
	c, err := loadAccelerationFixture(t, acceleratedYAML)
	if err != nil {
		t.Fatal(err)
	}
	c.Acceleration.ObjectStorage = &ObjectStorage{ObjectLocation: ObjectLocation{Provider: "s3", Endpoint: "https://objects.example.com", Bucket: "snapshots", Prefix: "kelvo", Region: "us-east-1"}, ReadCredentials: ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_READER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_READER_SECRET"}, WriteCredentials: ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_WRITER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_WRITER_SECRET"}}
	c.Acceleration.Datasets[0].Multipart = &MultipartConfig{MaxParts: 2, MaxPartBytes: 1 << 20}
	if err := c.validateAcceleration(t.TempDir()); err == nil || !strings.Contains(err.Error(), "local storage") {
		t.Fatalf("object multipart accepted or unrelated error: %v", err)
	}
}
