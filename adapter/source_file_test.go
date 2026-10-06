// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/filesnapshot"
)

func TestProcessFileSnapshotBindsDescriptorAndBudgets(t *testing.T) {
	now := time.Unix(1800000000, 0)
	r := processFixture(t, now)
	r.Source.Engine, r.Source.DSN = "csv", ""
	r.SourceFile = &filesnapshot.Descriptor{Version: 1, Format: "csv", Bytes: 8, SHA256: strings.Repeat("a", 64)}
	r.Source.Options = map[string]string{"file_format": "csv", "file_bytes": strconv.FormatInt(r.SourceFile.Bytes, 10), "file_sha256": r.SourceFile.SHA256}
	r.Limits.MemoryMB, r.Limits.Threads, r.Limits.MaxTempMB = 64, 1, 16
	if err := r.ValidateAt(now); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ProcessRequest){
		"missing descriptor":     func(r *ProcessRequest) { r.SourceFile = nil },
		"wrong engine":           func(r *ProcessRequest) { r.Source.Engine = "postgres" },
		"source URL":             func(r *ProcessRequest) { r.Source.URL = "https://other.invalid" },
		"source password":        func(r *ProcessRequest) { r.Source.Password = "private" },
		"unbound format":         func(r *ProcessRequest) { r.Source.Options["file_format"] = "parquet" },
		"unbound size":           func(r *ProcessRequest) { r.Source.Options["file_bytes"] = "9" },
		"unbound hash":           func(r *ProcessRequest) { r.Source.Options["file_sha256"] = strings.Repeat("b", 64) },
		"path option":            func(r *ProcessRequest) { r.Source.Options["path"] = "/tmp/private" },
		"missing memory budget":  func(r *ProcessRequest) { r.Limits.MemoryMB = 0 },
		"missing thread budget":  func(r *ProcessRequest) { r.Limits.Threads = 0 },
		"missing scratch budget": func(r *ProcessRequest) { r.Limits.MaxTempMB = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := r
			bad.Source.Options = make(map[string]string, len(r.Source.Options))
			for k, v := range r.Source.Options {
				bad.Source.Options[k] = v
			}
			mutate(&bad)
			if bad.ValidateAt(now) == nil {
				t.Fatal("unbound source file accepted")
			}
		})
	}
}
