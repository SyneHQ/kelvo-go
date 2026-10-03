//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestSandboxMultipartExactGrants(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "worker")
	parts := []string{filepath.Join(dir, "one.parquet"), filepath.Join(dir, "two.parquet")}
	for _, path := range append([]string{binary}, parts...) {
		if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	job := filepath.Join(dir, "job")
	if err := os.Mkdir(job, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := catalog.Config{Sources: []catalog.Source{{ID: "data", Type: "parquet", ParquetPaths: parts}}}
	args, err := SandboxCommand(binary, job, cfg, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--read", parts[0], "--read", parts[1], "--write", job, "--", binary, "worker"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("got %q want %q", args, want)
	}
	cfg.Sources[0].ParquetPaths = []string{dir}
	if _, err := SandboxCommand(binary, job, cfg, query.DefaultLimits()); err == nil {
		t.Fatal("granted source directory")
	}
	cfg.Sources[0].ParquetPaths = []string{filepath.Join(dir, "missing")}
	if _, err := SandboxCommand(binary, job, cfg, query.DefaultLimits()); err == nil {
		t.Fatal("granted missing part")
	}
	cfg.Sources[0].Type = "csv"
	cfg.Sources[0].ParquetPaths = parts
	if _, err := SandboxCommand(binary, job, cfg, query.DefaultLimits()); err == nil {
		t.Fatal("granted multipart CSV")
	}
}

func TestSandboxMultipartCombinedGrantLimit(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "worker")
	if err := os.WriteFile(binary, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	var parts []string
	for i := 0; i < 1025; i++ {
		path := filepath.Join(dir, fmt.Sprintf("part-%d.parquet", i))
		if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, path)
	}
	cfg := catalog.Config{}
	for i := 0; i < 4; i++ {
		cfg.Sources = append(cfg.Sources, catalog.Source{ID: fmt.Sprintf("data%d", i), Type: "parquet", ParquetPaths: parts[i*256 : (i+1)*256]})
	}
	if _, err := SandboxCommand(binary, dir, cfg, query.DefaultLimits()); err != nil {
		t.Fatal("1024 exact grants rejected", err)
	}
	cfg.Sources = append(cfg.Sources, catalog.Source{ID: "extra", Type: "parquet", Path: parts[1024]})
	if _, err := SandboxCommand(binary, dir, cfg, query.DefaultLimits()); err == nil {
		t.Fatal("combined reads exceed launcher capacity")
	}
}

func TestSandboxMultipartPathBytesBound(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "worker")
	if err := os.WriteFile(binary, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	deep := dir
	for len(deep) < 1100 {
		deep = filepath.Join(deep, strings.Repeat("d", 60))
	}
	if err := os.MkdirAll(deep, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := catalog.Config{}
	for group := 0; group < 2; group++ {
		var parts []string
		for i := 0; i < 256; i++ {
			path := filepath.Join(deep, fmt.Sprintf("part-%d-%d.parquet", group, i))
			if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
				t.Fatal(err)
			}
			parts = append(parts, path)
		}
		cfg.Sources = append(cfg.Sources, catalog.Source{ID: fmt.Sprintf("data%d", group), Type: "parquet", ParquetPaths: parts})
	}
	if _, err := SandboxCommand(binary, dir, cfg, query.DefaultLimits()); err == nil || !strings.Contains(err.Error(), "path byte limit") {
		t.Fatalf("oversized argv not rejected explicitly: %v", err)
	}
}
