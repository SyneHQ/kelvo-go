// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func TestCLIRequestAcceptsSQLAndMongoForms(t *testing.T) {
	sql, err := makeRequest("SELECT ?", "federated", "", "source", `[{"type":"int64","value":"9223372036854775807"}]`, "", "")
	if err != nil || sql.Mongo != nil || len(sql.Parameters) != 1 {
		t.Fatalf("SQL request: %+v %v", sql, err)
	}
	mongo, err := makeRequest("", "native", "mongo", "", "[]", "events", `[{"$match":{"id":{"$numberLong":"9223372036854775807"}}}]`)
	if err != nil || mongo.Mongo == nil || len(mongo.Mongo.Pipeline) != 1 || mongo.SQL != "" {
		t.Fatalf("Mongo request: %+v %v", mongo, err)
	}
	empty, err := makeRequest("", "native", "mongo", "", "[]", "events", "")
	if err != nil || empty.Mongo == nil || len(empty.Mongo.Pipeline) != 0 {
		t.Fatal("default empty Mongo pipeline failed")
	}
}

func TestCLIRequestRejectsMixedAndMalformedMongoForms(t *testing.T) {
	for _, args := range [][7]string{
		{"SELECT 1", "native", "mongo", "", "[]", "events", "[]"},
		{"", "native", "mongo", "", "[]", "", "[]"},
		{"", "native", "mongo", "", "[]", "events", "{}"},
		{"", "native", "mongo", "", "[]", "events", "null"},
		{"", "native", "mongo", "", "[]", "events", "["},
		{"", "federated", "", "", "[]", "events", "[]"},
		{"", "native", "mongo", "extra", "[]", "events", "[]"},
		{"", "native", "mongo", "", `[{"type":"int64","value":1}]`, "events", "[]"},
	} {
		if _, err := makeRequest(args[0], args[1], args[2], args[3], args[4], args[5], args[6]); err == nil {
			t.Fatalf("accepted %+v", args)
		}
	}
}

func TestCLISandboxIsOptionalAndResolvesRelativePath(t *testing.T) {
	if path, err := resolveCLISandbox(""); err != nil || path != "" {
		t.Fatalf("default sandbox changed: %q %v", path, err)
	}
	if runtime.GOOS != "linux" {
		if _, err := resolveCLISandbox("launcher"); err == nil {
			t.Fatal("non-Linux sandbox was accepted")
		}
		return
	}
	directory := t.TempDir()
	t.Chdir(directory)
	if err := os.WriteFile("launcher", []byte("#!/bin/sh\nexit 125\n"), 0700); err != nil {
		t.Fatal(err)
	}
	path, err := resolveCLISandbox("launcher")
	if err != nil || path != filepath.Join(directory, "launcher") {
		t.Fatalf("relative sandbox path: %q %v", path, err)
	}
}

func TestCLIRejectsInvalidSandboxBeforeStartingQueryOrServer(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("launcher permissions are checked on Linux")
	}
	directory := t.TempDir()
	config := filepath.Join(directory, "catalog.yml")
	if err := os.WriteFile(config, []byte("sources: []\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		mode os.FileMode
	}{
		{name: "not-executable", mode: 0600},
		{name: "group-writable", mode: 0720},
		{name: "other-writable", mode: 0702},
		{name: "missing"},
		{name: "directory", mode: os.ModeDir | 0700},
	} {
		t.Run(tc.name, func(t *testing.T) {
			launcher := filepath.Join(directory, tc.name)
			if tc.mode.IsDir() {
				if err := os.Mkdir(launcher, tc.mode.Perm()); err != nil {
					t.Fatal(err)
				}
			} else if tc.mode != 0 {
				if err := os.WriteFile(launcher, []byte("#!/bin/sh\nexit 125\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(launcher, tc.mode); err != nil {
					t.Fatal(err)
				}
			}
			for _, command := range []string{"query", "serve"} {
				err := run([]string{command, "--config", config, "--sandbox", launcher})
				if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" ||
					query.PublicError(err).Message != "Sandbox launcher must be executable and not writable by group or others" {
					t.Fatalf("%s accepted an invalid sandbox or failed elsewhere: %v", command, err)
				}
			}
		})
	}
}

func TestCLIResultCompressionValidatedBeforeConfiguration(t *testing.T) {
	missingConfig := filepath.Join(t.TempDir(), "missing.yml")
	for _, command := range []string{"query", "serve"} {
		for _, codec := range []string{"", "none", "lz4_frame", "lz4", "zstd", "LZ4_FRAME"} {
			t.Run(command+"/"+codec, func(t *testing.T) {
				err := run([]string{command, "--config", missingConfig, "--result-compression", codec})
				if err == nil {
					t.Fatal("missing configuration was accepted")
				}
				want := "INVALID_ARGUMENT"
				if codec == "" || codec == "none" || codec == "lz4_frame" {
					want = "CONFIGURATION_ERROR"
				}
				if got := query.PublicError(err).Code; got != want {
					t.Fatalf("compression %q: code=%s want=%s error=%v", codec, got, want, err)
				}
			})
		}
	}
}

func TestCLIRowBatchTargetValidatedBeforeConfiguration(t *testing.T) {
	missingConfig := filepath.Join(t.TempDir(), "missing.yml")
	for _, command := range []string{"query", "serve"} {
		for _, target := range []int64{-1, 0, 1023, 1024, 1 << 20, 64 << 20, (64 << 20) + 1} {
			t.Run(command+"/"+strconv.FormatInt(target, 10), func(t *testing.T) {
				err := run([]string{command, "--config", missingConfig, "--row-batch-target-bytes", strconv.FormatInt(target, 10)})
				if err == nil {
					t.Fatal("missing configuration was accepted")
				}
				want := "INVALID_ARGUMENT"
				if target == 0 || (target >= 1024 && target <= 64<<20) {
					want = "CONFIGURATION_ERROR"
				}
				if got := query.PublicError(err).Code; got != want {
					t.Fatalf("batch target %d: code=%s want=%s error=%v", target, got, want, err)
				}
			})
		}
	}
}

func TestRefreshFactoryPreservesRowBatchTarget(t *testing.T) {
	// Refresh/watch use each dataset's limits; they do not share the standalone
	// query/serve CLI flags or override other datasets' batching choices.
	for _, target := range []int64{0, 1 << 20} {
		limits := query.DefaultLimits()
		limits.RowBatchTargetBytes = target
		executor, err := refreshFactory("")(catalog.Config{}, limits)
		if err != nil {
			t.Fatal(err)
		}
		process, ok := executor.(*worker.Executor)
		if !ok || process.Limits != limits {
			t.Fatalf("refresh dropped dataset limits: %T, target=%d", executor, target)
		}
	}
}
