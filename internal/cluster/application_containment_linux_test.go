//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/operationrun"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func writeApplicationConfig(t *testing.T, containmentBlock string) string {
	t.Helper()
	root := t.TempDir()
	adapter := filepath.Join(root, "adapter")
	if err := os.WriteFile(adapter, []byte("#!/bin/sh\n"), 0700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("#!/bin/sh\n"))
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"state", "scratch"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	_, workerTLS := exportIntegrationTLS(t)
	raw := fmt.Sprintf(`version: 1
listen: 127.0.0.1:0
state_directory: state
instance_id: a
app_scope: team-a
issuer: oos
audience: kelvo
service_principal: oos-service
public_key: %s
tls: {cert_file: %s, key_file: %s}
resolver: {url: "https://127.0.0.1:9443/internal/kelvo/resolve", ca_file: %s, cert_file: %s, key_file: %s, timeout: 5s, max_concurrent: 2}
adapter: {binary: %s, sha256: %s}
resources: {max_concurrent: 2, memory_mb: 1024, baseline_mb: 64, overhead_mb: 352, scratch_mb: 4096}
sandbox_path: /usr/local/bin/kelvo-landlock
scratch_directory: scratch
%s`, base64.StdEncoding.EncodeToString(public), workerTLS.CertFile, workerTLS.KeyFile, workerTLS.CAFile, workerTLS.CertFile, workerTLS.KeyFile, adapter, hex.EncodeToString(sum[:]), containmentBlock)
	path := filepath.Join(root, "application.yml")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestApplicationContainmentIsOptional(t *testing.T) {
	path := writeApplicationConfig(t, "")
	cfg, err := LoadApplication(path)
	if err != nil || cfg.Containment != nil {
		t.Fatal("application without containment rejected", err)
	}
	path = writeApplicationConfig(t, "containment: {root: /sys/fs/cgroup/owned/jobs, state_directory: containment, native_overhead_mb: 64, parent_overhead_mb: 32, max_processes: 64}\n")
	contained, err := LoadApplication(path)
	if err != nil || contained.Containment == nil || contained.Containment.StateDirectory != filepath.Join(filepath.Dir(path), "containment") {
		t.Fatal("valid containment rejected or unresolved", err)
	}
	for _, block := range []string{
		"containment: {}\n",
		"containment: {root: /sys/fs/cgroup/owned/jobs, state_directory: containment, native_overhead_mb: 8, parent_overhead_mb: 32, max_processes: 64}\n",
		"containment: {root: /sys/fs/cgroup/owned/jobs, state_directory: state, native_overhead_mb: 64, parent_overhead_mb: 32, max_processes: 64}\n",
	} {
		path = writeApplicationConfig(t, block)
		if _, err := LoadApplication(path); err == nil {
			t.Fatal("present but invalid containment accepted", block)
		}
	}
}

func TestApplicationRunsWithoutContainment(t *testing.T) {
	path := writeApplicationConfig(t, "")
	cfg, err := LoadApplication(path)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := worker.NewConnectionResolver(cfg.Resolver, WorkerIdentity("a", "a1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(resolver.Close)
	executor, err := worker.New(catalog.Config{}, cfg.Limits)
	if err != nil {
		t.Fatal(err)
	}
	executor, err = executor.WithConnectionResolvers(map[string]*worker.ConnectionResolver{cfg.Issuer: resolver})
	if err != nil {
		t.Fatal(err)
	}
	executor.SandboxPath = cfg.SandboxPath
	if executor.ScratchRoot, err = worker.OpenScratchRoot(cfg.ScratchDirectory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = executor.ScratchRoot.Close() })
	if executor.ResourcePool, err = cfg.Resources.NewPool(); err != nil {
		t.Fatal(err)
	}
	executor.ResourceOverheadBytes = cfg.Resources.OverheadMB << 20
	token := strings.Repeat("t", 32)
	ctx := context.Background()
	mismatched := cfg
	mismatched.Containment = &ContainmentConfig{}
	if _, err := NewApplication(ctx, mismatched, token, executor); err == nil {
		t.Fatal("configured containment accepted an uncontained executor")
	}
	app, err := NewApplication(ctx, cfg, token, executor)
	if err != nil {
		t.Fatal("uncontained application rejected", err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	if !app.ready() {
		t.Fatal("uncontained application never became ready")
	}
	if err := (&preparedApplicationOperation{app: app}).Close(); err != nil {
		t.Fatal("completion without containment failed", err)
	}
	failed := &preparedApplicationOperation{app: app, sink: &operationResultSink{aborted: true, abortErr: operationrun.ErrCleanup}}
	if err := failed.Close(); !errors.Is(err, operationrun.ErrCleanup) || !app.failed.Load() || app.ready() {
		t.Fatal("uncertain cleanup did not fail the uncontained application", err)
	}
	shutdown, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := errors.Join(app.Drain(shutdown), app.Close(shutdown)); err != nil {
		t.Fatal(err)
	}
}
