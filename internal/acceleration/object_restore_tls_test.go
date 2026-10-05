//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/testutil/protectedobject"
	"go.yaml.in/yaml/v3"
)

type restoreTLSOutput struct {
	bytes.Buffer
	truncated bool
}

func (b *restoreTLSOutput) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) > 64<<10-b.Len() {
		p = p[:64<<10-b.Len()]
		b.truncated = true
	}
	_, _ = b.Buffer.Write(p)
	return n, nil
}

type restoreTLSExecutor struct{ calls *atomic.Int64 }

func (e restoreTLSExecutor) Execute(context.Context, query.Request, query.Sink) (query.Stats, error) {
	e.calls.Add(1)
	return query.Stats{}, errors.New("restore must not execute the source")
}

func TestProtectedRestoreTLS(t *testing.T) {
	for _, kind := range []string{"single-to-single", "single-to-two-parts", "two-parts-to-single", "verified-noop"} {
		t.Run(kind, func(t *testing.T) {
			if os.Getenv("KELVO_RESTORE_TLS_CHILD") != kind {
				// A fresh process keeps Go's cached system roots isolated. Normal
				// certificate verification still checks the fixture's private CA.
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProtectedRestoreTLS$/^"+kind+"$", "-test.v", "-test.timeout=50s")
				command.Env = append(os.Environ(), "KELVO_RESTORE_TLS_CHILD="+kind)
				command.WaitDelay = 5 * time.Second
				var output restoreTLSOutput
				command.Stdout, command.Stderr = &output, &output
				if err := command.Run(); err != nil || output.truncated {
					t.Fatalf("protected restore TLS child failed: %v\n%s", err, output.String())
				}
				t.Log(output.String())
				return
			}
			runProtectedRestoreTLS(t, kind)
		})
	}
}

func runProtectedRestoreTLS(t *testing.T, kind string) {
	t.Helper()
	service := protectedobject.New(t, protectedobject.Options{Tenants: []string{"tenant-a"}})
	storage := service.Storage("tenant-a")
	input := filepath.Join(t.TempDir(), "unused.csv")
	if err := os.WriteFile(input, []byte("id\n7\n"), 0600); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	limits := query.DefaultLimits()
	limits.Timeout = 20 * time.Second
	config := catalog.Config{
		Sources: []catalog.Source{{ID: "source", Type: "csv", Path: input}},
		Acceleration: &catalog.AccelerationConfig{Directory: directory, TenantID: "tenant-a", ObjectStorage: &storage,
			Datasets: []catalog.Dataset{{ID: "events", Query: query.Request{Mode: "federated", Sources: []string{"source"}, SQL: "SELECT * FROM source"},
				MaxAge: time.Hour, AuthorizationVersion: "fixture-v1", Limits: limits, Verification: &catalog.VerificationLimits{MaxBytes: 32 << 20}}}},
	}
	runtime, err := OpenObjectRuntime(config)
	if runtime != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := runtime.Close(cleanup); err != nil {
				t.Error("runtime cleanup failed", err)
			}
			select {
			case <-runtime.Quiesced():
			case <-cleanup.Done():
				t.Error("runtime did not hand back its work")
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	var factories, executions atomic.Int64
	manager, err := NewManagerWithRuntime(config, func(catalog.Config, query.Limits) (query.Executor, error) {
		factories.Add(1)
		return restoreTLSExecutor{calls: &executions}, nil
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error("manager cleanup failed", err)
		}
	})
	fingerprint, err := config.DatasetFingerprint("events")
	if err != nil {
		t.Fatal(err)
	}
	backend := runtime.backend
	first := commitRemoteRecovery(t, backend, "id", fingerprint)
	second := commitRemoteRecovery(t, backend, "id", fingerprint)
	multipart := commitVerificationMultipart(t, backend, fingerprint)
	if len(multipart.Parts) != 2 {
		t.Fatal("fixture did not publish two real Parquet parts")
	}
	current, target := multipart, first
	if kind == "single-to-single" || kind == "single-to-two-parts" {
		if _, err := manager.Restore(context.Background(), "events", first.Generation, multipart.Generation); err != nil {
			t.Fatal("fixture setup restore failed", err)
		}
		current, target = first, second
		if kind == "single-to-two-parts" {
			target = multipart
		}
	} else if kind == "verified-noop" {
		target = multipart
	}
	readRoot := func() objectManifest {
		object, exists := service.Root("tenant-a", "events")
		if !exists {
			t.Fatal("fixture root is missing")
		}
		var root objectManifest
		if err := yaml.Unmarshal(object.Data, &root); err != nil {
			t.Fatal(err)
		}
		return root
	}
	beforeRoot := readRoot()
	_, expectedCommit := restoreEntry(beforeRoot, target.Generation)
	if expectedCommit == nil || beforeRoot.Writer != nil {
		t.Fatal("fixture target or released writer is missing")
	}
	immutable := service.ImmutableObjects("tenant-a", "events")
	if len(immutable) != 5 {
		t.Fatal("fixture must contain two single objects, two parts and a descriptor")
	}
	paths := func() []string {
		var result []string
		if err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
			if err == nil {
				result = append(result, path)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return result
	}
	beforePaths, before := paths(), service.Snapshot()
	if before.Total.StageWrites != 3 || before.Total.SealWrites != 3 || before.Total.PayloadWriteRequests != 5 {
		t.Fatal("fixture did not observe three staged/sealed generations and five immutable writes", before.Total)
	}
	restored, err := manager.Restore(context.Background(), "events", target.Generation, current.Generation)
	if err != nil {
		t.Fatal("TLS restore failed", err)
	}
	after, afterRoot := service.Snapshot(), readRoot()
	if restored.Generation != target.Generation || restored.ObjectVersion != target.ObjectVersion || restored.Rows != target.Rows ||
		restored.SchemaHash != target.SchemaHash || restored.SHA256 != target.SHA256 || !restored.RefreshedAt.Equal(target.RefreshedAt) ||
		!reflect.DeepEqual(restored.Parts, target.Parts) || !reflect.DeepEqual(afterRoot.Committed, expectedCommit) || afterRoot.Writer != nil {
		t.Fatal("restore changed immutable identity, age, schema or writer release")
	}
	if kind == "verified-noop" && !reflect.DeepEqual(beforeRoot, afterRoot) {
		t.Fatal("no-op changed the retained catalog")
	}
	if !reflect.DeepEqual(immutable, service.ImmutableObjects("tenant-a", "events")) || !reflect.DeepEqual(beforePaths, paths()) {
		t.Fatal("restore changed payloads, versions or local staging")
	}
	pins := 2
	if kind == "verified-noop" {
		pins = 1
	}
	if after.Total.PinAcquires-before.Total.PinAcquires != pins || after.Total.PinReleases-before.Total.PinReleases != pins ||
		after.Total.RangeRequests <= before.Total.RangeRequests || after.Readers != 0 || after.Total.Violations != 0 ||
		after.Total.PayloadWriteRequests != before.Total.PayloadWriteRequests || after.Total.StageWrites != before.Total.StageWrites ||
		after.Total.SealWrites != before.Total.SealWrites || factories.Load() != 0 || executions.Load() != 0 {
		t.Fatal("restore bypassed verification, staged data, queried a source or retained pins", before.Total, after.Total)
	}
	if verificationRuntimeOperations(runtime) != 0 {
		t.Fatal("successful restore retained operation capacity")
	}
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.WaitIdle(cleanup); err != nil {
		t.Fatal("fixture retained HTTP handlers", err)
	}
}
