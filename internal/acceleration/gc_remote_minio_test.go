//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
	"go.yaml.in/yaml/v3"
)

// Real S3-compatible provider run (MinIO in CI-like Docker). Skipped unless
// KELVO_TEST_S3_ENDPOINT/BUCKET and KELVO_SOURCE_TEST_S3_ID/SECRET are set.
func TestRemoteGCMinIOConformance(t *testing.T) {
	endpoint, bucket := os.Getenv("KELVO_TEST_S3_ENDPOINT"), os.Getenv("KELVO_TEST_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("S3 conformance endpoint not configured")
	}
	for _, role := range []string{"READ", "WRITE", "REGISTRY"} {
		t.Setenv("KELVO_SOURCE_"+role+"_ID", os.Getenv("KELVO_SOURCE_TEST_S3_ID"))
		t.Setenv("KELVO_SOURCE_"+role+"_SECRET", os.Getenv("KELVO_SOURCE_TEST_S3_SECRET"))
	}
	c := objectRuntimeTestConfig(t)
	storage := c.Acceleration.ObjectStorage
	storage.Endpoint, storage.Bucket, storage.Prefix = endpoint, bucket, fmt.Sprintf("conformance/gc-%d", time.Now().UnixNano())
	runtime, err := OpenObjectRuntime(c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runtime.Close(ctx)
	}()
	ctx := context.Background()
	backend := runtime.backend
	fp := protectedFingerprint(t, c)
	old := commitRemoteRecovery(t, backend, "id", fp)
	state, err := backend.readState(ctx, "events", backend.reader)
	if err != nil {
		t.Fatal(err)
	}
	binding := *state.manifest.Committed.ReaderBinding
	current := commitRemoteRecovery(t, backend, "id", fp)
	writer, err := objectstore.New(storage.ObjectLocation, storage.WriteCredentials)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	readCurrent := func() []byte {
		body, _, err := writer.Get(ctx, backend.key("events", current.Generation+".parquet"), current.ObjectVersion)
		if err != nil {
			t.Fatal(err)
		}
		defer body.Close()
		raw, _ := io.ReadAll(body)
		return raw
	}
	before := readCurrent()

	lease, err := runtime.registry.Acquire(ctx, binding)
	if err != nil {
		t.Fatal(err)
	}
	// Retention drops the old generation: it is now unreferenced.
	state, err = backend.readState(ctx, "events", writer)
	if err != nil {
		t.Fatal(err)
	}
	manifest := state.manifest
	manifest.History = nil
	if _, err := backend.writeState(ctx, writer, state, manifest); err != nil {
		t.Fatal(err)
	}
	gc := func(del bool) RemoteGCGeneration {
		t.Helper()
		report, err := RunRemoteGC(ctx, c, "events", RemoteGCOptions{Delete: del, Grace: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("mode=%s complete=%v classes=%v quarantined=%d deleted=%d", report.Mode, report.Complete, report.Classes, report.Quarantined, report.Deleted)
		return generationAction(report, old.Generation)
	}
	if g := gc(false); g.Class != "orphan" || g.Action != "kept" {
		t.Fatalf("dry run: %+v", g)
	}
	if g := gc(true); g.Action != "quarantined" {
		t.Fatalf("quarantine: %+v", g)
	}
	if _, err := runtime.registry.Acquire(ctx, binding); !errors.Is(err, readerlease.ErrRetired) {
		t.Fatal("retired generation admitted a reader", err)
	}
	// Backdate the marker with a real conditional replace.
	markerKey := backend.key("events", "gc/"+old.Generation+".quarantine.yaml")
	body, info, err := writer.Get(ctx, markerKey, "")
	if err != nil {
		t.Fatal(err)
	}
	var marker gcMarker
	raw, _ := io.ReadAll(body)
	body.Close()
	if err := yaml.Unmarshal(raw, &marker); err != nil {
		t.Fatal(err)
	}
	marker.QuarantinedAt = marker.QuarantinedAt.Add(-48 * time.Hour)
	raw, _ = yaml.Marshal(marker)
	if _, err := writer.Put(ctx, markerKey, bytes.NewReader(raw), int64(len(raw)), objectSHA256(raw), objectstore.Condition{Version: info.Version}); err != nil {
		t.Fatal(err)
	}
	if g := gc(true); g.Action != "kept: readers may be live" {
		t.Fatalf("collected under a live reader: %+v", g)
	}
	if err := lease.Check(); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Head(ctx, backend.key("events", old.Generation+".parquet"), old.ObjectVersion); err != nil {
		t.Fatal("live reader lost its object", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if g := gc(true); g.Action != "deleted" {
		t.Fatalf("delete: %+v", g)
	}
	for _, name := range []string{old.Generation + ".parquet", "reader-leases/" + old.Generation + ".yml", "gc/" + old.Generation + ".quarantine.yaml"} {
		if _, err := writer.Head(ctx, backend.key("events", name), ""); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatal("left behind", name, err)
		}
	}
	if g := gc(true); g.Generation != "" {
		t.Fatalf("rerun not idempotent: %+v", g)
	}
	snapshot, err := backend.Status(ctx, "events")
	if err != nil || snapshot.Generation != current.Generation || !bytes.Equal(readCurrent(), before) {
		t.Fatal("current generation changed", err)
	}
}
