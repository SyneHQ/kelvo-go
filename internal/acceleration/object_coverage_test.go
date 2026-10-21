//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestObjectBackendsPersistCoveredUntil(t *testing.T) {
	backend := testObjectBackend(t, newFakeSnapshotObjects())
	tx, err := backend.Begin(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	if _, err := io.WriteString(tx.File(), "payload"); err != nil {
		t.Fatal(err)
	}
	tx.(CoverageWriter).SetCoveredUntil("42")
	tx.(LastFullAtWriter).SetLastFullAt("2026-03-01T00:00:00Z")
	if snapshot, err := tx.Commit("source-config", 3); err != nil || snapshot.CoveredUntil != "42" || snapshot.LastFullAt != "2026-03-01T00:00:00Z" {
		t.Fatalf("single commit %+v %v", snapshot, err)
	}
	if status, err := backend.Status(context.Background(), "events"); err != nil || status.CoveredUntil != "42" || status.LastFullAt != "2026-03-01T00:00:00Z" {
		t.Fatalf("single status %+v %v", status, err)
	}

	multi, _ := remoteMultipartFixture(t)
	writer := beginRemoteMultipart(t, multi)
	if err := writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := writer.SetSchema(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)); err != nil {
		t.Fatal(err)
	}
	writer.(CoverageWriter).SetCoveredUntil("2026-03-01T00:00:00Z")
	writer.(LastFullAtWriter).SetLastFullAt("2026-03-02T00:00:00Z")
	if snapshot, err := writer.Commit("config-v1"); err != nil || snapshot.CoveredUntil == "" {
		t.Fatalf("multipart commit %+v %v", snapshot, err)
	}
	if status, err := multi.Status(context.Background(), "events"); err != nil || status.CoveredUntil != "2026-03-01T00:00:00Z" || status.LastFullAt != "2026-03-02T00:00:00Z" {
		t.Fatalf("multipart status %+v %v", status, err)
	}
}
