//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

func TestRemoteSingleVerifyRejectsChecksumIntactMetadataMismatch(t *testing.T) {
	for _, defect := range []string{"rows", "schema_hash"} {
		t.Run(defect, func(t *testing.T) {
			backend, client := recoveryObjectBackend(t)
			original := commitRemoteRecovery(t, backend, "id", "config-v1")
			client.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) {
				if defect == "rows" {
					m.Committed.Rows++
				} else {
					m.Committed.SchemaHash = strings.Repeat("0", 64)
				}
			})
			status, err := backend.Status(context.Background(), "events")
			if err != nil || status.SHA256 != original.SHA256 || status.ObjectVersion != original.ObjectVersion {
				t.Fatalf("fixture changed payload identity instead of metadata: %v", err)
			}
			if _, err := backend.verifyObjectBytes(context.Background(), status); err != nil {
				t.Fatalf("fixture failed checksum-only verification: %v", err)
			}
			if _, err := backend.Verify(context.Background(), "events"); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("public Verify accepted corrupt %s: %v", defect, err)
			}
			if client.ranges == 0 {
				t.Fatal("verification did not inspect the bounded Parquet footer")
			}
		})
	}
}

func TestRemoteSingleVerifyLegacySchemaHashRemainsOptional(t *testing.T) {
	backend, client := recoveryObjectBackend(t)
	original := commitRemoteRecovery(t, backend, "id", "config-v1")
	client.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) {
		m.Committed.SchemaHash = ""
	})
	verified, err := backend.Verify(context.Background(), "events")
	if err != nil || verified.SchemaHash != "" || verified.Generation != original.Generation || verified.Rows != original.Rows || !verified.RefreshedAt.Equal(original.RefreshedAt) {
		t.Fatalf("valid legacy single file rejected or rewritten: %v", err)
	}
	if client.ranges == 0 {
		t.Fatal("legacy metadata bypassed footer verification")
	}
	status, err := backend.Status(context.Background(), "events")
	if err != nil || status.SchemaHash != "" {
		t.Fatalf("Verify rewrote the legacy manifest: %v", err)
	}
	client.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) {
		m.Committed.Rows++
	})
	if _, err := backend.Verify(context.Background(), "events"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("legacy empty schema hash bypassed row verification: %v", err)
	}
}

func TestRemoteSingleVerifyRequiresRangeClient(t *testing.T) {
	backend, client := recoveryObjectBackend(t)
	commitRemoteRecovery(t, backend, "id", "config-v1")
	// Expose only the baseline interface, even though the wrapped fixture has
	// range support. External backends must explicitly implement that capability.
	backend.reader = struct{ objectstore.Client }{Client: client}
	client.mu.Lock()
	before := client.payloadGets
	client.mu.Unlock()
	if _, err := backend.Verify(context.Background(), "events"); !errors.Is(err, ErrRecoveryUnsupported) {
		t.Fatalf("checksum-only client claimed complete verification: %v", err)
	}
	client.mu.Lock()
	after := client.payloadGets
	client.mu.Unlock()
	if after != before {
		t.Fatal("unsupported verification downloaded a payload")
	}
}
