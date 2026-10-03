//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// Change both copies so tests reach payload/metadata verification rather than
// merely rejecting a disagreement between current and immutable sidecar YAML.
func rewriteVerificationManifest(t *testing.T, s *Store, generation string, change func(*storeManifest)) {
	t.Helper()
	dir, err := s.openDataset("events", false)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	current, err := storeReadManifest(dir, "events")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := storeReadManifestNamed(dir, "events", generationManifestName(generation))
	if err != nil {
		t.Fatal(err)
	}
	change(&manifest)
	encoded, err := yaml.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{generationManifestName(generation)}
	if current.Generation == generation {
		names = append(names, storeManifestName)
	}
	for _, name := range names {
		path := filepath.Join(dir.Name(), name)
		if err := os.Chmod(path, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0400); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSingleVerificationRejectsMetadataMismatchAcrossRecovery(t *testing.T) {
	for _, defect := range []string{"rows", "schema_hash", "malformed_parquet"} {
		for _, affected := range []string{"current", "target"} {
			t.Run(defect+"/"+affected, func(t *testing.T) {
				s, _ := newTestStore(t)
				first := commitRecoverySnapshot(t, s, "id", "config-v1")
				second := commitRecoverySnapshot(t, s, "id", "config-v1")
				broken := first
				if affected == "current" {
					broken = second
				}
				var malformed []byte
				if defect == "malformed_parquet" {
					// Even a checksum-consistent file must have valid Parquet and
					// original Arrow metadata before it is declared verified.
					malformed = []byte("PAR1invalid fixture footerPAR1")
					if err := os.Chmod(broken.Path, 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(broken.Path, malformed, 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.Chmod(broken.Path, 0400); err != nil {
						t.Fatal(err)
					}
				}
				rewriteVerificationManifest(t, s, broken.Generation, func(m *storeManifest) {
					switch defect {
					case "rows":
						m.Rows++
					case "schema_hash":
						m.SchemaHash = strings.Repeat("0", 64)
					case "malformed_parquet":
						digest := sha256.Sum256(malformed)
						m.SHA256 = hex.EncodeToString(digest[:])
						m.Bytes = int64(len(malformed))
					}
					if defect != "malformed_parquet" && m.SHA256 != broken.SHA256 {
						t.Fatal("metadata-only corruption changed the payload digest")
					}
				})
				_, err := s.Verify(context.Background(), "events")
				if affected == "current" && !errors.Is(err, ErrCorrupt) {
					t.Fatalf("Verify accepted invalid current metadata: %v", err)
				}
				if affected == "target" && err != nil {
					t.Fatalf("unrelated retained corruption broke current Verify: %v", err)
				}
				inventory, err := s.Inventory(context.Background(), "events")
				if err != nil || len(inventory) != 2 {
					t.Fatalf("Inventory failed instead of classifying readable metadata: %v", err)
				}
				for _, entry := range inventory {
					if entry.Verified == (entry.Snapshot.Generation == broken.Generation) {
						t.Fatal("Inventory returned incorrect verification status")
					}
				}
				if _, err := s.Restore(context.Background(), restoreRequest(first, second)); !errors.Is(err, ErrCorrupt) {
					t.Fatalf("Restore accepted invalid current or target metadata: %v", err)
				}
				current, err := s.Status("events")
				if err != nil || current.Generation != second.Generation {
					t.Fatalf("failed Restore changed current pointer: %v", err)
				}
			})
		}
	}
}

func TestSingleVerificationLegacySchemaHashRemainsOptional(t *testing.T) {
	s, _ := newTestStore(t)
	snapshot := commitRecoverySnapshot(t, s, "id", "config-v1")
	rewriteVerificationManifest(t, s, snapshot.Generation, func(m *storeManifest) { m.SchemaHash = "" })
	verified, err := s.Verify(context.Background(), "events")
	if err != nil || verified.SchemaHash != "" || verified.Generation != snapshot.Generation {
		t.Fatalf("legacy valid Parquet was rejected or rewritten: %v", err)
	}
	inventory, err := s.Inventory(context.Background(), "events")
	if err != nil || len(inventory) != 1 || !inventory[0].Verified {
		t.Fatalf("legacy Inventory: %v", err)
	}
	restored, err := s.Restore(context.Background(), restoreRequest(snapshot, snapshot))
	if err != nil || restored.SchemaHash != "" || !restored.RefreshedAt.Equal(snapshot.RefreshedAt) {
		t.Fatalf("legacy Restore changed contract or freshness: %v", err)
	}
	rewriteVerificationManifest(t, s, snapshot.Generation, func(m *storeManifest) { m.Rows++ })
	if _, err := s.Verify(context.Background(), "events"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("legacy missing schema hash bypassed row verification: %v", err)
	}
}

func TestSingleAndMultipartVerificationRejectFooterRowMismatch(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		name := "single"
		if multipart {
			name = "multipart"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := newTestStore(t)
			var snapshot Snapshot
			if multipart {
				snapshot = commitMultipartTest(t, s, 1)
			} else {
				snapshot = commitRecoverySnapshot(t, s, "id", "config-v1")
			}
			rewriteVerificationManifest(t, s, snapshot.Generation, func(m *storeManifest) {
				m.Rows++
				if multipart {
					m.Parts[0].Rows++
					m.SHA256 = multipartDigest(m.Parts)
				}
			})
			if _, err := s.Verify(context.Background(), "events"); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("footer row mismatch passed: %v", err)
			}
		})
	}
}
