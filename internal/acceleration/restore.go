// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"go.yaml.in/yaml/v3"
)

var (
	ErrRecoveryUnsupported = errors.New("generation recovery is unsupported by this backend")
	ErrInventoryLimit      = errors.New("generation inventory exceeds the 256 entry limit; prune retained generations")
	ErrRestoreConflict     = errors.New("current generation changed since restore was requested")
)

const InventoryLimit = 256

// RecoveryBackend is an operator-only, optional backend capability. Callers must
// derive Fingerprint from their currently authorized catalog, never user input.
type RecoveryBackend interface {
	Inventory(context.Context, string) ([]Generation, error)
	Restore(context.Context, RestoreRequest) (Snapshot, error)
}
type RestoreRequest struct {
	Dataset            string
	Generation         string
	Fingerprint        string
	ExpectedGeneration string
}
type Generation struct {
	CatalogScope     string   `yaml:"catalog_scope,omitempty"`
	CatalogTruncated bool     `yaml:"catalog_truncated,omitempty"`
	Snapshot         Snapshot `yaml:"snapshot"`
	Active           bool     `yaml:"active"`
	Verified         bool     `yaml:"verified"`
}

func generationManifestName(generation string) string { return generation + ".yaml" }

// Persist immutable metadata before publishing current.yaml. Existing sidecars
// must describe precisely the same generation; they are never overwritten.
func storeSaveGeneration(dir *os.File, manifest storeManifest) error {
	name := generationManifestName(manifest.Generation)
	data, err := yaml.Marshal(manifest)
	if err != nil {
		return err
	}
	if len(data) > storeManifestLimit {
		return fmt.Errorf("%w: generation manifest exceeds size limit", ErrCorrupt)
	}
	err = storeWriteManifest(dir, name, data)
	if errors.Is(err, os.ErrExist) {
		prior, readErr := storeReadManifestNamed(dir, manifest.Dataset, name)
		if readErr != nil {
			return readErr
		}
		if !reflect.DeepEqual(prior, manifest) {
			return fmt.Errorf("%w: generation metadata changed", ErrCorrupt)
		}
		return nil
	}
	return err
}

// Verify checksum before parsing any Parquet footer. Metadata hashes are checked
// against the actual Arrow schema, not accepted merely because a sidecar says so.
func storeVerifyGeneration(ctx context.Context, dir *os.File, manifest storeManifest) (*arrow.Schema, error) {
	payload, err := storeValidatePayload(dir, manifest)
	if err != nil {
		return nil, err
	}
	defer payload.Close()
	digest, err := storeHash(ctx, payload)
	if err != nil {
		return nil, err
	}
	if digest != manifest.SHA256 {
		return nil, fmt.Errorf("%w: generation checksum mismatch", ErrCorrupt)
	}
	schema, err := ReadParquetSchema(payload, manifest.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: generation Parquet schema is invalid", ErrCorrupt)
	}
	schemaHash, err := SchemaFingerprint(schema)
	if err != nil {
		return nil, err
	}
	if manifest.SchemaHash != "" && manifest.SchemaHash != schemaHash {
		return nil, fmt.Errorf("%w: generation schema checksum mismatch", ErrCorrupt)
	}
	return schema, nil
}

func (s *Store) Inventory(ctx context.Context, dataset string) ([]Generation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := s.openDataset(dataset, false)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	// Prevent pruning while checksums are read; ordinary acquired readers continue.
	writer, err := storeLockNamed(ctx, dir, ".writer.lock", false)
	if err != nil {
		return nil, err
	}
	defer writer.Close()
	metadata, err := storeLockNamed(ctx, dir, ".metadata.lock", false)
	if err != nil {
		return nil, err
	}
	defer metadata.Close()
	current, err := storeReadManifest(dir, dataset)
	if err != nil {
		return nil, err
	}
	manifests := map[string]storeManifest{current.Generation: current}
	for {
		entries, readErr := dir.ReadDir(128)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			name := entry.Name()
			generation := strings.TrimSuffix(name, ".yaml")
			if name == generation || !storeGenerationID.MatchString(generation) {
				continue
			}
			manifest, err := storeReadManifestNamed(dir, dataset, name)
			if err != nil {
				return nil, err
			}
			if manifest.Generation != generation {
				return nil, fmt.Errorf("%w: generation sidecar identity mismatch", ErrCorrupt)
			}
			if generation == current.Generation && !reflect.DeepEqual(manifest, current) {
				return nil, fmt.Errorf("%w: current generation metadata differs", ErrCorrupt)
			}
			manifests[generation] = manifest
			if len(manifests) > InventoryLimit {
				return nil, ErrInventoryLimit
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	result := make([]Generation, 0, len(manifests))
	for _, manifest := range manifests {
		_, verifyErr := storeVerifyGeneration(ctx, dir, manifest)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result = append(result, Generation{Snapshot: manifest.snapshot(dir.Name()), Active: manifest.Generation == current.Generation, Verified: verifyErr == nil})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Snapshot.RefreshedAt.Equal(result[j].Snapshot.RefreshedAt) {
			return result[i].Snapshot.Generation < result[j].Snapshot.Generation
		}
		return result[i].Snapshot.RefreshedAt.After(result[j].Snapshot.RefreshedAt)
	})
	return result, nil
}

func (s *Store) Restore(ctx context.Context, request RestoreRequest) (snapshot Snapshot, err error) {
	if !storeDatasetID.MatchString(request.Dataset) || !storeGenerationID.MatchString(request.Generation) || !storeGenerationID.MatchString(request.ExpectedGeneration) || request.Fingerprint == "" || len(request.Fingerprint) > storeFingerprintLimit {
		return Snapshot{}, errors.New("restore requires dataset, generation, current generation and authorized fingerprint")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	dir, err := s.openDataset(request.Dataset, false)
	if err != nil {
		return Snapshot{}, err
	}
	defer dir.Close()
	writer, err := storeLockNamed(ctx, dir, ".writer.lock", true)
	if err != nil {
		return Snapshot{}, err
	}
	defer writer.Close()
	metadata, err := storeLockNamed(ctx, dir, ".metadata.lock", true)
	if err != nil {
		return Snapshot{}, err
	}
	defer metadata.Close()
	current, err := storeReadManifest(dir, request.Dataset)
	if err != nil {
		return Snapshot{}, err
	}
	if current.Generation != request.ExpectedGeneration {
		return Snapshot{}, ErrRestoreConflict
	}
	target := current
	if request.Generation != current.Generation {
		target, err = storeReadManifestNamed(dir, request.Dataset, generationManifestName(request.Generation))
		if err != nil {
			return Snapshot{}, err
		}
	}
	if target.Generation != request.Generation {
		return Snapshot{}, fmt.Errorf("%w: target generation identity mismatch", ErrCorrupt)
	}
	// Old authorization/configuration cannot be revived by reverting a pointer.
	if target.Fingerprint != request.Fingerprint || current.Fingerprint != request.Fingerprint {
		return Snapshot{}, ErrFingerprintMismatch
	}
	targetSchema, err := storeVerifyGeneration(ctx, dir, target)
	if err != nil {
		return Snapshot{}, err
	}
	currentSchema, err := storeVerifyGeneration(ctx, dir, current)
	if err != nil {
		return Snapshot{}, fmt.Errorf("current generation must be verifiable before restore: %w", err)
	}
	if !SchemaEqual(currentSchema, targetSchema) {
		return Snapshot{}, fmt.Errorf("%w: restore would change the schema contract", ErrCorrupt)
	}
	if request.Generation == current.Generation {
		return current.snapshot(dir.Name()), nil
	}
	if err := storeSaveGeneration(dir, current); err != nil {
		return Snapshot{}, err
	}
	data, err := yaml.Marshal(target)
	if err != nil {
		return Snapshot{}, err
	}
	stage := ".restore-" + request.Generation + ".yaml"
	// A previous crash can leave this unpublished stage. Its deterministic name
	// is private; refuse a link or insecure file before removing it.
	stale, openErr := storeOpenFile(dir, stage, os.O_RDONLY, 0)
	if openErr == nil {
		checkErr := storeCheckPrivate(stale, false, true)
		stale.Close()
		if checkErr != nil {
			return Snapshot{}, checkErr
		}
		if err := storeRemove(dir, stage); err != nil {
			return Snapshot{}, err
		}
	} else if !errors.Is(openErr, os.ErrNotExist) {
		return Snapshot{}, openErr
	}
	if err := storeWriteManifest(dir, stage, data); err != nil {
		return Snapshot{}, err
	}
	defer storeRemove(dir, stage)
	if err := dir.Sync(); err != nil {
		return Snapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err := storeRename(dir, stage, storeManifestName); err != nil {
		return Snapshot{}, err
	}
	snapshot = target.snapshot(dir.Name()) // never reset RefreshedAt during restore
	if err := dir.Sync(); err != nil {
		return snapshot, fmt.Errorf("snapshot restored but directory durability is uncertain: %w", err)
	}
	return snapshot, nil
}

var _ RecoveryBackend = (*localBackend)(nil)
var _ RecoveryBackend = (*objectBackend)(nil)
