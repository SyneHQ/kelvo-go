// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/apache/arrow-go/v18/arrow"
	"go.yaml.in/yaml/v3"
)

// ObjectHistoryLimit bounds the retained manifest catalog, not physical storage.
// Dropped metadata never authorizes deleting immutable objects or reader data.
const ObjectHistoryLimit = 16
const objectHistoryLeaseReserve = 1024

func validateObjectCommit(snapshot *objectCommitted) error {
	if snapshot == nil || (snapshot.SchemaHash != "" && !storeDigest.MatchString(snapshot.SchemaHash)) || !storeGenerationID.MatchString(snapshot.Generation) || snapshot.Fingerprint == "" || len(snapshot.Fingerprint) > storeFingerprintLimit || !storeDigest.MatchString(snapshot.SHA256) || snapshot.Rows < 0 || snapshot.Bytes <= 0 || snapshot.RefreshedAt.IsZero() {
		return fmt.Errorf("%w: invalid remote snapshot fields", ErrCorrupt)
	}
	if snapshot.Descriptor == nil {
		if snapshot.Bytes > objectstore.MaxUploadBytes || !validObjectVersion(snapshot.ObjectVersion) {
			return fmt.Errorf("%w: invalid remote single-file snapshot", ErrCorrupt)
		}
	} else {
		ref := snapshot.Descriptor
		if snapshot.ObjectVersion != "" || snapshot.Bytes > 1<<40 || !storeDigest.MatchString(snapshot.SchemaHash) || !validObjectVersion(ref.ObjectVersion) || ref.Bytes <= 0 || ref.Bytes > objectDescriptorLimit || ref.PartCount < 1 || ref.PartCount > 256 {
			return fmt.Errorf("%w: invalid remote multipart snapshot", ErrCorrupt)
		}
	}
	return nil
}
func nextObjectManifest(previous objectManifest, next *objectCommitted) (objectManifest, error) {
	manifest := objectManifest{Version: 4, Dataset: previous.Dataset, Committed: next, HistoryTruncated: previous.HistoryTruncated}
	if err := validateObjectCommit(next); err != nil {
		return manifest, err
	}
	candidates := append([]*objectCommitted{}, previous.Committed)
	candidates = append(candidates, previous.History...)
	seen := map[string]bool{next.Generation: true}
	for _, candidate := range candidates {
		if candidate == nil || seen[candidate.Generation] {
			continue
		}
		seen[candidate.Generation] = true
		if len(manifest.History) == ObjectHistoryLimit {
			manifest.HistoryTruncated = true
			continue
		}
		copied := *candidate
		manifest.History = append(manifest.History, &copied)
	}
	for {
		encoded, err := yaml.Marshal(manifest)
		if err != nil {
			return manifest, err
		}
		if len(encoded) <= storeManifestLimit-objectHistoryLeaseReserve {
			return manifest, nil
		}
		if len(manifest.History) == 0 {
			return manifest, errors.New("remote generation metadata exceeds recovery catalog budget")
		}
		manifest.History = manifest.History[:len(manifest.History)-1]
		manifest.HistoryTruncated = true
	}
}

func (backend *objectBackend) verifyObjectGeneration(ctx context.Context, dataset string, committed *objectCommitted, reference time.Time) (Snapshot, *arrow.Schema, error) {
	snapshot, err := backend.loadObjectSnapshot(ctx, dataset, committed, reference, backend.reader)
	if err != nil {
		return Snapshot{}, nil, err
	}
	schema, err := backend.verifyObjectSnapshot(ctx, snapshot)
	if err != nil {
		return Snapshot{}, nil, err
	}
	return snapshot, schema, nil
}

// Inventory describes only current and retained manifest entries. It never lists
// storage. Legacy untracked generations and dropped entries remain undiscovered.
func (backend *objectBackend) Inventory(ctx context.Context, dataset string) ([]Generation, error) {
	if _, ok := backend.reader.(objectstore.RangeClient); !ok {
		return nil, ErrRecoveryUnsupported
	}
	if err := backend.check(ctx, dataset); err != nil {
		return nil, err
	}
	state, err := backend.readState(ctx, dataset, backend.reader)
	if err != nil {
		return nil, err
	}
	if state.manifest.Committed == nil {
		return nil, ErrNotFound
	}
	candidates := append([]*objectCommitted{state.manifest.Committed}, state.manifest.History...)
	result := make([]Generation, 0, len(candidates))
	for index, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		snapshot, err := backend.loadObjectSnapshot(ctx, dataset, candidate, state.now(), backend.reader)
		if err != nil {
			return nil, err
		}
		_, _, verifyErr := backend.verifyObjectGeneration(ctx, dataset, candidate, state.now())
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result = append(result, Generation{Snapshot: snapshot, Active: index == 0, Verified: verifyErr == nil, CatalogScope: "retained_manifest", CatalogTruncated: state.manifest.HistoryTruncated})
	}
	return result, nil
}

// Restore serializes with refresh through the same writer lease, then swaps
// only the current pointer and retained catalog. All data checks use the exact
// immutable object versions, with bounded streaming checksum and range reads.
func (backend *objectBackend) Restore(ctx context.Context, request RestoreRequest) (snapshot Snapshot, err error) {
	if _, ok := backend.reader.(objectstore.RangeClient); !ok {
		return Snapshot{}, ErrRecoveryUnsupported
	}
	if !storeDatasetID.MatchString(request.Dataset) || !storeGenerationID.MatchString(request.Generation) || !storeGenerationID.MatchString(request.ExpectedGeneration) || request.Fingerprint == "" || len(request.Fingerprint) > storeFingerprintLimit {
		return Snapshot{}, errors.New("restore requires dataset, generation, current generation and authorized fingerprint")
	}
	writer, err := backend.Begin(ctx, request.Dataset)
	if err != nil {
		return Snapshot{}, err
	}
	tx := writer.(*objectTransaction)
	tx.finishMu.Lock()
	defer tx.finishMu.Unlock()
	tx.done = true
	defer func() {
		if cause := context.Cause(tx.ctx); err != nil && cause != nil {
			err = errors.Join(err, cause)
		}
		err = errors.Join(err, tx.cleanup())
	}()
	tx.stateMu.Lock()
	initial := tx.state
	tx.stateMu.Unlock()
	current := initial.manifest.Committed
	if current == nil {
		return Snapshot{}, ErrNotFound
	}
	if current.Generation != request.ExpectedGeneration {
		return Snapshot{}, ErrRestoreConflict
	}
	target := current
	if current.Generation != request.Generation {
		target = nil
		for _, candidate := range initial.manifest.History {
			if candidate.Generation == request.Generation {
				target = candidate
				break
			}
		}
	}
	if target == nil {
		return Snapshot{}, ErrNotFound
	}
	if target.Fingerprint != request.Fingerprint || current.Fingerprint != request.Fingerprint {
		return Snapshot{}, ErrFingerprintMismatch
	}
	targetSnapshot, targetSchema, err := backend.verifyObjectGeneration(tx.ctx, request.Dataset, target, initial.now())
	if err != nil {
		return Snapshot{}, err
	}
	currentSchema := targetSchema
	if current.Generation != target.Generation {
		_, currentSchema, err = backend.verifyObjectGeneration(tx.ctx, request.Dataset, current, initial.now())
		if err != nil {
			return Snapshot{}, err
		}
	}
	if !SchemaEqual(currentSchema, targetSchema) {
		return Snapshot{}, fmt.Errorf("%w: restore would change the schema contract", ErrCorrupt)
	}
	tx.stopRenewal()
	if err := context.Cause(tx.ctx); err != nil {
		return Snapshot{}, err
	}
	state, err := backend.readState(tx.ctx, request.Dataset, tx.client)
	if err != nil {
		return Snapshot{}, err
	}
	if state.manifest.Writer == nil || state.manifest.Writer.Owner != tx.owner || !state.manifest.Writer.ExpiresAt.After(state.now()) {
		return Snapshot{}, ErrLeaseLost
	}
	if !sameObjectCommit(state.manifest.Committed, current) {
		return Snapshot{}, ErrRestoreConflict
	}
	// A verified no-op is not a publication. Keep ownership so cleanup clears
	// our lease and reports any release failure; an unchanged pointer cannot
	// prove that an attempted publication/release actually reached storage.
	if current.Generation == target.Generation {
		return targetSnapshot.observeClock(state.now()), nil
	}
	manifest, err := nextObjectManifest(state.manifest, target)
	if err != nil {
		return Snapshot{}, err
	}
	published, err := backend.writeState(tx.ctx, tx.client, state, manifest)
	if err != nil {
		if errors.Is(err, objectstore.ErrConflict) {
			return Snapshot{}, ErrLeaseLost
		}
		tx.owned = false // ambiguous CAS must never clear another owner's state
		reconcile, cancel := context.WithTimeout(context.WithoutCancel(tx.ctx), 5*time.Second)
		defer cancel()
		latest, readErr := backend.readState(reconcile, request.Dataset, tx.client)
		if readErr == nil && sameObjectCommit(latest.manifest.Committed, target) {
			return targetSnapshot.observeClock(latest.now()), nil
		}
		return Snapshot{}, fmt.Errorf("%w: %w", ErrPublicationUnknown, errors.Join(err, readErr))
	}
	tx.owned = false
	return targetSnapshot.observeClock(published.now()), nil
}
