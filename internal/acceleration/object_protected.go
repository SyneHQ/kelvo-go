// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
	"go.yaml.in/yaml/v3"
)

const protectedManifestVersion = 5

var ErrProtectionRequired = errors.New("object snapshot requires its configured protected runtime and manifest")

// A failed Close must not remain classifiable as a missing/stale generation and
// trigger fallback. Callers keep their operation/consumer custody until it joins.
func finishProtectedRead(body io.ReadCloser, resultErr *error) {
	if err := body.Close(); err != nil {
		*resultErr = fmt.Errorf("%w: protected object response did not close cleanly", errReaderCleanupUnknown)
	}
}

func (backend *objectBackend) protected() bool {
	return backend.config.ObjectStorage != nil && backend.config.ObjectStorage.ReaderRegistry != nil
}

func (backend *objectBackend) manifestVersion() int {
	if backend.protected() {
		return protectedManifestVersion
	}
	return 4
}

func sameReaderBinding(left, right *readerlease.Binding) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func sameReaderReference(left, right *readerlease.Reference) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func validProtectedReference(ref *readerlease.Reference, dataset, generation string) bool {
	return ref != nil && storeTenantID.MatchString(ref.Tenant) && ref.Dataset == dataset &&
		ref.Generation == generation && storeGenerationID.MatchString(ref.Generation) && storeDigest.MatchString(ref.Incarnation)
}

func validateManifestProtection(manifest objectManifest) error {
	protected := manifest.Version == protectedManifestVersion
	if writer := manifest.Writer; writer != nil {
		if protected != (writer.ReaderReference != nil) || protected && !validProtectedReference(writer.ReaderReference, manifest.Dataset, writer.Owner) {
			return fmt.Errorf("%w: invalid writer protection identity", ErrCorrupt)
		}
	}
	for _, committed := range append([]*objectCommitted{manifest.Committed}, manifest.History...) {
		if committed == nil {
			continue
		}
		binding := committed.ReaderBinding
		if protected != (binding != nil) || protected && (!validProtectedReference(&binding.Reference, manifest.Dataset, committed.Generation) || !storeDigest.MatchString(binding.ContentSHA256)) {
			return fmt.Errorf("%w: invalid generation protection identity", ErrCorrupt)
		}
	}
	return nil
}

func cloneObjectCommit(source *objectCommitted) *objectCommitted {
	if source == nil {
		return nil
	}
	result := *source
	if source.Descriptor != nil {
		copy := *source.Descriptor
		result.Descriptor = &copy
	}
	if source.ReaderBinding != nil {
		copy := *source.ReaderBinding
		result.ReaderBinding = &copy
	}
	return &result
}

func (tx *objectTransaction) writerLease(expires time.Time) *objectWriterLease {
	lease := &objectWriterLease{Owner: tx.owner, ExpiresAt: expires}
	if tx.readerReference != nil {
		copy := *tx.readerReference
		lease.ReaderReference = &copy
	}
	return lease
}

func (tx *objectTransaction) ownsWriter(state objectState) bool {
	writer := state.manifest.Writer
	return writer != nil && writer.Owner == tx.owner && writer.ExpiresAt.After(state.now()) &&
		sameReaderReference(writer.ReaderReference, tx.readerReference)
}

// The writer renewer remains live throughout Seal. Its exact protected identity
// is checked again after renewal joins and before the conditional root write.
func (tx *objectTransaction) sealProtectedCommit(committed *objectCommitted) error {
	if tx.backend.runtime == nil || tx.backend.runtime.registry == nil || tx.readerReference == nil || committed.ReaderBinding != nil || !committed.RefreshedAt.IsZero() {
		return ErrProtectionRequired
	}
	state, err := tx.backend.readState(tx.ctx, tx.dataset, tx.client)
	if err != nil {
		return err
	}
	if !tx.ownsWriter(state) {
		return ErrLeaseLost
	}
	committed.RefreshedAt = state.now()
	binding, err := objectReaderBinding(*tx.readerReference, tx.backend.config.ObjectStorage.ObjectLocation, committed)
	if err != nil {
		return err
	}
	committed.ReaderBinding = &binding
	if err := tx.backend.runtime.registry.Seal(tx.ctx, binding, tx.owner); err != nil {
		if cause := context.Cause(tx.ctx); cause != nil {
			return cause
		}
		// Seal rereads and accepts only the identical reference, writer and digest.
		// Reconciliation never changes the identity or freshness of a sealed write.
		reconcile, cancel := context.WithTimeout(tx.ctx, 5*time.Second)
		defer cancel()
		if confirmErr := tx.backend.runtime.registry.Seal(reconcile, binding, tx.owner); confirmErr != nil {
			return errors.Join(err, confirmErr)
		}
	}
	return context.Cause(tx.ctx)
}

// Protected documents use canonical scalar types before typed decoding. Legacy
// parsing is unchanged; aliases, numeric coercion and duplicate keys cannot
// establish a protected identity or a different writer fence.
func validateProtectedYAML(raw []byte) error {
	bad := fmt.Errorf("%w: invalid protected manifest encoding", ErrCorrupt)
	var document yaml.Node
	if yaml.Unmarshal(raw, &document) != nil || len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return bad
	}
	var visit func(*yaml.Node, string) bool
	visit = func(node *yaml.Node, field string) bool {
		if node.Anchor != "" || node.Kind == yaml.AliasNode {
			return false
		}
		if node.Kind == yaml.MappingNode {
			seen := map[string]bool{}
			for i := 0; i < len(node.Content); i += 2 {
				key := node.Content[i]
				if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || key.Anchor != "" || seen[key.Value] {
					return false
				}
				seen[key.Value] = true
				if !visit(node.Content[i+1], key.Value) {
					return false
				}
			}
			return true
		}
		if node.Kind == yaml.SequenceNode {
			if field != "history" {
				return false
			}
			for _, child := range node.Content {
				if child.Kind != yaml.MappingNode || !visit(child, "") {
					return false
				}
			}
			return true
		}
		if node.Kind != yaml.ScalarNode {
			return false
		}
		switch field {
		case "version", "rows", "bytes", "part_count":
			value, err := strconv.ParseInt(node.Value, 10, 64)
			return node.Tag == "!!int" && err == nil && value >= 0 && strconv.FormatInt(value, 10) == node.Value
		case "history_truncated":
			return node.Tag == "!!bool" && (node.Value == "true" || node.Value == "false")
		case "refreshed_at", "expires_at":
			_, err := time.Parse(time.RFC3339Nano, node.Value)
			return node.Tag == "!!timestamp" && err == nil
		default:
			return node.Tag == "!!str"
		}
	}
	if !visit(document.Content[0], "") {
		return bad
	}
	return nil
}
