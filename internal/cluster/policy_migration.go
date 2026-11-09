// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// PolicyMigration is the outcome of comparing (and optionally replacing) the
// cluster policy stored in NATS metadata.
type PolicyMigration string

const (
	// PolicyMigrationPending: the stored policy is the expected old one; a run
	// with apply would replace it.
	PolicyMigrationPending PolicyMigration = "pending"
	// PolicyMigrationApplied: the stored policy was replaced by the new one.
	PolicyMigrationApplied PolicyMigration = "applied"
	// PolicyMigrationCurrent: the stored policy already is the new one.
	PolicyMigrationCurrent PolicyMigration = "current"
)

// policyJSON is the exact metadata value OpenStore compares: the validated,
// cloned policy marshaled by encoding/json.
func policyJSON(p Policy) ([]byte, error) {
	p, err := clonePolicy(p)
	if err != nil {
		return nil, errors.New("invalid cluster policy")
	}
	if err := ValidatePolicy(p); err != nil {
		return nil, fmt.Errorf("invalid cluster policy: %w", err)
	}
	return json.Marshal(p)
}

func shortHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:6])
}

// decidePolicyMigration only ever moves the stored value from exactly the
// expected old policy to the new one. Anything else is refused, reporting
// hashes rather than policy contents.
func decidePolicyMigration(stored, from, to []byte) (PolicyMigration, error) {
	switch {
	case bytes.Equal(stored, to):
		return PolicyMigrationCurrent, nil
	case bytes.Equal(stored, from):
		return PolicyMigrationPending, nil
	default:
		return "", fmt.Errorf("stored cluster policy matches neither the old nor the new policy (stored %s, old %s, new %s)",
			shortHash(stored), shortHash(from), shortHash(to))
	}
}

// MigratePolicy replaces the cluster policy stored in NATS metadata, which
// cluster-init, the gateway and nodes compare byte for byte before they start.
// It connects with the tenant's NATS settings (the provisioner identity), and
// with apply it writes the new policy with a compare-and-swap on the entry's
// revision, so a concurrent change makes it fail instead of overwrite. Run it
// immediately before deploying gateway and node configs carrying the new policy:
// processes started with the old policy will refuse to restart afterwards.
func MigratePolicy(parent context.Context, c NATSConfig, from, to Policy, apply bool) (PolicyMigration, error) {
	if from.TenantID != to.TenantID {
		return "", errors.New("old and new policies belong to different tenants")
	}
	fromRaw, err := policyJSON(from)
	if err != nil {
		return "", fmt.Errorf("old policy: %w", err)
	}
	toRaw, err := policyJSON(to)
	if err != nil {
		return "", fmt.Errorf("new policy: %w", err)
	}
	if bytes.Equal(fromRaw, toRaw) {
		return "", errors.New("old and new policies are identical")
	}
	nc, js, err := dialJetStream(c)
	if err != nil {
		return "", err
	}
	defer nc.Close()
	return migratePolicyValue(parent, js, fromRaw, toRaw, apply)
}

func migratePolicyValue(parent context.Context, js jetstream.JetStream, fromRaw, toRaw []byte, apply bool) (PolicyMigration, error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	meta, err := js.KeyValue(ctx, metaBucket)
	if err != nil {
		return "", errors.New("cluster metadata is missing")
	}
	entry, err := meta.Get(ctx, metadataKey)
	if err != nil {
		return "", errors.New("cluster metadata value is missing")
	}
	state, err := decidePolicyMigration(entry.Value(), fromRaw, toRaw)
	if err != nil || state != PolicyMigrationPending || !apply {
		return state, err
	}
	if _, err := meta.Update(ctx, metadataKey, toRaw, entry.Revision()); err != nil {
		return "", errors.New("cluster metadata changed during migration; nothing was written")
	}
	return PolicyMigrationApplied, nil
}
