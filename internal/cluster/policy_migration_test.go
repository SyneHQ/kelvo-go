// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestDecidePolicyMigration(t *testing.T) {
	from, to := []byte(`{"a":1}`), []byte(`{"a":2}`)
	if state, err := decidePolicyMigration(from, from, to); err != nil || state != PolicyMigrationPending {
		t.Fatalf("old stored: %v %v", state, err)
	}
	if state, err := decidePolicyMigration(to, from, to); err != nil || state != PolicyMigrationCurrent {
		t.Fatalf("new stored: %v %v", state, err)
	}
	_, err := decidePolicyMigration([]byte(`{"a":3,"secret":"x"}`), from, to)
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unknown stored value must be refused without echoing it: %v", err)
	}
}

func TestPolicyJSONMatchesStoredMetadataFormat(t *testing.T) {
	policy := Policy{TenantID: "fmt", MaxQueries: 2, JobTTL: 45 * time.Second, LeaseDuration: 5 * time.Second,
		Replicas: 1, Limits: query.DefaultLimits(), Workers: map[string]int{"worker-a": 2}}
	raw, err := policyJSON(policy)
	if err != nil {
		t.Fatal(err)
	}
	clone, err := clonePolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	// openClusterMetadata stores json.Marshal of the cloned, validated policy.
	want, _ := json.Marshal(clone)
	if string(raw) != string(want) {
		t.Fatalf("policyJSON diverged from the stored metadata format")
	}
}

func TestMigratePolicyIntegration(t *testing.T) {
	cfg, from := fixtureConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := OpenStore(ctx, cfg, from, true)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	_ = store.Close()
	to := from
	to.Limits.MemoryMB = from.Limits.MemoryMB * 2

	if state, err := MigratePolicy(ctx, cfg, from, to, false); err != nil || state != PolicyMigrationPending {
		t.Fatalf("dry run: %v %v", state, err)
	}
	if _, err := OpenStore(ctx, cfg, from, false); err != nil {
		t.Fatalf("a dry run must not change the stored policy: %v", err)
	}
	if state, err := MigratePolicy(ctx, cfg, from, to, true); err != nil || state != PolicyMigrationApplied {
		t.Fatalf("apply: %v %v", state, err)
	}
	t.Cleanup(func() {
		if _, err := MigratePolicy(context.Background(), cfg, to, from, true); err != nil {
			t.Errorf("restore fixture policy: %v", err)
		}
	})
	if s, err := OpenStore(ctx, cfg, to, false); err != nil {
		t.Fatalf("a process with the new policy must start: %v", err)
	} else {
		_ = s.Close()
	}
	if _, err := OpenStore(ctx, cfg, from, false); err == nil {
		t.Fatal("a process with the old policy must be refused")
	}
	if state, err := MigratePolicy(ctx, cfg, from, to, true); err != nil || state != PolicyMigrationCurrent {
		t.Fatalf("re-run: %v %v", state, err)
	}
	stranger := from
	stranger.Limits.MemoryMB = from.Limits.MemoryMB * 3
	if _, err := MigratePolicy(ctx, cfg, stranger, from, true); err == nil {
		t.Fatal("a stored policy that is neither old nor new must be refused")
	}
	if _, err := MigratePolicy(ctx, cfg, from, from, true); err == nil {
		t.Fatal("identical policies must be refused")
	}
}
