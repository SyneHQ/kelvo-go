//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
)

// Phase 4 gate: read capacity changes by restarting the node with different
// settings, against the NATS metadata record and audit journal the first start
// provisioned. Nothing is deleted, re-initialized or replaced between starts.
// Runs against the real broker provisioned by scripts/cluster_fixture.py.
func TestReadCapacityChangesByRestartOnly(t *testing.T) {
	natsConfig, policy := fixtureConfig(t)
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(policyJSON), "read_workers") || strings.Contains(string(policyJSON), "max_in_flight") || strings.Contains(string(policyJSON), "max_pending") {
		t.Fatalf("read capacity leaked into the cluster policy: %s", policyJSON)
	}
	scope := audit.Scope{ServiceID: "restart-gate", ServiceKind: "worker", Tenants: []string{policy.TenantID}}
	journalDir := filepath.Join(t.TempDir(), "journal")
	type start struct {
		reads   ReadWorkersConfig
		pending int
	}
	starts := []start{
		{ReadWorkersConfig{MaxWorkers: 1}.withDefaults(), 8},
		{ReadWorkersConfig{MaxWorkers: 4, MaxInFlight: 32, PoolSize: 8, IdleTTL: time.Minute, QueueWait: 2 * time.Second}.withDefaults(), 2}, // below the receipts held across this restart
		{ReadWorkersConfig{MaxWorkers: 2, MaxInFlight: 8}.withDefaults(), 64},
	}
	var record string
	for i, s := range starts {
		if err := s.reads.validate(&ResourceConfig{MaxConcurrent: 4}); err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		// The first start provisions; every restart verifies against what exists.
		store, err := OpenStore(ctx, natsConfig, policy, i == 0)
		if err != nil {
			cancel()
			t.Fatalf("start %d (read_workers %+v): store: %v", i, s.reads, err)
		}
		meta, err := store.js.KeyValue(ctx, metaBucket)
		if err != nil {
			cancel()
			t.Fatalf("start %d: metadata bucket: %v", i, err)
		}
		entry, err := meta.Get(ctx, metadataKey)
		if err != nil {
			cancel()
			t.Fatalf("start %d: metadata read: %v", i, err)
		}
		if i == 0 {
			record = string(entry.Value())
		} else if string(entry.Value()) != record {
			cancel()
			t.Fatalf("start %d rewrote the cluster metadata record", i)
		}
		store.Close()
		cancel()
		journal, err := audit.Open(audit.Config{Directory: journalDir, MaxEntries: 128, MaxPending: s.pending, Retention: time.Hour, WriteTimeout: 2 * time.Second}, scope)
		if err != nil {
			t.Fatalf("start %d (max_pending %d): journal: %v", i, s.pending, err)
		}
		if got := journal.Snapshot().MaxPending; got != s.pending {
			t.Fatalf("start %d: journal limit %d, want %d", i, got, s.pending)
		}
		binding := audit.Binding{TenantID: policy.TenantID, ServiceID: scope.ServiceID, ServiceKind: scope.ServiceKind, PrincipalID: "analyst-one", PrincipalKind: "user", PolicyVersion: strings.Repeat("a", 64)}
		switch i {
		case 0:
			// Leave three executions in flight and three finished when the node stops.
			for n := range 6 {
				r, err := journal.Begin(context.Background(), binding, audit.QuerySubmit)
				if err != nil {
					t.Fatalf("begin %d: %v", n, err)
				}
				if n < 3 {
					if err := r.Finish(context.Background(), audit.Succeeded, audit.None); err != nil {
						t.Fatal(err)
					}
				}
			}
		case 1:
			// max_pending dropped below the six receipts the journal holds: they
			// are all retained, the in-flight ones are finalized as interrupted,
			// and the new limit applies to new work only.
			if s := journal.Snapshot(); s.Retained != 6 || s.Active != 0 || s.MaxPending != 2 {
				t.Fatalf("restart with lower max_pending: %+v", s)
			}
			var held []*audit.Receipt
			for range 2 {
				r, err := journal.Begin(context.Background(), binding, audit.QuerySubmit)
				if err != nil {
					t.Fatalf("new work under the lower limit: %v", err)
				}
				held = append(held, r)
			}
			if _, err := journal.Begin(context.Background(), binding, audit.QuerySubmit); !errors.Is(err, audit.ErrBusy) {
				t.Fatalf("lower max_pending not enforced: %v", err)
			}
			for _, r := range held {
				if err := r.Finish(context.Background(), audit.Succeeded, audit.None); err != nil {
					t.Fatal(err)
				}
			}
		default:
			if s := journal.Snapshot(); s.Retained != 8 || s.Active != 0 {
				t.Fatalf("raised max_pending lost receipts: %+v", s)
			}
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := journal.Close(closeCtx); err != nil {
			t.Fatal(err)
		}
		closeCancel()
		if i == 1 {
			counts := map[audit.Outcome]int{}
			for cursor := 0; ; {
				page, err := audit.ReadPage(context.Background(), journalDir, cursor, 16)
				if err != nil {
					t.Fatal(err)
				}
				for _, e := range page.Events {
					counts[e.Outcome]++
					if e.Outcome == audit.Unknown && e.Category != audit.Interrupted {
						t.Fatalf("in-flight receipt finalized as %q", e.Category)
					}
				}
				if page.Done {
					break
				}
				cursor = page.Next
			}
			if counts[audit.Succeeded] != 5 || counts[audit.Unknown] != 3 {
				t.Fatalf("receipts after restart (3 finished, 3 interrupted, 2 new): %v", counts)
			}
		}
	}
}
