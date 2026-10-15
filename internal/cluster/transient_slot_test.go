// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// Two things keep a slot from being reused too early, and a scan in another
// gateway relies on the first alone: slotReusable decides what any Submit may
// overwrite, and the freed-slot hint (which skips slotReusable by updating a
// revision this process saw) must name only slots slotReusable would allow.
// Each is pinned on its own here, with no broker.

func TestSlotReusableRules(t *testing.T) {
	lease := 5 * time.Second
	s := &NATSStore{policy: Policy{LeaseDuration: lease}}
	now := time.Now()
	fresh, aged := now.Add(-time.Second), now.Add(-2*lease)
	live := now.Add(time.Minute)
	type result struct{ strict, saturated bool }
	for _, c := range []struct {
		name string
		job  Job
		want result
	}{
		{"queued", Job{State: Queued, ExpiresAt: live, HeartbeatAt: aged}, result{false, false}},
		{"expired and not terminal", Job{State: Running, ExpiresAt: now.Add(-time.Second), HeartbeatAt: aged}, result{true, true}},
		{"plain terminal, fresh", Job{State: Succeeded, ExpiresAt: live, HeartbeatAt: fresh}, result{false, true}}, // only when saturated
		{"plain terminal, aged", Job{State: Failed, ExpiresAt: live, HeartbeatAt: aged}, result{true, true}},
		{"transient succeeded, fresh", Job{Transient: true, State: Succeeded, Claim: "c", ExpiresAt: live, HeartbeatAt: fresh}, result{true, true}},
		{"transient cancelled before claim, fresh", Job{Transient: true, State: Cancelled, ExpiresAt: live, HeartbeatAt: fresh}, result{true, true}},
		{"transient failed before claim, fresh", Job{Transient: true, State: Failed, ExpiresAt: live, HeartbeatAt: fresh}, result{true, true}},
		{"transient cancelled after claim, fresh", Job{Transient: true, State: Cancelled, Claim: "c", ExpiresAt: live, HeartbeatAt: fresh}, result{false, false}},
		{"transient failed after claim, fresh", Job{Transient: true, State: Failed, Claim: "c", ExpiresAt: live, HeartbeatAt: fresh}, result{false, false}},
		{"transient cancelled after claim, aged", Job{Transient: true, State: Cancelled, Claim: "c", ExpiresAt: live, HeartbeatAt: aged}, result{true, true}},
		{"transient queued", Job{Transient: true, State: Queued, ExpiresAt: live, HeartbeatAt: aged}, result{false, false}},
	} {
		if got := (result{s.slotReusable(c.job, now, true), s.slotReusable(c.job, now, false)}); got != c.want {
			t.Errorf("%s: strict/saturated = %v, want %v", c.name, got, c.want)
		}
	}
}

// The hint is pushed exactly for the ends slotReusable frees at once.
func TestFreedSlotHintNamesOnlySlotsThatAreFreeAtOnce(t *testing.T) {
	for _, c := range []struct {
		name      string
		transient bool
		claimed   bool
		end       string
		hint      bool
	}{
		{"transient succeeded", true, true, Succeeded, true},
		{"transient cancelled before claim", true, false, Cancelled, true},
		{"transient failed before claim", true, false, Failed, true},
		{"transient cancelled after claim", true, true, Cancelled, false},
		{"transient failed after claim", true, true, Failed, false},
		{"plain succeeded", false, true, Succeeded, false},
		{"plain failed before claim", false, false, Failed, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := testPolicy()
			p.Workers = map[string]int{"a1": 2}
			store := &NATSStore{policy: p, kv: &principalKV{}}
			ctx := context.Background()
			request := query.Request{Mode: "federated", SQL: "SELECT 1"}
			var snap Snapshot
			var err error
			if c.transient {
				snap, err = store.SubmitTransient(ctx, request)
			} else {
				snap, err = store.Submit(ctx, request)
			}
			if err != nil {
				t.Fatal(err)
			}
			step := func(mutate func(*Job)) {
				j := snap.Job
				mutate(&j)
				if snap, err = store.CompareAndSwap(ctx, snap, j); err != nil {
					t.Fatalf("to %s: %v", j.State, err)
				}
			}
			if c.claimed {
				step(func(j *Job) { j.State, j.WorkerID, j.Owner = Assigned, "a1", randomHex(16) })
				step(func(j *Job) { j.State, j.Claim = Claimed, randomHex(16) })
				if c.end == Succeeded {
					step(func(j *Job) { j.State = Running })
					step(func(j *Job) { j.State = ResultReady })
				}
			}
			step(func(j *Job) {
				j.State = c.end
				if c.end != Succeeded {
					j.Error = query.PublicError(query.NewError("QUERY_FAILED", fmt.Sprint(c.end)))
				}
			})
			store.freedMu.Lock()
			hints := len(store.freed)
			store.freedMu.Unlock()
			if (hints == 1) != c.hint || hints > 1 {
				t.Fatalf("%d reuse hints, want a hint=%v", hints, c.hint)
			}
		})
	}
}
