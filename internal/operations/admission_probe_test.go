// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	api "github.com/SYNEHQ/kelvo-go/operations"
)

// Occupy whichever shard admission visits first, so the fallback regression
// does not depend on random key placement. The seeded records use normal keyed
// submission and valid custody state in the underlying store.
type probeBackend struct {
	*memoryBackend
	getKeys []string
	before  func(context.Context, string)
}

func (b *probeBackend) Get(ctx context.Context, key string) (Entry, error) {
	b.getKeys = append(b.getKeys, key)
	if b.before != nil {
		b.before(ctx, key)
	}
	return b.memoryBackend.Get(ctx, key)
}

func probeFixture(t *testing.T, shards, slots int) *fixture {
	t.Helper()
	f := newFixture(t)
	policy := f.store.policy
	policy.Shards, policy.SlotsPerShard = shards, slots
	var err error
	f.store, err = New(f.backend, policy)
	if err != nil {
		t.Fatal(err)
	}
	f.store.now = func() time.Time { return f.now }
	return f
}

func readSubmission(t *testing.T, f *fixture, key string) Submission {
	t.Helper()
	input := f.input
	input.Request = api.Request{Version: api.Version, Kind: api.QueryRead,
		Connection: f.input.Request.Connection, IdempotencyKey: key,
		Spec: api.Spec{Query: &api.QuerySpec{SQL: "SELECT 1"}}}
	var err error
	input.RequestRef, _, err = api.SealRequest(input.Request, "read-request")
	if err != nil {
		t.Fatal(err)
	}
	return input
}

func occupyShard(t *testing.T, f *fixture, key string) {
	t.Helper()
	index, err := strconv.ParseUint(key[len(key)-4:], 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(f.backend, f.store.policy)
	if err != nil {
		t.Fatal(err)
	}
	store.now = f.store.now
	filled := 0
	for candidate := 0; filled < store.policy.SlotsPerShard; candidate++ {
		input := readSubmission(t, f, fmt.Sprintf("occupied-%04x-%d", index, candidate))
		_, shard := store.identity(input.Scope, input.Request.IdempotencyKey)
		if shard != int(index) {
			continue
		}
		if _, duplicate, err := store.Submit(context.Background(), input); err != nil || duplicate {
			t.Fatal("cannot seed full shard", duplicate, err)
		}
		filled++
	}
}

func TestUnkeyedReadAdmissionUsesFreeSuccessorAndRetainsCustody(t *testing.T) {
	f := probeFixture(t, 4, 1)
	b := &probeBackend{memoryBackend: f.backend}
	var deadline time.Time
	b.before = func(ctx context.Context, key string) {
		got, ok := ctx.Deadline()
		if !ok || (!deadline.IsZero() && !got.Equal(deadline)) {
			t.Fatal("probe did not inherit the original storage deadline")
		}
		deadline = got
		if len(b.getKeys) == 1 {
			occupyShard(t, f, key)
		}
	}
	f.store.backend = b
	input := readSubmission(t, f, "")
	snapshot, duplicate, err := f.store.Submit(context.Background(), input)
	if err != nil || duplicate || len(b.getKeys) != 2 || b.getKeys[0] == b.getKeys[1] {
		t.Fatal("read did not use a distinct free successor", duplicate, err, b.getKeys)
	}
	r := snapshot.Record
	digest, _ := api.Digest(input.Request)
	if r.RequestSHA256 != digest || r.RequestRef != input.RequestRef || r.AuthorityToken != input.AuthorityToken || r.AuthoritySHA256 != input.AuthoritySHA256 || r.Scope != input.Scope || r.Kind != api.QueryRead {
		t.Fatal("fallback changed request or authority")
	}
	if f.store.key(mustShard(t, f.store, r.ID)) != b.getKeys[1] {
		t.Fatal("operation ID did not encode its actual shard")
	}
	// Lifecycle calls have independent deadlines; only admission candidates
	// must share the original submission's storage budget.
	b.before = nil
	if _, err := f.store.Get(context.Background(), f.scope, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Claim(context.Background(), f.scope, r.ID, f.binding); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Start(context.Background(), f.scope, r.ID, f.binding); err != nil {
		t.Fatal(err)
	}
	receipt := api.Receipt{Version: api.Version, OperationID: r.ID, RequestSHA256: digest, Outcome: api.Completed, Effect: api.EffectNone}
	if _, err := f.store.Complete(context.Background(), f.scope, r.ID, f.binding, receipt); err != nil {
		t.Fatal(err)
	}
}

func mustShard(t *testing.T, store *Store, id string) int {
	t.Helper()
	shard, err := store.shard(id)
	if err != nil {
		t.Fatal(err)
	}
	return shard
}

func TestUnkeyedReadAdmissionBoundsDistinctCapacityProbes(t *testing.T) {
	for _, shards := range []int{1, 3, 32} {
		t.Run(fmt.Sprint(shards), func(t *testing.T) {
			f := probeFixture(t, shards, 1)
			seen := map[string]bool{}
			b := &probeBackend{memoryBackend: f.backend}
			b.before = func(_ context.Context, key string) {
				if seen[key] {
					t.Fatal("capacity probe revisited a shard")
				}
				seen[key] = true
				occupyShard(t, f, key)
			}
			f.store.backend = b
			if _, _, err := f.store.Submit(context.Background(), readSubmission(t, f, "")); !errors.Is(err, ErrCapacity) {
				t.Fatal("expected bounded capacity rejection", err)
			}
			if len(seen) != min(16, shards) {
				t.Fatal("unexpected broker read bound", len(seen))
			}
		})
	}
}

func TestKeyedReadAndMutationNeverProbeAnotherShard(t *testing.T) {
	for _, mutation := range []bool{false, true} {
		t.Run(fmt.Sprint(mutation), func(t *testing.T) {
			f := probeFixture(t, 4, 1)
			b := &probeBackend{memoryBackend: f.backend}
			b.before = func(_ context.Context, key string) {
				if len(b.getKeys) == 1 {
					occupyShard(t, f, key)
				}
			}
			f.store.backend = b
			input := readSubmission(t, f, "stable-read")
			if mutation {
				input = f.input
			}
			if _, _, err := f.store.Submit(context.Background(), input); !errors.Is(err, ErrCapacity) || len(b.getKeys) != 1 {
				t.Fatal("keyed admission relocated", err, b.getKeys)
			}
		})
	}
}

func TestReadProbeStopsAfterLostAdmissionAcknowledgement(t *testing.T) {
	for _, fullFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(fullFirst), func(t *testing.T) {
			f := probeFixture(t, 4, 1)
			b := &probeBackend{memoryBackend: f.backend}
			b.before = func(_ context.Context, key string) {
				if fullFirst && len(b.getKeys) == 1 {
					occupyShard(t, f, key)
				} else {
					f.backend.loseNextAck = true
				}
			}
			f.store.backend = b
			if _, _, err := f.store.Submit(context.Background(), readSubmission(t, f, "")); !errors.Is(err, ErrUnavailable) {
				t.Fatal("lost acknowledgement did not remain uncertain", err)
			}
			want := 1
			if fullFirst {
				want++
			}
			if len(b.getKeys) != want || len(f.backend.entries) != want {
				t.Fatal("lost acknowledgement triggered another admission", b.getKeys)
			}
		})
	}
}

func TestReadProbeStopsOnCancellation(t *testing.T) {
	f := probeFixture(t, 4, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &probeBackend{memoryBackend: f.backend}
	b.before = func(_ context.Context, key string) {
		occupyShard(t, f, key)
		cancel()
	}
	f.store.backend = b
	if _, _, err := f.store.Submit(ctx, readSubmission(t, f, "")); !errors.Is(err, ErrUnavailable) || len(b.getKeys) != 1 {
		t.Fatal("cancelled request advanced to another shard", err, b.getKeys)
	}
}

func TestReadProbeStopsAfterLostExpiryAcknowledgement(t *testing.T) {
	f := probeFixture(t, 4, 1)
	input := readSubmission(t, f, "")
	input.AuthorityUntil = f.now.Add(4 * time.Minute)
	b := &probeBackend{memoryBackend: f.backend}
	b.before = func(_ context.Context, key string) {
		if len(b.getKeys) != 1 {
			t.Fatal("unknown expiry acknowledgement advanced to another shard")
		}
		occupyShard(t, f, key)
		f.now = f.now.Add(2 * time.Minute)
		f.backend.loseNextAck = true
	}
	f.store.backend = b
	if _, _, err := f.store.Submit(context.Background(), input); !errors.Is(err, ErrUnavailable) {
		t.Fatal("expiry write acknowledgement must remain uncertain", err)
	}
	if len(b.getKeys) != 1 {
		t.Fatal("uncertain expiry was treated as proven capacity")
	}
}

func TestConcurrentUnkeyedReadsUseBoundedSharedCapacity(t *testing.T) {
	f := probeFixture(t, 4, 2)
	const count = 8
	input := readSubmission(t, f, "")
	var wg sync.WaitGroup
	results := make(chan Snapshot, count)
	errors := make(chan error, count)
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, _, err := f.store.Submit(context.Background(), input)
			results <- s
			errors <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	ids := map[string]bool{}
	for snapshot := range results {
		id := snapshot.Record.ID
		if ids[id] || !strings.Contains(id, "-") {
			t.Fatal("concurrent read identities collided")
		}
		ids[id] = true
	}
}
