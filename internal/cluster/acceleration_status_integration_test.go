// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/nats-io/nats.go/jetstream"
)

// This suite is part of the existing env-gated TestNATS release/CI gate. It uses
// the isolated store-test account and does not need source databases.
func TestNATSRefreshStatusDurabilityResetAndBudget(t *testing.T) {
	store := openFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := store.js.Stream(ctx, refreshQueueStream); err == nil {
		t.Skip("refresh integration fixture must not already contain a refresh queue")
	} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatal(err)
	}
	q, err := store.OpenRefreshQueue(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := store.js.DeleteStream(cleanup, refreshQueueStream); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Errorf("cleanup refresh queue: %v", err)
		}
	})
	kv, err := store.js.KeyValue(ctx, refreshStatusBucket)
	if err != nil {
		t.Fatal(err)
	}
	newJob := func(t *testing.T, dataset string) RefreshJob {
		t.Helper()
		var nonce [32]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			t.Fatal(err)
		}
		job := RefreshJob{Dataset: dataset, Fingerprint: hex.EncodeToString(nonce[:])}
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			if err := kv.Purge(cleanup, refreshStatusKey(job)); err != nil {
				t.Errorf("cleanup status: %v", err)
			}
		})
		return job
	}
	next := func(t *testing.T, queue *RefreshQueue) jetstream.Msg {
		t.Helper()
		pull, cancel := context.WithTimeout(ctx, refreshPullWait)
		defer cancel()
		msg, err := queue.consumer.Next(jetstream.FetchContext(pull))
		if err != nil {
			t.Fatalf("consume fixture refresh: %v", err)
		}
		return msg
	}
	sequence := func(t *testing.T) uint64 {
		t.Helper()
		n, err := q.latestSequence(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, code := range []string{"SCHEMA_MISMATCH", "PERMISSION_DENIED"} {
		t.Run(code, func(t *testing.T) {
			job := newJob(t, "permanent_fixture")
			if err := q.Publish(ctx, job, "permanent:"+job.Fingerprint); err != nil {
				t.Fatal(err)
			}
			msg := next(t, q)
			err := q.processRefresh(ctx, msg, job, func(context.Context, RefreshJob) error {
				return query.NewError(code, "SELECT secret_password FROM private_table")
			}, time.Second)
			if err == nil || strings.Contains(err.Error(), "secret_password") {
				t.Fatalf("unsafe failure: %v", err)
			}
			reopened, err := store.OpenRefreshQueue(ctx, false)
			if err != nil {
				t.Fatal(err)
			}
			status, err := reopened.Status(ctx, job)
			if err != nil || status.State != "permanent" || status.Attempts != 1 {
				t.Fatalf("reopened state: %+v %v", status, err)
			}
			entry, err := kv.Get(ctx, refreshStatusKey(job))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(entry.Value()), "secret_password") || strings.Contains(string(entry.Value()), "private_table") {
				t.Fatal("source diagnostics persisted")
			}
			before := sequence(t)
			if err := reopened.Publish(ctx, job, "blocked:"+job.Fingerprint); err != nil {
				t.Fatal(err)
			}
			if after := sequence(t); after != before {
				t.Fatal("stopped dataset was published after queue reopen")
			}
			// Inject a broker message already queued before the operator reset. It
			// must be fenced even though it has never run in this process.
			raw, _ := json.Marshal(job)
			if _, err := store.js.Publish(ctx, refreshQueueSubject, raw); err != nil {
				t.Fatal(err)
			}
			stale := next(t, reopened)
			staleMetadata, err := stale.Metadata()
			if err != nil {
				t.Fatal(err)
			}
			prior, err := kv.Get(ctx, refreshStatusKey(job))
			if err != nil {
				t.Fatal(err)
			}
			if err := reopened.Reset(ctx, job); err != nil {
				t.Fatal(err)
			}
			reset, err := reopened.Status(ctx, job)
			if err != nil || reset.State != "ready" || reset.ResetThroughSequence < staleMetadata.Sequence.Stream {
				t.Fatalf("reset fence: %+v %v", reset, err)
			}
			// The actual broker rejects stale last-revision updates; they cannot
			// overwrite the reset with the preceding permanent state.
			if _, err := kv.Update(ctx, refreshStatusKey(job), prior.Value(), prior.Revision()); err == nil {
				t.Fatal("broker accepted stale status CAS")
			}
			if err := reopened.processRefresh(ctx, stale, job, func(context.Context, RefreshJob) error { t.Error("pre-reset message executed"); return nil }, time.Second); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Publish(ctx, job, "after-reset:"+job.Fingerprint); err != nil {
				t.Fatal(err)
			}
			fresh := next(t, reopened)
			called := false
			if err := reopened.processRefresh(ctx, fresh, job, func(context.Context, RefreshJob) error { called = true; return nil }, time.Second); err != nil {
				t.Fatal(err)
			}
			success, err := reopened.Status(ctx, job)
			if err != nil || !called || success.State != "ready" || success.LastSuccessAt.IsZero() || success.Attempts != 0 || success.ResetThroughSequence != reset.ResetThroughSequence {
				t.Fatalf("successful reset refresh: %+v %v", success, err)
			}
		})
	}
	t.Run("budget_across_scheduled_messages", func(t *testing.T) {
		job := newJob(t, "retry_budget_fixture")
		for attempt := uint64(1); attempt <= refreshMaxAttempts; attempt++ {
			if err := q.Publish(ctx, job, fmt.Sprintf("budget:%s:%d", job.Fingerprint, attempt)); err != nil {
				t.Fatal(err)
			}
			msg := &scheduledRetryFixture{Msg: next(t, q), ctx: ctx}
			if err := q.processRefresh(ctx, msg, job, func(context.Context, RefreshJob) error { return errors.New("private transient error") }, time.Second); err == nil {
				t.Fatal("missing transient outcome")
			}
			reopened, err := store.OpenRefreshQueue(ctx, false)
			if err != nil {
				t.Fatal(err)
			}
			status, err := reopened.Status(ctx, job)
			if err != nil || status.Attempts != attempt {
				t.Fatalf("durable failure budget: %+v %v", status, err)
			}
			if attempt == refreshMaxAttempts {
				if status.State != "exhausted" {
					t.Fatal("retry budget did not stop")
				}
				before := sequence(t)
				if err := reopened.Publish(ctx, job, "exhausted:"+job.Fingerprint); err != nil {
					t.Fatal(err)
				}
				if sequence(t) != before {
					t.Fatal("exhausted dataset dispatched")
				}
				break
			}
			if status.State != "retrying" || msg.delay < 5*time.Second || msg.delay > refreshMaxRetryDelay {
				t.Fatalf("invalid retry state/delay: %+v %s", status, msg.delay)
			}
			// Advance this fixture's stored cooldown with CAS instead of sleeping.
			// Budget logic still runs against real KV and real independent messages.
			entry, err := kv.Get(ctx, refreshStatusKey(job))
			if err != nil {
				t.Fatal(err)
			}
			status.NextRetryAt = time.Unix(1, 0).UTC()
			raw, _ := json.Marshal(status)
			if _, err := kv.Update(ctx, refreshStatusKey(job), raw, entry.Revision()); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// This fixture tests budgets across independent scheduled messages. Settle each
// retry delivery immediately so unrelated delayed broker timers cannot race the
// next fixture job. Real delayed redelivery is covered separately by
// TestNATSRefreshQueueProvisionDedupRetry.
type scheduledRetryFixture struct {
	jetstream.Msg
	ctx   context.Context
	delay time.Duration
}

func (m *scheduledRetryFixture) NakWithDelay(delay time.Duration) error {
	m.delay = delay
	return m.Msg.DoubleAck(m.ctx)
}
