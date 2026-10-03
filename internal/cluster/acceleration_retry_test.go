// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/nats-io/nats.go/jetstream"
)

type statusEntry struct {
	jetstream.KeyValueEntry
	raw      []byte
	revision uint64
}

func (e statusEntry) Value() []byte    { return append([]byte(nil), e.raw...) }
func (e statusEntry) Revision() uint64 { return e.revision }

type statusFixture struct {
	mu       sync.Mutex
	entries  map[string]statusEntry
	revision uint64
	fail     bool
}

func newStatusFixture() *statusFixture { return &statusFixture{entries: map[string]statusEntry{}} }
func (s *statusFixture) Get(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return nil, errors.New("credential secret backend failure")
	}
	e, ok := s.entries[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return e, nil
}
func (s *statusFixture) Create(_ context.Context, key string, raw []byte, _ ...jetstream.KVCreateOpt) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return 0, errors.New("credential secret backend failure")
	}
	if _, ok := s.entries[key]; ok {
		return 0, jetstream.ErrKeyExists
	}
	s.revision++
	s.entries[key] = statusEntry{raw: append([]byte(nil), raw...), revision: s.revision}
	return s.revision, nil
}
func (s *statusFixture) Update(_ context.Context, key string, raw []byte, revision uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return 0, errors.New("credential secret backend failure")
	}
	if e, ok := s.entries[key]; !ok || e.revision != revision {
		return 0, jetstream.ErrKeyExists
	}
	s.revision++
	s.entries[key] = statusEntry{raw: append([]byte(nil), raw...), revision: s.revision}
	return s.revision, nil
}
func (s *statusFixture) Delete(_ context.Context, key string, _ ...jetstream.KVDeleteOpt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	return nil
}
func (s *statusFixture) expireCooldown(t *testing.T, job RefreshJob) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	key := refreshStatusKey(job)
	entry := s.entries[key]
	var status RefreshStatus
	if err := json.Unmarshal(entry.raw, &status); err != nil {
		t.Fatal(err)
	}
	status.NextRetryAt = time.Now().Add(-time.Second)
	entry.raw, _ = json.Marshal(status)
	s.entries[key] = entry
}

type retryAfterFailure time.Duration

func (e retryAfterFailure) Error() string             { return "sensitive provider payload" }
func (e retryAfterFailure) RetryAfter() time.Duration { return time.Duration(e) }

func TestRefreshClassificationAndBoundedJitter(t *testing.T) {
	for _, tc := range []struct {
		code, category string
		permanent      bool
	}{
		{"SCHEMA_MISMATCH", "schema", true}, {"CONFIGURATION_ERROR", "configuration", true},
		{"UNSUPPORTED", "configuration", true}, {"NOT_SUPPORTED", "configuration", true},
		{"PERMISSION_DENIED", "access", true}, {"RESOURCE_EXHAUSTED", "resource", true},
		{"UNAVAILABLE", "unavailable", false}, {"QUERY_FAILED", "unknown", false},
	} {
		failure := classifyRefreshFailure(fmt.Errorf("wrapped: %w", query.NewError(tc.code, "SELECT secret_password")))
		if failure.Category != tc.category || failure.Permanent != tc.permanent || strings.Contains(failure.Error(), "secret") {
			t.Fatalf("classification: %+v", failure)
		}
	}
	for attempt := uint64(1); attempt < 100; attempt++ {
		delay, ok := refreshBackoff(attempt, errors.New("transient"))
		if !ok || delay < 5*time.Second || delay > refreshMaxRetryDelay {
			t.Fatalf("delay %d=%s", attempt, delay)
		}
	}
	delay, ok := refreshBackoff(1, retryAfterFailure(2*time.Minute))
	if !ok || delay < 2*time.Minute {
		t.Fatal("Retry-After ignored")
	}
	if _, ok := refreshBackoff(1, retryAfterFailure(time.Hour)); ok {
		t.Fatal("retry scheduled before provider allowance")
	}
}

func TestRefreshPermanentStatusSuppressesAndResetFencesOldJobs(t *testing.T) {
	ctx := context.Background()
	store := newStatusFixture()
	publisher := &refreshTestPublisher{}
	q := &RefreshQueue{status: store, js: publisher}
	job := refreshTestJob()
	msg := &refreshTestMessage{}
	// Use the actual sentinel emitted by the snapshot encoder, not a synthetic
	// query error: a plain errors.New sentinel previously retried real drift.
	drift := fmt.Errorf("secret source SQL: %w", acceleration.ErrSchemaMismatch)
	if !errors.Is(drift, acceleration.ErrSchemaMismatch) {
		t.Fatal("wrapped drift lost its sentinel")
	}
	failure := classifyRefreshFailure(drift)
	if failure.Category != "schema" || !failure.Permanent || strings.Contains(failure.Error(), "secret") {
		t.Fatalf("actual schema drift misclassified: %+v", failure)
	}
	err := q.processRefresh(ctx, msg, job, func(context.Context, RefreshJob) error { return drift }, time.Second)
	if err == nil || strings.Contains(err.Error(), "secret") || msg.terms.Load() != 1 || msg.naks.Load() != 0 {
		t.Fatalf("permanent outcome: %v", err)
	}
	status, err := q.Status(ctx, job)
	if err != nil || status.State != "permanent" || status.Category != "schema" || status.Attempts != 1 || !status.NextRetryAt.IsZero() {
		t.Fatalf("durable status: %+v %v", status, err)
	}
	if err := q.Publish(ctx, job, "next-schedule"); err != nil || publisher.calls != 0 {
		t.Fatal("permanent state republished")
	}
	// A restarted queue reads the same durable stop.
	q = &RefreshQueue{status: store, js: publisher, latestSequence: func(context.Context) (uint64, error) { return 50, nil }}
	msg = &refreshTestMessage{}
	_ = q.processRefresh(ctx, msg, job, func(context.Context, RefreshJob) error { t.Fatal("stopped dataset ran"); return nil }, time.Second)
	if msg.terms.Load() != 1 || msg.naks.Load() != 0 || msg.acks.Load() != 0 {
		t.Fatal("stopped delivery was not terminated without retry")
	}
	oldTime := time.Now().Add(-time.Second)
	if err := q.Reset(ctx, job); err != nil {
		t.Fatal(err)
	}
	status, err = q.Status(ctx, job)
	if err != nil || status.State != "ready" || status.Attempts != 0 || status.ResetAt.IsZero() {
		t.Fatalf("reset: %+v %v", status, err)
	}
	msg = &refreshTestMessage{sentAt: oldTime, delivered: 6, streamSequence: 40}
	if err := q.processRefresh(ctx, msg, job, func(context.Context, RefreshJob) error { t.Fatal("pre-reset delivery replayed"); return nil }, time.Second); err != nil {
		t.Fatal(err)
	}
	if msg.terms.Load() != 1 {
		t.Fatal("old delivery not terminated")
	}
	if err := q.Publish(ctx, job, "after-reset"); err != nil || publisher.calls != 1 {
		t.Fatal("reset did not permit future dispatch")
	}
	msg = &refreshTestMessage{}
	if err := q.processRefresh(ctx, msg, job, func(context.Context, RefreshJob) error { return nil }, time.Second); err != nil {
		t.Fatal(err)
	}
	status, err = q.Status(ctx, job)
	if err != nil || status.LastSuccessAt.IsZero() || status.ResetAt.IsZero() || status.State != "ready" {
		t.Fatalf("success status: %+v %v", status, err)
	}
}

func TestRefreshFailureBudgetSpansScheduledMessages(t *testing.T) {
	ctx := context.Background()
	store := newStatusFixture()
	q := &RefreshQueue{status: store}
	job := refreshTestJob()
	for attempt := uint64(1); attempt <= refreshMaxAttempts; attempt++ {
		msg := &refreshTestMessage{} // each independent scheduled job is delivery 1
		err := q.processRefresh(ctx, msg, job, func(context.Context, RefreshJob) error { return errors.New("source unavailable") }, time.Second)
		if err == nil {
			t.Fatal("missing failure")
		}
		status, e := q.Status(ctx, job)
		if e != nil || status.Attempts != attempt {
			t.Fatalf("attempt budget: %+v %v", status, e)
		}
		if attempt < refreshMaxAttempts {
			if msg.naks.Load() != 1 || status.State != "retrying" {
				t.Fatal("transient not retried")
			}
			store.expireCooldown(t, job)
		} else if msg.terms.Load() != 1 || status.State != "exhausted" {
			t.Fatal("budget did not stop")
		}
	}
}

func TestRefreshStatusFailureDoesNotAckOrExecute(t *testing.T) {
	store := newStatusFixture()
	q := &RefreshQueue{status: store}
	job := refreshTestJob()
	msg := &refreshTestMessage{}
	err := q.processRefresh(context.Background(), msg, job, func(context.Context, RefreshJob) error { store.fail = true; return errors.New("source secret") }, time.Second)
	if err == nil || strings.Contains(err.Error(), "secret") || msg.terms.Load() != 0 || msg.naks.Load() != 0 || msg.acks.Load() != 0 {
		t.Fatalf("failed persistence acknowledged: %v", err)
	}
	msg = &refreshTestMessage{}
	err = q.processRefresh(context.Background(), msg, job, func(context.Context, RefreshJob) error { t.Fatal("unreadable status executed source"); return nil }, time.Second)
	if err == nil {
		t.Fatal("missing status error")
	}
	store.fail = false
	msg = &refreshTestMessage{delivered: refreshMaxAttempts + 1}
	err = q.processRefresh(context.Background(), msg, job, func(context.Context, RefreshJob) error { t.Fatal("delivery budget executed source"); return nil }, time.Second)
	if err == nil || msg.terms.Load() != 1 {
		t.Fatal("delivery budget not terminal")
	}
}
