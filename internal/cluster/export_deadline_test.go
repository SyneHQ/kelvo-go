// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type exportLeaseReadHook struct {
	jetstream.KeyValue
	before func()
}

func (k exportLeaseReadHook) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	k.before()
	return k.KeyValue.Get(ctx, key)
}

func TestExportStoreQueueDeadlineCoversAssignmentAndClaim(t *testing.T) {
	for _, state := range []string{ExportAssigned, ExportClaimed} {
		for _, operation := range []string{"advance", "renew", "reconcile"} {
			t.Run(state+"/"+operation, func(t *testing.T) {
				f := newExportStoreFixture(t)
				f.store.policy.Exports.QueueTimeout = time.Second
				s := f.step(t, f.submit(t), ExportAssigned)
				if state == ExportClaimed {
					s = f.step(t, s, ExportClaimed)
				}
				f.now = s.Job.QueueDeadline
				f.lease(t, exportTestOwner)
				if operation == "reconcile" {
					if err := f.store.ReconcileExports(f.ctx); err != nil {
						t.Fatal(err)
					}
					got, err := f.store.GetExport(f.ctx, s.Job.ID)
					if err != nil || got.Job.State != ExportFailed {
						t.Fatal("overdue queued execution was not fenced", err)
					}
					return
				}
				next := s.Job
				if operation == "renew" {
					next.AuthorityUntil = f.now.Add(f.store.policy.LeaseDuration)
				} else if state == ExportAssigned {
					next.State, next.Claim = ExportClaimed, exportTestClaim
				} else {
					next.State = ExportRunning
				}
				if _, err := f.store.CompareAndSwapExport(f.ctx, s, next); !errors.Is(err, ErrExportConflict) {
					t.Fatal("overdue execution advanced", err)
				}
			})
		}
	}
}

func TestExportStoreRechecksDeadlinesAfterWorkerLeaseRead(t *testing.T) {
	for _, boundary := range []string{"authority", "expiry", "queue", "execution", "valid-progress"} {
		t.Run(boundary, func(t *testing.T) {
			f := newExportStoreFixture(t)
			f.store.policy.Exports.QueueTimeout = time.Second
			if boundary != "authority" {
				f.store.policy.Limits.Timeout = time.Second
			}
			s := f.step(t, f.step(t, f.submit(t), ExportAssigned), ExportClaimed)
			if boundary != "queue" && boundary != "valid-progress" {
				s = f.step(t, s, ExportRunning)
			}
			next := s.Job
			if s.Job.State == ExportClaimed {
				next.State = ExportRunning
			}
			f.store.base.kv = exportLeaseReadHook{KeyValue: f.workers, before: func() {
				switch boundary {
				case "authority":
					f.now = s.Job.AuthorityUntil
				case "expiry":
					// Retention expiry also ends authority by contract:
					// AuthorityUntil can never exceed ExpiresAt.
					f.now = s.Job.ExpiresAt
				case "queue":
					f.now = s.Job.QueueDeadline
				case "execution":
					f.now = s.Job.ExecutionDeadline
				default:
					f.now = f.now.Add(10 * time.Millisecond)
				}
				// Keep the worker live, so its own lease cannot mask an
				// expired export authority or execution budget.
				f.lease(t, exportTestOwner)
			}}
			got, err := f.store.CompareAndSwapExport(f.ctx, s, next)
			if boundary == "valid-progress" {
				if err != nil || !got.Job.HeartbeatAt.Equal(f.now) || !got.Job.StartedAt.Equal(f.now) || !got.Job.ExecutionDeadline.Equal(f.now.Add(time.Second)) {
					t.Fatal("worker progress did not use the post-I/O clock", err)
				}
				return
			}
			if !errors.Is(err, ErrExportConflict) {
				t.Fatal("expired export advanced after lease I/O", err)
			}
			current, err := f.store.GetExport(f.ctx, s.Job.ID)
			if err != nil || current.Revision != s.Revision || current.Job.State != s.Job.State {
				t.Fatal("rejected transition modified durable state", err)
			}
		})
	}
}

func TestExportStoreChecksDeadlineImmediatelyBeforeCAS(t *testing.T) {
	f := newExportStoreFixture(t)
	s := f.running(t)
	instant, calls := f.now, 0
	f.store.now = func() time.Time {
		calls++
		if calls >= 4 {
			return s.Job.AuthorityUntil
		}
		return instant
	}
	if _, err := f.store.CompareAndSwapExport(f.ctx, s, s.Job); !errors.Is(err, ErrExportConflict) || calls != 4 {
		t.Fatal("last deadline check did not fence publication", err, calls)
	}
}

func TestExportStoreDeadlineExceptionsPreserveStoredPublicationAndStop(t *testing.T) {
	f := newExportStoreFixture(t)
	f.store.policy.Limits.Timeout = time.Second
	stored := f.stored(t)
	f.now = stored.Job.ExecutionDeadline
	f.lease(t, exportTestOwner)
	ready := f.step(t, stored, ExportReady)
	f.now = ready.Job.ExpiresAt
	f.store.base.kv = exportLeaseReadHook{KeyValue: f.workers, before: func() {
		t.Fatal("withdrawal required a live worker")
	}}
	f.step(t, ready, ExportCancelled)
}
