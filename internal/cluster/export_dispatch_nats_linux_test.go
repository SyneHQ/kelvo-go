//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/nats-io/nats.go/jetstream"
)

// This discriminator uses the actual dispatcher, JetStream CAS/deduplication,
// restricted initializer/gateway/worker identities, and a synthetic Arrow
// executor. It does not replace the separate sandboxed-worker lifecycle gate.
func TestNATSExportDispatchRenewalConflictAndDelayedRedelivery(t *testing.T) {
	const prefix = "KELVO_TEST_EXPORT_DISPATCH_NATS"
	url, ca := os.Getenv(prefix+"_URL"), os.Getenv(prefix+"_CA_FILE")
	if url == "" || ca == "" {
		t.Skip("set the dedicated dispatch TLS broker fixture with separate restricted roles")
	}
	cfg := runtimeExportConfigFixture(t)
	cfg.WorkerID = "a1"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	open := func(role string, initialize bool) *NATSStore {
		name := prefix
		if role != "initializer" {
			name += "_" + role
		}
		config := NATSConfig{URL: url, CAFile: ca, Username: os.Getenv(name + "_USER"), PasswordEnv: name + "_PASSWORD"}
		if config.Username == "" || len(os.Getenv(config.PasswordEnv)) < 32 {
			t.Fatal("missing distinct restricted broker identity", role)
		}
		store, err := OpenStore(ctx, config, cfg.Policy, initialize)
		if err != nil {
			t.Fatal("open restricted broker role", role, err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	}
	initializer := open("initializer", true)
	if _, err := OpenExportStore(ctx, initializer, true); err != nil {
		t.Fatal("initialize export namespace", err)
	}
	if err := initializer.Close(); err != nil {
		t.Fatal(err)
	}
	worker, gateway := open("WORKER", false), open("GATEWAY", false)
	workerExports, err := OpenExportStore(ctx, worker, false)
	if err != nil {
		t.Fatal(err)
	}
	gatewayExports, err := OpenExportStore(ctx, gateway, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.ClaimWorker(ctx, cfg.WorkerID, exportTestOwner); err != nil {
		t.Fatal(err)
	}
	probe := newExportDispatchProbe(workerExports, ExportAssigned)
	var calls atomic.Int32
	runtime := openDispatchRuntime(t, cfg, probe, &calls)
	var resume sync.Once
	defer resume.Do(func() { close(probe.resume) })
	authorityCtx := dispatchAuthority(t, ctx, cfg.Policy)
	job, err := gatewayExports.SubmitExport(authorityCtx, ExportSubmission{
		Request: query.Request{Mode: "federated", SQL: "SELECT 7"}, SupervisorOwner: exportTestOwner,
		AuthorityUntil: time.Now().UTC().Add(cfg.Policy.LeaseDuration / 3)})
	if err != nil {
		t.Fatal(err)
	}
	if err := gatewayExports.EnqueueExport(ctx, job.Job.ID); err != nil {
		t.Fatal(err)
	}
	stale := dispatchSnapshot(t, probe)
	dispatchDeliveryTime(t, ctx, probe)
	if stale.Job.ID != job.Job.ID || stale.Revision != job.Revision {
		t.Fatal("dispatcher did not hold the original queued revision")
	}
	g := &Gateway{exportOwner: exportTestOwner}
	if err := g.renewExportAuthority(authorityCtx, gatewayExports, stale); err != nil {
		t.Fatal("gateway could not legally renew Queued authority", err)
	}
	renewed, err := gatewayExports.GetExport(ctx, job.Job.ID)
	if err != nil || renewed.Revision == stale.Revision || renewed.Job.State != ExportQueued || !renewed.Job.AuthorityUntil.After(stale.Job.AuthorityUntil) {
		t.Fatal("queued authority did not advance", err)
	}
	// A reconciler republishes this same ID. Prove the broker's existing
	// deduplication window suppresses it: recovery must use this delivery.
	duplicate, err := gateway.js.Publish(ctx, exportQueueSubject, []byte(job.Job.ID), jetstream.WithMsgID(job.Job.ID), jetstream.WithExpectStream(exportQueueStream))
	if err != nil || !duplicate.Duplicate {
		t.Fatal("republication was not deduplicated", err)
	}
	resume.Do(func() { close(probe.resume) })
	retriedAt := dispatchDecision(t, probe, "retry")
	dispatchDecision(t, probe, "ack")
	redeliveredAt := dispatchDeliveryTime(t, ctx, probe)
	delay := min(cfg.Policy.LeaseDuration/3, 250*time.Millisecond)
	if elapsed := redeliveredAt.Sub(retriedAt); elapsed < delay/2 || elapsed > 3*time.Second {
		t.Fatal("broker redelivery was immediate or exceeded the bounded test wait", elapsed)
	}
	assigned, err := gatewayExports.GetExport(ctx, job.Job.ID)
	if err != nil || assigned.Job.State != ExportAssigned || probe.conflicts.Load() != 1 || probe.assignments.Load() != 1 || calls.Load() != 0 {
		t.Fatal("renewed queued delivery was lost, reassigned, or executed before claim", err)
	}
	// Queue a genuine duplicate before cancelling the assignment. The
	// dispatcher must acknowledge the terminal state when its slot is free.
	if _, err := gateway.js.Publish(ctx, exportQueueSubject, []byte(job.Job.ID), jetstream.WithMsgID(job.Job.ID+"-duplicate"), jetstream.WithExpectStream(exportQueueStream)); err != nil {
		t.Fatal(err)
	}
	// Release the first assignment reservation by cancelling it through the
	// gateway, allowing the single-capacity dispatcher to read the duplicate.
	next := assigned.Job
	next.State, next.Error = ExportCancelled, query.PublicError(context.Canceled)
	if _, err := gatewayExports.CompareAndSwapExport(ctx, assigned, next); err != nil {
		t.Fatal(err)
	}
	dispatchDecision(t, probe, "ack")
	if calls.Load() != 0 || probe.assignments.Load() != 1 {
		t.Fatal("terminal duplicate replayed assignment or SQL")
	}
	assertDispatchDrained(t, runtime)
}
