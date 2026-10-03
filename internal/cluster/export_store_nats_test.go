// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// This gate requires its own freshly provisioned synthetic broker account.
// It must not share a fixture with interactive tests or a running deployment.
func TestNATSExportRetainedLifecycleAndProvisioning(t *testing.T) {
	url := os.Getenv("KELVO_TEST_EXPORT_NATS_URL")
	ca := os.Getenv("KELVO_TEST_EXPORT_NATS_CA_FILE")
	if url == "" || ca == "" {
		t.Skip("set dedicated KELVO_TEST_EXPORT_NATS_URL and CA_FILE for export broker acceptance")
	}
	cfg := NATSConfig{URL: url, CAFile: ca, Username: os.Getenv("KELVO_TEST_EXPORT_NATS_USER"), PasswordEnv: "KELVO_TEST_EXPORT_NATS_PASSWORD"}
	if cfg.Username == "" || len(os.Getenv(cfg.PasswordEnv)) < 32 {
		t.Fatal("missing dedicated export fixture authentication")
	}
	p := runtimeExportConfigFixture(t).Policy
	p.Exports.MaxJobs = 2
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	base, err := OpenStore(ctx, cfg, p, true)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	if _, err = OpenExportStore(ctx, base, false); err == nil {
		t.Fatal("read-only open initialized resources")
	}
	st, err := OpenExportStore(ctx, base, true)
	if err != nil {
		if stream, e := base.js.Stream(ctx, exportQueueStream); e == nil {
			if consumer, e := stream.Consumer(ctx, exportQueueConsumer); e == nil {
				if info, e := consumer.Info(ctx); e == nil {
					t.Logf("consumer observed=%+v expected=%+v", info.Config, exportConsumerConfig(p))
				}
			}
		}
		t.Fatal("provision", err)
	}
	if _, err = OpenExportStore(ctx, base, false); err != nil {
		t.Fatal("reopen", err)
	}
	if err = base.ClaimWorker(ctx, "a1", exportTestOwner); err != nil {
		t.Fatal(err)
	}
	a, _ := authorityForPrincipal(p, "reports")
	ctx = context.WithValue(ctx, jobAuthorityKey{}, a)
	s, err := st.SubmitExport(ctx, ExportSubmission{Request: query.Request{Mode: "federated", SQL: "SELECT 1"}, SupervisorOwner: exportTestOwner, AuthorityUntil: time.Now().UTC().Add(p.LeaseDuration - time.Millisecond)})
	if err != nil {
		t.Fatal("submit", err)
	}
	if err = st.EnqueueExport(ctx, s.Job.ID); err != nil {
		t.Fatal(err)
	}
	d, err := st.NextExport(ctx)
	if err != nil || d.ID() != s.Job.ID {
		t.Fatal("dispatch", err)
	}
	next := s.Job
	next.State = ExportAssigned
	next.WorkerID = "a1"
	next.WorkerOwner = exportTestOwner
	s, err = st.CompareAndSwapExport(ctx, s, next)
	if err != nil {
		t.Fatal("assign", err)
	}
	if err = d.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	if err = st.EnqueueExport(ctx, s.Job.ID); !errors.Is(err, ErrExportConflict) {
		t.Fatal("assigned job reenqueued", err)
	}
	next = s.Job
	next.State = ExportClaimed
	next.Claim = exportTestClaim
	s, err = st.CompareAndSwapExport(ctx, s, next)
	if err != nil {
		t.Fatal("claim", err)
	}
	next = s.Job
	next.State = ExportRunning
	s, err = st.CompareAndSwapExport(ctx, s, next)
	if err != nil {
		t.Fatal("run", err)
	}
	if s.Job.StartedAt.IsZero() || s.Job.ExecutionDeadline.IsZero() {
		t.Fatal("missing durable execution deadline")
	}
	next = s.Job
	next.State = ExportCancelled
	next.Error = query.PublicError(context.Canceled)
	s, err = st.CompareAndSwapExport(context.Background(), s, next)
	if err != nil {
		t.Fatal("cancel", err)
	}
	if _, err = st.SubmitExport(ctx, ExportSubmission{Request: s.Job.Request, SupervisorOwner: exportTestOwner, AuthorityUntil: time.Now().UTC().Add(p.LeaseDuration - time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if _, err = st.SubmitExport(ctx, ExportSubmission{Request: s.Job.Request, SupervisorOwner: exportTestOwner, AuthorityUntil: time.Now().UTC().Add(p.LeaseDuration - time.Millisecond)}); !errors.Is(err, ErrExportCapacity) {
		t.Fatal("cancelled retained slot released", err)
	}
	got, err := st.GetExport(ctx, s.Job.ID)
	if err != nil || got.Job.State != ExportCancelled {
		t.Fatal("retained state", err)
	}
	stream, err := base.js.Stream(ctx, exportQueueStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changed := info.Config
	changed.MaxMsgs++
	if _, err = base.js.UpdateStream(ctx, changed); err != nil {
		t.Fatal(err)
	}
	for _, initialize := range []bool{false, true} {
		if _, err = OpenExportStore(ctx, base, initialize); err == nil {
			t.Fatal("silently accepted mismatched queue configuration")
		}
	}
}
