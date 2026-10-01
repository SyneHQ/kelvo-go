// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/nats-io/nats.go"
)

func TestSlot(t *testing.T) {
	valid := "1a-0123456789abcdef0123456789abcdef"
	n, ok := slot(valid)
	if !ok || n != 26 {
		t.Fatalf("slot(%q) = %d, %v", valid, n, ok)
	}
	for _, id := range []string{
		"01-0123456789abcdef0123456789abcdef",
		"1A-0123456789abcdef0123456789abcdef",
		"1a-0123456789ABCDEF0123456789ABCDEF",
		"1a-0123456789abcdef0123456789abcdeg",
		"1a-0123456789abcdef0123456789abcde",
		"-0123456789abcdef0123456789abcdef",
	} {
		if _, ok := slot(id); ok {
			t.Errorf("slot accepted malformed ID %q", id)
		}
	}
}

func TestValidTransition(t *testing.T) {
	allowed := [][2]string{{Queued, Queued}, {Queued, Assigned}, {Queued, Failed}, {Assigned, Claimed}, {Claimed, Running}, {Running, ResultReady}, {ResultReady, ResultReady}, {ResultReady, Succeeded}, {Running, Cancelled}}
	for _, transition := range allowed {
		if !validTransition(transition[0], transition[1]) {
			t.Errorf("rejected transition %s -> %s", transition[0], transition[1])
		}
	}
	for _, transition := range [][2]string{{Queued, Running}, {Assigned, Succeeded}, {Claimed, Queued}, {Running, Succeeded}, {ResultReady, Running}, {Succeeded, Succeeded}, {Failed, Queued}, {Cancelled, Running}} {
		if validTransition(transition[0], transition[1]) {
			t.Errorf("accepted transition %s -> %s", transition[0], transition[1])
		}
	}
}

func TestClaimValidation(t *testing.T) {
	if !validClaim("0123456789abcdef0123456789abcdef") {
		t.Fatal("valid claim rejected")
	}
	for _, claim := range []string{"", "0123456789abcdef0123456789abcde", "0123456789ABCDEF0123456789ABCDEF", "0123456789abcdef0123456789abcdeg"} {
		if validClaim(claim) {
			t.Errorf("invalid claim accepted: %q", claim)
		}
	}
}

func fixtureConfig(t *testing.T) (NATSConfig, Policy) {
	t.Helper()
	url, ca := os.Getenv("KELVO_TEST_NATS_URL"), os.Getenv("KELVO_TEST_NATS_CA_FILE")
	if url == "" || ca == "" {
		t.Skip("set KELVO_TEST_NATS_URL and KELVO_TEST_NATS_CA_FILE to run JetStream integration tests")
	}
	cfg := NATSConfig{URL: url, CAFile: ca, CredentialsFile: os.Getenv("KELVO_TEST_NATS_CREDS_FILE")}
	if cfg.CredentialsFile == "" {
		cfg.Username, cfg.PasswordEnv = os.Getenv("KELVO_TEST_NATS_USERNAME"), os.Getenv("KELVO_TEST_NATS_PASSWORD_ENV")
		if cfg.Username == "" || cfg.PasswordEnv == "" || len(os.Getenv(cfg.PasswordEnv)) < 32 {
			t.Skip("set KELVO_TEST_NATS_CREDS_FILE or KELVO_TEST_NATS_USERNAME and KELVO_TEST_NATS_PASSWORD_ENV")
		}
	}
	replicas := 1
	if raw := os.Getenv("KELVO_TEST_NATS_REPLICAS"); raw != "" {
		var err error
		replicas, err = strconv.Atoi(raw)
		if err != nil || (replicas != 1 && replicas != 3) {
			t.Fatalf("invalid KELVO_TEST_NATS_REPLICAS")
		}
	}
	return cfg, Policy{
		TenantID:      "store-test",
		MaxQueries:    2,
		JobTTL:        45 * time.Second,
		LeaseDuration: 5 * time.Second,
		Replicas:      replicas,
		Limits:        query.DefaultLimits(),
		Workers:       map[string]int{"worker-a": 2},
	}
}

func openFixture(t *testing.T) *NATSStore {
	t.Helper()
	cfg, policy := fixtureConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := OpenStore(ctx, cfg, policy, true)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func failJob(t *testing.T, store *NATSStore, snap Snapshot) Snapshot {
	t.Helper()
	next := snap.Job
	next.State = Failed
	next.Error = query.PublicError(query.NewError("QUERY_FAILED", "test cleanup"))
	updated, err := store.CompareAndSwap(context.Background(), snap, next)
	if err != nil {
		t.Fatalf("fail job: %v", err)
	}
	return updated
}

func TestNATSStoreAdmissionCASAndReuse(t *testing.T) {
	store := openFixture(t)
	ctx := context.Background()
	if _, err := store.Submit(ctx, query.Request{SQL: "select 1"}); err == nil {
		t.Fatal("submit accepted a request that bypassed request validation")
	}
	request := query.Request{Mode: "federated", SQL: "select 1"}
	seed, err := store.Submit(ctx, request)
	if err != nil {
		t.Fatalf("seed admission: %v", err)
	}
	defer failJob(t, store, seed)
	type result struct {
		snapshot Snapshot
		err      error
	}
	var wg sync.WaitGroup
	out := make(chan result, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snapshot, err := store.Submit(ctx, request)
			out <- result{snapshot, err}
		}()
	}
	wg.Wait()
	close(out)
	var admitted Snapshot
	var successes, capacity int
	for result := range out {
		switch {
		case result.err == nil:
			successes++
			admitted = result.snapshot
		case errors.Is(result.err, ErrCapacity):
			capacity++
		default:
			t.Fatalf("concurrent submit: %v", result.err)
		}
	}
	if successes != 1 || capacity != 1 {
		t.Fatalf("last-slot admission: %d successes, %d capacity errors", successes, capacity)
	}

	assignedJob := admitted.Job
	assignedJob.State, assignedJob.WorkerID, assignedJob.Owner = Assigned, "worker-a", "0123456789abcdef0123456789abcdef"
	assigned, err := store.CompareAndSwap(ctx, admitted, assignedJob)
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	stale := assigned.Job
	stale.Request.SQL = "select secret"
	if _, err = store.CompareAndSwap(ctx, assigned, stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("mutable request accepted: %v", err)
	}
	claim := assigned.Job
	claim.State, claim.Claim = Claimed, "0123456789abcdef0123456789abcdef"
	claimed, err := store.CompareAndSwap(ctx, assigned, claim)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	reassigned := claimed.Job
	reassigned.WorkerID = "worker-b"
	if _, err = store.CompareAndSwap(ctx, claimed, reassigned); !errors.Is(err, ErrConflict) {
		t.Fatalf("reassignment accepted: %v", err)
	}
	reowned := claimed.Job
	reowned.Owner = "fedcba9876543210fedcba9876543210"
	if _, err = store.CompareAndSwap(ctx, claimed, reowned); !errors.Is(err, ErrConflict) {
		t.Fatalf("owner reassignment accepted: %v", err)
	}
	if _, err = store.CompareAndSwap(ctx, assigned, claim); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale CAS = %v, want conflict", err)
	}
	runningJob := claimed.Job
	runningJob.State = Running
	running, err := store.CompareAndSwap(ctx, claimed, runningJob)
	if err != nil {
		t.Fatalf("start execution: %v", err)
	}
	readyJob := running.Job
	readyJob.State = ResultReady
	readyJob.Stats.Rows = 1
	ready, err := store.CompareAndSwap(ctx, running, readyJob)
	if err != nil {
		t.Fatalf("persist result: %v", err)
	}
	staleClaim := ready.Job
	staleClaim.Claim = "fedcba9876543210fedcba9876543210"
	if _, err = store.CompareAndSwap(ctx, ready, staleClaim); !errors.Is(err, ErrConflict) {
		t.Fatalf("ready result accepted a changed claim: %v", err)
	}
	if _, err = store.Submit(ctx, request); !errors.Is(err, ErrCapacity) {
		t.Fatalf("ready result slot was reusable: %v", err)
	}
	successJob := ready.Job
	successJob.State = Succeeded
	succeeded, err := store.CompareAndSwap(ctx, ready, successJob)
	if err != nil {
		t.Fatalf("commit successful result: %v", err)
	}
	replacement, err := store.Submit(ctx, request)
	if err != nil {
		t.Fatalf("reuse terminal slot: %v", err)
	}
	if replacement.Job.ID == succeeded.Job.ID {
		t.Fatal("slot reuse retained the old query ID")
	}
	if _, err = store.Get(ctx, succeeded.Job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old ID lookup = %v, want not found", err)
	}
	_ = failJob(t, store, replacement)
}

func TestNATSStoreQueueAndWorkerLease(t *testing.T) {
	store := openFixture(t)
	ctx := context.Background()
	// A shared fixture may retain a previous test process's lease. Wait for it
	// to expire before asserting ownership semantics.
	time.Sleep(store.policy.LeaseDuration + 50*time.Millisecond)
	if err := store.ClaimWorker(ctx, "worker-a", "0123456789abcdef0123456789abcdef"); err != nil {
		t.Fatalf("claim worker: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, "worker-a", "0123456789abcdef0123456789abcdef"); err != nil {
		t.Fatalf("heartbeat worker: %v", err)
	}
	secondOwner := "fedcba9876543210fedcba9876543210"
	if err := store.HeartbeatWorker(ctx, "worker-a", secondOwner); !errors.Is(err, ErrConflict) {
		t.Fatalf("foreign heartbeat = %v, want conflict", err)
	}
	time.Sleep(store.policy.LeaseDuration + 50*time.Millisecond)
	if err := store.ClaimWorker(ctx, "worker-a", secondOwner); err != nil {
		t.Fatalf("take over expired worker lease: %v", err)
	}
	if err := store.HeartbeatWorker(ctx, "worker-a", "0123456789abcdef0123456789abcdef"); !errors.Is(err, ErrConflict) {
		t.Fatalf("previous owner heartbeat = %v, want conflict", err)
	}
	snap, err := store.Submit(ctx, query.Request{Mode: "federated", SQL: "select 2"})
	if err != nil {
		t.Skipf("fixture has no available slot: %v", err)
	}
	if err = store.Enqueue(ctx, snap.Job.ID); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	delivery, err := store.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if delivery.ID() != snap.Job.ID {
		t.Fatalf("delivery ID = %q, want %q", delivery.ID(), snap.Job.ID)
	}
	if err = delivery.Ack(ctx); err != nil {
		t.Fatalf("ack: %v", err)
	}
	_ = failJob(t, store, snap)

	// This job was admitted but never published. Reconciliation must recover it
	// without depending on JetStream's message-ID deduplication window.
	recovery, err := store.Submit(ctx, query.Request{Mode: "federated", SQL: "select 3"})
	if err != nil {
		t.Fatalf("submit unqueued job: %v", err)
	}
	if err = store.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	delivery, err = store.Next(ctx)
	if err != nil {
		t.Fatalf("next after reconcile: %v", err)
	}
	if delivery.ID() != recovery.Job.ID {
		t.Fatalf("reconciled delivery ID = %q, want %q", delivery.ID(), recovery.Job.ID)
	}
	if err = delivery.Ack(ctx); err != nil {
		t.Fatalf("reconciled ack: %v", err)
	}
	_ = failJob(t, store, recovery)
}

func TestNATSStoreRejectsMetadataMismatch(t *testing.T) {
	cfg, policy := fixtureConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := OpenStore(ctx, cfg, policy, true)
	if err != nil {
		t.Fatalf("bootstrap fixture: %v", err)
	}
	_ = store.Close()
	policy.TenantID = "other-tenant"
	if _, err = OpenStore(ctx, cfg, policy, false); err == nil {
		t.Fatal("metadata mismatch was accepted")
	}
}

func TestNATSStoreReconcileFailsStaleAssigned(t *testing.T) {
	store := openFixture(t)
	ctx := context.Background()
	snap, err := store.Submit(ctx, query.Request{Mode: "federated", SQL: "select 4"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	assigned := snap.Job
	assigned.State, assigned.WorkerID, assigned.Owner = Assigned, "worker-a", "0123456789abcdef0123456789abcdef"
	assignedSnap, err := store.CompareAndSwap(ctx, snap, assigned)
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	stale := assignedSnap.Job
	stale.HeartbeatAt = time.Now().UTC().Add(-store.policy.LeaseDuration - time.Second)
	value, err := encodeJob(stale)
	if err != nil {
		t.Fatalf("encode stale job: %v", err)
	}
	slot, ok := slot(stale.ID)
	if !ok {
		t.Fatal("assigned job had an invalid ID")
	}
	if _, err = store.kv.Update(ctx, "slot."+strconv.FormatInt(int64(slot), 16), value, assignedSnap.Revision); err != nil {
		t.Fatalf("make heartbeat stale: %v", err)
	}
	if err = store.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile stale assigned job: %v", err)
	}
	current, err := store.Get(ctx, stale.ID)
	if err != nil {
		t.Fatalf("get reconciled job: %v", err)
	}
	if current.Job.State != Failed || current.Job.Error == nil || current.Job.Error.Code != "UNAVAILABLE" {
		t.Fatalf("stale assigned job = %#v, want terminal worker-loss failure", current.Job)
	}
}

func TestNATSStoreCorruptJobFailsClosed(t *testing.T) {
	store := openFixture(t)
	ctx := context.Background()
	snap, err := store.Submit(ctx, query.Request{Mode: "federated", SQL: "select 5"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	n, ok := slot(snap.Job.ID)
	if !ok {
		t.Fatal("submitted job had an invalid ID")
	}
	key := "slot." + strconv.FormatInt(int64(n), 16)
	entry, err := store.kv.Get(ctx, key)
	if err != nil {
		t.Fatalf("read job entry: %v", err)
	}
	corruptRevision, err := store.kv.Update(ctx, key, []byte("{"), entry.Revision())
	if err != nil {
		t.Fatalf("corrupt job entry: %v", err)
	}
	if _, err = store.Get(ctx, snap.Job.ID); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("corrupt job lookup = %v, want store error", err)
	}
	if _, err = store.kv.Update(ctx, key, entry.Value(), corruptRevision); err != nil {
		t.Fatalf("restore job entry: %v", err)
	}
	current, err := store.Get(ctx, snap.Job.ID)
	if err != nil {
		t.Fatalf("get restored job: %v", err)
	}
	_ = failJob(t, store, current)
}

func fixtureAccountConn(t *testing.T, userEnv, passwordEnv string) *nats.Conn {
	t.Helper()
	rawURL, caFile := os.Getenv("KELVO_TEST_NATS_URL"), os.Getenv("KELVO_TEST_NATS_CA_FILE")
	user, passwordKey := os.Getenv(userEnv), os.Getenv(passwordEnv)
	if rawURL == "" || caFile == "" || user == "" || passwordKey == "" || os.Getenv(passwordKey) == "" {
		t.Skip("account-isolation fixture credentials are not configured")
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		t.Fatal("invalid fixture NATS URL")
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("read fixture CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("fixture CA has no certificates")
	}
	nc, err := nats.Connect(rawURL,
		nats.Secure(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: u.Hostname()}),
		nats.UserInfo(user, os.Getenv(passwordKey)),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("connect fixture account %s: %v", user, err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestNATSAccountIsolation(t *testing.T) {
	a := fixtureAccountConn(t, "KELVO_TEST_NATS_A_USERNAME", "KELVO_TEST_NATS_A_PASSWORD_ENV")
	b := fixtureAccountConn(t, "KELVO_TEST_NATS_B_USERNAME", "KELVO_TEST_NATS_B_PASSWORD_ENV")
	const subject = "kelvo.isolation.store-test"
	aSub, err := a.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("subscribe account A: %v", err)
	}
	bSub, err := b.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("subscribe account B: %v", err)
	}
	if err = a.Flush(); err != nil {
		t.Fatalf("flush account A subscription: %v", err)
	}
	if err = b.Flush(); err != nil {
		t.Fatalf("flush account B subscription: %v", err)
	}
	if err = a.Publish(subject, []byte("account-a-only")); err != nil {
		t.Fatalf("publish account A: %v", err)
	}
	if err = a.Flush(); err != nil {
		t.Fatalf("flush account A publication: %v", err)
	}
	message, err := aSub.NextMsg(time.Second)
	if err != nil || string(message.Data) != "account-a-only" {
		t.Fatalf("account A delivery = %v, %v", message, err)
	}
	if _, err = bSub.NextMsg(250 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("account B received account A subject: %v", err)
	}
}
