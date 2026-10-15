// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapter/frame"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

// switchableSecrets is a source secret store that can rotate a credential,
// lose it (not found), or fail (an outage).
type switchableSecrets struct {
	mu      sync.Mutex
	value   string
	missing bool
	failing bool
	calls   int
}

func (s *switchableSecrets) Resolve(context.Context, string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	switch {
	case s.failing:
		return "", true, errors.New("secret store unavailable")
	case s.missing:
		return "", false, nil
	}
	return s.value, true, nil
}

func (s *switchableSecrets) set(update func(*switchableSecrets)) {
	s.mu.Lock()
	update(s)
	s.mu.Unlock()
}

// recordingServe answers every request and records what the worker received.
func recordingServe(fs *fakeSet, seen *[]adapter.ProcessRequest) func(p *fakeWorkerProcess) {
	return func(p *fakeWorkerProcess) {
		var mu sync.Mutex
		serveFake(p, func(f frame.Frame) {
			if f.Type == frame.Request {
				var r adapter.ProcessRequest
				_ = json.Unmarshal(f.Payload, &r)
				fs.mu.Lock()
				*seen = append(*seen, r)
				fs.mu.Unlock()
				mu.Lock()
				replyOK(p.stdoutW, f.Stream, f.Payload)
				mu.Unlock()
			}
		})
	}
}

// staticAuthorizationExecutor serves a static source whose credential comes
// only from secrets: the process environment holds none.
func staticAuthorizationExecutor(t *testing.T, fs *fakeSet, secrets *switchableSecrets) (*Executor, *WarmPoolManager) {
	t.Helper()
	e, mgr := readWorkerExecutor(t, fs)
	_ = os.Unsetenv("KELVO_TEST_READ_DSN")
	mgr.cfg.CacheDisabled, mgr.cache = true, nil
	e.Secrets = secrets
	return e, mgr
}

func TestReadAuthorizationOncePerWindow(t *testing.T) {
	fs := &fakeSet{}
	var seen []adapter.ProcessRequest
	fs.serve = recordingServe(fs, &seen)
	secrets := &switchableSecrets{value: "postgres://reader:secret@db.internal:5432/appdb?sslmode=verify-full"}
	e, mgr := staticAuthorizationExecutor(t, fs, secrets)
	mgr.cfg.ReauthorizeEvery = 300 * time.Millisecond
	read := func() error {
		_, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}, &workerTestSink{})
		return err
	}
	for i := 0; i < 5; i++ {
		if err := read(); err != nil {
			t.Fatal(err)
		}
	}
	fs.mu.Lock()
	if mgr.metrics.Authorizations.Load() != 1 || len(seen) != 5 || seen[0].Source.DSN == "" {
		t.Fatalf("authorizations=%d requests=%d", mgr.metrics.Authorizations.Load(), len(seen))
	}
	for _, r := range seen[1:] {
		if r.Source.DSN != "" || len(r.Source.Options) != 0 {
			t.Fatal("a later read carried credentials")
		}
	}
	fs.mu.Unlock()
	time.Sleep(350 * time.Millisecond)
	if err := read(); err != nil {
		t.Fatal(err)
	}
	fs.mu.Lock()
	last := seen[len(seen)-1]
	fs.mu.Unlock()
	if mgr.metrics.Authorizations.Load() != 2 || last.Source.DSN == "" {
		t.Fatal("stale authorization reused", mgr.metrics.Authorizations.Load())
	}
	// An expired authorization is kept while its worker lives, so a later
	// denial can still revoke it (TestReadAuthorizationExpiredStillRevokes).
	time.Sleep(350 * time.Millisecond)
	mgr.reapExpired()
	if mgr.currentAuthorization(readAuthKeyFor(readIdentity{tenant: "static"}, "app", "app", "static")) == nil {
		t.Fatal("expired authorization of a live worker was pruned")
	}
}

// An outage of the secret store refuses the read but revokes nothing; a
// credential that is gone (not found) revokes and kills the worker.
func TestReadAuthorizationRevokesOnlyOnDefinitiveDenial(t *testing.T) {
	fs := &fakeSet{}
	var seen []adapter.ProcessRequest
	fs.serve = recordingServe(fs, &seen)
	secrets := &switchableSecrets{value: "postgres://reader:secret@db.internal:5432/appdb?sslmode=verify-full"}
	e, mgr := staticAuthorizationExecutor(t, fs, secrets)
	mgr.cfg.ReauthorizeEvery = 100 * time.Millisecond
	read := func() error {
		_, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}, &workerTestSink{})
		return err
	}
	if err := read(); err != nil {
		t.Fatal(err)
	}
	secrets.set(func(s *switchableSecrets) { s.failing = true })
	time.Sleep(150 * time.Millisecond)
	if err := read(); err == nil {
		t.Fatal("read served without a checkable authorization")
	}
	if fs.procs[0].killed.Load() || mgr.metrics.EvictionsAuth.Load() != 0 {
		t.Fatal("an outage revoked the authorization")
	}
	secrets.set(func(s *switchableSecrets) { s.failing, s.missing = false, true })
	if err := read(); err == nil {
		t.Fatal("read served for a credential that is gone")
	}
	waitFor(t, func() bool { return fs.procs[0].killed.Load() })
	if mgr.metrics.EvictionsAuth.Load() != 1 {
		t.Fatal("definitive denial did not kill the worker", mgr.metrics.EvictionsAuth.Load())
	}
	// Compare-and-swap: revoking an authorization another caller already
	// replaced does nothing.
	stale := &readAuthorization{workerKey: "w1"}
	current := &readAuthorization{workerKey: "w2"}
	mgr.storeAuthorization("cas", current)
	_ = mgr.revokeAuthorization(context.Background(), "cas", stale)
	if mgr.currentAuthorization("cas") != current {
		t.Fatal("revocation removed an authorization it did not observe")
	}
}

// A changed revision (credential rotation) drains the old worker: its read in
// flight finishes, new reads go to a fresh worker, nothing is killed mid-read.
func TestReadAuthorizationRotationDrainsBusyWorker(t *testing.T) {
	release := make(chan struct{})
	fs := &fakeSet{serve: holdingServe(release)}
	secrets := &switchableSecrets{value: "postgres://reader:secret@db.internal:5432/appdb?sslmode=verify-full"}
	e, mgr := staticAuthorizationExecutor(t, fs, secrets)
	mgr.cfg.ReauthorizeEvery = 150 * time.Millisecond
	slow := make(chan error, 1)
	go func() {
		_, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 'hold'"}, &workerTestSink{})
		slow <- err
	}()
	waitFor(t, func() bool { return mgr.metrics.Spawns.Load() == 1 })
	secrets.set(func(s *switchableSecrets) {
		s.value = "postgres://reader:rotated@db.internal:5432/appdb?sslmode=verify-full"
	})
	time.Sleep(200 * time.Millisecond)
	if _, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}, &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	if len(fs.procs) != 2 || fs.procs[0].killed.Load() {
		t.Fatal("rotation did not move new reads to a fresh worker, or killed the busy one")
	}
	close(release)
	if err := <-slow; err != nil {
		t.Fatal("rotation broke the read in flight:", err)
	}
	waitFor(t, func() bool { return fs.procs[0].killed.Load() })
	if mgr.metrics.Rotations.Load() != 1 || mgr.metrics.EvictionsAuth.Load() != 0 {
		t.Fatal("rotation counted as a revocation", mgr.metrics.Rotations.Load(), mgr.metrics.EvictionsAuth.Load())
	}
}

// A worker evicted while it launches never serves: its launch kills it.
func TestReadAuthorizationEvictionDuringLaunch(t *testing.T) {
	unblock := make(chan struct{})
	launcher := &mockLauncher{onLaunch: func(context.Context) (WorkerProcess, func() error, error) {
		<-unblock
		proc := newFakeWorkerProcess()
		go serveFake(proc, nil)
		return proc, proc.Kill, nil
	}}
	mgr, exec := newTestPool(t, launcher)
	req := opReq("op-launching")
	key := warmKey(req.Source.TenantID, req.AppTeam, req.Source.ConnectionID, req.Source.Database, req.Source.Schema, req.Source.Revision)
	result := make(chan error, 1)
	go func() {
		_, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, req, &workerTestSink{})
		result <- err
	}()
	waitFor(t, func() bool { mgr.mu.Lock(); defer mgr.mu.Unlock(); return len(mgr.workers) == 1 })
	killed := make(chan error, 1)
	go func() { killed <- mgr.killKey(context.Background(), key) }()
	waitFor(t, func() bool { mgr.mu.Lock(); defer mgr.mu.Unlock(); return len(mgr.workers) == 0 })
	close(unblock)
	if err := <-result; !errors.Is(err, errWarmUnavailable) {
		t.Fatal("a worker evicted during launch served a read:", err)
	}
	if err := <-killed; err != nil {
		t.Fatal(err)
	}
	if mgr.metrics.Spawns.Load() != 0 {
		t.Fatal("an evicted launch counted as a spawn")
	}
	// An eviction is not a launch failure: the key may relaunch at once.
	mgr.mu.Lock()
	_, backingOff := mgr.backoff[key]
	mgr.mu.Unlock()
	if mgr.metrics.LaunchFailures.Load() != 0 || backingOff {
		t.Fatal("an evicted launch opened a backoff window")
	}
}

// Eviction waits are bounded by their context.
func TestReadAuthorizationKillWaitHonoursContext(t *testing.T) {
	launcher := &mockLauncher{onLaunch: func(context.Context) (WorkerProcess, func() error, error) {
		proc := newFakeWorkerProcess()
		go serveFake(proc, nil)
		return proc, func() error { time.Sleep(2 * time.Second); return proc.Kill() }, nil
	}}
	mgr, exec := newTestPool(t, launcher)
	req := opReq("op-slow-cleanup")
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, req, &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	key := warmKey(req.Source.TenantID, req.AppTeam, req.Source.ConnectionID, req.Source.Database, req.Source.Schema, req.Source.Revision)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := mgr.killKey(ctx, key); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatal("kill wait ignored its context", err, time.Since(start))
	}
}

// dynamicReads serves delegated reads through the fixture resolver. Each
// principal's grant names its saved connection; the resolver answers with
// validFor windows and counts calls per grant.
type dynamicReads struct {
	t        *testing.T
	e        *Executor
	mgr      *WarmPoolManager
	fs       *fakeSet
	seen     []adapter.ProcessRequest
	calls    sync.Map // grant -> *atomic.Int32
	validFor atomic.Int64
	grantTTL atomic.Int64 // how long each read's delegation lasts
	hold     atomic.Pointer[chan struct{}]
	deny     sync.Map // grant -> true
}

func newDynamicReads(t *testing.T) *dynamicReads {
	t.Helper()
	d := &dynamicReads{t: t, fs: &fakeSet{}}
	d.validFor.Store(int64(30 * time.Second)) // inside the grants' one-minute expiry
	d.grantTTL.Store(int64(time.Minute))
	d.fs.serve = recordingServe(d.fs, &d.seen)
	resolverClient := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		var body resolver.QueryRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		counter, _ := d.calls.LoadOrStore(body.Delegation, new(atomic.Int32))
		counter.(*atomic.Int32).Add(1)
		if hold := d.hold.Load(); hold != nil {
			<-*hold
		}
		if _, denied := d.deny.Load(body.Delegation); denied {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		writeConnectionResponse(t, w, connectionResolution{Version: 1, DelegationSHA256: delegation.Digest(body.Delegation),
			ValidUntil: time.Now().Add(time.Duration(d.validFor.Load())).Unix(),
			Sources:    []catalog.Source{{ID: "selected", Type: "postgres", DSNEnv: "KELVO_SOURCE_REQUEST_0_DSN"}},
			Secrets:    map[string]string{"KELVO_SOURCE_REQUEST_0_DSN": connectionFixtureDSN}})
	})
	resolverClient.slots = make(chan struct{}, 32)
	e, mgr := readWorkerExecutor(t, d.fs)
	mgr.cfg.CacheDisabled, mgr.cache = true, nil
	e, err := e.WithConnectionResolvers(map[string]*ConnectionResolver{"gateway": resolverClient})
	if err != nil {
		t.Fatal(err)
	}
	d.e, d.mgr = e, mgr
	return d
}

// read sends one delegated read as subject, through saved connection.
func (d *dynamicReads) read(ctx context.Context, subject, saved, sql string) error {
	r := query.Request{Mode: "native", ConnectionID: "selected", SQL: sql, Delegation: "grant-" + subject + "-" + saved}
	digest, err := delegation.QueryDigest(r)
	if err != nil {
		return err
	}
	claims := delegation.Claims{Issuer: "gateway", ClusterTenant: "tenant", AppTeam: "team-a", ServicePrincipal: "db-api", Subject: delegation.Subject{Kind: "user", ID: subject},
		QuerySHA256: digest, ExpiresAt: time.Now().Add(time.Duration(d.grantTTL.Load())).Unix(), Sources: []delegation.Source{{Alias: "selected", ConnectionID: saved, Database: "analytics"}}}
	ctx = delegation.WithExecution(ctx, claims, r.Delegation, delegation.ExecutionBinding{JobID: "job", WorkerID: "worker", Owner: "owner", Claim: "claim"})
	_, err = d.e.Execute(ctx, r, &workerTestSink{})
	return err
}

func (d *dynamicReads) resolves(subject, saved string) int32 {
	counter, ok := d.calls.Load("grant-" + subject + "-" + saved)
	if !ok {
		return 0
	}
	return counter.(*atomic.Int32).Load()
}

// Two principals with the same alias but different saved connections, or
// different subjects, never share an authorization; principals of one saved
// connection share its worker.
func TestReadAuthorizationKeyedByPrincipalAndSavedConnection(t *testing.T) {
	d := newDynamicReads(t)
	for i := 0; i < 2; i++ {
		for _, p := range [][2]string{{"p1", "saved-a"}, {"p2", "saved-b"}, {"p3", "saved-a"}} {
			if err := d.read(context.Background(), p[0], p[1], "SELECT 1"); err != nil {
				t.Fatal(p, err)
			}
		}
	}
	if d.resolves("p1", "saved-a") != 1 || d.resolves("p2", "saved-b") != 1 || d.resolves("p3", "saved-a") != 1 {
		t.Fatal("principals shared an authorization", d.resolves("p1", "saved-a"), d.resolves("p2", "saved-b"), d.resolves("p3", "saved-a"))
	}
	// Workers are keyed by saved connection: p1 and p3 share saved-a's worker,
	// p2 has saved-b's.
	if len(d.fs.procs) != 2 {
		t.Fatal("workers not keyed by saved connection", len(d.fs.procs))
	}
}

// The resolver's ValidUntil bounds an authorization below reauthorize_every,
// and is what worker requests carry.
func TestReadAuthorizationHonoursResolverValidUntil(t *testing.T) {
	d := newDynamicReads(t)
	d.validFor.Store(int64(2 * time.Second))
	if err := d.read(context.Background(), "p1", "saved-a", "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	d.fs.mu.Lock()
	first := d.seen[0]
	d.fs.mu.Unlock()
	if first.CredentialsValidUntil > time.Now().Add(2*time.Second).Unix() {
		t.Fatal("request carried a window beyond the resolver's", first.CredentialsValidUntil)
	}
	if err := d.read(context.Background(), "p1", "saved-a", "SELECT 1"); err != nil || d.resolves("p1", "saved-a") != 1 {
		t.Fatal("fresh authorization not reused", err)
	}
	time.Sleep(2100 * time.Millisecond)
	if err := d.read(context.Background(), "p1", "saved-a", "SELECT 1"); err != nil || d.resolves("p1", "saved-a") != 2 {
		t.Fatal("authorization outlived the resolver's ValidUntil", err, d.resolves("p1", "saved-a"))
	}
}

// Concurrent reads that find the authorization stale re-authorize once; a
// caller cancelling during re-authorization leaves the others served and the
// shared worker alive.
func TestReadAuthorizationConcurrentReauthorizationAndCancel(t *testing.T) {
	d := newDynamicReads(t)
	d.mgr.cfg.ReauthorizeEvery = 200 * time.Millisecond
	if err := d.read(context.Background(), "p1", "saved-a", "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	hold := make(chan struct{})
	d.hold.Store(&hold)
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leader := make(chan error, 1)
	go func() { leader <- d.read(leaderCtx, "p1", "saved-a", "SELECT 1") }()
	waitFor(t, func() bool { return d.resolves("p1", "saved-a") == 2 })
	var wg sync.WaitGroup
	var failed atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.read(context.Background(), "p1", "saved-a", "SELECT 1"); err != nil {
				failed.Add(1)
				t.Log("waiter:", err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	cancelLeader()
	if err := <-leader; !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled caller did not return its cancellation", err)
	}
	d.hold.Store(nil)
	close(hold)
	wg.Wait()
	if failed.Load() != 0 || d.fs.procs[0].killed.Load() || d.mgr.metrics.EvictionsAuth.Load() != 0 {
		t.Fatal("a cancelled re-authorization disturbed other callers", failed.Load(), d.fs.procs[0].killed.Load())
	}
	// The first read, the leader's cancelled re-authorization, and exactly
	// one more shared by all eight waiters.
	if n := d.resolves("p1", "saved-a"); n != 3 {
		t.Fatal("concurrent reads did not share one re-authorization", n)
	}
}

// A denial for one principal revokes only that principal's authorization; a
// worker another principal's authorization still uses stays alive.
func TestReadAuthorizationDenialSparesOtherPrincipals(t *testing.T) {
	d := newDynamicReads(t)
	d.mgr.cfg.ReauthorizeEvery = 150 * time.Millisecond
	for _, p := range []string{"p1", "p2"} {
		if err := d.read(context.Background(), p, "saved-a", "SELECT 1"); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	if err := d.read(context.Background(), "p2", "saved-a", "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	d.deny.Store("grant-p1-saved-a", true)
	if err := d.read(context.Background(), "p1", "saved-a", "SELECT 1"); err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatal("denied principal served", err)
	}
	if d.fs.procs[0].killed.Load() {
		t.Fatal("one principal's denial killed a worker another principal still uses")
	}
	if err := d.read(context.Background(), "p2", "saved-a", "SELECT 1"); err != nil {
		t.Fatal("other principal refused after a denial", err)
	}
}

// db-api issues query-path read credentials for 75s (reauthorize_every plus
// 15s slack). The node accepts them, and an authorization is then fresh for
// reauthorize_every (60s), not the 5s operation-resolver window.
func TestReadAuthorizationAcceptsSeventyFiveSecondReadCredential(t *testing.T) {
	d := newDynamicReads(t)
	d.grantTTL.Store(int64(5 * time.Minute))
	d.validFor.Store(int64(75 * time.Second))
	start := time.Now()
	if err := d.read(context.Background(), "p1", "saved-a", "SELECT 1"); err != nil {
		t.Fatal("75s read credential refused:", err)
	}
	if err := d.read(context.Background(), "p1", "saved-a", "SELECT 1"); err != nil || d.resolves("p1", "saved-a") != 1 {
		t.Fatal("75s authorization not reused", err)
	}
	_, saved, principal := readPrincipal(delegation.Execution{Claims: delegation.Claims{Issuer: "gateway", ClusterTenant: "tenant", AppTeam: "team-a", ServicePrincipal: "db-api",
		Subject: delegation.Subject{Kind: "user", ID: "p1"}, Sources: []delegation.Source{{Alias: "selected", ConnectionID: "saved-a", Database: "analytics"}}}}, true, "selected")
	id, _, _ := readPrincipal(delegation.Execution{Claims: delegation.Claims{Issuer: "gateway", ClusterTenant: "tenant", AppTeam: "team-a",
		Sources: []delegation.Source{{Alias: "selected", ConnectionID: "saved-a", Database: "analytics"}}}}, true, "selected")
	a := d.mgr.currentAuthorization(readAuthKeyFor(id, "selected", saved, principal))
	if a == nil || a.validUntil.Before(start.Add(74*time.Second)) {
		t.Fatal("the resolver's 75s window was not kept", a)
	}
	// Fresh until reauthorize_every (60s) even though the credential runs 75s.
	if !a.fresh(start.Add(59*time.Second), d.mgr.cfg.reauthorizeEvery()) || a.fresh(start.Add(61*time.Second), d.mgr.cfg.reauthorizeEvery()) {
		t.Fatal("authorization window is not min(reauthorize_every, ValidUntil)")
	}
}

// A pruned-looking (expired) authorization whose worker still lives is kept,
// so a later definitive denial still kills that worker; once the worker is
// gone the reaper drops it.
func TestReadAuthorizationExpiredStillRevokes(t *testing.T) {
	fs := &fakeSet{}
	var seen []adapter.ProcessRequest
	fs.serve = recordingServe(fs, &seen)
	secrets := &switchableSecrets{value: "postgres://reader:secret@db.internal:5432/appdb?sslmode=verify-full"}
	e, mgr := staticAuthorizationExecutor(t, fs, secrets)
	mgr.cfg.ReauthorizeEvery = 100 * time.Millisecond
	read := func() error {
		_, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}, &workerTestSink{})
		return err
	}
	if err := read(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	mgr.reapExpired() // expired, but its worker lives
	secrets.set(func(s *switchableSecrets) { s.missing = true })
	if err := read(); err == nil {
		t.Fatal("read served for a credential that is gone")
	}
	waitFor(t, func() bool { return fs.procs[0].killed.Load() })
	waitFor(t, func() bool { mgr.mu.Lock(); defer mgr.mu.Unlock(); return len(mgr.workers) == 0 })
	mgr.reapExpired()
	mgr.authMu.Lock()
	left := len(mgr.auths)
	mgr.authMu.Unlock()
	if mgr.metrics.EvictionsAuth.Load() != 1 || left != 0 {
		t.Fatal("denial after expiry did not revoke, or the reaper kept a dead authorization", mgr.metrics.EvictionsAuth.Load(), left)
	}
}

// A rotation waits for the old idle worker to end before the new read
// launches its replacement, so the old reservation is free.
func TestReadAuthorizationRotationWaitsForIdleWorker(t *testing.T) {
	var cleaned atomic.Int32
	launcher := &mockLauncher{onLaunch: func(context.Context) (WorkerProcess, func() error, error) {
		proc := newFakeWorkerProcess()
		go serveFake(proc, nil)
		return proc, func() error { time.Sleep(100 * time.Millisecond); cleaned.Add(1); return proc.Kill() }, nil
	}}
	mgr, exec := newTestPool(t, launcher)
	req := opReq("op-a")
	if _, err := mgr.Execute(context.Background(), exec, OperationProcessConfig{}, req, &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	key := warmKey(req.Source.TenantID, req.AppTeam, req.Source.ConnectionID, req.Source.Database, req.Source.Schema, req.Source.Revision)
	mgr.storeAuthorization("p", &readAuthorization{workerKey: key, at: time.Now()})
	mgr.storeAuthorization("p", &readAuthorization{workerKey: "rotated", at: time.Now()})
	if cleaned.Load() != 1 {
		t.Fatal("rotation returned before the idle old worker was cleaned up")
	}
}

// A caller that finds a fresh authorization (another caller just stored it)
// uses it instead of authorizing again; a caller that needs credentials for a
// new worker always authorizes.
func TestAuthorizeOnceReusesFreshAuthorization(t *testing.T) {
	mgr, _ := newTestPool(t, &mockLauncher{})
	stored := &readAuthorization{workerKey: "w", at: time.Now()}
	mgr.storeAuthorization("p", stored)
	var calls atomic.Int32
	authorize := func() (*readAuthorization, string, error) {
		calls.Add(1)
		return &readAuthorization{workerKey: "w", at: time.Now()}, "dsn", nil
	}
	a, dsn, err := mgr.authorizeOnce(context.Background(), "p", true, authorize)
	if err != nil || a != stored || dsn != "" || calls.Load() != 0 {
		t.Fatal("a fresh authorization was not reused", a == stored, dsn, calls.Load())
	}
	if _, dsn, err := mgr.authorizeOnce(context.Background(), "p", false, authorize); err != nil || dsn != "dsn" || calls.Load() != 1 {
		t.Fatal("a caller needing credentials did not authorize", dsn, calls.Load())
	}
}

// A read that outwaits its authorization while queued for a slot is
// authorized again and served on the worker, not failed as stale.
func TestReadOutwaitingItsAuthorizationIsReauthorized(t *testing.T) {
	release := make(chan struct{})
	fs := &fakeSet{serve: holdingServe(release)}
	e, mgr := readWorkerExecutor(t, fs)
	mgr.cfg.MaxInFlight, mgr.cfg.MaxPerKey, mgr.cfg.ReauthorizeEvery = 1, 1, time.Second
	read := func(sql string) error {
		_, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: sql}, &workerTestSink{})
		return err
	}
	held := make(chan error, 1)
	go func() { held <- read("SELECT 'hold'") }() // takes the worker's only slot
	waitFor(t, func() bool { return mgr.metrics.Spawns.Load() == 1 })
	queued := make(chan error, 1)
	go func() { queued <- read("SELECT 2") }()
	waitFor(t, func() bool { mgr.mu.Lock(); defer mgr.mu.Unlock(); return len(mgr.waiters) == 1 })
	authorized := mgr.metrics.Authorizations.Load()
	time.Sleep(2100 * time.Millisecond) // past the authorization's bound
	close(release)
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if err := <-queued; err != nil || mgr.metrics.Fallbacks.Load() != 0 || mgr.metrics.Authorizations.Load() <= authorized {
		t.Fatal("a read that outwaited its authorization was not re-authorized:", err, mgr.metrics.Fallbacks.Load(), mgr.metrics.Authorizations.Load())
	}
}
