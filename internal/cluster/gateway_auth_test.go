// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func bareAuthenticator(t *testing.T) *gatewayAuthenticator {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	a := &gatewayAuthenticator{config: GatewayAuthenticationConfig{KeysFile: "/unused-private-fixture", ReloadInterval: time.Second, MinRevision: 1}, tenants: map[string]bool{"a": true, "b": true}, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	t.Cleanup(func() { cancel(); a.invalidate() })
	return a
}

func TestGatewayKeyOverlapAndRevocationPreserveTenantBoundary(t *testing.T) {
	a := bareAuthenticator(t)
	if !a.apply(rotationSet(t, 1, rotationOld), time.Now()) {
		t.Fatal("initial keys rejected")
	}
	tenant, old, ok := a.lookup(rotationOld)
	if !ok || tenant != "a" {
		t.Fatal("tenant not derived from key")
	}
	_, other, _ := a.lookup(rotationOther)
	if !a.apply(rotationSet(t, 2, rotationOld, rotationNew), time.Now()) {
		t.Fatal("overlap rejected")
	}
	_, retained, _ := a.lookup(rotationOld)
	if retained != old || old.Err() != nil {
		t.Fatal("overlap canceled an unchanged key")
	}
	_, replacement, ok := a.lookup(rotationNew)
	if !ok {
		t.Fatal("new key unavailable")
	}
	if !a.apply(rotationSet(t, 3, rotationNew), time.Now()) {
		t.Fatal("revocation rejected")
	}
	if old.Err() == nil || replacement.Err() != nil || other.Err() != nil {
		t.Fatal("revocation not scoped to removed key")
	}
	if _, _, ok := a.lookup(rotationOld); ok {
		t.Fatal("revoked key still authenticates")
	}
	// Registering after the cancellation sweep must still observe revocation.
	done := make(chan struct{})
	stop := context.AfterFunc(old, func() { close(done) })
	defer stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("late request registration escaped revocation")
	}
}

func TestGatewayKeysFailClosedForRevisionEquivocationAndExpiry(t *testing.T) {
	a := bareAuthenticator(t)
	high := rotationSet(t, 8, rotationNew)
	if !a.apply(high, time.Now()) {
		t.Fatal("initial keys rejected")
	}
	_, active, _ := a.lookup(rotationNew)
	if a.apply(rotationSet(t, 8, rotationOld), time.Now()) || active.Err() == nil || a.ready() {
		t.Fatal("same revision changed authority")
	}
	if a.apply(rotationSet(t, 7, rotationOld), time.Now()) {
		t.Fatal("lower revision restored revoked key")
	}
	if !a.apply(high, time.Now()) {
		t.Fatal("identical good revision failed recovery")
	}
	_, recovered, _ := a.lookup(rotationNew)
	a.mu.Lock()
	a.validUntil = time.Now().Add(-time.Second)
	a.mu.Unlock()
	if _, _, ok := a.lookup(rotationNew); ok || recovered.Err() == nil || a.ready() {
		t.Fatal("stalled reloader retained expired authority")
	}
	if a.apply(high, time.Now().Add(-4*time.Second)) {
		t.Fatal("late read extended stale authority")
	}
	if !a.apply(high, time.Now()) {
		t.Fatal("fresh valid read failed recovery")
	}
	_, recovered, _ = a.lookup(rotationNew)
	a.invalidate()
	if recovered.Err() == nil || a.ready() {
		t.Fatal("read failure retained active keys")
	}
}

func TestGatewayKeyReadTimeoutHasOneOutstandingReaderAndCancelsWithoutRequests(t *testing.T) {
	a := bareAuthenticator(t)
	a.config.ReloadInterval = 20 * time.Millisecond // exercise the scheduler without a one-second initial wait
	if !a.apply(rotationSet(t, 1, rotationOld), time.Now()) {
		t.Fatal("initial keys rejected")
	}
	_, active, _ := a.lookup(rotationOld)
	var calls atomic.Int32
	entered, release, recovered := make(chan struct{}), make(chan struct{}), make(chan struct{})
	raw := rotationDocument(t, 1, map[string][]string{"a": {rotationOld}, "b": {rotationOther}})
	a.read = func(context.Context, string, int) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release // emulate a filesystem syscall which ignores context
			return append([]byte(nil), raw...), nil
		}
		select {
		case <-recovered:
		default:
			close(recovered)
		}
		return nil, errors.New("unavailable fixture")
	}
	go a.run()
	defer a.close()
	<-entered
	select {
	case <-active.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("background expiry did not cancel active request")
	}
	if calls.Load() != 1 {
		t.Fatal("stuck read spawned replacement goroutines")
	}
	close(release)
	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("loader failed to resume after late result")
	}
	if a.ready() {
		t.Fatal("late timed-out read repopulated authority")
	}
}

func TestGatewayKeyStartupRejectsPrivateFailuresWithoutDetails(t *testing.T) {
	secret := "sensitive-private-fixture"
	_, err := newGatewayAuthenticator(GatewayAuthenticationConfig{KeysFile: "/private/" + secret}, map[string]bool{"a": true}, func(context.Context, string, int) ([]byte, error) { return nil, errors.New(secret) })
	if err != errGatewayAuthUnavailable || strings.Contains(err.Error(), secret) {
		t.Fatal("private read error exposed")
	}
}

func TestGatewayFileAuthenticationProbesHeadersAndParkedCancellation(t *testing.T) {
	gateway, store, stop := admissionGateway(t, map[string]string{"waiting": Queued})
	defer stop()
	a := bareAuthenticator(t)
	gateway.auth = a
	if !a.apply(rotationSet(t, 1, rotationOld), time.Now()) {
		t.Fatal("initial keys rejected")
	}
	done := serveAdmission(gateway, context.Background(), "waiting")
	waitAdmission(t, func() bool { return len(gateway.resultWaiters) == 1 })
	a.invalidate()
	receiveAdmission(t, done)
	job, _ := store.Get(context.Background(), "waiting")
	if job.Job.State != Queued || job.Job.Claim != "" || len(gateway.resultWaiters) != 0 {
		t.Fatal("revocation consumed parked job or leaked permit")
	}
	for path, expected := range map[string]int{"/ready": 503, "/health": 200, "/v1/queries/waiting": 401} {
		out := httptest.NewRecorder()
		gateway.ServeHTTP(out, admissionRequest(context.Background(), path))
		if out.Code != expected {
			t.Fatalf("%s=%d", path, out.Code)
		}
	}
	if !a.apply(rotationSet(t, 2, rotationOld), time.Now()) {
		t.Fatal("recovery rejected")
	}
	request := admissionRequest(context.Background(), "/v1/queries/waiting")
	request.Header.Add("Authorization", "Bearer "+rotationOther)
	out := httptest.NewRecorder()
	gateway.ServeHTTP(out, request)
	if out.Code != 401 {
		t.Fatal("duplicate Authorization headers accepted")
	}
	for _, value := range []string{"Bearer " + rotationOld + ", " + rotationOther, "Bearer  " + rotationOld, "Bearer " + rotationOld + " ", "bearer " + rotationOld} {
		request := admissionRequest(context.Background(), "/v1/queries/waiting")
		request.Header.Set("Authorization", value)
		out := httptest.NewRecorder()
		gateway.ServeHTTP(out, request)
		if out.Code != 401 {
			t.Fatal("ambiguous bearer header accepted")
		}
	}
}

func TestGatewayKeyRevocationAbortsActiveRelayWithoutCompletion(t *testing.T) {
	gateway, store, stop := admissionGateway(t, map[string]string{"active": Assigned})
	defer stop()
	a := bareAuthenticator(t)
	gateway.auth = a
	if !a.apply(rotationSet(t, 1, rotationOld), time.Now()) {
		t.Fatal("initial keys rejected")
	}
	started := make(chan struct{})
	encoded, _ := compressedRelayFixture(t, "none")
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = w.Write(encoded[:100])
		w.(http.Flusher).Flush()
		close(started)
		<-request.Context().Done()
	}))
	defer upstream.Close()
	endpoint, _ := url.Parse(upstream.URL)
	gateway.tenants["a"] = gatewayTenant{store: store, workers: map[string]workerEndpoint{"a1": {url: endpoint, client: upstream.Client()}}}
	finished := make(chan any, 1)
	output := httptest.NewRecorder()
	go func() {
		defer func() { finished <- recover() }()
		gateway.ServeHTTP(output, admissionRequest(context.Background(), "/v1/queries/active/results"))
	}()
	<-started
	if !a.apply(rotationSet(t, 2, rotationNew), time.Now()) {
		t.Fatal("revocation failed")
	}
	select {
	case result := <-finished:
		if result != http.ErrAbortHandler {
			t.Fatal("revoked active relay did not abort", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("revocation failed to cancel active relay")
	}
	job, _ := store.Get(context.Background(), "active")
	if job.Job.State != Failed || len(gateway.permits) != 0 || strings.HasSuffix(output.Body.String(), string([]byte{255, 255, 255, 255, 0, 0, 0, 0})) {
		t.Fatal("revoked stream completed or leaked permit")
	}
}

func TestGatewayKeyConcurrentReadersAndReloads(t *testing.T) {
	a := bareAuthenticator(t)
	first, second := rotationSet(t, 1, rotationOld), rotationSet(t, 2, rotationNew)
	a.apply(first, time.Now())
	var group sync.WaitGroup
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 100 {
				a.lookup(rotationOld)
				a.lookup(rotationNew)
				a.ready()
			}
		}()
	}
	a.apply(second, time.Now())
	for range 100 {
		a.apply(second, time.Now())
	}
	group.Wait()
	if _, _, ok := a.lookup(rotationOld); ok {
		t.Fatal("old key survived reload")
	}
}

func TestGatewayKeyOwnerFenceSurvivesInvalidationAndRepeatedReload(t *testing.T) {
	a := bareAuthenticator(t)
	original := rotationSet(t, 1, rotationOld)
	raw := rotationDocument(t, 2, map[string][]string{"a": {}, "b": {rotationOld, rotationOther}})
	moved, err := parseGatewayKeys(raw, a.tenants, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"immediate", "invalid file", "expired"} {
		if !a.apply(original, time.Now()) {
			t.Fatal("valid ownership failed recovery")
		}
		if mode == "invalid file" {
			a.invalidate()
		}
		if mode == "expired" {
			a.mu.Lock()
			a.validUntil = time.Now().Add(-time.Second)
			a.mu.Unlock()
			a.ready()
		}
		for range 3 {
			if a.apply(moved, time.Now()) {
				t.Fatalf("%s: repeated reload moved a key across tenants", mode)
			}
			if _, _, ok := a.lookup(rotationOld); ok {
				t.Fatal("moved key authorized a tenant")
			}
		}
	}
}

func TestGatewayKeyFreshReloadDoesNotReviveExpiredRequestContext(t *testing.T) {
	a := bareAuthenticator(t)
	set := rotationSet(t, 1, rotationOld)
	if !a.apply(set, time.Now()) {
		t.Fatal("initial keys rejected")
	}
	_, original, _ := a.lookup(rotationOld)
	a.mu.Lock()
	a.validUntil = time.Now().Add(-time.Second)
	a.mu.Unlock()
	// Simulate a completed fresh read winning select against a due expiry timer.
	if !a.apply(set, time.Now()) {
		t.Fatal("fresh key document rejected")
	}
	_, current, ok := a.lookup(rotationOld)
	if !ok || current == original || original.Err() == nil || current.Err() != nil {
		t.Fatal("fresh reload revived an expired request context")
	}
}
