// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type admissionStore struct {
	gatewayStore
	mu           sync.Mutex
	jobs         map[string]Snapshot
	getErr       error
	claims       int
	promoteAfter string
}

func (s *admissionStore) Get(_ context.Context, id string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getErr != nil {
		return Snapshot{}, s.getErr
	}
	job, ok := s.jobs[id]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	return job, nil
}
func (s *admissionStore) CompareAndSwap(ctx context.Context, previous Snapshot, next Job) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return Snapshot{}, ctx.Err()
	}
	current, ok := s.jobs[next.ID]
	if !ok || current.Revision != previous.Revision {
		return Snapshot{}, ErrConflict
	}
	if next.State == Claimed {
		s.claims++
	}
	nextSnapshot := Snapshot{Job: next, Revision: current.Revision + 1}
	s.jobs[next.ID] = nextSnapshot
	if next.ID == s.promoteAfter && next.State == Succeeded {
		for id, queued := range s.jobs {
			if queued.Job.State == Queued {
				queued.Job.State = Assigned
				queued.Revision++
				s.jobs[id] = queued
			}
		}
	}
	return nextSnapshot, nil
}
func (s *admissionStore) ready(id, claim string, stats query.Stats) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok || job.Job.State != Claimed || job.Job.Claim != claim || claim == "" {
		return false
	}
	job.Job.State, job.Job.Stats = ResultReady, stats
	job.Revision++
	s.jobs[id] = job
	return true
}

const admissionToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func admissionGateway(t *testing.T, states map[string]string) (*Gateway, *admissionStore, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	store := &admissionStore{gatewayStore: gatewayStore{policy: Policy{TenantID: "a", Limits: query.DefaultLimits()}}, jobs: map[string]Snapshot{}}
	for id, state := range states {
		store.jobs[id] = Snapshot{Revision: 1, Job: Job{ID: id, TenantID: "a", State: state, WorkerID: "a1", Owner: "owned", ExpiresAt: time.Now().Add(time.Minute)}}
	}
	gateway := &Gateway{tenants: map[string]gatewayTenant{"a": {store: store}},
		tokens:  map[[32]byte]string{sha256.Sum256([]byte(admissionToken)): "a"},
		permits: make(chan struct{}, 2), resultWaiters: make(chan struct{}, 2), ctx: ctx,
		reconcileOK: map[string]bool{"a": true}}
	return gateway, store, cancel
}
func admissionRequest(ctx context.Context, path string) *http.Request {
	request := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+admissionToken)
	return request
}
func waitAdmission(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("admission condition did not become true")
}
func serveAdmission(gateway *Gateway, ctx context.Context, id string) <-chan *httptest.ResponseRecorder {
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		output := httptest.NewRecorder()
		gateway.ServeHTTP(output, admissionRequest(ctx, "/v1/queries/"+id+"/results"))
		done <- output
	}()
	return done
}
func receiveAdmission(t *testing.T, done <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case output := <-done:
		return output
	case <-time.After(3 * time.Second):
		t.Fatal("result handler blocked after release")
		return nil
	}
}

func TestQueuedResultsCannotBlockEarlierAssignedDelivery(t *testing.T) {
	gateway, store, stop := admissionGateway(t, map[string]string{"earlier": Assigned, "later1": Queued, "later2": Queued})
	defer stop()
	store.promoteAfter = "earlier"
	encoded, stats := compressedRelayFixture(t, "none")
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		id := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/internal/queries/"), "/results")
		if !store.ready(id, request.Header.Get("X-Kelvo-Claim"), stats) {
			http.Error(w, "invalid claim", http.StatusConflict)
			return
		}
		_, _ = w.Write(encoded)
	}))
	defer upstream.Close()
	endpoint, _ := url.Parse(upstream.URL)
	gateway.tenants["a"] = gatewayTenant{store: store, workers: map[string]workerEndpoint{"a1": {url: endpoint, client: upstream.Client()}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	later1, later2 := serveAdmission(gateway, ctx, "later1"), serveAdmission(gateway, ctx, "later2")
	waitAdmission(t, func() bool { return len(gateway.resultWaiters) == 2 })
	if len(gateway.permits) != 0 {
		t.Fatal("parked results retain active permits")
	}
	status := httptest.NewRecorder()
	gateway.ServeHTTP(status, admissionRequest(ctx, "/v1/queries/earlier"))
	if status.Code != http.StatusOK {
		t.Fatal("queued results starved status request")
	}
	earlier := receiveAdmission(t, serveAdmission(gateway, ctx, "earlier"))
	if earlier.Code != http.StatusOK || !bytes.Equal(earlier.Body.Bytes(), encoded) {
		t.Fatalf("parked later results blocked earlier exact Arrow delivery: status=%d", earlier.Code)
	}
	for _, pending := range []struct {
		id   string
		done <-chan *httptest.ResponseRecorder
	}{{"later1", later1}, {"later2", later2}} {
		output := receiveAdmission(t, pending.done)
		// A waiter may observe assignment before the earlier response releases
		// its active slot. Retry that explicit pre-claim 429 without replaying work.
		if output.Code == http.StatusTooManyRequests {
			unclaimed, _ := store.Get(ctx, pending.id)
			if unclaimed.Job.State != Assigned || unclaimed.Job.Claim != "" {
				t.Fatal("reactivation rejection consumed claim")
			}
			output = receiveAdmission(t, serveAdmission(gateway, ctx, pending.id))
		}
		if output.Code != http.StatusOK || !bytes.Equal(output.Body.Bytes(), encoded) {
			t.Fatalf("out-of-order result lost exact Arrow stream: status=%d", output.Code)
		}
	}
	for id := range store.jobs {
		final, err := store.Get(ctx, id)
		if err != nil || final.Job.State != Succeeded {
			t.Fatalf("result %s did not commit", id)
		}
	}
	if store.claims != 3 || len(gateway.permits) != 0 || len(gateway.resultWaiters) != 0 {
		t.Fatal("claim or permit leaked after out-of-order delivery")
	}
}

func TestQueuedWaiterLimitReturns429WithoutClaimAndCancellationReleases(t *testing.T) {
	gateway, store, stop := admissionGateway(t, map[string]string{"first": Queued, "second": Queued, "third": Queued})
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, second := serveAdmission(gateway, ctx, "first"), serveAdmission(gateway, ctx, "second")
	waitAdmission(t, func() bool { return len(gateway.resultWaiters) == 2 })
	third := receiveAdmission(t, serveAdmission(gateway, ctx, "third"))
	if third.Code != http.StatusTooManyRequests || third.Header().Get("Retry-After") != "1" || third.Header().Get("Kelvo-Result-Completion") != "" {
		t.Fatal("full waiter pool did not reject without blocking")
	}
	for _, path := range []string{"/health", "/ready", "/v1/queries/third"} {
		output := httptest.NewRecorder()
		gateway.ServeHTTP(output, admissionRequest(ctx, path))
		if output.Code != http.StatusOK {
			t.Fatalf("parked waiters blocked %s", path)
		}
	}
	cancel()
	receiveAdmission(t, first)
	receiveAdmission(t, second)
	if store.claims != 0 || len(gateway.permits) != 0 || len(gateway.resultWaiters) != 0 {
		t.Fatal("waiting cancellation claimed results or leaked permits")
	}
	thirdJob, _ := store.Get(context.Background(), "third")
	if thirdJob.Job.State != Queued {
		t.Fatal("rejected waiter consumed durable handle")
	}
}

func TestWaitingResultsReleaseAfterExpiryStoreFailureAndShutdown(t *testing.T) {
	for _, reason := range []string{"expiry", "store_failure", "shutdown"} {
		t.Run(reason, func(t *testing.T) {
			gateway, store, stop := admissionGateway(t, map[string]string{"waiting": Queued})
			defer stop()
			done := serveAdmission(gateway, context.Background(), "waiting")
			waitAdmission(t, func() bool { return len(gateway.resultWaiters) == 1 })
			store.mu.Lock()
			if reason == "expiry" {
				job := store.jobs["waiting"]
				job.Job.ExpiresAt = time.Now().Add(-time.Second)
				store.jobs["waiting"] = job
			} else if reason == "store_failure" {
				store.getErr = errors.New("private store detail")
			}
			store.mu.Unlock()
			if reason == "shutdown" {
				stop()
			}
			output := receiveAdmission(t, done)
			if reason == "expiry" && output.Code != http.StatusNotFound {
				t.Fatal("expiry did not fail closed")
			}
			if reason == "store_failure" && (output.Code != http.StatusServiceUnavailable || strings.Contains(output.Body.String(), "private")) {
				t.Fatal("store failure leaked details or succeeded")
			}
			if store.claims != 0 || len(gateway.permits) != 0 || len(gateway.resultWaiters) != 0 {
				t.Fatal("failed wait leaked admission")
			}
		})
	}
}

func TestRequestAdmissionTransitionFailureAndDoubleRelease(t *testing.T) {
	active, waiting := make(chan struct{}, 1), make(chan struct{}, 1)
	active <- struct{}{}
	lease := &requestAdmission{active: active, waiting: waiting, held: admissionActive}
	if !lease.park() || len(active) != 0 || len(waiting) != 1 {
		t.Fatal("failed to park")
	}
	active <- struct{}{}
	if lease.activate() || len(active) != 1 || len(waiting) != 1 {
		t.Fatal("activation exceeded active limit")
	}
	lease.release()
	lease.release()
	if len(active) != 1 || len(waiting) != 0 {
		t.Fatal("failed transition released another handler's permit")
	}
	<-active
}

func TestAssignedWaiterCannotClaimWithoutActivePermit(t *testing.T) {
	gateway, store, stop := admissionGateway(t, map[string]string{"waiting": Queued})
	defer stop()
	done := serveAdmission(gateway, context.Background(), "waiting")
	waitAdmission(t, func() bool { return len(gateway.resultWaiters) == 1 })
	gateway.permits <- struct{}{}
	gateway.permits <- struct{}{}
	store.mu.Lock()
	job := store.jobs["waiting"]
	job.Job.State = Assigned
	job.Revision++
	store.jobs["waiting"] = job
	store.mu.Unlock()
	output := receiveAdmission(t, done)
	if output.Code != http.StatusTooManyRequests || len(gateway.resultWaiters) != 0 || len(gateway.permits) != 2 {
		t.Fatal("failed reactivation blocked or released another handler's permit")
	}
	final, _ := store.Get(context.Background(), "waiting")
	if final.Job.State != Assigned || final.Job.Claim != "" || store.claims != 0 {
		t.Fatal("failed reactivation consumed the result claim")
	}
	<-gateway.permits
	<-gateway.permits
}

func TestGatewayCompletionCapabilityCannotCertifyInterruptedCopy(t *testing.T) {
	gateway, store, stop := admissionGateway(t, map[string]string{"partial": Assigned})
	defer stop()
	encoded, _ := compressedRelayFixture(t, "none")
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = w.Write(encoded[:100])
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	defer upstream.Close()
	endpoint, _ := url.Parse(upstream.URL)
	gateway.tenants["a"] = gatewayTenant{store: store, workers: map[string]workerEndpoint{"a1": {url: endpoint, client: upstream.Client()}}}
	output := httptest.NewRecorder()
	aborted := false
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				if recovered != http.ErrAbortHandler {
					t.Fatalf("unexpected panic: %v", recovered)
				}
				aborted = true
			}
		}()
		gateway.ServeHTTP(output, admissionRequest(context.Background(), "/v1/queries/partial/results"))
	}()
	final, _ := store.Get(context.Background(), "partial")
	if !aborted || final.Job.State != Failed || output.Header().Get("Kelvo-Result-Completion") != "durable-eos-v1" {
		t.Fatal("interrupted copy was not an aborted, failed capability-bearing stream")
	}
	if bytes.HasSuffix(output.Body.Bytes(), []byte{255, 255, 255, 255, 0, 0, 0, 0}) || len(gateway.permits) != 0 {
		t.Fatal("interrupted copy emitted completion EOS or retained request admission")
	}
}
