// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// streamStore is a tenant store whose Submit admits a job already assigned to
// the fixture worker, after failing busy a configurable number of times.
type streamStore struct {
	compressionRelayStore
	busy      atomic.Int32
	submits   atomic.Int32
	submitted chan query.Request
}

func (s *streamStore) Submit(_ context.Context, req query.Request) (Snapshot, error) {
	s.submits.Add(1)
	if s.busy.Add(-1) >= 0 {
		return Snapshot{}, ErrCapacity
	}
	select {
	case s.submitted <- req:
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot, nil
}

const streamToken = "ssssssssssssssssssssssssssssssss"

func streamFixture(t *testing.T, busy int32, wait time.Duration) (*Gateway, *streamStore, []byte, query.Stats, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	encoded, stats := compressedRelayFixture(t, "lz4_frame")
	eos := []byte{255, 255, 255, 255, 0, 0, 0, 0}
	limits := query.DefaultLimits()
	limits.ResultCompression = "lz4_frame"
	store := &streamStore{submitted: make(chan query.Request, 1)}
	store.gatewayStore = gatewayStore{policy: Policy{TenantID: "relay", Limits: limits}}
	store.snapshot = Snapshot{Revision: 1, Job: Job{ID: "stream-result", TenantID: "relay", State: Assigned, Owner: "worker-owner", WorkerID: "worker-one", ExpiresAt: time.Now().Add(time.Minute)}}
	store.busy.Store(busy)
	var workerHits atomic.Int32
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		workerHits.Add(1)
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		_, _ = w.Write(encoded[:len(encoded)-len(eos)])
		if !store.resultReady(r.Header.Get("X-Kelvo-Claim"), stats) {
			t.Error("worker did not receive the gateway's claim")
			return
		}
		_, _ = w.Write(eos)
	}))
	t.Cleanup(upstream.Close)
	endpoint, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(streamToken))
	g := &Gateway{
		tenants:       map[string]gatewayTenant{"relay": {store: store, workers: map[string]workerEndpoint{"worker-one": {url: endpoint, client: upstream.Client()}}}},
		tokens:        map[[32]byte]string{hash: "relay"},
		permits:       make(chan struct{}, 4),
		resultWaiters: make(chan struct{}, 4),
		ctx:           context.Background(),
		streamWait:    wait,
	}
	var requests atomic.Int32
	return g, store, encoded, stats, &requests, &workerHits
}

func streamRequest(t *testing.T, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+"/v1/queries/stream", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+streamToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

const streamBody = `{"sql":"select 1","mode":"native","connection_id":"pg","bypass_cache":true}`

func TestStreamQueryIsOneRequestWithArrowThenReceipt(t *testing.T) {
	g, store, encoded, stats, requests, workerHits := streamFixture(t, 0, time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		g.ServeHTTP(w, r)
	}))
	defer server.Close()
	resp := streamRequest(t, server.URL, streamBody)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Kelvo-Result-Completion") != "durable-eos-v1" || resp.Header.Get("Kelvo-Query-Id") != "stream-result" {
		t.Fatalf("status=%d headers=%v", resp.StatusCode, resp.Header)
	}
	if !bytes.Equal(body, encoded) {
		t.Fatal("stream changed the Arrow bytes")
	}
	var receipt struct {
		ID, State string
		Stats     query.Stats
	}
	if err := json.Unmarshal([]byte(resp.Trailer.Get(receiptTrailer)), &receipt); err != nil {
		t.Fatalf("receipt trailer: %v (%v)", err, resp.Trailer)
	}
	if receipt.ID != "stream-result" || receipt.State != Succeeded || receipt.Stats.Rows != stats.Rows || receipt.Stats.WireBytes != stats.WireBytes {
		t.Fatalf("receipt %+v", receipt)
	}
	if got := requests.Load(); got != 1 || store.submits.Load() != 1 || workerHits.Load() != 1 {
		t.Fatalf("one read must be one request: requests=%d submits=%d worker=%d", got, store.submits.Load(), workerHits.Load())
	}
	if store.snapshot.Job.State != Succeeded {
		t.Fatalf("job state %s", store.snapshot.Job.State)
	}
	// bypass_cache reaches the store, so the node's cache (when present) sees it.
	if submitted := <-store.submitted; !submitted.BypassCache {
		t.Fatal("bypass_cache was dropped")
	}
}

func TestStreamQueryWaitsForTenantCapacityThenSucceeds(t *testing.T) {
	g, store, encoded, _, _, _ := streamFixture(t, 3, 2*time.Second)
	server := httptest.NewServer(g)
	defer server.Close()
	resp := streamRequest(t, server.URL, streamBody)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, encoded) || store.submits.Load() != 4 {
		t.Fatalf("status=%d submits=%d", resp.StatusCode, store.submits.Load())
	}
}

func TestStreamQueryAboveCapacityGetsRetryableError(t *testing.T) {
	g, store, _, _, _, workerHits := streamFixture(t, 1<<20, 150*time.Millisecond)
	server := httptest.NewServer(g)
	defer server.Close()
	started := time.Now()
	resp := streamRequest(t, server.URL, streamBody)
	defer resp.Body.Close()
	var body struct{ Error query.Error }
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") != "1" || body.Error.Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("status=%d retry=%q body=%+v", resp.StatusCode, resp.Header.Get("Retry-After"), body)
	}
	if time.Since(started) < 150*time.Millisecond || workerHits.Load() != 0 || store.submits.Load() < 2 {
		t.Fatalf("no bounded wait: waited=%s submits=%d", time.Since(started), store.submits.Load())
	}
}

func TestStreamQueryWaitsForRequestSlot(t *testing.T) {
	g, _, encoded, _, _, _ := streamFixture(t, 0, 2*time.Second)
	g.permits = make(chan struct{}, 1)
	g.permits <- struct{}{}
	go func() { time.Sleep(100 * time.Millisecond); <-g.permits }()
	server := httptest.NewServer(g)
	defer server.Close()
	resp := streamRequest(t, server.URL, streamBody)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, encoded) {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	// The three-call path keeps its immediate refusal.
	g.permits <- struct{}{}
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/queries", strings.NewReader(streamBody))
	req.Header.Set("Authorization", "Bearer "+streamToken)
	plain, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	plain.Body.Close()
	if plain.StatusCode != http.StatusTooManyRequests || plain.Header.Get("Retry-After") != "1" {
		t.Fatalf("submit status=%d", plain.StatusCode)
	}
}

func TestStreamQueryRejectsUnauthenticatedAndInvalid(t *testing.T) {
	g, store, _, _, _, _ := streamFixture(t, 0, time.Second)
	server := httptest.NewServer(g)
	defer server.Close()
	resp, err := http.Post(server.URL+"/v1/queries/stream", "application/json", strings.NewReader(streamBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d", resp.StatusCode)
	}
	bad := streamRequest(t, server.URL, `{"sql":"select 1","mode":"native","connection_id":"pg","unknown":1}`)
	bad.Body.Close()
	if bad.StatusCode != http.StatusBadRequest || store.submits.Load() != 0 {
		t.Fatalf("invalid status=%d submits=%d", bad.StatusCode, store.submits.Load())
	}
}
