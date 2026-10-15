// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/client"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

// streamStore is a tenant store whose Submit admits a job already assigned to
// the fixture worker, after failing busy a configurable number of times.
type streamStore struct {
	compressionRelayStore
	busy        atomic.Int32
	submits     atomic.Int32
	submitted   chan query.Request
	afterSubmit func()
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
	if s.afterSubmit != nil {
		s.afterSubmit()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot, nil
}

const streamToken = "ssssssssssssssssssssssssssssssss"

func streamFixture(t *testing.T, busy int32, wait time.Duration, skew int64) (*Gateway, *streamStore, []byte, query.Stats, *atomic.Int32, *atomic.Int32) {
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
	reported := stats
	reported.WireBytes += skew // what the worker claims it sent
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		workerHits.Add(1)
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		_, _ = w.Write(encoded[:len(encoded)-len(eos)])
		if !store.resultReady(r.Header.Get("X-Kelvo-Claim"), reported) {
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
	g, store, encoded, stats, requests, workerHits := streamFixture(t, 0, time.Second, 0)
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
	t.Logf("trace: POST /v1/queries/stream -> 200, %d Arrow bytes, receipt %s; gateway requests=%d store submits=%d worker fetches=%d", len(body), resp.Trailer.Get(receiptTrailer), requests.Load(), store.submits.Load(), workerHits.Load())
	if job := store.job(); job.State != Succeeded {
		t.Fatalf("job state %s", job.State)
	}
	// bypass_cache reaches the store, so the node's cache (when present) sees it.
	if submitted := <-store.submitted; !submitted.BypassCache {
		t.Fatal("bypass_cache was dropped")
	}
}

func TestStreamQueryWaitsForTenantCapacityThenSucceeds(t *testing.T) {
	g, store, encoded, _, _, _ := streamFixture(t, 3, 2*time.Second, 0)
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
	g, store, _, _, _, workerHits := streamFixture(t, 1<<20, 150*time.Millisecond, 0)
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
	g, _, encoded, _, _, _ := streamFixture(t, 0, 2*time.Second, 0)
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
	g, store, _, _, _, _ := streamFixture(t, 0, time.Second, 0)
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

func (s *streamStore) job() Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot.Job
}

func serveStreamDirect(g *Gateway, adm *requestAdmission, ctx context.Context) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/queries/stream", strings.NewReader(streamBody)).WithContext(ctx)
	w := httptest.NewRecorder()
	g.streamQuery(w, r, g.tenants["relay"], adm)
	return w
}

// A stream whose result slot is taken waits for it instead of failing after the
// job was submitted and audited.
func TestStreamQueryWaitsForResultSlot(t *testing.T) {
	g, store, encoded, _, _, _ := streamFixture(t, 0, 2*time.Second, 0)
	adm := &requestAdmission{active: make(chan struct{}, 1), waiting: make(chan struct{}, 1), held: admissionWaiting}
	adm.active <- struct{}{}
	adm.waiting <- struct{}{}
	go func() { time.Sleep(100 * time.Millisecond); <-adm.active }()
	w := serveStreamDirect(g, adm, context.Background())
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), encoded) || store.job().State != Succeeded {
		t.Fatalf("status=%d state=%s", w.Code, store.job().State)
	}
}

func TestStreamQueryFullResultSlotsRefuseAfterBoundedWaitAndCancelJob(t *testing.T) {
	g, store, _, _, _, workerHits := streamFixture(t, 0, 150*time.Millisecond, 0)
	adm := &requestAdmission{active: make(chan struct{}, 1), waiting: make(chan struct{}, 1), held: admissionWaiting}
	adm.active <- struct{}{}
	adm.waiting <- struct{}{}
	started := time.Now()
	w := serveStreamDirect(g, adm, context.Background())
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" || time.Since(started) < 150*time.Millisecond {
		t.Fatalf("status=%d retry=%q waited=%s", w.Code, w.Header().Get("Retry-After"), time.Since(started))
	}
	if store.job().State != Cancelled || workerHits.Load() != 0 {
		t.Fatalf("job %s worker=%d: a refused stream must not keep its slot", store.job().State, workerHits.Load())
	}
}

// Anyone could otherwise queue for the request slots with no credentials.
func TestUnauthenticatedStreamNeverQueuesForRequestSlot(t *testing.T) {
	g, store, _, _, _, _ := streamFixture(t, 0, 5*time.Second, 0)
	g.permits = make(chan struct{}, 1)
	g.permits <- struct{}{}
	server := httptest.NewServer(g)
	defer server.Close()
	started := time.Now()
	resp, err := http.Post(server.URL+"/v1/queries/stream", "application/json", strings.NewReader(streamBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized || time.Since(started) > time.Second || store.submits.Load() != 0 {
		t.Fatalf("status=%d waited=%s", resp.StatusCode, time.Since(started))
	}
}

// If the audit write after Submit fails, the accepted job is cancelled.
func TestStreamQueryCancelsJobWhenAuditSuccessFails(t *testing.T) {
	g, store, _, _, _, workerHits := streamFixture(t, 0, time.Second, 0)
	// The request's authority ends between Submit and the audit write, so the
	// audit step refuses a job that is already stored.
	ctx, end := context.WithCancel(context.Background())
	store.afterSubmit = end
	w := serveStreamDirect(g, nil, ctx)
	if w.Code != http.StatusForbidden || store.submits.Load() != 1 {
		t.Fatalf("status=%d submits=%d", w.Code, store.submits.Load())
	}
	if store.job().State != Cancelled || workerHits.Load() != 0 {
		t.Fatalf("orphaned job: state=%s", store.job().State)
	}
}

func TestStreamHeaderOnEveryStreamResponseOnly(t *testing.T) {
	g, _, _, _, _, _ := streamFixture(t, 1<<20, 50*time.Millisecond, 0)
	server := httptest.NewServer(g)
	defer server.Close()
	busy := streamRequest(t, server.URL, streamBody) // 429 after the bounded wait
	busy.Body.Close()
	bad := streamRequest(t, server.URL, `{"nope":1}`)
	bad.Body.Close()
	anon, _ := http.Post(server.URL+"/v1/queries/stream", "application/json", strings.NewReader(streamBody))
	anon.Body.Close()
	for name, resp := range map[string]*http.Response{"429": busy, "400": bad, "401": anon} {
		if resp.Header.Get(streamHeader) != "1" {
			t.Fatalf("%s response lacks %s", name, streamHeader)
		}
	}
	status, _ := http.Get(server.URL + "/v1/queries/x")
	status.Body.Close()
	if status.Header.Get(streamHeader) != "" {
		t.Fatal("marker leaked onto another endpoint")
	}
}

// The receipt promises the worker's stats; they must match what was relayed.
func TestStreamQueryRefusesReceiptWhenWireBytesDisagree(t *testing.T) {
	g, store, _, _, _, _ := streamFixture(t, 0, time.Second, 1)
	server := httptest.NewServer(g)
	defer server.Close()
	resp := streamRequest(t, server.URL, streamBody)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err == nil {
		t.Fatalf("stream completed despite mismatch (%d bytes, trailer %v)", len(body), resp.Trailer)
	}
	if resp.Trailer.Get(receiptTrailer) != "" {
		t.Fatal("receipt sent despite mismatch")
	}
	if bytes.HasSuffix(body, []byte{255, 255, 255, 255, 0, 0, 0, 0}) || store.job().State == Succeeded {
		t.Fatalf("EOS released or job committed: %s", store.job().State)
	}
}

// End to end with a real encoder and the real SDK: the worker's EncodedBytes
// equal what the client decoded, so the receipt check is satisfiable.
func TestStreamQueryEndToEndWithSDK(t *testing.T) {
	g, store, encoded, stats, _, _ := streamFixture(t, 0, time.Second, 0)
	server := httptest.NewTLSServer(g)
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	c, err := client.New(client.Config{URL: server.URL, BearerToken: streamToken, TLSConfig: &tls.Config{RootCAs: roots}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var rows int64
	got, err := c.QueryStream(context.Background(), query.Request{SQL: "select 1", Mode: "native", ConnectionID: "pg", BypassCache: true}, client.Authority{}, &countSink{rows: &rows})
	if err != nil {
		t.Fatal(err)
	}
	if got.WireBytes != stats.WireBytes || got.WireBytes != int64(len(encoded)) || got.Server.WireBytes != stats.WireBytes || rows != 768 || store.job().State != Succeeded {
		t.Fatalf("client=%+v worker=%+v rows=%d", got, stats, rows)
	}
	t.Logf("worker EncodedBytes=%d client decoded WireBytes=%d rows=%d", stats.WireBytes, got.WireBytes, rows)
}

type countSink struct{ rows *int64 }

func (s *countSink) Schema(*arrow.Schema) error { return nil }
func (s *countSink) Write(b arrow.RecordBatch) error {
	*s.rows += b.NumRows()
	return nil
}
