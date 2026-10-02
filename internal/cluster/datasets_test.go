// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type datasetReaderFixture struct {
	mu                sync.Mutex
	snapshots         map[string]acceleration.Snapshot
	failures          map[string]error
	blocks            map[string]<-chan struct{}
	calls             map[string]int
	active, maxActive int
}

func (f *datasetReaderFixture) Status(ctx context.Context, id string) (acceleration.Snapshot, error) {
	f.mu.Lock()
	f.calls[id]++
	f.active++
	f.maxActive = max(f.maxActive, f.active)
	snapshot, failure, block := f.snapshots[id], f.failures[id], f.blocks[id]
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.active--; f.mu.Unlock() }()
	if block != nil {
		select {
		case <-ctx.Done():
			return acceleration.Snapshot{}, ctx.Err()
		case <-block:
		}
	}
	return snapshot, failure
}
func datasetReporterFixture(t *testing.T, ids, required []string) (*DatasetReporter, *datasetReaderFixture) {
	t.Helper()
	c := catalog.Config{Sources: []catalog.Source{{ID: "db", Type: "postgres", DSNEnv: "KELVO_PRIVATE_SOURCE"}}, Acceleration: &catalog.AccelerationConfig{TenantID: "tenant-a"}}
	for _, id := range ids {
		c.Acceleration.Datasets = append(c.Acceleration.Datasets, catalog.Dataset{ID: id, MaxAge: time.Hour, Query: query.Request{Mode: "native", ConnectionID: "db", SQL: "SELECT private_column FROM sensitive_table"}})
	}
	f := &datasetReaderFixture{snapshots: map[string]acceleration.Snapshot{}, failures: map[string]error{}, blocks: map[string]<-chan struct{}{}, calls: map[string]int{}}
	for _, id := range ids {
		fingerprint, err := c.DatasetFingerprint(id)
		if err != nil {
			t.Fatal(err)
		}
		f.snapshots[id] = acceleration.Snapshot{Dataset: id, Generation: strings.Repeat("a", 32), SchemaHash: strings.Repeat("b", 64), Fingerprint: fingerprint, RefreshedAt: time.Now().UTC(), Path: "/private/secret.parquet", ObjectKey: "s3://secret-bucket/key"}
	}
	r, err := NewDatasetReporter(c, f, required)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r, f
}
func reporterRows(t *testing.T, r *DatasetReporter) []DatasetHealth {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/datasets", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("diagnostic status: %d", w.Code)
	}
	for _, secret := range []string{"private", "secret", "sensitive", "KELVO_", "s3://", "fingerprint", "dsn"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("diagnostics leak: %s", secret)
		}
	}
	var rows []DatasetHealth
	if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestDatasetDiagnosticsClassifyWithoutLeakingBackendDetails(t *testing.T) {
	r, f := datasetReporterFixture(t, []string{"ready", "missing", "stale", "changed", "broken"}, nil)
	f.failures["missing"] = acceleration.ErrNotFound
	f.failures["broken"] = errors.New("secret source DSN and credentials")
	stale := f.snapshots["stale"]
	stale.RefreshedAt = time.Now().Add(-2 * time.Hour)
	f.snapshots["stale"] = stale
	changed := f.snapshots["changed"]
	changed.Fingerprint = "old"
	f.snapshots["changed"] = changed
	expected := map[string]string{"ready": "ready", "missing": "missing", "stale": "stale", "changed": "configuration_changed", "broken": "unavailable"}
	for _, row := range reporterRows(t, r) {
		if row.State != expected[row.ID] {
			t.Fatalf("unexpected state: %+v", row)
		}
	}
	if !r.Ready(context.Background()) {
		t.Fatal("optional failure affected readiness")
	}
}

func TestDatasetRequiredReadinessAndExpiryWithinCache(t *testing.T) {
	r, f := datasetReporterFixture(t, []string{"critical", "optional"}, []string{"critical"})
	r.entries["critical"].maxAge = 100 * time.Millisecond
	if !r.Ready(context.Background()) {
		t.Fatal("fresh required dataset not ready")
	}
	f.mu.Lock()
	optionalCalls := f.calls["optional"]
	criticalCalls := f.calls["critical"]
	f.mu.Unlock()
	if optionalCalls != 0 || criticalCalls != 1 {
		t.Fatal("readiness queried optional datasets")
	}
	// The snapshot age is evaluated at response time, even though cached
	// metadata remains within its two-second TTL.
	r.mu.Lock()
	entry := r.entries["critical"]
	entry.snapshot.RefreshedAt = time.Now().Add(-time.Second)
	r.mu.Unlock()
	if r.Ready(context.Background()) {
		t.Fatal("stale snapshot stayed ready inside cache TTL")
	}
	f.mu.Lock()
	calls := f.calls["critical"]
	f.mu.Unlock()
	if calls != 1 {
		t.Fatal("fresh cache caused another backend call")
	}
}

func TestDatasetProbesCoalesceAndBoundConcurrency(t *testing.T) {
	r, f := datasetReporterFixture(t, []string{"a", "b", "c", "d", "e", "f"}, nil)
	block := make(chan struct{})
	for id := range f.snapshots {
		f.blocks[id] = block
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/datasets", nil))
		}()
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		active := f.active
		f.mu.Unlock()
		if active == 4 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(block)
	wg.Wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.maxActive > 4 {
		t.Fatalf("probe concurrency exceeded: %d", f.maxActive)
	}
	for id, count := range f.calls {
		if count != 1 {
			t.Fatalf("uncoalesced %s: %d", id, count)
		}
	}
}

func TestDatasetOptionalHungChecksDoNotStarveRequiredReadiness(t *testing.T) {
	r, f := datasetReporterFixture(t, []string{"critical", "optional_a", "optional_b", "optional_c"}, []string{"critical"})
	block := make(chan struct{})
	for _, id := range []string{"optional_a", "optional_b", "optional_c"} {
		f.blocks[id] = block
	}
	r.start([]string{"optional_a", "optional_b", "optional_c"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !r.Ready(ctx) {
		t.Fatal("optional hung checks starved required readiness")
	}
	close(block)
}

func TestDatasetWholeRequestDeadlineAndCloseCancellation(t *testing.T) {
	r, f := datasetReporterFixture(t, []string{"a", "b", "c", "d", "e", "f"}, []string{"a", "b", "c", "d", "e", "f"})
	r.timeout = 50 * time.Millisecond
	block := make(chan struct{})
	for id := range f.snapshots {
		f.blocks[id] = block
	}
	started := time.Now()
	if r.Ready(context.Background()) {
		t.Fatal("hung required snapshots marked ready")
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("deadline applied sequentially per dataset")
	}
	r.Close()
	f.mu.Lock()
	active := f.active
	f.mu.Unlock()
	if active != 0 {
		t.Fatal("close left probe active")
	}
}

func TestDatasetNodeEndpointsRequireMTLSAndHealthRemainsIndependent(t *testing.T) {
	r, f := datasetReporterFixture(t, []string{"critical"}, []string{"critical"})
	f.failures["critical"] = acceleration.ErrNotFound
	node := &Node{cfg: NodeConfig{RuntimeDatasets: r}, ctx: context.Background()}
	w := httptest.NewRecorder()
	node.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/datasets", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatal("dataset diagnostics bypassed mTLS")
	}
	uri, _ := url.Parse(GatewayIdentity)
	cert := &x509.Certificate{URIs: []*url.URL{uri}}
	for path, want := range map[string]int{"/datasets": http.StatusOK, "/ready": http.StatusServiceUnavailable, "/health": http.StatusOK} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}
		response := httptest.NewRecorder()
		node.ServeHTTP(response, req)
		if response.Code != want {
			t.Fatalf("%s: got%d want%d", path, response.Code, want)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/datasets", nil)
	req.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}
	w = httptest.NewRecorder()
	node.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatal("diagnostics accepted mutation method")
	}
}

func TestDatasetReporterRejectsUnknownAndDuplicateRequirements(t *testing.T) {
	for _, required := range [][]string{{"missing"}, {"missing", "missing"}} {
		if _, err := NewDatasetReporter(catalog.Config{}, nil, required); err == nil {
			t.Fatal("unknown required dataset accepted")
		}
	}
	empty, err := NewDatasetReporter(catalog.Config{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if !empty.Ready(context.Background()) {
		t.Fatal("no required datasets should be ready")
	}
}

func TestDatasetRequiredGateFailsClosedWithoutRuntimeReporter(t *testing.T) {
	node := &Node{cfg: NodeConfig{RequiredDatasets: []string{"critical"}}, ctx: context.Background()}
	uri, _ := url.Parse(GatewayIdentity)
	cert := &x509.Certificate{URIs: []*url.URL{uri}}
	for path, want := range map[string]int{"/ready": http.StatusServiceUnavailable, "/health": http.StatusOK} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}
		response := httptest.NewRecorder()
		node.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("%s: got %d want %d", path, response.Code, want)
		}
	}
}

func TestDatasetRequiredGateRejectsWeakerAttachedReporter(t *testing.T) {
	reporter, _ := datasetReporterFixture(t, []string{"critical"}, nil)
	if reporter.hasRequired([]string{"critical"}) {
		t.Fatal("reporter silently ignored configured requirement")
	}
	if !reporter.hasRequired(nil) {
		t.Fatal("empty requirement set rejected")
	}
}
