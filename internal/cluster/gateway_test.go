package cluster

import (
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type gatewayStore struct {
	policy Policy
	jobs   map[string]Snapshot
}

func (s *gatewayStore) Policy() Policy { return s.policy }
func (s *gatewayStore) Submit(context.Context, query.Request) (Snapshot, error) {
	return Snapshot{}, ErrCapacity
}
func (s *gatewayStore) Get(_ context.Context, id string) (Snapshot, error) {
	v, ok := s.jobs[id]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	return v, nil
}
func (s *gatewayStore) CompareAndSwap(context.Context, Snapshot, Job) (Snapshot, error) {
	return Snapshot{}, ErrConflict
}
func (s *gatewayStore) Enqueue(context.Context, string) error                 { return nil }
func (s *gatewayStore) Next(context.Context) (Delivery, error)                { return nil, ErrNoJob }
func (s *gatewayStore) Reconcile(context.Context) error                       { return nil }
func (s *gatewayStore) ClaimWorker(context.Context, string, string) error     { return nil }
func (s *gatewayStore) HeartbeatWorker(context.Context, string, string) error { return nil }
func (s *gatewayStore) Close() error                                          { return nil }

func TestGatewayDerivesTenantFromTokenNotHeader(t *testing.T) {
	a := &gatewayStore{policy: Policy{TenantID: "a"}, jobs: map[string]Snapshot{"a": {Job: Job{ID: "a", TenantID: "a", State: Queued, ExpiresAt: time.Now().Add(time.Minute)}}}}
	b := &gatewayStore{policy: Policy{TenantID: "b"}, jobs: map[string]Snapshot{}}
	ta, tb := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	ha, hb := sha256.Sum256([]byte(ta)), sha256.Sum256([]byte(tb))
	g := &Gateway{tenants: map[string]gatewayTenant{"a": {store: a}, "b": {store: b}}, tokens: map[[32]byte]string{ha: "a", hb: "b"}, permits: make(chan struct{}, 1), ctx: context.Background()}
	r := httptest.NewRequest(http.MethodGet, "/v1/queries/a", nil)
	r.Header.Set("Authorization", "Bearer "+ta)
	r.Header.Set("X-Kelvo-Tenant", "b")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	r = httptest.NewRequest(http.MethodGet, "/v1/queries/a", nil)
	r.Header.Set("Authorization", "Bearer "+tb)
	w = httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant status=%d", w.Code)
	}
}

type bufferWriter struct{ b []byte }

func (w *bufferWriter) Write(p []byte) (int, error) { w.b = append(w.b, p...); return len(p), nil }
func TestArrowEOSTailWithholdsUntilDurableSuccess(t *testing.T) {
	w := &bufferWriter{}
	tail := &arrowEOSTail{w: w}
	payload := append([]byte("records"), []byte{255, 255, 255, 255, 0, 0, 0, 0}...)
	if _, err := tail.Write(payload); err != nil {
		t.Fatal(err)
	}
	if string(w.b) != "records" {
		t.Fatalf("premature EOS: %q", w.b)
	}
	if err := tail.FlushEOS(); err != nil {
		t.Fatal(err)
	}
	if string(w.b) != string(payload) {
		t.Fatal("EOS not released")
	}
}
func TestArrowEOSTailDoesNotEmitOnErrorOrCancel(t *testing.T) {
	w := &bufferWriter{}
	tail := &arrowEOSTail{w: w}
	payload := append([]byte("records"), []byte{255, 255, 255, 255, 0, 0, 0, 0}...)
	if _, err := tail.Write(payload); err != nil {
		t.Fatal(err)
	}
	// Gateway deliberately does not call FlushEOS for failed/cancelled durable state.
	if string(w.b) != "records" {
		t.Fatalf("unexpected EOS: %q", w.b)
	}
}
func TestGatewayMissingAuthentication(t *testing.T) {
	g := &Gateway{tenants: map[string]gatewayTenant{}, tokens: map[[32]byte]string{}, permits: make(chan struct{}, 1), ctx: context.Background()}
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/queries/x", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestQueuedResultsWaitUntilJobExpiry(t *testing.T) {
	s := &gatewayStore{policy: Policy{TenantID: "a"}, jobs: map[string]Snapshot{"pending": {Job: Job{ID: "pending", TenantID: "a", State: Queued, ExpiresAt: time.Now().Add(30 * time.Millisecond)}}}}
	w := httptest.NewRecorder()
	g := &Gateway{}
	g.results(w, httptest.NewRequest(http.MethodGet, "/v1/queries/pending/results", nil), gatewayTenant{store: s}, "pending")
	if w.Code != http.StatusNotFound {
		t.Fatalf("expired queued job status=%d", w.Code)
	}
	// Waiting for assignment does not consume or mutate the durable handle.
	if s.jobs["pending"].Job.State != Queued {
		t.Fatal("queued job was claimed")
	}
}
