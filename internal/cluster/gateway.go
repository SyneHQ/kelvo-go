// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

const gatewayRequestLimit = 1 << 20

type gatewayTenant struct {
	store   Store
	workers map[string]workerEndpoint
}
type workerEndpoint struct {
	url    *url.URL
	client *http.Client
}
type Gateway struct {
	tenants      map[string]gatewayTenant
	tokens       map[[32]byte]string
	permits      chan struct{}
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	handlers     sync.WaitGroup
	mu           sync.RWMutex
	reconcileOK  map[string]bool
	reconcileErr error
	closed       bool
	once         sync.Once
}

func NewGateway(cfg GatewayConfig, stores map[string]Store) (*Gateway, error) {
	if cfg.MaxHTTPRequests < 1 || cfg.MaxHTTPRequests > 4096 || len(cfg.Tenants) == 0 {
		return nil, errors.New("cluster gateway: max_http_requests must be positive")
	}
	gctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{tenants: map[string]gatewayTenant{}, tokens: map[[32]byte]string{}, permits: make(chan struct{}, cfg.MaxHTTPRequests), ctx: gctx, cancel: cancel, reconcileOK: map[string]bool{}}
	for _, tc := range cfg.Tenants {
		tenant := tc.Policy.TenantID
		st := stores[tenant]
		_, duplicateTenant := g.tenants[tenant]
		if ValidatePolicy(tc.Policy) != nil || st == nil || !reflect.DeepEqual(st.Policy(), tc.Policy) || duplicateTenant || len(tc.Workers) != len(tc.Policy.Workers) {
			cancel()
			return nil, errors.New("cluster gateway: missing or mismatched tenant store")
		}
		token := os.Getenv(tc.TokenEnv)
		if len(token) < 32 {
			cancel()
			return nil, errors.New("cluster gateway: tenant token must contain at least 32 characters")
		}
		sum := sha256.Sum256([]byte(token))
		if _, ok := g.tokens[sum]; ok {
			cancel()
			return nil, errors.New("cluster gateway: duplicate tenant token")
		}
		g.tokens[sum] = tenant
		gt := gatewayTenant{store: st, workers: map[string]workerEndpoint{}}
		for _, ep := range tc.Workers {
			u, err := url.Parse(ep.URL)
			if err != nil || tc.Policy.Workers[ep.ID] < 1 || !validEndpoint(ep.URL) {
				cancel()
				return nil, errors.New("cluster gateway: invalid worker endpoint")
			}
			if _, exists := gt.workers[ep.ID]; exists {
				cancel()
				return nil, errors.New("cluster gateway: duplicate worker endpoint")
			}
			tlsCfg, err := BuildClientTLS(cfg.WorkerTLS, "spiffe://kelvo/tenant/"+tenant+"/worker/"+ep.ID)
			if err != nil {
				cancel()
				return nil, err
			}
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.Proxy = nil
			transport.TLSClientConfig = tlsCfg
			transport.TLSHandshakeTimeout = 5 * time.Second
			transport.ResponseHeaderTimeout = tc.Policy.Limits.Timeout
			transport.DisableCompression = true
			gt.workers[ep.ID] = workerEndpoint{url: u, client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
		}
		g.tenants[tenant] = gt
	}
	for tenant := range g.tenants {
		g.wg.Add(1)
		go g.reconcile(tenant)
	}
	return g, nil
}
func (g *Gateway) Close() error {
	g.once.Do(func() {
		g.mu.Lock()
		g.closed = true
		g.mu.Unlock()
		g.cancel()
		g.handlers.Wait()
		g.wg.Wait()
		for _, t := range g.tenants {
			for _, endpoint := range t.workers {
				endpoint.client.CloseIdleConnections()
			}
			_ = t.store.Close()
		}
	})
	return nil
}
func (g *Gateway) reconcile(tenant string) {
	defer g.wg.Done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		if err := g.tenants[tenant].store.Reconcile(g.ctx); err != nil && !errors.Is(err, context.Canceled) {
			g.mu.Lock()
			g.reconcileOK[tenant] = false
			g.reconcileErr = err
			g.mu.Unlock()
		} else if err == nil {
			g.mu.Lock()
			g.reconcileOK[tenant] = true
			g.reconcileErr = nil
			g.mu.Unlock()
		}
		select {
		case <-g.ctx.Done():
			return
		case <-tick.C:
		}
	}
}
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		g.err(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Service unavailable")
		return
	}
	g.handlers.Add(1)
	g.mu.Unlock()
	defer g.handlers.Done()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stopShutdown := context.AfterFunc(g.ctx, cancel)
	defer stopShutdown()
	r = r.WithContext(ctx)
	select {
	case g.permits <- struct{}{}:
		defer func() { <-g.permits }()
	default:
		g.err(w, http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "Request capacity unavailable")
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/health" {
		g.json(w, 200, map[string]string{"status": "ok"})
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/ready" {
		g.mu.RLock()
		ready := len(g.reconcileOK) == len(g.tenants)
		for tenant := range g.tenants {
			ready = ready && g.reconcileOK[tenant]
		}
		g.mu.RUnlock()
		if !ready {
			g.err(w, 503, "UNAVAILABLE", "Service unavailable")
		} else {
			g.json(w, 200, map[string]string{"status": "ready"})
		}
		return
	}
	tenant, ok := g.tenant(r)
	if !ok {
		w.Header().Set("WWW-Authenticate", "Bearer")
		g.err(w, 401, "UNAUTHENTICATED", "Authentication required")
		return
	}
	p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(p) < 2 || p[0] != "v1" || p[1] != "queries" {
		g.err(w, 404, "NOT_FOUND", "Not found")
		return
	}
	if len(p) == 2 && r.Method == "POST" {
		g.submit(w, r, g.tenants[tenant])
		return
	}
	if len(p) == 3 && r.Method == "GET" {
		g.status(w, r, g.tenants[tenant], p[2])
		return
	}
	if len(p) == 4 && p[3] == "cancel" && r.Method == "POST" {
		g.cancelJob(w, r, g.tenants[tenant], p[2])
		return
	}
	if len(p) == 4 && p[3] == "results" && r.Method == "GET" {
		g.results(w, r, g.tenants[tenant], p[2])
		return
	}
	g.err(w, 404, "NOT_FOUND", "Not found")
}
func (g *Gateway) tenant(r *http.Request) (string, bool) {
	v := r.Header.Get("Authorization")
	if !strings.HasPrefix(v, "Bearer ") {
		return "", false
	}
	h := sha256.Sum256([]byte(strings.TrimPrefix(v, "Bearer ")))
	t, ok := g.tokens[h]
	return t, ok
}
func (g *Gateway) submit(w http.ResponseWriter, r *http.Request, t gatewayTenant) {
	r.Body = http.MaxBytesReader(w, r.Body, gatewayRequestLimit)
	defer r.Body.Close()
	var req query.Request
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(&req) != nil || d.Decode(new(any)) != io.EOF {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid query request")
		return
	}
	if e := query.ValidateRequest(req); e != nil {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid query request")
		return
	}
	s, e := t.store.Submit(r.Context(), req)
	if errors.Is(e, ErrCapacity) {
		g.err(w, 429, "RESOURCE_EXHAUSTED", "Query capacity unavailable")
		return
	}
	if e != nil {
		g.err(w, 503, "UNAVAILABLE", "Service unavailable")
		return
	}
	// Submission is durable before enqueue. Reconciliation republishes queued jobs,
	// so a transient broker failure must not discard this accepted handle.
	_ = t.store.Enqueue(r.Context(), s.Job.ID)
	g.json(w, 201, map[string]string{"id": s.Job.ID, "state": s.Job.State})
}
func (g *Gateway) status(w http.ResponseWriter, r *http.Request, t gatewayTenant, id string) {
	s, e := t.store.Get(r.Context(), id)
	if e != nil {
		if errors.Is(e, ErrNotFound) {
			g.err(w, 404, "NOT_FOUND", "Query not found")
		} else {
			g.err(w, 503, "UNAVAILABLE", "Service unavailable")
		}
		return
	}
	g.json(w, 200, map[string]any{"id": s.Job.ID, "state": s.Job.State, "stats": s.Job.Stats, "error": s.Job.Error})
}
func (g *Gateway) cancelJob(w http.ResponseWriter, r *http.Request, t gatewayTenant, id string) {
	for range 8 {
		s, e := t.store.Get(r.Context(), id)
		if errors.Is(e, ErrNotFound) {
			g.err(w, 404, "NOT_FOUND", "Query not found")
			return
		}
		if e != nil {
			g.err(w, 503, "UNAVAILABLE", "Service unavailable")
			return
		}
		if !s.Job.Terminal() {
			n := s.Job
			n.State = Cancelled
			n.Error = &query.Error{Code: "CANCELLED", Message: "Query cancelled"}
			s, e = t.store.CompareAndSwap(r.Context(), s, n)
			if errors.Is(e, ErrConflict) {
				continue
			}
			if e != nil {
				g.err(w, 503, "UNAVAILABLE", "Service unavailable")
				return
			}
		}
		g.json(w, 200, map[string]string{"id": id, "state": s.Job.State})
		return
	}
	g.err(w, 409, "CONFLICT", "Query state changed")
}
func (g *Gateway) results(w http.ResponseWriter, r *http.Request, t gatewayTenant, id string) {
	var s Snapshot
	var e error
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		s, e = t.store.Get(r.Context(), id)
		if e != nil {
			if errors.Is(e, ErrNotFound) {
				g.err(w, 404, "NOT_FOUND", "Query not found")
			} else {
				g.err(w, 503, "UNAVAILABLE", "Service unavailable")
			}
			return
		}
		if time.Now().After(s.Job.ExpiresAt) {
			g.err(w, 404, "NOT_FOUND", "Query not found")
			return
		}
		if s.Job.State == Assigned {
			break
		}
		if s.Job.State == Queued {
			select {
			case <-r.Context().Done():
				return
			case <-poll.C:
				continue
			}
		}
		if s.Job.State == Claimed || s.Job.State == Running || s.Job.State == ResultReady || s.Job.State == Succeeded {
			g.err(w, 409, "ALREADY_CONSUMED", "Query results are single-consumer")
			return
		}
		g.err(w, 409, "QUERY_FAILED", "Query is not available")
		return
	}
	claim := make([]byte, 16)
	if _, e = rand.Read(claim); e != nil {
		g.err(w, 500, "INTERNAL", "Unable to claim query")
		return
	}
	n := s.Job
	n.State = Claimed
	n.Claim = hex.EncodeToString(claim)
	s, e = t.store.CompareAndSwap(r.Context(), s, n)
	if e != nil {
		g.err(w, 409, "ALREADY_CONSUMED", "Query results are single-consumer")
		return
	}
	ep, ok := t.workers[s.Job.WorkerID]
	if !ok || s.Job.Owner == "" {
		g.fail(t, s, "Worker unavailable")
		g.err(w, 503, "UNAVAILABLE", "Worker unavailable")
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), minTime(s.Job.ExpiresAt, time.Now().Add(t.store.Policy().Limits.Timeout)))
	defer cancel()
	u := *ep.url
	u.Path = strings.TrimRight(u.Path, "/") + "/internal/queries/" + url.PathEscape(id) + "/results"
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if e != nil {
		g.fail(t, s, "Worker unavailable")
		g.err(w, 503, "UNAVAILABLE", "Worker unavailable")
		return
	}
	req.Header.Set("X-Kelvo-Claim", n.Claim)
	resp, e := ep.client.Do(req)
	if e != nil {
		g.fail(t, s, "Worker unavailable")
		g.err(w, 503, "UNAVAILABLE", "Worker unavailable")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		g.fail(t, s, "Worker unavailable")
		g.err(w, 503, "UNAVAILABLE", "Worker unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(200)
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(t.store.Policy().Limits.Timeout))
	done := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			_ = controller.SetWriteDeadline(time.Now())
		case <-done:
		}
	}()
	defer func() { close(done); <-watcherDone; _ = controller.SetWriteDeadline(time.Time{}) }()
	limited := io.LimitReader(resp.Body, t.store.Policy().Limits.MaxBytes+1)
	// Arrow IPC EOS is the final eight bytes. Keep it local until durable
	// state confirms success: clients treat EOS as a complete result even if
	// the HTTP connection is aborted immediately afterwards.
	tail := &arrowEOSTail{w: w}
	written, e := io.CopyBuffer(tail, limited, make([]byte, 32<<10))
	if e != nil || written > t.store.Policy().Limits.MaxBytes {
		g.fail(t, s, "Arrow stream failed")
		panic(http.ErrAbortHandler)
	}
	if !tail.validEOS() || g.commitResult(ctx, t, s) != nil || tail.FlushEOS() != nil {
		g.fail(t, s, "Arrow stream incomplete")
		panic(http.ErrAbortHandler)
	}
}

// Pin the durable slot until the gateway has checked the worker's completion.
// Only then is it terminal and reusable by a subsequent submission.
func (g *Gateway) commitResult(ctx context.Context, t gatewayTenant, claimed Snapshot) error {
	for range 8 {
		final, err := t.store.Get(ctx, claimed.Job.ID)
		if err != nil {
			return err
		}
		if final.Job.State != ResultReady || final.Job.Owner != claimed.Job.Owner || final.Job.Claim != claimed.Job.Claim || final.Job.WorkerID != claimed.Job.WorkerID {
			return ErrConflict
		}
		next := final.Job
		next.State = Succeeded
		_, err = t.store.CompareAndSwap(ctx, final, next)
		if !errors.Is(err, ErrConflict) {
			return err
		}
	}
	return ErrConflict
}

// arrowEOSTail withholds the final eight Arrow IPC bytes until success state.
type arrowEOSTail struct {
	w    io.Writer
	tail []byte
}

func (t *arrowEOSTail) Write(p []byte) (int, error) {
	if len(t.tail)+len(p) <= 8 {
		t.tail = append(t.tail, p...)
		return len(p), nil
	}
	write := func(b []byte) error {
		if len(b) == 0 {
			return nil
		}
		n, err := t.w.Write(b)
		if err == nil && n != len(b) {
			return io.ErrShortWrite
		}
		return err
	}
	if len(p) >= 8 {
		if err := write(t.tail); err != nil {
			return 0, err
		}
		if err := write(p[:len(p)-8]); err != nil {
			return 0, err
		}
		t.tail = append(t.tail[:0], p[len(p)-8:]...)
	} else {
		extra := len(t.tail) + len(p) - 8
		if err := write(t.tail[:extra]); err != nil {
			return 0, err
		}
		copy(t.tail, t.tail[extra:])
		t.tail = t.tail[:len(t.tail)-extra]
		t.tail = append(t.tail, p...)
	}
	return len(p), nil
}
func (t *arrowEOSTail) validEOS() bool {
	return bytes.Equal(t.tail, []byte{255, 255, 255, 255, 0, 0, 0, 0})
}
func (t *arrowEOSTail) FlushEOS() error {
	if !t.validEOS() {
		return io.ErrUnexpectedEOF
	}
	n, err := t.w.Write(t.tail)
	if err != nil {
		return err
	}
	if n != len(t.tail) {
		return io.ErrShortWrite
	}
	return nil
}

func (g *Gateway) fail(t gatewayTenant, s Snapshot, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		current, err := t.store.Get(ctx, s.Job.ID)
		if err != nil || current.Job.Terminal() {
			return
		}
		if current.Job.Owner != s.Job.Owner || current.Job.Claim != s.Job.Claim {
			return
		}
		n := current.Job
		n.State = Failed
		n.Error = &query.Error{Code: "QUERY_FAILED", Message: message}
		if _, err = t.store.CompareAndSwap(ctx, current, n); err == nil {
			return
		}
	}
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func (g *Gateway) json(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (g *Gateway) err(w http.ResponseWriter, status int, code, msg string) {
	g.json(w, status, map[string]any{"error": &query.Error{Code: code, Message: msg}})
}

var _ http.Handler = (*Gateway)(nil)
