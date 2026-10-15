// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
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

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/httpstream"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
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
	operations             map[string]*gatewayOperations
	exports                map[string]ExportStore
	exportSupervisors      chan struct{}
	exportDownloads        chan struct{}
	exportOwner            string
	tracing                *tracing.Recorder
	audit                  *ServiceAudit
	closeErr               error
	tenants                map[string]gatewayTenant
	tokens                 map[[32]byte]string
	auth                   *gatewayAuthenticator
	workerIdentity         *tlsIdentity
	workerTrust            *tlsTrust
	workerCertificateUntil time.Time
	permits                chan struct{}
	resultWaiters          chan struct{}
	ctx                    context.Context
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	handlers               sync.WaitGroup
	mu                     sync.RWMutex
	reconcileOK            map[string]bool
	workerHealth           gatewayWorkerHealth
	workerHealthFreshness  time.Duration
	// streamWait overrides defaultStreamWait for tests.
	streamWait   time.Duration
	reconcileErr error
	draining     bool
	closed       bool
	once         sync.Once
}

func NewGateway(cfg GatewayConfig, stores map[string]Store) (*Gateway, error) {
	if cfg.MaxHTTPRequests < 1 || cfg.MaxHTTPRequests > 4096 || len(cfg.Tenants) == 0 {
		return nil, errors.New("cluster gateway: max_http_requests must be positive")
	}
	if err := validateGatewayAuthentication(&cfg); err != nil {
		return nil, err
	}
	authority, err := bindGatewayKeyAuthority(cfg)
	if err != nil {
		return nil, err
	}
	gctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{tenants: map[string]gatewayTenant{}, tokens: map[[32]byte]string{}, permits: make(chan struct{}, cfg.MaxHTTPRequests), resultWaiters: make(chan struct{}, cfg.MaxHTTPRequests), ctx: gctx, cancel: cancel, reconcileOK: map[string]bool{}}
	if cfg.WorkerTLS.IdentityFile != "" {
		var err error
		g.workerIdentity, err = newTLSIdentity(cfg.WorkerTLS, GatewayIdentity, x509.ExtKeyUsageClientAuth, nil)
		if err != nil {
			cancel()
			return nil, err
		}
	}
	started := false
	defer func() {
		if !started {
			_ = g.closeOperations()
			_ = g.audit.CloseBounded()
			_ = g.closeTracing()
			if g.workerIdentity != nil {
				g.workerIdentity.close()
			}
			if g.workerTrust != nil {
				g.workerTrust.close()
			}
		}
	}()
	if cfg.Tracing != nil {
		var err error
		g.tracing, err = tracing.New(*cfg.Tracing)
		if err != nil {
			cancel()
			return nil, err
		}
	}
	if cfg.WorkerTLS.Trust != nil {
		var err error
		g.workerTrust, err = newTLSTrust(*cfg.WorkerTLS.Trust, nil)
		if err != nil {
			cancel()
			return nil, err
		}
	}
	for _, tc := range cfg.Tenants {
		tenant := tc.Policy.TenantID
		st := stores[tenant]
		_, duplicateTenant := g.tenants[tenant]
		if ValidatePolicy(tc.Policy) != nil || st == nil || !reflect.DeepEqual(st.Policy(), tc.Policy) || duplicateTenant || len(tc.Workers) != len(tc.Policy.Workers) {
			cancel()
			return nil, errors.New("cluster gateway: missing or mismatched tenant store")
		}
		if cfg.Authentication == nil {
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
		}
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
			tlsCfg, err := buildClientTLSWithTrust(cfg.WorkerTLS, "spiffe://kelvo/tenant/"+tenant+"/worker/"+ep.ID, g.workerIdentity, g.workerTrust)
			if err != nil {
				cancel()
				return nil, err
			}
			if g.workerIdentity == nil {
				until := gatewayStaticCertificateExpiry(tlsCfg)
				if g.workerCertificateUntil.IsZero() || until.Before(g.workerCertificateUntil) {
					g.workerCertificateUntil = until
				}
			}
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.Proxy = nil
			transport.TLSClientConfig = tlsCfg
			transport.TLSHandshakeTimeout = 5 * time.Second
			transport.ResponseHeaderTimeout = tc.Policy.Limits.Timeout
			transport.DisableCompression = true
			var roundTripper http.RoundTripper = transport
			if g.workerTrust != nil {
				roundTripper, err = newTLSTrustTransport(transport, g.workerTrust, g.workerIdentity, WorkerIdentity(tenant, ep.ID))
				if err != nil {
					cancel()
					return nil, err
				}
			} else if g.workerIdentity != nil {
				roundTripper = &tlsIdentityTransport{Transport: transport, identity: g.workerIdentity}
			}
			gt.workers[ep.ID] = workerEndpoint{url: u, client: &http.Client{Transport: roundTripper, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
		}
		g.tenants[tenant] = gt
	}
	var endpointCount uint64
	for _, tenant := range g.tenants {
		count := uint64(len(tenant.workers))
		if count > ^uint64(0)-endpointCount {
			cancel()
			return nil, errors.New("cluster gateway: worker readiness budget overflow")
		}
		endpointCount += count
	}
	g.workerHealthFreshness = gatewayReadinessFreshness(endpointCount)
	if g.workerHealthFreshness <= 0 {
		cancel()
		return nil, errors.New("cluster gateway: invalid worker readiness budget")
	}
	if cfg.Authentication != nil {
		identities := make(map[string]bool, len(g.tenants))
		for tenant := range g.tenants {
			identities[tenant] = true
		}
		var err error
		g.auth, err = newGatewayAuthenticatorWithAuthority(*cfg.Authentication, identities, nil, openGatewayAuthState, authority)
		if err != nil {
			cancel()
			for _, tenant := range g.tenants {
				for _, endpoint := range tenant.workers {
					endpoint.client.CloseIdleConnections()
				}
			}
			return nil, err
		}
	}
	if cfg.Audit != nil {
		tenants := make([]string, 0, len(g.tenants))
		for tenant := range g.tenants {
			tenants = append(tenants, tenant)
		}
		var err error
		g.audit, err = OpenServiceAudit(cfg.Audit, "gateway", tenants)
		if err != nil {
			cancel()
			if g.auth != nil {
				err = errors.Join(err, g.auth.close())
			}
			return nil, err
		}
	}
	if err := g.initExports(cfg); err != nil {
		cancel()
		if g.auth != nil {
			err = errors.Join(err, g.auth.close())
		}
		for _, tenant := range g.tenants {
			for _, endpoint := range tenant.workers {
				endpoint.client.CloseIdleConnections()
			}
		}
		return nil, err
	}
	if err := g.initOperations(cfg); err != nil {
		cancel()
		if g.auth != nil {
			err = errors.Join(err, g.auth.close())
		}
		for _, tenant := range g.tenants {
			for _, endpoint := range tenant.workers {
				endpoint.client.CloseIdleConnections()
			}
		}
		return nil, err
	}
	for tenant := range g.tenants {
		g.wg.Add(1)
		go g.reconcile(tenant)
	}
	g.wg.Add(1)
	go g.watchWorkerReadiness()
	started = true
	return g, nil
}

// BeginDrain rejects new submissions without interrupting accepted handles.
func (g *Gateway) BeginDrain() {
	g.mu.Lock()
	g.draining = true
	g.mu.Unlock()
}

// Drain keeps status, cancellation and result retrieval available for the full
// grace period: durable queued handles can outlive any current HTTP request.
func (g *Gateway) Drain(ctx context.Context) error {
	g.BeginDrain()
	<-ctx.Done()
	return ctx.Err()
}

func (g *Gateway) Close() error {
	g.once.Do(func() {
		g.mu.Lock()
		g.closed = true
		g.mu.Unlock()
		g.cancel()
		var authCloseErr error
		if g.auth != nil {
			authCloseErr = g.auth.close()
		}
		if g.workerIdentity != nil {
			g.workerIdentity.close()
		}
		if g.workerTrust != nil {
			g.workerTrust.close()
		}
		g.handlers.Wait()
		g.wg.Wait()
		g.closeErr = errors.Join(authCloseErr, g.closeOperations(), g.audit.CloseBounded(), g.closeTracing())
		for _, t := range g.tenants {
			for _, endpoint := range t.workers {
				endpoint.client.CloseIdleConnections()
			}
			_ = t.store.Close()
		}
	})
	return g.closeErr
}
func (g *Gateway) reconcile(tenant string) {
	defer g.wg.Done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		err := g.tenants[tenant].store.Reconcile(g.ctx)
		if err == nil && g.exports[tenant] != nil {
			ctx, stop := context.WithTimeout(g.ctx, g.tenants[tenant].store.Policy().LeaseDuration)
			err = g.exports[tenant].ReconcileExports(ctx)
			stop()
		}
		if err == nil && g.operations[tenant] != nil {
			ctx, stop := context.WithTimeout(g.ctx, 5*time.Second)
			err = g.reconcileOperations(ctx, tenant)
			stop()
		}
		if err != nil && !errors.Is(err, context.Canceled) {
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
	if isStreamQuery(r) {
		// Every response from this endpoint, errors included, says that a node
		// that has it answered. A client falls back only without this header.
		w.Header().Set(streamHeader, "1")
	}
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
	if r.Method == http.MethodGet && r.URL.Path == "/health" {
		g.json(w, 200, map[string]string{"status": "ok"})
		return
	}
	if (g.workerIdentity != nil && !g.workerIdentity.ready()) || (g.workerTrust != nil && !g.workerTrust.ready()) {
		g.err(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Service unavailable")
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/ready" {
		g.mu.RLock()
		ready := !g.draining && g.ctx.Err() == nil && g.audit.Ready() && len(g.reconcileOK) == len(g.tenants) && g.workersReadyLocked(time.Now())
		if g.auth != nil {
			ready = ready && g.auth.ready()
		}
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
	select {
	case g.permits <- struct{}{}:
	default:
		// A single-call read waits a bounded time for a request slot; the
		// three-call path keeps its immediate refusal.
		if !isStreamQuery(r) {
			g.retryableBusy(w, "Request capacity unavailable")
			return
		}
		// Only an authenticated request may queue for a slot.
		if _, authContext, ok := g.authenticate(r); !ok || (authContext != nil && authContext.Err() != nil) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			g.audit.authenticationDenied()
			g.err(w, 401, "UNAUTHENTICATED", "Authentication required")
			return
		}
		if !g.waitForPermit(r.Context()) {
			g.retryableBusy(w, "Request capacity unavailable")
			return
		}
	}
	admission := requestAdmission{active: g.permits, waiting: g.resultWaiters, held: admissionActive}
	defer admission.release()
	tenant, authContext, ok := g.authenticate(r)
	if !ok {
		w.Header().Set("WWW-Authenticate", "Bearer")
		g.audit.authenticationDenied()
		g.err(w, 401, "UNAUTHENTICATED", "Authentication required")
		return
	}
	if authContext != nil {
		stopAuth := context.AfterFunc(authContext, cancel)
		defer stopAuth()
		if authContext.Err() != nil {
			w.Header().Set("WWW-Authenticate", "Bearer")
			g.audit.authenticationDenied()
			g.err(w, 401, "UNAUTHENTICATED", "Authentication required")
			return
		}
	}
	if authContext != nil {
		r = r.WithContext(context.WithValue(r.Context(), keyAuthorizationContext{}, keyAuthorization{g.auth, authContext}))
	}
	principalID := ""
	if authContext != nil {
		principalID, _ = authContext.Value(keyPrincipalID{}).(string)
	}
	policy := g.tenants[tenant].store.Policy()
	if policy.Access != nil {
		authority, permitted := authorityForPrincipal(policy, principalID)
		if !permitted {
			g.audit.authenticationDenied()
			g.err(w, 401, "UNAUTHENTICATED", "Authentication required")
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), jobAuthorityKey{}, authority))
	} else if principalID != "" {
		g.audit.authenticationDenied()
		g.err(w, 401, "UNAUTHENTICATED", "Authentication required")
		return
	}
	operationParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(operationParts) >= 2 && operationParts[0] == "v1" && operationParts[1] == "operation-inputs" {
		g.serveOperationUpload(w, r, tenant)
		return
	}
	if len(r.Header.Values(operationInputGrantHeader)) != 0 {
		g.err(w, 403, "PERMISSION_DENIED", "Input grants cannot authorize operations or queries")
		return
	}
	if len(operationParts) >= 2 && operationParts[0] == "v1" && operationParts[1] == "operations" {
		g.serveOperationHTTP(w, r, tenant, operationParts)
		return
	}
	if len(r.Header.Values(operationGrantHeader)) != 0 {
		g.err(w, 403, "PERMISSION_DENIED", "Operation grants cannot authorize queries")
		return
	}
	delegatedContext, stopDelegation, delegatedErr := delegatedHTTPContext(r, policy)
	if delegatedErr != nil {
		g.err(w, 403, "PERMISSION_DENIED", "Query access denied")
		return
	}
	defer stopDelegation()
	r = r.WithContext(delegatedContext)
	p := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(p) >= 2 && p[0] == "v1" && p[1] == "exports" {
		if r.Header.Get("X-Kelvo-Delegation") != "" {
			g.err(w, 403, "PERMISSION_DENIED", "Delegated exports are unavailable")
			return
		}
		g.serveExportHTTP(w, r, tenant, p)
		return
	}
	if len(p) < 2 || p[0] != "v1" || p[1] != "queries" {
		g.err(w, 404, "NOT_FOUND", "Not found")
		return
	}
	if len(p) == 2 && r.Method == "POST" {
		g.mu.RLock()
		draining := g.draining
		g.mu.RUnlock()
		if draining {
			g.err(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Service draining")
			return
		}
		g.submit(w, r, g.tenants[tenant])
		return
	}
	if len(p) == 3 && p[2] == "stream" && r.Method == "POST" {
		g.mu.RLock()
		draining := g.draining
		g.mu.RUnlock()
		if draining {
			g.err(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Service draining")
			return
		}
		g.streamQuery(w, r, g.tenants[tenant], &admission)
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
		g.results(w, r, g.tenants[tenant], p[2], &admission)
		return
	}
	if len(p) == 4 && p[3] == "connection-lease" && r.Method == http.MethodPost {
		g.connectionLease(w, r, g.tenants[tenant], p[2])
		return
	}
	g.err(w, 404, "NOT_FOUND", "Not found")
}
func (g *Gateway) authenticate(r *http.Request) (string, context.Context, bool) {
	values := r.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return "", nil, false
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	if g.auth != nil {
		return g.auth.lookup(token)
	}
	h := sha256.Sum256([]byte(token))
	t, ok := g.tokens[h]
	return t, nil, ok
}
func (g *Gateway) submit(w http.ResponseWriter, r *http.Request, t gatewayTenant) {
	if s, ok := g.submitJob(w, r, t, 0, nil); ok {
		g.json(w, 201, map[string]string{"id": s.Job.ID, "state": s.Job.State})
	}
}

// submitJob authorizes, audits and stores one query. On failure it has already
// written the response. A positive wait retries a full tenant for that long
// before refusing with a retryable error.
func (g *Gateway) submitJob(w http.ResponseWriter, r *http.Request, t gatewayTenant, wait time.Duration, accepted func(string)) (Snapshot, bool) {
	op, auditErr := g.audit.beginRequest(r.Context(), t.store.Policy().TenantID, audit.QuerySubmit)
	if auditErr != nil {
		g.err(w, 503, "UNAVAILABLE", "Audit storage unavailable")
		return Snapshot{}, false
	}
	defer op.abort(r.Context())

	r.Body = http.MaxBytesReader(w, r.Body, gatewayRequestLimit)
	defer r.Body.Close()
	var req query.Request
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(&req) != nil || d.Decode(new(any)) != io.EOF {
		g.auditError(w, op, 400, "INVALID_ARGUMENT", "Invalid query request")
		return Snapshot{}, false
	}
	if e := query.ValidateRequest(req); e != nil {
		g.auditError(w, op, 400, "INVALID_ARGUMENT", "Invalid query request")
		return Snapshot{}, false
	}
	if _, err := submissionAuthority(r.Context(), t.store.Policy(), req); err != nil {
		g.auditError(w, op, 403, "PERMISSION_DENIED", "Query access denied")
		return Snapshot{}, false
	}
	// Mint context only after authorization; client headers and ambient context
	// cannot choose a trace, including when tracing is disabled.
	span := g.tracing.Start(tracing.Carrier{}, tracing.ClusterSubmit)
	outcome := telemetry.OutcomeError
	defer finishClusterTrace(span, r.Context(), &outcome)
	ctx := tracing.WithCarrier(r.Context(), span.Carrier())
	s, e := t.store.Submit(ctx, req)
	if wait > 0 && errors.Is(e, ErrCapacity) {
		deadline := time.NewTimer(wait)
		defer deadline.Stop()
		poll := time.NewTicker(25 * time.Millisecond)
		defer poll.Stop()
	waiting:
		for errors.Is(e, ErrCapacity) {
			select {
			case <-ctx.Done():
				return Snapshot{}, false
			case <-deadline.C:
				break waiting
			case <-poll.C:
				s, e = t.store.Submit(ctx, req)
			}
		}
	}
	if errors.Is(e, ErrCapacity) {
		w.Header().Set("Retry-After", "1")
		message := "Query capacity unavailable"
		if wait > 0 {
			message += "; retry after 1s"
		}
		g.auditError(w, op, 429, "RESOURCE_EXHAUSTED", message)
		return Snapshot{}, false
	}
	if e != nil {
		g.auditError(w, op, 503, "UNAVAILABLE", "Service unavailable")
		return Snapshot{}, false
	}
	if accepted != nil {
		accepted(s.Job.ID)
	}
	// Submission is durable before enqueue. Reconciliation republishes queued jobs,
	// so a transient broker failure must not discard this accepted handle.
	_ = t.store.Enqueue(r.Context(), s.Job.ID)
	if !g.auditSuccess(w, r, op) {
		return Snapshot{}, false
	}
	outcome = telemetry.OutcomeSuccess
	return s, true
}
func (g *Gateway) status(w http.ResponseWriter, r *http.Request, t gatewayTenant, id string) {
	s, e := principalSnapshot(r.Context(), t, id)
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
	op, auditErr := g.audit.beginRequest(r.Context(), t.store.Policy().TenantID, audit.QueryCancel)
	if auditErr != nil {
		g.err(w, 503, "UNAVAILABLE", "Audit storage unavailable")
		return
	}
	defer op.abort(r.Context())

	for range 8 {
		s, e := principalSnapshot(r.Context(), t, id)
		if errors.Is(e, ErrNotFound) {
			g.auditError(w, op, 404, "NOT_FOUND", "Query not found")
			return
		}
		if e != nil {
			g.auditError(w, op, 503, "UNAVAILABLE", "Service unavailable")
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
				g.auditError(w, op, 503, "UNAVAILABLE", "Service unavailable")
				return
			}
		}
		if !g.auditSuccess(w, r, op) {
			return
		}
		g.json(w, 200, map[string]string{"id": id, "state": s.Job.State})
		return
	}
	g.auditError(w, op, 409, "CONFLICT", "Query state changed")
}
func (g *Gateway) results(w http.ResponseWriter, r *http.Request, t gatewayTenant, id string, admission *requestAdmission) {
	g.relayResults(w, r, t, id, admission, 0)
}

// relayResults serves one result. A positive wait is the single-call read: it
// waits that long for a result slot instead of refusing at once, and ends the
// body with the Kelvo-Query-Receipt trailer.
func (g *Gateway) relayResults(w http.ResponseWriter, r *http.Request, t gatewayTenant, id string, admission *requestAdmission, slotWait time.Duration) {
	op, auditErr := g.audit.beginRequest(r.Context(), t.store.Policy().TenantID, audit.QueryResults)
	if auditErr != nil {
		g.err(w, 503, "UNAVAILABLE", "Audit storage unavailable")
		return
	}
	defer op.abort(r.Context())

	var s Snapshot
	var e error
	var wait *tracing.Lifecycle
	waitStarted := false
	waitOutcome := telemetry.OutcomeError
	defer func() { finishClusterTrace(wait, r.Context(), &waitOutcome) }()
	// Verify the job's authority once; a re-read costs one store get. The wait
	// then sleeps until the store says the job changed (or the job expires),
	// not on a fixed 50 ms tick that re-verified two signatures each time.
	var authorized authorizedJob
	waiter := newJobWaiter(r.Context(), t.store, id)
	defer waiter.stop()
	for {
		s, e = authorized.snapshot(r.Context(), t, id)
		if e != nil {
			if errors.Is(e, ErrNotFound) {
				g.auditError(w, op, 404, "NOT_FOUND", "Query not found")
			} else {
				g.auditError(w, op, 503, "UNAVAILABLE", "Service unavailable")
			}
			return
		}
		if time.Now().After(s.Job.ExpiresAt) {
			g.auditError(w, op, 404, "NOT_FOUND", "Query not found")
			return
		}
		if s.Job.State == Assigned {
			waitOutcome = telemetry.OutcomeSuccess
			wait.End(waitOutcome)
			if !activateWithin(r.Context(), admission, slotWait) {
				w.Header().Set("Retry-After", "1")
				g.auditError(w, op, http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "Result request capacity unavailable")
				return
			}
			break
		}
		if s.Job.State == Queued {
			if !waitStarted {
				// This is locally observed polling wait, not broker queue latency.
				wait = g.tracing.Start(jobTrace(s.Job.Trace), tracing.ClusterResultWait)
				waitStarted = true
			}
			if !parkWithin(r.Context(), admission, slotWait) {
				w.Header().Set("Retry-After", "1")
				g.auditError(w, op, http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "Queued result wait capacity unavailable")
				return
			}
			if !waiter.wait(r.Context(), s.Job.ExpiresAt) {
				return
			}
			continue
		}
		if s.Job.State == Claimed || s.Job.State == Running || s.Job.State == ResultReady || s.Job.State == Succeeded {
			g.auditError(w, op, 409, "ALREADY_CONSUMED", "Query results are single-consumer")
			return
		}
		g.auditError(w, op, 409, "QUERY_FAILED", "Query is not available")
		return
	}
	// Authorization was checked on the durable snapshot above. This interval
	// includes worker execution and backpressure; it is not pure network time.
	relay := g.tracing.Start(jobTrace(s.Job.Trace), tracing.ClusterRelay)
	relayOutcome := telemetry.OutcomeError
	defer finishClusterTrace(relay, r.Context(), &relayOutcome)
	claim := make([]byte, 16)
	if _, e = rand.Read(claim); e != nil {
		g.auditError(w, op, 500, "INTERNAL", "Unable to claim query")
		return
	}
	n := s.Job
	n.State = Claimed
	n.Claim = hex.EncodeToString(claim)
	s, e = t.store.CompareAndSwap(r.Context(), s, n)
	if e != nil {
		g.auditError(w, op, 409, "ALREADY_CONSUMED", "Query results are single-consumer")
		return
	}
	ep, ok := t.workers[s.Job.WorkerID]
	if !ok || s.Job.Owner == "" {
		g.fail(t, s, "Worker unavailable")
		g.auditError(w, op, 503, "UNAVAILABLE", "Worker unavailable")
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), minTime(s.Job.ExpiresAt, time.Now().Add(t.store.Policy().Limits.Timeout)))
	defer cancel()
	u := *ep.url
	u.Path = strings.TrimRight(u.Path, "/") + "/internal/queries/" + url.PathEscape(id) + "/results"
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if e != nil {
		g.fail(t, s, "Worker unavailable")
		g.auditError(w, op, 503, "UNAVAILABLE", "Worker unavailable")
		return
	}
	req.Header.Set("X-Kelvo-Claim", n.Claim)
	resp, e := ep.client.Do(req)
	if e != nil {
		g.fail(t, s, "Worker unavailable")
		g.auditError(w, op, 503, "UNAVAILABLE", "Worker unavailable")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		g.fail(t, s, "Worker unavailable")
		g.auditError(w, op, 503, "UNAVAILABLE", "Worker unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.Header().Set("Cache-Control", "no-store")
	// This advertises the protocol, not success: only the final EOS below is
	// released after durable Succeeded CAS. Partial/aborted bodies prove nothing.
	w.Header().Set("Kelvo-Result-Completion", "durable-eos-v1")
	if slotWait > 0 {
		w.Header().Set("Trailer", receiptTrailer)
	}
	w.WriteHeader(200)
	stopWrites := httpstream.WatchWriteDeadline(ctx, w, time.Now().Add(t.store.Policy().Limits.Timeout))
	defer stopWrites()
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
	var done Job
	committed := func() bool {
		var verify func(Job) bool
		if slotWait > 0 {
			// The receipt promises these stats, so they must describe the bytes
			// this gateway actually relayed (the worker's encoded length, EOS
			// included). A mismatch fails the read; no receipt is sent.
			verify = func(j Job) bool { return j.Stats.WireBytes == written }
		}
		job, err := g.commitResult(ctx, t, s, verify)
		done = job
		return err == nil
	}
	if !tail.validEOS() || requestAuthorityErr(ctx) != nil || op.complete(nil) != nil || requestAuthorityErr(ctx) != nil || !committed() || requestAuthorityErr(ctx) != nil || tail.FlushEOS() != nil {
		g.fail(t, s, "Arrow stream incomplete")
		panic(http.ErrAbortHandler)
	}
	if slotWait > 0 {
		receipt, _ := json.Marshal(map[string]any{"id": done.ID, "state": done.State, "stats": done.Stats})
		w.Header().Set(receiptTrailer, string(receipt))
	}
	relayOutcome = telemetry.OutcomeSuccess
}

// Pin the durable slot until the gateway has checked the worker's completion.
// Only then is it terminal and reusable by a subsequent submission.
func (g *Gateway) commitResult(ctx context.Context, t gatewayTenant, claimed Snapshot, verify func(Job) bool) (Job, error) {
	for range 8 {
		final, err := principalSnapshot(ctx, t, claimed.Job.ID)
		if err != nil {
			return Job{}, err
		}
		if final.Job.State != ResultReady || final.Job.Owner != claimed.Job.Owner || final.Job.Claim != claimed.Job.Claim || final.Job.WorkerID != claimed.Job.WorkerID {
			return Job{}, ErrConflict
		}
		if verify != nil && !verify(final.Job) {
			return Job{}, ErrConflict
		}
		next := final.Job
		next.State = Succeeded
		_, err = t.store.CompareAndSwap(ctx, final, next)
		if !errors.Is(err, ErrConflict) {
			return next, err
		}
	}
	return Job{}, ErrConflict
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

const (
	streamQueryPath = "/v1/queries/stream"
	receiptTrailer  = "Kelvo-Query-Receipt"
	streamHeader    = "Kelvo-Stream"
	// defaultStreamWait bounds how long a single-call read waits at each
	// admission point (request slot, tenant query slot, result slot) before it
	// is refused with a retry hint.
	defaultStreamWait = 5 * time.Second
)

// isStreamQuery matches what the router matches: the path is trimmed of
// slashes there, so /v1/queries/stream/ reaches the stream handler too.
func isStreamQuery(r *http.Request) bool {
	return r.Method == http.MethodPost && strings.Trim(r.URL.Path, "/") == strings.Trim(streamQueryPath, "/")
}

func (g *Gateway) streamWaitFor() time.Duration {
	if g.streamWait != 0 {
		return g.streamWait
	}
	return defaultStreamWait
}

func (g *Gateway) waitForPermit(ctx context.Context) bool {
	timer := time.NewTimer(g.streamWaitFor())
	defer timer.Stop()
	select {
	case g.permits <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

// retryableBusy is the capacity refusal: 429 with Retry-After.
func (g *Gateway) retryableBusy(w http.ResponseWriter, message string) {
	w.Header().Set("Retry-After", "1")
	g.err(w, http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", message)
}

// parkWithin retries a waiter slot for up to wait, like activateWithin.
func parkWithin(ctx context.Context, a *requestAdmission, wait time.Duration) bool {
	if a.park() {
		return true
	}
	if wait <= 0 {
		return false
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	poll := time.NewTicker(25 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-poll.C:
			if a.park() {
				return true
			}
		}
	}
}

// activateWithin retries a result slot for up to wait. The slot is never held
// while waiting: activate itself does not block.
func activateWithin(ctx context.Context, a *requestAdmission, wait time.Duration) bool {
	if a.activate() {
		return true
	}
	if wait <= 0 {
		return false
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	poll := time.NewTicker(25 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-poll.C:
			if a.activate() {
				return true
			}
		}
	}
}

// streamQuery is submit, wait and fetch in one request: the same audit,
// authorization, tenant slots and limits as the three calls, composed on the
// gateway. The body is the Arrow IPC stream, whose final EOS is released only
// after the durable Succeeded commit, followed by the Kelvo-Query-Receipt
// trailer. Every refusal before the first byte is a JSON error; capacity
// refusals are 429 with Retry-After after a bounded wait.
func (g *Gateway) streamQuery(w http.ResponseWriter, r *http.Request, t gatewayTenant, admission *requestAdmission) {
	wait := g.streamWaitFor()
	// A read nobody will consume must not hold its tenant slot until expiry.
	// Registered before submitJob returns: the job exists from Submit on, even
	// if the audit write that follows fails.
	var accepted string
	defer func() {
		if accepted != "" {
			g.cancelUnfinished(t, accepted)
		}
	}()
	s, ok := g.submitJob(w, r, t, wait, func(id string) { accepted = id })
	if !ok {
		return
	}
	w.Header().Set("Kelvo-Query-Id", s.Job.ID)
	g.relayResults(w, r, t, s.Job.ID, admission, wait)
}

// cancelUnfinished cancels a job the stream did not carry to a terminal state.
func (g *Gateway) cancelUnfinished(t gatewayTenant, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range 4 {
		current, err := t.store.Get(ctx, id)
		if err != nil || current.Job.Terminal() {
			return
		}
		next := current.Job
		next.State = Cancelled
		next.Error = &query.Error{Code: "CANCELLED", Message: "Query cancelled"}
		if _, err = t.store.CompareAndSwap(ctx, current, next); !errors.Is(err, ErrConflict) {
			return
		}
	}
}
