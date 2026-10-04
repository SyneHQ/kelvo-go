// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/httpstream"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// Initialization creates no supervisor or dispatch goroutines. Export stores
// borrow the already authenticated tenant connection and are never closed here.
func (g *Gateway) initExports(cfg GatewayConfig) error {
	if err := validateGatewayExports(cfg); err != nil {
		return err
	}
	bad := errors.New("invalid gateway export configuration")
	enabled := 0
	for _, tenant := range g.tenants {
		if tenant.store.Policy().Exports != nil {
			enabled++
		}
	}
	if cfg.Exports == nil {
		if enabled != 0 || len(cfg.RuntimeExportStores) != 0 {
			return bad
		}
		return nil
	}
	if enabled == 0 || g.auth == nil || cfg.Exports.MaxSupervisors < 1 || cfg.Exports.MaxSupervisors > 4096 || cfg.Exports.MaxDownloads < 1 || cfg.Exports.MaxDownloads > 4096 {
		return bad
	}
	for tenant := range cfg.RuntimeExportStores {
		t, ok := g.tenants[tenant]
		if !ok || t.store.Policy().Exports == nil {
			return bad
		}
	}
	stores := make(map[string]ExportStore, enabled)
	for id, tenant := range g.tenants {
		policy := tenant.store.Policy()
		if policy.Exports == nil {
			continue
		}
		if policy.Access == nil {
			return bad
		}
		store := cfg.RuntimeExportStores[id]
		if store == nil {
			base, ok := tenant.store.(*NATSStore)
			if !ok {
				return bad
			}
			var err error
			ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
			store, err = OpenExportStore(ctx, base, false)
			cancel()
			if err != nil {
				return err
			}
		}
		if !reflect.DeepEqual(store.Policy(), policy) {
			return bad
		}
		stores[id] = store
	}
	owner, err := randomToken()
	if err != nil {
		return err
	}
	g.exports, g.exportOwner = stores, owner
	g.exportSupervisors = make(chan struct{}, cfg.Exports.MaxSupervisors)
	g.exportDownloads = make(chan struct{}, cfg.Exports.MaxDownloads)
	return nil
}

// exportPrincipalSnapshot never returns foreign handles or their state. The
// current key and provisioned policy are checked on both sides of storage I/O.
func exportPrincipalSnapshot(ctx context.Context, store ExportStore, id string) (ExportSnapshot, error) {
	if err := requestAuthorityErr(ctx); err != nil {
		return ExportSnapshot{}, err
	}
	snapshot, err := store.GetExport(ctx, id)
	if err != nil {
		return ExportSnapshot{}, err
	}
	if err := requestAuthorityErr(ctx); err != nil {
		return ExportSnapshot{}, err
	}
	policy := store.Policy()
	authority, ok := jobAuthorityFromContext(ctx)
	if !ok || policy.Exports == nil || snapshot.Job.TenantID != policy.TenantID || authority != snapshot.Job.Authority.Principal || snapshot.Job.Authority.AuthorizationVersion != policy.Exports.AuthorizationVersion || validateJobAuthority(policy, &authority, snapshot.Job.Request) != nil || !time.Now().Before(snapshot.Job.ExpiresAt) {
		return ExportSnapshot{}, ErrExportNotFound
	}
	if _, err := exportIdentity(policy, snapshot.Job.Authority); err != nil {
		return ExportSnapshot{}, ErrExportNotFound
	}
	if err := validateExportJob(policy, snapshot.Job); err != nil {
		return ExportSnapshot{}, err
	}
	return snapshot, nil
}

func exportAuthorityDeadline(ctx context.Context, policy Policy, expiry time.Time) (time.Time, error) {
	if err := requestAuthorityErr(ctx); err != nil {
		return time.Time{}, err
	}
	auth, ok := ctx.Value(keyAuthorizationContext{}).(keyAuthorization)
	if !ok || auth.authenticator == nil || !auth.authenticator.active(auth.key) {
		return time.Time{}, context.Canceled
	}
	until := minTime(expiry, minTime(time.Now().Add(policy.LeaseDuration), auth.authenticator.expiry()))
	if !time.Now().Before(until) || requestAuthorityErr(ctx) != nil {
		return time.Time{}, context.Canceled
	}
	return until, nil
}

// Only selected authority values survive the submitting HTTP request. A
// disconnected client does not cancel accepted work; the exact initiating key,
// gateway lifetime and immutable execution budget still do.
func (g *Gateway) detachedExportContext(request context.Context) (context.Context, context.CancelFunc, error) {
	if err := requestAuthorityErr(request); err != nil {
		return nil, nil, err
	}
	authority, ok := jobAuthorityFromContext(request)
	auth, authenticated := request.Value(keyAuthorizationContext{}).(keyAuthorization)
	if !ok || !authenticated || auth.authenticator == nil || !auth.authenticator.active(auth.key) {
		return nil, nil, context.Canceled
	}
	ctx, cancel := context.WithCancel(g.ctx)
	ctx = context.WithValue(ctx, jobAuthorityKey{}, authority)
	ctx = context.WithValue(ctx, keyAuthorizationContext{}, auth)
	stop := context.AfterFunc(auth.key, cancel)
	return ctx, func() { stop(); cancel() }, nil
}

// Registration shares Close's closed fence. The permit covers the whole
// queued/executing/publication lifecycle and is independent of HTTP admission.
func (g *Gateway) registerExportSupervisor() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.draining || g.exportSupervisors == nil {
		return false
	}
	select {
	case g.exportSupervisors <- struct{}{}:
		g.wg.Add(1)
		return true
	default:
		return false
	}
}

func (g *Gateway) releaseExportSupervisor() {
	<-g.exportSupervisors
	g.wg.Done()
}

func exportPollInterval(policy Policy) time.Duration {
	return max(10*time.Millisecond, min(250*time.Millisecond, policy.LeaseDuration/4))
}

func (g *Gateway) superviseExport(parent context.Context, release context.CancelFunc, store ExportStore, initial ExportSnapshot) {
	defer g.releaseExportSupervisor()
	defer release()
	deadline := minTime(initial.Job.ExpiresAt, initial.Job.QueueDeadline.Add(initial.Job.Spec.QueryLimits.Timeout))
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	completed := false
	defer func() {
		if !completed {
			g.withdrawSupervisedExport(store, initial)
		}
	}()
	op, err := g.audit.beginRequest(ctx, store.Policy().TenantID, audit.ExportExecution)
	if err != nil {
		return
	}
	defer op.abort(ctx)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		g.watchExportAuthority(ctx, cancel, store, initial)
	}()
	defer func() { cancel(); <-watchDone }()
	claimed, err := g.claimSupervisedExport(ctx, store, initial)
	if err != nil {
		_ = op.complete(err)
		return
	}
	result, err := g.executeSupervisedExport(ctx, store, claimed)
	if err != nil {
		_ = op.complete(err)
		return
	}
	if requestAuthorityErr(ctx) != nil || op.complete(nil) != nil || requestAuthorityErr(ctx) != nil || g.publishSupervisedExport(ctx, store, claimed, result) != nil || requestAuthorityErr(ctx) != nil {
		return
	}
	completed = true
}

func (g *Gateway) watchExportAuthority(ctx context.Context, cancel context.CancelFunc, store ExportStore, initial ExportSnapshot) {
	tick := time.NewTicker(exportPollInterval(store.Policy()))
	defer tick.Stop()
	for {
		if err := g.renewExportAuthority(ctx, store, initial); err != nil {
			cancel()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (g *Gateway) renewExportAuthority(ctx context.Context, store ExportStore, initial ExportSnapshot) error {
	for range 8 {
		current, err := exportPrincipalSnapshot(ctx, store, initial.Job.ID)
		if err != nil {
			return err
		}
		if current.Job.SupervisorOwner != g.exportOwner || current.Job.SupervisorOwner != initial.Job.SupervisorOwner {
			return ErrExportConflict
		}
		if current.Job.State == ExportReady {
			return nil
		}
		if !exportActive(current.Job) || !time.Now().Before(current.Job.AuthorityUntil) {
			return ErrExportConflict
		}
		// Polling keeps cancellation responsive, but a read does not require
		// a write. Renew only in the latter half of the bounded authority
		// lease to avoid racing every worker heartbeat with redundant CAS.
		if time.Until(current.Job.AuthorityUntil) > store.Policy().LeaseDuration/2 {
			return nil
		}
		until, err := exportAuthorityDeadline(ctx, store.Policy(), current.Job.ExpiresAt)
		if err != nil {
			return err
		}
		if !until.After(current.Job.AuthorityUntil) {
			return nil
		}
		next := current.Job
		next.AuthorityUntil = until
		if err := requestAuthorityErr(ctx); err != nil {
			return err
		}
		_, err = store.CompareAndSwapExport(ctx, current, next)
		if !errors.Is(err, ErrExportConflict) {
			return err
		}
	}
	return ErrExportConflict
}

func (g *Gateway) claimSupervisedExport(ctx context.Context, store ExportStore, initial ExportSnapshot) (ExportSnapshot, error) {
	tick := time.NewTicker(exportPollInterval(store.Policy()))
	defer tick.Stop()
	for {
		current, err := exportPrincipalSnapshot(ctx, store, initial.Job.ID)
		if err != nil {
			return ExportSnapshot{}, err
		}
		if current.Job.SupervisorOwner != g.exportOwner || current.Job.SupervisorOwner != initial.Job.SupervisorOwner || !time.Now().Before(current.Job.AuthorityUntil) {
			return ExportSnapshot{}, ErrExportConflict
		}
		switch current.Job.State {
		case ExportAssigned:
			claim, err := randomToken()
			if err != nil {
				return ExportSnapshot{}, err
			}
			next := current.Job
			next.State, next.Claim = ExportClaimed, claim
			claimed, err := store.CompareAndSwapExport(ctx, current, next)
			if errors.Is(err, ErrExportConflict) {
				continue
			}
			return claimed, err
		case ExportQueued:
			if !time.Now().Before(current.Job.QueueDeadline) {
				return ExportSnapshot{}, context.DeadlineExceeded
			}
		default:
			// Even our own prior claim is never adopted or executed twice.
			return ExportSnapshot{}, ErrExportConflict
		}
		select {
		case <-ctx.Done():
			return ExportSnapshot{}, ctx.Err()
		case <-tick.C:
		}
	}
}

func (g *Gateway) executeSupervisedExport(ctx context.Context, store ExportStore, claimed ExportSnapshot) (ExportRunResult, error) {
	var result ExportRunResult
	endpoint, ok := g.tenants[store.Policy().TenantID].workers[claimed.Job.WorkerID]
	if !ok || claimed.Job.WorkerOwner == "" || requestAuthorityErr(ctx) != nil {
		return result, ErrExportConflict
	}
	// Exactly one execution attempt. A lost response never authorizes replay.
	u := *endpoint.url
	u.Path = strings.TrimRight(u.Path, "/") + "/internal/exports/" + url.PathEscape(claimed.Job.ID) + "/execute"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return result, err
	}
	req.Header.Set("X-Kelvo-Claim", claimed.Job.Claim)
	response, err := endpoint.client.Do(req)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, errors.New("export worker unavailable")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, gatewayRequestLimit+1))
	if err != nil || len(raw) > gatewayRequestLimit {
		return ExportRunResult{}, errors.New("invalid export completion")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&result) != nil || decoder.Decode(new(any)) != io.EOF {
		return ExportRunResult{}, errors.New("invalid export completion")
	}
	return result, requestAuthorityErr(ctx)
}

func (g *Gateway) publishSupervisedExport(ctx context.Context, store ExportStore, claimed ExportSnapshot, result ExportRunResult) error {
	for range 8 {
		current, err := exportPrincipalSnapshot(ctx, store, claimed.Job.ID)
		if err != nil {
			return err
		}
		if current.Job.State != ExportStored || current.Job.SupervisorOwner != g.exportOwner || current.Job.SupervisorOwner != claimed.Job.SupervisorOwner || current.Job.WorkerID != claimed.Job.WorkerID || current.Job.WorkerOwner != claimed.Job.WorkerOwner || current.Job.Claim != claimed.Job.Claim || !time.Now().Before(current.Job.AuthorityUntil) || !sameExportReceipt(current.Job.Receipt, &result.Receipt) || !reflect.DeepEqual(current.Job.Stats, result.Stats) || validateExportReceipt(store.Policy(), current.Job) != nil {
			return ErrExportConflict
		}
		if err := requestAuthorityErr(ctx); err != nil {
			return err
		}
		next := current.Job
		next.State = ExportReady
		_, err = store.CompareAndSwapExport(ctx, current, next)
		if !errors.Is(err, ErrExportConflict) {
			return err
		}
	}
	return ErrExportConflict
}

// Loss of a supervisor never replays SQL. A failed withdrawal remains bounded
// by persisted authority/expiry; uncertain publication is retained for review.
func (g *Gateway) withdrawSupervisedExport(store ExportStore, initial ExportSnapshot) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range 8 {
		current, err := store.GetExport(ctx, initial.Job.ID)
		if err != nil || current.Job.SupervisorOwner != g.exportOwner || current.Job.SupervisorOwner != initial.Job.SupervisorOwner || current.Job.Authority != initial.Job.Authority || (!exportActive(current.Job) && current.Job.State != ExportReady) {
			return
		}
		next := current.Job
		next.State = ExportCancelled
		next.Error = &query.Error{Code: "CANCELLED", Message: "Export supervision ended"}
		_, err = store.CompareAndSwapExport(ctx, current, next)
		if !errors.Is(err, ErrExportConflict) {
			return
		}
	}
}

// Called only after the normal gateway authentication/HTTP admission path.
func (g *Gateway) serveExportHTTP(w http.ResponseWriter, r *http.Request, tenant string, path []string) bool {
	if len(path) < 2 || path[0] != "v1" || path[1] != "exports" {
		return false
	}
	store := g.exports[tenant]
	if store == nil {
		g.err(w, http.StatusNotFound, "NOT_FOUND", "Not found")
		return true
	}
	switch {
	case len(path) == 2 && r.Method == http.MethodPost:
		g.submitExport(w, r, store)
	case len(path) == 3 && r.Method == http.MethodGet:
		g.exportStatus(w, r, store, path[2])
	case len(path) == 4 && path[3] == "cancel" && r.Method == http.MethodPost:
		g.cancelExport(w, r, store, path[2])
	case len(path) == 4 && path[3] == "manifest" && r.Method == http.MethodGet:
		g.exportManifest(w, r, store, path[2])
	case len(path) == 5 && path[3] == "parts" && r.Method == http.MethodGet:
		g.exportPart(w, r, store, path[2], path[4])
	default:
		g.err(w, http.StatusNotFound, "NOT_FOUND", "Not found")
	}
	return true
}

func (g *Gateway) exportHTTPError(w http.ResponseWriter, op *auditOperation, err error) {
	status, code, message := http.StatusServiceUnavailable, "UNAVAILABLE", "Export unavailable"
	switch {
	case errors.Is(err, ErrExportNotFound):
		status, code, message = http.StatusNotFound, "NOT_FOUND", "Export not found"
	case errors.Is(err, ErrExportCapacity):
		status, code, message = http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "Export capacity unavailable"
	case errors.Is(err, ErrExportConflict):
		status, code, message = http.StatusConflict, "CONFLICT", "Export state changed"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		status, code, message = http.StatusUnauthorized, "UNAUTHENTICATED", "Request authority expired"
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	g.auditError(w, op, status, code, message)
}

func (g *Gateway) submitExport(w http.ResponseWriter, r *http.Request, store ExportStore) {
	op, err := g.audit.beginRequest(r.Context(), store.Policy().TenantID, audit.ExportSubmit)
	if err != nil {
		g.exportHTTPError(w, nil, err)
		return
	}
	defer op.abort(r.Context())
	g.mu.RLock()
	draining := g.draining || g.closed
	g.mu.RUnlock()
	if draining {
		g.auditError(w, op, http.StatusServiceUnavailable, "UNAVAILABLE", "Service draining")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, gatewayRequestLimit)
	defer r.Body.Close()
	var request ExportSubmitRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	policy := store.Policy()
	if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF || request.Query.Mode != "federated" || query.ValidateRequest(request.Query) != nil || request.TTLSeconds < 0 || request.TTLSeconds > int64(policy.Exports.MaxTTL/time.Second) || (request.Compression != "" && request.Compression != "none" && request.Compression != "lz4_frame") {
		g.auditError(w, op, http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid export request")
		return
	}
	if _, err := normalizeExportSpec(policy, request.Compression); err != nil {
		g.auditError(w, op, http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid export request")
		return
	}
	if _, err := submissionAuthority(r.Context(), policy, request.Query); err != nil {
		g.auditError(w, op, http.StatusForbidden, "PERMISSION_DENIED", "Export access denied")
		return
	}
	ctx, cancel, err := g.detachedExportContext(r.Context())
	if err != nil {
		g.exportHTTPError(w, op, err)
		return
	}
	if !g.registerExportSupervisor() {
		cancel()
		g.exportHTTPError(w, op, ErrExportCapacity)
		return
	}
	started := false
	defer func() {
		if !started {
			cancel()
			g.releaseExportSupervisor()
		}
	}()
	ttl := time.Duration(request.TTLSeconds) * time.Second
	if ttl == 0 {
		ttl = policy.Exports.DefaultTTL
	}
	until, err := exportAuthorityDeadline(ctx, policy, time.Now().Add(ttl))
	if err != nil {
		g.exportHTTPError(w, op, err)
		return
	}
	snapshot, err := store.SubmitExport(r.Context(), ExportSubmission{Request: request.Query, TTL: ttl, Compression: request.Compression, SupervisorOwner: g.exportOwner, AuthorityUntil: until})
	if err != nil {
		g.exportHTTPError(w, op, err)
		return
	}
	if !g.auditSuccess(w, r, op) {
		g.withdrawSupervisedExport(store, snapshot)
		return
	}
	started = true
	go g.superviseExport(ctx, cancel, store, snapshot)
	// A temporary enqueue error leaves this accepted job and its supervisor
	// intact. Tenant reconciliation republishes only still-queued work.
	enqueueContext, stopEnqueue := context.WithTimeout(ctx, 3*time.Second)
	_ = store.EnqueueExport(enqueueContext, snapshot.Job.ID)
	stopEnqueue()
	if err := requestAuthorityErr(r.Context()); err != nil {
		g.exportHTTPError(w, nil, err)
		return
	}
	g.json(w, http.StatusCreated, ExportAcceptedResponse{ID: snapshot.Job.ID, State: snapshot.Job.State, ExpiresAt: snapshot.Job.ExpiresAt})
}

func exportPublicStatus(job ExportJob) ExportStatusResponse {
	stats := job.Stats
	public := ExportStatusResponse{ID: job.ID, State: job.State, CreatedAt: job.CreatedAt, ExpiresAt: job.ExpiresAt, Stats: query.Stats{Rows: stats.Rows, Batches: stats.Batches, Bytes: stats.Bytes, WireBytes: stats.WireBytes, DurationNS: stats.DurationNS}}
	if job.Error != nil {
		public.Error = &query.Error{Code: "EXPORT_FAILED", Message: "Export unavailable"}
		if job.State == ExportCancelled {
			public.Error = &query.Error{Code: "CANCELLED", Message: "Export cancelled"}
		} else if job.State == ExportPublicationUncertain {
			public.Error = &query.Error{Code: "PUBLICATION_UNCERTAIN", Message: "Export publication requires operator review"}
		}
	}
	return public
}

func (g *Gateway) exportStatus(w http.ResponseWriter, r *http.Request, store ExportStore, id string) {
	snapshot, err := exportPrincipalSnapshot(r.Context(), store, id)
	if err != nil {
		g.exportHTTPError(w, nil, err)
		return
	}
	if err := requestAuthorityErr(r.Context()); err != nil {
		g.exportHTTPError(w, nil, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	g.json(w, http.StatusOK, exportPublicStatus(snapshot.Job))
}

func (g *Gateway) cancelExport(w http.ResponseWriter, r *http.Request, store ExportStore, id string) {
	op, err := g.audit.beginRequest(r.Context(), store.Policy().TenantID, audit.ExportCancel)
	if err != nil {
		g.exportHTTPError(w, nil, err)
		return
	}
	defer op.abort(r.Context())
	for range 8 {
		snapshot, err := exportPrincipalSnapshot(r.Context(), store, id)
		if err != nil {
			g.exportHTTPError(w, op, err)
			return
		}
		if exportActive(snapshot.Job) || snapshot.Job.State == ExportReady {
			next := snapshot.Job
			next.State = ExportCancelled
			next.Error = &query.Error{Code: "CANCELLED", Message: "Export cancelled"}
			if err := requestAuthorityErr(r.Context()); err != nil {
				g.exportHTTPError(w, op, err)
				return
			}
			snapshot, err = store.CompareAndSwapExport(r.Context(), snapshot, next)
			if errors.Is(err, ErrExportConflict) {
				continue
			}
			if err != nil {
				g.exportHTTPError(w, op, err)
				return
			}
		}
		if !g.auditSuccess(w, r, op) {
			return
		}
		g.json(w, http.StatusOK, exportPublicStatus(snapshot.Job))
		return
	}
	g.exportHTTPError(w, op, ErrExportConflict)
}

func readyExportSnapshot(ctx context.Context, store ExportStore, id string, expected *ExportReceipt) (ExportSnapshot, error) {
	snapshot, err := exportPrincipalSnapshot(ctx, store, id)
	if err != nil {
		return ExportSnapshot{}, err
	}
	if snapshot.Job.State != ExportReady || validateExportReceipt(store.Policy(), snapshot.Job) != nil || (expected != nil && !sameExportReceipt(snapshot.Job.Receipt, expected)) {
		return ExportSnapshot{}, ErrExportConflict
	}
	return snapshot, nil
}

func (g *Gateway) exportManifest(w http.ResponseWriter, r *http.Request, store ExportStore, id string) {
	op, err := g.audit.beginRequest(r.Context(), store.Policy().TenantID, audit.ExportResults)
	if err != nil {
		g.exportHTTPError(w, nil, err)
		return
	}
	defer op.abort(r.Context())
	snapshot, err := readyExportSnapshot(r.Context(), store, id, nil)
	if err != nil {
		g.exportHTTPError(w, op, err)
		return
	}
	manifest := snapshot.Job.Receipt.Manifest
	response := ExportManifestResponse{ID: id, ExpiresAt: snapshot.Job.ExpiresAt, SchemaSHA256: manifest.SchemaSHA256, Rows: manifest.Rows, EncodedBytes: manifest.EncodedBytes, DecodedBytes: manifest.DecodedBytes, Parts: make([]ExportPartInfo, len(manifest.Parts))}
	for i, part := range manifest.Parts {
		response.Parts[i] = ExportPartInfo{Index: part.Index, Rows: part.Rows, Batches: part.Batches, EncodedBytes: part.EncodedBytes, DecodedBytes: part.DecodedBytes, SHA256: part.SHA256}
	}
	if !g.auditSuccess(w, r, op) {
		return
	}
	if _, err := readyExportSnapshot(r.Context(), store, id, snapshot.Job.Receipt); err != nil {
		g.exportHTTPError(w, nil, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	g.json(w, http.StatusOK, response)
}

func (g *Gateway) exportPart(w http.ResponseWriter, r *http.Request, store ExportStore, id, rawIndex string) {
	op, err := g.audit.beginRequest(r.Context(), store.Policy().TenantID, audit.ExportResults)
	if err != nil {
		g.exportHTTPError(w, nil, err)
		return
	}
	defer op.abort(r.Context())
	// Do not reveal part/range behavior for another principal's handle.
	if _, err := exportPrincipalSnapshot(r.Context(), store, id); err != nil {
		g.exportHTTPError(w, op, err)
		return
	}
	if len(r.Header.Values("Range")) != 0 {
		g.auditError(w, op, http.StatusRequestedRangeNotSatisfiable, "INVALID_ARGUMENT", "Export byte ranges are not supported")
		return
	}
	index, err := strconv.Atoi(rawIndex)
	if err != nil || index < 0 || index >= 256 || strconv.Itoa(index) != rawIndex {
		g.auditError(w, op, http.StatusNotFound, "NOT_FOUND", "Export part not found")
		return
	}
	select {
	case g.exportDownloads <- struct{}{}:
		defer func() { <-g.exportDownloads }()
	default:
		g.exportHTTPError(w, op, ErrExportCapacity)
		return
	}
	snapshot, err := readyExportSnapshot(r.Context(), store, id, nil)
	if err != nil {
		g.exportHTTPError(w, op, err)
		return
	}
	if index >= len(snapshot.Job.Receipt.Manifest.Parts) {
		g.exportHTTPError(w, op, ErrExportNotFound)
		return
	}
	part := snapshot.Job.Receipt.Manifest.Parts[index]
	endpoint, ok := g.tenants[store.Policy().TenantID].workers[snapshot.Job.WorkerID]
	if !ok {
		g.exportHTTPError(w, op, errors.New("export worker unavailable"))
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), minTime(snapshot.Job.ExpiresAt, time.Now().Add(store.Policy().Limits.Timeout)))
	defer cancel()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		tick := time.NewTicker(exportPollInterval(store.Policy()))
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if _, err := readyExportSnapshot(ctx, store, id, snapshot.Job.Receipt); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-watchDone }()
	u := *endpoint.url
	u.Path = strings.TrimRight(u.Path, "/") + "/internal/exports/" + url.PathEscape(id) + "/parts/" + strconv.Itoa(index)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		g.exportHTTPError(w, op, err)
		return
	}
	request.Header.Set("X-Kelvo-Export-Receipt", snapshot.Job.Receipt.ReceiptSHA256)
	response, err := endpoint.client.Do(request)
	if err != nil {
		g.exportHTTPError(w, op, err)
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/vnd.apache.arrow.stream" || response.Header.Get("Content-Encoding") != "" || (response.ContentLength >= 0 && response.ContentLength != part.EncodedBytes) {
		g.exportHTTPError(w, op, errors.New("invalid export part response"))
		return
	}
	if _, err := readyExportSnapshot(ctx, store, id, snapshot.Job.Receipt); err != nil {
		g.exportHTTPError(w, op, err)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Kelvo-Result-Completion", "durable-eos-v1")
	w.WriteHeader(http.StatusOK)
	stopWrites := httpstream.WatchWriteDeadline(ctx, w, minTime(snapshot.Job.ExpiresAt, time.Now().Add(store.Policy().Limits.Timeout)))
	defer stopWrites()
	tail, digest := &arrowEOSTail{w: w}, sha256.New()
	written, err := io.CopyBuffer(tail, io.TeeReader(io.LimitReader(response.Body, part.EncodedBytes+1), digest), make([]byte, 32<<10))
	if err != nil || written != part.EncodedBytes || hex.EncodeToString(digest.Sum(nil)) != part.SHA256 || !tail.validEOS() || requestAuthorityErr(ctx) != nil {
		panic(http.ErrAbortHandler)
	}
	if _, err := readyExportSnapshot(ctx, store, id, snapshot.Job.Receipt); err != nil || op.complete(nil) != nil || requestAuthorityErr(ctx) != nil {
		panic(http.ErrAbortHandler)
	}
	if _, err := readyExportSnapshot(ctx, store, id, snapshot.Job.Receipt); err != nil || requestAuthorityErr(ctx) != nil || tail.FlushEOS() != nil {
		panic(http.ErrAbortHandler)
	}
}
