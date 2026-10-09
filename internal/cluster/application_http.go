// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/httpstream"
	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

func (a *Application) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	values := r.Header.Values("Authorization")
	if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+a.token)) != 1 {
		applicationError(w, 401, "UNAUTHENTICATED", "Service access denied")
		return
	}
	if r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" || r.Header.Get("X-Kelvo-Delegation") != "" {
		applicationError(w, 400, "INVALID_ARGUMENT", "Invalid operation envelope")
		return
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		applicationError(w, 503, "UNAVAILABLE", "Service closed")
		return
	}
	select {
	case a.httpSlots <- struct{}{}:
	default:
		a.mu.Unlock()
		applicationError(w, 429, "RESOURCE_EXHAUSTED", "Request capacity unavailable")
		return
	}
	a.httpUsers.Add(1)
	a.mu.Unlock()
	defer func() { <-a.httpSlots; a.httpUsers.Done() }()
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		if !a.ready() {
			applicationError(w, 503, "UNAVAILABLE", "Service unavailable")
			return
		}
		applicationJSON(w, 200, map[string]string{"status": "ready"})
		return
	}
	if r.URL.Path != "/v1/operations" && !strings.HasPrefix(r.URL.Path, "/v1/operations/") {
		applicationError(w, 404, "NOT_FOUND", "Endpoint not found")
		return
	}
	grants := r.Header.Values(operationGrantHeader)
	if len(grants) != 1 {
		applicationError(w, 403, "PERMISSION_DENIED", "Operation access denied")
		return
	}
	control := r.Method == http.MethodGet || (r.Method == http.MethodPost && (r.URL.Path == "/v1/operations/lookup" || strings.HasSuffix(r.URL.Path, "/cancel")))
	claims, retained, err := a.verifyHTTPGrant(grants[0], control)
	if err != nil || !a.cfg.allows(claims) {
		applicationError(w, 403, "PERMISSION_DENIED", "Operation access denied")
		return
	}
	deadline := time.Now().Add(a.cfg.Limits.Timeout)
	if !retained {
		deadline = minTime(deadline, time.Unix(claims.ExpiresAt, 0))
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	if retained {
		ctx = context.WithValue(ctx, retainedApplicationGrant{}, operations.GrantDigest(grants[0]))
	}
	stop := context.AfterFunc(a.ctx, cancel)
	defer func() { stop(); cancel() }()
	r = r.WithContext(ctx)
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/operations"), "/")
	if len(parts) == 1 && parts[0] == "" && r.Method == http.MethodPost {
		a.submit(w, r, claims, grants[0])
		return
	}
	if len(parts) == 2 && parts[1] == "lookup" && r.Method == http.MethodPost {
		a.lookup(w, r, claims)
		return
	}
	if len(parts) < 2 || len(parts) > 3 || !operations.ValidID(parts[1]) {
		applicationError(w, 404, "NOT_FOUND", "Operation not found")
		return
	}
	record, err := operationRecord(ctx, a.ledger, claims, parts[1])
	if err != nil {
		applicationStorageError(w, err)
		return
	}
	if !applicationControlMatches(ctx, record) {
		applicationError(w, 403, "PERMISSION_DENIED", "Retained operation access denied")
		return
	}
	switch {
	case len(parts) == 2 && r.Method == http.MethodGet:
		applicationJSON(w, 200, operationResponse(record))
	case len(parts) == 3 && parts[2] == "results" && r.Method == http.MethodGet:
		a.result(w, r, claims, record)
	case len(parts) == 3 && parts[2] == "cancel" && r.Method == http.MethodPost:
		op, err := a.beginAudit(ctx, audit.OperationCancel)
		if err != nil {
			applicationStorageError(w, err)
			return
		}
		defer op.abort(ctx)
		updated, err := a.ledger.Cancel(ctx, record.Scope, record.ID)
		if err != nil {
			_ = op.complete(err)
			applicationStorageError(w, err)
			return
		}
		if err = op.complete(nil); err != nil {
			a.failed.Store(true)
			a.BeginDrain()
			applicationStorageError(w, err)
			return
		}
		applicationJSON(w, 200, operationResponse(updated.Record))
	case len(parts) == 3 && parts[2] == "connection-lease" && r.Method == http.MethodPost:
		a.connectionLease(w, r, claims, record)
	default:
		applicationError(w, 404, "NOT_FOUND", "Operation endpoint not found")
	}
}

type retainedApplicationGrant struct{}

// An expired grant proves identity only for the exact retained attempt. It
// cannot admit work, renew a lease, or extend execution authority.
func (a *Application) verifyHTTPGrant(token string, control bool) (operations.GrantClaims, bool, error) {
	now := time.Now()
	claims, err := operations.VerifyGrantClaims(token, a.trust, now)
	if err == nil {
		return claims, false, nil
	}
	if !control || len(token) > operations.MaxGrantBytes {
		return operations.GrantClaims{}, false, operations.ErrInvalid
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return operations.GrantClaims{}, false, operations.ErrInvalid
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(parts[1])
	if err != nil || operations.DecodeStrict(raw, &claims, operations.MaxGrantBytes) != nil || claims.IssuedAt > now.Unix() || claims.ExpiresAt > now.Unix() {
		return operations.GrantClaims{}, false, operations.ErrInvalid
	}
	verified, err := operations.VerifyGrantClaims(token, a.trust, time.Unix(claims.IssuedAt, 0))
	return verified, err == nil, err
}

func applicationControlMatches(ctx context.Context, record operationstore.Record) bool {
	digest, retained := ctx.Value(retainedApplicationGrant{}).(string)
	return !retained || digest == record.AuthoritySHA256
}

func (a *Application) ready() bool {
	a.mu.Lock()
	draining := a.draining || a.closed
	a.mu.Unlock()
	return a.started.Load() && !draining && !a.failed.Load() && a.runtime.Err() == nil && a.audit.Ready() && a.executor.Containment.Healthy()
}

func applicationBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	defer r.Body.Close()
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(r.Header.Values("Content-Type")) != 1 {
		return nil, operations.ErrInvalid
	}
	return io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
}

func (a *Application) submit(w http.ResponseWriter, r *http.Request, claims operations.GrantClaims, grant string) {
	if !a.ready() {
		applicationError(w, 503, "UNAVAILABLE", "Service cannot admit operations")
		return
	}
	raw, err := applicationBody(w, r, operations.MaxRequestBytes)
	if err != nil {
		applicationError(w, 400, "INVALID_ARGUMENT", "Invalid operation request")
		return
	}
	request, err := operations.ParseRequest(raw)
	clear(raw)
	if err != nil {
		applicationError(w, 400, "INVALID_ARGUMENT", "Invalid operation request")
		return
	}
	if _, err = operations.VerifyGrant(grant, a.trust, request, time.Now()); err != nil {
		applicationError(w, 403, "PERMISSION_DENIED", "Operation access denied")
		return
	}
	op, err := a.beginAudit(r.Context(), audit.OperationSubmit)
	if err != nil {
		applicationStorageError(w, err)
		return
	}
	defer op.abort(r.Context())
	scope := operationScope(claims)
	digest := operations.GrantDigest(grant)
	identity := operationInputIdentity(scope, digest)
	ref, err := a.inputs.PutRequest(r.Context(), identity, time.Unix(claims.ExpiresAt, 0), request)
	if err != nil {
		_ = op.complete(err)
		if errors.Is(err, exports.ErrLimit) || errors.Is(err, operationinput.ErrLimit) {
			applicationRejected(w, claims.RequestSHA256, digest)
			return
		}
		applicationStorageError(w, err)
		return
	}
	if !a.ready() || r.Context().Err() != nil {
		applicationStorageError(w, operationstore.ErrUnavailable)
		return
	}
	snapshot, duplicate, err := a.ledger.Submit(r.Context(), operationstore.Submission{Scope: scope, Request: request, RequestRef: ref, AuthoritySHA256: digest, AuthorityToken: grant, AuthorityUntil: time.Unix(claims.ExpiresAt, 0)})
	if err != nil {
		_ = op.complete(err)
		// Unknown acknowledgement retains the sealed input until expiry.
		if errors.Is(err, operationstore.ErrCapacity) {
			a.discardInput(identity, ref)
			applicationRejected(w, claims.RequestSHA256, digest)
			return
		}
		applicationStorageError(w, err)
		return
	}
	if duplicate && snapshot.Record.RequestRef.ID != ref.ID {
		a.discardInput(identity, ref)
	}
	if err = op.complete(nil); err != nil {
		a.failed.Store(true)
		a.BeginDrain()
		applicationStorageError(w, err)
		return
	}
	a.runtime.Wake()
	applicationJSON(w, http.StatusAccepted, operationResponse(snapshot.Record))
}

func (a *Application) discardInput(identity exports.Identity, ref operations.InputRef) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.inputs.Cancel(ctx, identity, ref); err != nil {
		a.failed.Store(true)
		a.BeginDrain()
	}
}

func (a *Application) lookup(w http.ResponseWriter, r *http.Request, claims operations.GrantClaims) {
	raw, err := applicationBody(w, r, 1024)
	var request operations.LookupRequest
	if err != nil || operations.DecodeStrict(raw, &request, 1024) != nil || request.Validate() != nil {
		applicationError(w, 400, "INVALID_ARGUMENT", "Invalid operation lookup")
		return
	}
	if request.RequestSHA256 != claims.RequestSHA256 {
		applicationError(w, 403, "PERMISSION_DENIED", "Operation lookup access denied")
		return
	}
	snapshot, err := a.ledger.Lookup(r.Context(), operationScope(claims), request.IdempotencyKey, request.RequestSHA256)
	if err != nil {
		applicationStorageError(w, err)
		return
	}
	if snapshot.Record.Kind != claims.Operation {
		applicationStorageError(w, operationstore.ErrNotFound)
		return
	}
	if !applicationControlMatches(r.Context(), snapshot.Record) {
		applicationError(w, 403, "PERMISSION_DENIED", "Retained operation access denied")
		return
	}
	applicationJSON(w, 200, operationResponse(snapshot.Record))
}

func (a *Application) connectionLease(w http.ResponseWriter, r *http.Request, claims operations.GrantClaims, record operationstore.Record) {
	raw, err := applicationBody(w, r, resolver.MaxLeaseBytes)
	var wire resolver.Binding
	if err != nil || operations.DecodeStrict(raw, &wire, resolver.MaxLeaseBytes) != nil || wire.Validate() != nil {
		applicationError(w, 400, "INVALID_ARGUMENT", "Invalid operation lease request")
		return
	}
	binding := operationstore.Binding{WorkerID: wire.WorkerID, Owner: wire.Owner, Claim: wire.Claim}
	if binding.WorkerID != "application" || binding.Owner != a.owner || record.Binding != binding || a.ctx.Err() != nil || a.failed.Load() {
		applicationStorageError(w, operationstore.ErrConflict)
		return
	}
	current, err := a.ledger.Current(r.Context(), record.Scope, record.ID, binding)
	if err != nil {
		applicationStorageError(w, err)
		return
	}
	until := minTime(time.Now().Add(5*time.Second), minTime(current.Record.LeaseUntil, minTime(current.Record.ExecuteBefore, time.Unix(claims.ExpiresAt, 0))))
	if until.Unix() <= time.Now().Unix() || r.Context().Err() != nil {
		applicationStorageError(w, operationstore.ErrConflict)
		return
	}
	applicationJSON(w, 200, resolver.LeaseResponse{ValidUntil: until.Unix()})
}

func (a *Application) result(w http.ResponseWriter, r *http.Request, claims operations.GrantClaims, initial operationstore.Record) {
	ctx := r.Context()
	allocation, err := a.executor.ResourcePool.Acquire(ctx, admission.Request{MemoryBytes: 3*a.cfg.MaxResultBytes + (2 << 20)})
	if err != nil {
		applicationStorageError(w, err)
		return
	}
	defer allocation.Release()
	record, err := a.readyResult(ctx, claims, initial.ID)
	if err != nil {
		applicationStorageError(w, err)
		return
	}
	op, err := a.beginAudit(ctx, audit.OperationResults)
	if err != nil {
		applicationStorageError(w, err)
		return
	}
	defer op.abort(ctx)
	ref := record.Receipt.Result
	payload, err := a.results.Load(ctx, operationInputIdentity(record.Scope, record.AuthoritySHA256), operations.InputRef{ID: ref.ID, SHA256: ref.SHA256, Bytes: ref.Bytes, Format: ref.Format})
	if err != nil {
		_ = op.complete(err)
		applicationStorageError(w, err)
		return
	}
	defer clear(payload)
	current, err := a.readyResult(ctx, claims, record.ID)
	if err != nil || !reflect.DeepEqual(current.Receipt, record.Receipt) {
		applicationStorageError(w, operationstore.ErrUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.Header().Set("Kelvo-Result-Completion", "durable-eos-v1")
	deadline, _ := ctx.Deadline()
	stop := httpstream.WatchWriteDeadline(ctx, w, deadline)
	defer stop()
	tail := &arrowEOSTail{w: w}
	for len(payload) > 0 {
		if ctx.Err() != nil {
			panic(http.ErrAbortHandler)
		}
		count := min(len(payload), 64<<10)
		if n, err := tail.Write(payload[:count]); err != nil || n != count {
			panic(http.ErrAbortHandler)
		}
		payload = payload[count:]
	}
	current, err = a.readyResult(ctx, claims, record.ID)
	if err != nil || !reflect.DeepEqual(current.Receipt, record.Receipt) || !tail.validEOS() || op.complete(nil) != nil || ctx.Err() != nil || tail.FlushEOS() != nil {
		panic(http.ErrAbortHandler)
	}
}

func (a *Application) readyResult(ctx context.Context, claims operations.GrantClaims, id string) (operationstore.Record, error) {
	record, err := operationRecord(ctx, a.ledger, claims, id)
	if err != nil {
		return operationstore.Record{}, err
	}
	if !applicationControlMatches(ctx, record) || record.State != string(operations.Completed) || record.Receipt == nil || record.Receipt.Validate() != nil || record.Receipt.Result == nil || record.Receipt.Result.Format != "arrow_ipc" || record.Binding.WorkerID != "application" || !time.Now().Before(record.RetainUntil) {
		return operationstore.Record{}, operationstore.ErrNotFound
	}
	return record, nil
}

func applicationRejected(w http.ResponseWriter, requestDigest, grantDigest string) {
	applicationJSON(w, 429, operations.AdmissionRejection{Version: 1, Admission: "not_admitted", Code: "RESOURCE_EXHAUSTED", RequestSHA256: requestDigest, GrantSHA256: grantDigest})
}
func applicationJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func applicationError(w http.ResponseWriter, status int, code, message string) {
	applicationJSON(w, status, map[string]any{"error": &query.Error{Code: code, Message: message}})
}
func applicationStorageError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, operationstore.ErrNotFound):
		applicationError(w, 404, "NOT_FOUND", "Operation not found")
	case errors.Is(err, operationstore.ErrConflict):
		applicationError(w, 409, "CONFLICT", "Operation identity or custody changed")
	case errors.Is(err, operationstore.ErrCapacity), errors.Is(err, exports.ErrLimit):
		applicationError(w, 429, "RESOURCE_EXHAUSTED", "Operation capacity unavailable")
	default:
		applicationError(w, 503, "UNAVAILABLE", "Operation state is unavailable. Reconcile before retrying.")
	}
}
