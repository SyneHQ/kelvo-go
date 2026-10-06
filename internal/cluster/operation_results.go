// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/httpstream"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

const operationPrincipalHeader = "X-Kelvo-Operation-Principal"

func readyOperationResult(ctx context.Context, store *operationstore.Store, claims operations.GrantClaims, id, workerID string) (operationstore.Record, error) {
	if ctx.Err() != nil || !time.Now().Before(time.Unix(claims.ExpiresAt, 0)) {
		return operationstore.Record{}, operationstore.ErrNotFound
	}
	snapshot, err := store.Get(ctx, operationScope(claims), id)
	if err != nil {
		return operationstore.Record{}, err
	}
	r := snapshot.Record
	if r.State != string(operations.Completed) || r.Receipt == nil || r.Receipt.Validate() != nil || r.Receipt.Result == nil || r.Receipt.Result.Format != "arrow_ipc" ||
		r.RequestSHA256 != claims.RequestSHA256 || r.Kind != claims.Operation || (workerID != "" && r.Binding.WorkerID != workerID) || !time.Now().Before(r.RetainUntil) || ctx.Err() != nil {
		return operationstore.Record{}, operationstore.ErrNotFound
	}
	return r, nil
}

func (n *Node) serveOperationResult(w http.ResponseWriter, r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, "/internal/operations/") {
		return false
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if n.operations == nil || len(parts) != 4 || parts[3] != "results" || !operations.ValidID(parts[2]) || r.Method != http.MethodGet || r.URL.RawQuery != "" || r.URL.RawPath != "" {
		http.NotFound(w, r)
		return true
	}
	principals, grants := r.Header.Values(operationPrincipalHeader), r.Header.Values(operationGrantHeader)
	if len(principals) != 1 || len(principals[0]) > 256 || len(grants) != 1 {
		http.Error(w, "Operation access denied", 403)
		return true
	}
	trust, err := operationTrust(n.cfg.Policy, principals[0])
	claims, verifyErr := operations.VerifyGrantClaims(grants[0], trust, time.Now())
	if err != nil || verifyErr != nil || !slices.Contains(n.cfg.Policy.Access.Principals[principals[0]].Operations, claims.Operation) {
		http.Error(w, "Operation access denied", 403)
		return true
	}
	n.mu.Lock()
	if n.ctx.Err() != nil || n.draining || n.operations.failed.Load() {
		n.mu.Unlock()
		http.Error(w, "Worker unavailable", 503)
		return true
	}
	select {
	case n.operations.downloads <- struct{}{}:
	default:
		n.mu.Unlock()
		http.Error(w, "Operation capacity unavailable", 429)
		return true
	}
	n.streams.Add(1)
	n.mu.Unlock()
	defer func() { <-n.operations.downloads; n.streams.Done() }()
	ctx, cancel := context.WithDeadline(r.Context(), minTime(time.Unix(claims.ExpiresAt, 0), time.Now().Add(n.cfg.Policy.Limits.Timeout)))
	stop := context.AfterFunc(n.ctx, cancel)
	defer func() { stop(); cancel() }()
	allocation, err := n.operations.executor.ResourcePool.Acquire(ctx, admission.Request{MemoryBytes: operationDownloadMemory(n.cfg.Operations)})
	if err != nil {
		http.Error(w, "Operation capacity unavailable", 429)
		return true
	}
	defer allocation.Release()
	record, err := readyOperationResult(ctx, n.operations.ledger, claims, parts[2], n.cfg.WorkerID)
	if err != nil {
		http.NotFound(w, r)
		return true
	}
	authority, ok := authorityForPrincipal(n.cfg.Policy, claims.ServicePrincipal)
	if !ok {
		http.Error(w, "Operation access denied", 403)
		return true
	}
	op, err := n.audit.begin(ctx, n.cfg.Policy.TenantID, &authority, audit.OperationResults)
	if err != nil {
		http.Error(w, "Audit unavailable", 503)
		return true
	}
	defer op.abort(ctx)
	ref := record.Receipt.Result
	payload, err := n.operations.results.Load(ctx, operationInputIdentity(record.Scope, record.AuthoritySHA256), operations.InputRef{ID: ref.ID, Bytes: ref.Bytes, SHA256: ref.SHA256, Format: ref.Format})
	if err != nil {
		_ = op.complete(err)
		http.Error(w, "Operation result unavailable", 503)
		return true
	}
	defer clear(payload)
	current, err := readyOperationResult(ctx, n.operations.ledger, claims, record.ID, n.cfg.WorkerID)
	if err != nil || !reflect.DeepEqual(current.Receipt, record.Receipt) {
		http.Error(w, "Operation result unavailable", 503)
		return true
	}
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	deadline, _ := ctx.Deadline()
	stopWrites := httpstream.WatchWriteDeadline(ctx, w, deadline)
	defer stopWrites()
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
	current, err = readyOperationResult(ctx, n.operations.ledger, claims, record.ID, n.cfg.WorkerID)
	if err != nil || !reflect.DeepEqual(current.Receipt, record.Receipt) || !tail.validEOS() || op.complete(nil) != nil || ctx.Err() != nil || tail.FlushEOS() != nil {
		panic(http.ErrAbortHandler)
	}
	return true
}

func (g *Gateway) operationResults(w http.ResponseWriter, r *http.Request, state *gatewayOperations, claims operations.GrantClaims, initial operationstore.Record) {
	op, err := g.audit.beginRequest(r.Context(), claims.ClusterTenant, audit.OperationResults)
	if err != nil {
		g.operationError(w, err)
		return
	}
	defer op.abort(r.Context())
	record, err := readyOperationResult(r.Context(), state.store, claims, initial.ID, "")
	if err != nil {
		g.operationError(w, err)
		return
	}
	endpoint, ok := g.tenants[claims.ClusterTenant].workers[record.Binding.WorkerID]
	if !ok {
		g.operationError(w, operationstore.ErrUnavailable)
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), minTime(record.RetainUntil, time.Now().Add(g.tenants[claims.ClusterTenant].store.Policy().Limits.Timeout)))
	defer cancel()
	u := *endpoint.url
	u.Path = strings.TrimRight(u.Path, "/") + "/internal/operations/" + record.ID + "/results"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		g.operationError(w, err)
		return
	}
	request.Header.Set(operationGrantHeader, r.Header.Get(operationGrantHeader))
	request.Header.Set(operationPrincipalHeader, claims.ServicePrincipal)
	response, err := endpoint.client.Do(request)
	if err != nil {
		g.operationError(w, err)
		return
	}
	defer response.Body.Close()
	ref := record.Receipt.Result
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/vnd.apache.arrow.stream" || response.Header.Get("Content-Encoding") != "" || (response.ContentLength >= 0 && response.ContentLength != ref.Bytes) {
		g.operationError(w, operationstore.ErrUnavailable)
		return
	}
	if requestAuthorityErr(ctx) != nil {
		g.operationError(w, operationstore.ErrUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.Header().Set("Kelvo-Result-Completion", "durable-eos-v1")
	deadline, _ := ctx.Deadline()
	stopWrites := httpstream.WatchWriteDeadline(ctx, w, deadline)
	defer stopWrites()
	tail, digest := &arrowEOSTail{w: w}, sha256.New()
	written, err := io.CopyBuffer(tail, io.TeeReader(io.LimitReader(response.Body, ref.Bytes+1), digest), make([]byte, 32<<10))
	if err != nil || written != ref.Bytes || hex.EncodeToString(digest.Sum(nil)) != ref.SHA256 || !tail.validEOS() || requestAuthorityErr(ctx) != nil {
		panic(http.ErrAbortHandler)
	}
	current, err := readyOperationResult(ctx, state.store, claims, record.ID, "")
	if err != nil || !reflect.DeepEqual(record.Receipt, current.Receipt) || op.complete(nil) != nil || requestAuthorityErr(ctx) != nil || tail.FlushEOS() != nil {
		panic(http.ErrAbortHandler)
	}
}
