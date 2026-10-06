// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"io"
	"mime"
	"net/http"
	"time"

	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// This attests custody, not permission to run additional statements. The
// resolver independently binds the signed grant to the exact operation body.
func (g *Gateway) operationConnectionLease(w http.ResponseWriter, r *http.Request, tenant gatewayTenant, state *gatewayOperations, claims operations.GrantClaims, record operationstore.Record) {
	defer r.Body.Close()
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid operation lease request")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	var binding operationstore.Binding
	if err != nil || operations.DecodeStrict(raw, &binding, 4096) != nil || binding.Validate() != nil {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid operation lease request")
		return
	}
	if record.State != operationstore.Running || record.Binding != binding {
		g.err(w, 409, "UNAVAILABLE", "Operation lease is unavailable")
		return
	}
	until, err := currentOperationLease(r.Context(), tenant.store, state.store, record, binding)
	if err != nil {
		g.operationError(w, err)
		return
	}
	until = minTime(until, time.Unix(claims.ExpiresAt, 0))
	if until.Unix() <= time.Now().Unix() || requestAuthorityErr(r.Context()) != nil {
		g.err(w, 409, "UNAVAILABLE", "Operation lease is unavailable")
		return
	}
	g.json(w, 200, map[string]int64{"valid_until": until.Unix()})
}

func currentOperationLease(ctx context.Context, clusterStore Store, ledger *operationstore.Store, initial operationstore.Record, binding operationstore.Binding) (time.Time, error) {
	reader, ok := clusterStore.(workerLeaseReader)
	if !ok || ledger == nil || binding != initial.Binding {
		return time.Time{}, operationstore.ErrConflict
	}
	current, err := ledger.Current(ctx, initial.Scope, initial.ID, binding)
	if err != nil {
		return time.Time{}, err
	}
	workerUntil, err := reader.WorkerLease(ctx, binding.WorkerID, binding.Owner)
	if err != nil {
		return time.Time{}, operationstore.ErrConflict
	}
	now := time.Now()
	until := minTime(now.Add(5*time.Second), minTime(workerUntil, minTime(current.Record.LeaseUntil, current.Record.ExecuteBefore)))
	// Cancellation or a source revocation can race with the worker lookup.
	latest, err := ledger.Current(ctx, initial.Scope, initial.ID, binding)
	if err != nil || latest.Record.RequestSHA256 != initial.RequestSHA256 || latest.Record.AuthoritySHA256 != initial.AuthoritySHA256 || !time.Now().Before(until) {
		return time.Time{}, operationstore.ErrConflict
	}
	return until, ctx.Err()
}
