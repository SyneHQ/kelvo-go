// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"io"
	"mime"
	"net/http"
	"time"

	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

func (g *Gateway) operationCleanupLease(w http.ResponseWriter, r *http.Request, tenant gatewayTenant, state *gatewayOperations, claims operations.GrantClaims, record ledger.Record) {
	defer r.Body.Close()
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(r.Header.Values("Content-Type")) != 1 {
		g.err(w, 400, "INVALID_ARGUMENT", "Operation cleanup lease requires one JSON content type")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, resolver.MaxLeaseBytes))
	var wire resolver.CleanupBinding
	if err != nil || operations.DecodeStrict(raw, &wire, resolver.MaxLeaseBytes) != nil || wire.Validate() != nil {
		g.err(w, 400, "INVALID_ARGUMENT", "Operation cleanup lease binding is invalid")
		return
	}
	// A fresh status grant can select the same operation but cannot replace the
	// original grant retained for this accepted physical source.
	if operations.GrantDigest(r.Header.Get(operationGrantHeader)) != record.AuthoritySHA256 {
		g.err(w, 403, "PERMISSION_DENIED", "Operation cleanup requires the original grant")
		return
	}
	lease, err := currentOperationCleanupLease(r.Context(), tenant.store, state.store, record, wire)
	if err != nil {
		g.operationError(w, err)
		return
	}
	lease.ValidUntil = min(lease.ValidUntil, claims.ExpiresAt)
	if lease.ValidateAt(time.Now()) != nil || requestAuthorityErr(r.Context()) != nil {
		g.err(w, 409, "UNAVAILABLE", "Operation cleanup lease is unavailable")
		return
	}
	g.json(w, http.StatusOK, lease)
}

func currentOperationCleanupLease(ctx context.Context, clusterStore Store, store *ledger.Store, initial ledger.Record, wire resolver.CleanupBinding) (resolver.CleanupLeaseResponse, error) {
	var zero resolver.CleanupLeaseResponse
	binding := ledger.Binding{WorkerID: wire.WorkerID, Owner: wire.Owner, Claim: wire.Claim}
	reader, ok := clusterStore.(workerLeaseReader)
	if !ok || store == nil || wire.Validate() != nil || binding != initial.Binding {
		return zero, ledger.ErrConflict
	}
	current, err := store.CurrentCleanup(ctx, initial.Scope, initial.ID, binding, wire.DataTicketSHA256, wire.AcceptanceID)
	if err != nil {
		return zero, err
	}
	workerUntil, err := reader.WorkerLease(ctx, binding.WorkerID, binding.Owner)
	if err != nil {
		return zero, ledger.ErrConflict
	}
	latest, err := store.CurrentCleanup(ctx, initial.Scope, initial.ID, binding, wire.DataTicketSHA256, wire.AcceptanceID)
	if err != nil || latest.Record.AuthoritySHA256 != initial.AuthoritySHA256 || latest.Record.RequestSHA256 != initial.RequestSHA256 || *current.Record.AcceptedCleanup != *latest.Record.AcceptedCleanup {
		return zero, ledger.ErrConflict
	}
	a := latest.Record.AcceptedCleanup
	result := resolver.CleanupLeaseResponse{ValidUntil: minTime(a.CleanupUntil, workerUntil).Unix(), CancellationStartedAt: a.CancellationStartedAt.Unix()}
	if result.ValidateAt(time.Now()) != nil || ctx.Err() != nil {
		return zero, ledger.ErrConflict
	}
	return result, nil
}
