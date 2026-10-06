// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type gatewayOperations struct {
	store   *operationstore.Store
	inputs  *operationinput.Store
	custody *exports.Custody
	cursor  int // owned by the tenant reconciliation goroutine
}

func (g *Gateway) reconcileOperations(ctx context.Context, tenant string) error {
	state := g.operations[tenant]
	for range min(16, state.store.Policy().Shards) {
		if err := state.store.RecoverShard(ctx, state.cursor); err != nil {
			return err
		}
		state.cursor = (state.cursor + 1) % state.store.Policy().Shards
	}
	_, err := state.inputs.Cleanup(ctx, 32)
	return err
}

func (g *Gateway) initOperations(config GatewayConfig) error {
	if err := validateGatewayOperations(config); err != nil {
		return err
	}
	if config.Operations == nil {
		return nil
	}
	g.operations = make(map[string]*gatewayOperations)
	for tenant, input := range config.Operations.Inputs {
		st, ok := g.tenants[tenant].store.(*NATSStore)
		if !ok {
			return errOperationConfig
		}
		ctx, stop := context.WithTimeout(g.ctx, 5*time.Second)
		store, err := st.OpenOperations(ctx, false)
		if err != nil {
			stop()
			return err
		}
		custody, err := exports.OpenCustody(ctx, input.Directory, tenant, "gateway")
		stop()
		if err != nil {
			return err
		}
		state := &gatewayOperations{store: store, custody: custody}
		g.operations[tenant] = state
		state.inputs, err = operationinput.Open(operationinput.Config{MaxInputBytes: max(operations.MaxRequestBytes, operations.MaxSealedInputBytes),
			Storage: exports.Config{Directory: custody.DataDirectory(), Tenant: tenant, MaxEntries: input.MaxEntries,
				MaxStoredBytes: input.MaxStoredBytes, MaxTTL: 6 * time.Minute}})
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Gateway) closeOperations() error {
	var result error
	for _, state := range g.operations {
		if state.inputs != nil {
			if err := state.inputs.Close(); err != nil {
				result = errors.Join(result, err)
				continue
			}
		}
		if state.custody != nil {
			result = errors.Join(result, state.custody.Close())
		}
	}
	return result
}

func (g *Gateway) serveOperationHTTP(w http.ResponseWriter, r *http.Request, tenant string, parts []string) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	state := g.operations[tenant]
	if state == nil {
		g.err(w, 404, "NOT_FOUND", "Operations are unavailable")
		return
	}
	if r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("X-Kelvo-Delegation") != "" || r.Header.Get("Content-Encoding") != "" {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid operation request")
		return
	}
	grants := r.Header.Values(operationGrantHeader)
	if len(grants) != 1 || strings.ContainsAny(grants[0], " \t\r\n") {
		g.err(w, 403, "PERMISSION_DENIED", "Operation access denied")
		return
	}
	policy := g.tenants[tenant].store.Policy()
	claims, err := authorizeOperationEnvelope(r.Context(), policy, grants[0])
	if err != nil {
		g.err(w, 403, "PERMISSION_DENIED", "Operation access denied")
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), time.Unix(claims.ExpiresAt, 0))
	defer cancel()
	r = r.WithContext(ctx)
	if len(parts) == 2 && r.Method == http.MethodPost {
		g.submitOperation(w, r, state, policy, claims, grants[0])
		return
	}
	if len(parts) == 3 && parts[2] == "lookup" {
		if r.Method != http.MethodPost {
			g.err(w, 405, "INVALID_ARGUMENT", "Operation lookup requires POST")
			return
		}
		g.lookupOperation(w, r, state, claims)
		return
	}
	if len(parts) < 3 || !operations.ValidID(parts[2]) {
		g.err(w, 404, "NOT_FOUND", "Operation not found")
		return
	}
	record, err := operationRecord(ctx, state.store, claims, parts[2])
	if err != nil {
		g.operationError(w, err)
		return
	}
	switch {
	case len(parts) == 3 && r.Method == http.MethodGet:
		g.json(w, 200, operationResponse(record))
	case len(parts) == 4 && parts[3] == "results" && r.Method == http.MethodGet:
		g.operationResults(w, r, state, claims, record)
	case len(parts) == 4 && parts[3] == "cancel" && r.Method == http.MethodPost:
		op, err := g.audit.beginRequest(ctx, tenant, audit.OperationCancel)
		if err != nil {
			g.operationError(w, err)
			return
		}
		defer op.abort(ctx)
		updated, err := state.store.Cancel(ctx, record.Scope, record.ID)
		if err != nil {
			_ = op.complete(err)
			g.operationError(w, err)
			return
		}
		if !g.auditSuccess(w, r, op) {
			return
		}
		g.json(w, 200, operationResponse(updated.Record))
	case len(parts) == 4 && parts[3] == "connection-lease" && r.Method == http.MethodPost:
		g.operationConnectionLease(w, r, g.tenants[tenant], state, claims, record)
	default:
		g.err(w, 404, "NOT_FOUND", "Operation endpoint not found")
	}
}

func (g *Gateway) submitOperation(w http.ResponseWriter, r *http.Request, state *gatewayOperations, policy Policy, claims operations.GrantClaims, grant string) {
	g.mu.RLock()
	draining := g.draining
	g.mu.RUnlock()
	if draining {
		g.err(w, 503, "UNAVAILABLE", "Service draining")
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid operation content type")
		return
	}
	defer r.Body.Close()
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, operations.MaxRequestBytes))
	if err != nil {
		g.err(w, 413, "INVALID_ARGUMENT", "Operation request is too large")
		return
	}
	request, err := operations.ParseRequest(raw)
	clear(raw)
	if err != nil {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid operation request")
		return
	}
	trust, err := operationTrust(policy, claims.ServicePrincipal)
	if err != nil {
		g.operationError(w, err)
		return
	}
	if _, err = operations.VerifyGrant(grant, trust, request, time.Now()); err != nil {
		g.err(w, 403, "PERMISSION_DENIED", "Operation access denied")
		return
	}
	if err := g.verifyOperationBulk(r.Context(), state, operationScope(claims), request); err != nil {
		g.err(w, 400, "INVALID_ARGUMENT", "Sealed input is unavailable or outside operation scope")
		return
	}
	op, err := g.audit.beginRequest(r.Context(), policy.TenantID, audit.OperationSubmit)
	if err != nil {
		g.operationError(w, err)
		return
	}
	defer op.abort(r.Context())
	scope := operationScope(claims)
	digest := operations.GrantDigest(grant)
	identity := operationInputIdentity(scope, digest)
	ref, err := state.inputs.PutRequest(r.Context(), identity, time.Unix(claims.ExpiresAt, 0), request)
	if err != nil {
		_ = op.complete(err)
		g.operationError(w, err)
		return
	}
	if err = requestAuthorityErr(r.Context()); err != nil {
		_ = op.complete(err)
		g.operationError(w, err)
		return
	}
	snapshot, duplicate, err := state.store.Submit(r.Context(), operationstore.Submission{Scope: scope, Request: request,
		RequestRef: ref, AuthoritySHA256: digest, AuthorityToken: grant, AuthorityUntil: time.Unix(claims.ExpiresAt, 0)})
	if err != nil {
		// A lost storage acknowledgement might already have admitted this ref.
		// Leave it sealed until expiry; never cancel or resubmit an unknown write.
		_ = op.complete(err)
		g.operationError(w, err)
		return
	}
	if duplicate && snapshot.Record.RequestRef.ID != ref.ID {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		_ = state.inputs.Cancel(cleanup, identity, ref)
		stop()
	}
	if !g.auditSuccess(w, r, op) {
		return
	}
	g.json(w, http.StatusAccepted, operationResponse(snapshot.Record))
}

func (g *Gateway) operationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, operationstore.ErrNotFound):
		g.err(w, 404, "NOT_FOUND", "Operation not found")
	case errors.Is(err, operationstore.ErrConflict):
		g.err(w, 409, "CONFLICT", "Operation identity or custody changed")
	case errors.Is(err, operationstore.ErrCapacity), errors.Is(err, exports.ErrLimit):
		g.err(w, 429, "RESOURCE_EXHAUSTED", "Operation capacity unavailable")
	default:
		g.err(w, 503, "UNAVAILABLE", "Operation state is unavailable; reconcile before retrying")
	}
}
