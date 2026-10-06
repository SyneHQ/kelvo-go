// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

const operationInputGrantHeader = "X-Kelvo-Operation-Input-Grant"

func operationUploadScope(c operations.InputUploadClaims) operationstore.Scope {
	return operationstore.Scope{Issuer: c.Issuer, ClusterTenant: c.ClusterTenant, ServicePrincipal: c.ServicePrincipal,
		AppTeam: c.AppTeam, SubjectKind: c.Subject.Kind, SubjectID: c.Subject.ID, SubjectJobID: c.Subject.JobID, ConnectionID: c.ConnectionID}
}

// The storage owner includes the entire execution scope. Its authorization
// binds content, length and format rather than a short-lived upload token, so a
// later operation grant can select the returned opaque reference in that scope.
func operationBulkIdentity(scope operationstore.Scope, ref operations.InputRef) exports.Identity {
	raw, _ := json.Marshal(struct {
		Scope  operationstore.Scope `json:"scope"`
		SHA256 string               `json:"sha256"`
		Bytes  int64                `json:"bytes"`
		Format string               `json:"format"`
	}{scope, ref.SHA256, ref.Bytes, ref.Format})
	hash := sha256.Sum256(append([]byte("kelvo.operation.bulk-input.v1\x00"), raw...))
	return operationInputIdentity(scope, hex.EncodeToString(hash[:]))
}

func authorizeOperationUpload(ctx context.Context, policy Policy, token string) (operations.InputUploadClaims, error) {
	if err := requestAuthorityErr(ctx); err != nil {
		return operations.InputUploadClaims{}, err
	}
	authority, ok := jobAuthorityFromContext(ctx)
	current, exists := authorityForPrincipal(policy, authority.PrincipalID)
	if !ok || !exists || authority != current {
		return operations.InputUploadClaims{}, operations.ErrInvalid
	}
	trust, err := operationTrust(policy, authority.PrincipalID)
	if err != nil {
		return operations.InputUploadClaims{}, err
	}
	claims, err := operations.VerifyInputUploadGrant(token, trust, time.Now())
	if err != nil || !slices.Contains(policy.Access.Principals[authority.PrincipalID].Operations, operations.SealedInputOperation(claims.Format)) {
		return operations.InputUploadClaims{}, operations.ErrInvalid
	}
	return claims, nil
}

func (g *Gateway) serveOperationUpload(w http.ResponseWriter, r *http.Request, tenant string) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	state := g.operations[tenant]
	if state == nil || r.Method != http.MethodPost || r.URL.Path != "/v1/operation-inputs" {
		g.err(w, 404, "NOT_FOUND", "Operation inputs are unavailable")
		return
	}
	g.mu.RLock()
	draining := g.draining
	g.mu.RUnlock()
	if draining {
		g.err(w, 503, "UNAVAILABLE", "Service draining")
		return
	}
	grants := r.Header.Values(operationInputGrantHeader)
	if r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" || len(r.Header.Values(operationGrantHeader)) != 0 || r.Header.Get("X-Kelvo-Delegation") != "" || len(grants) != 1 || strings.ContainsAny(grants[0], " \t\r\n") {
		g.err(w, 403, "PERMISSION_DENIED", "Input upload access denied")
		return
	}
	claims, err := authorizeOperationUpload(r.Context(), g.tenants[tenant].store.Policy(), grants[0])
	if err != nil {
		g.err(w, 403, "PERMISSION_DENIED", "Input upload access denied")
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), time.Unix(claims.ExpiresAt, 0))
	defer cancel()
	r = r.WithContext(ctx)
	defer r.Body.Close()
	stopRead := context.AfterFunc(ctx, func() { _ = r.Body.Close() })
	defer stopRead()
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || len(r.Header.Values("Content-Type")) != 1 || media != "application/octet-stream" || (r.ContentLength != -1 && r.ContentLength != claims.Bytes) {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid input content")
		return
	}
	// Storage admission remains bounded across restarts. The HTTP request slot
	// bounds this one-MiB hash-verification buffer before any data is published.
	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, claims.Bytes+1))
	defer clear(payload)
	sum := sha256.Sum256(payload)
	if err != nil || int64(len(payload)) != claims.Bytes || hex.EncodeToString(sum[:]) != claims.SHA256 {
		g.err(w, 400, "INVALID_ARGUMENT", "Input content does not match grant")
		return
	}
	if _, err = authorizeOperationUpload(ctx, g.tenants[tenant].store.Policy(), grants[0]); err != nil {
		g.err(w, 403, "PERMISSION_DENIED", "Input upload access denied")
		return
	}
	op, err := g.audit.beginRequest(ctx, tenant, audit.OperationInputUpload)
	if err != nil {
		g.operationError(w, err)
		return
	}
	defer op.abort(ctx)
	bound := operations.InputRef{SHA256: claims.SHA256, Bytes: claims.Bytes, Format: claims.Format}
	ref, err := state.inputs.Put(ctx, operationBulkIdentity(operationUploadScope(claims), bound), time.Unix(claims.ExpiresAt, 0), operationinput.Format(claims.Format), bytes.NewReader(payload))
	if err != nil {
		_ = op.complete(err)
		g.operationError(w, err)
		return
	}
	if !g.auditSuccess(w, r, op) {
		return
	}
	g.json(w, http.StatusCreated, ref)
}

func (g *Gateway) verifyOperationBulk(ctx context.Context, state *gatewayOperations, scope operationstore.Scope, request operations.Request) error {
	ref := request.InputReference()
	if ref == nil {
		return nil
	}
	if required := operations.SealedInputOperation(ref.Format); required == "" || request.Kind != required || ref.Bytes > operations.MaxSealedInputBytes {
		return operations.ErrInvalid
	}
	payload, err := state.inputs.Load(ctx, operationBulkIdentity(scope, *ref), *ref)
	clear(payload)
	return err
}
