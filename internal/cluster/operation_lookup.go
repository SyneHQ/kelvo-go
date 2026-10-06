// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"io"
	"mime"
	"net/http"

	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func (g *Gateway) lookupOperation(w http.ResponseWriter, r *http.Request, state *gatewayOperations, claims operations.GrantClaims) {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(r.Header.Values("Content-Type")) != 1 {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid operation lookup content type")
		return
	}
	defer r.Body.Close()
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
	var request operations.LookupRequest
	if err != nil || operations.DecodeStrict(raw, &request, 1024) != nil || request.Validate() != nil {
		g.err(w, 400, "INVALID_ARGUMENT", "Invalid operation lookup")
		return
	}
	if request.RequestSHA256 != claims.RequestSHA256 {
		g.err(w, 403, "PERMISSION_DENIED", "Operation lookup access denied")
		return
	}
	if err := requestAuthorityErr(r.Context()); err != nil {
		g.operationError(w, err)
		return
	}
	value, err := state.store.Lookup(r.Context(), operationScope(claims), request.IdempotencyKey, request.RequestSHA256)
	if err != nil {
		g.operationError(w, err)
		return
	}
	if value.Record.Kind != claims.Operation {
		g.operationError(w, operationstore.ErrNotFound)
		return
	}
	if err := requestAuthorityErr(r.Context()); err != nil {
		g.operationError(w, err)
		return
	}
	g.json(w, 200, operationResponse(value.Record))
}
