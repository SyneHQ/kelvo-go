// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"errors"
	"net/http"
)

// release mirrors the standalone API's DELETE /v1/queries/{id}: only the
// owning principal may release, and only a terminal job. Durable job records
// are immutable once terminal, and the store already reuses terminal slots
// when no other slot is free, so release confirms ownership and state and
// keeps the record for status reads until expiry.
func (g *Gateway) release(w http.ResponseWriter, r *http.Request, t gatewayTenant, id string) {
	s, err := principalSnapshot(r.Context(), t, id)
	if errors.Is(err, ErrNotFound) {
		g.err(w, http.StatusNotFound, "NOT_FOUND", "Query not found")
		return
	}
	if err != nil {
		g.err(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Service unavailable")
		return
	}
	if !s.Job.Terminal() {
		g.err(w, http.StatusConflict, "QUERY_ACTIVE", "Query is not finished; cancel it first")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}
