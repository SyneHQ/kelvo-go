// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

// NewSourceHealth binds observations to configured live sources only. Snapshot
// and file reads cannot establish health of an upstream native connection.
func NewSourceHealth(c catalog.Config, cfg telemetry.SourceHealthConfig) (*telemetry.SourceHealth, error) {
	ids := make([]string, 0, min(len(c.Sources), telemetry.MaxHealthSources))
	for _, source := range c.Sources {
		switch source.Type {
		case "accelerated", "csv", "parquet", "duckdb", "sqlite":
			continue
		}
		if len(ids) == telemetry.MaxHealthSources {
			return nil, errors.New("source health supports at most 256 configured sources")
		}
		ids = append(ids, source.ID)
	}
	return telemetry.NewSourceHealth(cfg, ids)
}

// Called only after Node.ServeHTTP verifies the internal gateway mTLS identity.
// The node and registry are tenant-bound; no requested tenant/source can expand
// this response. It is deliberately absent from the public gateway API.
func (n *Node) serveSourceHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodHead {
		return
	}
	_ = json.NewEncoder(w).Encode(n.cfg.RuntimeSourceHealth.Entries())
}
