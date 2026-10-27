// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestGatewayReleaseMatchesStandaloneSemantics(t *testing.T) {
	p := principalTestPolicy()
	a, _ := authorityForPrincipal(p, "analyst")
	job := func(id string, state string) Snapshot {
		return Snapshot{Job: Job{ID: id, TenantID: "a", Authority: &a, State: state, ExpiresAt: time.Now().Add(time.Minute), Request: query.Request{Mode: "federated", Sources: []string{"sales"}, SQL: "SELECT * FROM sales"}}}
	}
	st := &gatewayStore{policy: p, jobs: map[string]Snapshot{"done": job("done", Succeeded), "failed": job("failed", Failed), "running": job("running", Running)}}
	auth := bareAuthenticator(t)
	if !auth.apply(principalKeySet(t, 1, map[string][]string{"analyst": {rotationOld}, "reports": {rotationNew}}, map[string][]string{}), time.Now()) {
		t.Fatal("auth")
	}
	g := &Gateway{tenants: map[string]gatewayTenant{"a": {store: st}}, auth: auth, permits: make(chan struct{}, 2), resultWaiters: make(chan struct{}, 2), ctx: context.Background()}
	del := func(key, id string) int {
		r := httptest.NewRequest(http.MethodDelete, "/v1/queries/"+id, nil)
		r.Header.Set("Authorization", "Bearer "+key)
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		return w.Code
	}
	for _, tt := range []struct {
		key, id string
		want    int
	}{
		{rotationNew, "done", http.StatusNotFound}, // another principal: no existence leak
		{rotationOld, "running", http.StatusConflict},
		{rotationOld, "missing", http.StatusNotFound},
		{rotationOld, "done", http.StatusNoContent},
		{rotationOld, "failed", http.StatusNoContent},
		{"unknown-key-unknown-key-unknown-key", "done", http.StatusUnauthorized},
	} {
		if got := del(tt.key, tt.id); got != tt.want {
			t.Errorf("DELETE %s with %s = %d, want %d", tt.id, tt.key[:4], got, tt.want)
		}
	}
}
