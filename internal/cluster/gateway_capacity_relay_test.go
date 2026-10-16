// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// A node that refuses a read for capacity replies 409 with its public error.
// The gateway relays that as a retryable 429, not as "Worker unavailable".
func TestGatewayRelaysANodeCapacityRefusalAsRetryable(t *testing.T) {
	for name, tc := range map[string]struct {
		status   int
		body     string
		wantCode int
		retry    string
		errCode  string
	}{
		"capacity refusal":        {409, `{"error":{"code":"RESOURCE_EXHAUSTED","message":"Read worker is busy; retry after 3s"}}`, 429, "3", "RESOURCE_EXHAUSTED"},
		"capacity without a hint": {409, `{"error":{"code":"RESOURCE_EXHAUSTED","message":"Read capacity in use"}}`, 429, "1", "RESOURCE_EXHAUSTED"},
		"unsafe message replaced": {409, `{"error":{"code":"RESOURCE_EXHAUSTED","message":"<script>x</script> retry after 2s"}}`, 429, "2", "RESOURCE_EXHAUSTED"},
		"another query failure":   {409, `{"error":{"code":"QUERY_FAILED","message":"Source read failed"}}`, 503, "", "UNAVAILABLE"},
		"unparseable":             {409, `not json`, 503, "", "UNAVAILABLE"},
		"worker error":            {500, `{"error":{"code":"RESOURCE_EXHAUSTED","message":"retry after 9s"}}`, 503, "", "UNAVAILABLE"},
	} {
		t.Run(name, func(t *testing.T) {
			store := &compressionRelayStore{
				gatewayStore: gatewayStore{policy: Policy{TenantID: "relay", Limits: query.DefaultLimits()}},
				snapshot: Snapshot{Revision: 1, Job: Job{ID: "capacity-refused", TenantID: "relay", State: Assigned,
					Owner: "worker-owner", WorkerID: "worker-one", ExpiresAt: time.Now().Add(time.Minute)}},
			}
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer upstream.Close()
			endpoint, err := url.Parse(upstream.URL)
			if err != nil {
				t.Fatal(err)
			}
			tenant := gatewayTenant{store: store, workers: map[string]workerEndpoint{"worker-one": {url: endpoint, client: upstream.Client()}}}
			out := httptest.NewRecorder()
			(&Gateway{}).results(out, httptest.NewRequest(http.MethodGet, "/v1/queries/capacity-refused/results", nil), tenant, "capacity-refused", nil)
			if out.Code != tc.wantCode || out.Header().Get("Retry-After") != tc.retry || !strings.Contains(out.Body.String(), tc.errCode) {
				t.Fatalf("status=%d retry-after=%q body=%q", out.Code, out.Header().Get("Retry-After"), out.Body.String())
			}
			if strings.Contains(out.Body.String(), "<script>") {
				t.Fatal("a node message that is not plain text reached the client")
			}
		})
	}
}
