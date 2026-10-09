// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/resolver"
)

func TestCleanupLeaseUsesControlAdmissionAndFixedDeadline(t *testing.T) {
	grant := operationFixtureGrant(t, operationFixtureRequest())
	proof := resolver.CleanupBinding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32), DataTicketSHA256: strings.Repeat("c", 64), AcceptanceID: strings.Repeat("d", 32)}
	now := time.Now().Unix()
	want := resolver.CleanupLeaseResponse{ValidUntil: now + 5, CancellationStartedAt: now}
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		var got resolver.CleanupBinding
		if r.URL.Path != "/v1/operations/operation-a/cleanup-lease" || r.Method != http.MethodPost || r.Header.Get("X-Kelvo-Operation-Grant") != grant || json.NewDecoder(r.Body).Decode(&got) != nil || got != proof {
			t.Error("cleanup lease lost binding")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(want)
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	_, release, err := c.operationContext(context.Background(), Authority{OperationGrant: grant})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for i := 0; i < 2; i++ {
		got, err := c.ValidateOperationCleanupLease(context.Background(), "operation-a", grant, proof)
		if err != nil || got != want || len(c.permits) != 1 {
			t.Fatal("cleanup lease blocked or changed deadline", got, err)
		}
	}
}

func TestCleanupLeaseRejectsMalformedOrExtendedAuthority(t *testing.T) {
	grant := operationFixtureGrant(t, operationFixtureRequest())
	for _, name := range []string{"digest", "acceptance", "worker", "id", "duplicate", "unknown", "extended", "expired", "future", "denied"} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				n := time.Now().Unix()
				start, until := n, n+5
				if name == "extended" {
					until++
				}
				if name == "expired" {
					until = n
				}
				if name == "future" {
					start = n + 1
				}
				if name == "denied" {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				body := fmt.Sprintf(`{"valid_until":%d,"cancellation_started_at":%d}`, until, start)
				if name == "duplicate" {
					body = fmt.Sprintf(`{"valid_until":%d,"valid_until":%d,"cancellation_started_at":%d}`, until, until, start)
				}
				if name == "unknown" {
					body = strings.TrimSuffix(body, "}") + `,"new_authority":true}`
				}
				_, _ = w.Write([]byte(body))
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			proof := resolver.CleanupBinding{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Claim: strings.Repeat("b", 32), DataTicketSHA256: strings.Repeat("c", 64), AcceptanceID: strings.Repeat("d", 32)}
			id := "operation-a"
			badInput := true
			switch name {
			case "digest":
				proof.DataTicketSHA256 = "bad"
			case "acceptance":
				proof.AcceptanceID = "bad"
			case "worker":
				proof.WorkerID = "../worker"
			case "id":
				id = "../id"
			default:
				badInput = false
			}
			if _, err := c.ValidateOperationCleanupLease(context.Background(), id, grant, proof); err == nil {
				t.Fatal("invalid cleanup lease accepted")
			}
			if badInput && calls.Load() != 0 {
				t.Fatal("invalid binding reached network")
			}
		})
	}
}
