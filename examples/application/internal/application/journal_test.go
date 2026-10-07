// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package application

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/client"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestUncertainMutationRecoveryNeverResubmits(t *testing.T) {
	var submits, lookups atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/operations":
			submits.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/v1/operations/lookup":
			lookups.Add(1)
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	transport := server.Client().Transport.(*http.Transport)
	tlsConfig := transport.TLSClientConfig.Clone()
	tlsConfig.MinVersion = tls.VersionTLS13
	gateway, err := client.New(client.Config{URL: server.URL, TLSConfig: tlsConfig})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	c := Config{Identity: Identity{Issuer: "example", Audience: "operations", ClusterTenant: "demo", ServicePrincipal: "app"}}
	o := ExerciseOptions{Team: "team-a", Subject: "alice", Connection: "saved-a", RunID: "run-1", Journal: filepath.Join(t.TempDir(), "mutation.json")}
	r := operations.Request{Version: operations.Version, Kind: operations.StatementExecute, Connection: operations.ConnectionRef{ID: "saved-a", Database: "example"}, IdempotencyKey: "example:team-a:saved-a:run-1", Spec: operations.Spec{Statement: &operations.StatementSpec{SQL: "UPDATE fixture SET value=1", Transaction: operations.TransactionRequired}}}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := mutate(context.Background(), c, key, gateway, o, r); err == nil {
			t.Fatal("uncertain mutation was treated as committed")
		}
	}
	if submits.Load() != 1 || lookups.Load() != 1 {
		t.Fatalf("recovery sent submissions=%d lookups=%d", submits.Load(), lookups.Load())
	}
}
