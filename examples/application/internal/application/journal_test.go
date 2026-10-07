// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package application

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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
	digest, err := operations.Digest(r)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		_, err := mutate(context.Background(), c, key, gateway, o, r)
		var uncertain *client.OperationUncertainError
		if !errors.As(err, &uncertain) || uncertain.RequestSHA256 != digest || uncertain.IdempotencyKey != r.IdempotencyKey {
			t.Fatal("mutation recovery lost its original SDK identity")
		}
		wantCode := []string{"UNAVAILABLE", "NOT_FOUND"}[attempt]
		if exerciseErrorCode(err) != wantCode {
			t.Fatal("mutation recovery lost the safe SDK error code")
		}
	}
	if submits.Load() != 1 || lookups.Load() != 1 {
		t.Fatalf("recovery sent submissions=%d lookups=%d", submits.Load(), lookups.Load())
	}
}

func TestMutationRecoveryPreservesDeadlineBeforeSubmission(t *testing.T) {
	var requests atomic.Int32
	gateway := mutationErrorClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	c, key, o, r := mutationErrorFixture(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := mutate(ctx, c, key, gateway, o, r)
	if !errors.Is(err, context.DeadlineExceeded) || exerciseErrorCode(err) != "DEADLINE_EXCEEDED" {
		t.Fatal("mutation lost the caller's deadline identity")
	}
	if requests.Load() != 0 {
		t.Fatal("expired mutation context reached the network")
	}
}

func TestMutationRecoveryPreservesCancelledPollAndNeverResubmits(t *testing.T) {
	c, key, o, r := mutationErrorFixture(t)
	digest, err := operations.Digest(r)
	if err != nil {
		t.Fatal(err)
	}
	const id = "operation-fixture-1"
	var submits, polls atomic.Int32
	polling := make(chan struct{})
	gateway := mutationErrorClient(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/operations":
			submits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(operations.Response{Version: operations.Version, ID: id, RequestSHA256: digest, State: "queued"})
		case "/v1/operations/" + id:
			if polls.Add(1) == 1 {
				close(polling)
				<-request.Context().Done()
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			t.Error("recovery used an unexpected endpoint")
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := mutate(ctx, c, key, gateway, o, r)
		done <- err
	}()
	select {
	case <-polling:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatal("mutation did not reach its status poll")
	}
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled mutation did not return")
	}
	var uncertain *client.OperationUncertainError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &uncertain) || uncertain.OperationID != id || uncertain.RequestSHA256 != digest || exerciseErrorCode(err) != "CANCELLED" {
		t.Fatal("cancelled status wait lost its SDK recovery or cancellation identity")
	}
	_, err = mutate(context.Background(), c, key, gateway, o, r)
	if !errors.As(err, &uncertain) || uncertain.OperationID != id || uncertain.RequestSHA256 != digest {
		t.Fatal("journal recovery lost the retained operation identity")
	}
	if submits.Load() != 1 || polls.Load() != 2 {
		t.Fatal("journal recovery did not use only the existing operation status")
	}
}

func TestMutationRecoveryErrorSuppressesCauseText(t *testing.T) {
	cause := errors.New("remote-private-diagnostic")
	err := &mutationRecoveryError{cause: cause}
	if !errors.Is(err, cause) || err.Error() != "mutation outcome is unresolved; preserve the journal and reconcile without resubmitting" {
		t.Fatal("mutation recovery error leaked diagnostics or discarded its cause")
	}
}

func mutationErrorFixture(t *testing.T) (Config, ed25519.PrivateKey, ExerciseOptions, operations.Request) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	c := Config{Identity: Identity{Issuer: "example", Audience: "operations", ClusterTenant: "demo", ServicePrincipal: "app"}}
	o := ExerciseOptions{Team: "team-a", Subject: "alice", Connection: "saved-a", RunID: "run-1", Journal: filepath.Join(t.TempDir(), "mutation.json")}
	r := operations.Request{Version: operations.Version, Kind: operations.StatementExecute, Connection: operations.ConnectionRef{ID: "saved-a", Database: "example"}, IdempotencyKey: "example:team-a:saved-a:run-1", Spec: operations.Spec{Statement: &operations.StatementSpec{SQL: "UPDATE fixture SET value=1", Transaction: operations.TransactionRequired}}}
	return c, key, o, r
}

func mutationErrorClient(t *testing.T, handler http.Handler) *client.Client {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	transport := server.Client().Transport.(*http.Transport)
	tlsConfig := transport.TLSClientConfig.Clone()
	tlsConfig.MinVersion = tls.VersionTLS13
	gateway, err := client.New(client.Config{URL: server.URL, TLSConfig: tlsConfig})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gateway.Close() })
	return gateway
}
