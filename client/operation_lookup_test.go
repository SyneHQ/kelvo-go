// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestLookupOperationOnlyReadsRetainedLedgerEntry(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	for _, state := range []string{"queued", "running", string(operations.Completed)} {
		t.Run(state, func(t *testing.T) {
			want := operationFixtureStatus(t, request, state)
			var calls atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var lookup operations.LookupRequest
				raw, _ := io.ReadAll(r.Body)
				if r.Method != http.MethodPost || r.URL.Path != "/v1/operations/lookup" || r.Header.Get("X-Kelvo-Operation-Grant") != grant || r.Header.Get("Authorization") != "Bearer "+clientFixtureToken || r.Header.Get("Accept-Encoding") != "identity" || r.TLS.Version != tls.VersionTLS13 || operations.DecodeStrict(raw, &lookup, 1024) != nil || lookup.Version != operations.Version || lookup.IdempotencyKey != request.IdempotencyKey || lookup.RequestSHA256 != want.RequestSHA256 {
					t.Error("lookup changed scope, identity or endpoint")
				}
				writeOperationStatus(w, want, http.StatusOK)
			}, tls.VersionTLS13)
			client := clientFixtureClient(t, clientFixtureConfig(t, server))
			got, err := client.LookupOperation(context.Background(), request.IdempotencyKey, want.RequestSHA256, Authority{OperationGrant: grant})
			if err != nil || !reflect.DeepEqual(got, want) || calls.Load() != 1 {
				t.Fatal("lookup submitted or polled source work", got, err, calls.Load())
			}
		})
	}
}

func TestLookupFailureRetainsUncertaintyAndNeverReplays(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	status := operationFixtureStatus(t, request, "queued")
	for _, code := range []int{http.StatusNotFound, http.StatusConflict, http.StatusForbidden, http.StatusCreated, http.StatusAccepted, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var calls atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/v1/operations/lookup" {
					t.Error("lookup created or altered an operation")
				}
				writeOperationStatus(w, status, code)
			}, tls.VersionTLS13)
			client := clientFixtureClient(t, clientFixtureConfig(t, server))
			got, err := client.LookupOperation(context.Background(), request.IdempotencyKey, status.RequestSHA256, Authority{OperationGrant: grant})
			uncertain := assertOperationUncertain(t, err, "", status.RequestSHA256)
			if uncertain.IdempotencyKey != request.IdempotencyKey || got.ID != "" || calls.Load() != 1 {
				t.Fatal("lookup failure lost its barrier or retried", got, calls.Load())
			}
		})
	}
}

func TestLookupRejectsUnboundAndAmbiguousReceipts(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	status := operationFixtureStatus(t, request, string(operations.Completed))
	raw, _ := json.Marshal(status)
	for name, body := range map[string]string{
		"wrong-digest": strings.ReplaceAll(string(raw), status.RequestSHA256, strings.Repeat("0", 64)),
		"duplicate":    `{"id":"other",` + string(raw[1:]),
		"unknown":      `{"extra":true,` + string(raw[1:]),
		"trailing":     string(raw) + `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := clientFixtureServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}, tls.VersionTLS13)
			client := clientFixtureClient(t, clientFixtureConfig(t, server))
			_, err := client.LookupOperation(context.Background(), request.IdempotencyKey, status.RequestSHA256, Authority{OperationGrant: grant})
			assertOperationUncertain(t, err, "", status.RequestSHA256)
		})
	}
}

func TestLookupRejectsInvalidInputAndRedirects(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	status := operationFixtureStatus(t, request, "queued")
	var calls, redirects atomic.Int32
	destination := clientFixtureServer(t, func(http.ResponseWriter, *http.Request) { redirects.Add(1) }, tls.VersionTLS13)
	server := clientFixtureServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Location", destination.URL+"/v1/operations")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}, tls.VersionTLS13)
	client := clientFixtureClient(t, clientFixtureConfig(t, server))
	for _, invalid := range [][3]string{{"", status.RequestSHA256, grant}, {request.IdempotencyKey, "invalid", grant}, {request.IdempotencyKey, status.RequestSHA256, "invalid"}} {
		if _, err := client.LookupOperation(context.Background(), invalid[0], invalid[1], Authority{OperationGrant: invalid[2]}); err == nil || calls.Load() != 0 {
			t.Fatal("invalid lookup reached transport")
		}
	}
	_, err := client.LookupOperation(context.Background(), request.IdempotencyKey, status.RequestSHA256, Authority{OperationGrant: grant})
	assertOperationUncertain(t, err, "", status.RequestSHA256)
	if redirects.Load() != 0 || calls.Load() != 1 {
		t.Fatal("lookup followed redirect or replayed")
	}
}

func TestLookupShutdownCancelsAndRetainsCorrelation(t *testing.T) {
	request := operationFixtureRequest()
	grant := operationFixtureGrant(t, request)
	status := operationFixtureStatus(t, request, "queued")
	entered := make(chan struct{})
	server := clientFixtureServer(t, func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}, tls.VersionTLS13)
	client := clientFixtureClient(t, clientFixtureConfig(t, server))
	done := make(chan error, 1)
	go func() {
		_, err := client.LookupOperation(context.Background(), request.IdempotencyKey, status.RequestSHA256, Authority{OperationGrant: grant})
		done <- err
	}()
	clientFixtureAwait(t, entered)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	err := clientFixtureAwait(t, done)
	uncertain := assertOperationUncertain(t, err, "", status.RequestSHA256)
	if uncertain.IdempotencyKey != request.IdempotencyKey || !errors.Is(err, context.Canceled) {
		t.Fatal("shutdown lost lookup correlation", err)
	}
}
