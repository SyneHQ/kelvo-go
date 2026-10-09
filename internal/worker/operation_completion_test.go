// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
)

type operationCleanupTestSink struct {
	workerTestSink
	cleaned func(context.Context) error
}

func (s *operationCleanupTestSink) OperationCleaned(ctx context.Context) error { return s.cleaned(ctx) }

func TestOperationCleanupObserverNotCalledBeforeAdmission(t *testing.T) {
	called := false
	sink := &operationCleanupTestSink{cleaned: func(context.Context) error { called = true; return nil }}
	input := operationProcessInput(t, operations.QueryRead, "fixture")
	var e *Executor
	if _, err := e.ExecuteOperation(context.Background(), OperationProcessConfig{}, input, sink); err == nil || called {
		t.Fatal("unadmitted operation claimed physical cleanup")
	}
}

func TestOperationCleanupObserverRequiresBothPhysicalProofs(t *testing.T) {
	for _, state := range []operationCleanupState{{}, {prepared: true}, {prepared: true, process: true}, {prepared: true, scratch: true}, {process: true, scratch: true}, {prepared: true, process: true, scratch: true, pendingIO: true}, {prepared: true, process: true, scratch: true, pendingPrivate: true}} {
		called := false
		sink := &operationCleanupTestSink{cleaned: func(context.Context) error { called = true; return nil }}
		if err := state.notify(sink); err != nil || called {
			t.Fatal("incomplete physical proof emitted completion", state)
		}
	}
}

func TestOperationCompletionBindsCustodyAndContainsNoCredentials(t *testing.T) {
	record, _, _ := operationResolverFixture(t)
	// Physical cleanup can follow expiry. No current lease is used as proof.
	record.AuthorityUntil = time.Now().Add(-time.Minute)
	calls := 0
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		raw, err := io.ReadAll(io.LimitReader(r.Body, 4096))
		var got operationCompletion
		if r.Method != http.MethodPost || r.URL.Path != "/internal/kelvo/complete-operation" || err != nil || operations.DecodeStrict(raw, &got, 4096) != nil ||
			got.Version != 1 || got.OperationID != record.ID || got.RequestSHA256 != record.RequestSHA256 || got.GrantSHA256 != record.AuthoritySHA256 || got.WorkerID != record.Binding.WorkerID || got.Owner != record.Binding.Owner || got.Claim != record.Binding.Claim {
			t.Error("completion lost exact admitted custody")
			w.WriteHeader(400)
			return
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		if len(fields) != 7 || strings.Contains(string(raw), record.AuthorityToken) || strings.Contains(string(raw), connectionFixtureDSN) {
			t.Error("completion disclosed source authority or credentials")
		}
		w.WriteHeader(http.StatusNoContent)
	})
	resolver.url += "/internal/kelvo/resolve"
	e := &Executor{connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
	if err := e.CompleteOperationCleanup(context.Background(), record); err != nil || calls != 1 {
		t.Fatal("valid completion was not acknowledged", err, calls)
	}
	changed := record
	changed.AuthoritySHA256 = strings.Repeat("d", 64)
	if e.CompleteOperationCleanup(context.Background(), changed) == nil || calls != 1 {
		t.Fatal("changed authority completed custody")
	}
}

func TestOperationCompletionRejectsUnverifiedOrAmbiguousAcknowledgements(t *testing.T) {
	for _, mode := range []string{"status", "redirect", "compressed", "expired-peer", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			record, _, _ := operationResolverFixture(t)
			resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
				if mode == "status" {
					w.WriteHeader(200)
					return
				}
				if mode == "redirect" {
					http.Redirect(w, r, "/untrusted", 307)
					return
				}
				if mode == "compressed" {
					w.Header().Set("Content-Encoding", "gzip")
				}
				w.WriteHeader(204)
			})
			resolver.url += "/internal/kelvo/resolve"
			if mode == "expired-peer" {
				resolver.client.Transport.(*connectionTestTransport).state.VerifiedChains[0][0].NotAfter = time.Now().Add(-time.Second)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			e := &Executor{connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
			if e.CompleteOperationCleanup(ctx, record) == nil {
				t.Fatal("unverified cleanup acknowledgement accepted")
			}
		})
	}
}
