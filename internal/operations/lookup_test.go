// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	api "github.com/SYNEHQ/kelvo-go/operations"
)

func TestLookupRecoversLostSubmissionWithoutAnotherWrite(t *testing.T) {
	f := newFixture(t)
	f.backend.loseNextAck = true
	if _, _, err := f.store.Submit(context.Background(), f.input); !errors.Is(err, ErrUnavailable) {
		t.Fatal("submission reply was not lost", err)
	}
	writes := f.backend.updates
	digest, _ := api.Digest(f.input.Request)
	first, err := f.store.Lookup(context.Background(), f.scope, f.input.Request.IdempotencyKey, digest)
	if err != nil || first.Record.ID == "" || first.Record.State != Queued || first.Record.Receipt != nil {
		t.Fatal("lost submission was not recovered", err)
	}
	// Lookup must not even persist deadline transitions; a later explicit
	// status/reconciliation request owns those writes.
	f.now = f.now.Add(2 * time.Minute)
	again, err := f.store.Lookup(context.Background(), f.scope, f.input.Request.IdempotencyKey, digest)
	if err != nil || again.Record.ID != first.Record.ID || again.Record.State != Queued || again.Revision != first.Revision || f.backend.updates != writes {
		t.Fatal("lookup changed or dispatched the retained attempt", err)
	}
}

func TestLookupDoesNotCreateAbsentOrExpiredTombstones(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		f := newFixture(t)
		digest, _ := api.Digest(f.input.Request)
		if _, err := f.store.Lookup(context.Background(), f.scope, f.input.Request.IdempotencyKey, digest); !errors.Is(err, ErrNotFound) {
			t.Fatal("absent identity appeared", err)
		}
		if f.backend.updates != 0 || len(f.backend.entries) != 0 {
			t.Fatal("lookup created a shard or reservation")
		}
	})
	t.Run("expired", func(t *testing.T) {
		f := newFixture(t)
		running := f.running(t)
		terminal, err := f.store.Complete(context.Background(), f.scope, running.Record.ID, f.binding, completed(running))
		if err != nil {
			t.Fatal(err)
		}
		writes := f.backend.updates
		f.now = terminal.Record.RetainUntil.Add(time.Second)
		if _, err := f.store.Lookup(context.Background(), f.scope, f.input.Request.IdempotencyKey, running.Record.RequestSHA256); !errors.Is(err, ErrNotFound) {
			t.Fatal("expired retention was presented as current evidence", err)
		}
		if f.backend.updates != writes {
			t.Fatal("lookup erased expired evidence")
		}
	})
}

func TestLookupRejectsChangedPayloadAndForeignScope(t *testing.T) {
	f := newFixture(t)
	first := f.submit(t)
	writes := f.backend.updates
	if _, err := f.store.Lookup(context.Background(), f.scope, f.input.Request.IdempotencyKey, strings.Repeat("f", 64)); !errors.Is(err, ErrConflict) {
		t.Fatal("changed payload retrieved original attempt", err)
	}
	for _, change := range []func(*Scope){
		func(s *Scope) { s.Issuer = "another-issuer" },
		func(s *Scope) { s.ClusterTenant = "another-cluster-tenant" },
		func(s *Scope) { s.ServicePrincipal = "another-principal" },
		func(s *Scope) { s.AppTeam = "another-team" },
		func(s *Scope) { s.SubjectID = "another-subject" },
		func(s *Scope) { s.ConnectionID = "another-connection" },
	} {
		scope := f.scope
		change(&scope)
		if value, err := f.store.Lookup(context.Background(), scope, f.input.Request.IdempotencyKey, first.Record.RequestSHA256); err == nil || value.Record.ID != "" {
			t.Fatal("foreign authority retrieved retained identity")
		}
	}
	if f.backend.updates != writes {
		t.Fatal("denied lookup changed custody")
	}
}

func TestLookupDoesNotRepairCorruptDocuments(t *testing.T) {
	f := newFixture(t)
	first := f.submit(t)
	writes := f.backend.updates
	for key, entry := range f.backend.entries {
		entry.Value = []byte(`{"version":1,"policy_sha256":"changed","records":[]}`)
		f.backend.entries[key] = entry
	}
	if _, err := f.store.Lookup(context.Background(), f.scope, f.input.Request.IdempotencyKey, first.Record.RequestSHA256); !errors.Is(err, ErrUnavailable) {
		t.Fatal("corrupt retention treated as safe absence", err)
	}
	if f.backend.updates != writes {
		t.Fatal("lookup repaired or replaced retained state")
	}
}
