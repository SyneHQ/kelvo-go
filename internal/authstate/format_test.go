// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authstate

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func testScope() Scope { return Scope{ID: "gateway-one", Tenants: []string{"a", "b"}} }
func testCandidate(revision uint64, names ...string) Candidate {
	result := Candidate{Revision: revision, DocumentSHA256: sha256.Sum256([]byte(fmt.Sprint(revision, names))), Owners: make(map[[32]byte]Owner)}
	for _, name := range names {
		result.Owners[sha256.Sum256([]byte(name))] = Owner{TenantID: "a", PrincipalID: "analyst"}
	}
	return result
}

func TestStateCanonicalRoundTripAndCapacity(t *testing.T) {
	scope, err := copyScope(Scope{ID: "gateway-one", Tenants: []string{"b", "a"}})
	if err != nil {
		t.Fatal(err)
	}
	state := testCandidate(18446744073709551615)
	for i := 0; i < MaxOwners; i++ {
		state.Owners[sha256.Sum256([]byte(fmt.Sprint(i)))] = Owner{TenantID: "a", PrincipalID: strings.Repeat("p", 32)}
	}
	raw, err := encodeState(scope, state)
	if err != nil || len(raw) > stateLimit {
		t.Fatal("maximum ownership history cannot be encoded", err)
	}
	got, err := decodeState(raw, scope)
	if err != nil || !reflect.DeepEqual(got, state) {
		t.Fatal("maximum canonical state did not round trip", err)
	}
	again, err := encodeState(scope, got)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("state is nondeterministic", err)
	}
	full := state
	full.Revision = 1
	if _, changed, err := advance(full, full); err != nil || changed {
		t.Fatal("unchanged history rejected at capacity", err)
	}
	if _, _, err := advance(full, testCandidate(2, "new-fingerprint")); err != ErrRejected {
		t.Fatal("merge evicted history or exceeded capacity", err)
	}
	state.Owners[sha256.Sum256([]byte("overflow"))] = Owner{TenantID: "a"}
	if _, err := copyCandidate(state, scope); err != ErrRejected {
		t.Fatal("capacity overflow accepted", err)
	}
}

func TestStateRejectsMalformedOrForeignDocuments(t *testing.T) {
	raw, err := encodeState(testScope(), testCandidate(8, "one"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	cases := map[string]string{
		"empty": "", "oversized": strings.Repeat(" ", stateLimit+1),
		"duplicate": "version: 1\n" + text, "unknown": text + "extra: 1\n", "multiple": text + "---\n" + text,
		"version":         strings.Replace(text, "version: 1", "version: 2", 1),
		"hex revision":    strings.Replace(text, "revision: 8", "revision: 0x8", 1),
		"fraction":        strings.Replace(text, "revision: 8", "revision: 8.0", 1),
		"quoted revision": strings.Replace(text, "revision: 8", "revision: \"8\"", 1),
		"zero":            strings.Replace(text, "revision: 8", "revision: 0", 1),
		"overflow":        strings.Replace(text, "revision: 8", "revision: 18446744073709551616", 1),
		"scope":           strings.Replace(text, "gateway-one", "gateway-two", 1),
		"tenant":          strings.Replace(text, "tenant_id: \"a\"", "tenant_id: \"c\"", 1),
		"principal":       strings.Replace(text, "analyst", "../analyst", 1),
		"anchor":          strings.Replace(text, "owners:\n", "owners: &owners\n", 1),
		"alias":           strings.Replace(text, "principal_id: \"analyst\"", "principal_id: *owners", 1),
		"comment":         text + "# generated state cannot be edited\n",
		"digest case":     strings.Replace(text, "document_sha256: \"", "document_sha256: \"A", 1),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeState([]byte(input), testScope()); err != ErrUnavailable {
				t.Fatal("invalid state accepted", err)
			}
		})
	}
}

func TestStateOwnershipAndRevisionTransitions(t *testing.T) {
	first := testCandidate(1, "retired", "retained")
	second := testCandidate(2, "retained")
	state, changed, err := advance(first, second)
	if err != nil || !changed || len(state.Owners) != 2 {
		t.Fatal("retired ownership lost", err)
	}
	if _, changed, err = advance(state, second); err != nil || changed {
		t.Fatal("identical document was not a no-op", err)
	}
	for _, invalid := range []Candidate{first, testCandidate(2, "different")} {
		if _, _, err = advance(state, invalid); err != ErrRejected {
			t.Fatal("rollback/equivocation accepted", err)
		}
	}
	unknown := second
	unknown.Owners = map[[32]byte]Owner{sha256.Sum256([]byte("unknown")): {TenantID: "a"}}
	if _, _, err = advance(state, unknown); err != ErrRejected {
		t.Fatal("same digest added owner", err)
	}
	for _, owner := range []Owner{{TenantID: "b", PrincipalID: "analyst"}, {TenantID: "a", PrincipalID: "other"}, {TenantID: "a"}} {
		candidate := testCandidate(3, "retired")
		candidate.Owners[sha256.Sum256([]byte("retired"))] = owner
		if _, _, err = advance(state, candidate); err != ErrRejected {
			t.Fatal("retired key reassigned", err)
		}
	}
	if len(first.Owners) != 2 || first.Revision != 1 {
		t.Fatal("advance mutated prior input")
	}
}

func TestStateDefensiveCopiesAndScopeValidation(t *testing.T) {
	original := testScope()
	scope, err := copyScope(original)
	if err != nil {
		t.Fatal(err)
	}
	original.Tenants[0] = "changed"
	if scope.Tenants[0] != "a" {
		t.Fatal("scope aliases caller")
	}
	input := testCandidate(1, "one")
	copy, err := copyCandidate(input, scope)
	if err != nil {
		t.Fatal(err)
	}
	clear(input.Owners)
	if len(copy.Owners) != 1 {
		t.Fatal("candidate aliases caller")
	}
	for _, invalid := range []Scope{{ID: "A", Tenants: []string{"a"}}, {ID: strings.Repeat("a", 64), Tenants: []string{"a"}}, {ID: "ok", Tenants: []string{"a", "a"}}, {ID: "ok", Tenants: []string{"../a"}}, {ID: "ok"}} {
		if _, err := copyScope(invalid); err != ErrUnavailable {
			t.Fatal("invalid scope accepted", err)
		}
	}
}
