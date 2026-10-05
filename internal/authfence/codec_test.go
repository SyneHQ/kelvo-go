// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

const codecDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func codecFixtureRecord(t testing.TB) Record {
	t.Helper()
	scope, err := NewScope("fleet", []string{"beta", "alpha"}, []string{"gateway-b", "gateway-a"})
	if err != nil {
		t.Fatal(err)
	}
	document, err := NewDocument(8, codecDigest)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewRecord(scope, document)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestRecordCanonicalBytesAndMembership(t *testing.T) {
	record := codecFixtureRecord(t)
	expected := "version: 1\nscope: \"fleet\"\ntenants:\n  - \"alpha\"\n  - \"beta\"\ngateways:\n  - \"gateway-a\"\n  - \"gateway-b\"\nrevision: 8\ndocument_sha256: \"" + codecDigest + "\"\n"
	raw, err := EncodeRecord(record)
	if err != nil || string(raw) != expected {
		t.Fatalf("canonical record differs: %q, %v", raw, err)
	}
	decoded, err := DecodeRecord(raw)
	if err != nil || !decoded.Equal(record) {
		t.Fatal("canonical record did not round trip", err)
	}
	again, err := EncodeRecord(decoded)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("canonical encoding changed", err)
	}
	expectedMembership := sha256.Sum256([]byte("kelvo-key-authority-members-v1\ntenant:alpha\ntenant:beta\ngateway:gateway-a\ngateway:gateway-b\n"))
	if record.Scope().MembershipSHA256() != fmt.Sprintf("%x", expectedMembership) {
		t.Fatal("membership digest does not bind the specified domains and order")
	}
	swapped, err := NewScope("fleet", []string{"gateway-a", "gateway-b"}, []string{"alpha", "beta"})
	if err != nil || swapped.MembershipSHA256() == record.Scope().MembershipSHA256() {
		t.Fatal("tenant and gateway digest domains overlap", err)
	}
	otherID, err := NewScope("other-fleet", record.Scope().Tenants(), record.Scope().Gateways())
	if err != nil || otherID.MembershipSHA256() != record.Scope().MembershipSHA256() || otherID.Equal(record.Scope()) {
		t.Fatal("scope identity and membership are not separately bound", err)
	}
}

func TestRecordMaximumMembershipAndRevision(t *testing.T) {
	tenants := make([]string, MaxTenants)
	gateways := make([]string, MaxGateways)
	for i := range tenants {
		tenants[i] = fmt.Sprintf("t%031d", i)
	}
	for i := range gateways {
		gateways[i] = fmt.Sprintf("g%031d", i)
	}
	scope, err := NewScope(strings.Repeat("s", 63), tenants, gateways)
	if err != nil {
		t.Fatal(err)
	}
	document, err := NewDocument(^uint64(0), codecDigest)
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewRecord(scope, document)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeRecord(record)
	if err != nil || len(raw) > MaxRecordBytes {
		t.Fatal("maximum valid record exceeds its bound", err)
	}
	decoded, err := DecodeRecord(raw)
	if err != nil || !decoded.Equal(record) {
		t.Fatal("maximum valid record failed to round trip", err)
	}
	if _, err := NewScope("fleet", append(tenants, "overflow"), gateways); err != ErrInvalid {
		t.Fatal("tenant capacity overflow accepted", err)
	}
	if _, err := NewScope("fleet", tenants, append(gateways, "overflow")); err != ErrInvalid {
		t.Fatal("gateway capacity overflow accepted", err)
	}
}

func TestRecordRejectsMalformedAndNoncanonicalBytes(t *testing.T) {
	raw, err := EncodeRecord(codecFixtureRecord(t))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	cases := map[string]string{
		"empty":                 "",
		"oversized":             strings.Repeat(" ", MaxRecordBytes+1),
		"exact limit junk":      strings.Repeat(" ", MaxRecordBytes),
		"malformed yaml":        "scope: [private-untrusted-input",
		"duplicate field":       text + "scope: \"private-untrusted-input\"\n",
		"duplicate replaces":    strings.Replace(text, "gateways:", "tenants:", 1),
		"unknown field":         text + "private-untrusted-input: value\n",
		"missing field":         strings.Replace(text, "scope: \"fleet\"\n", "", 1),
		"second document":       text + "---\n" + text,
		"empty second document": text + "---\n",
		"document start":        "---\n" + text,
		"document end":          text + "...\n",
		"reordered fields":      strings.Replace(text, "version: 1\nscope: \"fleet\"", "scope: \"fleet\"\nversion: 1", 1),
		"unknown version":       strings.Replace(text, "version: 1", "version: 2", 1),
		"quoted version":        strings.Replace(text, "version: 1", "version: \"1\"", 1),
		"hex version":           strings.Replace(text, "version: 1", "version: 0x1", 1),
		"zero revision":         strings.Replace(text, "revision: 8", "revision: 0", 1),
		"negative revision":     strings.Replace(text, "revision: 8", "revision: -1", 1),
		"overflow revision":     strings.Replace(text, "revision: 8", "revision: 18446744073709551616", 1),
		"hex revision":          strings.Replace(text, "revision: 8", "revision: 0x8", 1),
		"padded revision":       strings.Replace(text, "revision: 8", "revision: 08", 1),
		"signed revision":       strings.Replace(text, "revision: 8", "revision: +8", 1),
		"quoted revision":       strings.Replace(text, "revision: 8", "revision: \"8\"", 1),
		"float revision":        strings.Replace(text, "revision: 8", "revision: 8.0", 1),
		"null revision":         strings.Replace(text, "revision: 8", "revision: null", 1),
		"uppercase digest":      strings.Replace(text, codecDigest, strings.ToUpper(codecDigest), 1),
		"short digest":          strings.Replace(text, codecDigest, codecDigest[:63], 1),
		"long digest":           strings.Replace(text, codecDigest, codecDigest+"a", 1),
		"nonhex digest":         strings.Replace(text, codecDigest, "g"+codecDigest[1:], 1),
		"invalid scope":         strings.Replace(text, "\"fleet\"", "\"fleet.*\"", 1),
		"unicode scope":         strings.Replace(text, "\"fleet\"", "\"fléet\"", 1),
		"reserved gateway":      strings.Replace(text, "gateway-a", ControlReplicaID, 1),
		"duplicate gateway":     strings.Replace(text, "gateway-b", "gateway-a", 1),
		"duplicate tenant":      strings.Replace(text, "\"beta\"", "\"alpha\"", 1),
		"unsorted tenants":      strings.Replace(text, "  - \"alpha\"\n  - \"beta\"", "  - \"beta\"\n  - \"alpha\"", 1),
		"unsorted gateways":     strings.Replace(text, "  - \"gateway-a\"\n  - \"gateway-b\"", "  - \"gateway-b\"\n  - \"gateway-a\"", 1),
		"empty tenants":         strings.Replace(text, "tenants:\n  - \"alpha\"\n  - \"beta\"", "tenants: []", 1),
		"flow roster":           strings.Replace(text, "tenants:\n  - \"alpha\"\n  - \"beta\"", "tenants: [\"alpha\", \"beta\"]", 1),
		"roster mapping":        strings.Replace(text, "tenants:\n  - \"alpha\"\n  - \"beta\"", "tenants: {alpha: beta}", 1),
		"nested roster":         strings.Replace(text, "  - \"alpha\"", "  - [\"alpha\"]", 1),
		"anchor":                strings.Replace(text, "scope: \"fleet\"", "scope: &s \"fleet\"", 1),
		"alias":                 strings.Replace(text, "scope: \"fleet\"", "scope: *private-untrusted-input", 1),
		"merge":                 text + "<<: {scope: \"private-untrusted-input\"}\n",
		"explicit tag":          strings.Replace(text, "scope: \"fleet\"", "scope: !!str \"fleet\"", 1),
		"foreign tag":           strings.Replace(text, "scope: \"fleet\"", "scope: !private \"fleet\"", 1),
		"quoted field":          strings.Replace(text, "scope:", "\"scope\":", 1),
		"unquoted string":       strings.Replace(text, "scope: \"fleet\"", "scope: fleet", 1),
		"single quoted string":  strings.Replace(text, "scope: \"fleet\"", "scope: 'fleet'", 1),
		"equivalent escape":     strings.Replace(text, "scope: \"fleet\"", "scope: \"\\x66leet\"", 1),
		"head comment":          "# private-untrusted-input\n" + text,
		"inline comment":        strings.Replace(text, "version: 1", "version: 1 # private-untrusted-input", 1),
		"foot comment":          text + "# private-untrusted-input\n",
		"extra newline":         text + "\n",
		"missing newline":       strings.TrimSuffix(text, "\n"),
		"crlf":                  strings.ReplaceAll(text, "\n", "\r\n"),
		"nul":                   text + "\x00",
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			record, err := DecodeRecord([]byte(input))
			if err != ErrInvalid || record.Valid() {
				t.Fatal("invalid record accepted or nonstatic error returned", err)
			}
		})
	}
}

func TestRecordQuotedYAMLKeywordsAreOrdinaryIDs(t *testing.T) {
	scope, err := NewScope("null", []string{"true", "false", "control"}, []string{"yes", "no"})
	if err != nil {
		t.Fatal(err)
	}
	record, err := NewRecord(scope, codecFixtureRecord(t).Document())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRecord(raw)
	if err != nil || !decoded.Equal(record) {
		t.Fatal("quoted identifiers were interpreted as YAML values", err)
	}
}

func TestRecordConstructorsRejectInvalidValues(t *testing.T) {
	for _, id := range []string{"", "A", "1fleet", "a.b", "a/b", "a_b", "a*", "a>", "a\x00", "a\n", "a b", "é", strings.Repeat("a", 64)} {
		if _, err := NewScope(id, []string{"tenant"}, []string{"gateway"}); err != ErrInvalid {
			t.Fatalf("invalid scope %q accepted: %v", id, err)
		}
	}
	for _, invalid := range [][]string{nil, {}, {"a", "a"}, {"a.b"}, {"a/b"}, {strings.Repeat("a", 33)}} {
		if _, err := NewScope("fleet", invalid, []string{"gateway"}); err != ErrInvalid {
			t.Fatal("invalid tenants accepted", invalid, err)
		}
		if _, err := NewScope("fleet", []string{"tenant"}, invalid); err != ErrInvalid {
			t.Fatal("invalid gateways accepted", invalid, err)
		}
	}
	if _, err := NewScope("fleet", []string{"tenant"}, []string{ControlReplicaID}); err != ErrInvalid {
		t.Fatal("reserved control replica accepted", err)
	}
	for _, digest := range []string{"", codecDigest[:63], codecDigest + "a", strings.ToUpper(codecDigest), "g" + codecDigest[1:]} {
		if _, err := NewDocument(1, digest); err != ErrInvalid {
			t.Fatal("invalid digest accepted", err)
		}
	}
	if _, err := NewDocument(0, codecDigest); err != ErrInvalid {
		t.Fatal("zero revision accepted", err)
	}
	valid := codecFixtureRecord(t)
	if _, err := NewRecord(Scope{}, valid.Document()); err != ErrInvalid {
		t.Fatal("invalid scope record accepted", err)
	}
	if _, err := NewRecord(valid.Scope(), Document{}); err != ErrInvalid {
		t.Fatal("invalid document record accepted", err)
	}
	if raw, err := EncodeRecord(Record{}); err != ErrInvalid || raw != nil {
		t.Fatal("zero record encoded", err)
	}
	if (Scope{}).Valid() || (Scope{}).Equal(Scope{}) || (Scope{}).HasReplica("gateway") || (Scope{}).MembershipSHA256() != "" ||
		(Document{}).Valid() || (Document{}).Equal(Document{}) || (Record{}).Valid() || (Record{}).Equal(Record{}) || (Snapshot{}).Valid() {
		t.Fatal("zero value acquired validity")
	}
}

func TestRecordValuesDetachInputsAndAccessors(t *testing.T) {
	tenants, gateways := []string{"beta", "alpha"}, []string{"gateway-b", "gateway-a"}
	scope, err := NewScope("fleet", tenants, gateways)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(tenants, []string{"beta", "alpha"}) || !slices.Equal(gateways, []string{"gateway-b", "gateway-a"}) {
		t.Fatal("constructor sorted caller slices")
	}
	tenants[0], gateways[0] = "mutated", "mutated"
	if !slices.Equal(scope.Tenants(), []string{"alpha", "beta"}) || !slices.Equal(scope.Gateways(), []string{"gateway-a", "gateway-b"}) {
		t.Fatal("scope aliases constructor input")
	}
	scope.Tenants()[0], scope.Gateways()[0] = "mutated", "mutated"
	if !scope.HasReplica("gateway-a") || scope.HasReplica(ControlReplicaID) || scope.Tenants()[0] != "alpha" {
		t.Fatal("scope aliases an accessor result")
	}
	record, err := NewRecord(scope, codecFixtureRecord(t).Document())
	if err != nil {
		t.Fatal(err)
	}
	scope.tenants[0] = "mutated"
	if record.Scope().Tenants()[0] != "alpha" {
		t.Fatal("record aliases the supplied scope")
	}
	returnedScope := record.Scope()
	returnedScope.tenants[0] = "mutated"
	returnedScope.gateways[0] = "mutated"
	if !record.Equal(codecFixtureRecord(t)) {
		t.Fatal("record aliases its returned scope")
	}
	snapshot := Snapshot{record: record, authoritySequence: 17}
	returnedRecord := snapshot.Record()
	returnedRecord.scope.tenants[0] = "mutated"
	observation := VerifiedObservation{snapshot: snapshot}
	returnedSnapshot := observation.Snapshot()
	returnedSnapshot.record.scope.gateways[0] = "mutated"
	result := MutationResult{outcome: OutcomeAdvanced, snapshot: snapshot}
	returnedResult := result.Snapshot()
	returnedResult.record.scope.tenants[0] = "mutated"
	if !snapshot.Record().Equal(record) || !observation.Snapshot().Record().Equal(record) || !result.Snapshot().Record().Equal(record) || result.Outcome() != OutcomeAdvanced {
		t.Fatal("snapshot/result accessor exposed mutable membership")
	}
	raw, err := EncodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	clear(raw)
	if !decoded.Equal(record) {
		t.Fatal("decoded record aliases raw input")
	}
}

func TestAttemptIdentityAndDeadlineBounds(t *testing.T) {
	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	deadline := start.Add(MaxAttemptDuration)
	attempt, err := NewAttempt(start, deadline)
	if err != nil || !attempt.ValidAt(start) || !attempt.ValidAt(deadline.Add(-time.Nanosecond)) || attempt.ValidAt(deadline) || attempt.ValidAt(start.Add(-time.Nanosecond)) {
		t.Fatal("attempt deadline boundary failed", err)
	}
	if attempt.Started() != start || attempt.Deadline() != deadline || attempt.ID() == [16]byte{} {
		t.Fatal("attempt changed the supplied timing or omitted identity")
	}
	other, err := NewAttempt(start, deadline)
	if err != nil || attempt.ID() == other.ID() {
		t.Fatal("attempt identity reused", err)
	}
	for _, times := range [][2]time.Time{{{}, deadline}, {start, {}}, {start, start}, {deadline, start}, {start, deadline.Add(time.Nanosecond)}} {
		if _, err := NewAttempt(times[0], times[1]); err != ErrInvalid {
			t.Fatal("invalid attempt budget accepted", err)
		}
	}
	if (Attempt{}).ValidAt(start) {
		t.Fatal("zero attempt accepted")
	}
}

func TestVerifiedObservationRequiresEveryAuthorityBinding(t *testing.T) {
	record := codecFixtureRecord(t)
	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	attempt, err := NewAttempt(start, start.Add(MaxAttemptDuration))
	if err != nil {
		t.Fatal(err)
	}
	now := start.Add(time.Second)
	valid := VerifiedObservation{snapshot: Snapshot{record: record, authoritySequence: 17}, witnessSequence: 18, attempt: attempt, verified: true}
	if !valid.ValidFor(record.Scope(), attempt, record.Document(), now) || valid.WitnessSequence() != 18 {
		t.Fatal("valid correlated observation rejected")
	}
	if (VerifiedObservation{}).ValidFor(record.Scope(), attempt, record.Document(), now) ||
		(VerifiedObservation{snapshot: valid.snapshot}).ValidFor(record.Scope(), attempt, record.Document(), now) {
		t.Fatal("ordinary read or zero observation acquired verification")
	}
	for name, mutate := range map[string]func(*VerifiedObservation){
		"unverified":         func(v *VerifiedObservation) { v.verified = false },
		"zero authority":     func(v *VerifiedObservation) { v.snapshot.authoritySequence = 0 },
		"zero witness":       func(v *VerifiedObservation) { v.witnessSequence = 0 },
		"equal witness":      func(v *VerifiedObservation) { v.witnessSequence = 17 },
		"older witness":      func(v *VerifiedObservation) { v.witnessSequence = 16 },
		"wrong identity":     func(v *VerifiedObservation) { v.attempt.id[0] ^= 1 },
		"different start":    func(v *VerifiedObservation) { v.attempt.started = v.attempt.started.Add(time.Nanosecond) },
		"different deadline": func(v *VerifiedObservation) { v.attempt.deadline = v.attempt.deadline.Add(-time.Nanosecond) },
		"invalid record":     func(v *VerifiedObservation) { v.snapshot.record = Record{} },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if candidate.ValidFor(record.Scope(), attempt, record.Document(), now) {
				t.Fatal("mismatched proof accepted")
			}
		})
	}
	otherScope, err := NewScope("other-fleet", record.Scope().Tenants(), record.Scope().Gateways())
	if err != nil {
		t.Fatal(err)
	}
	otherMembership, err := NewScope("fleet", []string{"other-tenant"}, record.Scope().Gateways())
	if err != nil {
		t.Fatal(err)
	}
	otherRevision, err := NewDocument(9, codecDigest)
	if err != nil {
		t.Fatal(err)
	}
	otherDigest, err := NewDocument(8, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []Scope{Scope{}, otherScope, otherMembership} {
		if valid.ValidFor(scope, attempt, record.Document(), now) {
			t.Fatal("observation crossed scope or membership")
		}
	}
	for _, document := range []Document{Document{}, otherRevision, otherDigest} {
		if valid.ValidFor(record.Scope(), attempt, document, now) {
			t.Fatal("observation crossed document revision or digest")
		}
	}
	if valid.ValidFor(record.Scope(), attempt, record.Document(), attempt.Deadline()) ||
		valid.ValidFor(record.Scope(), attempt, record.Document(), start.Add(-time.Nanosecond)) ||
		valid.ValidFor(record.Scope(), Attempt{}, record.Document(), now) {
		t.Fatal("late, future or invalid attempt accepted")
	}
}

func FuzzDecodeRecord(f *testing.F) {
	raw, err := EncodeRecord(codecFixtureRecord(f))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte{})
	f.Add([]byte("version: 1\nscope: &s [*s]\n"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		record, err := DecodeRecord(raw)
		if err != nil {
			if err != ErrInvalid || record.Valid() {
				t.Fatal("invalid decode did not return a static failure")
			}
			return
		}
		canonical, err := EncodeRecord(record)
		if err != nil || !record.Valid() || len(raw) > MaxRecordBytes || !bytes.Equal(raw, canonical) {
			t.Fatal("decoder accepted noncanonical or invalid bytes", err)
		}
	})
}
