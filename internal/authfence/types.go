// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package authfence implements a bounded, fixed-scope key-document protocol.
// A read snapshot is observational; verification still requires local durable
// admission before a future authentication consumer can publish keys.
package authfence

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"time"
)

const (
	Version            = 1
	MaxRecordBytes     = 16 * 1024
	MaxTenants         = 256
	MaxGateways        = 16
	MaxWitnessBytes    = 256
	MaxAttemptDuration = 2 * time.Second
	StreamName         = "KELVO_AUTHORITY"
	AuthoritySubject   = "auth.current"
	ControlReplicaID   = "control"
)

var (
	ErrInvalid     = errors.New("key authority input invalid")
	ErrConflict    = errors.New("key authority revision conflict")
	ErrDenied      = errors.New("key authority operation denied")
	ErrNotFound    = errors.New("key authority record not found")
	ErrUnavailable = errors.New("key authority unavailable")
	ErrUnknown     = errors.New("key authority outcome unknown")
	ErrClosed      = errors.New("key authority client closed")
	ErrBusy        = errors.New("key authority operation outstanding")
	ErrExpired     = errors.New("key authority attempt expired")
)

// Scope is a detached, fixed fleet membership. Its zero value is invalid.
type Scope struct {
	id                string
	tenants, gateways []string
}

func NewScope(id string, tenants, gateways []string) (Scope, error) {
	if !identifier(id, 63) {
		return Scope{}, ErrInvalid
	}
	t, ok := roster(tenants, MaxTenants, false)
	if !ok {
		return Scope{}, ErrInvalid
	}
	g, ok := roster(gateways, MaxGateways, true)
	if !ok {
		return Scope{}, ErrInvalid
	}
	return Scope{id: strings.Clone(id), tenants: t, gateways: g}, nil
}

func (s Scope) ID() string         { return s.id }
func (s Scope) Tenants() []string  { return slices.Clone(s.tenants) }
func (s Scope) Gateways() []string { return slices.Clone(s.gateways) }
func (s Scope) HasReplica(id string) bool {
	return s.Valid() && id != ControlReplicaID && slices.Contains(s.gateways, id)
}

func (s Scope) Valid() bool {
	return identifier(s.id, 63) && validRoster(s.tenants, MaxTenants, false) && validRoster(s.gateways, MaxGateways, true)
}

func (s Scope) Equal(other Scope) bool {
	return s.Valid() && other.Valid() && s.id == other.id &&
		slices.Equal(s.tenants, other.tenants) && slices.Equal(s.gateways, other.gateways)
}

// MembershipSHA256 binds sorted tenant and gateway IDs with separate domains.
// Scope identity is bound separately by the policy. Invalid scopes return "".
func (s Scope) MembershipSHA256() string {
	if !s.Valid() {
		return ""
	}
	h := sha256.New()
	h.Write([]byte("kelvo-key-authority-members-v1\n"))
	for _, id := range s.tenants {
		h.Write([]byte("tenant:" + id + "\n"))
	}
	for _, id := range s.gateways {
		h.Write([]byte("gateway:" + id + "\n"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s Scope) detached() Scope {
	return Scope{id: strings.Clone(s.id), tenants: slices.Clone(s.tenants), gateways: slices.Clone(s.gateways)}
}

func identifier(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for i := range len(value) {
		c := value[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

func roster(input []string, maximum int, gateways bool) ([]string, bool) {
	if len(input) == 0 || len(input) > maximum {
		return nil, false
	}
	result := make([]string, len(input))
	for i, id := range input {
		if !identifier(id, 32) || (gateways && id == ControlReplicaID) {
			return nil, false
		}
		result[i] = strings.Clone(id)
	}
	slices.Sort(result)
	return result, validRoster(result, maximum, gateways)
}

func validRoster(ids []string, maximum int, gateways bool) bool {
	if len(ids) == 0 || len(ids) > maximum {
		return false
	}
	for i, id := range ids {
		if !identifier(id, 32) || (gateways && id == ControlReplicaID) || (i > 0 && ids[i-1] >= id) {
			return false
		}
	}
	return true
}

// Document identifies exact private-file bytes without retaining those bytes.
type Document struct {
	revision uint64
	digest   [32]byte
}

func NewDocument(revision uint64, digest string) (Document, error) {
	if revision == 0 || len(digest) != 64 {
		return Document{}, ErrInvalid
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || hex.EncodeToString(decoded) != digest {
		return Document{}, ErrInvalid
	}
	result := Document{revision: revision}
	copy(result.digest[:], decoded)
	return result, nil
}

func (d Document) Revision() uint64 { return d.revision }
func (d Document) SHA256() string   { return hex.EncodeToString(d.digest[:]) }
func (d Document) Valid() bool      { return d.revision != 0 }
func (d Document) Equal(other Document) bool {
	return d.Valid() && other.Valid() && d == other
}

type Record struct {
	scope    Scope
	document Document
}

func NewRecord(scope Scope, document Document) (Record, error) {
	if !scope.Valid() || !document.Valid() {
		return Record{}, ErrInvalid
	}
	return Record{scope: scope.detached(), document: document}, nil
}

func (r Record) Scope() Scope       { return r.scope.detached() }
func (r Record) Document() Document { return r.document }
func (r Record) Valid() bool        { return r.scope.Valid() && r.document.Valid() }
func (r Record) Equal(other Record) bool {
	return r.scope.Equal(other.scope) && r.document.Equal(other.document)
}

// Snapshot is an ordinary read, not an authorization or verified observation.
// Only package protocol operations construct it; the zero value is invalid.
type Snapshot struct {
	record            Record
	authoritySequence uint64
}

func (s Snapshot) Record() Record {
	return Record{scope: s.record.scope.detached(), document: s.record.document}
}
func (s Snapshot) AuthoritySequence() uint64 { return s.authoritySequence }
func (s Snapshot) Valid() bool               { return s.authoritySequence > 0 && s.record.Valid() }

// Attempt is frozen by the caller before private-file work. Its identity and
// original deadline remain unchanged across read, witness and local admission.
type Attempt struct {
	id                [16]byte
	started, deadline time.Time
}

func NewAttempt(started, deadline time.Time) (Attempt, error) {
	result := Attempt{started: started, deadline: deadline}
	if !result.validTimes() {
		return Attempt{}, ErrInvalid
	}
	if _, err := rand.Read(result.id[:]); err != nil || result.id == [16]byte{} {
		return Attempt{}, ErrUnavailable
	}
	return result, nil
}

func (a Attempt) ID() [16]byte        { return a.id }
func (a Attempt) Started() time.Time  { return a.started }
func (a Attempt) Deadline() time.Time { return a.deadline }
func (a Attempt) ValidAt(now time.Time) bool {
	return a.id != [16]byte{} && a.validTimes() && !now.Before(a.started) && now.Before(a.deadline)
}

func (a Attempt) validTimes() bool {
	return !a.started.IsZero() && !a.deadline.IsZero() && a.deadline.After(a.started) &&
		a.deadline.Sub(a.started) <= MaxAttemptDuration
}

func (a Attempt) same(other Attempt) bool {
	return a.id == other.id && a.started == other.started && a.deadline == other.deadline
}

// VerifiedObservation has no public constructor. Only a successful, correlated
// guarded-witness path may construct one. It does not admit local auth state.
type VerifiedObservation struct {
	snapshot        Snapshot
	witnessSequence uint64
	attempt         Attempt
	verified        bool
}

func (v VerifiedObservation) Snapshot() Snapshot {
	return Snapshot{record: v.snapshot.Record(), authoritySequence: v.snapshot.authoritySequence}
}
func (v VerifiedObservation) WitnessSequence() uint64 { return v.witnessSequence }

// ValidFor fences scope, document, attempt identity and the original deadline.
// A future consumer must also require its own successful durable admission.
func (v VerifiedObservation) ValidFor(scope Scope, attempt Attempt, document Document, now time.Time) bool {
	return v.verified && v.snapshot.Valid() && v.snapshot.record.scope.Equal(scope) &&
		v.snapshot.record.document.Equal(document) && v.witnessSequence > v.snapshot.authoritySequence &&
		v.attempt.same(attempt) && attempt.ValidAt(now)
}

type Outcome string

const (
	OutcomeCreated         Outcome = "created"
	OutcomeAdvanced        Outcome = "advanced"
	OutcomeCurrentVerified Outcome = "current_verified"
	OutcomeConflict        Outcome = "conflict"
	OutcomeDenied          Outcome = "denied"
	OutcomeInvalid         Outcome = "invalid"
	OutcomeUnknown         Outcome = "unknown"
)

// MutationResult reports protocol state, never gateway authentication admission.
type MutationResult struct {
	outcome  Outcome
	snapshot Snapshot
}

func (r MutationResult) Outcome() Outcome { return r.outcome }
func (r MutationResult) Snapshot() Snapshot {
	return Snapshot{record: r.snapshot.Record(), authoritySequence: r.snapshot.authoritySequence}
}

// Lifetime retains operation/transport custody even when Close's caller times
// out. Quiesced closes only after all owned work has actually stopped.
type Lifetime interface {
	Close(context.Context) error
	Quiesced() <-chan struct{}
}

type Verifier interface {
	Lifetime
	Read(context.Context, Attempt) (Snapshot, error)
	Verify(context.Context, Attempt, Document) (VerifiedObservation, error)
}

type Controller interface {
	Lifetime
	Initialize(context.Context, Attempt, Document) (MutationResult, error)
	Advance(context.Context, Attempt, uint64, Document) (MutationResult, error)
	Check(context.Context, Attempt, Document) (MutationResult, error)
}
