// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package audit records bounded local lifecycle receipts for trusted callers.
// It never authenticates a caller or accepts SQL, error text, or freeform labels.
package audit

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalid     = errors.New("invalid audit configuration or event")
	ErrCorrupt     = errors.New("audit journal is invalid; preserve it for investigation")
	ErrBusy        = errors.New("audit capacity is busy")
	ErrFull        = errors.New("audit retention capacity is full")
	ErrClosed      = errors.New("audit journal is closed")
	ErrUnavailable = errors.New("audit storage is unavailable")
	ErrUncertain   = errors.New("audit durability is uncertain; protected work must fail closed")
	ErrUnsupported = errors.New("audit journal requires Linux")
)

const (
	maximumEntries = 1 << 20
	maximumPending = 4096
	maximumPage    = 256
	headerSize     = 16 << 10
	frameSize      = 1024
	slotSize       = 2 * frameSize
)

// Config is opt-in operator configuration. No zero value means unlimited.
// Directory must be private and absolute. Open persists and verifies the limits.
type Config struct {
	Directory    string        `yaml:"directory" json:"-"`
	MaxEntries   int           `yaml:"max_entries" json:"max_entries"`
	MaxPending   int           `yaml:"max_pending" json:"max_pending"`
	Retention    time.Duration `yaml:"retention" json:"retention_ns"`
	WriteTimeout time.Duration `yaml:"write_timeout" json:"write_timeout_ns"`
}

func (c Config) Validate() error {
	if !filepath.IsAbs(c.Directory) || filepath.Clean(c.Directory) != c.Directory || c.Directory == "/" || len(c.Directory) > 4096 ||
		c.MaxEntries < 1 || c.MaxEntries > maximumEntries || c.MaxPending < 1 || c.MaxPending > maximumPending || c.MaxPending > c.MaxEntries ||
		c.Retention <= 0 || c.Retention > 30*24*time.Hour || c.WriteTimeout <= 0 || c.WriteTimeout > 30*time.Second {
		return ErrInvalid
	}
	return nil
}

// Scope comes from provisioned service configuration, not a request. Reopening
// an existing journal cannot change its service identity or allowed tenant set.
type Scope struct {
	ServiceID   string   `json:"service_id"`
	ServiceKind string   `json:"service_kind"`
	Tenants     []string `json:"tenants"`
}

func normalizedScope(s Scope) (Scope, error) {
	if !identifier(s.ServiceID, 64) || (s.ServiceKind != "gateway" && s.ServiceKind != "worker") || len(s.Tenants) < 1 || len(s.Tenants) > 256 {
		return Scope{}, ErrInvalid
	}
	out := Scope{ServiceID: strings.Clone(s.ServiceID), ServiceKind: strings.Clone(s.ServiceKind), Tenants: make([]string, len(s.Tenants))}
	for i, tenant := range s.Tenants {
		if !identifier(tenant, 32) {
			return Scope{}, ErrInvalid
		}
		out.Tenants[i] = strings.Clone(tenant)
	}
	slices.Sort(out.Tenants)
	for i := 1; i < len(out.Tenants); i++ {
		if out.Tenants[i] == out.Tenants[i-1] {
			return Scope{}, ErrInvalid
		}
	}
	return out, nil
}

// Binding contains trusted, effective authority at the operation boundary.
// Unknown means no principal identity was authenticated; it is never a user ID
// inferred from tenant API keys. An empty PolicyVersion explicitly marks legacy
// authority without a principal-policy digest.
type Binding struct {
	TenantID      string `json:"tenant_id"`
	ServiceID     string `json:"service_id"`
	ServiceKind   string `json:"service_kind"`
	PrincipalID   string `json:"principal_id"`
	PrincipalKind string `json:"principal_kind"`
	PolicyVersion string `json:"policy_version"`
}

type Kind string

const (
	Authentication   Kind = "authentication"
	QuerySubmit      Kind = "query_submit"
	QueryCancel      Kind = "query_cancel"
	QueryResults     Kind = "query_results"
	QueryExecution   Kind = "query_execution"
	RefreshExecution Kind = "refresh_execution"
)

type Outcome string

const (
	Succeeded Outcome = "succeeded"
	Denied    Outcome = "denied"
	Failed    Outcome = "failed"
	Cancelled Outcome = "cancelled"
	Unknown   Outcome = "unknown"
)

type Category string

const (
	None                  Category = "none"
	AuthenticationFailure Category = "authentication"
	AuthorizationFailure  Category = "authorization"
	InvalidRequest        Category = "invalid"
	Capacity              Category = "capacity"
	Cancellation          Category = "cancelled"
	Timeout               Category = "timeout"
	Unavailable           Category = "unavailable"
	Internal              Category = "internal"
	Interrupted           Category = "interrupted"
)

// Event is a local operation observation, not proof of remote/client receipt.
// ID is journal-generated. Zero FinishedAt means the writer disappeared before
// a terminal frame was persisted; readers report Unknown without guessing why.
type Event struct {
	ID         string    `json:"id"`
	Binding    Binding   `json:"binding"`
	Kind       Kind      `json:"kind"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Outcome    Outcome   `json:"outcome"`
	Category   Category  `json:"category"`
}

type Page struct {
	Events []Event `json:"events"`
	Next   int     `json:"next"`
	Done   bool    `json:"done"`
}

func validKind(kind Kind) bool {
	switch kind {
	case Authentication, QuerySubmit, QueryCancel, QueryResults, QueryExecution, RefreshExecution:
		return true
	}
	return false
}

func validOutcome(outcome Outcome, category Category) bool {
	switch outcome {
	case Succeeded:
		return category == None
	case Denied:
		return category == AuthenticationFailure || category == AuthorizationFailure || category == InvalidRequest || category == Capacity || category == Unavailable
	case Failed:
		return category == InvalidRequest || category == Capacity || category == Unavailable || category == Internal || category == AuthorizationFailure
	case Cancelled:
		return category == Cancellation || category == Timeout
	case Unknown:
		return category == Interrupted || category == Unavailable || category == Internal
	default:
		return false
	}
}

func identifier(value string, maximum int) bool {
	if len(value) < 1 || len(value) > maximum || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, c := range []byte(value) {
		if !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '-' {
			return false
		}
	}
	return true
}

func principalID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, c := range []byte(value) {
		if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '-' && c != '_' && c != '.' && c != ':' && c != '@' {
			return false
		}
	}
	return true
}

func hexID(value string, size int) bool {
	if len(value) != size {
		return false
	}
	for _, c := range []byte(value) {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validateBinding(scope Scope, binding Binding, kind Kind) error {
	if !validKind(kind) || binding.ServiceID != scope.ServiceID || binding.ServiceKind != scope.ServiceKind {
		return ErrInvalid
	}
	if binding.TenantID == "" {
		if kind != Authentication || binding.PrincipalKind != "unknown" || binding.PolicyVersion != "" {
			return ErrInvalid
		}
	} else if !slices.Contains(scope.Tenants, binding.TenantID) {
		return ErrInvalid
	}
	if binding.PrincipalKind == "unknown" {
		if binding.PrincipalID != "" {
			return ErrInvalid
		}
	} else if (binding.PrincipalKind != "user" && binding.PrincipalKind != "service") || !principalID(binding.PrincipalID) {
		return ErrInvalid
	}
	if binding.PolicyVersion != "" && !hexID(binding.PolicyVersion, 64) {
		return ErrInvalid
	}
	return nil
}

func cloneBinding(b Binding) Binding {
	return Binding{TenantID: strings.Clone(b.TenantID), ServiceID: strings.Clone(b.ServiceID), ServiceKind: strings.Clone(b.ServiceKind), PrincipalID: strings.Clone(b.PrincipalID), PrincipalKind: strings.Clone(b.PrincipalKind), PolicyVersion: strings.Clone(b.PolicyVersion)}
}

// Receipt owns one preallocated terminal frame. Finish selects one outcome and
// is concurrency-safe. After an enqueued finish times out it is never retried;
// the original I/O still owns its descriptor and slot. Use a fresh bounded
// cleanup context when the execution context has already been cancelled.
type Receipt struct {
	journal   *Journal
	id        string
	slot      int
	mu        sync.Mutex
	operation *operation
}

func (r *Receipt) ID() string {
	if r == nil {
		return ""
	}
	return r.id
}

func (r *Receipt) Finish(ctx context.Context, outcome Outcome, category Category) error {
	if r == nil || r.journal == nil || !validOutcome(outcome, category) {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	if r.operation != nil && (r.operation.outcome != outcome || r.operation.category != category) {
		r.mu.Unlock()
		return ErrInvalid
	}
	if r.operation == nil {
		op, err := r.journal.enqueueFinish(ctx, r, outcome, category)
		if err != nil {
			r.mu.Unlock()
			return err
		}
		r.operation = op
	}
	op := r.operation
	r.mu.Unlock()
	_, err := r.journal.await(ctx, op)
	return err
}
