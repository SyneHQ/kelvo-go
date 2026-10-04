// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package federation defines the compiled-in native federation adapter contract.
// Adapters are trusted Go code registered at process initialization, never
// selected or loaded from query text. This package has no internal dependencies.
package federation

import (
	"context"
	"errors"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Source contains one operator-selected identity and environment references,
// never resolved credentials. Options are bounded, non-secret provider settings.
// Credential names must use the gateway's permitted environment namespace.
type Source struct {
	ID, Type                                           string
	DSNEnv, URLEnv, UsernameEnv, PasswordEnv, TokenEnv string
	Options                                            map[string]string
}

// Table is an operator-selected relation. Database and Schema are explicit
// namespaces interpreted and validated by the adapter, never SQL expressions.
type Table struct {
	Name, Database, Schema, Table string
}

// Limits apply per independent scan. Exceeding them must fail, never silently
// truncate a relation. MemoryMB is not a total process RSS guarantee.
type Limits struct {
	MaxRows, MaxBytes int64
	Timeout           time.Duration
	MemoryMB, Threads int
}

// ScanPlan names the exact ordered projection and required typed predicates.
// Every filter must be applied exactly or Scan must return ErrUnsupported.
// DuckDB does not reapply pushed filters. The gateway normalizes a row-count
// scan's empty projection to one real source column before calling an adapter.
type ScanPlan struct {
	Columns []string `json:"columns"`
	Filters []Filter `json:"filters"`
}

// Filter is a typed predicate, never SQL. Kinds are comparison, is_null,
// is_not_null, and, or. Comparisons use eq/ne/lt/le/gt/ge and canonical lexical
// values with type int8/int16/int32/int64/uint8/uint16/uint32/uint64/bool/date32.
// Date32 is a signed Int32 count of days since 1970-01-01, including DuckDB's
// infinity encodings (2147483647 and -2147483647). Built-in support is selected
// per executor; advisory capability v1 does not advertise Date32 to adapters.
type Filter struct {
	Kind     string   `json:"kind"`
	Column   string   `json:"column,omitempty"`
	Op       string   `json:"op,omitempty"`
	Type     string   `json:"type,omitempty"`
	Value    string   `json:"value,omitempty"`
	Children []Filter `json:"children,omitempty"`
}

// Sink is synchronous: call Schema once, including for an empty result, then
// Write with batches matching that schema and plan order. A batch is borrowed
// only for Write's duration; retaining it beyond that requires Retain/Release.
// Stop immediately on a sink error. Never call a sink concurrently.
type Sink interface {
	Schema(*arrow.Schema) error
	Write(arrow.RecordBatch) error
}

// ScanStats measures encoded source response body bytes consumed, including
// incomplete or rejected results, excluding discovery and transport headers.
// Leave zero if unavailable. Kelvo counts rows, batches and Arrow bytes itself.
type ScanStats struct {
	SourceWireBytes int64
}

// Driver validates configuration without resolving secrets or doing I/O. Open
// receives one authorized table and must honor ctx. Kelvo opens and closes one
// relation for discovery, then an independent relation per scan; driver calls
// may overlap. TLS, credentials, read-only access, exact decoding, cancellation
// and source resource limits remain the adapter's responsibility.
type Driver interface {
	Validate(Source, Table) error
	Open(context.Context, Source, Table, Limits) (Relation, error)
}

// Relation owns source resources. Schema is the stable complete table schema.
// Scan emits its projection with exact field types and metadata. Kelvo closes
// each relation after discovery or Scan, including on failure. Separate
// relations may run concurrently. Close releases all source resources. Raw
// driver errors are never public client messages.
type Relation interface {
	Schema() *arrow.Schema
	Scan(context.Context, ScanPlan, Sink) (ScanStats, error)
	Close() error
}

// ErrUnsupported rejects a required operation without changing its semantics.
// It may be wrapped; the gateway discards diagnostic error text.
var ErrUnsupported = errors.New("federation operation is unsupported")
