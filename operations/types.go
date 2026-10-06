// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package operations defines the driver-free database operation wire contract.
// Authentication, credentials, driver handles and source URLs are deliberately
// absent. A valid request is not evidence that its caller is authorized.
package operations

import "encoding/json"

const Version = 1
const MaxRequestBytes = 256 << 10
const MaxReceiptBytes = 32 << 10

type Kind string

const (
	ConnectionTest   Kind = "connection.test"
	MetadataInspect  Kind = "metadata.inspect"
	QueryRead        Kind = "query.read"
	StatementExecute Kind = "statement.execute"
	NativeRead       Kind = "native.read"
	NativeExecute    Kind = "native.execute"
	SchemaApply      Kind = "schema.apply"
	MigrationStatus  Kind = "migration.status"
	MigrationApply   Kind = "migration.apply"
	IngestionInstall Kind = "ingestion.install"
	IngestionState   Kind = "ingestion.state"
	IngestionCommit  Kind = "ingestion.commit"
	WatchInstall     Kind = "watch.install"
	WatchRemove      Kind = "watch.remove"
	WatchRead        Kind = "watch.read"
	WatchAck         Kind = "watch.ack"
)

// Request carries exactly one Spec variant, except connection.test (none).
// IdempotencyKey is required for mutations. ApprovalID names an optional
// upstream approval; it is never proof of that approval or write authority.
type Request struct {
	Version        int           `json:"version"`
	Kind           Kind          `json:"kind"`
	Connection     ConnectionRef `json:"connection"`
	IdempotencyKey string        `json:"idempotency_key"`
	ApprovalID     string        `json:"approval_id,omitempty"`
	Spec           Spec          `json:"spec"`
}

// ID is the application's saved connection ID, not a preloaded Kelvo catalog ID.
// Identifiers are data. Drivers must validate/quote them for their own dialect.
type ConnectionRef struct {
	ID       string `json:"id"`
	Database string `json:"database,omitempty"`
	Schema   string `json:"schema,omitempty"`
}

type Spec struct {
	Query     *QuerySpec     `json:"query,omitempty"`
	Statement *StatementSpec `json:"statement,omitempty"`
	Metadata  *MetadataSpec  `json:"metadata,omitempty"`
	Schema    *SchemaSpec    `json:"schema,omitempty"`
	Migration *MigrationSpec `json:"migration,omitempty"`
	Native    *NativeSpec    `json:"native,omitempty"`
	Ingestion *IngestionSpec `json:"ingestion,omitempty"`
	Watch     *WatchSpec     `json:"watch,omitempty"`
}

// Values retain JSON lexical precision. Drivers reject unsupported types rather
// than converting large integers/decimals through float64.
type Parameter struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

type QuerySpec struct {
	SQL        string      `json:"sql"`
	Parameters []Parameter `json:"parameters,omitempty"`
}

type TransactionMode string

const (
	TransactionRequired   TransactionMode = "required"
	TransactionAutocommit TransactionMode = "autocommit"
)

// Statement contains one exact statement or a bounded ordered batch. SQL is
// never split into statements; larger scripts use sealed migration plans.
type StatementSpec struct {
	SQL         string          `json:"sql"`
	Parameters  []Parameter     `json:"parameters,omitempty"`
	Transaction TransactionMode `json:"transaction"`
	Role        string          `json:"role,omitempty"`
	Batch       *BatchSpec      `json:"batch,omitempty"`
	Isolation   string          `json:"isolation,omitempty"`
}

// BoundStatement keeps SQL and exact typed parameters together.
type BoundStatement struct {
	SQL        string      `json:"sql"`
	Parameters []Parameter `json:"parameters,omitempty"`
}

type BatchSpec struct {
	Statements []BoundStatement `json:"statements"`
}

type ObjectRef struct {
	Catalog string `json:"catalog,omitempty"`
	Schema  string `json:"schema,omitempty"`
	Name    string `json:"name,omitempty"`
}

type MetadataSpec struct {
	Object     string    `json:"object"`
	ObjectKind string    `json:"object_kind,omitempty"`
	Target     ObjectRef `json:"target"`
	Cursor     string    `json:"cursor,omitempty"`
	Limit      int       `json:"limit"`
}

// InputRef identifies sealed tenant-owned bytes. It is never a path or URL.
// Bulk data and migration plans travel over a separate bounded input stream,
// not the operation ledger or queue. All four fields are digest-bound.
type InputRef struct {
	ID     string `json:"id"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	Format string `json:"format"`
}

type SchemaSpec struct {
	Target         ObjectRef       `json:"target"`
	ExpectedSHA256 string          `json:"expected_sha256"`
	Plan           InputRef        `json:"plan"`
	Transaction    TransactionMode `json:"transaction"`
}

type MigrationSpec struct {
	ID              string          `json:"id"`
	ExpectedVersion string          `json:"expected_version"`
	Plan            InputRef        `json:"plan"`
	Transaction     TransactionMode `json:"transaction"`
}

// Native commands have an explicit provider vocabulary implemented by the
// adapter. NativeRead must additionally reject every mutating command there;
// the operation kind is not a replacement for provider syntax validation.
type NativeSpec struct {
	Provider   string      `json:"provider"`
	Command    string      `json:"command"`
	Parameters []Parameter `json:"parameters,omitempty"`
	Input      *InputRef   `json:"input,omitempty"`
	// ReturnResult retains the provider response for a mutation. Failure to
	// deliver it never changes a confirmed source effect or permits replay.
	ReturnResult bool `json:"return_result,omitempty"`
}

type IngestionScope struct {
	SourceID string `json:"source_id"`
	Stream   string `json:"stream"`
	Binding  string `json:"binding"`
}

type IngestionSpec struct {
	Scope            IngestionScope `json:"scope"`
	BatchID          string         `json:"batch_id,omitempty"`
	ExpectedSequence int64          `json:"expected_sequence,omitempty"`
	Input            *InputRef      `json:"input,omitempty"`
}

type WatchSpec struct {
	ID                string    `json:"id"`
	Generation        string    `json:"generation"`
	Target            ObjectRef `json:"target"`
	Mode              string    `json:"mode"`
	Checkpoint        *InputRef `json:"checkpoint,omitempty"`
	Resume            *InputRef `json:"resume,omitempty"`
	MaxEvents         int       `json:"max_events,omitempty"`
	MaxWaitMS         int       `json:"max_wait_ms,omitempty"`
	SinkReceiptSHA256 string    `json:"sink_receipt_sha256,omitempty"`
}

type Outcome string
type Effect string

const (
	Completed            Outcome = "completed"
	Rejected             Outcome = "rejected"
	Failed               Outcome = "failed"
	CancelledBeforeStart Outcome = "cancelled_before_start"
	OutcomeUnknown       Outcome = "outcome_unknown"
	EffectNone           Effect  = "none"
	EffectCommitted      Effect  = "committed"
	EffectPartial        Effect  = "partial"
	EffectUnknown        Effect  = "unknown"
)

// Receipt describes source effects separately from transport completion.
// A nil AffectedRows means unavailable. ErrorCode is a fixed public category,
// never a raw driver message. Input/credential data must never appear here.
type Receipt struct {
	Version           int           `json:"version"`
	OperationID       string        `json:"operation_id"`
	RequestSHA256     string        `json:"request_sha256"`
	Outcome           Outcome       `json:"outcome"`
	Effect            Effect        `json:"effect"`
	AffectedRows      *int64        `json:"affected_rows,omitempty"`
	Result            *ResultRef    `json:"result,omitempty"`
	Steps             []StepReceipt `json:"steps,omitempty"`
	ProviderReference string        `json:"provider_reference,omitempty"`
	ErrorCode         string        `json:"error_code,omitempty"`
}

type ResultRef struct {
	ID     string `json:"id"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	Rows   int64  `json:"rows"`
	Format string `json:"format"`
}

// Response is the public submit/status/cancel envelope. Queued work has no
// receipt; a terminal state always carries the same request-bound receipt.
type Response struct {
	Version       int      `json:"version"`
	ID            string   `json:"id"`
	RequestSHA256 string   `json:"request_sha256"`
	State         string   `json:"state"`
	Receipt       *Receipt `json:"receipt,omitempty"`
}

type StepReceipt struct {
	Index  int    `json:"index"`
	SHA256 string `json:"sha256"`
	Effect Effect `json:"effect"`
}

type Capability struct {
	Kind                        Kind              `json:"kind"`
	ParameterTypes              []string          `json:"parameter_types,omitempty"`
	Transactions                []TransactionMode `json:"transactions,omitempty"`
	IsolationLevels             []string          `json:"isolation_levels,omitempty"`
	Roles                       bool              `json:"roles,omitempty"`
	Idempotency                 string            `json:"idempotency"` // none, transactional, provider
	IdempotencyRetentionSeconds int64             `json:"idempotency_retention_seconds,omitempty"`
	Cancellation                string            `json:"cancellation"` // unsupported, best_effort, confirmed
}

// Capability declarations are mandatory but are not conformance evidence.
type Capabilities struct {
	Version    int          `json:"version"`
	Engine     string       `json:"engine"`
	Operations []Capability `json:"operations"`
}
