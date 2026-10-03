// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

const (
	ExportQueued               = "queued"
	ExportAssigned             = "assigned"
	ExportClaimed              = "claimed"
	ExportRunning              = "running"
	ExportStored               = "stored"
	ExportReady                = "ready"
	ExportFailed               = "failed"
	ExportCancelled            = "cancelled"
	ExportPublicationUncertain = "publication_uncertain"
)

var (
	ErrExportDisabled = errors.New("exports are disabled")
	ErrExportNotFound = errors.New("export not found")
	ErrExportConflict = errors.New("export state changed")
	ErrExportCapacity = errors.New("export capacity exhausted")
	ErrNoExport       = errors.New("no queued export")
)

// ExportPolicy is an optional, immutable part of the tenant Policy. A nil
// configuration disables export routes, broker resources and background work.
// All gateways and workers must use identical values. AuthorizationVersion is
// an operator-controlled nonempty version: catalog/source authorization changes
// require a coordinated version cutover, including a fresh tenant policy.
type ExportPolicy struct {
	AuthorizationVersion string         `json:"authorization_version" yaml:"authorization_version"`
	MaxJobs              int            `json:"max_jobs" yaml:"max_jobs"`
	QueueTimeout         time.Duration  `json:"queue_timeout" yaml:"queue_timeout"`
	DefaultTTL           time.Duration  `json:"default_ttl" yaml:"default_ttl"`
	MaxTTL               time.Duration  `json:"max_ttl" yaml:"max_ttl"`
	Limits               exports.Limits `json:"limits" yaml:"limits"`
}

// ExportAuthority is derived only from authenticated principal authority and
// trusted tenant policy. Its identity digest covers a versioned domain, tenant,
// every Principal field and AuthorizationVersion. It contains no key material.
// A matching digest is not proof of live key membership or an unexpired lease.
type ExportAuthority struct {
	Principal            JobAuthority `json:"principal"`
	AuthorizationVersion string       `json:"authorization_version"`
}

// ExportSpec is normalized at durable submission and never mutated. QueryLimits
// are inherited from the tenant query policy; storage row/encoded/decoded limits
// cannot exceed that policy or ExportPolicy.Limits. Compression is opt-in and
// never increases decoded admission. Client input cannot select workload class.
type ExportSpec struct {
	QueryLimits   query.Limits   `json:"query_limits"`
	StorageLimits exports.Limits `json:"storage_limits"`
}

// ExportSubmission is trusted gateway input, not a public request DTO. Submit
// derives Authority and normalized Spec itself and rechecks current request
// authority before durable admission. Zero TTL selects DefaultTTL. The initial
// lease and all renewals must be no later than now+Policy.LeaseDuration, the
// authenticator's current validity deadline and the immutable export expiry.
type ExportSubmission struct {
	Request         query.Request
	TTL             time.Duration
	Compression     string
	SupervisorOwner string
	AuthorityUntil  time.Time
}

// ExportLocator binds a result to one persistent worker root and writer fence.
// StorageID survives worker process restarts; WorkerOwner does not. Paths and
// provider credentials never enter this receipt or any broker message.
type ExportLocator struct {
	StorageID string `json:"storage_id"`
	ExportID  string `json:"export_id"`
	Fence     string `json:"fence"`
}

// ExportReceipt is internal immutable metadata. Manifest is the exact bounded
// value returned by a successful exports.Writer.Commit, including schema and
// ordered part digests/totals. ReceiptSHA256 identifies a canonical versioned
// encoding of Locator and Manifest; it is not a claim about raw YAML bytes.
// Downloads compare the acquired manifest with this receipt before any bytes.
type ExportReceipt struct {
	Version       int              `json:"version"`
	Locator       ExportLocator    `json:"locator"`
	Manifest      exports.Manifest `json:"manifest"`
	ReceiptSHA256 string           `json:"receipt_sha256"`
}

// ExportJob lives in its own bounded retained namespace. Unlike query slots,
// no export slot is reusable before ExpiresAt, including failed/cancelled jobs.
// Queued -> Assigned -> Claimed -> Running -> Stored -> Ready is the success
// path. Cancellation may withdraw Ready. Stored alone never authorizes reads.
// PublicationUncertain is never automatically adopted, replayed or made Ready.
type ExportJob struct {
	Version   int             `json:"version"`
	ID        string          `json:"id"`
	TenantID  string          `json:"tenant_id"`
	Authority ExportAuthority `json:"authority"`
	Request   query.Request   `json:"request"`
	Spec      ExportSpec      `json:"spec"`

	CreatedAt     time.Time `json:"created_at"`
	QueueDeadline time.Time `json:"queue_deadline"`
	ExpiresAt     time.Time `json:"expires_at"`

	// SupervisorOwner is immutable. Its current process keeps only the key's
	// authorization context, never the submit token/body. A lost supervisor is
	// not adopted; expired authority fails queued/active jobs without SQL replay.
	SupervisorOwner string    `json:"supervisor_owner"`
	AuthorityUntil  time.Time `json:"authority_until"`

	State             string    `json:"state"`
	WorkerID          string    `json:"worker_id,omitempty"`
	WorkerOwner       string    `json:"worker_owner,omitempty"`
	Claim             string    `json:"claim,omitempty"`
	HeartbeatAt       time.Time `json:"heartbeat_at,omitempty"`
	StartedAt         time.Time `json:"started_at,omitempty"`
	ExecutionDeadline time.Time `json:"execution_deadline,omitempty"`

	// Local is attached once before executing SQL; Receipt once after a fully
	// successful local Commit. Cancellation/failure cannot replace either one.
	Local   *ExportLocator `json:"local,omitempty"`
	Receipt *ExportReceipt `json:"receipt,omitempty"`
	Stats   query.Stats    `json:"stats"`
	Error   *query.Error   `json:"error,omitempty"`
}

type ExportSnapshot struct {
	Job      ExportJob
	Revision uint64
}

// ExportStore borrows the parent NATSStore's authenticated connection and worker
// identity leases. It has no independent Close or ClaimWorker operation. The
// implementation is opened by OpenExportStore(ctx, base, initialize); its KV,
// stream and consumer are distinct from interactive jobs and strictly bounded.
// Snapshots and Policy values returned by the store are detached copies.
type ExportStore interface {
	Policy() Policy
	SubmitExport(context.Context, ExportSubmission) (ExportSnapshot, error)
	GetExport(context.Context, string) (ExportSnapshot, error)
	CompareAndSwapExport(context.Context, ExportSnapshot, ExportJob) (ExportSnapshot, error)
	EnqueueExport(context.Context, string) error
	NextExport(context.Context) (Delivery, error)
	ReconcileExports(context.Context) error
}

// ExportRunResult is an internal mTLS completion response. Success requires the
// worker to have durably written Stored with exactly this receipt/stats. The
// gateway independently reads that state and checks live authority before its
// Ready CAS; a response alone, including a repeated response, cannot publish.
type ExportRunResult struct {
	Receipt ExportReceipt `json:"receipt"`
	Stats   query.Stats   `json:"stats"`
}

// ExportRunner owns local execution/storage and uses the supplied owned Claimed
// snapshot plus its configured ExportStore to enter Running, reserve before SQL,
// attach Local, execute, Commit and write Stored. It retains export-class custody
// through publication and verified cleanup. It must never publish Ready or adopt
// an already Assigned/Claimed/Running job after owner or supervisor loss.
type ExportRunner interface {
	RunExport(context.Context, ExportSnapshot) (ExportRunResult, error)
}

// ExportSubmitRequest is the only public submission payload. The first slice
// accepts federated queries only. TTLSeconds and Compression may only select a
// configured bounded TTL and allowed codec; there are no authority, path, claim,
// admission-class or resource-budget fields. Unknown fields are rejected.
type ExportSubmitRequest struct {
	Query       query.Request `json:"query"`
	TTLSeconds  int64         `json:"ttl_seconds,omitempty"`
	Compression string        `json:"compression,omitempty"`
}

type ExportAcceptedResponse struct {
	ID        string    `json:"id"`
	State     string    `json:"state"`
	ExpiresAt time.Time `json:"expires_at"`
}

// ExportStatusResponse exposes no SQL, worker/root identifiers, fences or
// authentication material. Authorization is required before constructing it.
type ExportStatusResponse struct {
	ID        string       `json:"id"`
	State     string       `json:"state"`
	CreatedAt time.Time    `json:"created_at"`
	ExpiresAt time.Time    `json:"expires_at"`
	Stats     query.Stats  `json:"stats"`
	Error     *query.Error `json:"error,omitempty"`
}

// ExportManifestResponse contains only authorized public download metadata.
// Each indexed part is a complete Arrow stream, not one concatenated stream.
// The first slice rejects HTTP Range rather than advertising byte resumability.
type ExportManifestResponse struct {
	ID           string           `json:"id"`
	ExpiresAt    time.Time        `json:"expires_at"`
	SchemaSHA256 string           `json:"schema_sha256"`
	Rows         int64            `json:"rows"`
	EncodedBytes int64            `json:"encoded_bytes"`
	DecodedBytes int64            `json:"decoded_bytes"`
	Parts        []ExportPartInfo `json:"parts"`
}

// ExportPartInfo deliberately does not embed storage types whose serialization
// contract is private YAML. Public field names are stable and contain no path.
type ExportPartInfo struct {
	Index        int    `json:"index"`
	Rows         int64  `json:"rows"`
	Batches      int64  `json:"batches"`
	EncodedBytes int64  `json:"encoded_bytes"`
	DecodedBytes int64  `json:"decoded_bytes"`
	SHA256       string `json:"sha256"`
}
