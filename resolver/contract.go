// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package resolver defines the private, mutually authenticated Kelvo authority
// protocol. It contains no database, runtime, storage-path, or application types.
package resolver

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
)

// Version identifies the existing v1 callback protocol. Request and custody
// bodies retain their original schema; response versions are checked explicitly.
const Version = 1

const (
	QueryPath                = "/internal/kelvo/resolve"
	OperationPath            = "/internal/kelvo/resolve-operation"
	CompletionPath           = "/internal/kelvo/complete-operation"
	FileReadPath             = "/internal/kelvo/operation-file"
	FileCommitPath           = "/internal/kelvo/operation-file-commit"
	FileCommitMediaType      = "application/vnd.kelvo.file-update"
	FileSHA256Header         = "X-Kelvo-File-SHA256"
	FileVerifiedTrailer      = "X-Kelvo-File-Verified"
	SourceValidUntilTrailer  = "X-Kelvo-Source-Valid-Until"
	MaxQueryRequestBytes     = 256 << 10
	MaxOperationRequestBytes = operations.MaxRequestBytes + operations.MaxGrantBytes + 4096
	MaxFileReadRequestBytes  = operations.MaxRequestBytes + operations.MaxGrantBytes + 8192
	MaxResponseBytes         = 1 << 20
	MaxCompletionBytes       = 2048
	MaxLeaseBytes            = 4096
	MaxSecretBytes           = 32 << 10
)

var ErrInvalid = errors.New("invalid private resolver contract")
var workerPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)
var queryIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var sourceIDPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
var leasePattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

// Binding is the v1 custody request body. Owner and Claim identify the exact
// worker incarnation and execution attempt; neither may be inferred from an ID.
type Binding struct {
	WorkerID string `json:"worker_id"`
	Owner    string `json:"owner"`
	Claim    string `json:"claim"`
}

func (b Binding) Validate() error {
	if !workerPattern.MatchString(b.WorkerID) || !leasePattern.MatchString(b.Owner) || !leasePattern.MatchString(b.Claim) {
		return ErrInvalid
	}
	return nil
}

type LeaseResponse struct {
	ValidUntil int64 `json:"valid_until"`
}

// LeaseVerifier reads live custody from the authenticated Kelvo gateway. An
// implementation must check the complete binding and signed authority, reject
// redirects, and return only the gateway's current short lease, never renew it.
// The public HTTP client implements this interface without a package cycle.
type LeaseVerifier interface {
	ValidateConnectionLease(context.Context, string, string, Binding) (time.Time, error)
	ValidateOperationLease(context.Context, string, string, Binding) (time.Time, error)
}

// Source is a credential-reference descriptor, not a runtime capability. Paths,
// adapter executables, local/object snapshots and object ranges are not legal
// wire fields and are rejected by strict decoding before runtime conversion.
type Source struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	DSNEnv      string            `json:"dsn_env,omitempty"`
	URLEnv      string            `json:"url_env,omitempty"`
	UsernameEnv string            `json:"username_env,omitempty"`
	PasswordEnv string            `json:"password_env,omitempty"`
	TokenEnv    string            `json:"token_env,omitempty"`
	Options     map[string]string `json:"options,omitempty"`
	Federation  *Federation       `json:"federation,omitempty"`
}

type Federation struct {
	Tables       []delegation.Table `json:"tables"`
	MaxScanRows  int64              `json:"max_scan_rows,omitempty"`
	MaxScanBytes int64              `json:"max_scan_bytes,omitempty"`
}

type QueryRequest struct {
	Delegation string        `json:"delegation"`
	Query      query.Request `json:"query"`
	JobID      string        `json:"job_id"`
	WorkerID   string        `json:"worker_id"`
	Owner      string        `json:"owner"`
	Claim      string        `json:"claim"`
}

func (r QueryRequest) Binding() Binding { return Binding{r.WorkerID, r.Owner, r.Claim} }
func (r QueryRequest) Validate() error {
	if r.Query.Delegation != "" || !queryIDPattern.MatchString(r.JobID) || r.Binding().Validate() != nil || len(r.Delegation) == 0 || len(r.Delegation) > delegation.MaxTokenBytes || query.ValidateRequest(r.Query) != nil {
		return ErrInvalid
	}
	return nil
}

type QueryResponse struct {
	Version          int               `json:"version"`
	DelegationSHA256 string            `json:"delegation_sha256"`
	ValidUntil       int64             `json:"valid_until"`
	Sources          []Source          `json:"sources"`
	Secrets          map[string]string `json:"secrets"`
}

type OperationRequest struct {
	Grant       string             `json:"grant"`
	Operation   operations.Request `json:"operation"`
	OperationID string             `json:"operation_id"`
	WorkerID    string             `json:"worker_id"`
	Owner       string             `json:"owner"`
	Claim       string             `json:"claim"`
}

func (r OperationRequest) Binding() Binding { return Binding{r.WorkerID, r.Owner, r.Claim} }
func (r OperationRequest) Validate() error {
	if !operations.ValidID(r.OperationID) || r.Binding().Validate() != nil || len(r.Grant) == 0 || len(r.Grant) > operations.MaxGrantBytes || r.Operation.Validate() != nil {
		return ErrInvalid
	}
	return nil
}

type OperationResponse struct {
	Version        int               `json:"version"`
	GrantSHA256    string            `json:"grant_sha256"`
	RequestSHA256  string            `json:"request_sha256"`
	SourceRevision string            `json:"source_revision"`
	ValidUntil     int64             `json:"valid_until"`
	Source         Source            `json:"source"`
	Secrets        map[string]string `json:"secrets"`
}

// CompletionRequest reports observed physical cleanup, including after a grant
// expires. It must match retained custody; a terminal ledger state is not proof.
type CompletionRequest struct {
	Version       int    `json:"version"`
	OperationID   string `json:"operation_id"`
	RequestSHA256 string `json:"request_sha256"`
	GrantSHA256   string `json:"grant_sha256"`
	WorkerID      string `json:"worker_id"`
	Owner         string `json:"owner"`
	Claim         string `json:"claim"`
}

func (r CompletionRequest) Binding() Binding { return Binding{r.WorkerID, r.Owner, r.Claim} }
func (r CompletionRequest) Validate() error {
	if r.Version != Version || !operations.ValidID(r.OperationID) || !operations.ValidDigest(r.RequestSHA256) || !operations.ValidDigest(r.GrantSHA256) || r.Binding().Validate() != nil {
		return ErrInvalid
	}
	return nil
}

type FileReadRequest struct {
	OperationRequest
	SourceRevision string                  `json:"source_revision"`
	Snapshot       filesnapshot.Descriptor `json:"snapshot"`
}

func (r FileReadRequest) Validate() error {
	if r.OperationRequest.Validate() != nil || !operations.ValidDigest(r.SourceRevision) || r.Snapshot.Validate() != nil || r.Operation.Connection.Schema != "" {
		return ErrInvalid
	}
	switch r.Operation.Kind {
	case operations.QueryRead, operations.ConnectionTest, operations.MetadataInspect:
		return nil
	case operations.StatementExecute:
		if r.Snapshot.Format == "sqlite" || r.Snapshot.Format == "duckdb" {
			return nil
		}
	}
	return ErrInvalid
}

// FileCommitRequest is the JSON prefix of a v1 publication: four big-endian
// bytes containing its length, then this JSON, then exactly Replacement.Bytes.
type FileCommitRequest struct {
	OperationRequest
	SourceRevision string                   `json:"source_revision"`
	Snapshot       filesnapshot.Descriptor  `json:"snapshot"`
	Publication    filesnapshot.Publication `json:"publication"`
}

func (r FileCommitRequest) Validate() error {
	if (FileReadRequest{r.OperationRequest, r.SourceRevision, r.Snapshot}).Validate() != nil || r.Operation.Kind != operations.StatementExecute || r.Publication.Validate() != nil || r.Publication.OperationID != r.OperationID || r.Publication.Original != r.Snapshot {
		return ErrInvalid
	}
	digest, err := operations.Digest(r.Operation)
	if err != nil || digest != r.Publication.RequestSHA256 {
		return ErrInvalid
	}
	return nil
}

// SecretReference is the only permitted per-source secret namespace. Supplying
// a value in Secrets never authorizes an ambient environment-variable lookup.
func SecretReference(index int, kind string) (string, error) {
	if index < 0 || index >= 32 {
		return "", ErrInvalid
	}
	switch kind {
	case "DSN", "URL", "USERNAME", "PASSWORD", "TOKEN":
	default:
		return "", ErrInvalid
	}
	return fmt.Sprintf("KELVO_SOURCE_REQUEST_%d_%s", index, kind), nil
}

func validateSources(sources []Source, secrets map[string]string) error {
	if len(sources) < 1 || len(sources) > 32 || len(secrets) > 5*len(sources) {
		return ErrInvalid
	}
	refs, aliases := map[string]bool{}, map[string]bool{}
	for i, s := range sources {
		if !sourceIDPattern.MatchString(s.ID) || aliases[strings.ToLower(s.ID)] || s.Type == "" || len(s.Type) > 64 || len(s.Options) > 16 {
			return ErrInvalid
		}
		aliases[strings.ToLower(s.ID)] = true
		for key, value := range s.Options {
			if key == "" || len(key) > 64 || len(value) > 64<<10 || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
				return ErrInvalid
			}
		}
		for _, ref := range []struct{ name, kind string }{{s.DSNEnv, "DSN"}, {s.URLEnv, "URL"}, {s.UsernameEnv, "USERNAME"}, {s.PasswordEnv, "PASSWORD"}, {s.TokenEnv, "TOKEN"}} {
			if ref.name == "" {
				continue
			}
			want, _ := SecretReference(i, ref.kind)
			v, ok := secrets[ref.name]
			if ref.name != want || refs[ref.name] || !ok || v == "" || len(v) > MaxSecretBytes || !utf8.ValidString(v) || strings.IndexByte(v, 0) >= 0 {
				return ErrInvalid
			}
			refs[ref.name] = true
		}
	}
	if len(refs) != len(secrets) {
		return ErrInvalid
	}
	return nil
}
