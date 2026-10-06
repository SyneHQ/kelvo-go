// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/ingestion"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/watch"
)

// IngestionSession owns an already resolved destination. The caller constructs
// Scope from verified custody and the exact operation, never from input bytes.
type IngestionSession interface {
	Session
	InstallIngestion(context.Context, ingestion.Scope) error
	IngestionState(context.Context, ingestion.Scope) (ingestion.State, error)
	CommitIngestion(context.Context, ingestion.Scope, ingestion.Batch) (ingestion.Receipt, error)
}

func (r ProcessRequest) ingestionScope() (ingestion.Scope, error) {
	if r.Request.Spec.Ingestion == nil {
		return ingestion.Scope{}, ErrInvalid
	}
	spec := r.Request.Spec.Ingestion.Scope
	scope := ingestion.Scope{TeamID: r.AppTeam, SourceID: spec.SourceID, ConnectionID: r.Request.Connection.ID,
		Database: r.Request.Connection.Database, Schema: r.Request.Connection.Schema, Stream: spec.Stream, Binding: spec.Binding}
	if err := scope.Validate(); err != nil {
		return ingestion.Scope{}, ErrInvalid
	}
	return scope, nil
}

// IngestionScope is safe only after ProcessRequest.Validate succeeds.
func (r ProcessRequest) IngestionScope() (ingestion.Scope, error) { return r.ingestionScope() }

// ValidateOperationInput verifies bounded content before credential resolution.
func ValidateOperationInput(request operations.Request, appTeam string, input []byte) error {
	if request.Validate() != nil {
		return ErrInvalid
	}
	return (ProcessRequest{Request: request, AppTeam: appTeam, Input: input}).validateInput()
}

func (r ProcessRequest) validateInput() error {
	if len(r.AppTeam) > 256 || !utf8.ValidString(r.AppTeam) || strings.ContainsAny(r.AppTeam, "\x00\r\n") {
		return ErrInvalid
	}
	if r.Request.Spec.Ingestion != nil {
		if _, err := r.ingestionScope(); err != nil {
			return err
		}
	}
	if r.Request.Spec.Watch != nil {
		if _, err := r.WatchScope(); err != nil {
			return err
		}
	}
	ref := r.Request.InputReference()
	if ref == nil {
		if len(r.Input) != 0 {
			return ErrInvalid
		}
		if r.Request.Spec.Migration != nil {
			return migrationInput(r.Request, nil)
		}
		return nil
	}
	if ref.Validate() != nil || ref.Bytes > operations.MaxSealedInputBytes || len(r.Input) != int(ref.Bytes) {
		return ErrInvalid
	}
	digest := sha256.Sum256(r.Input)
	if hex.EncodeToString(digest[:]) != ref.SHA256 {
		return ErrInvalid
	}
	if r.Request.Kind == operations.MigrationApply && ref.Format == "migration_plan_v1" {
		return migrationInput(r.Request, r.Input)
	}
	if r.Request.Kind == operations.WatchInstall && ref.Format == "mongo_watch_resume_v1" {
		scope, err := r.WatchScope()
		if err != nil {
			return err
		}
		value, err := watch.DecodeImport(r.Input, scope)
		if err != nil {
			return ErrInvalid
		}
		if r.Source.Revision != "" && (r.Source.Engine != "mongodb" || value.SourceRevision != r.Source.Revision) {
			return ErrInvalid
		}
		return nil
	}
	if r.Request.Kind == operations.WatchAck && ref.Format == "watch_checkpoint_v1" {
		scope, err := r.WatchScope()
		if err != nil {
			return err
		}
		if _, err := watch.ParseCheckpoint(r.Input, scope); err != nil {
			return ErrInvalid
		}
		return nil
	}
	if r.Request.Kind != operations.IngestionCommit || ref.Format != "ingestion_batch_v1" {
		return ErrUnsupported
	}
	batch, err := ingestion.ParseBatch(r.Input)
	if err != nil || batch.ID != r.Request.Spec.Ingestion.BatchID || batch.ExpectedSequence != r.Request.Spec.Ingestion.ExpectedSequence {
		return ErrInvalid
	}
	return nil
}
