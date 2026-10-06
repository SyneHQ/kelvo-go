// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/ingestion"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func ingestionRequest(t *testing.T, kind operations.Kind) adapter.ProcessRequest {
	t.Helper()
	r := runtimeRequest(t, operations.ConnectionTest)
	r.Request.Kind = kind
	r.Request.Connection.Schema, r.Source.Schema = "ingested", "ingested"
	r.AppTeam = "team-1"
	r.Request.Spec.Ingestion = &operations.IngestionSpec{Scope: operations.IngestionScope{SourceID: "source-1", Stream: "events", Binding: strings.Repeat("a", 64)}}
	if kind.Mutating() {
		r.Request.IdempotencyKey = "ingest-1"
	}
	if kind == operations.IngestionCommit {
		batch := ingestion.Batch{ID: "batch-1", RunID: "run-1", ObservedAt: "2026-10-06T00:00:00Z", Records: []ingestion.Record{{ID: "row-1", Payload: json.RawMessage(`{"n":9007199254740993}`)}}, Checkpoint: json.RawMessage(`{"page":1}`)}
		r.Input, _ = json.Marshal(batch)
		hash := sha256.Sum256(r.Input)
		r.Request.Spec.Ingestion.BatchID = batch.ID
		r.Request.Spec.Ingestion.Input = &operations.InputRef{ID: strings.Repeat("1", 32), SHA256: hex.EncodeToString(hash[:]), Bytes: int64(len(r.Input)), Format: "ingestion_batch_v1"}
	}
	var err error
	r.RequestSHA256, err = operations.Digest(r.Request)
	if err != nil || r.Validate() != nil {
		t.Fatal("invalid fixture", err)
	}
	return r
}

type ingestionSession struct {
	err                       error
	closed, installs, commits int
	scope                     ingestion.Scope
	receipt                   ingestion.Receipt
}

func (s *ingestionSession) Close() error { s.closed++; return nil }
func (s *ingestionSession) InstallIngestion(_ context.Context, scope ingestion.Scope) error {
	s.installs++
	s.scope = scope
	return s.err
}
func (s *ingestionSession) IngestionState(_ context.Context, scope ingestion.Scope) (ingestion.State, error) {
	s.scope = scope
	return ingestion.State{Checkpoint: json.RawMessage(`{}`)}, s.err
}
func (s *ingestionSession) CommitIngestion(_ context.Context, scope ingestion.Scope, batch ingestion.Batch) (ingestion.Receipt, error) {
	s.commits++
	s.scope = scope
	hash, _ := batch.Digest()
	s.receipt = ingestion.Receipt{BatchID: batch.ID, Digest: hash, Sequence: batch.ExpectedSequence + 1, Records: len(batch.Records), CommittedAt: time.Unix(1800000000, 123000).UTC()}
	return s.receipt, s.err
}

func runIngestion(t *testing.T, request adapter.ProcessRequest, session *ingestionSession, output io.Writer) (operations.Receipt, error) {
	t.Helper()
	raw, _ := json.Marshal(request)
	var receipt bytes.Buffer
	runner := Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
		return session, nil
	}}
	err := runner.Run(context.Background(), bytes.NewReader(raw), output, &receipt)
	var result operations.Receipt
	if operations.DecodeStrict(receipt.Bytes(), &result, operations.MaxReceiptBytes) != nil || result.Validate() != nil {
		t.Fatal("invalid receipt", err)
	}
	if session.closed != 1 {
		t.Fatal("session not disposed")
	}
	return result, err
}

func TestIngestionCommitReturnsExactReceiptInArrow(t *testing.T) {
	request := ingestionRequest(t, operations.IngestionCommit)
	session := &ingestionSession{}
	var output bytes.Buffer
	receipt, err := runIngestion(t, request, session, &output)
	if err != nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectCommitted || receipt.Result == nil || session.commits != 1 || session.scope.TeamID != request.AppTeam {
		t.Fatal("commit failed", err)
	}
	hash := sha256.Sum256(output.Bytes())
	if receipt.Result.SHA256 != hex.EncodeToString(hash[:]) || receipt.Result.Bytes != int64(output.Len()) {
		t.Fatal("result hash mismatch")
	}
	reader, err := ipc.NewReader(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() || reader.RecordBatch().NumRows() != 1 {
		t.Fatal("missing receipt row")
	}
	got, err := ingestion.ParseReceipt(reader.RecordBatch().Column(0).(*array.Binary).Value(0))
	if err != nil || got != session.receipt || reader.Next() || reader.Err() != nil {
		t.Fatal("receipt changed during transport", err)
	}
}

type ingestionBrokenWriter struct{}

func (ingestionBrokenWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestIngestionCommittedEffectSurvivesBrokenResultPipe(t *testing.T) {
	receipt, err := runIngestion(t, ingestionRequest(t, operations.IngestionCommit), &ingestionSession{}, ingestionBrokenWriter{})
	if err == nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectCommitted || receipt.Result != nil {
		t.Fatal("confirmed commit lost to result error", err)
	}
}

func TestIngestionFailureOutcomesDoNotReplay(t *testing.T) {
	for _, test := range []struct {
		err     error
		code    string
		unknown bool
	}{{ingestion.ErrOutcomeUnknown, "OUTCOME_UNKNOWN", true}, {ingestion.ErrConflict, "CONFLICT", false}, {ingestion.ErrUninitialized, "NOT_INITIALIZED", false}, {ingestion.ErrDatabase, "DATABASE_MISMATCH", false}, {errors.New("driver failure"), "SOURCE_FAILED", false}} {
		t.Run(test.code, func(t *testing.T) {
			session := &ingestionSession{err: test.err}
			var output bytes.Buffer
			receipt, err := runIngestion(t, ingestionRequest(t, operations.IngestionCommit), session, &output)
			if err == nil || receipt.ErrorCode != test.code || session.commits != 1 || output.Len() != 0 || (receipt.Outcome == operations.OutcomeUnknown) != test.unknown {
				t.Fatal("incorrect failure or repeated execution", err)
			}
		})
	}
}

func TestIngestionStateDoesNotInstall(t *testing.T) {
	session := &ingestionSession{}
	var output bytes.Buffer
	receipt, err := runIngestion(t, ingestionRequest(t, operations.IngestionState), session, &output)
	if err != nil || receipt.Effect != operations.EffectNone || receipt.Result == nil || session.installs != 0 || session.commits != 0 {
		t.Fatal("state mutated destination", err)
	}
}
