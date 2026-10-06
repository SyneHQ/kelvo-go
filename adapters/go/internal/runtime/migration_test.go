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
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type migrationSession struct {
	reads, applies, closed int
	err                    error
	effect                 operations.Effect
}

func (s *migrationSession) Close() error { s.closed++; return nil }
func (s *migrationSession) MigrationStatus(context.Context) (migration.State, error) {
	s.reads++
	return migration.State{Version: -1}, s.err
}
func (s *migrationSession) ApplyMigration(_ context.Context, p migration.Plan) (migration.Result, error) {
	s.applies++
	return migration.Result{Version: migration.Version, From: p.Expected, To: migration.State{Version: 1}, FilesApplied: []string{"1_orders.up.sql"}, Effect: s.effect}, s.err
}

func migrationRequest(t *testing.T, kind operations.Kind) adapter.ProcessRequest {
	t.Helper()
	r := runtimeRequest(t, operations.ConnectionTest)
	r.Request.Kind = kind
	r.Request.Spec.Migration = &operations.MigrationSpec{ID: migration.Table}
	if kind == operations.MigrationApply {
		r.Request.IdempotencyKey = "migration-1"
		plan := migration.Plan{Version: migration.Version, Expected: migration.State{Version: -1}, Direction: "up", Files: []migration.File{{Name: "1_orders.up.sql", Content: "CREATE TABLE orders(id bigint)"}}}
		r.Input, _ = json.Marshal(plan)
		sum := sha256.Sum256(r.Input)
		r.Request.Spec.Migration.ExpectedVersion = "-1"
		r.Request.Spec.Migration.Transaction = operations.TransactionAutocommit
		r.Request.Spec.Migration.Plan = operations.InputRef{ID: "sealed", SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(r.Input)), Format: "migration_plan_v1"}
	}
	var err error
	r.RequestSHA256, err = operations.Digest(r.Request)
	if err != nil || r.Validate() != nil {
		t.Fatal("invalid migration fixture", err)
	}
	return r
}

func runMigration(t *testing.T, r adapter.ProcessRequest, s *migrationSession, output io.Writer) (operations.Receipt, error) {
	t.Helper()
	raw, _ := json.Marshal(r)
	var out bytes.Buffer
	runner := Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
		return s, nil
	}}
	err := runner.Run(context.Background(), bytes.NewReader(raw), output, &out)
	var receipt operations.Receipt
	if operations.DecodeStrict(out.Bytes(), &receipt, operations.MaxReceiptBytes) != nil || receipt.Validate() != nil {
		t.Fatal("invalid migration receipt", err)
	}
	if s.closed != 1 {
		t.Fatal("session leaked")
	}
	return receipt, err
}

func TestMigrationStatusUsesOnlyReadAndReturnsArrow(t *testing.T) {
	s := &migrationSession{}
	var output bytes.Buffer
	receipt, err := runMigration(t, migrationRequest(t, operations.MigrationStatus), s, &output)
	if err != nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectNone || receipt.Result == nil || s.reads != 1 || s.applies != 0 {
		t.Fatal(receipt, err)
	}
	reader, err := ipc.NewReader(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() || reader.Schema().Field(0).Name != "migration" {
		t.Fatal("migration result missing")
	}
	var state migration.State
	if json.Unmarshal(reader.RecordBatch().Column(0).(*array.Binary).Value(0), &state) != nil || state.Version != -1 || reader.Next() || reader.Err() != nil {
		t.Fatal("invalid status")
	}
}

func TestMigrationConfirmedEffectSurvivesDeliveryFailure(t *testing.T) {
	s := &migrationSession{effect: operations.EffectCommitted}
	receipt, err := runMigration(t, migrationRequest(t, operations.MigrationApply), s, failedMigrationWriter{})
	if err == nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectCommitted || receipt.Result != nil || s.applies != 1 {
		t.Fatal(receipt, err)
	}
}

type failedMigrationWriter struct{}

func (failedMigrationWriter) Write([]byte) (int, error) { return 0, errors.New("pipe closed") }

func TestMigrationUncertaintyNeverBecomesRetryableFailure(t *testing.T) {
	s := &migrationSession{effect: operations.EffectUnknown, err: migration.ErrOutcomeUnknown}
	receipt, err := runMigration(t, migrationRequest(t, operations.MigrationApply), s, io.Discard)
	if err == nil || receipt.Outcome != operations.OutcomeUnknown || receipt.Effect != operations.EffectUnknown || receipt.Result != nil || s.applies != 1 {
		t.Fatal(receipt, err)
	}
}
