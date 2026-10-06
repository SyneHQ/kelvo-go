// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/ingestion"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func ingestionProcess(t *testing.T) ProcessRequest {
	t.Helper()
	request := processFixture(t, time.Now())
	batch := ingestion.Batch{ID: "batch-1", RunID: "run-1", ObservedAt: "2026-10-06T00:00:00Z", Checkpoint: json.RawMessage(`{}`)}
	raw, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	request.Request.Kind, request.Request.IdempotencyKey = operations.IngestionCommit, "batch-1"
	request.Request.Spec = operations.Spec{Ingestion: &operations.IngestionSpec{
		Scope: operations.IngestionScope{SourceID: "source-1", Stream: "events", Binding: strings.Repeat("a", 64)}, BatchID: batch.ID,
		Input: &operations.InputRef{ID: strings.Repeat("1", 32), SHA256: hex.EncodeToString(hash[:]), Bytes: int64(len(raw)), Format: "ingestion_batch_v1"}}}
	request.RequestSHA256, err = operations.Digest(request.Request)
	if err != nil {
		t.Fatal(err)
	}
	request.AppTeam, request.Input = "team-1", raw
	return request
}

func TestIngestionInputBindsExactBatchAndTeam(t *testing.T) {
	request := ingestionProcess(t)
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	scope, err := request.IngestionScope()
	if err != nil || scope.TeamID != request.AppTeam || scope.ConnectionID != request.Request.Connection.ID || scope.Schema != request.Request.Connection.Schema {
		t.Fatal("scope not bound to private custody", err)
	}
	for name, mutate := range map[string]func(*ProcessRequest){
		"missing team":   func(r *ProcessRequest) { r.AppTeam = "" },
		"missing input":  func(r *ProcessRequest) { r.Input = nil },
		"altered byte":   func(r *ProcessRequest) { r.Input[0] = '[' },
		"wrong batch":    func(r *ProcessRequest) { r.Request.Spec.Ingestion.BatchID = "other" },
		"wrong sequence": func(r *ProcessRequest) { r.Request.Spec.Ingestion.ExpectedSequence = 1 },
		"unsafe schema":  func(r *ProcessRequest) { r.Request.Connection.Schema = "pg_catalog"; r.Source.Schema = "pg_catalog" },
	} {
		t.Run(name, func(t *testing.T) {
			r := ingestionProcess(t)
			mutate(&r)
			r.RequestSHA256, _ = operations.Digest(r.Request)
			if r.Validate() == nil {
				t.Fatal("invalid ingestion input accepted")
			}
		})
	}
}

func TestReadCannotSmuggleSealedPayload(t *testing.T) {
	request := processFixture(t, time.Now())
	request.Input = []byte(`{}`)
	if request.Validate() == nil {
		t.Fatal("unbound input accepted")
	}
}

func TestLargeSealedInputFitsPrivatePipeBound(t *testing.T) {
	request := ingestionProcess(t)
	batch, err := ingestion.ParseBatch(request.Input)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		batch.Records = append(batch.Records, ingestion.Record{ID: fmt.Sprintf("row-%d", i), Payload: json.RawMessage(`{"value":"` + strings.Repeat("x", 100<<10) + `"}`)})
	}
	request.Input, err = json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(request.Input)
	request.Request.Spec.Ingestion.Input.SHA256 = hex.EncodeToString(hash[:])
	request.Request.Spec.Ingestion.Input.Bytes = int64(len(request.Input))
	request.RequestSHA256, _ = operations.Digest(request.Request)
	raw, err := EncodeProcessRequest(request)
	if err != nil || len(raw) <= 1<<20 || len(raw) > MaxProcessRequestBytes {
		t.Fatal("valid large batch cannot be encoded", err)
	}
	decoded, err := ParseProcessRequest(raw)
	if err != nil || !bytes.Equal(decoded.Input, request.Input) {
		t.Fatal("large private input did not round trip", err)
	}
}
