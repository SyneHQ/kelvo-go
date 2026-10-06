// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/watch"
)

func TestWatchCheckpointValidatedBeforeCredentials(t *testing.T) {
	scope := watch.Scope{TeamID: "team", ConnectionID: "saved", Database: "app", Schema: "public", Table: "orders", ID: "watcher", Generation: "generation"}
	batch, err := watch.NewBatch(scope, []watch.Event{{ID: "1", Operation: "INSERT", Data: json.RawMessage(`{"id":1}`), OldData: json.RawMessage(`null`), Timestamp: time.Now().UTC()}})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(batch.Checkpoint)
	digest := sha256.Sum256(raw)
	request := operations.Request{Version: operations.Version, Kind: operations.WatchAck, Connection: operations.ConnectionRef{ID: scope.ConnectionID, Database: scope.Database, Schema: scope.Schema}, IdempotencyKey: "ack-1", Spec: operations.Spec{Watch: &operations.WatchSpec{ID: scope.ID, Generation: scope.Generation, Mode: "native", Target: operations.ObjectRef{Schema: scope.Schema, Name: scope.Table}, SinkReceiptSHA256: strings.Repeat("a", 64), Checkpoint: &operations.InputRef{ID: "sealed", SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(raw)), Format: "watch_checkpoint_v1"}}}}
	if err := ValidateOperationInput(request, scope.TeamID, raw); err != nil {
		t.Fatal(err)
	}
	if err := ValidateOperationInput(request, "other", raw); err == nil {
		t.Fatal("cross-team checkpoint accepted")
	}
	request.Spec.Watch.Generation = "new-generation"
	if err := ValidateOperationInput(request, scope.TeamID, raw); err == nil {
		t.Fatal("stale generation accepted")
	}
	request.Spec.Watch.Generation = scope.Generation
	request.Spec.Watch.Target.Schema = "other"
	if err := ValidateOperationInput(request, scope.TeamID, raw); err == nil {
		t.Fatal("cross-schema checkpoint accepted")
	}
}

func TestLegacyResumeInputCannotChangeSourceRevisionOrBeIgnored(t *testing.T) {
	scope := watch.Scope{TeamID: "team", ConnectionID: "saved", Database: "app", Schema: "app", Table: "orders", ID: "watcher", Generation: "generation"}
	value := watch.Import{Version: watch.Version, ScopeSHA256: scope.Key(), Generation: scope.Generation, SourceIdentitySHA256: strings.Repeat("a", 64), SourceRevision: "revision-a", ResumeToken: json.RawMessage(`{"_data":"legacy"}`)}
	raw, err := watch.EncodeImport(value, scope, value.SourceRevision)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	request := operations.Request{Version: operations.Version, Kind: operations.WatchInstall, Connection: operations.ConnectionRef{ID: scope.ConnectionID, Database: scope.Database, Schema: scope.Schema}, IdempotencyKey: "import-1", Spec: operations.Spec{Watch: &operations.WatchSpec{ID: scope.ID, Generation: scope.Generation, Mode: "native", Target: operations.ObjectRef{Name: scope.Table}, Resume: &operations.InputRef{ID: "sealed", SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(raw)), Format: "mongo_watch_resume_v1"}}}}
	if err := ValidateOperationInput(request, scope.TeamID, raw); err != nil {
		t.Fatal(err)
	}
	process := ProcessRequest{Request: request, AppTeam: scope.TeamID, Input: raw, Source: ConnectionSpec{Engine: "mongodb", Revision: "revision-a"}}
	if err := process.validateInput(); err != nil {
		t.Fatal(err)
	}
	process.Source.Revision = "revision-b"
	if err := process.validateInput(); err == nil {
		t.Fatal("changed private revision accepted")
	}
	process.Source.Revision = "revision-a"
	process.Source.Engine = "postgresql"
	if err := process.validateInput(); err == nil {
		t.Fatal("non-Mongo adapter could ignore resume")
	}
	request.Kind = operations.WatchRemove
	if request.Validate() == nil {
		t.Fatal("resume input accepted for removal")
	}
}
