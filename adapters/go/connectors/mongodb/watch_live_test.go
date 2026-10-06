package mongodb

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/watch"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func testMongoWatchLive(t *testing.T, s *Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	scope := watch.Scope{TeamID: "live-test", ConnectionID: "live-source", Database: s.database, Schema: s.database, Table: "watch_orders", ID: "watch-live", Generation: "generation-one"}
	db := s.client.Database(s.database)
	if err := db.CreateCollection(ctx, scope.Table); err != nil {
		t.Fatal("watch fixture collection creation failed")
	}
	defer db.Collection(scope.Table).Drop(context.Background())
	if err := s.InstallWatch(ctx, scope); err != nil {
		t.Fatalf("initial watcher installation failed (error_type=%T)", err)
	}
	if err := s.InstallWatch(ctx, scope); err != nil {
		t.Fatal("watch installation replay failed")
	}
	collection := db.Collection(scope.Table)
	if _, err := collection.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "exact", Value: int64(9007199254740993)}, {Key: "nullable", Value: nil}}); err != nil {
		t.Fatal("watch fixture insert failed")
	}
	if _, err := collection.UpdateOne(ctx, bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "$set", Value: bson.D{{Key: "total", Value: int64(9007199254740993)}}}}); err != nil {
		t.Fatal("watch fixture update failed")
	}
	if _, err := collection.DeleteOne(ctx, bson.D{{Key: "_id", Value: 1}}); err != nil {
		t.Fatal("watch fixture delete failed")
	}
	first, err := s.ReadWatch(ctx, scope, 2, 1000, watch.MaxBatchBytes)
	if err != nil || len(first.Events) != 2 {
		t.Fatalf("watch initial read failed (error_type=%T count=%d)", err, len(first.Events))
	}
	again, err := s.ReadWatch(ctx, scope, 2, 1000, watch.MaxBatchBytes)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatal("uncaptured events did not replay exactly")
	}
	if first.Events[0].Operation != "INSERT" || first.Events[1].DataKind != "update_delta" || !strings.Contains(string(first.Events[0].Data), `"$numberLong":"9007199254740993"`) {
		t.Fatal("immutable event shape or exact BSON changed")
	}
	receipt := strings.Repeat("a", 64)
	if err := s.AckWatch(ctx, scope, *first.Checkpoint, receipt); err != nil {
		t.Fatalf("watch acknowledgement failed (error_type=%T)", err)
	}
	if err := s.AckWatch(ctx, scope, *first.Checkpoint, receipt); err != nil {
		t.Fatal("exact watch acknowledgement replay failed")
	}
	bad := *first.Checkpoint
	resume := *bad.Resume
	resume.From = tokenText("unrelated")
	bad.Resume = &resume
	if err := s.AckWatch(ctx, scope, bad, receipt); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("watch acknowledged different predecessor")
	}
	last, err := s.ReadWatch(ctx, scope, 10, 1000, watch.MaxBatchBytes)
	if err != nil || len(last.Events) != 1 || last.Events[0].Operation != "DELETE" || last.Events[0].OldDataKind != "document_key" {
		t.Fatalf("watch did not resume at delete (error_type=%T count=%d)", err, len(last.Events))
	}
	if err := s.AckWatch(ctx, scope, *last.Checkpoint, receipt); err != nil {
		t.Fatal("delete acknowledgement failed")
	}
	if _, err := collection.InsertOne(ctx, bson.D{{Key: "_id", Value: 2}, {Key: "large", Value: strings.Repeat("x", 8192)}}); err != nil {
		t.Fatal("large event insert failed")
	}
	if _, err := s.ReadWatch(ctx, scope, 1, 1000, 1024); !errors.Is(err, watch.ErrLimit) {
		t.Fatal("oversized event did not fail its byte limit")
	}
	large, err := s.ReadWatch(ctx, scope, 1, 1000, watch.MaxBatchBytes)
	if err != nil || len(large.Events) != 1 {
		t.Fatal("byte-limited event was skipped")
	}
	if err := s.AckWatch(ctx, scope, *large.Checkpoint, receipt); err != nil {
		t.Fatal("large event acknowledgement failed")
	}
	idle, err := s.ReadWatch(ctx, scope, 10, 100, watch.MaxBatchBytes)
	if err != nil || len(idle.Events) != 0 {
		t.Fatal("idle source replayed acknowledged events")
	}
	if idle.Checkpoint != nil {
		if err := s.AckWatch(ctx, scope, *idle.Checkpoint, receipt); err != nil {
			t.Fatal("idle token advancement failed")
		}
	}
	if err := s.RemoveWatch(ctx, scope); err != nil {
		t.Fatal("watch removal failed")
	}
	if _, err := s.ReadWatch(ctx, scope, 1, 100, watch.MaxBatchBytes); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("retired watcher remained readable")
	}
	if err := s.InstallWatch(ctx, scope); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("old generation resurrected")
	}
	replacement := scope
	replacement.Generation = "generation-two"
	if err := s.InstallWatch(ctx, replacement); err != nil {
		t.Fatal("replacement generation failed")
	}
	if err := s.AckWatch(ctx, scope, *large.Checkpoint, receipt); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("old acknowledgement affected replacement")
	}
	if err := s.RemoveWatch(ctx, replacement); err != nil {
		t.Fatal("replacement cleanup failed")
	}
	t.Log("verified MongoDB initial token, immutable replay, exact acknowledgements, update deltas, delete keys, limits and generation isolation")
	testMongoLegacyResumeLive(t, s)
}

func testMongoLegacyResumeLive(t *testing.T, s *Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	scope := watch.Scope{TeamID: "live-test", ConnectionID: "live-source", Database: s.database, Schema: s.database, Table: "legacy_orders", ID: "legacy-import", Generation: "generation-import"}
	db := s.client.Database(s.database)
	if err := db.CreateCollection(ctx, scope.Table); err != nil {
		t.Fatal("legacy fixture collection unavailable")
	}
	defer db.Collection(scope.Table).Drop(context.Background())
	backend := mongoWatchBackend{database: db}
	legacy, err := backend.open(ctx, scope, nil, 1000)
	if err != nil {
		t.Fatal("legacy stream unavailable")
	}
	initial := append(bson.Raw{}, legacy.ResumeToken()...)
	closeWatchCursor(legacy)
	if _, err := encodeWatchToken(initial); err != nil {
		t.Fatal("legacy stream had no server position")
	}
	raw, err := bson.MarshalExtJSON(initial, false, false)
	if err != nil {
		t.Fatal("legacy EJSON serialization failed")
	}
	resume := watch.Import{Version: watch.Version, ScopeSHA256: scope.Key(), Generation: scope.Generation, SourceIdentitySHA256: strings.Repeat("a", 64), SourceRevision: s.revision, ResumeToken: raw}
	for _, id := range []int{1, 2} {
		if _, err := db.Collection(scope.Table).InsertOne(ctx, bson.D{{Key: "_id", Value: id}}); err != nil {
			t.Fatal("legacy fixture insert failed")
		}
	}
	if err := s.ImportWatch(ctx, scope, resume); err != nil {
		t.Fatalf("legacy token import failed (%T)", err)
	}
	first, err := s.ReadWatch(ctx, scope, 1, 1000, watch.MaxBatchBytes)
	if err != nil || len(first.Events) != 1 {
		t.Fatal("legacy import skipped existing events")
	}
	receipt := strings.Repeat("b", 64)
	if err := s.AckWatch(ctx, scope, *first.Checkpoint, receipt); err != nil {
		t.Fatal("legacy acknowledgement failed")
	}
	if err := s.ImportWatch(ctx, scope, resume); err != nil {
		t.Fatal("legacy import replay failed")
	}
	second, err := s.ReadWatch(ctx, scope, 1, 1000, watch.MaxBatchBytes)
	if err != nil || len(second.Events) != 1 || second.Events[0].ID == first.Events[0].ID {
		t.Fatal("legacy import replay reset progress")
	}
	if err := s.RemoveWatch(ctx, scope); err != nil {
		t.Fatal("legacy import cleanup failed")
	}
	if err := s.ImportWatch(ctx, scope, resume); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("retired legacy generation revived")
	}
	invalid := scope
	invalid.ID = "invalid-legacy"
	bad := resume
	bad.ScopeSHA256 = invalid.Key()
	bad.ResumeToken = []byte(`{"_data":"invalid-server-token"}`)
	if err := s.ImportWatch(ctx, invalid, bad); err == nil {
		t.Fatal("invalid legacy token reset to current time")
	}
	if _, err := backend.load(ctx, invalid); !errors.Is(err, watch.ErrUninitialized) {
		t.Fatal("invalid legacy import installed state")
	}
	t.Log("verified real legacy EJSON import, backlog preservation, idempotency after ACK, retirement and invalid-token rejection")
}
