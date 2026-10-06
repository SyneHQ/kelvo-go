// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/watch"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func mongoWatchScope() watch.Scope {
	return watch.Scope{TeamID: "team", ConnectionID: "saved", Database: "app", Schema: "app", Table: "orders", ID: "watcher", Generation: "generation"}
}
func testToken(value string) bson.Raw {
	raw, _ := bson.Marshal(bson.D{{Key: "_data", Value: value}})
	return raw
}
func tokenText(value string) string { encoded, _ := encodeWatchToken(testToken(value)); return encoded }
func changeEvent(operation, token string) bson.Raw {
	doc := bson.D{{Key: "_id", Value: testToken(token)}, {Key: "operationType", Value: operation}, {Key: "ns", Value: bson.D{{Key: "db", Value: "app"}, {Key: "coll", Value: "orders"}}}, {Key: "documentKey", Value: bson.D{{Key: "_id", Value: int64(9007199254740993)}}}, {Key: "clusterTime", Value: bson.Timestamp{T: 1791244800, I: 1}}}
	switch operation {
	case "insert", "replace":
		doc = append(doc, bson.E{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: int64(9007199254740993)}, {Key: "nullable", Value: nil}, {Key: "binary", Value: bson.Binary{Subtype: 128, Data: []byte{1, 2, 3}}}}})
	case "update":
		doc = append(doc, bson.E{Key: "updateDescription", Value: bson.D{{Key: "updatedFields", Value: bson.D{{Key: "amount", Value: int64(9007199254740993)}}}, {Key: "removedFields", Value: bson.A{}}}})
	}
	raw, _ := bson.Marshal(doc)
	return raw
}

type fakeWatchBackend struct {
	state                  watchState
	exists                 bool
	cursor                 *fakeWatchCursor
	opens, creates, writes int
	openErr, writeErr      error
}

func (b *fakeWatchBackend) load(context.Context, watch.Scope) (watchState, error) {
	if !b.exists {
		return watchState{}, watch.ErrUninitialized
	}
	return b.state, nil
}
func (b *fakeWatchBackend) create(_ context.Context, s watchState) error {
	b.creates++
	if b.writeErr != nil {
		return b.writeErr
	}
	b.state, b.exists = s, true
	return nil
}
func (b *fakeWatchBackend) compareAndSwap(_ context.Context, old, next watchState) (bool, error) {
	b.writes++
	if b.writeErr != nil {
		return false, b.writeErr
	}
	if !reflect.DeepEqual(old, b.state) {
		return false, nil
	}
	b.state = next
	return true, nil
}
func (b *fakeWatchBackend) open(_ context.Context, _ watch.Scope, from bson.Raw, _ int) (watchCursor, error) {
	b.opens++
	if b.openErr != nil {
		return nil, b.openErr
	}
	if b.cursor == nil {
		return nil, errors.New("no fake stream")
	}
	copy := *b.cursor
	copy.index = 0
	return &copy, nil
}

type fakeWatchCursor struct {
	initial, empty bson.Raw
	events         []bson.Raw
	index          int
	err            error
}

func (c *fakeWatchCursor) TryNext(context.Context) bool {
	if c.index >= len(c.events) {
		return false
	}
	c.index++
	return true
}
func (c *fakeWatchCursor) CurrentDocument() bson.Raw { return c.events[c.index-1] }
func (c *fakeWatchCursor) ResumeToken() bson.Raw {
	if c.index == 0 {
		return c.initial
	}
	if c.index <= len(c.events) {
		return c.events[c.index-1].Lookup("_id").Document()
	}
	return c.empty
}
func (c *fakeWatchCursor) Err() error                { return c.err }
func (*fakeWatchCursor) Close(context.Context) error { return nil }
func activeFake() (*fakeWatchBackend, mongoWatch) {
	scope := mongoWatchScope()
	backend := &fakeWatchBackend{exists: true, state: watchState{ID: scope.Key(), Generation: scope.Generation, Status: "active", Token: tokenText("initial"), Retired: []string{}}}
	return backend, mongoWatch{scope, backend}
}

func TestMongoWatcherInstallNeverConsumesFirstEventOrResetsToken(t *testing.T) {
	scope := mongoWatchScope()
	backend := &fakeWatchBackend{cursor: &fakeWatchCursor{initial: testToken("initial")}}
	w := mongoWatch{scope, backend}
	if err := w.install(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.creates != 1 || backend.state.Token != tokenText("initial") {
		t.Fatal("initial position not persisted")
	}
	backend.cursor.initial = testToken("later")
	if err := w.install(context.Background()); err != nil || backend.opens != 1 || backend.state.Token != tokenText("initial") {
		t.Fatal("installed position was reset")
	}
	if err := w.remove(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w.install(context.Background()); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("retired generation resurrected")
	}
	w.scope.Generation = "replacement"
	if err := w.install(context.Background()); err != nil {
		t.Fatal(err)
	}
	w.scope.Generation = "generation"
	if err := w.remove(context.Background()); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("old generation removed replacement")
	}
	missing := &fakeWatchBackend{cursor: &fakeWatchCursor{events: []bson.Raw{changeEvent("insert", "first")}}}
	if err := (mongoWatch{scope, missing}).install(context.Background()); err == nil || missing.creates != 0 {
		t.Fatal("first event used as initial checkpoint")
	}
}

func TestMongoWatcherReadReplaysUntilExactAcknowledgement(t *testing.T) {
	b, w := activeFake()
	b.cursor = &fakeWatchCursor{initial: testToken("initial"), events: []bson.Raw{changeEvent("insert", "one"), changeEvent("update", "two"), changeEvent("delete", "three")}}
	first, err := w.read(context.Background(), 2, 100, watch.MaxBatchBytes)
	if err != nil {
		t.Fatal(err)
	}
	again, err := w.read(context.Background(), 2, 100, watch.MaxBatchBytes)
	if err != nil || !reflect.DeepEqual(first, again) || len(first.Events) != 2 || b.writes != 0 {
		t.Fatal("read advanced token before capture", err)
	}
	if first.Events[1].DataKind != "update_delta" || !strings.Contains(string(first.Events[1].Data), "9007199254740993") {
		t.Fatal("update identity/type changed")
	}
	receipt := strings.Repeat("a", 64)
	if err = w.ack(context.Background(), *first.Checkpoint, receipt); err != nil {
		t.Fatal(err)
	}
	if err = w.ack(context.Background(), *first.Checkpoint, receipt); err != nil || b.writes != 1 {
		t.Fatal("exact acknowledgement did not replay idempotently", err)
	}
	bad := *first.Checkpoint
	copy := *bad.Resume
	copy.From = tokenText("different")
	bad.Resume = &copy
	if err = w.ack(context.Background(), bad, receipt); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("same To with different From replayed")
	}
	bad = *first.Checkpoint
	bad.Entries = append([]watch.Entry{}, bad.Entries...)
	bad.Entries[1].SHA256 = strings.Repeat("b", 64)
	if err = w.ack(context.Background(), bad, receipt); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("changed acknowledgement replayed")
	}
	if _, err = w.read(context.Background(), 1, 100, 1024); err == nil {
		t.Fatal("unbounded first event accepted")
	}
}

func TestMongoWatcherEmptyProgressAndUnknownMutation(t *testing.T) {
	b, w := activeFake()
	b.cursor = &fakeWatchCursor{initial: testToken("empty-advance")}
	batch, err := w.read(context.Background(), 10, 100, watch.MaxBatchBytes)
	if err != nil || len(batch.Events) != 0 || batch.Checkpoint == nil {
		t.Fatal("empty-batch checkpoint missing", err)
	}
	if err = w.ack(context.Background(), *batch.Checkpoint, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	b.writeErr = errors.New("response lost")
	if err = w.remove(context.Background()); !errors.Is(err, watch.ErrOutcomeUnknown) {
		t.Fatal("mutation uncertainty hidden")
	}
	b.exists = false
	if _, err = w.read(context.Background(), 1, 100, watch.MaxBatchBytes); !errors.Is(err, watch.ErrUninitialized) || b.creates != 0 {
		t.Fatal("read installed source objects")
	}
}

func TestMongoWatchEnvelopePreservesTypesAndExplicitPayloadKinds(t *testing.T) {
	for _, operation := range []string{"insert", "update", "replace", "delete"} {
		raw := changeEvent(operation, "position")
		event, token, err := decodeWatchEvent(mongoWatchScope(), raw, watch.MaxBatchBytes)
		if err != nil {
			t.Fatal(err)
		}
		if event.ID != watchEventID(token) || event.Timestamp != time.Unix(1791244800, 0).UTC() {
			t.Fatal("event identity/time changed")
		}
		var context map[string]json.RawMessage
		if json.Unmarshal(event.Context, &context) != nil || string(context["operationType"]) != `"`+operation+`"` {
			t.Fatal("filter envelope lost")
		}
		if operation == "insert" && !strings.Contains(string(event.Data), `"$numberLong":"9007199254740993"`) {
			t.Fatal("BSON integer type lost")
		}
		if operation == "delete" && event.OldDataKind != "document_key" {
			t.Fatal("delete key advertised as a complete image")
		}
		if operation == "update" && event.DataKind != "update_delta" {
			t.Fatal("update delta advertised as a complete image")
		}
	}
	if _, _, err := decodeWatchEvent(mongoWatchScope(), changeEvent("drop", "position"), watch.MaxBatchBytes); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("invalidation silently advanced")
	}
	scope := mongoWatchScope()
	scope.Table = "other"
	if _, _, err := decodeWatchEvent(scope, changeEvent("insert", "position"), watch.MaxBatchBytes); !errors.Is(err, watch.ErrConflict) {
		t.Fatal("cross-collection event accepted")
	}
	if _, err := decodeWatchToken("not-a-token"); err == nil {
		t.Fatal("invalid token accepted")
	}
}
