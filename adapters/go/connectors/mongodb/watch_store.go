// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"context"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/watch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
)

const watchStateCollection = "_kelvo_watch_state"
const maxRetiredGenerations = 1000

type watchState struct {
	ID           string   `bson:"_id"`
	Generation   string   `bson:"generation"`
	Status       string   `bson:"status"`
	Token        string   `bson:"token"`
	Retired      []string `bson:"retired"`
	LastFrom     string   `bson:"last_from"`
	LastAck      string   `bson:"last_ack"`
	LastReceipt  string   `bson:"last_receipt"`
	ImportSHA256 string   `bson:"import_sha256,omitempty"`
}

func (s watchState) validate(scope watch.Scope) error {
	if s.ImportSHA256 != "" && !operations.ValidDigest(s.ImportSHA256) {
		return watch.ErrConflict
	}
	if s.ID != scope.Key() || !operations.ValidID(s.Generation) || len(s.Retired) > maxRetiredGenerations {
		return watch.ErrConflict
	}
	if _, err := decodeWatchToken(s.Token); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, generation := range s.Retired {
		if !operations.ValidID(generation) || seen[generation] {
			return watch.ErrConflict
		}
		seen[generation] = true
	}
	if s.Status != "active" && s.Status != "retired" || s.Status == "active" && seen[s.Generation] || s.Status == "retired" && !seen[s.Generation] {
		return watch.ErrConflict
	}
	if s.LastFrom != "" || s.LastAck != "" || s.LastReceipt != "" {
		if _, err := decodeWatchToken(s.LastFrom); err != nil || !operations.ValidDigest(s.LastAck) || !operations.ValidDigest(s.LastReceipt) {
			return watch.ErrConflict
		}
	}
	return nil
}

type mongoWatchBackend struct{ database *mongo.Database }

func (b mongoWatchBackend) stateCollection() *mongo.Collection {
	return b.database.Collection(watchStateCollection, options.Collection().SetReadConcern(readconcern.Majority()))
}
func (b mongoWatchBackend) load(ctx context.Context, scope watch.Scope) (watchState, error) {
	var state watchState
	// Bound the state document before decoding. Generations are bounded and no
	// source event payload is persisted in this collection.
	raw, err := b.stateCollection().FindOne(ctx, bson.D{{Key: "_id", Value: scope.Key()}}).Raw()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return state, watch.ErrUninitialized
	}
	if err != nil {
		return state, err
	}
	if len(raw) > 512<<10 {
		return state, watch.ErrLimit
	}
	err = bson.Unmarshal(raw, &state)
	return state, err
}
func (b mongoWatchBackend) create(ctx context.Context, state watchState) error {
	_, err := b.stateCollection().InsertOne(ctx, state)
	return err
}
func (b mongoWatchBackend) compareAndSwap(ctx context.Context, previous, next watchState) (bool, error) {
	filter := bson.D{{Key: "_id", Value: previous.ID}, {Key: "generation", Value: previous.Generation}, {Key: "status", Value: previous.Status}, {Key: "token", Value: previous.Token}, {Key: "last_from", Value: previous.LastFrom}, {Key: "last_ack", Value: previous.LastAck}, {Key: "last_receipt", Value: previous.LastReceipt}, {Key: "retired", Value: previous.Retired}}
	if previous.ImportSHA256 != "" {
		filter = append(filter, bson.E{Key: "import_sha256", Value: previous.ImportSHA256})
	} else {
		filter = append(filter, bson.E{Key: "import_sha256", Value: bson.D{{Key: "$exists", Value: false}}})
	}
	result, err := b.stateCollection().ReplaceOne(ctx, filter, next)
	if err != nil {
		return false, err
	}
	return result.MatchedCount == 1, nil
}
func (b mongoWatchBackend) open(ctx context.Context, scope watch.Scope, token bson.Raw, waitMS int) (watchCursor, error) {
	// UpdateLookup can return a later document; optional images can disappear
	// between retries. The default immutable envelope carries update deltas and
	// delete keys, with explicit payload kinds for consumers.
	settings := options.ChangeStream().SetFullDocument(options.Default).SetMaxAwaitTime(time.Duration(min(waitMS, 1000)) * time.Millisecond).SetBatchSize(1)
	if token != nil {
		settings.SetResumeAfter(token)
	}
	stream, err := b.database.Collection(scope.Table).Watch(ctx, mongo.Pipeline{}, settings)
	if err != nil {
		return nil, err
	}
	return mongoWatchCursor{stream}, nil
}

type mongoWatchCursor struct{ *mongo.ChangeStream }

func (c mongoWatchCursor) CurrentDocument() bson.Raw { return c.Current }
