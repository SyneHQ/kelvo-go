// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/watch"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var _ adapter.WatchSession = (*Session)(nil)

type watchCursor interface {
	TryNext(context.Context) bool
	CurrentDocument() bson.Raw
	ResumeToken() bson.Raw
	Err() error
	Close(context.Context) error
}

type watchBackend interface {
	load(context.Context, watch.Scope) (watchState, error)
	create(context.Context, watchState) error
	compareAndSwap(context.Context, watchState, watchState) (bool, error)
	open(context.Context, watch.Scope, bson.Raw, int) (watchCursor, error)
}

type mongoWatch struct {
	scope   watch.Scope
	backend watchBackend
}

func (w mongoWatch) validate(ctx context.Context) error {
	if ctx == nil || w.backend == nil || w.scope.Validate() != nil || w.scope.Schema != w.scope.Database || !validCollection(w.scope.Table) || w.scope.Table == watchStateCollection {
		return watch.ErrInvalid
	}
	return ctx.Err()
}

func (s *Session) watcher(scope watch.Scope) (mongoWatch, error) {
	if s == nil || s.client == nil || scope.Validate() != nil || scope.Database != s.database || scope.Schema != s.database || !validCollection(scope.Table) || scope.Table == watchStateCollection {
		return mongoWatch{}, adapter.ErrInvalid
	}
	return mongoWatch{scope: scope, backend: mongoWatchBackend{database: s.client.Database(s.database)}}, nil
}
func (s *Session) InstallWatch(ctx context.Context, scope watch.Scope) error {
	w, err := s.watcher(scope)
	if err != nil {
		return err
	}
	return w.install(ctx)
}
func (s *Session) ReadWatch(ctx context.Context, scope watch.Scope, maximum, waitMS int, maxBytes int64) (watch.Batch, error) {
	w, err := s.watcher(scope)
	if err != nil {
		return watch.Batch{}, err
	}
	return w.read(ctx, maximum, waitMS, maxBytes)
}
func (s *Session) AckWatch(ctx context.Context, scope watch.Scope, checkpoint watch.Checkpoint, receipt string) error {
	w, err := s.watcher(scope)
	if err != nil {
		return err
	}
	return w.ack(ctx, checkpoint, receipt)
}
func (s *Session) RemoveWatch(ctx context.Context, scope watch.Scope) error {
	w, err := s.watcher(scope)
	if err != nil {
		return err
	}
	return w.remove(ctx)
}

func encodeWatchToken(raw bson.Raw) (string, error) {
	if len(raw) < 5 || len(raw) > watch.MaxResumeTokenBytes || raw.Validate() != nil {
		return "", watch.ErrInvalid
	}
	elements, err := raw.Elements()
	if err != nil || len(elements) == 0 {
		return "", watch.ErrInvalid
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}
func decodeWatchToken(encoded string) (bson.Raw, error) {
	if len(encoded) > base64.StdEncoding.EncodedLen(watch.MaxResumeTokenBytes) {
		return nil, watch.ErrLimit
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return nil, watch.ErrInvalid
	}
	canonical, err := encodeWatchToken(bson.Raw(raw))
	if err != nil || canonical != encoded {
		return nil, watch.ErrInvalid
	}
	return bson.Raw(raw), nil
}
func watchEventID(token bson.Raw) string {
	sum := sha256.Sum256(token)
	return "mongo:" + hex.EncodeToString(sum[:])
}
func closeWatchCursor(cursor watchCursor) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = cursor.Close(ctx)
}

func (w mongoWatch) install(ctx context.Context) error {
	if err := w.validate(ctx); err != nil {
		return err
	}
	previous, err := w.backend.load(ctx, w.scope)
	exists := err == nil
	if err != nil && !errors.Is(err, watch.ErrUninitialized) {
		return err
	}
	if exists {
		if previous.validate(w.scope) != nil {
			return watch.ErrConflict
		}
		if previous.Status == "active" {
			if previous.Generation == w.scope.Generation {
				return nil
			}
			return watch.ErrConflict
		}
		for _, generation := range previous.Retired {
			if generation == w.scope.Generation {
				return watch.ErrConflict
			}
		}
		if len(previous.Retired) >= maxRetiredGenerations {
			return watch.ErrLimit
		}
	}
	cursor, err := w.backend.open(ctx, w.scope, nil, 1000)
	if err != nil {
		return err
	}
	defer closeWatchCursor(cursor)
	// Never consume the first event to manufacture a starting position. If the
	// server supplied no initial token, fail before installing anything.
	token, err := encodeWatchToken(cursor.ResumeToken())
	if err != nil {
		return err
	}
	next := watchState{ID: w.scope.Key(), Generation: w.scope.Generation, Status: "active", Token: token, Retired: append([]string{}, previous.Retired...)}
	if exists {
		ok, err := w.backend.compareAndSwap(ctx, previous, next)
		if err != nil {
			return errors.Join(watch.ErrOutcomeUnknown, err)
		}
		if !ok {
			return watch.ErrConflict
		}
		return nil
	}
	if err = w.backend.create(ctx, next); err != nil {
		// A racing or uncertain insert is safe only if the exact generation is
		// already active. Never reset an existing cursor or retired generation.
		current, loadErr := w.backend.load(ctx, w.scope)
		if loadErr == nil && current.validate(w.scope) == nil && current.Generation == w.scope.Generation && current.Status == "active" {
			return nil
		}
		return errors.Join(watch.ErrOutcomeUnknown, err)
	}
	return nil
}

func (w mongoWatch) active(ctx context.Context) (watchState, error) {
	state, err := w.backend.load(ctx, w.scope)
	if err != nil {
		return state, err
	}
	if state.validate(w.scope) != nil || state.Generation != w.scope.Generation || state.Status != "active" {
		return state, watch.ErrConflict
	}
	return state, nil
}

func (w mongoWatch) read(ctx context.Context, maximum, waitMS int, maxBytes int64) (watch.Batch, error) {
	if err := w.validate(ctx); err != nil {
		return watch.Batch{}, err
	}
	if ctx == nil || maximum < 1 || maximum > watch.MaxEvents || waitMS < 1 || waitMS > 60000 || maxBytes < 1024 {
		return watch.Batch{}, watch.ErrInvalid
	}
	maxBytes = min(maxBytes, int64(watch.MaxBatchBytes))
	state, err := w.active(ctx)
	if err != nil {
		return watch.Batch{}, err
	}
	from, err := decodeWatchToken(state.Token)
	if err != nil {
		return watch.Batch{}, err
	}
	cursor, err := w.backend.open(ctx, w.scope, from, waitMS)
	if err != nil {
		return watch.Batch{}, err
	}
	defer closeWatchCursor(cursor)
	events := []watch.Event{}
	to := state.Token
	var batch watch.Batch
	for len(events) < maximum {
		found := cursor.TryNext(ctx)
		if err = cursor.Err(); err != nil {
			return watch.Batch{}, err
		}
		if err = ctx.Err(); err != nil {
			return watch.Batch{}, err
		}
		if !found {
			// Advance an empty-batch token only when no unacknowledged event is
			// represented in this result. This bounds oplog age on idle sources.
			if len(events) == 0 && len(cursor.ResumeToken()) > 0 {
				to, err = encodeWatchToken(cursor.ResumeToken())
				if err != nil {
					return watch.Batch{}, err
				}
			}
			break
		}
		event, token, decodeErr := decodeWatchEvent(w.scope, cursor.CurrentDocument(), maxBytes)
		if decodeErr != nil {
			return watch.Batch{}, decodeErr
		}
		candidate := append(append([]watch.Event{}, events...), event)
		nextToken, tokenErr := encodeWatchToken(token)
		if tokenErr != nil {
			return watch.Batch{}, tokenErr
		}
		nextBatch, batchErr := watch.NewResumeBatch(w.scope, candidate, state.Token, nextToken)
		encoded, _ := json.Marshal(nextBatch)
		if batchErr != nil || int64(len(encoded)) > maxBytes {
			if len(events) == 0 {
				return watch.Batch{}, watch.ErrLimit
			}
			break
		}
		events, to, batch = candidate, nextToken, nextBatch
	}
	current, err := w.active(ctx)
	if err != nil {
		return watch.Batch{}, err
	}
	if current.Token != state.Token {
		return watch.Batch{}, watch.ErrConflict
	}
	if len(events) > 0 {
		return batch, nil
	}
	if to == state.Token {
		return watch.NewBatch(w.scope, nil)
	}
	batch, err = watch.NewResumeBatch(w.scope, nil, state.Token, to)
	encoded, _ := json.Marshal(batch)
	if int64(len(encoded)) > maxBytes {
		return watch.Batch{}, watch.ErrLimit
	}
	return batch, err
}

func (w mongoWatch) ack(ctx context.Context, checkpoint watch.Checkpoint, receipt string) error {
	if err := w.validate(ctx); err != nil {
		return err
	}
	if checkpoint.Validate(w.scope) != nil || checkpoint.Resume == nil || !operations.ValidDigest(receipt) {
		return watch.ErrInvalid
	}
	_, err := decodeWatchToken(checkpoint.Resume.From)
	if err != nil {
		return err
	}
	to, err := decodeWatchToken(checkpoint.Resume.To)
	if err != nil {
		return err
	}
	if len(checkpoint.Entries) > 0 && checkpoint.Entries[len(checkpoint.Entries)-1].ID != watchEventID(to) {
		return watch.ErrConflict
	}
	raw, _ := json.Marshal(checkpoint)
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	state, err := w.active(ctx)
	if err != nil {
		return err
	}
	if state.Token == checkpoint.Resume.To && state.LastFrom == checkpoint.Resume.From && state.LastAck == digest && state.LastReceipt == receipt {
		return nil
	}
	if state.Token != checkpoint.Resume.From {
		return watch.ErrConflict
	}
	next := state
	next.Token, next.LastFrom, next.LastAck, next.LastReceipt = checkpoint.Resume.To, checkpoint.Resume.From, digest, receipt
	ok, err := w.backend.compareAndSwap(ctx, state, next)
	if err != nil {
		return errors.Join(watch.ErrOutcomeUnknown, err)
	}
	if !ok {
		return watch.ErrConflict
	}
	return nil
}

func (w mongoWatch) remove(ctx context.Context) error {
	if err := w.validate(ctx); err != nil {
		return err
	}
	state, err := w.backend.load(ctx, w.scope)
	if err != nil {
		return err
	}
	if state.validate(w.scope) != nil || state.Generation != w.scope.Generation {
		return watch.ErrConflict
	}
	if state.Status == "retired" {
		return nil
	}
	if len(state.Retired) >= maxRetiredGenerations {
		return watch.ErrLimit
	}
	next := state
	next.Status = "retired"
	next.Retired = append(append([]string{}, state.Retired...), state.Generation)
	ok, err := w.backend.compareAndSwap(ctx, state, next)
	if err != nil {
		return errors.Join(watch.ErrOutcomeUnknown, err)
	}
	if !ok {
		return watch.ErrConflict
	}
	return nil
}
