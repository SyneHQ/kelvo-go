package mongodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/watch"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var _ adapter.WatchImportSession = (*Session)(nil)

func (s *Session) ImportWatch(ctx context.Context, scope watch.Scope, resume watch.Import) error {
	if s == nil || s.revision == "" || resume.SourceRevision != s.revision {
		return watch.ErrInvalid
	}
	w, err := s.watcher(scope)
	if err != nil {
		return err
	}
	return w.importLegacy(ctx, resume)
}

func (w mongoWatch) importLegacy(ctx context.Context, resume watch.Import) error {
	if err := w.validate(ctx); err != nil {
		return err
	}
	raw, err := watch.EncodeImport(resume, w.scope, resume.SourceRevision)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	clear(raw)
	var document bson.D
	if bson.UnmarshalExtJSON(resume.ResumeToken, false, &document) != nil {
		return watch.ErrInvalid
	}
	token, err := bson.Marshal(document)
	if err != nil {
		return watch.ErrInvalid
	}
	encoded, err := encodeWatchToken(token)
	if err != nil {
		return err
	}
	// _data is the opaque server-issued token, not a filter or a shell command.
	if data, ok := bson.Raw(token).Lookup("_data").StringValueOK(); !ok || data == "" {
		return watch.ErrInvalid
	}
	previous, err := w.backend.load(ctx, w.scope)
	exists := err == nil
	if err != nil && !errors.Is(err, watch.ErrUninitialized) {
		return err
	}
	exact := func(state watchState) bool {
		return state.validate(w.scope) == nil && state.Status == "active" && state.Generation == w.scope.Generation && state.ImportSHA256 == digest
	}
	if exists {
		if previous.validate(w.scope) != nil {
			return watch.ErrConflict
		}
		if previous.Status == "active" {
			if exact(previous) {
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
	// The source must accept the exact legacy token. An expired/foreign token
	// fails here; never silently replace it with the current cluster position.
	cursor, err := w.backend.open(ctx, w.scope, bson.Raw(token), 1000)
	if err != nil {
		return err
	}
	defer closeWatchCursor(cursor)
	if err := cursor.Err(); err != nil {
		return err
	}
	next := watchState{ID: w.scope.Key(), Generation: w.scope.Generation, Status: "active", Token: encoded, Retired: append([]string{}, previous.Retired...), ImportSHA256: digest}
	if exists {
		changed, writeErr := w.backend.compareAndSwap(ctx, previous, next)
		if writeErr == nil {
			if !changed {
				return watch.ErrConflict
			}
			return nil
		}
		if current, loadErr := w.backend.load(ctx, w.scope); loadErr == nil && exact(current) {
			return nil
		}
		return errors.Join(watch.ErrOutcomeUnknown, writeErr)
	}
	if err := w.backend.create(ctx, next); err != nil {
		if current, loadErr := w.backend.load(ctx, w.scope); loadErr == nil && exact(current) {
			return nil
		}
		return errors.Join(watch.ErrOutcomeUnknown, err)
	}
	return nil
}
