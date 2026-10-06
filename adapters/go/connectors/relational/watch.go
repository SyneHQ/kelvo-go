// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package relational

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/watchers"
	"github.com/SYNEHQ/kelvo-go/watch"
)

var _ adapter.WatchSession = (*Session)(nil)

type watchStore interface {
	Install(context.Context) error
	Read(context.Context, int, int, int64) (watch.Batch, error)
	Ack(context.Context, watch.Checkpoint, string) error
	Remove(context.Context) error
}

func (s *Session) watchStore(scope watch.Scope) (watchStore, error) {
	if s == nil || s.Session == nil || s.Pool == nil {
		return nil, adapter.ErrUnsupported
	}
	if scope.Validate() != nil || scope.Database != s.database || scope.Schema != s.schema {
		return nil, adapter.ErrInvalid
	}
	switch s.Engine {
	case "postgresql":
		return watchers.PostgreSQL{DB: s.Pool, Scope: scope}, nil
	case "mysql", "mariadb":
		return watchers.MySQL{DB: s.Pool, Scope: scope}, nil
	default:
		return nil, adapter.ErrUnsupported
	}
}

func (s *Session) InstallWatch(ctx context.Context, scope watch.Scope) error {
	store, err := s.watchStore(scope)
	if err != nil {
		return err
	}
	return store.Install(ctx)
}

func (s *Session) ReadWatch(ctx context.Context, scope watch.Scope, maximum, waitMS int, maxBytes int64) (watch.Batch, error) {
	store, err := s.watchStore(scope)
	if err != nil {
		return watch.Batch{}, err
	}
	return store.Read(ctx, maximum, waitMS, maxBytes)
}

func (s *Session) AckWatch(ctx context.Context, scope watch.Scope, checkpoint watch.Checkpoint, sinkReceiptSHA256 string) error {
	store, err := s.watchStore(scope)
	if err != nil {
		return err
	}
	return store.Ack(ctx, checkpoint, sinkReceiptSHA256)
}

func (s *Session) RemoveWatch(ctx context.Context, scope watch.Scope) error {
	store, err := s.watchStore(scope)
	if err != nil {
		return err
	}
	return store.Remove(ctx)
}
