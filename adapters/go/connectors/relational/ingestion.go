// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package relational

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/adapter"
	pg "github.com/SYNEHQ/kelvo-go/adapters/go/connectors/ingestion"
	"github.com/SYNEHQ/kelvo-go/ingestion"
)

var _ adapter.IngestionSession = (*Session)(nil)

func (s *Session) ingestionStore(scope ingestion.Scope) (pg.Store, error) {
	if s == nil || s.Session == nil || s.Pool == nil || s.Engine != "postgresql" {
		return pg.Store{}, adapter.ErrUnsupported
	}
	if scope.Validate() != nil || scope.Database != s.database || scope.Schema != s.schema {
		return pg.Store{}, adapter.ErrInvalid
	}
	return pg.Store{DB: s.Pool, Scope: scope}, nil
}

func (s *Session) InstallIngestion(ctx context.Context, scope ingestion.Scope) error {
	store, err := s.ingestionStore(scope)
	if err != nil {
		return err
	}
	return store.Install(ctx)
}

func (s *Session) IngestionState(ctx context.Context, scope ingestion.Scope) (ingestion.State, error) {
	store, err := s.ingestionStore(scope)
	if err != nil {
		return ingestion.State{}, err
	}
	return store.State(ctx)
}

func (s *Session) CommitIngestion(ctx context.Context, scope ingestion.Scope, batch ingestion.Batch) (ingestion.Receipt, error) {
	store, err := s.ingestionStore(scope)
	if err != nil {
		return ingestion.Receipt{}, err
	}
	return store.Commit(ctx, batch)
}
