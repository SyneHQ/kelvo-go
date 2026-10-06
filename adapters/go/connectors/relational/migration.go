// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package relational

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/migrations"
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
)

var _ adapter.MigrationSession = (*Session)(nil)

func (s *Session) MigrationStatus(ctx context.Context) (migration.State, error) {
	if s == nil || s.Session == nil || s.Pool == nil {
		return migration.State{}, adapter.ErrUnsupported
	}
	if s.Engine == "postgresql" {
		return (migrations.PostgreSQL{DB: s.Pool, Database: s.database, Schema: s.schema}).Status(ctx)
	}
	return (migrations.Relational{DB: s.Pool, Engine: s.Engine, Database: s.database, Schema: s.schema}).Status(ctx)
}
func (s *Session) ApplyMigration(ctx context.Context, plan migration.Plan) (migration.Result, error) {
	result := migration.Result{Version: migration.Version, From: plan.Expected, To: plan.Expected, Effect: operations.EffectNone}
	if s == nil || s.Session == nil || s.Pool == nil || ctx == nil {
		return result, adapter.ErrUnsupported
	}
	if s.Engine == "postgresql" {
		return (migrations.PostgreSQL{DB: s.Pool, Database: s.database, Schema: s.schema}).Apply(ctx, plan)
	}
	pool := s.Pool
	if s.Engine == "mysql" || s.Engine == "mariadb" {
		if s.openMigrationPool == nil {
			return result, adapter.ErrUnsupported
		}
		var err error
		pool, err = s.openMigrationPool(ctx)
		if err != nil {
			return result, err
		}
		defer pool.Close()
	}
	return (migrations.Relational{DB: pool, Engine: s.Engine, Database: s.database, Schema: s.schema}).Apply(ctx, plan)
}
