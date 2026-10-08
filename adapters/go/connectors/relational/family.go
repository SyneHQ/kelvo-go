// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package relational

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/migrations"
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// PGFamily reuses the verified PostgreSQL wire connection, while preserving
// vendor identity and withholding PG-only watcher, CDC and advisory-lock claims.
type PGFamily struct{ Engine string }

func (d PGFamily) Capabilities() operations.Capabilities {
	c := operations.Capabilities{Version: operations.Version, Engine: d.Engine}
	if d.Engine != "cockroachdb" && d.Engine != "redshift" && d.Engine != "alloydb" {
		return c
	}
	for _, capability := range NewPostgreSQL().Capabilities().Operations {
		switch capability.Kind {
		case operations.QueryRead, operations.ConnectionTest, operations.MetadataInspect:
			c.Operations = append(c.Operations, capability)
		case operations.StatementExecute:
			capability.Roles = false
			if d.Engine != "alloydb" {
				capability.Transactions = []operations.TransactionMode{operations.TransactionAutocommit}
				capability.IsolationLevels = nil
			}
			c.Operations = append(c.Operations, capability)
		case operations.MigrationStatus, operations.MigrationApply:
			if d.Engine != "alloydb" {
				c.Operations = append(c.Operations, capability)
			}
		}
	}
	return c
}
func (d PGFamily) Open(ctx context.Context, c adapter.Connection) (adapter.Session, error) {
	if c.Engine != d.Engine || len(d.Capabilities().Operations) == 0 || c.DialContext != nil || c.DialCancellation != nil {
		return nil, adapter.ErrInvalid
	}
	c.Engine = "postgresql"
	base, err := NewPostgreSQL().Open(ctx, c)
	if err != nil {
		return nil, err
	}
	return &familySession{Session: base.(*Session), engine: d.Engine}, nil
}

type familySession struct {
	*Session
	engine string
}

func (s *familySession) Execute(ctx context.Context, change adapter.Change) (adapter.ChangeResult, error) {
	if change.Role != "" || s.engine != "alloydb" && (change.Transaction || change.Isolation != "") {
		return adapter.ChangeResult{Outcome: "failed"}, adapter.ErrUnsupported
	}
	return s.Session.Execute(ctx, change)
}
func (s *familySession) MigrationStatus(ctx context.Context) (migration.State, error) {
	if s.engine == "alloydb" {
		return migration.State{}, adapter.ErrUnsupported
	}
	return (migrations.PersistentSQL{DB: s.Pool, Engine: s.engine, Database: s.database, Schema: s.schema}).Status(ctx)
}
func (s *familySession) ApplyMigration(ctx context.Context, plan migration.Plan) (migration.Result, error) {
	if s.engine == "alloydb" {
		return migration.Result{}, adapter.ErrUnsupported
	}
	return (migrations.PersistentSQL{DB: s.Pool, Engine: s.engine, Database: s.database, Schema: s.schema}).Apply(ctx, plan)
}
