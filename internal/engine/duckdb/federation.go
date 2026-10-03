//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"database/sql/driver"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/federation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type federationBindings struct {
	tables     []*federation.Table
	factories  []*duckbridge.Factory
	identities [][2]string
}

// Close runs after DuckDB has released all bound plans, connection and database.
func (b *federationBindings) Close() {
	for _, f := range b.factories {
		f.Close()
	}
	for _, t := range b.tables {
		_ = t.Close()
	}
}

func (b *federationBindings) callbackError(fallback error) error {
	for _, f := range b.factories {
		if err := f.Err(); err != nil {
			return err
		}
	}
	return fallback
}

func (b *federationBindings) attach(ctx context.Context, raw any, sources []catalog.Source, limits query.Limits) error {
	// Bound catalog discovery and outcome metadata before resolving credentials
	// or making any source request; per-source limits alone are insufficient.
	tables := 0
	for _, source := range sources {
		if source.Federation != nil {
			for _, table := range source.Federation.Tables {
				if _, _, allowed := access.Lookup(ctx, source.ID, table.Name); allowed {
					tables++
				}
			}
		}
	}
	if tables > 32 {
		return query.NewError("RESOURCE_EXHAUSTED", "A query may expose at most 32 custom federation tables")
	}
	ctx = federation.WithScanBudget(ctx, min(limits.Threads, 4))
	conn, ok := raw.(driver.Conn)
	if !ok {
		return query.NewError("CONFIGURATION_ERROR", "DuckDB native connection is unavailable")
	}
	for _, source := range sources {
		if source.Federation == nil {
			continue
		}
		if err := source.ValidateFederation(); err != nil {
			return query.NewError("CONFIGURATION_ERROR", "Invalid custom federation source")
		}
		if !duckbridge.Available() {
			return query.NewError("UNSUPPORTED", "Custom federation requires the pinned DuckDB bridge build")
		}
		exec, ok := raw.(driver.ExecerContext)
		if !ok {
			return query.NewError("CONFIGURATION_ERROR", "Trusted federation setup is unavailable")
		}
		if _, err := exec.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdentifier(source.ID), nil); err != nil {
			return err
		}
		for _, selected := range source.Federation.Tables {
			if _, _, allowed := access.Lookup(ctx, source.ID, selected.Name); !allowed {
				continue
			}
			table, err := federation.New(ctx, source, selected, limits)
			if err != nil {
				return err
			}
			b.tables = append(b.tables, table)
			b.identities = append(b.identities, [2]string{source.ID, selected.Name})
			factory, err := duckbridge.New(ctx, table.Schema(), table.Scan)
			if err != nil {
				return err
			}
			b.factories = append(b.factories, factory)
			if err := factory.Register(conn, source.ID, selected.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *federationBindings) stats() []query.FederationScan {
	var out []query.FederationScan
	for i, table := range b.tables {
		s := table.Stats()
		out = append(out, query.FederationScan{Source: b.identities[i][0], Table: b.identities[i][1], Scans: s.Scans, Rows: s.Rows, Bytes: s.Bytes, Batches: s.Batches, SourceWireBytes: s.SourceWireBytes})
	}
	return out
}
