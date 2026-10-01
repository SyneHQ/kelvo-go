//go:build !duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type Engine struct{}

func New(catalog.Config, query.Limits) (*Engine, error) {
	return nil, query.NewError("UNAVAILABLE", "DuckDB Arrow support requires the duckdb_arrow build tag")
}
func (*Engine) Execute(context.Context, query.Request, query.Sink) (query.Stats, error) {
	return query.Stats{}, query.NewError("UNAVAILABLE", "DuckDB Arrow support requires the duckdb_arrow build tag")
}
