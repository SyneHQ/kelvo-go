// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package native selects a source-specific executor without widening its catalog.
package native

import (
	"context"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/athena"
	"github.com/SYNEHQ/kelvo-go/internal/sources/bigquery"
	"github.com/SYNEHQ/kelvo-go/internal/sources/clickhouse"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cosmosdb"
	"github.com/SYNEHQ/kelvo-go/internal/sources/d1"
	"github.com/SYNEHQ/kelvo-go/internal/sources/databricks"
	"github.com/SYNEHQ/kelvo-go/internal/sources/dbapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/dynamodb"
	"github.com/SYNEHQ/kelvo-go/internal/sources/elasticsearch"
	"github.com/SYNEHQ/kelvo-go/internal/sources/exasol"
	"github.com/SYNEHQ/kelvo-go/internal/sources/flightsql"
	"github.com/SYNEHQ/kelvo-go/internal/sources/ignite"
	"github.com/SYNEHQ/kelvo-go/internal/sources/mongodb"
	"github.com/SYNEHQ/kelvo-go/internal/sources/mysql"
	"github.com/SYNEHQ/kelvo-go/internal/sources/oracle"
	"github.com/SYNEHQ/kelvo-go/internal/sources/postgres"
	"github.com/SYNEHQ/kelvo-go/internal/sources/snowflake"
	"github.com/SYNEHQ/kelvo-go/internal/sources/spanner"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlserver"
	"github.com/SYNEHQ/kelvo-go/internal/sources/trino"
)

type Engine interface {
	query.Executor
	Close() error
}

// New chooses exactly one registered source. Request syntax must match that
// source before any driver constructor or credential lookup runs.
func New(config catalog.Config, limits query.Limits, request query.Request) (Engine, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	if err := query.ValidateRequest(request); err != nil {
		return nil, err
	}
	if request.Mode != "native" {
		return nil, query.NewError("INVALID_ARGUMENT", "Native source requires native mode")
	}
	var source catalog.Source
	matches := 0
	for _, candidate := range config.Sources {
		if candidate.ID == request.ConnectionID {
			source = candidate
			matches++
		}
	}
	if matches != 1 {
		return nil, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	source.Type = catalog.CanonicalType(source.Type)
	if err := validateSyntax(source.Type, request); err != nil {
		return nil, err
	}
	for _, name := range []string{source.DSNEnv, source.URLEnv, source.UsernameEnv, source.PasswordEnv, source.TokenEnv} {
		if name != "" {
			if err := catalog.ValidateEnvironment(name); err != nil {
				return nil, query.NewError("CONFIGURATION_ERROR", "Source environment reference is not permitted")
			}
		}
	}
	selected := catalog.Config{Sources: []catalog.Source{source}}
	var engine Engine
	var err error
	if source.Adapter != "" {
		if err := source.ValidateAdapter(); err != nil {
			return nil, query.NewError("CONFIGURATION_ERROR", "Invalid source adapter configuration")
		}
		switch source.Adapter {
		case "dbapi":
			engine, err = dbapi.New(selected, limits)
		case "flightsql":
			bridge := source
			bridge.Type, bridge.Adapter = "arrow_flight", ""
			bridge.Options = map[string]string{"protocol": "flightsql"}
			if len(source.Options) != 0 {
				return nil, query.NewError("CONFIGURATION_ERROR", "Flight SQL adapter does not accept provider options")
			}
			engine, err = flightsql.New(catalog.Config{Sources: []catalog.Source{bridge}}, limits)
		}
		if err != nil {
			return nil, query.PublicError(err)
		}
		return &boundEngine{Engine: engine, sourceID: source.ID, sourceType: source.Type, adapter: source.Adapter, timeout: limits.Timeout}, nil
	}
	switch source.Type {
	case "clickhouse":
		engine, err = clickhouse.New(selected, limits)
	case "databricks":
		engine, err = databricks.New(selected, limits)
	case "snowflake":
		engine, err = snowflake.New(selected, limits)
	case "d1":
		engine, err = d1.New(selected, limits)
	case "mongodb":
		engine, err = mongodb.New(selected, limits)
	case "sqlserver":
		engine, err = sqlserver.New(selected, limits)
	case "oracle":
		engine, err = oracle.New(selected, limits)
	case "postgres":
		engine, err = postgres.New(selected, limits)
	case "cockroachdb":
		engine, err = postgres.NewCockroachDB(selected, limits)
	case "alloydb":
		engine, err = postgres.NewAlloyDB(selected, limits)
	case "redshift":
		engine, err = postgres.NewRedshift(selected, limits)
	case "mysql":
		engine, err = mysql.New(selected, limits)
	case "mariadb":
		engine, err = mysql.NewMariaDB(selected, limits)
	case "bigquery":
		engine, err = bigquery.New(selected, limits)
	case "elasticsearch":
		engine, err = elasticsearch.New(selected, limits)
	case "exasol":
		engine, err = exasol.New(selected, limits)
	case "spanner":
		engine, err = spanner.New(selected, limits)
	case "ignite":
		engine, err = ignite.New(selected, limits)
	case "athena":
		engine, err = athena.New(selected, limits)
	case "dynamodb":
		engine, err = dynamodb.New(selected, limits)
	case "cosmosdb":
		engine, err = cosmosdb.New(selected, limits)
	case "trino", "presto":
		engine, err = trino.New(selected, limits)
	case "arrow_flight":
		engine, err = flightsql.New(selected, limits)
	default:
		return nil, query.NewError("UNSUPPORTED", "This source does not support native execution")
	}
	if err != nil {
		return nil, query.PublicError(err)
	}
	return &boundEngine{Engine: engine, sourceID: source.ID, sourceType: source.Type, timeout: limits.Timeout}, nil
}

type boundEngine struct {
	Engine
	sourceID, sourceType string
	adapter              string
	timeout              time.Duration
}

func (e *boundEngine) Execute(ctx context.Context, request query.Request, sink query.Sink) (query.Stats, error) {
	if err := query.ValidateRequest(request); err != nil {
		return query.Stats{}, err
	}
	if request.Mode != "native" || request.ConnectionID != e.sourceID || sink == nil {
		return query.Stats{}, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if err := validateSyntax(e.sourceType, request); err != nil {
		return query.Stats{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	stats, err := e.Engine.Execute(ctx, request, sink)
	if e.adapter != "" {
		stats.Backend = e.sourceType + "/" + e.adapter
	}
	return stats, err
}

func validateSyntax(sourceType string, request query.Request) error {
	if sourceType == "mongodb" {
		if len(request.Parameters) != 0 {
			return query.NewError("UNSUPPORTED", "MongoDB SQL parameters are not supported")
		}
	} else if request.Mongo != nil {
		return query.NewError("INVALID_ARGUMENT", "MongoDB aggregation requires a MongoDB source")
	}
	return nil
}
