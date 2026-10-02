// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/clickhouse"
	"github.com/SYNEHQ/kelvo-go/internal/sources/mysql"
	"github.com/SYNEHQ/kelvo-go/internal/sources/postgres"
)

type scanDialect uint8

const (
	dialectClickHouse scanDialect = iota
	dialectPostgres
	dialectMySQL
)

func dialectFor(kind string) (scanDialect, error) {
	switch kind {
	case "clickhouse":
		return dialectClickHouse, nil
	case "postgres":
		return dialectPostgres, nil
	case "mysql":
		return dialectMySQL, nil
	default:
		return 0, query.NewError("UNSUPPORTED", "Source has no native federation dialect")
	}
}

func (d scanDialect) executor(config catalog.Config, limits query.Limits) (execution, error) {
	switch d {
	case dialectClickHouse:
		return clickhouse.New(config, limits)
	case dialectPostgres:
		return postgres.New(config, limits)
	case dialectMySQL:
		return mysql.New(config, limits)
	default:
		return nil, query.NewError("UNSUPPORTED", "Source has no native federation dialect")
	}
}

func (d scanDialect) tableName(table catalog.FederationTable) (string, error) {
	namespace := table.Database
	if d == dialectPostgres {
		namespace = table.Schema
	}
	qualifier, err := d.quoteIdentifier(namespace)
	if err != nil {
		return "", err
	}
	name, err := d.quoteIdentifier(table.Table)
	if err != nil {
		return "", err
	}
	return qualifier + "." + name, nil
}
