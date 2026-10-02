// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/bigquery"
	"github.com/SYNEHQ/kelvo-go/internal/sources/clickhouse"
	"github.com/SYNEHQ/kelvo-go/internal/sources/databricks"
	"github.com/SYNEHQ/kelvo-go/internal/sources/mysql"
	"github.com/SYNEHQ/kelvo-go/internal/sources/oracle"
	"github.com/SYNEHQ/kelvo-go/internal/sources/postgres"
	"github.com/SYNEHQ/kelvo-go/internal/sources/snowflake"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlserver"
)

type scanDialect uint8

const (
	dialectClickHouse scanDialect = iota
	dialectPostgres
	dialectMySQL
	dialectSQLServer
	dialectOracle
	dialectSnowflake
	dialectDatabricks
	dialectBigQuery
)

func dialectFor(kind string) (scanDialect, error) {
	switch kind {
	case "clickhouse":
		return dialectClickHouse, nil
	case "postgres":
		return dialectPostgres, nil
	case "mysql":
		return dialectMySQL, nil
	case "sqlserver":
		return dialectSQLServer, nil
	case "oracle":
		return dialectOracle, nil
	case "snowflake":
		return dialectSnowflake, nil
	case "databricks":
		return dialectDatabricks, nil
	case "bigquery":
		return dialectBigQuery, nil
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
	case dialectSQLServer:
		return sqlserver.New(config, limits)
	case dialectOracle:
		return oracle.New(config, limits)
	case dialectSnowflake:
		return withSourceWire(snowflake.New(config, limits))
	case dialectDatabricks:
		return withSourceWire(databricks.New(config, limits))
	case dialectBigQuery:
		return withSourceWire(bigquery.New(config, limits))
	default:
		return nil, query.NewError("UNSUPPORTED", "Source has no native federation dialect")
	}
}

func (d scanDialect) tableName(table catalog.FederationTable) (string, error) {
	if d == dialectBigQuery {
		// GoogleSQL quotes the complete project.dataset.table path; the
		// catalog validates each part separately, including project hyphens.
		return d.quoteIdentifier(table.Database + "." + table.Schema + "." + table.Table)
	}
	namespace := table.Database
	if d == dialectPostgres || d == dialectOracle {
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
	if d == dialectSQLServer || d == dialectSnowflake || d == dialectDatabricks {
		schema, err := d.quoteIdentifier(table.Schema)
		if err != nil {
			return "", err
		}
		return qualifier + "." + schema + "." + name, nil
	}
	return qualifier + "." + name, nil
}

func (d scanDialect) describeSQL(remoteName string) string {
	switch d {
	case dialectSQLServer:
		return "SELECT TOP (0) * FROM " + remoteName
	case dialectOracle:
		return "SELECT * FROM " + remoteName + " WHERE 1 = 0"
	default:
		return "SELECT * FROM " + remoteName + " LIMIT 0"
	}
}
