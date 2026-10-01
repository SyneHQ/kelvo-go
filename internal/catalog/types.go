// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import "errors"

// CanonicalType normalizes spelling aliases, never different database products.
func CanonicalType(kind string) string {
	switch kind {
	case "postgresql":
		return "postgres"
	case "mssql", "msql":
		return "sqlserver"
	case "mongo":
		return "mongodb"
	case "cockroach":
		return "cockroachdb"
	default:
		return kind
	}
}

// KnownType includes engines reachable through optional external adapters.
// Presence here is not a claim that a built-in driver implements the engine.
func KnownType(kind string) bool {
	switch CanonicalType(kind) {
	case "postgres", "mysql", "mariadb", "sqlite", "duckdb", "cockroachdb",
		"sqlserver", "clickhouse", "cosmosdb", "oracle", "dynamodb", "trino",
		"clickhouse_lambda", "alloydb", "presto", "athena", "hive", "h2",
		"ignite", "spanner", "db2", "exasol", "sap_hana", "sap_ase",
		"salesforce", "google_ads", "facebook_ads", "spark", "d1", "snowflake",
		"bigquery", "databricks", "redshift", "mongodb", "cassandra", "scylla",
		"elasticsearch", "csv", "parquet", "posthog", "ga4", "stripe", "redis",
		"arrow_flight", "google_sheets":
		return true
	}
	return false
}

func FileType(kind string) bool {
	switch CanonicalType(kind) {
	case "csv", "parquet", "duckdb", "sqlite":
		return true
	}
	return false
}

func NativeType(kind string) bool {
	switch CanonicalType(kind) {
	case "clickhouse", "databricks", "snowflake", "d1", "mongodb", "sqlserver",
		"oracle", "postgres", "mysql", "mariadb", "cockroachdb", "alloydb",
		"redshift", "bigquery", "elasticsearch", "trino", "presto", "arrow_flight",
		"exasol", "spanner", "ignite", "athena", "dynamodb", "cosmosdb":
		return true
	}
	return false
}

func (s Source) ValidateAdapter() error {
	if !KnownType(s.Type) {
		return errors.New("adapter engine is unknown")
	}
	if s.Adapter != "dbapi" && s.Adapter != "flightsql" {
		return errors.New("adapter must be dbapi or flightsql")
	}
	if s.URLEnv == "" || s.TokenEnv == "" {
		return errors.New("adapter requires url_env and token_env")
	}
	if s.Path != "" || s.DSNEnv != "" || s.UsernameEnv != "" || s.PasswordEnv != "" {
		return errors.New("adapter credentials must belong only to its endpoint")
	}
	return nil
}
