// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import "testing"

func TestReferenceEngineCatalogRequiresRealExecutionPath(t *testing.T) {
	// This is the audited 44-ID compatibility target. Presence in an enum alone
	// must never turn a missing implementation into a native connector.
	kinds := []string{"postgresql", "mysql", "mariadb", "sqlite", "duckdb", "cockroachdb", "sqlserver", "clickhouse", "cosmosdb", "oracle", "dynamodb", "trino", "clickhouse_lambda", "alloydb", "presto", "athena", "hive", "h2", "ignite", "spanner", "db2", "exasol", "sap_hana", "sap_ase", "salesforce", "google_ads", "facebook_ads", "spark", "d1", "snowflake", "bigquery", "databricks", "redshift", "mongodb", "cassandra", "scylla", "elasticsearch", "csv", "posthog", "ga4", "stripe", "redis", "arrow_flight", "google_sheets"}
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			if !KnownType(kind) {
				t.Fatal("reference engine missing")
			}
			text := "sources:\n  - id: source\n    type: " + kind + "\n    adapter: dbapi\n    url_env: KELVO_SOURCE_ADAPTER_URL\n    token_env: KELVO_SOURCE_ADAPTER_TOKEN\n    options:\n      remote_connection_id: reference-id\n"
			c := loadCatalog(t, text)
			if c.Sources[0].Type != CanonicalType(kind) || c.Sources[0].Adapter != "dbapi" {
				t.Fatal("source identity or adapter changed")
			}
			if !NativeType(kind) && !FileType(kind) {
				if _, err := loadCatalogError(t, "sources:\n  - id: source\n    type: "+kind+"\n"); err == nil {
					t.Fatal("missing native implementation accepted")
				}
			}
		})
	}
}

func TestAdapterRejectsCredentialsFromAnotherBoundary(t *testing.T) {
	base := "sources:\n  - id: source\n    type: db2\n    adapter: flightsql\n    url_env: KELVO_SOURCE_ADAPTER_URL\n    token_env: KELVO_SOURCE_ADAPTER_TOKEN\n"
	for _, field := range []string{"path: /etc/passwd", "dsn_env: KELVO_SOURCE_DSN", "username_env: KELVO_SOURCE_USER", "password_env: KELVO_SOURCE_PASSWORD"} {
		if _, err := loadCatalogError(t, base+"    "+field+"\n"); err == nil {
			t.Fatalf("accepted unused credential %s", field)
		}
	}
}
