// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
)

type catalogCustomDriver struct{}

func (catalogCustomDriver) Validate(source federationapi.Source, table federationapi.Table) error {
	if source.Options["reject"] != "" {
		return errors.New("private provider diagnostic")
	}
	if source.Options["panic"] != "" {
		panic("private panic diagnostic")
	}
	return nil
}
func (catalogCustomDriver) Open(context.Context, federationapi.Source, federationapi.Table, federationapi.Limits) (federationapi.Relation, error) {
	panic("catalog validation must not open a source")
}
func init() { federationapi.MustRegister("catalog_fixture", catalogCustomDriver{}) }

func expandedSource(kind string) Source {
	table := FederationTable{Name: "orders", Database: "analytics", Schema: "reporting", Table: "orders"}
	source := Source{ID: "warehouse", Type: kind, Federation: &FederationConfig{Tables: []FederationTable{table}}}
	switch kind {
	case "sqlserver", "oracle":
		source.DSNEnv = "KELVO_SOURCE_WAREHOUSE_DSN"
		if kind == "oracle" {
			source.Federation.Tables[0].Database = ""
		}
	case "snowflake", "databricks", "bigquery":
		source.URLEnv, source.TokenEnv = "KELVO_SOURCE_WAREHOUSE_URL", "KELVO_SOURCE_WAREHOUSE_TOKEN"
		if kind == "databricks" {
			source.Options = map[string]string{"warehouse_id": "warehouse-1"}
		}
		if kind == "bigquery" {
			source.Federation.Tables[0].Database = "my-project"
			source.Options = map[string]string{"project": "billing-project", "location": "us", "maximum_bytes_billed": "1000000"}
		}
	}
	return source
}

func TestExpandedFederationNamespacesAndCredentialIsolation(t *testing.T) {
	for _, kind := range []string{"sqlserver", "oracle", "snowflake", "databricks", "bigquery"} {
		t.Run(kind, func(t *testing.T) {
			if err := expandedSource(kind).ValidateFederation(); err != nil {
				t.Fatal(err)
			}
			for _, change := range []func(*Source){
				func(s *Source) { s.Path = "/private/source" },
				func(s *Source) { s.Adapter = "dbapi" },
				func(s *Source) { s.UsernameEnv = "KELVO_SOURCE_UNRELATED_USER" },
				func(s *Source) { s.Federation.Tables[0].Schema = "" },
				func(s *Source) { s.Federation.Tables[0].Table = "orders; SELECT 1" },
				func(s *Source) { s.Federation.Tables[0].Schema = "other.schema" },
				func(s *Source) { s.Options = map[string]string{"unsafe": "true"} },
				func(s *Source) { s.TokenEnv = "KELVO_TOKEN" },
			} {
				bad := expandedSource(kind)
				change(&bad)
				if bad.ValidateFederation() == nil {
					t.Fatal("invalid namespace, option or unrelated credentials accepted")
				}
			}
			bad := expandedSource(kind)
			if kind == "oracle" {
				bad.Federation.Tables[0].Database = "other"
			} else {
				bad.Federation.Tables[0].Database = ""
			}
			if bad.ValidateFederation() == nil {
				t.Fatal("invalid database namespace accepted")
			}
		})
	}
}

func TestExpandedFederationRequiredProviderOptions(t *testing.T) {
	for _, kind := range []string{"databricks", "bigquery"} {
		source := expandedSource(kind)
		source.Options = nil
		if source.ValidateFederation() == nil {
			t.Fatalf("%s accepted missing required provider options", kind)
		}
	}
	source := expandedSource("bigquery")
	for _, project := range []string{"my-project", "abcdef", "a12345"} {
		source.Federation.Tables[0].Database = project
		if err := source.ValidateFederation(); err != nil {
			t.Fatal(err)
		}
	}
	for _, project := range []string{"bad.project", "my_project", "project-", "x` JOIN secrets"} {
		source.Federation.Tables[0].Database = project
		if source.ValidateFederation() == nil {
			t.Fatalf("invalid BigQuery project accepted: %s", project)
		}
	}
}

func TestCustomFederationCatalogRequiresExplicitRegistrationAndTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kelvo.yml")
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	valid := "sources:\n  - id: custom\n    type: catalog_fixture\n    federation:\n      tables:\n        - name: orders\n          table: orders\n"
	write(valid)
	config, err := Load(path)
	if err != nil || len(config.Sources) != 1 || config.Sources[0].Type != "catalog_fixture" {
		t.Fatalf("registered custom source failed to load: %v", err)
	}
	for _, body := range []string{
		"sources:\n  - id: custom\n    type: catalog_fixture\n",
		strings.Replace(valid, "catalog_fixture", "unregistered_fixture", 1),
		strings.Replace(valid, "    federation:", "    dsn_env: KELVO_TOKEN\n    federation:", 1),
		strings.Replace(valid, "    federation:", "    path: /private/file\n    federation:", 1),
	} {
		write(body)
		if _, err := Load(path); err == nil {
			t.Fatal("custom catalog widened registration or credential boundary")
		}
	}
	for _, option := range []string{"reject", "panic"} {
		write(strings.Replace(valid, "    federation:", "    options: {"+option+": yes}\n    federation:", 1))
		if _, err := Load(path); err == nil || strings.Contains(err.Error(), "private") {
			t.Fatalf("custom configuration error was not sanitized: %v", err)
		}
	}
}

func TestClickHouseFederationKeepsArrowCompression(t *testing.T) {
	source := Source{ID: "warehouse", Type: "clickhouse", URLEnv: "KELVO_SOURCE_CH_URL", Options: map[string]string{"arrow_compression": "lz4_frame"}, Federation: &FederationConfig{Tables: []FederationTable{{Name: "orders", Database: "analytics", Table: "orders"}}}}
	if err := source.ValidateFederation(); err != nil {
		t.Fatal(err)
	}
	source.Options["arrow_compression"] = "zstd"
	if source.ValidateFederation() == nil {
		t.Fatal("unsupported ClickHouse codec accepted")
	}
}
