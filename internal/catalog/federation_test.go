// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFederationSourceContract(t *testing.T) {
	good := Source{ID: "warehouse", Type: "clickhouse", URLEnv: "KELVO_SOURCE_WAREHOUSE_URL", Federation: &FederationConfig{Tables: []FederationTable{{Name: "events", Database: "analytics", Table: "events"}}, MaxScanRows: 10000000, MaxScanBytes: 1 << 30}}
	if err := good.ValidateFederation(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Source){
		func(s *Source) { s.Type = "mongodb" },
		func(s *Source) { s.Adapter = "flightsql" },
		func(s *Source) { s.Federation.Tables = nil },
		func(s *Source) { s.Federation.Tables = append(s.Federation.Tables, s.Federation.Tables[0]) },
		func(s *Source) { s.Federation.Tables[0].Database = "analytics.other" },
		func(s *Source) { s.Federation.Tables[0].Table = "events; DROP TABLE events" },
		func(s *Source) { s.Federation.MaxScanRows = -1 },
		func(s *Source) { s.Federation.MaxScanBytes = 1 },
	} {
		bad := good
		f := *good.Federation
		f.Tables = append([]FederationTable(nil), f.Tables...)
		bad.Federation = &f
		change(&bad)
		if err := bad.ValidateFederation(); err == nil {
			t.Fatal("accepted invalid federation configuration", bad)
		}
	}
}

func TestRelationalFederationNamespacesAndCredentials(t *testing.T) {
	for _, kind := range []string{"postgres", "mysql"} {
		t.Run(kind, func(t *testing.T) {
			table := FederationTable{Name: "orders", Table: "orders"}
			if kind == "postgres" {
				table.Schema = "reporting"
			} else {
				table.Database = "analytics"
			}
			source := Source{ID: "db", Type: kind, DSNEnv: "KELVO_SOURCE_DB_DSN", Federation: &FederationConfig{Tables: []FederationTable{table}}}
			if err := source.ValidateFederation(); err != nil {
				t.Fatal(err)
			}
			for _, mutate := range []func(*Source){
				func(s *Source) { s.Federation.Tables[0].Schema = "other.schema" },
				func(s *Source) { s.Federation.Tables[0].Database = "other.database" },
				func(s *Source) {
					s.Federation.Tables[0].Schema = "public"
					s.Federation.Tables[0].Database = "analytics"
				},
				func(s *Source) { s.Federation.Tables[0].Schema = ""; s.Federation.Tables[0].Database = "" },
				func(s *Source) { s.DSNEnv = "" },
				func(s *Source) { s.TokenEnv = "KELVO_SOURCE_OTHER_TOKEN" },
				func(s *Source) { s.URLEnv = "KELVO_SOURCE_OTHER_URL" },
				func(s *Source) { s.Options = map[string]string{"search_path": "other"} },
			} {
				bad := source
				bad.Federation = &FederationConfig{Tables: []FederationTable{table}}
				mutate(&bad)
				if bad.ValidateFederation() == nil {
					t.Fatal("accepted ambiguous namespace or credentials")
				}
			}
		})
	}
}

func TestLoadPostgreSQLFederationSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kelvo.yml")
	data := "sources:\n  - id: pg\n    type: postgresql\n    dsn_env: KELVO_SOURCE_PG_DSN\n    federation:\n      tables:\n        - name: orders\n          schema: reporting\n          table: orders\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	source := config.Sources[0]
	if source.Type != "postgres" || source.Federation.Tables[0].Schema != "reporting" || source.Federation.Tables[0].Database != "" {
		t.Fatal("PostgreSQL namespace was not preserved")
	}
}
