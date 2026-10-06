// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"maps"
	"strings"
	"testing"
)

func TestCloudSQLPrivateSourceBindsAccountAndNamespace(t *testing.T) {
	for _, s := range []ConnectionSpec{
		{Engine: "d1", URL: "https://api.cloudflare.com", Token: "fixture", Database: "01234567-89ab-cdef-0123-456789abcdef", Options: map[string]string{"account_id": strings.Repeat("a", 32), "database_id": "01234567-89ab-cdef-0123-456789abcdef"}},
		{Engine: "databricks", URL: "https://workspace.example", Token: "fixture", Database: "analytics", Schema: "public", Options: map[string]string{"warehouse_id": "ab123", "catalog": "analytics", "schema": "public"}},
	} {
		t.Run(s.Engine, func(t *testing.T) {
			if err := ValidateCloudSQLProcessSource(s); err != nil {
				t.Fatal(err)
			}
			for name, change := range map[string]func(*ConnectionSpec){
				"empty token":    func(s *ConnectionSpec) { s.Token = "" },
				"DSN":            func(s *ConnectionSpec) { s.DSN = "private-dsn" },
				"plaintext":      func(s *ConnectionSpec) { s.URL = "http://source.example" },
				"path":           func(s *ConnectionSpec) { s.URL += "/query" },
				"query":          func(s *ConnectionSpec) { s.URL += "?token=other" },
				"database":       func(s *ConnectionSpec) { s.Database = "other" },
				"ambient option": func(s *ConnectionSpec) { s.Options["token_env"] = "TOKEN" },
			} {
				t.Run(name, func(t *testing.T) {
					c := s
					c.Options = maps.Clone(s.Options)
					change(&c)
					if ValidateCloudSQLProcessSource(c) == nil {
						t.Fatal("unbounded cloud source accepted")
					}
				})
			}
		})
	}
}
