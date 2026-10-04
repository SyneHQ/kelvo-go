// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import "testing"

func flightFederationSource() Source {
	return Source{
		ID: "warehouse", Type: "arrow_flight",
		URLEnv: "KELVO_SOURCE_FLIGHT_URL", TokenEnv: "KELVO_SOURCE_FLIGHT_TOKEN",
		Options:    map[string]string{"protocol": "flightsql", "federation_dialect": "ansi"},
		Federation: &FederationConfig{Tables: []FederationTable{{Name: "orders", Schema: "reporting", Table: "orders"}}},
	}
}

func TestFlightFederationCatalogRequiresExplicitProfile(t *testing.T) {
	// Validation is static: unset or unusable environment values cannot cause
	// a credential read, network connection or dependence on the current host.
	t.Setenv("KELVO_SOURCE_FLIGHT_URL", "not a URL")
	t.Setenv("KELVO_SOURCE_FLIGHT_TOKEN", "")
	if err := flightFederationSource().ValidateFederation(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*Source)
	}{
		{"missing_profile", func(s *Source) { delete(s.Options, "federation_dialect") }},
		{"other_profile", func(s *Source) { s.Options["federation_dialect"] = "mysql" }},
		{"other_protocol", func(s *Source) { s.Options["protocol"] = "flight" }},
		{"extra_option", func(s *Source) { s.Options["insecure"] = "true" }},
		{"url_missing", func(s *Source) { s.URLEnv = "" }},
		{"token_missing", func(s *Source) { s.TokenEnv = "" }},
		{"unrelated_token", func(s *Source) { s.TokenEnv = "KELVO_TOKEN" }},
		{"dsn", func(s *Source) { s.DSNEnv = "KELVO_SOURCE_OTHER_DSN" }},
		{"username", func(s *Source) { s.UsernameEnv = "KELVO_SOURCE_OTHER_USER" }},
		{"password", func(s *Source) { s.PasswordEnv = "KELVO_SOURCE_OTHER_PASSWORD" }},
		{"adapter", func(s *Source) { s.Adapter = "other" }},
		{"path", func(s *Source) { s.Path = "/private/data" }},
		{"database", func(s *Source) { s.Federation.Tables[0].Database = "other" }},
		{"schema_missing", func(s *Source) { s.Federation.Tables[0].Schema = "" }},
		{"schema_expression", func(s *Source) { s.Federation.Tables[0].Schema = "reporting.other" }},
		{"table_expression", func(s *Source) { s.Federation.Tables[0].Table = "orders; SELECT 1" }},
		{"duplicate_alias", func(s *Source) { s.Federation.Tables = append(s.Federation.Tables, s.Federation.Tables[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := flightFederationSource()
			tc.edit(&source)
			if err := source.ValidateFederation(); err == nil {
				t.Fatal("accepted unsupported Flight SQL configuration")
			}
		})
	}
}
