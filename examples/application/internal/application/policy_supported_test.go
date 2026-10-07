// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package application

import (
	"context"
	"errors"
	"testing"

	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/query"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

func TestUnsupportedDemoEngineRejectedByAuthorization(t *testing.T) {
	for _, kind := range []string{"postgresql", "mysql", "clickhouse", "sqlserver", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			store, policy := policyFixture(t)
			c := policy.Connections["saved-a"]
			c.Type = kind
			c.CredentialsFile = "must-not-be-opened.yaml"
			policy.Connections["saved-a"] = c
			saveYAML(t, store.Config.PolicyFile, policy)
			input := resolver.QueryRequest{Query: query.Request{Mode: "native", ConnectionID: "source_1", SQL: c.ReadSQL, Parameters: []query.Parameter{{Type: "string", Value: []byte(`"fixture"`)}}}}
			claims := delegation.Claims{AppTeam: "team-a", Subject: delegation.Subject{Kind: "user", ID: "alice"}, Sources: []delegation.Source{{Alias: "source_1", ConnectionID: "saved-a", Database: c.Database, Schema: c.Schema}}}
			if _, err := store.AuthorizeQuery(context.Background(), input, claims); !errors.Is(err, ErrConfiguration) {
				t.Fatal("unsupported demo engine passed authorization before credential lookup")
			}
		})
	}
}
