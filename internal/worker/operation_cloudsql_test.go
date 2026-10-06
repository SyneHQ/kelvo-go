// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestOperationCloudSQLResolvesSignedNamespaceOnDemand(t *testing.T) {
	for _, engine := range []string{"d1", "databricks"} {
		t.Run(engine, func(t *testing.T) {
			record, request, response := operationResolverFixture(t)
			request.Kind, record.Kind = operations.ConnectionTest, operations.ConnectionTest
			request.Spec = operations.Spec{}
			response.Source = catalog.Source{ID: "source_1", Type: engine, URLEnv: "KELVO_SOURCE_REQUEST_0_URL", TokenEnv: "KELVO_SOURCE_REQUEST_0_TOKEN"}
			response.Secrets = map[string]string{response.Source.TokenEnv: "current-fixture-token"}
			if engine == "d1" {
				request.Connection.Database = "01234567-89ab-cdef-0123-456789abcdef"
				request.Connection.Schema = ""
				response.Source.Options = map[string]string{"account_id": strings.Repeat("a", 32), "database_id": request.Connection.Database}
				response.Secrets[response.Source.URLEnv] = "https://api.cloudflare.com"
			} else {
				response.Source.Options = map[string]string{"warehouse_id": "ab123", "catalog": request.Connection.Database, "schema": request.Connection.Schema}
				response.Secrets[response.Source.URLEnv] = "https://workspace.example"
			}
			record.RequestSHA256, _ = operations.Digest(request)
			response.RequestSHA256 = record.RequestSHA256
			resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
			})
			resolver.url += "/internal/kelvo/resolve"
			e := &Executor{Limits: query.DefaultLimits(), connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
			input, err := e.ResolveOperationSource(context.Background(), record, request)
			if err != nil || input.Source.Token != "current-fixture-token" || input.Source.Database != request.Connection.Database {
				t.Fatal("cloud source lost private binding", err)
			}
			if engine == "d1" {
				response.Source.Options["database_id"] = "11111111-1111-1111-1111-111111111111"
			} else {
				response.Source.Options["catalog"] = "other"
			}
			if _, err = e.ResolveOperationSource(context.Background(), record, request); err == nil {
				t.Fatal("cross-namespace cloud source accepted")
			}
		})
	}
}
