// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestOperationSaaSResolverBindsDemandCredentials(t *testing.T) {
	accounts := map[string]string{"stripe": "acct_fixture", "ga4": "1234", "google_ads": "1234", "facebook_ads": "act_1234", "salesforce": "001123456789012"}
	for engine, account := range accounts {
		t.Run(engine, func(t *testing.T) {
			record, request, response := operationResolverFixture(t)
			request.Kind, record.Kind = operations.ConnectionTest, operations.ConnectionTest
			request.Connection.Database, request.Connection.Schema = account, ""
			request.Spec = operations.Spec{}
			record.RequestSHA256, _ = operations.Digest(request)
			response.RequestSHA256 = record.RequestSHA256
			response.Source = catalog.Source{ID: "source_1", Type: engine, TokenEnv: "KELVO_SOURCE_REQUEST_0_TOKEN"}
			response.Secrets = map[string]string{response.Source.TokenEnv: "current-fixture-token"}
			if engine == "salesforce" {
				response.Source.URLEnv = "KELVO_SOURCE_REQUEST_0_URL"
				response.Secrets[response.Source.URLEnv] = "https://fixture.my.salesforce.com"
			}
			if engine == "google_ads" || engine == "salesforce" {
				response.Source.UsernameEnv = "KELVO_SOURCE_REQUEST_0_USERNAME"
				response.Secrets[response.Source.UsernameEnv] = "fixture-client"
			}
			if engine == "google_ads" {
				response.Source.Options = map[string]string{"login_customer_id": "4321"}
			}
			resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
			})
			resolver.url += "/internal/kelvo/resolve"
			e := &Executor{Limits: query.DefaultLimits(), connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
			input, err := e.ResolveOperationSource(context.Background(), record, request)
			if err != nil || input.Source.Token != "current-fixture-token" || input.Source.Database != account || input.Source.URL != response.Secrets[response.Source.URLEnv] || input.Source.Username != response.Secrets[response.Source.UsernameEnv] {
				t.Fatal("SaaS source lost signed account or current private credentials", err)
			}
			valid := response
			for name, change := range map[string]func(*operationResolution){
				"ambient token":   func(r *operationResolution) { r.Source.TokenEnv = "PROVIDER_TOKEN" },
				"extra secret":    func(r *operationResolution) { r.Secrets["UNREFERENCED"] = "value" },
				"missing token":   func(r *operationResolution) { delete(r.Secrets, r.Source.TokenEnv) },
				"unscoped origin": func(r *operationResolution) { r.Source.URLEnv = "UNSCOPED_URL" },
				"path":            func(r *operationResolution) { r.Source.Path = "/private/source" },
				"adapter":         func(r *operationResolution) { r.Source.Adapter = "other" },
				"DSN":             func(r *operationResolution) { r.Source.DSNEnv = "KELVO_SOURCE_REQUEST_0_DSN" },
				"federation":      func(r *operationResolution) { r.Source.Federation = &catalog.FederationConfig{} },
				"TLS override":    func(r *operationResolution) { r.Source.Options = map[string]string{"tls_server_name": "other"} },
			} {
				t.Run(name, func(t *testing.T) {
					response = valid
					response.Secrets = maps.Clone(valid.Secrets)
					change(&response)
					if _, err := e.ResolveOperationSource(context.Background(), record, request); err == nil {
						t.Fatal("unbounded SaaS source accepted")
					}
				})
			}
			response = valid
			request.Kind = operations.StatementExecute
			if validateOperationSaaSCatalog(response, request) == nil {
				t.Fatal("SaaS source granted mutation authority")
			}
		})
	}
}
