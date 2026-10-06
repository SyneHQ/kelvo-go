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

func TestOperationSheetsResolverBindsPrivateSheet(t *testing.T) {
	record, request, response := operationResolverFixture(t)
	request.Kind, record.Kind = operations.ConnectionTest, operations.ConnectionTest
	request.Connection.Schema, request.Spec = "", operations.Spec{}
	record.RequestSHA256, _ = operations.Digest(request)
	response.RequestSHA256 = record.RequestSHA256
	response.Source = catalog.Source{ID: "source_1", Type: "google_sheets", TokenEnv: "KELVO_SOURCE_REQUEST_0_TOKEN", Options: map[string]string{"spreadsheet_id": "fixture_sheet_12345678901234567890"}}
	response.Secrets = map[string]string{response.Source.TokenEnv: "fixture-private-token"}
	resolver := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})
	resolver.url += "/internal/kelvo/resolve"
	e := &Executor{Limits: query.DefaultLimits(), connectionResolvers: map[string]*ConnectionResolver{"gateway": resolver}}
	input, err := e.ResolveOperationSource(context.Background(), record, request)
	if err != nil || input.Source.Token != "fixture-private-token" || input.Source.Options["spreadsheet_id"] != response.Source.Options["spreadsheet_id"] {
		t.Fatal("sheet lost private credential or selected sheet", err)
	}
	valid := response
	for name, change := range map[string]func(*operationResolution){
		"ambient token": func(r *operationResolution) { r.Source.TokenEnv = "GOOGLE_TOKEN" },
		"extra secret":  func(r *operationResolution) { r.Secrets["UNREFERENCED"] = "value" },
		"missing token": func(r *operationResolution) { delete(r.Secrets, r.Source.TokenEnv) },
		"URL":           func(r *operationResolution) { r.Source.URLEnv = "KELVO_SOURCE_REQUEST_0_URL" },
		"path":          func(r *operationResolution) { r.Source.Path = "/private/tenant" },
		"URL sheet": func(r *operationResolution) {
			r.Source.Options = map[string]string{"spreadsheet_id": "https://other.invalid/sheet"}
		},
		"extra option": func(r *operationResolution) {
			r.Source.Options = map[string]string{"spreadsheet_id": "fixture_sheet_12345678901234567890", "endpoint": "https://other.invalid"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			response = valid
			response.Secrets = maps.Clone(valid.Secrets)
			change(&response)
			if _, err := e.ResolveOperationSource(context.Background(), record, request); err == nil {
				t.Fatal("unbounded Sheets source accepted")
			}
		})
	}
}
