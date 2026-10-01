// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package native

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

func TestNativeFactoryRoutesOnlySelectedSource(t *testing.T) {
	t.Setenv("KELVO_SOURCE_DISPATCH_URL", "https://example.invalid")
	t.Setenv("KELVO_SOURCE_DISPATCH_TOKEN", "fixture-token")
	t.Setenv("KELVO_SOURCE_DISPATCH_USER", "fixture-user")
	t.Setenv("KELVO_SOURCE_FLIGHT_URL", "grpcs://example.invalid:443")
	for _, test := range []struct{ kind, implementation string }{
		{"clickhouse", "*clickhouse.Engine"}, {"databricks", "*databricks.Engine"},
		{"snowflake", "*snowflake.Engine"}, {"d1", "*d1.Engine"}, {"mongodb", "*mongodb.Engine"},
		{"sqlserver", "*sqlnative.Engine"}, {"oracle", "*sqlnative.Engine"}, {"mssql", "*sqlnative.Engine"},
		{"postgres", "*sqlnative.Engine"}, {"postgresql", "*sqlnative.Engine"}, {"cockroachdb", "*sqlnative.Engine"},
		{"alloydb", "*sqlnative.Engine"}, {"redshift", "*sqlnative.Engine"}, {"mysql", "*sqlnative.Engine"}, {"mariadb", "*sqlnative.Engine"},
		{"bigquery", "*bigquery.Engine"}, {"elasticsearch", "*elasticsearch.Engine"},
		{"trino", "*trino.Engine"}, {"presto", "*trino.Engine"}, {"arrow_flight", "*flightsql.Engine"},
	} {
		t.Run(test.kind, func(t *testing.T) {
			source := catalog.Source{ID: "selected", Type: test.kind, DSNEnv: "KELVO_SOURCE_DISPATCH_DSN", URLEnv: "KELVO_SOURCE_DISPATCH_URL", TokenEnv: "KELVO_SOURCE_DISPATCH_TOKEN"}
			switch test.kind {
			case "mongodb":
				source.Options = map[string]string{"database": "analytics"}
			case "databricks":
				source.Options = map[string]string{"warehouse_id": "warehouse"}
			case "d1":
				source.Options = map[string]string{"account_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "database_id": "11111111-1111-4111-8111-111111111111"}
			case "bigquery":
				source.Options = map[string]string{"project": "fixture-project", "location": "us"}
			case "trino", "presto":
				source.UsernameEnv = "KELVO_SOURCE_DISPATCH_USER"
			case "arrow_flight":
				source.URLEnv = "KELVO_SOURCE_FLIGHT_URL"
				source.Options = map[string]string{"protocol": "flightsql"}
			}
			r := query.Request{Mode: "native", ConnectionID: source.ID, SQL: "SELECT 1"}
			if test.kind == "mongodb" {
				r.SQL = ""
				r.Mongo = &query.MongoRequest{Collection: "events"}
			}
			// This deliberately invalid unrelated entry must not enter New.
			config := catalog.Config{Sources: []catalog.Source{{ID: "unselected", Type: test.kind, DSNEnv: "HOME"}, source}}
			e, err := New(config, query.DefaultLimits(), r)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if actual := fmt.Sprintf("%T", e.(*boundEngine).Engine); actual != test.implementation {
				t.Fatalf("routed to %s", actual)
			}
		})
	}
}

func TestNativeFactoryRequiresExplicitAdapter(t *testing.T) {
	t.Setenv("KELVO_SOURCE_ADAPTER_URL", "https://adapter.invalid")
	t.Setenv("KELVO_SOURCE_ADAPTER_FLIGHT", "grpcs://adapter.invalid:443")
	t.Setenv("KELVO_SOURCE_ADAPTER_TOKEN", "fixture-token")
	for _, kind := range []string{"h2", "hive", "spark", "db2", "ignite", "exasol", "sap_hana", "sap_ase", "redis", "cassandra", "scylla", "cosmosdb", "dynamodb", "spanner", "athena", "clickhouse_lambda", "salesforce", "google_sheets", "stripe", "posthog", "ga4", "google_ads", "facebook_ads"} {
		t.Run(kind, func(t *testing.T) {
			source := catalog.Source{ID: "selected", Type: kind, URLEnv: "KELVO_SOURCE_ADAPTER_URL", TokenEnv: "KELVO_SOURCE_ADAPTER_TOKEN"}
			r := query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT 1"}
			if _, err := New(catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits(), r); err == nil {
				t.Fatal("missing built-in adapter was accepted")
			}
			for _, adapter := range []string{"dbapi", "flightsql"} {
				source.Adapter = adapter
				source.Options = map[string]string{"remote_connection_id": "configured-connection"}
				source.URLEnv = "KELVO_SOURCE_ADAPTER_URL"
				if adapter == "flightsql" {
					source.Options = nil
					source.URLEnv = "KELVO_SOURCE_ADAPTER_FLIGHT"
				}
				e, err := New(catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits(), r)
				if err != nil {
					t.Fatal(err)
				}
				if e.(*boundEngine).sourceType != kind || e.(*boundEngine).adapter != adapter {
					t.Fatal("adapter lost source identity")
				}
				e.Close()
			}
		})
	}
}

func TestNativeFactoryRejectsSyntaxMismatchAndUnavailableSource(t *testing.T) {
	for _, test := range []struct {
		kind    string
		request query.Request
	}{
		{"mongodb", query.Request{Mode: "native", ConnectionID: "registered", SQL: "SELECT ? FROM events", Parameters: []query.Parameter{{Type: "null"}}}},
		{"clickhouse", query.Request{Mode: "native", ConnectionID: "registered", Mongo: &query.MongoRequest{Collection: "events"}}},
		{"sqlserver", query.Request{Mode: "native", ConnectionID: "registered", Mongo: &query.MongoRequest{Collection: "events"}}},
		{"mongodb", query.Request{Mode: "native", ConnectionID: "other", Mongo: &query.MongoRequest{Collection: "events"}}},
		{"csv", query.Request{Mode: "native", ConnectionID: "registered", SQL: "SELECT 1"}},
	} {
		config := catalog.Config{Sources: []catalog.Source{{ID: "registered", Type: test.kind}}}
		if _, err := New(config, query.DefaultLimits(), test.request); err == nil {
			t.Fatalf("accepted %+v for %s", test.request, test.kind)
		}
	}
}

type spyEngine struct{ called bool }

func (e *spyEngine) Close() error { return nil }
func (e *spyEngine) Execute(ctx context.Context, r query.Request, sink query.Sink) (query.Stats, error) {
	e.called = true
	if _, ok := ctx.Deadline(); !ok {
		panic("factory lost deadline")
	}
	return query.Stats{}, nil
}

type discardSink struct{}

func (discardSink) Schema(*arrow.Schema) error    { return nil }
func (discardSink) Write(arrow.RecordBatch) error { return nil }

func TestBoundEngineRejectsSourceSubstitution(t *testing.T) {
	spy := new(spyEngine)
	e := &boundEngine{Engine: spy, sourceID: "registered", sourceType: "sqlserver", timeout: time.Second}
	for _, r := range []query.Request{
		{Mode: "native", ConnectionID: "other", SQL: "SELECT 1"},
		{Mode: "native", ConnectionID: "registered", Mongo: &query.MongoRequest{Collection: "events"}},
	} {
		if _, err := e.Execute(context.Background(), r, discardSink{}); err == nil {
			t.Fatal("bound source replaced")
		}
	}
	if spy.called {
		t.Fatal("rejected request reached the driver")
	}
	if _, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "registered", SQL: "SELECT 1"}, discardSink{}); err != nil || !spy.called {
		t.Fatal("valid bound request failed")
	}
}
