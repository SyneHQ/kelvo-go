package cloudapi_test

import (
	"io"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/bigquery"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/d1"
	"github.com/SYNEHQ/kelvo-go/internal/sources/databricks"
	"github.com/SYNEHQ/kelvo-go/internal/sources/elasticsearch"
	"github.com/SYNEHQ/kelvo-go/internal/sources/snowflake"
	"github.com/SYNEHQ/kelvo-go/internal/sources/spanner"
	"github.com/SYNEHQ/kelvo-go/internal/sources/trino"
)

func TestResolvedEnginesDoNotRequireAmbientCredentialReferences(t *testing.T) {
	limits := query.DefaultLimits()
	credentials := cloudapi.Credentials{URL: "https://query.example.invalid", Token: "request-only"}
	for _, tc := range []struct {
		kind    string
		options map[string]string
		open    func(catalog.Config, cloudapi.Credentials) (io.Closer, error)
	}{
		{"databricks", map[string]string{"warehouse_id": "warehouse-1"}, func(c catalog.Config, v cloudapi.Credentials) (io.Closer, error) {
			return databricks.NewResolved(c, limits, v)
		}},
		{"d1", map[string]string{"account_id": "0123456789abcdef0123456789abcdef", "database_id": "12345678-1234-1234-1234-123456789abc"}, func(c catalog.Config, v cloudapi.Credentials) (io.Closer, error) { return d1.NewResolved(c, limits, v) }},
		{"bigquery", map[string]string{"project": "fixture-project", "location": "US"}, func(c catalog.Config, v cloudapi.Credentials) (io.Closer, error) {
			return bigquery.NewResolved(c, limits, v)
		}},
		{"snowflake", nil, func(c catalog.Config, v cloudapi.Credentials) (io.Closer, error) {
			return snowflake.NewResolved(c, limits, v)
		}},
		{"elasticsearch", nil, func(c catalog.Config, v cloudapi.Credentials) (io.Closer, error) {
			return elasticsearch.NewResolved(c, limits, v)
		}},
		{"spanner", map[string]string{"project": "fixture-project", "instance": "fixture-instance", "database": "fixture-db"}, func(c catalog.Config, v cloudapi.Credentials) (io.Closer, error) {
			return spanner.NewResolved(c, limits, v)
		}},
		{"trino", nil, func(c catalog.Config, v cloudapi.Credentials) (io.Closer, error) {
			return trino.NewResolved(c, limits, v, "request-user")
		}},
		{"presto", nil, func(c catalog.Config, v cloudapi.Credentials) (io.Closer, error) {
			return trino.NewResolved(c, limits, v, "request-user")
		}},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			config := catalog.Config{Sources: []catalog.Source{{ID: "source_1", Type: tc.kind, Options: tc.options}}}
			engine, err := tc.open(config, credentials)
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Close(); err != nil {
				t.Fatal(err)
			}
			if engine, err := tc.open(config, cloudapi.Credentials{URL: credentials.URL}); err == nil {
				engine.Close()
				t.Fatal("missing token accepted")
			}
		})
	}
	config := catalog.Config{Sources: []catalog.Source{{ID: "source_1", Type: "trino"}}}
	if engine, err := trino.NewResolved(config, limits, credentials, ""); err == nil {
		engine.Close()
		t.Fatal("missing saved username accepted")
	}
}
