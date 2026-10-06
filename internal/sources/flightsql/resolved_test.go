package flightsql

import (
	"context"
	"crypto/tls"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"testing"
)

func TestResolvedFlightSQLPinsRequestCredentials(t *testing.T) {
	endpoint, pool, stop := start(t, false)
	defer stop()
	t.Setenv("FLIGHT_URL", "grpcs://unselected.invalid:443")
	t.Setenv("FLIGHT_TOKEN", "unselected-token")
	cfg := catalog.Config{Sources: []catalog.Source{{ID: "flight", Type: "arrow_flight", URLEnv: "FLIGHT_URL", TokenEnv: "FLIGHT_TOKEN", Options: map[string]string{"protocol": "flightsql"}}}}
	e, err := NewResolved(cfg, query.DefaultLimits(), cloudapi.Credentials{URL: endpoint, Token: "current-token", TLS: &tls.Config{RootCAs: pool}})
	if err != nil {
		t.Fatal(err)
	}
	var got capture
	stats, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "flight", SQL: "SELECT amount"}, &got)
	if err != nil || stats.Rows != 2 || got.decimal != decimal128.FromI64(12345) {
		t.Fatal("resolved Flight query failed", err)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "flight", SQL: "SELECT amount"}, &capture{}); err == nil {
		t.Fatal("closed credentials reused")
	}
	if _, err = NewResolved(cfg, query.DefaultLimits(), cloudapi.Credentials{URL: endpoint, Token: "current-token", TLS: &tls.Config{InsecureSkipVerify: true}}); err == nil {
		t.Fatal("TLS bypass accepted")
	}
}
