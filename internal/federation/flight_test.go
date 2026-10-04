// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"testing"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

func TestFlightRequiredFiltersFailBeforeScanDiscovery(t *testing.T) {
	field := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}
	table := &Table{
		config:       catalog.Config{Sources: []catalog.Source{{Type: "arrow_flight"}}},
		customDriver: predicateNoIODriver{},
		schema:       arrow.NewSchema([]arrow.Field{field}, nil),
		columns:      map[string]arrow.Field{"id": field},
	}
	for _, filter := range []federationapi.Filter{
		{Kind: "comparison", Column: "id", Op: "eq", Type: "int64", Value: "1"},
		{Kind: "is_null", Column: "id"},
		{Kind: "is_not_null", Column: "id"},
		{Kind: "and", Children: []federationapi.Filter{{Kind: "is_null", Column: "id"}}},
		{Kind: "unknown"},
	} {
		// scan must reject before touching admission or launching a producer;
		// this table deliberately has no live source, budget or cancellation.
		reader, err := table.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"id"}, Filters: []federationapi.Filter{filter}})
		if reader != nil || err == nil || query.PublicError(err).Code != "UNSUPPORTED" {
			t.Fatalf("required filter reached scan admission: reader=%v err=%v", reader, err)
		}
	}
	if columns := table.PredicateCapabilities().Columns; len(columns) != 0 {
		t.Fatalf("advertised source predicates: %v", columns)
	}
}
