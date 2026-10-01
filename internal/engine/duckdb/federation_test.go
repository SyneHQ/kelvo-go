//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

type federationDiscardSink struct{}

func (federationDiscardSink) Schema(*arrow.Schema) error    { return nil }
func (federationDiscardSink) Write(arrow.RecordBatch) error { return nil }

func TestRawArrowEntryPointsRemainPrivate(t *testing.T) {
	for _, sql := range []string{"SELECT * FROM arrow_scan(1,2,3)", "SELECT * FROM ARROW_SCAN_DUMB(1,2,3)"} {
		if !containsDeniedCapability(sql) {
			t.Fatal("raw pointer table function allowed")
		}
	}
}

func TestCustomFederationNeedsExplicitBridgeBuild(t *testing.T) {
	if duckbridge.Available() {
		t.Skip("bridge is present in this build")
	}
	source := catalog.Source{ID: "warehouse", Type: "clickhouse", URLEnv: "KELVO_SOURCE_UNSET_FEDERATION", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "events", Database: "analytics", Table: "events"}}}}
	e, err := New(catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Execute(context.Background(), query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: "SELECT * FROM warehouse.events"}, federationDiscardSink{})
	var qe *query.Error
	if !errors.As(err, &qe) || qe.Code != "UNSUPPORTED" {
		t.Fatal("did not reject missing bridge before reading credentials", err)
	}
}

func TestCustomFederationDiscoveryIsBoundedBeforeSourceAccess(t *testing.T) {
	makeSource := func(id string, n int) catalog.Source {
		s := catalog.Source{ID: id, Type: "clickhouse", URLEnv: "KELVO_SOURCE_UNSET_FEDERATION", Federation: &catalog.FederationConfig{}}
		for i := 0; i < n; i++ {
			s.Federation.Tables = append(s.Federation.Tables, catalog.FederationTable{Name: fmt.Sprintf("table_%d", i), Database: "analytics", Table: fmt.Sprintf("table_%d", i)})
		}
		return s
	}
	e, err := New(catalog.Config{Sources: []catalog.Source{makeSource("one", 32), makeSource("two", 1)}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Execute(context.Background(), query.Request{Mode: "federated", Sources: []string{"one", "two"}, SQL: "SELECT 1"}, federationDiscardSink{})
	var qe *query.Error
	if !errors.As(err, &qe) || qe.Code != "RESOURCE_EXHAUSTED" {
		t.Fatal("discovery was not bounded before reading credentials", err)
	}
}
