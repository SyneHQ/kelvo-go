// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckbridge

import (
	"reflect"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

func date32Comparison(op, value string) Filter {
	return Filter{Kind: "comparison", Column: "event_day", Type: "date32", Op: op, Value: value}
}

func TestDate32PredicateCapabilitiesBindExactStorage(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "integer", Type: arrow.PrimitiveTypes.Int32},
		{Name: "event_day", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "millis", Type: arrow.FixedWidthTypes.Date64},
		{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
		{Name: "decimal", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}},
	}, nil)
	caps := PredicateCapabilities{Columns: []string{"event_day"}}
	eligible, ordinals, err := bindPredicateCapabilities(schema, caps)
	if err != nil || !reflect.DeepEqual(eligible, map[string]arrow.Type{"event_day": arrow.DATE32}) || !reflect.DeepEqual(ordinals, []uint32{1}) {
		t.Fatalf("Date32 storage or ordinal changed: %v %v %v", eligible, ordinals, err)
	}
	caps.Columns[0] = "integer"
	if err := validatePredicatePlan(ScanPlan{Filters: []Filter{date32Comparison("eq", "-1")}}, eligible); err != nil {
		t.Fatal("caller changed bound Date32 eligibility", err)
	}
	for _, columns := range [][]string{{"millis"}, {"timestamp"}, {"decimal"}, {"event_day", "event_day"}, {"missing"}} {
		if _, _, err := bindPredicateCapabilities(schema, PredicateCapabilities{Columns: columns}); err == nil {
			t.Fatalf("unsupported Date32 registration accepted: %v", columns)
		}
	}
}

func TestDate32PredicatePlanAcceptsFullSignedDayDomain(t *testing.T) {
	eligible := map[string]arrow.Type{"event_day": arrow.DATE32}
	// The physical Int32 domain includes both DATE infinities and values well
	// outside source calendar ranges. Do not narrow it to finite calendar text.
	for _, value := range []string{"-2147483648", "-2147483647", "-2147483646", "-719528", "-25568", "-1", "0", "1", "11016", "120530", "2932897", "2147483646", "2147483647"} {
		for _, op := range []string{"eq", "ne", "lt", "le", "gt", "ge"} {
			if err := validatePredicatePlan(ScanPlan{Filters: []Filter{date32Comparison(op, value)}}, eligible); err != nil {
				t.Fatalf("valid signed-day comparison rejected: %s %s: %v", op, value, err)
			}
		}
	}
	for _, kind := range []string{"is_null", "is_not_null"} {
		if err := validatePredicatePlan(ScanPlan{Filters: []Filter{{Kind: kind, Column: "event_day"}}}, eligible); err != nil {
			t.Fatal("Date32 NULL predicate rejected", err)
		}
	}
	compound := Filter{Kind: "or", Children: []Filter{
		{Kind: "and", Children: []Filter{date32Comparison("ge", "-1"), date32Comparison("le", "1")}},
		{Kind: "is_null", Column: "event_day"},
	}}
	if err := validatePredicatePlan(ScanPlan{Filters: []Filter{compound}}, eligible); err != nil {
		t.Fatal("Date32 logical tree rejected", err)
	}
}

func TestDate32PredicatePlanRejectsMalformedValuesAndTypes(t *testing.T) {
	eligible := map[string]arrow.Type{"event_day": arrow.DATE32}
	for _, value := range []string{"", "-", "+1", "01", "-01", "-0", " 1", "1 ", "1\n", "1.0", "1e0", "0x1", "1970-01-01", "infinity", "-infinity", "2147483648", "-2147483649", strings.Repeat("9", 22)} {
		filter := date32Comparison("eq", value)
		for _, candidate := range []Filter{filter, {Kind: "or", Children: []Filter{date32Comparison("eq", "0"), filter}}} {
			if err := validatePredicatePlan(ScanPlan{Filters: []Filter{candidate}}, eligible); err == nil || query.PublicError(err).Code != "UNSUPPORTED" {
				t.Fatalf("malformed signed day accepted: %q: %v", value, err)
			}
		}
	}
	for _, kind := range []string{"int32", "date64", "timestamp", "decimal128", "string"} {
		filter := date32Comparison("eq", "0")
		filter.Type = kind
		if err := validatePredicatePlan(ScanPlan{Filters: []Filter{filter}}, eligible); err == nil {
			t.Fatalf("different scalar authorized by Date32: %s", kind)
		}
	}
	for _, wrong := range []map[string]arrow.Type{nil, {"other": arrow.DATE32}, {"event_day": arrow.INT32}, {"event_day": arrow.DATE64}, {"event_day": arrow.TIMESTAMP}} {
		if err := validatePredicatePlan(ScanPlan{Filters: []Filter{date32Comparison("eq", "0")}}, wrong); err == nil {
			t.Fatal("unregistered or differently bound date reached producer")
		}
	}
	for _, filter := range []Filter{
		date32Comparison("in", "0"),
		{Kind: "is_null", Column: "event_day", Type: "date32"},
		{Kind: "is_not_null", Column: "event_day", Value: "0"},
		{Kind: "comparison", Column: "event_day", Type: "date32", Op: "eq", Value: "0", Children: []Filter{date32Comparison("eq", "1")}},
	} {
		if err := validatePredicatePlan(ScanPlan{Filters: []Filter{filter}}, eligible); err == nil {
			t.Fatal("malformed Date32 filter shape accepted")
		}
	}
}
