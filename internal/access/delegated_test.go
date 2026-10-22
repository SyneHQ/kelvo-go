// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package access

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/delegation"
	federationapi "github.com/SYNEHQ/kelvo-go/federation"
)

func delegatedUserPolicy(tenant string, columns ...string) *delegation.DelegatedPolicy {
	return &delegation.DelegatedPolicy{Revision: strings.Repeat("d", 64), Sources: map[string]delegation.DelegatedSourcePolicy{"src": {Tables: map[string]delegation.DelegatedTablePolicy{
		"orders": {Columns: columns, Rows: &delegation.DelegatedPredicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: tenant}}}}}}
}

func delegatedContext(t *testing.T, d *delegation.DelegatedPolicy) context.Context {
	t.Helper()
	p, err := FromDelegated(d)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := WithPolicy(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

// Two delegated users on one connection and table see only their own rows
// through the same relation machinery, and neither can reach the hidden column
// the predicate is written on.
func TestDelegatedUsersWithDifferentPredicatesNeverSeeEachOthersRows(t *testing.T) {
	record := policyRecord(t)
	scanIDs := func(ctx context.Context, plan federationapi.ScanPlan) ([]uint64, error) {
		table, restricted, allowed := Lookup(ctx, "src", "orders")
		if !restricted || !allowed {
			t.Fatal("policy not found for the delegated table")
		}
		relation, err := NewRelation(record.Schema(), table, recordProducer(record, nil))
		if err != nil {
			t.Fatal(err)
		}
		if relation.Schema().NumFields() != len(table.Columns) {
			t.Fatal("hidden column in the exposed schema")
		}
		reader, err := relation.Scan(ctx, plan)
		if err != nil {
			return nil, err
		}
		return readIDs(t, reader), nil
	}
	seven := delegatedContext(t, delegatedUserPolicy("7", "id", "amount"))
	eight := delegatedContext(t, delegatedUserPolicy("8", "id", "amount"))
	plan := federationapi.ScanPlan{Columns: []string{"id", "amount"}}
	got7, err7 := scanIDs(seven, plan)
	got8, err8 := scanIDs(eight, plan)
	if err7 != nil || err8 != nil || !reflect.DeepEqual(got7, []uint64{1, 9007199254740993}) || !reflect.DeepEqual(got8, []uint64{18446744073709551615}) {
		t.Fatalf("users saw %v and %v (%v, %v)", got7, got8, err7, err8)
	}
	// A pushed filter naming the other user's row returns nothing, never that row.
	other := federationapi.ScanPlan{Columns: []string{"id"}, Filters: []federationapi.Filter{{Kind: "comparison", Column: "id", Type: "uint64", Op: "eq", Value: "18446744073709551615"}}}
	if got, err := scanIDs(seven, other); err != nil || len(got) != 0 {
		t.Fatalf("filter reached the other user's row: %v %v", got, err)
	}
	// The predicate column and secrets stay unreachable, projected or filtered.
	for _, plan := range []federationapi.ScanPlan{
		{Columns: []string{"tenant_id"}},
		{Columns: []string{"secret"}},
		{Columns: []string{"id"}, Filters: []federationapi.Filter{{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "8"}}},
	} {
		if got, err := scanIDs(seven, plan); err == nil {
			t.Fatalf("hidden column reachable: %v", got)
		}
	}
	// A table missing from the policy is hidden, not unrestricted.
	if _, restricted, allowed := Lookup(seven, "src", "customers"); !restricted || allowed {
		t.Fatal("table missing from a delegated policy was not hidden")
	}
	// A source missing from the policy keeps whatever whole-source grant it has.
	if _, restricted, allowed := Lookup(seven, "elsewhere", "orders"); restricted || !allowed {
		t.Fatal("policy leaked onto an unlisted source")
	}
}

func TestFromDelegatedAppliesTheEngineRulesAndFailsClosed(t *testing.T) {
	if _, err := FromDelegated(nil); err == nil {
		t.Fatal("nil policy accepted")
	}
	mutate := func(f func(*delegation.DelegatedTablePolicy)) *delegation.DelegatedPolicy {
		d := delegatedUserPolicy("7", "id", "amount")
		table := d.Sources["src"].Tables["orders"]
		f(&table)
		d.Sources["src"].Tables["orders"] = table
		return d
	}
	bad := map[string]*delegation.DelegatedPolicy{
		"empty sources":     {Revision: strings.Repeat("d", 64)},
		"star column":       mutate(func(t *delegation.DelegatedTablePolicy) { t.Columns = []string{"*"} }),
		"duplicate columns": mutate(func(t *delegation.DelegatedTablePolicy) { t.Columns = []string{"id", "ID"} }),
		"unknown operator":  mutate(func(t *delegation.DelegatedTablePolicy) { t.Rows.Op = "like" }),
		"unknown kind":      mutate(func(t *delegation.DelegatedTablePolicy) { t.Rows.Kind = "regex" }),
		"leading zero int":  mutate(func(t *delegation.DelegatedTablePolicy) { t.Rows.Value = "07" }),
		"both rows kinds":   mutate(func(t *delegation.DelegatedTablePolicy) { t.AllRows = true }),
		"no row rule":       mutate(func(t *delegation.DelegatedTablePolicy) { t.Rows = nil }),
		"empty and":         mutate(func(t *delegation.DelegatedTablePolicy) { t.Rows = &delegation.DelegatedPredicate{Kind: "and"} }),
	}
	for name, d := range bad {
		if _, err := FromDelegated(d); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	good, err := FromDelegated(mutate(func(t *delegation.DelegatedTablePolicy) {
		t.Rows = &delegation.DelegatedPredicate{Kind: "or", Children: []delegation.DelegatedPredicate{
			{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"}, {Kind: "is_null", Column: "tenant_id"}}}
	}))
	if err != nil || len(good.Sources["src"].Tables["orders"].Rows.Children) != 2 {
		t.Fatalf("valid compound predicate refused: %v", err)
	}
	same, _ := FromDelegated(delegatedUserPolicy("7", "id", "amount"))
	if !SamePolicy(same, same) || SamePolicy(same, good) {
		t.Fatal("SamePolicy does not separate different policies")
	}
	otherTenant, _ := FromDelegated(delegatedUserPolicy("8", "id", "amount"))
	otherColumns, _ := FromDelegated(delegatedUserPolicy("7", "id"))
	if SamePolicy(same, otherTenant) || SamePolicy(same, otherColumns) {
		t.Fatal("SamePolicy ignored a predicate value or a column")
	}
}
