//go:build duckdb_arrow && duckbridge && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// delegatedUserContext builds the context a node derives from a verified grant
// (access.FromDelegated + access.WithPolicy) for a user limited to one tenant.
func delegatedUserContext(t *testing.T, tenant string) context.Context {
	t.Helper()
	predicate := func() *delegation.DelegatedPredicate {
		return &delegation.DelegatedPredicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: tenant}
	}
	policy, err := access.FromDelegated(&delegation.DelegatedPolicy{Revision: strings.Repeat("a", 64), Sources: map[string]delegation.DelegatedSourcePolicy{"warehouse": {Tables: map[string]delegation.DelegatedTablePolicy{
		"orders":    {Columns: []string{"id", "amount"}, Rows: predicate()},
		"customers": {Columns: []string{"id", "secret"}, Rows: predicate()},
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := access.WithPolicy(context.Background(), policy)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

// Two delegated users share one connection and table with different row
// predicates. Whatever shape the SQL takes, each sees only their own tenant.
func TestDelegatedUsersNeverSeeEachOthersRowsThroughAnyQueryShape(t *testing.T) {
	engine, _ := accessEngine(t)
	userA, userB := delegatedUserContext(t, "7"), delegatedUserContext(t, "8")
	cases := []struct {
		name, sql string
		a, b      [][]int64
	}{
		{"projection", "SELECT id::BIGINT FROM warehouse.orders ORDER BY id", [][]int64{{1}, {2}, {4}}, [][]int64{{3}, {5}}},
		{"select star", "SELECT * FROM warehouse.orders ORDER BY id", [][]int64{{1, 10}, {2, 20}, {4, -1}}, [][]int64{{3, 9000}, {5, 8000}}},
		{"aggregate", "SELECT count(*)::BIGINT, sum(amount)::BIGINT FROM warehouse.orders", [][]int64{{3, 29}}, [][]int64{{2, 17000}}},
		{"join", "SELECT count(*)::BIGINT FROM warehouse.orders o JOIN warehouse.customers c ON o.id=c.id", [][]int64{{2}}, [][]int64{{0}}},
		{"self join", "SELECT count(*)::BIGINT FROM warehouse.orders a JOIN warehouse.orders b ON a.id=b.id", [][]int64{{3}}, [][]int64{{2}}},
		{"cte", "WITH t AS (SELECT id, amount FROM warehouse.orders) SELECT sum(amount)::BIGINT FROM t", [][]int64{{29}}, [][]int64{{17000}}},
		{"window", "SELECT id::BIGINT, sum(amount) OVER (ORDER BY id)::BIGINT FROM warehouse.orders ORDER BY id", [][]int64{{1, 10}, {2, 30}, {4, 29}}, [][]int64{{3, 9000}, {5, 17000}}},
		{"alias", "SELECT id::BIGINT AS tenant_id FROM warehouse.orders ORDER BY 1", [][]int64{{1}, {2}, {4}}, [][]int64{{3}, {5}}},
		{"union", "SELECT id::BIGINT FROM warehouse.orders UNION ALL SELECT id::BIGINT FROM warehouse.customers ORDER BY 1", [][]int64{{1}, {1}, {2}, {4}, {4}}, [][]int64{{2}, {3}, {5}}},
		{"scalar subquery", "SELECT id::BIGINT FROM warehouse.orders WHERE amount > (SELECT avg(amount) FROM warehouse.orders) ORDER BY 1", [][]int64{{1}, {2}}, [][]int64{{3}}},
		{"not in subquery", "SELECT id::BIGINT FROM warehouse.orders WHERE id NOT IN (SELECT id FROM warehouse.customers) ORDER BY 1", [][]int64{{2}}, [][]int64{{3}, {5}}},
	}
	for _, tc := range cases {
		for user, want := range map[string]struct {
			ctx    context.Context
			values [][]int64
		}{"A": {userA, tc.a}, "B": {userB, tc.b}} {
			sink := &accessValueSink{}
			if _, err := engine.Execute(want.ctx, query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: tc.sql}, sink); err != nil {
				t.Fatalf("%s as %s: %v", tc.name, user, err)
			}
			if !reflect.DeepEqual(sink.values, want.values) {
				t.Fatalf("%s as %s: got %v want %v", tc.name, user, sink.values, want.values)
			}
		}
	}
}

func TestDelegatedPolicyHidesColumnsEvenThroughAliasesAndSubqueries(t *testing.T) {
	engine, _ := accessEngine(t)
	for _, user := range []context.Context{delegatedUserContext(t, "7"), delegatedUserContext(t, "8")} {
		for _, sql := range []string{
			"SELECT tenant_id AS x FROM warehouse.orders",
			"SELECT x FROM (SELECT tenant_id AS x FROM warehouse.orders) s",
			"SELECT o.tenant_id FROM warehouse.orders o",
			"SELECT secret FROM warehouse.orders",
			"SELECT count(*) FROM warehouse.orders WHERE tenant_id IN (7, 8)",
			"SELECT * FROM warehouse.hidden",
		} {
			sink := &accessValueSink{}
			if _, err := engine.Execute(user, query.Request{Mode: "federated", Sources: []string{"warehouse"}, SQL: sql}, sink); err == nil || sink.rows != 0 {
				t.Fatalf("hidden column or table reachable: %s", sql)
			}
		}
	}
}
