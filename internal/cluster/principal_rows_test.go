// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"go.yaml.in/yaml/v3"
)

func rowPrincipalPolicy() Policy {
	p := principalTestPolicy()
	g := p.Access.Principals["analyst"]
	g.RowColumnPolicy = &access.Policy{Sources: map[string]access.SourcePolicy{
		"sales": {Tables: map[string]access.TablePolicy{"orders": {Columns: []string{"amount"}, Rows: &access.Predicate{Kind: "comparison", Column: "account_id", Type: "int64", Op: "eq", Value: "42"}}}},
	}}
	p.Access.Principals["analyst"] = g
	return p
}

func TestPrincipalRowsAreDerivedFromCurrentAuthority(t *testing.T) {
	p := rowPrincipalPolicy()
	if err := ValidatePolicy(p); err != nil {
		t.Fatal(err)
	}
	a, _ := authorityForPrincipal(p, "analyst")
	r := query.Request{Mode: "federated", Sources: []string{"sales"}, SQL: "SELECT sum(amount) FROM sales.orders"}
	ctx, err := executionAuthorityContext(context.Background(), p, &a, r)
	if err != nil {
		t.Fatal(err)
	}
	policy, restricted := access.PolicyFromContext(ctx)
	if !restricted || len(policy.Sources) != 1 || policy.Sources["sales"].Tables["orders"].Rows.Value != "42" {
		t.Fatal("effective restriction lost")
	}
	policy.Sources["sales"].Tables["orders"].Rows.Value = "0"
	copy, _ := access.PolicyFromContext(ctx)
	if copy.Sources["sales"].Tables["orders"].Rows.Value != "42" {
		t.Fatal("execution policy is mutable")
	}
	// No stale or forged authority can select restrictions from today's grant.
	if _, err := executionAuthorityContext(context.Background(), p, nil, r); err == nil {
		t.Fatal("missing authority accepted")
	}
	p.Access.Principals["analyst"].RowColumnPolicy.Sources["sales"].Tables["orders"].Rows.Value = "43"
	if _, err := executionAuthorityContext(context.Background(), p, &a, r); err == nil {
		t.Fatal("old policy digest accepted")
	}
	a, _ = authorityForPrincipal(p, "analyst")
	ctx, err = executionAuthorityContext(context.Background(), p, &a, query.Request{Mode: "federated", Sources: []string{"daily_sales"}, SQL: "SELECT 1"})
	if err != nil || access.Restricted(ctx) {
		t.Fatal("unselected restrictions crossed a whole-source grant")
	}
}

func TestPrincipalRowsRejectAmbiguousOrUnboundedConfiguration(t *testing.T) {
	for _, mutate := range []func(*PrincipalGrant){
		func(g *PrincipalGrant) { g.NativeSources = append(g.NativeSources, "sales") },
		func(g *PrincipalGrant) { g.FederatedSources = nil },
		func(g *PrincipalGrant) { g.RowColumnPolicy = &access.Policy{} },
		func(g *PrincipalGrant) {
			table := g.RowColumnPolicy.Sources["sales"].Tables["orders"]
			table.AllRows = true
			g.RowColumnPolicy.Sources["sales"].Tables["orders"] = table
		},
	} {
		p := rowPrincipalPolicy()
		g := p.Access.Principals["analyst"]
		mutate(&g)
		p.Access.Principals["analyst"] = g
		if ValidatePolicy(p) == nil {
			t.Fatal("unsafe row policy configuration accepted")
		}
	}
	p := rowPrincipalPolicy()
	g := p.Access.Principals["analyst"]
	table := g.RowColumnPolicy.Sources["sales"].Tables["orders"]
	for i := range 6 {
		table.Columns = append(table.Columns, fmt.Sprintf("c%d%s", i, strings.Repeat("a", 900)))
	}
	g.RowColumnPolicy.Sources["sales"].Tables["orders"] = table
	p.Access.Principals = make(map[string]PrincipalGrant)
	for i := range 64 {
		p.Access.Principals[fmt.Sprintf("user-%d", i)] = g
	}
	if ValidatePolicy(p) == nil {
		t.Fatal("oversized durable policy accepted")
	}
}

func TestPrincipalRowPolicySurvivesYAMLAndDigestBinding(t *testing.T) {
	p := rowPrincipalPolicy()
	raw, err := yaml.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var restored Policy
	if err := yaml.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePolicy(restored); err != nil {
		t.Fatal(err)
	}
	if principalPolicyVersion(restored) != principalPolicyVersion(p) {
		t.Fatal("YAML changed immutable policy authority")
	}
}
