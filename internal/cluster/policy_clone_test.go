// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func policyIsolationFixture(t *testing.T) Policy {
	t.Helper()
	p := runtimeExportConfigFixture(t).Policy
	p.SourceQuotas = map[string]int{"sales": 1}
	p.Access = rowPrincipalPolicy().Access
	p.Access.CatalogBinding = &CatalogBinding{Version: 1, SHA256: strings.Repeat("a", 64)}
	grant := p.Access.Principals["analyst"]
	table := grant.RowColumnPolicy.Sources["sales"].Tables["orders"]
	table.Rows = &access.Predicate{Kind: "and", Children: []access.Predicate{
		{Kind: "or", Children: []access.Predicate{*table.Rows}},
		{Kind: "is_not_null", Column: "amount", Children: []access.Predicate{}},
	}}
	grant.RowColumnPolicy.Sources["sales"].Tables["orders"] = table
	p.Access.Principals["analyst"] = grant
	reports := p.Access.Principals["reports"]
	reports.NativeSources = []string{}
	p.Access.Principals["reports"] = reports
	if err := ValidatePolicy(p); err != nil {
		t.Fatal(err)
	}
	return p
}

func mutatePolicyGraph(p Policy) {
	p.Workers["a1"] = 9
	p.SourceQuotas["sales"] = 9
	if p.Exports != nil {
		p.Exports.AuthorizationVersion = "changed"
		p.Exports.Limits.MaxRows = 9
	}
	p.Access.Revision++
	p.Access.CatalogBinding.SHA256 = strings.Repeat("b", 64)
	grant := p.Access.Principals["analyst"]
	grant.NativeSources[0] = "private_native"
	grant.FederatedSources[0] = "private_federated"
	table := grant.RowColumnPolicy.Sources["sales"].Tables["orders"]
	table.Columns[0] = "private_column"
	table.Rows.Children[0].Children[0].Value = "99"
	table.Rows.Children[1].Column = "private_column"
	grant.RowColumnPolicy.Sources["sales"].Tables["extra"] = table
	grant.RowColumnPolicy.Sources["extra"] = access.SourcePolicy{Tables: map[string]access.TablePolicy{}}
	grant.Kind = "service"
	p.Access.Principals["analyst"] = grant
	delete(p.Access.Principals, "reports")
}

func policyBytes(t *testing.T, p Policy) []byte {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestPolicyClonePreservesAuthorityAndShape(t *testing.T) {
	for _, p := range []Policy{{}, {SourceQuotas: map[string]int{}}, policyIsolationFixture(t)} {
		copy, err := clonePolicy(p)
		if err != nil || !reflect.DeepEqual(copy, p) || !bytes.Equal(policyBytes(t, copy), policyBytes(t, p)) {
			t.Fatal("copy changed policy values, nil/empty shape or durable bytes", err)
		}
	}
	p := policyIsolationFixture(t)
	want := policyBytes(t, p)
	version := principalPolicyVersion(p)
	copy, err := clonePolicy(p)
	if err != nil {
		t.Fatal(err)
	}
	mutatePolicyGraph(p)
	if !bytes.Equal(policyBytes(t, copy), want) || principalPolicyVersion(copy) != version {
		t.Fatal("caller mutation changed detached authority")
	}
	second, err := clonePolicy(copy)
	if err != nil {
		t.Fatal(err)
	}
	mutatePolicyGraph(copy)
	if !bytes.Equal(policyBytes(t, second), want) {
		t.Fatal("copies share mutable policy edges")
	}
}

func TestPolicyCloneRejectsCyclicRowsBeforeConstruction(t *testing.T) {
	p := policyIsolationFixture(t)
	row := p.Access.Principals["analyst"].RowColumnPolicy.Sources["sales"].Tables["orders"].Rows
	row.Children = make([]access.Predicate, 1)
	row.Children[0] = *row
	if copy, err := clonePolicy(p); err == nil || !reflect.DeepEqual(copy, Policy{}) {
		t.Fatal("cyclic policy graph was copied or partially returned")
	}
	if store, err := OpenStore(context.Background(), NATSConfig{}, p, false); err == nil || store != nil {
		t.Fatal("invalid row graph reached store construction")
	}
	if node, err := newNode(NodeConfig{Policy: p}, nil, nil); err == nil || node != nil {
		t.Fatal("invalid row graph reached node construction")
	}
}

func TestStorePolicyExposureIsDetached(t *testing.T) {
	for name, create := range map[string]func(Policy) interface{ Policy() Policy }{
		"query":  func(p Policy) interface{ Policy() Policy } { return &NATSStore{policy: p} },
		"export": func(p Policy) interface{ Policy() Policy } { return &natsExportStore{policy: p} },
	} {
		t.Run(name, func(t *testing.T) {
			p := policyIsolationFixture(t)
			want := policyBytes(t, p)
			store := create(p)
			first, second := store.Policy(), store.Policy()
			if !reflect.DeepEqual(first, p) {
				t.Fatal("policy exposure changed nil/empty shape")
			}
			mutatePolicyGraph(first)
			if !bytes.Equal(policyBytes(t, second), want) || !bytes.Equal(policyBytes(t, store.Policy()), want) {
				t.Fatal("returned policy changed the store or a sibling snapshot")
			}
			// Returned graphs may be changed independently while the store keeps
			// serving its original immutable policy. The race gate checks this too.
			var wg sync.WaitGroup
			for range 4 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range 8 {
						copy := store.Policy()
						mutatePolicyGraph(copy)
						current, err := json.Marshal(store.Policy())
						if err != nil || !bytes.Equal(current, want) {
							t.Error("concurrent snapshot mutation changed stored policy")
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}

func TestNodeOwnsPolicyAfterConstruction(t *testing.T) {
	p := policyIsolationFixture(t)
	p.Exports = nil
	stored := policyIsolationFixture(t)
	stored.Exports = nil
	want := policyBytes(t, p)
	authority, ok := authorityForPrincipal(p, "analyst")
	if !ok {
		t.Fatal("fixture authority missing")
	}
	store := &nodeTestStore{p: stored, jobs: map[string]Snapshot{}, queue: make(chan Delivery)}
	node, err := newNode(NodeConfig{Policy: p, WorkerID: "a1"}, store, probeExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	mutatePolicyGraph(p)
	if !bytes.Equal(policyBytes(t, node.cfg.Policy), want) {
		t.Fatal("node retained caller policy aliases")
	}
	request := query.Request{Mode: "federated", Sources: []string{"sales"}, SQL: "SELECT amount FROM sales.orders"}
	ctx, err := executionAuthorityContext(context.Background(), node.cfg.Policy, &authority, request)
	if err != nil {
		t.Fatal("caller mutation invalidated the node's original authority", err)
	}
	rows, ok := access.PolicyFromContext(ctx)
	if !ok || rows.Sources["sales"].Tables["orders"].Rows.Children[0].Children[0].Value != "42" {
		t.Fatal("caller mutation changed the execution predicate")
	}
}
