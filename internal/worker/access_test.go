// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func accessFixture(t *testing.T) (context.Context, catalog.Source) {
	t.Helper()
	p := access.Policy{Sources: map[string]access.SourcePolicy{"selected": {Tables: map[string]access.TablePolicy{"orders": {Columns: []string{"id"}, Rows: &access.Predicate{Kind: "comparison", Column: "account_id", Type: "int64", Op: "eq", Value: "42"}}}}}}
	ctx, err := access.WithPolicy(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	source := catalog.Source{ID: "selected", Type: "clickhouse", URLEnv: "KELVO_SOURCE_SELECTED_URL", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "orders", Table: "orders", Database: "reports"}}}}
	return ctx, source
}

func accessChildMatches(in Input) bool {
	if in.Access == nil || len(in.Access.Sources) != 1 || len(in.Config.Sources) != 1 || in.Config.Sources[0].ID != "selected" {
		return false
	}
	ctx, err := in.ExecutionContext(context.Background())
	if err != nil {
		return false
	}
	table, restricted, allowed := access.Lookup(ctx, "selected", "orders")
	return restricted && allowed && len(table.Columns) == 1 && table.Columns[0] == "id" && table.Rows != nil && table.Rows.Value == "42"
}

func TestExecutorForwardsTrustedPolicyToRealChild(t *testing.T) {
	ctx, source := accessFixture(t)
	t.Setenv("KELVO_SOURCE_SELECTED_URL", "http://localhost:8123")
	e, err := New(catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := e.Execute(ctx, query.Request{Sources: []string{"selected"}, SQL: "SELECT access_policy"}, &workerTestSink{})
	if err != nil || stats.Rows != 3 {
		t.Fatalf("trusted child envelope failed: %v", err)
	}
}

func TestRestrictedExecutorRejectsBypassesBeforeAnySourceWork(t *testing.T) {
	ctx, callback := accessFixture(t)
	for _, source := range []catalog.Source{
		{ID: "selected", Type: "accelerated"},
		{ID: "selected", Type: "parquet", Path: "/must-not-open"},
		{ID: "selected", Type: "postgres", DSNEnv: "KELVO_SOURCE_DSN"},
	} {
		e := &Executor{Config: catalog.Config{Sources: []catalog.Source{source}}, Limits: query.DefaultLimits(), Binary: "/must-not-start"}
		e.Secrets = secretResolverFunc(func(context.Context, string) (string, bool, error) {
			t.Fatal("restriction checked after secret access")
			return "", false, nil
		})
		_, err := e.Execute(ctx, query.Request{Sources: []string{"selected"}, SQL: "SELECT 1"}, &workerTestSink{})
		if err == nil || query.PublicError(err).Code != "UNSUPPORTED" {
			t.Fatalf("source bypass reached execution: %v", err)
		}
	}
	p, _ := access.PolicyFromContext(ctx)
	in := Input{Access: &p, Config: catalog.Config{Sources: []catalog.Source{callback}}, Limits: query.DefaultLimits(), Request: query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT 1"}}
	if _, err := in.ExecutionContext(context.Background()); err == nil {
		t.Fatal("child native bypass accepted")
	}
	in.Access = &access.Policy{}
	if _, err := in.ExecutionContext(context.Background()); err == nil {
		t.Fatal("malformed child policy accepted")
	}
}
