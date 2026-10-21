// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func routeExecutor(t *testing.T) (*Executor, query.Request) {
	t.Helper()
	c := catalog.Config{
		Sources: []catalog.Source{{ID: "warehouse", Type: "clickhouse", URLEnv: "W_URL"}, {ID: "live", Type: "postgres", DSNEnv: "LIVE_DSN"}},
		Acceleration: &catalog.AccelerationConfig{TenantID: "tenant-a", Directory: "/x", Datasets: []catalog.Dataset{
			{ID: "orders", AuthorizationVersion: "v1", MaxAge: time.Hour, Query: query.Request{Mode: "native", ConnectionID: "warehouse", SQL: "SELECT 1"},
				Route: &catalog.Route{TimeColumn: "ts", LiveSource: "live"}}}},
	}
	fingerprint, err := c.DatasetFingerprint("orders")
	if err != nil {
		t.Fatal(err)
	}
	router := &acceleration.Router{Status: func(context.Context, string) (acceleration.Snapshot, error) {
		return acceleration.Snapshot{Fingerprint: fingerprint, CoveredUntil: "2026-03-01T00:00:00Z"}, nil
	}}
	return &Executor{Config: c, Router: router}, query.Request{Mode: "federated", Sources: []string{"orders"}, SQL: "SELECT * FROM orders WHERE ts > '2026-01-01'"}
}

func TestPlanRouteSendsUnboundedReadsToTheLiveSource(t *testing.T) {
	e, r := routeExecutor(t)
	routed, live := e.planRoute(context.Background(), r, false)
	if routed["orders"] != acceleration.RouteUnbounded || live.Sources[0] != "live" || r.Sources[0] != "orders" {
		t.Fatalf("routed %v %v", routed, live.Sources)
	}
	sources := []catalog.Source{{ID: "live", Type: "postgres", DSNEnv: "LIVE_DSN"}}
	versions := markRouted(e.Config, r, routed, sources, nil)
	if sources[0].ID != "orders" || sources[0].DSNEnv != "LIVE_DSN" || len(versions) != 1 ||
		!versions[0].RoutedLive || versions[0].RouteReason != acceleration.RouteUnbounded || versions[0].Dataset != "orders" || versions[0].Generation != "" {
		t.Fatalf("stats %+v sources %+v", versions, sources)
	}
}

func TestPlanRouteStaysOnTheSnapshotWhenLiveAccessIsNotDirect(t *testing.T) {
	e, r := routeExecutor(t)
	// A restricted principal's grants are keyed to the dataset alias, not the live source.
	restricted, err := access.WithPolicy(context.Background(), access.Policy{Sources: map[string]access.SourcePolicy{
		"orders": {Tables: map[string]access.TablePolicy{"orders": {Columns: []string{"id"}, AllRows: true}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if routed, live := e.planRoute(restricted, r, false); routed != nil || live.Sources[0] != "orders" {
		t.Fatalf("restricted read routed: %v %v", routed, live.Sources)
	}
	if routed, _ := e.planRoute(context.Background(), r, true); routed != nil {
		t.Fatal("delegated read routed")
	}
	e.Router = nil
	if routed, _ := e.planRoute(context.Background(), r, false); routed != nil {
		t.Fatal("node without a router routed")
	}
}
