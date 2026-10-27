// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func routerFixture(covered, fingerprintOverride string) (catalog.Config, *Router) {
	c := catalog.Config{
		Sources: []catalog.Source{{ID: "warehouse", Type: "clickhouse", URLEnv: "W_URL"}, {ID: "live", Type: "postgres", DSNEnv: "LIVE_DSN"}},
		Acceleration: &catalog.AccelerationConfig{TenantID: "tenant-a", Directory: "/x", Datasets: []catalog.Dataset{
			{ID: "orders", AuthorizationVersion: "v1", MaxAge: time.Hour, Query: query.Request{Mode: "native", ConnectionID: "warehouse", SQL: "SELECT 1"},
				Route: &catalog.Route{TimeColumn: "ts", LiveSource: "live"}},
			{ID: "plain", AuthorizationVersion: "v1", MaxAge: time.Hour, Query: query.Request{Mode: "native", ConnectionID: "warehouse", SQL: "SELECT 1"}},
		}},
	}
	fingerprint, _ := c.DatasetFingerprint("orders")
	if fingerprintOverride != "" {
		fingerprint = fingerprintOverride
	}
	return c, &Router{Status: func(context.Context, string) (Snapshot, error) {
		return Snapshot{Fingerprint: fingerprint, CoveredUntil: covered}, nil
	}}
}

func TestRoutedDatasetSkipsRefreshOnMiss(t *testing.T) {
	c, router := routerFixture("2026-03-01T00:00:00Z", "")
	c.Acceleration.Datasets[0].OnUnavailable = "refresh"
	live, _, ok := router.Plan(context.Background(), c, query.Request{Mode: "federated", SQL: "SELECT * FROM orders", Sources: []string{"orders"}})
	if !ok {
		t.Fatal("not routed")
	}
	miss := &OnMiss{
		Status: func(context.Context, string) (Snapshot, error) {
			t.Error("status read for a routed dataset")
			return Snapshot{}, ErrNotFound
		},
		Refresh: func(context.Context, string) error { t.Error("refresh for a routed dataset"); return nil },
	}
	if refreshed, err := miss.Ensure(context.Background(), c, live); err != nil || len(refreshed) != 0 {
		t.Fatal(refreshed, err)
	}
}

func TestRouterPlan(t *testing.T) {
	const covered = "2026-03-01T00:00:00Z"
	request := func(sql string, sources ...string) query.Request {
		return query.Request{Mode: "federated", SQL: sql, Sources: sources}
	}
	for _, tc := range []struct {
		name, covered, fingerprint string
		request                    query.Request
		reason                     string
		sources                    []string
	}{
		{"within covered reads the snapshot", covered, "", request("SELECT * FROM orders WHERE ts < '2026-02-01'", "orders"), "", nil},
		{"beyond covered reads live", covered, "", request("SELECT * FROM orders WHERE ts < '2026-04-01'", "orders"), RouteBeyondCovered, []string{"live"}},
		{"no where reads live", covered, "", request("SELECT * FROM orders", "orders"), RouteUnbounded, []string{"live"}},
		{"not understood reads live", covered, "", request("SELECT * FROM orders WHERE ts < '2026-02-01' OR ts IS NULL", "orders"), RouteUnparsed, []string{"live"}},
		{"missing covered_until never routes", "", "", request("SELECT * FROM orders", "orders"), "", nil},
		{"changed definition never routes", covered, "stale", request("SELECT * FROM orders", "orders"), "", nil},
		{"dataset without route", covered, "", request("SELECT * FROM plain", "plain"), "", nil},
		{"live source already requested", covered, "", request("SELECT * FROM orders", "orders", "live"), "", nil},
		{"native mode", covered, "", query.Request{Mode: "native", ConnectionID: "orders", SQL: "SELECT * FROM orders"}, "", nil},
	} {
		c, router := routerFixture(tc.covered, tc.fingerprint)
		live, reasons, ok := router.Plan(context.Background(), c, tc.request)
		if tc.sources == nil {
			if ok || reasons != nil || len(live.Sources) != len(tc.request.Sources) || (len(live.Sources) > 0 && live.Sources[0] != tc.request.Sources[0]) {
				t.Errorf("%s: routed %v %v", tc.name, live.Sources, reasons)
			}
			continue
		}
		if !ok || reasons["orders"] != tc.reason || len(live.Sources) != len(tc.sources) || live.Sources[0] != tc.sources[0] {
			t.Errorf("%s: got %v %v %v", tc.name, live.Sources, reasons, ok)
		}
		if tc.request.Sources[0] != "orders" {
			t.Errorf("%s: caller's request was modified", tc.name)
		}
	}
}

func TestRouteCoveredHonoursLateWindow(t *testing.T) {
	d := catalog.Dataset{Incremental: &catalog.Incremental{LateWindow: 10 * time.Minute}}
	if got := routeCovered(d, "2026-03-01T12:00:00Z"); got != "2026-03-01T11:50:00Z" {
		t.Fatalf("late window bound %q", got)
	}
	if got := routeCovered(catalog.Dataset{}, "2026-03-01T12:00:00Z"); got != "2026-03-01T12:00:00Z" {
		t.Fatalf("plain bound %q", got)
	}
	if got := routeCovered(d, "42"); got != "42" {
		t.Fatalf("integer bound %q", got)
	}
	// A read ending inside the window must go live.
	if RouteReason("SELECT * FROM orders WHERE ts <= '2026-03-01 11:55:00'", "orders", "ts", routeCovered(d, "2026-03-01T12:00:00Z")) == "" {
		t.Fatal("read inside late_window served from the snapshot")
	}
}
