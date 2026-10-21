// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// planRoute decides which accelerated datasets read their live source instead
// of the snapshot. A routed read is a direct read of the live source, so it
// must pass the same access check and quota as one: the substituted request is
// returned for both. Delegated and restricted reads never route, because their
// grants are keyed to the dataset alias, not to the live source.
func (e *Executor) planRoute(ctx context.Context, r query.Request, dynamic bool) (map[string]string, query.Request) {
	if e.Router == nil || dynamic || access.Restricted(ctx) {
		return nil, r
	}
	live, reasons, ok := e.Router.Plan(ctx, e.Config, r)
	if !ok || access.ValidateRequest(ctx, e.Config, live) != nil {
		return nil, r
	}
	return reasons, live
}

// markRouted makes each live source answer under its dataset alias, which is
// the name the SQL uses, and reports the routing in the read's stats.
func markRouted(config catalog.Config, r query.Request, routed map[string]string, sources []catalog.Source, versions []query.AccelerationVersion) []query.AccelerationVersion {
	for _, id := range r.Sources {
		reason, live := routed[id]
		if !live {
			continue
		}
		d, _ := config.Dataset(id)
		for i := range sources {
			if sources[i].ID == d.Route.LiveSource {
				sources[i].ID = id
			}
		}
		versions = append(versions, query.AccelerationVersion{Dataset: id, RoutedLive: true, RouteReason: reason})
	}
	return versions
}
