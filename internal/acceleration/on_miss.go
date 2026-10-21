// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"sync"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// OnMiss refreshes opted-in datasets (on_unavailable: refresh) whose snapshot is
// missing or expired, before the query resolves them. Refresh is the node's
// normal refresh path; the query child still only sees immutable Parquet paths.
// Concurrent callers for one dataset share a single refresh.
type OnMiss struct {
	Status  func(context.Context, string) (Snapshot, error)
	Refresh func(context.Context, string) error

	mu      sync.Mutex
	flights map[string]*missFlight
}

type missFlight struct {
	done chan struct{}
	err  error
}

// Ensure returns the datasets that were refreshed on demand for this request.
// ponytail: a follower inherits the leader's failure, including its own ctx
// cancelling; the follower's query then fails DATASET_UNAVAILABLE and can retry.
func (o *OnMiss) Ensure(ctx context.Context, c catalog.Config, request query.Request) (map[string]bool, error) {
	var refreshed map[string]bool
	for _, id := range request.Sources {
		d, ok := c.Dataset(id)
		if !ok || d.OnUnavailable != "refresh" {
			continue
		}
		fingerprint, err := c.DatasetFingerprint(id)
		if err != nil {
			continue // resolution reports the configuration failure
		}
		snapshot, err := o.Status(ctx, id)
		// Only missing or expired; a changed definition or corrupt snapshot stays unavailable.
		if !errors.Is(err, ErrNotFound) && (err != nil || snapshot.Fingerprint != fingerprint || d.MaxAge <= 0 || snapshot.Age() <= d.MaxAge) {
			continue
		}
		if err := o.refresh(ctx, id); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, query.NewError("DATASET_UNAVAILABLE", "Accelerated dataset is missing, stale, or requires a refresh")
		}
		if refreshed == nil {
			refreshed = map[string]bool{}
		}
		refreshed[id] = true
	}
	return refreshed, nil
}

func (o *OnMiss) refresh(ctx context.Context, id string) error {
	o.mu.Lock()
	f, running := o.flights[id]
	if !running {
		if o.flights == nil {
			o.flights = map[string]*missFlight{}
		}
		f = &missFlight{done: make(chan struct{})}
		o.flights[id] = f
	}
	o.mu.Unlock()
	if running {
		select {
		case <-f.done:
			return f.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.err = o.Refresh(ctx, id)
	o.mu.Lock()
	delete(o.flights, id)
	o.mu.Unlock()
	close(f.done)
	return f.err
}
