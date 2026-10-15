// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// transientSubmitter and submittedEnqueuer are optional Store capabilities used
// by the single-call read. A store without them behaves as before.
type transientSubmitter interface {
	SubmitTransient(ctx context.Context, r query.Request) (Snapshot, error)
}

type submittedEnqueuer interface {
	EnqueueSubmitted(ctx context.Context, snap Snapshot) error
}

// submitRead stores a job: a transient one when the caller reads the outcome
// from the same response and the store can say so.
func submitRead(ctx context.Context, store Store, r query.Request, transient bool) (Snapshot, error) {
	if ts, ok := store.(transientSubmitter); ok && transient {
		return ts.SubmitTransient(ctx, r)
	}
	return store.Submit(ctx, r)
}

// enqueueRead queues a job that was just submitted, skipping the read-back the
// general Enqueue does when the store allows.
func enqueueRead(ctx context.Context, store Store, snap Snapshot, justSubmitted bool) error {
	if eq, ok := store.(submittedEnqueuer); ok && justSubmitted {
		return eq.EnqueueSubmitted(ctx, snap)
	}
	return store.Enqueue(ctx, snap.Job.ID)
}

// submitAndEnqueue is what the gateway does to start a read.
func submitAndEnqueue(ctx context.Context, store Store, r query.Request, transient bool) (Snapshot, error) {
	s, err := submitRead(ctx, store, r, transient)
	if err != nil {
		return Snapshot{}, err
	}
	return s, enqueueRead(ctx, store, s, transient)
}
