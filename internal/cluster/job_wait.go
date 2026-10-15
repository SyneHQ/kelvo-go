// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"time"
)

// jobWatcher is an optional Store capability: a signal that the job's record
// changed. Stores without it are polled with a backoff.
type jobWatcher interface {
	WatchJob(ctx context.Context, id string) (changed <-chan struct{}, stop func(), err error)
}

const (
	pollFirst = 50 * time.Millisecond
	pollMax   = 500 * time.Millisecond
	// With a watch the loop still re-reads this often, so a missed event cannot
	// strand a request.
	watchedRecheck = time.Second
)

// jobWaiter sleeps between reads of one job. It wakes on a store notification,
// on the job's expiry, or on its safety timer; it never verifies anything.
type jobWaiter struct {
	store   Store
	id      string
	started bool
	changed <-chan struct{}
	stopFn  func()
	delay   time.Duration
}

// newJobWaiter costs nothing until the job is actually waited for: a job that
// is already assigned never opens a watch.
func newJobWaiter(_ context.Context, store Store, id string) *jobWaiter {
	return &jobWaiter{store: store, id: id, delay: pollFirst}
}

// start opens the watch on the first wait. The caller re-reads at once
// afterwards, so a change before the watch existed is still seen.
func (w *jobWaiter) start(ctx context.Context) {
	w.started = true
	if watcher, ok := w.store.(jobWatcher); ok {
		if changed, stop, err := watcher.WatchJob(ctx, w.id); err == nil {
			w.changed, w.stopFn = changed, stop
		}
	}
}

func (w *jobWaiter) stop() {
	if w.stopFn != nil {
		w.stopFn()
	}
}

// wait blocks until it is worth reading the job again. It reports false when
// the request ended.
func (w *jobWaiter) wait(ctx context.Context, expires time.Time) bool {
	if !w.started {
		w.start(ctx)
		return ctx.Err() == nil
	}
	delay := w.delay
	if w.changed != nil {
		delay = watchedRecheck
	} else if w.delay < pollMax {
		w.delay = min(w.delay*2, pollMax)
	}
	if until := time.Until(expires) + time.Millisecond; until > 0 && until < delay {
		delay = until
	}
	timer := time.NewTimer(max(delay, time.Millisecond))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-w.changed: // nil without a watch: never ready
		return true
	case <-timer.C:
		return true
	}
}
