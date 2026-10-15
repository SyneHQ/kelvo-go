// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"time"
)

const (
	pollFirst = 50 * time.Millisecond
	// pollMax keeps a waiting request's cost to a few store reads a second
	// while a job still becomes visible within 200 ms.
	pollMax = 200 * time.Millisecond
)

// jobWaiter sleeps between reads of one job: 50 ms, 100 ms, then 200 ms, or
// until the job expires. It never verifies anything; see authorizedJob.
//
// A store change notification (a KV watch) would remove the polling. It was
// tried and withdrawn: against the real JetStream store it failed the
// mixed-load and saturation acceptance steps, and without a JetStream
// integration environment here the cause was not established.
type jobWaiter struct{ delay time.Duration }

func newJobWaiter() *jobWaiter { return &jobWaiter{delay: pollFirst} }

// wait blocks until it is worth reading the job again. It reports false when
// the request ended.
func (w *jobWaiter) wait(ctx context.Context, expires time.Time) bool {
	delay := w.delay
	w.delay = min(w.delay*2, pollMax)
	if until := time.Until(expires) + time.Millisecond; until > 0 && until < delay {
		delay = until
	}
	timer := time.NewTimer(max(delay, time.Millisecond))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
