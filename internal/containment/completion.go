// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"sync"
)

// Completion extends worker reservations through the caller's publication and
// cleanup. It does not acquire capacity or replace process/scratch custody.
// The zero value is ready for use. Do not copy a Completion after first use.
type Completion struct {
	mu       sync.Mutex
	done     bool
	releases []func()
}

type completionContextKey struct{}

func WithCompletion(ctx context.Context, completion *Completion) context.Context {
	return context.WithValue(ctx, completionContextKey{}, completion)
}

// RetainUntilCompletion must run immediately after creating a reservation's
// custody, before credentials or source work. Without an outer owner it is a
// no-op, preserving standalone executor lifetimes.
func RetainUntilCompletion(ctx context.Context, custody *Custody) error {
	completion, _ := ctx.Value(completionContextKey{}).(*Completion)
	if completion == nil {
		return nil
	}
	completion.mu.Lock()
	defer completion.mu.Unlock()
	if completion.done {
		return ErrCompleted
	}
	release, err := custody.Hold()
	if err != nil {
		return err
	}
	completion.releases = append(completion.releases, release)
	return nil
}

// Complete releases only this owner's holds. A quarantined child or scratch
// workspace still retains its own hold and cannot be released by publication.
func (c *Completion) Complete() {
	c.mu.Lock()
	c.done = true
	releases := c.releases
	c.releases = nil
	c.mu.Unlock()
	for _, release := range releases {
		release()
	}
}
