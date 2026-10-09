// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"context"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

type CleanupIssuer interface {
	IssueCleanup(context.Context, transportissuer.CleanupRequest) (string, error)
}

// IssueCleanup uses a separate bounded admission lane. It never calls the
// ordinary resolver or renews a DATA ticket. The endpoint keeps the fixed issuer
// origin, worker certificate and HTTP response checks from normal issuance.
func (i *HTTPIssuer) IssueCleanup(parent context.Context, request transportissuer.CleanupRequest) (string, error) {
	if i == nil || parent == nil || request.Validate() != nil {
		return "", transportbroker.ErrInvalid
	}
	now := time.Now()
	if now.Before(i.notBefore) || !now.Before(i.notAfter) {
		return "", transportbroker.ErrScope
	}
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return "", transportbroker.ErrClosed
	}
	select {
	case i.cleanupSlots <- struct{}{}:
	default:
		i.mu.Unlock()
		return "", transportbroker.ErrCapacity
	}
	i.active.Add(1)
	i.mu.Unlock()
	defer func() { <-i.cleanupSlots; i.active.Done() }()
	ctx, cancel := context.WithDeadline(parent, minTime(now.Add(MaxSetupTime), i.notAfter))
	defer cancel()
	stop := context.AfterFunc(i.ctx, cancel)
	defer stop()
	if i.ctx.Err() != nil {
		cancel()
	}
	endpoint := strings.TrimSuffix(i.endpoint, transportissuer.Path) + transportissuer.CleanupPath
	return i.post(ctx, endpoint, request)
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
