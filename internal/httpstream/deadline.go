// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package httpstream bounds the lifetime of HTTP response writes.
package httpstream

import (
	"context"
	"net/http"
	"sync"
	"time"
)

const cancellationInterval = 25 * time.Millisecond

// WatchWriteDeadline sets the original response deadline and keeps an expired
// deadline in force after cancellation. Go's HTTP/1 chunk writer closes TLS on
// a write error; TLS close-notify replaces the expired deadline with a five-
// second deadline. A single cancellation update therefore cannot promptly join
// a blocked Write. Reassertion also interrupts that close-notify write.
//
// Call the returned function before the handler returns. It joins the watcher
// before the ResponseWriter becomes invalid. It leaves the deadline in place for
// net/http to flush buffered bytes after handler return and clear it afterward.
// Unsupported writers retain their existing
// behavior; this helper cannot add socket controls they do not implement.
func WatchWriteDeadline(ctx context.Context, writer http.ResponseWriter, deadline time.Time) func() {
	controller := http.NewResponseController(writer)
	if ctx.Err() != nil {
		deadline = time.Now()
	}
	if controller.SetWriteDeadline(deadline) != nil {
		return func() {}
	}
	finished, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-finished:
			return
		case <-ctx.Done():
		}
		tick := time.NewTicker(cancellationInterval)
		defer tick.Stop()
		for {
			select {
			case <-finished:
				return
			default:
			}
			if controller.SetWriteDeadline(time.Now()) != nil {
				return
			}
			select {
			case <-finished:
				return
			case <-tick.C:
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(finished)
			<-joined
		})
	}
}
