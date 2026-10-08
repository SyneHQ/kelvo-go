// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

// The Context contract permits a delay between a parent's Done closing and a
// registered AfterFunc running. Hold that propagation boundary deterministically.
// No goroutine or timer is started; operation cleanup unregisters the callback.
type delayedCancellationContext struct {
	mu       sync.Mutex
	done     chan struct{}
	err      error
	callback func()
}

func newDelayedCancellationContext() *delayedCancellationContext {
	return &delayedCancellationContext{done: make(chan struct{})}
}
func (*delayedCancellationContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *delayedCancellationContext) Done() <-chan struct{}     { return c.done }
func (*delayedCancellationContext) Value(any) any               { return nil }
func (c *delayedCancellationContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}
func (c *delayedCancellationContext) AfterFunc(callback func()) func() bool {
	c.mu.Lock()
	c.callback = callback
	c.mu.Unlock()
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.callback == nil {
			return false
		}
		c.callback = nil
		return true
	}
}
func (c *delayedCancellationContext) cancel(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
	close(c.done)
}

func TestProviderTransportCancellationAttribution(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		for _, method := range []string{"get", "head", "range", "put"} {
			for _, scenario := range []string{"caller-cancel", "caller-deadline", "wrapped-cancel", "wrapped-deadline", "unrelated"} {
				t.Run(provider+"/"+method+"/"+scenario, func(t *testing.T) {
					ctx := newDelayedCancellationContext()
					var expected error
					calls := 0
					client := uploadClient(t, provider, "https://objects.example.test", uploadTransport(func(r *http.Request) (*http.Response, error) {
						calls++
						if r.Body != nil {
							_ = r.Body.Close()
						}
						switch scenario {
						case "caller-cancel", "caller-deadline":
							expected = context.Canceled
							if scenario == "caller-deadline" {
								expected = context.DeadlineExceeded
							}
							ctx.cancel(expected)
							if r.Context().Err() != nil {
								t.Fatal("fixture failed to hold child cancellation propagation")
							}
						case "wrapped-cancel", "wrapped-deadline":
							expected = context.Canceled
							if scenario == "wrapped-deadline" {
								expected = context.DeadlineExceeded
							}
							return nil, fmt.Errorf("private-provider-detail: %w", expected)
						}
						return nil, errors.New("private-provider-detail")
					}))
					var err error
					switch method {
					case "get":
						_, _, err = client.Get(ctx, "kelvo/data", "")
					case "head":
						_, err = client.Head(ctx, "kelvo/data", "")
					case "range":
						version := `"v1"`
						if provider == "gcs" {
							version = "1"
						}
						_, _, err = client.(RangeClient).GetRange(ctx, "kelvo/data", version, 0, 1)
					case "put":
						_, err = client.Put(ctx, "kelvo/data", bytes.NewReader(nil), 0, azureTestDigest(""), Condition{Absent: true})
					}
					if calls != 1 {
						t.Fatal("provider failed before HTTP or retried a transport failure", err)
					}
					if expected != nil {
						if err != expected {
							t.Fatalf("lost cancellation or exposed wrapped provider details: %v", err)
						}
					} else {
						message := "object storage request failed"
						if provider == "azure" {
							message = "Azure snapshot request failed"
						}
						if err == nil || err.Error() != message {
							t.Fatalf("unrelated failure lost redaction or gained cancellation: %v", err)
						}
					}
					ctx.mu.Lock()
					defer ctx.mu.Unlock()
					if ctx.callback != nil {
						t.Fatal("operation cleanup retained its cancellation callback")
					}
				})
			}
		}
	}
}

// errors.Is reaches this failure after transportError samples both contexts.
// Hold that exact boundary to cancel during upload cleanup without a sleep.
type sampledTransportFailure struct {
	sampled, proceed chan struct{}
	sample, unblock  sync.Once
}

func (*sampledTransportFailure) Error() string { return "fixture transport failed" }
func (e *sampledTransportFailure) Is(error) bool {
	e.sample.Do(func() { close(e.sampled) })
	<-e.proceed
	return false
}
func (e *sampledTransportFailure) release() { e.unblock.Do(func() { close(e.proceed) }) }
