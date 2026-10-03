// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

type delayedReaderClose struct {
	objectstore.Client
	entered chan struct{}
	allowed chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (c *delayedReaderClose) Close() {
	c.calls.Add(1)
	c.once.Do(func() { close(c.entered) })
	<-c.allowed
}

func TestObjectBackendConcurrentCloseJoinsReader(t *testing.T) {
	reader := &delayedReaderClose{entered: make(chan struct{}), allowed: make(chan struct{})}
	backend := &objectBackend{reader: reader}
	var once sync.Once
	unblock := func() { once.Do(func() { close(reader.allowed) }) }
	var joins []<-chan struct{}
	t.Cleanup(func() {
		unblock()
		for _, done := range joins {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("backend fixture cleanup did not join Close")
			}
		}
	})
	first, second, started := make(chan struct{}), make(chan struct{}), make(chan struct{})
	joins = append(joins, first)
	go func() { _ = backend.Close(); close(first) }()
	select {
	case <-reader.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("backend did not begin reader Close")
	}
	joins = append(joins, second)
	go func() { close(started); _ = backend.Close(); close(second) }()
	<-started
	select {
	case <-second:
		t.Fatal("concurrent backend Close returned before reader completion")
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := backend.Status(context.Background(), "events"); !errors.Is(err, errBackendClosed) {
		t.Fatal("backend admitted a reader while shutdown was active")
	}
	unblock()
	for _, done := range joins {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("backend did not finish reader Close")
		}
	}
	if reader.calls.Load() != 1 {
		t.Fatal("backend closed its reader more than once")
	}
}
