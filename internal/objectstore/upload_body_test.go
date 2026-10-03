// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type uploadTestReader struct {
	*bytes.Reader
	entered chan struct{}
	proceed <-chan struct{}
	once    sync.Once
	reads   atomic.Int32
	closes  atomic.Int32
}

func (r *uploadTestReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	if r.entered != nil {
		r.once.Do(func() { close(r.entered) })
		<-r.proceed
	}
	return r.Reader.Read(p)
}

func (r *uploadTestReader) Close() error { r.closes.Add(1); return nil }

func uploadWait(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("upload fixture did not reach its required boundary")
	}
}

func uploadPending(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
		t.Fatal("upload ownership returned before its required completion proof")
	case <-time.After(30 * time.Millisecond):
	}
}

func uploadCleanupJoins(t *testing.T, signals ...<-chan struct{}) {
	t.Helper()
	for _, signal := range signals {
		select {
		case <-signal:
		case <-time.After(3 * time.Second):
			t.Error("upload fixture cleanup did not join its spawned work")
		}
	}
}

func TestUploadBodySealsPromptlyAndJoinsSerializedReads(t *testing.T) {
	proceed := make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(proceed) }) }
	t.Cleanup(unblock)
	reader := &uploadTestReader{Reader: bytes.NewReader([]byte("payload")), entered: make(chan struct{}), proceed: proceed}
	body := newUploadBody(reader, 7)
	first, second := make(chan struct{}), make(chan struct{})
	var exits []<-chan struct{}
	t.Cleanup(func() { _ = body.Close(); unblock(); uploadCleanupJoins(t, exits...) })
	go func() { defer close(first); _, _ = body.Read(make([]byte, 7)) }()
	exits = append(exits, first)
	uploadWait(t, reader.entered)
	go func() {
		defer close(second)
		if _, err := body.Read(make([]byte, 7)); !errors.Is(err, io.ErrClosedPipe) {
			t.Error("queued read reached the caller after the body sealed")
		}
	}()
	exits = append(exits, second)
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		body.mu.Lock()
		reads := body.reads
		body.mu.Unlock()
		if reads == 2 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("second body read did not enter the serialization boundary")
		case <-tick.C:
		}
	}
	closed := make(chan struct{})
	go func() { _ = body.Close(); close(closed) }()
	exits = append(exits, closed)
	uploadWait(t, closed)
	if _, err := body.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("read admitted after Close")
	}
	joined := make(chan struct{})
	go func() { body.wait(); close(joined) }()
	exits = append(exits, joined)
	uploadPending(t, joined)
	if reader.reads.Load() != 1 {
		t.Fatal("caller reader was accessed concurrently")
	}
	unblock()
	uploadWait(t, first)
	uploadWait(t, second)
	uploadWait(t, joined)
	_ = body.Close()
	if reader.reads.Load() != 1 || reader.closes.Load() != 0 {
		t.Fatal("upload body reopened or closed its caller-owned reader")
	}
}

func TestUploadBodyRequiresCloseAfterExactBoundedRead(t *testing.T) {
	reader := &uploadTestReader{Reader: bytes.NewReader([]byte("payload-extra"))}
	body := newUploadBody(reader, 7)
	got, err := io.ReadAll(body)
	if err != nil || string(got) != "payload" {
		t.Fatalf("bounded upload read: %q, %v", got, err)
	}
	joined := make(chan struct{})
	go func() { body.wait(); close(joined) }()
	t.Cleanup(func() { _ = body.Close(); uploadCleanupJoins(t, joined) })
	uploadPending(t, joined)
	_ = body.Close()
	uploadWait(t, joined)
	if reader.closes.Load() != 0 {
		t.Fatal("caller reader was closed")
	}
}
