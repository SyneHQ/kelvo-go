//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operationinput

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestStreamReservesBeforeProducerAndCommitsExactBytes(t *testing.T) {
	s, _ := testStore(t, 1, 2*ChunkBytes)
	w, err := s.BeginStream(context.Background(), identity, time.Now().Add(time.Minute), ArrowIPC)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := s.BeginStream(context.Background(), identity, time.Now().Add(time.Minute), ArrowIPC); !errors.Is(err, exports.ErrLimit) {
		t.Fatal("capacity not reserved before first write", err)
	}
	payload := bytes.Repeat([]byte("arrow fixture "), 8000)
	if n, err := w.Write(payload); err != nil || n != len(payload) {
		t.Fatal(n, err)
	}
	ref, err := w.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(context.Background(), identity, ref)
	if err != nil || !bytes.Equal(payload, got) {
		t.Fatal("committed stream changed or close withdrew it", err)
	}
}

func TestStreamClosePreservesCleanupErrorJoinedWithCancellation(t *testing.T) {
	for _, cause := range []error{context.Canceled, io.ErrClosedPipe} {
		reader, writer := io.Pipe()
		defer reader.Close()
		done := make(chan struct{})
		close(done)
		failure := errors.New("fixture writer cleanup failure")
		stream := &StreamWriter{pipe: writer, cancel: func() {}, done: done, err: errors.Join(cause, ErrCleanup, failure)}
		for range 2 {
			if err := stream.Close(); !errors.Is(err, cause) || !errors.Is(err, ErrCleanup) || !errors.Is(err, failure) {
				t.Fatal("cancellation hid failed cleanup", err)
			}
		}
	}
}

func TestStreamCancellationJoinsBlockedStorageReader(t *testing.T) {
	s, _ := testStore(t, 1, ChunkBytes)
	ctx, cancel := context.WithCancel(context.Background())
	w, err := s.BeginStream(ctx, identity, time.Now().Add(time.Minute), NativeInput)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("unfinished")); err != nil {
		t.Fatal(err)
	}
	cancel()
	done := make(chan error, 1)
	go func() { done <- w.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled producer retained a storage reader")
	}
	if _, err := s.Cleanup(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	next, err := s.BeginStream(context.Background(), identity, time.Now().Add(time.Minute), NativeInput)
	if err != nil {
		t.Fatal("aborted stream leaked capacity", err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStreamExcessNeverPublishesAReference(t *testing.T) {
	s, _ := testStore(t, 2, ChunkBytes)
	w, err := s.BeginStream(context.Background(), identity, time.Now().Add(time.Minute), ArrowIPC)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, _ = w.Write(bytes.Repeat([]byte("x"), ChunkBytes+10))
	if ref, err := w.Commit(); !errors.Is(err, ErrLimit) || ref.ID != "" {
		t.Fatal("oversized stream published", ref, err)
	}
}

func TestStreamLimitBoundsReservationAndPayload(t *testing.T) {
	s, config := testStore(t, 64, MaxInputBytes)
	ctx := context.Background()
	expiry := time.Now().Add(time.Minute)
	for _, maximum := range []int64{0, -1, MaxInputBytes + 1} {
		if _, err := s.BeginStreamLimit(ctx, identity, expiry, ArrowIPC, maximum); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid stream limit accepted", maximum, err)
		}
	}
	var last operations.InputRef
	// Four full-size reservations exceed this store's 64 MiB budget. Small
	// metadata results must retain their selected budget across reopen.
	for range 20 {
		stream, err := s.BeginStreamLimit(ctx, identity, expiry, ArrowIPC, ChunkBytes)
		if err != nil {
			t.Fatal("small result charged the full store limit", err)
		}
		if _, err := stream.Write([]byte("retained metadata")); err != nil {
			t.Fatal(err)
		}
		last, err = stream.Commit()
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if raw, err := s.Load(ctx, identity, last); err != nil || string(raw) != "retained metadata" {
		t.Fatal("bounded result did not survive reopen", err)
	}
	stream, err := s.BeginStreamLimit(ctx, identity, expiry, ArrowIPC, ChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	_, _ = stream.Write(bytes.Repeat([]byte("x"), ChunkBytes+1))
	if ref, err := stream.Commit(); !errors.Is(err, ErrLimit) || ref.ID != "" {
		t.Fatal("bounded result exceeded its reserved limit", ref, err)
	}
}
