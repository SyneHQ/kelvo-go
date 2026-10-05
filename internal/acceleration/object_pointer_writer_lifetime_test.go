//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

func TestPointerWriterConcurrentFinishJoinsClientAndPreservesCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "cancel-during-close"}[cancelled], func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			backend := runtime.backend
			commitRemoteRecovery(t, backend, "id", protectedFingerprint(t, config))
			backend.renewInterval = time.Hour
			closing := pointerGate(t)
			client := &pointerTestClient{Client: &protectedObjectClient{service}, closeGate: closing}
			backend.writeClient = func() (objectstore.Client, bool, error) { return client, true, nil }
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			op, err := runtime.beginOperation(ctx)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := backend.beginPointerWriter(op.Context(), "events", newVerificationReader(runtime.dataClient(), 1<<20))
			defer func() {
				closing.unblock()
				if tx != nil {
					_ = finishTestPointer(t, tx)
				}
				_ = op.Close()
			}()
			if err != nil {
				t.Fatal(err)
			}
			wait, stop := context.WithTimeout(context.Background(), objectWriterWait)
			defer stop()
			const waiters = 8
			results := make(chan error, waiters)
			for range waiters {
				go func() { results <- tx.finishPointerWriter(wait) }()
			}
			awaitObjectWriterSignal(t, closing.entered, "pointer close shared by finish callers")
			if client.closes.Load() != 1 || pointerWriters(backend) != 1 || verificationRuntimeOperations(runtime) != 1 {
				t.Fatal("concurrent finish lost exclusive cleanup or custody")
			}
			select {
			case err := <-results:
				t.Fatal("finish returned before the owned client joined", err)
			default:
			}
			cause := errors.New("fixture cancellation during pointer close")
			if cancelled {
				cancel(cause)
			}
			closing.unblock()
			for range waiters {
				err := verificationAwait(t, results)
				if cancelled && !errors.Is(err, cause) || !cancelled && err != nil {
					t.Fatal("finish lost operational cause or manufactured cancellation", err)
				}
			}
			if err := finishTestPointer(t, tx); cancelled && !errors.Is(err, cause) || !cancelled && err != nil {
				t.Fatal("repeated finish changed cleanup outcome", err)
			}
			if client.closes.Load() != 1 || pointerWriters(backend) != 0 || verificationRuntimeOperations(runtime) != 1 {
				t.Fatal("joined writer did not preserve the caller's operation ownership")
			}
		})
	}
}

func TestPointerWriterDrainRetainsLateFactoryAndClient(t *testing.T) {
	runtime, _, clients := objectRuntimeTestOpen(t)
	backend := runtime.backend
	opening, closing := pointerGate(t), pointerGate(t)
	client := &pointerTestClient{Client: backend.reader, closeGate: closing}
	backend.writeClient = func() (objectstore.Client, bool, error) {
		opening.block()
		return client, true, nil
	}
	op, err := runtime.beginOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		tx  *objectTransaction
		err error
	}
	results := make(chan result, 1)
	go func() {
		tx, err := backend.beginPointerWriter(op.Context(), "events", newVerificationReader(runtime.dataClient(), 1<<20))
		results <- result{tx, err}
	}()
	var tx *objectTransaction
	received := false
	defer func() {
		opening.unblock()
		closing.unblock()
		runtime.startClose()
		if !received {
			tx = verificationAwait(t, results).tx
		}
		if tx != nil {
			_ = finishTestPointer(t, tx)
		}
		_ = op.Close()
	}()
	awaitObjectWriterSignal(t, opening.entered, "pointer opening before runtime drain")
	short, stop := context.WithTimeout(context.Background(), time.Millisecond)
	err = runtime.Close(short)
	stop()
	if !errors.Is(err, errReaderCleanupUnknown) {
		t.Fatal("runtime drain did not report retained late construction", err)
	}
	got := verificationAwait(t, results)
	received = true
	tx = got.tx
	if tx == nil || got.err == nil || pointerWriters(backend) != 1 || verificationRuntimeOperations(runtime) != 1 {
		t.Fatal("drain released late construction custody", got.err)
	}
	if _, err := tx.readPointerState(context.Background()); !errors.Is(err, errReaderInvalid) {
		t.Fatal("partial pointer became a usable reader", err)
	}
	opening.unblock()
	awaitObjectWriterSignal(t, closing.entered, "late pointer client during drain")
	assertResourcesOpen := func() {
		t.Helper()
		select {
		case <-runtime.Quiesced():
			t.Fatal("runtime completed before retained work returned")
		default:
		}
		for _, owned := range clients {
			if owned.closes.Load() != 0 {
				t.Fatal("runtime closed shared providers before operation handback")
			}
		}
	}
	assertResourcesOpen()
	if pointerWriters(backend) != 1 || client.closes.Load() != 1 {
		t.Fatal("late client close escaped writer tracking")
	}
	closing.unblock()
	if err := finishTestPointer(t, tx); err == nil {
		t.Fatal("runtime cancellation disappeared from writer outcome")
	}
	if pointerWriters(backend) != 0 || verificationRuntimeOperations(runtime) != 1 {
		t.Fatal("writer finish prematurely returned the caller's operation")
	}
	assertResourcesOpen()
	_ = op.Close()
	awaitObjectWriterSignal(t, runtime.Quiesced(), "runtime drain after pointer handback")
	for _, owned := range clients {
		if owned.closes.Load() != 1 {
			t.Fatal("runtime provider did not close once after operation handback")
		}
	}
	if err := ownerTestClose(t, runtime.Close); !errors.Is(err, errReaderCleanupUnknown) {
		t.Fatal("late handback cleared an earlier uncertain shutdown", err)
	}
}
