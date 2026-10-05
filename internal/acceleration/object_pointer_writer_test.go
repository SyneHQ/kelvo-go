//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"go.yaml.in/yaml/v3"
)

type pointerTestClient struct {
	objectstore.Client
	closeGate *objectWriterGate
	closes    atomic.Int32
	put       func(context.Context, string, io.ReadSeeker, int64, string, objectstore.Condition) (objectstore.Info, error)
}

func (c *pointerTestClient) Close() {
	c.closes.Add(1)
	if c.closeGate != nil {
		c.closeGate.block()
	}
}

func (c *pointerTestClient) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
	if c.put != nil {
		return c.put(ctx, key, body, size, digest, condition)
	}
	return c.Client.Put(ctx, key, body, size, digest, condition)
}

func pointerGate(t *testing.T) *objectWriterGate {
	t.Helper()
	gate := &objectWriterGate{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(gate.unblock)
	return gate
}

func pointerWriters(backend *objectBackend) int {
	backend.writers.mu.Lock()
	defer backend.writers.mu.Unlock()
	return len(backend.writers.pending)
}

func finishTestPointer(t *testing.T, tx *objectTransaction) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), objectWriterWait)
	defer cancel()
	err := tx.finishPointerWriter(ctx)
	select {
	case <-tx.pointerQuiesced():
	case <-ctx.Done():
		t.Fatal("pointer writer did not hand back")
	}
	return err
}

func TestPointerWriterClaimsWithoutStagingOrRegistryMutation(t *testing.T) {
	runtime, service, config := protectedVerificationFixture(t)
	backend := runtime.backend
	committed := commitRemoteRecovery(t, backend, "id", protectedFingerprint(t, config))
	backend.renewInterval = time.Hour
	before, err := backend.readState(context.Background(), "events", backend.reader)
	if err != nil {
		t.Fatal(err)
	}
	service.takeEvents()
	client := &pointerTestClient{Client: &protectedObjectClient{service}}
	backend.writeClient = func() (objectstore.Client, bool, error) { return client, true, nil }
	op, err := runtime.beginOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer op.Close()
	meter := newVerificationReader(runtime.dataClient(), 1<<20)
	tx, err := backend.beginPointerWriter(op.Context(), "events", meter)
	if tx != nil {
		defer finishTestPointer(t, tx)
	}
	if err != nil || tx == nil {
		t.Fatal("pointer opening failed", err)
	}
	if tx.local != nil || tx.owner == committed.Generation || tx.readerReference == nil || tx.readerReference.Generation != tx.owner || !storeGenerationID.MatchString(tx.owner) {
		t.Fatal("pointer opening reused data generation or created staging")
	}
	remaining := meter.Remaining()
	state, err := tx.readPointerState(op.Context())
	if err != nil || !tx.ownsWriter(state) || !sameObjectCommit(state.manifest.Committed, before.manifest.Committed) || meter.Remaining() >= remaining {
		t.Fatal("pointer root read lost fence, content or shared accounting", err)
	}
	if err := finishTestPointer(t, tx); err != nil {
		t.Fatal("normal pointer cleanup was poisoned by internal cancellation", err)
	}
	if pointerWriters(backend) != 0 || client.closes.Load() != 1 {
		t.Fatal("pointer handback was not once-only")
	}
	after, err := backend.readState(op.Context(), "events", backend.reader)
	if err != nil || after.manifest.Writer != nil || !sameObjectCommit(after.manifest.Committed, before.manifest.Committed) {
		t.Fatal("pointer cleanup changed data identity", err)
	}
	for _, event := range service.takeEvents() {
		if event.operation == "stage" || event.operation == "seal" || event.operation == "payload" || strings.Contains(event.key, "/reader-leases/") {
			t.Fatal("pointer opening or cleanup changed protected payload/registry", event)
		}
	}
}

func TestPointerWriterLateFactoryAndOwnedCloseRemainRegistered(t *testing.T) {
	for _, partialError := range []bool{false, true} {
		t.Run(map[bool]string{false: "late-success", true: "partial-error"}[partialError], func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			backend := runtime.backend
			commitRemoteRecovery(t, backend, "id", protectedFingerprint(t, config))
			opening, closing := pointerGate(t), pointerGate(t)
			client := &pointerTestClient{Client: &protectedObjectClient{service}, closeGate: closing}
			factoryErr := errors.New("fixture partial factory failure")
			backend.writeClient = func() (objectstore.Client, bool, error) {
				opening.block()
				if partialError {
					return client, true, factoryErr
				}
				return client, true, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			op, err := runtime.beginOperation(ctx)
			if err != nil {
				t.Fatal(err)
			}
			type result struct {
				tx  *objectTransaction
				err error
			}
			finished := make(chan result, 1)
			go func() {
				tx, err := backend.beginPointerWriter(op.Context(), "events", newVerificationReader(runtime.dataClient(), 1<<20))
				finished <- result{tx, err}
			}()
			var tx *objectTransaction
			received := false
			defer func() {
				opening.unblock()
				closing.unblock()
				cancel()
				if !received {
					tx = verificationAwait(t, finished).tx
				}
				if tx != nil {
					_ = finishTestPointer(t, tx)
				}
				_ = op.Close()
			}()
			awaitObjectWriterSignal(t, opening.entered, "pointer factory")
			cancel()
			got := verificationAwait(t, finished)
			received = true
			tx = got.tx
			if got.tx == nil || !errors.Is(got.err, context.Canceled) || pointerWriters(backend) != 1 || verificationRuntimeOperations(runtime) != 1 {
				t.Fatal("cancelled opening lost partial writer custody", got.err)
			}
			if _, err := tx.readPointerState(context.Background()); !errors.Is(err, errReaderInvalid) {
				t.Fatal("partial opening exposed a reader", err)
			}
			opening.unblock()
			awaitObjectWriterSignal(t, closing.entered, "late pointer client close")
			short, stop := context.WithTimeout(context.Background(), time.Millisecond)
			err = got.tx.finishPointerWriter(short)
			stop()
			if !errors.Is(err, errReaderCleanupUnknown) || pointerWriters(backend) != 1 {
				t.Fatal("bounded finish released a blocked client", err)
			}
			closing.unblock()
			err = finishTestPointer(t, got.tx)
			if !errors.Is(err, context.Canceled) || partialError && !errors.Is(err, factoryErr) || client.closes.Load() != 1 || pointerWriters(backend) != 0 {
				t.Fatal("late client handback lost original cause or closed twice", err)
			}
		})
	}
}

func TestPointerWriterStrictReleaseProofAndAmbiguousConfirmation(t *testing.T) {
	for _, mode := range []string{"missing-writer", "changed-reference", "expired-writer", "conflict", "applied-lost-response", "not-applied", "changed-history"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			backend := runtime.backend
			commitRemoteRecovery(t, backend, "id", protectedFingerprint(t, config))
			backend.renewInterval = time.Hour
			client := &pointerTestClient{Client: &protectedObjectClient{service}}
			var releasing atomic.Bool
			client.put = func(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
				if !releasing.Load() {
					return client.Client.Put(ctx, key, body, size, digest, condition)
				}
				if mode == "conflict" {
					return objectstore.Info{}, objectstore.ErrConflict
				}
				if mode == "not-applied" {
					return objectstore.Info{}, errors.New("fixture lost release response")
				}
				info, err := client.Client.Put(ctx, key, body, size, digest, condition)
				if err == nil && (mode == "applied-lost-response" || mode == "changed-history") {
					if mode == "changed-history" {
						service.mutateManifest(t, key, func(root *objectManifest) { root.HistoryTruncated = !root.HistoryTruncated })
					}
					return info, errors.New("fixture lost release response")
				}
				return info, err
			}
			backend.writeClient = func() (objectstore.Client, bool, error) { return client, true, nil }
			op, err := runtime.beginOperation(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer op.Close()
			tx, err := backend.beginPointerWriter(op.Context(), "events", newVerificationReader(runtime.dataClient(), 1<<20))
			if tx != nil {
				defer finishTestPointer(t, tx)
			}
			if err != nil {
				t.Fatal(err)
			}
			tx.stopRenewal()
			switch mode {
			case "missing-writer":
				service.mutateManifest(t, backend.key("events", storeManifestName), func(root *objectManifest) { root.Writer = nil })
			case "changed-reference":
				service.mutateManifest(t, backend.key("events", storeManifestName), func(root *objectManifest) { root.Writer.ReaderReference.Incarnation = strings.Repeat("c", 64) })
			case "expired-writer":
				service.mutateManifest(t, backend.key("events", storeManifestName), func(root *objectManifest) { root.Writer.ExpiresAt = time.Now().Add(-time.Hour) })
			}
			releasing.Store(true)
			err = finishTestPointer(t, tx)
			if mode == "applied-lost-response" {
				if err != nil {
					t.Fatal("exact released root was not confirmed", err)
				}
			} else if err == nil {
				t.Fatal("unproven writer release was reported successful")
			}
			if mode == "expired-writer" && !errors.Is(err, ErrLeaseLost) {
				t.Fatal("clearing an expired exact writer cleared the loss of fencing", err)
			}
			if client.closes.Load() != 1 || pointerWriters(backend) != 0 {
				t.Fatal("failed release retained joined local work")
			}
		})
	}
}

func TestPointerWriterCleanupAfterBudgetExhaustionIsRootOnly(t *testing.T) {
	runtime, service, config := protectedVerificationFixture(t)
	backend := runtime.backend
	commitRemoteRecovery(t, backend, "id", protectedFingerprint(t, config))
	backend.renewInterval = time.Hour
	op, err := runtime.beginOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer op.Close()
	meter := newVerificationReader(runtime.dataClient(), 1<<20)
	tx, err := backend.beginPointerWriter(op.Context(), "events", meter)
	if tx != nil {
		defer finishTestPointer(t, tx)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := meter.Preflight(meter.Remaining() + 1); !errors.Is(err, errVerificationLimit) {
		t.Fatal("fixture failed to exhaust budget", err)
	}
	service.takeEvents()
	if _, err := tx.readPointerState(op.Context()); !errors.Is(err, errVerificationLimit) {
		t.Fatal("normal root reader bypassed exhausted meter", err)
	}
	if err := finishTestPointer(t, tx); err != nil {
		t.Fatal("exhausted budget prevented exact-owned root cleanup", err)
	}
	for _, event := range service.takeEvents() {
		if event.key != backend.key("events", storeManifestName) {
			t.Fatal("cleanup budget exception reached non-root data", event)
		}
	}
	service.mu.Lock()
	raw := append([]byte(nil), service.objects[backend.key("events", storeManifestName)].data...)
	service.mu.Unlock()
	var manifest objectManifest
	if err := yaml.Unmarshal(raw, &manifest); err != nil || manifest.Writer != nil {
		t.Fatal("cleanup did not release the exact writer", err)
	}
}
