//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"go.yaml.in/yaml/v3"
)

func TestProtectedRestoreBudgetIncludesWriterMetadata(t *testing.T) {
	for _, mode := range []string{"root", "combined-preflight", "sufficient"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			fp := protectedFingerprint(t, config)
			target := commitVerificationMultipart(t, runtime.backend, fp)
			current := commitVerificationMultipart(t, runtime.backend, fp)
			state, err := runtime.backend.readState(context.Background(), "events", runtime.backend.reader)
			if err != nil {
				t.Fatal(err)
			}
			limit := int64(32 << 20)
			if mode == "root" {
				limit = 1
			}
			if mode == "combined-preflight" {
				limit = state.info.Size + target.Bytes + current.Bytes + descriptorBytes(state.manifest.Committed) + descriptorBytes(state.manifest.History[0]) - 1
			}
			config.Acceleration.Datasets[0].Verification = &catalog.VerificationLimits{MaxBytes: limit}
			limited := openVerificationFixture(t, config, service)
			writer := attachProtectedRestoreWriter(limited.backend, service, target)
			service.takeEvents()
			got, err := limited.backend.Restore(context.Background(), restoreRequest(target, current))
			if mode == "sufficient" {
				if err != nil || got.Generation != target.Generation {
					t.Fatal("sufficient budget failed", err)
				}
			} else {
				if !errors.Is(err, errVerificationLimit) || writer.attempts.Load() != 0 {
					t.Fatal("budget exhaustion allowed publication", err)
				}
				for _, event := range service.takeEvents() {
					if protectedGenerationEvent(event) {
						t.Fatal("metadata/preflight budget exhausted after generation I/O", event)
					}
				}
			}
			state, err = limited.backend.readState(context.Background(), "events", limited.backend.reader)
			if err != nil || state.manifest.Writer != nil {
				t.Fatal("exhausted budget prevented exact-owned cleanup", err)
			}
			requireRestoreQuiescent(t, limited)
		})
	}
}

func TestProtectedRestoreReconciliationUsesOriginalDeadline(t *testing.T) {
	runtime, service, config := protectedVerificationFixture(t)
	fp := protectedFingerprint(t, config)
	target := commitRemoteRecovery(t, runtime.backend, "id", fp)
	current := commitRemoteRecovery(t, runtime.backend, "id", fp)
	config.Acceleration.Datasets[0].Limits.Timeout = 500 * time.Millisecond
	bounded := openVerificationFixture(t, config, service)
	writer := attachProtectedRestoreWriter(bounded.backend, service, target)
	var first, publication time.Time
	writer.get = func(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
		deadline, _ := ctx.Deadline()
		if first.IsZero() {
			first = deadline
		}
		if deadline != first {
			return nil, objectstore.Info{}, errors.New("restore reset its read deadline")
		}
		return writer.Client.Get(ctx, key, version)
	}
	writer.publication = func(ctx context.Context, _ func() (objectstore.Info, error)) (objectstore.Info, error) {
		publication, _ = ctx.Deadline()
		<-ctx.Done()
		return objectstore.Info{}, errObjectNetwork
	}
	got, err := bounded.backend.Restore(context.Background(), restoreRequest(target, current))
	if got.Generation != target.Generation || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrPublicationUnknown) || RestoreOutcomeOf(err) != RestoreUnknown || first.IsZero() || first != publication || writer.attempts.Load() != 1 {
		t.Fatal("reconciliation extended the deadline or lost uncertainty", first, publication, err)
	}
	requireRestoreQuiescent(t, bounded)
}

func TestProtectedRestoreRejectsSchemaAndCorruptGeneration(t *testing.T) {
	for _, mode := range []string{"schema", "target-checksum", "current-checksum"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			backend, fp := runtime.backend, protectedFingerprint(t, config)
			target := commitRemoteRecovery(t, backend, "id", fp)
			name := "id"
			if mode == "schema" {
				name = "different_column"
			}
			current := commitRemoteRecovery(t, backend, name, fp)
			if mode != "schema" {
				key := target.ObjectKey
				if mode == "current-checksum" {
					key = current.ObjectKey
				}
				service.mu.Lock()
				object := service.objects[key]
				object.data[0] ^= 1
				service.objects[key] = object
				service.mu.Unlock()
			}
			writer := attachProtectedRestoreWriter(backend, service, target)
			got, err := backend.Restore(context.Background(), restoreRequest(target, current))
			if !errors.Is(err, ErrCorrupt) || writer.attempts.Load() != 0 || RestoreOutcomeOf(err) != RestoreNotAttempted {
				t.Fatal("invalid generation published", err)
			}
			if mode == "target-checksum" && got.Generation != "" || mode != "target-checksum" && got.Generation != target.Generation {
				t.Fatal("verified candidate was cleared or unverified candidate exposed", mode, got.Generation)
			}
			requireRestoreQuiescent(t, runtime)
		})
	}
}

func TestProtectedRestoreSecondPinFailurePrecedesGenerationIO(t *testing.T) {
	runtime, service, config := protectedVerificationFixture(t)
	backend, fp := runtime.backend, protectedFingerprint(t, config)
	target := commitRemoteRecovery(t, backend, "id", fp)
	current := commitRemoteRecovery(t, backend, "id", fp)
	service.mu.Lock()
	delete(service.objects, backend.key("events", "reader-leases/"+current.Generation+".yml"))
	service.mu.Unlock()
	writer := attachProtectedRestoreWriter(backend, service, target)
	service.takeEvents()
	got, err := backend.Restore(context.Background(), restoreRequest(target, current))
	if err == nil || got.Generation != "" || writer.attempts.Load() != 0 {
		t.Fatal("failed second pin admitted a snapshot", err)
	}
	pins := 0
	for _, event := range service.takeEvents() {
		if event.operation == "pin" {
			pins++
		}
		if protectedGenerationEvent(event) {
			t.Fatal("second-pin failure allowed generation I/O", event)
		}
	}
	if pins != 1 {
		t.Fatal("test did not exercise one acquired pin", pins)
	}
	requireRestoreQuiescent(t, runtime)
}

func TestProtectedRestoreCloseFailuresPreventPublication(t *testing.T) {
	for _, boundary := range []string{"descriptor", "checksum", "footer"} {
		t.Run(boundary, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			backend, fp := runtime.backend, protectedFingerprint(t, config)
			target := commitVerificationMultipart(t, backend, fp)
			current := commitRemoteRecovery(t, backend, "id", fp)
			writer := attachProtectedRestoreWriter(backend, service, target)
			var bodies []*protectedFaultBody
			service.bodyHook = func(method, key string, body io.ReadCloser, err error) (io.ReadCloser, error) {
				match := boundary == "descriptor" && strings.HasSuffix(key, ".parts.yaml") || boundary == "checksum" && method == "get" && strings.HasSuffix(key, ".parquet") || boundary == "footer" && method == "range"
				if !match || body == nil || err != nil {
					return body, err
				}
				tracked := &protectedFaultBody{ReadCloser: body, closeErr: errObjectNetwork}
				bodies = append(bodies, tracked)
				return tracked, nil
			}
			got, err := backend.Restore(context.Background(), restoreRequest(target, current))
			if !errors.Is(err, errReaderCleanupUnknown) || got.Generation != "" || writer.attempts.Load() != 0 || len(bodies) == 0 {
				t.Fatal("body Close failure admitted publication", err)
			}
			for _, body := range bodies {
				if body.closed.Load() != 1 {
					t.Fatal("body not closed exactly once")
				}
			}
			requireRestoreQuiescent(t, runtime)
		})
	}
}

func TestProtectedRestoreNoOpNeedsExactReleaseProof(t *testing.T) {
	for _, failure := range []string{"network", "conflict", "foreign-owner"} {
		t.Run(failure, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			backend := runtime.backend
			target := commitRemoteRecovery(t, backend, "id", protectedFingerprint(t, config))
			writer := attachProtectedRestoreWriter(backend, service, target)
			writer.publication = func(_ context.Context, _ func() (objectstore.Info, error)) (objectstore.Info, error) {
				if failure == "conflict" {
					return objectstore.Info{}, objectstore.ErrConflict
				}
				if failure == "foreign-owner" {
					service.mutateManifest(t, backend.key("events", storeManifestName), func(root *objectManifest) {
						root.Writer.Owner = strings.Repeat("c", 32)
						root.Writer.ReaderReference.Generation = root.Writer.Owner
					})
				}
				return objectstore.Info{}, errObjectNetwork
			}
			got, err := backend.Restore(context.Background(), restoreRequest(target, target))
			if err == nil || got.Generation != target.Generation || RestoreOutcomeOf(err) != RestoreNotAttempted || writer.attempts.Load() != 1 {
				t.Fatal("unchanged root substituted for release proof", got.Generation, err)
			}
			requireRestoreQuiescent(t, runtime)
		})
	}
}

func TestProtectedRestoreStalledBodyCloseRetainsPinsAndOperation(t *testing.T) {
	runtime, service, config := protectedVerificationFixture(t)
	backend, fp := runtime.backend, protectedFingerprint(t, config)
	target := commitRemoteRecovery(t, backend, "id", fp)
	current := commitRemoteRecovery(t, backend, "id", fp)
	writer := attachProtectedRestoreWriter(backend, service, target)
	entered, release := make(chan struct{}), make(chan struct{})
	var closeOnce sync.Once
	unblock := func() { closeOnce.Do(func() { close(release) }) }
	defer unblock()
	var armed atomic.Bool
	service.bodyHook = func(method, key string, body io.ReadCloser, err error) (io.ReadCloser, error) {
		if method == "get" && key == target.ObjectKey && body != nil && err == nil && armed.CompareAndSwap(false, true) {
			return &protectedCloseBoundaryBody{ReadCloser: body, closed: func() { close(entered); <-release }}, nil
		}
		return body, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := backend.Restore(ctx, restoreRequest(target, current)); result <- err }()
	ownerTestWait(t, entered, "restore checksum Close")
	cancel()
	if verificationRuntimeOperations(runtime) != 1 || runtime.budget.snapshot().pins != 2 {
		t.Fatal("cancellation released custody before body Close")
	}
	select {
	case err := <-result:
		t.Fatal("restore returned before body Close", err)
	default:
	}
	unblock()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || writer.attempts.Load() != 0 {
			t.Fatal("stalled body published or lost cancellation", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("restore did not join body Close")
	}
	requireRestoreQuiescent(t, runtime)
}

func TestProtectedRestorePreservesObservedTargetThroughWriterCloseTimeout(t *testing.T) {
	runtime, service, config := protectedVerificationFixture(t)
	backend, fp := runtime.backend, protectedFingerprint(t, config)
	target := commitRemoteRecovery(t, backend, "id", fp)
	current := commitRemoteRecovery(t, backend, "id", fp)
	gate := &objectWriterGate{entered: make(chan struct{}), release: make(chan struct{})}
	defer gate.unblock()
	client := &objectWriterClient{Client: &protectedObjectClient{service}, closeGate: gate}
	backend.writeClient = func() (objectstore.Client, bool, error) { return client, true, nil }
	type result struct {
		snapshot Snapshot
		err      error
	}
	returned := make(chan result, 1)
	go func() {
		snapshot, err := backend.Restore(context.Background(), restoreRequest(target, current))
		returned <- result{snapshot, err}
	}()
	ownerTestWait(t, gate.entered, "restore writer client Close")
	select {
	case got := <-returned:
		if got.snapshot.Generation != target.Generation || !errors.Is(got.err, errReaderCleanupUnknown) || RestoreOutcomeOf(got.err) != RestoreTargetObserved {
			t.Fatal("cleanup timeout lost observed target", got.err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("bounded cleanup did not return")
	}
	if verificationRuntimeOperations(runtime) != 1 || runtime.budget.snapshot().pins != 2 || client.closes.Load() != 1 || client.closed.Load() != 0 {
		t.Fatal("bounded cleanup released unjoined custody")
	}
	// The reservation also remains enforceable when the other operation slots fill.
	ops := make([]*objectRuntimeOperation, 0, objectRuntimeOperationLimit-1)
	for range objectRuntimeOperationLimit - 1 {
		op, err := runtime.beginOperation(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ops = append(ops, op)
	}
	if _, err := runtime.beginOperation(context.Background()); !errors.Is(err, errReaderCapacity) {
		t.Fatal("unfinished restore did not consume operation capacity", err)
	}
	for _, op := range ops {
		_ = op.Close()
	}
	gate.unblock()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for verificationRuntimeOperations(runtime) != 0 {
		select {
		case <-deadline.C:
			t.Fatal("writer handback did not release operation")
		case <-time.After(time.Millisecond):
		}
	}
	if client.closes.Load() != 1 || client.closed.Load() != 1 {
		t.Fatal("writer client closed repeatedly")
	}
	requireRestoreQuiescent(t, runtime)
}

func TestRestoreOutcomeSanitizesProviderErrorsAndPreservesClassification(t *testing.T) {
	private := errors.New("https://secret-user:secret-password@private-bucket/object")
	for _, outcome := range []RestoreOutcome{RestoreNotAttempted, RestoreNotPublished, RestoreVerifiedNoOp, RestoreTargetObserved, RestoreUnknown} {
		err := WithRestoreOutcome(errors.Join(private, ErrRestoreConflict), outcome)
		if RestoreOutcomeOf(errors.Join(err, context.Canceled)) != outcome || !errors.Is(err, private) || !errors.Is(err, ErrRestoreConflict) || strings.Contains(err.Error(), "private-bucket") {
			t.Fatal("outcome changed classification or exposed provider details", err)
		}
	}
	if WithRestoreOutcome(nil, RestoreUnknown) != nil || RestoreOutcomeOf(errors.New("plain")) != RestoreNotAttempted || RestoreOutcomeOf(WithRestoreOutcome(private, "invalid")) != RestoreUnknown {
		t.Fatal("invalid outcome conversion")
	}
}

func TestProtectedRestoreStalledRenewalRetainsAllCustody(t *testing.T) {
	runtime, service, config := protectedVerificationFixture(t)
	fp := protectedFingerprint(t, config)
	target := commitRemoteRecovery(t, runtime.backend, "id", fp)
	current := commitRemoteRecovery(t, runtime.backend, "id", fp)
	config.Acceleration.Datasets[0].Limits.Timeout = 2 * time.Second
	bounded := openVerificationFixture(t, config, service)
	backend := bounded.backend
	backend.renewInterval = 10 * time.Millisecond
	gate := &objectWriterGate{entered: make(chan struct{}), release: make(chan struct{})}
	defer gate.unblock()
	client := &pointerTestClient{Client: &protectedObjectClient{service}}
	var leases, publications atomic.Int32
	client.put = func(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
		if strings.HasSuffix(key, storeManifestName) {
			raw, err := io.ReadAll(body)
			if err != nil {
				return objectstore.Info{}, err
			}
			if _, err := body.Seek(0, io.SeekStart); err != nil {
				return objectstore.Info{}, err
			}
			var root objectManifest
			if err := yaml.Unmarshal(raw, &root); err != nil {
				return objectstore.Info{}, err
			}
			if root.Writer != nil && leases.Add(1) == 3 {
				gate.block()
			}
			if root.Writer == nil && root.Committed.Generation == target.Generation {
				publications.Add(1)
			}
		}
		return client.Client.Put(ctx, key, body, size, digest, condition)
	}
	backend.writeClient = func() (objectstore.Client, bool, error) { return client, true, nil }
	var waited atomic.Bool
	service.bodyHook = func(method, key string, body io.ReadCloser, err error) (io.ReadCloser, error) {
		if method == "get" && key == target.ObjectKey && body != nil && err == nil && waited.CompareAndSwap(false, true) {
			return &protectedCloseBoundaryBody{ReadCloser: body, closed: func() { <-gate.entered }}, nil
		}
		return body, err
	}
	type result struct {
		snapshot Snapshot
		err      error
	}
	returned := make(chan result, 1)
	go func() {
		snapshot, err := backend.Restore(context.Background(), restoreRequest(target, current))
		returned <- result{snapshot, err}
	}()
	ownerTestWait(t, gate.entered, "in-flight pointer renewal")
	select {
	case got := <-returned:
		if got.snapshot.Generation != target.Generation || !errors.Is(got.err, context.DeadlineExceeded) || !errors.Is(got.err, errReaderCleanupUnknown) || RestoreOutcomeOf(got.err) != RestoreNotAttempted {
			t.Fatal("renewal timeout lost candidate or cleanup uncertainty", got.err)
		}
	case <-time.After(9 * time.Second):
		t.Fatal("stalled renewal prevented bounded restore return")
	}
	if verificationRuntimeOperations(bounded) != 1 || bounded.budget.snapshot().pins != 2 || pointerWriters(backend) != 1 || publications.Load() != 0 {
		t.Fatal("stalled renewal released custody or published")
	}
	gate.unblock()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for verificationRuntimeOperations(bounded) != 0 {
		select {
		case <-deadline.C:
			t.Fatal("renewal handback did not drain")
		case <-time.After(time.Millisecond):
		}
	}
	if client.closes.Load() != 1 {
		t.Fatal("renewal client closed repeatedly")
	}
	requireRestoreQuiescent(t, bounded)
}
