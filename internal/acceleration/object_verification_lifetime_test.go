//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
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
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
	"go.yaml.in/yaml/v3"
)

type verificationDeadlineClient struct {
	*protectedObjectClient
	armed     *atomic.Bool
	deadlines *[]time.Time
}

func (c *verificationDeadlineClient) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	if c.armed.Load() && strings.HasSuffix(key, ".parquet") {
		deadline, _ := ctx.Deadline()
		*c.deadlines = append(*c.deadlines, deadline)
		if len(*c.deadlines) == 2 {
			<-ctx.Done()
		}
	}
	return c.protectedObjectClient.Get(ctx, key, version)
}

func TestProtectedVerificationOneDeadlineAcrossHistory(t *testing.T) {
	config := objectRuntimeTestConfig(t)
	config.Acceleration.Datasets[0].Verification = &catalog.VerificationLimits{MaxBytes: 32 << 20}
	config.Acceleration.Datasets[0].Limits.Timeout = 2 * time.Second
	service := &protectedObjectService{fakeSnapshotObjects: newFakeSnapshotObjects()}
	var armed atomic.Bool
	var deadlines []time.Time
	runtime, err := openObjectRuntime(config, func(catalog.ObjectLocation, catalog.ObjectCredentials, *objectstore.SharedTransport) (objectstore.Client, error) {
		return &verificationDeadlineClient{&protectedObjectClient{service}, &armed, &deadlines}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	for range 2 {
		commitRemoteRecovery(t, runtime.backend, "id", protectedFingerprint(t, config))
	}
	armed.Store(true)
	entries, err := runtime.backend.Inventory(context.Background(), "events")
	if !errors.Is(err, context.DeadlineExceeded) || entries != nil || len(deadlines) != 2 || deadlines[0].IsZero() || deadlines[0] != deadlines[1] {
		t.Fatalf("deadline reset per generation: deadlines=%v entries=%d error=%v", deadlines, len(entries), err)
	}
	if verificationRuntimeOperations(runtime) != 0 {
		t.Fatal("expired but joined operation did not drain")
	}
}

func TestProtectedVerificationStalledBodyCloseRetainsConsumer(t *testing.T) {
	runtime, service, config := protectedVerificationFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var armed atomic.Bool
	service.bodyHook = func(method, key string, body io.ReadCloser, err error) (io.ReadCloser, error) {
		if armed.Load() && method == "get" && strings.HasSuffix(key, ".parquet") && body != nil && err == nil {
			return &protectedCloseBoundaryBody{ReadCloser: body, closed: func() { close(entered); <-release }}, nil
		}
		return body, err
	}
	commitRemoteRecovery(t, runtime.backend, "id", protectedFingerprint(t, config))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	armed.Store(true)
	go func() {
		entries, err := runtime.backend.Inventory(ctx, "events")
		if entries != nil {
			err = errors.New("stalled-close inventory escaped")
		}
		result <- err
	}()
	ownerTestWait(t, entered, "checksum Close entry")
	cancel()
	if verificationRuntimeOperations(runtime) != 1 {
		t.Fatal("cancellation released the operation before body Close")
	}
	runtime.owner.mu.Lock()
	var guard *ReadGuard
	for active := range runtime.owner.guards {
		guard = active
	}
	runtime.owner.mu.Unlock()
	if guard == nil {
		t.Fatal("body Close lost its guard")
	}
	guard.mu.Lock()
	consumers := guard.outstanding
	guard.mu.Unlock()
	if consumers != 1 {
		t.Fatal("body Close did not retain consumer custody", consumers)
	}
	select {
	case err := <-result:
		t.Fatal("operation returned before synchronous body Close", err)
	default:
	}
	unblock()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("stalled body cancellation disappeared", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("body handback did not finish the operation")
	}
	if verificationRuntimeOperations(runtime) != 0 {
		t.Fatal("body handback did not release operation capacity")
	}
}

func TestProtectedVerificationRejectsLeaseLossAtFinalClose(t *testing.T) {
	for _, boundary := range []string{"checksum", "footer"} {
		t.Run(boundary, func(t *testing.T) {
			runtime, service, config := protectedVerificationFixture(t)
			var armed atomic.Bool
			var closes atomic.Int32
			var committed Snapshot
			finalRange := int32(0)
			service.bodyHook = func(method, key string, body io.ReadCloser, err error) (io.ReadCloser, error) {
				matches := boundary == "checksum" && method == "get" || boundary == "footer" && method == "range"
				if !armed.Load() || !matches || key != committed.ObjectKey || body == nil || err != nil {
					return body, err
				}
				return &protectedCloseBoundaryBody{ReadCloser: body, closed: func() {
					count := closes.Add(1)
					if boundary == "checksum" || count == finalRange {
						loseProtectedRegistryPin(t, runtime, service, committed)
					}
				}}, nil
			}
			committed = commitRemoteRecovery(t, runtime.backend, "id", protectedFingerprint(t, config))
			service.takeEvents()
			if _, err := runtime.backend.Verify(context.Background(), "events"); err != nil {
				t.Fatal("baseline verification", err)
			}
			for _, event := range service.takeEvents() {
				if event.operation == "range" && event.key == committed.ObjectKey {
					finalRange++
				}
			}
			if finalRange < 1 {
				t.Fatal("baseline did not read a footer")
			}
			armed.Store(true)
			entries, err := runtime.backend.Inventory(context.Background(), "events")
			if entries != nil || !errors.Is(err, readerlease.ErrLost) || closes.Load() == 0 {
				t.Fatalf("final body Close hid lost pin: count=%d entries=%d error=%v", closes.Load(), len(entries), err)
			}
		})
	}
}

// Only reader-removal writes are stalled. Acquisitions and other readers keep
// using the real production registry while the release body remains owned.
type verificationReleaseClient struct {
	*protectedObjectClient
	registry bool
	gate     <-chan struct{}
	entered  chan<- struct{}
	closes   *atomic.Int32
}

func (c *verificationReleaseClient) Close() { c.closes.Add(1) }

func (c *verificationReleaseClient) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
	if c.registry && strings.Contains(key, "/reader-leases/") {
		raw, err := io.ReadAll(body)
		if err != nil {
			return objectstore.Info{}, err
		}
		if _, err := body.Seek(0, io.SeekStart); err != nil {
			return objectstore.Info{}, err
		}
		var old, next struct {
			Readers []any `yaml:"readers"`
		}
		c.service.mu.Lock()
		previous := bytes.Clone(c.service.objects[key].data)
		c.service.mu.Unlock()
		if err := yaml.Unmarshal(previous, &old); err != nil {
			return objectstore.Info{}, err
		}
		if err := yaml.Unmarshal(raw, &next); err != nil {
			return objectstore.Info{}, err
		}
		if len(next.Readers) < len(old.Readers) {
			c.entered <- struct{}{}
			<-c.gate // Deliberately model provider work ignoring cancellation.
		}
	}
	return c.protectedObjectClient.Put(ctx, key, body, size, digest, condition)
}

func TestProtectedVerificationCleanupTimeoutRetainsOperationCapacity(t *testing.T) {
	config := objectRuntimeTestConfig(t)
	config.Acceleration.Datasets[0].Verification = &catalog.VerificationLimits{MaxBytes: 32 << 20}
	config.Acceleration.Datasets[0].Limits.Timeout = 2 * time.Minute
	service := &protectedObjectService{fakeSnapshotObjects: newFakeSnapshotObjects()}
	gate, entered := make(chan struct{}), make(chan struct{}, objectRuntimeOperationLimit)
	var once sync.Once
	unblock := func() { once.Do(func() { close(gate) }) }
	var clientCloses atomic.Int32
	runtime, err := openObjectRuntime(config, func(_ catalog.ObjectLocation, credentials catalog.ObjectCredentials, _ *objectstore.SharedTransport) (objectstore.Client, error) {
		return &verificationReleaseClient{protectedObjectClient: &protectedObjectClient{service},
			registry: credentials == config.Acceleration.ObjectStorage.ReaderRegistry.Credentials,
			gate:     gate, entered: entered, closes: &clientCloses}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		unblock()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runtime.Close(ctx) // Earlier cleanup uncertainty is intentionally sticky.
		select {
		case <-runtime.Quiesced():
		case <-ctx.Done():
			t.Error("uncertain verification fixture did not eventually quiesce")
		}
	})
	commitRemoteRecovery(t, runtime.backend, "id", protectedFingerprint(t, config))
	results := make(chan error, objectRuntimeOperationLimit)
	for range objectRuntimeOperationLimit {
		go func() {
			entries, err := runtime.backend.Inventory(context.Background(), "events")
			if entries != nil {
				err = errors.New("cleanup uncertainty published inventory")
			}
			results <- err
		}()
		// Serialize only acquisition, avoiding a fake-service CAS storm. All
		// sixty-four real five-second cleanup waits overlap, not 64 x 5s.
		select {
		case <-entered:
		case err := <-results:
			if !errors.Is(err, errReaderCleanupUnknown) {
				t.Fatal("verification failed before its release boundary", err)
			}
			// A previous request may time out while the next acquires. Preserve
			// its result and continue waiting for this new release boundary.
			results <- err
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("next verification did not reach release")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("verification did not reach release")
		}
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for range objectRuntimeOperationLimit {
		select {
		case err := <-results:
			if !errors.Is(err, errReaderCleanupUnknown) {
				t.Fatal("bounded guard cleanup reported success", err)
			}
		case <-deadline.C:
			t.Fatal("bounded guard cleanup did not return")
		}
	}
	if verificationRuntimeOperations(runtime) != objectRuntimeOperationLimit {
		t.Fatal("timed-out cleanup released operation reservations")
	}
	service.takeEvents()
	if entries, err := runtime.backend.Inventory(context.Background(), "events"); entries != nil || !errors.Is(err, errReaderCapacity) {
		t.Fatal("unjoined cleanup did not enforce operation capacity", err)
	}
	if events := service.takeEvents(); len(events) != 0 {
		t.Fatal("capacity refusal performed provider I/O", events)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = runtime.Close(ctx)
	cancel()
	if !errors.Is(err, errReaderCleanupUnknown) || clientCloses.Load() != 0 {
		t.Fatal("runtime closed providers before release handback", err, clientCloses.Load())
	}
	unblock()
	select {
	case <-runtime.Quiesced():
	case <-time.After(10 * time.Second):
		t.Fatal("runtime did not drain after all providers handed back")
	}
	if verificationRuntimeOperations(runtime) != 0 || clientCloses.Load() != 3 {
		t.Fatal("final handback leaked operation slots or provider clients", verificationRuntimeOperations(runtime), clientCloses.Load())
	}
}
