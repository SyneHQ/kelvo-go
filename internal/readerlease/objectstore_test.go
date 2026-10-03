// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package readerlease

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

type registryClientObject struct {
	raw  []byte
	info objectstore.Info
}

type registryClientFixture struct {
	mu                        sync.Mutex
	objects                   map[string]registryClientObject
	version                   int
	getHook                   func(context.Context, string, string) (io.ReadCloser, objectstore.Info, error)
	putHook                   func(context.Context, string, io.ReadSeeker, int64, string, objectstore.Condition) (objectstore.Info, error)
	afterPut                  func() error
	gets, puts, closes, heads atomic.Int64
}

func registryDigest(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func (f *registryClientFixture) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	f.gets.Add(1)
	f.mu.Lock()
	hook := f.getHook
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx, key, version)
	}
	return f.get(key, version)
}

func (f *registryClientFixture) get(key, version string) (io.ReadCloser, objectstore.Info, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	object, exists := f.objects[key]
	if !exists {
		return nil, objectstore.Info{}, objectstore.ErrNotFound
	}
	if version != "" && version != object.info.Version {
		return nil, objectstore.Info{}, objectstore.ErrConflict
	}
	info := object.info
	info.ServerTime = time.Now().UTC()
	return io.NopCloser(bytes.NewReader(append([]byte(nil), object.raw...))), info, nil
}

func (f *registryClientFixture) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
	f.puts.Add(1)
	f.mu.Lock()
	hook, after := f.putHook, f.afterPut
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx, key, body, size, digest, condition)
	}
	raw, err := io.ReadAll(body)
	if err != nil {
		return objectstore.Info{}, err
	}
	if int64(len(raw)) != size || registryDigest(raw) != digest || condition.Absent == (condition.Version != "") {
		return objectstore.Info{}, errors.New("invalid fixture write")
	}
	f.mu.Lock()
	old, exists := f.objects[key]
	if (condition.Absent && exists) || (!condition.Absent && (!exists || old.info.Version != condition.Version)) {
		f.mu.Unlock()
		return objectstore.Info{}, objectstore.ErrConflict
	}
	f.version++
	info := objectstore.Info{Size: size, SHA256: digest, Version: fmt.Sprintf("\"v%d\"", f.version), ServerTime: time.Now().UTC()}
	f.objects[key] = registryClientObject{raw: raw, info: info}
	f.mu.Unlock()
	if after != nil {
		if err := after(); err != nil {
			return objectstore.Info{}, err
		}
	}
	return info, nil
}

func (f *registryClientFixture) Head(context.Context, string, string) (objectstore.Info, error) {
	f.heads.Add(1)
	return objectstore.Info{}, errors.New("head must not be called")
}
func (f *registryClientFixture) Close() { f.closes.Add(1) }

func objectAdapterFixture(t *testing.T) (Store, *registryClientFixture, string) {
	t.Helper()
	client := &registryClientFixture{objects: make(map[string]registryClientObject)}
	adapter, err := NewObjectStore(client, "snapshots/control", "tenant")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if client.closes.Load() != 0 || client.heads.Load() != 0 {
			t.Error("adapter closed borrowed client or used unsupported operation")
		}
	})
	return adapter, client, "snapshots/control/tenant/events/reader-leases/" + strings.Repeat("a", 32) + ".yml"
}

type trackedRegistryBody struct {
	reader        io.Reader
	close         func() error
	reads, closes atomic.Int64
}

func (b *trackedRegistryBody) Read(p []byte) (int, error) { b.reads.Add(1); return b.reader.Read(p) }
func (b *trackedRegistryBody) Close() error {
	b.closes.Add(1)
	if b.close != nil {
		return b.close()
	}
	return nil
}

func TestReaderObjectStoreRejectsForeignAndNonRegistryKeysBeforeIO(t *testing.T) {
	a, client, key := objectAdapterFixture(t)
	for _, candidate := range []string{
		strings.Replace(key, "/tenant/", "/other/", 1), strings.Replace(key, "snapshots/control/", "snapshots/control2/", 1),
		strings.Replace(key, "/events/", "/../", 1), strings.Replace(key, "/events/", "/events/extra/", 1),
		strings.Replace(key, "/events/", "/%65vents/", 1), strings.Replace(key, "/reader-leases/", "/current.yaml/", 1),
		strings.TrimSuffix(key, ".yml") + ".yaml", strings.Replace(key, strings.Repeat("a", 32), strings.Repeat("A", 32), 1),
		strings.TrimSuffix(key, ".yml") + ".parquet", key + "/extra", key + "?version=1", key + "#fragment", key + "\x00",
		"snapshots/control/tenant/events/current.yaml", "snapshots/control/tenant/" + strings.Repeat("/", 1<<20),
	} {
		if body, _, err := a.Get(context.Background(), candidate); body != nil || !errors.Is(err, ErrBinding) {
			t.Fatal("invalid read key accepted", err)
		}
		if _, err := a.CompareAndSwap(context.Background(), candidate, "", []byte("data")); !errors.Is(err, ErrBinding) {
			t.Fatal("invalid write key accepted", err)
		}
	}
	if client.gets.Load() != 0 || client.puts.Load() != 0 {
		t.Fatal("scope refusal reached provider")
	}
	var missing *registryClientFixture
	for _, candidate := range []objectstore.Client{nil, missing} {
		if _, err := NewObjectStore(candidate, "snapshots", "tenant"); !errors.Is(err, ErrInvalid) {
			t.Fatal("nil client accepted", err)
		}
	}
	for _, scope := range [][2]string{{"../other", "tenant"}, {"snapshots", "other/tenant"}, {"", "tenant"}} {
		if _, err := NewObjectStore(client, scope[0], scope[1]); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid scope accepted", err)
		}
	}
}

func TestReaderObjectStoreValidatesCompleteBodyBeforeExposure(t *testing.T) {
	for _, kind := range []string{"valid", "short", "long", "digest", "missing-digest", "missing-version", "missing-time", "zero-size", "oversized", "read-error", "close-error", "nil-body", "typed-nil-body"} {
		t.Run(kind, func(t *testing.T) {
			a, client, key := objectAdapterFixture(t)
			raw := []byte("version: 1\n")
			info := objectstore.Info{Size: int64(len(raw)), SHA256: registryDigest(raw), Version: "\"v1\"", ServerTime: time.Now().UTC()}
			body := &trackedRegistryBody{reader: bytes.NewReader(raw)}
			switch kind {
			case "short":
				body.reader = bytes.NewReader(raw[:len(raw)-1])
			case "long":
				body.reader = bytes.NewReader(append(append([]byte(nil), raw...), 'x'))
			case "digest":
				info.SHA256 = strings.Repeat("0", 64)
			case "missing-digest":
				info.SHA256 = ""
			case "missing-version":
				info.Version = ""
			case "missing-time":
				info.ServerTime = time.Time{}
			case "zero-size":
				info.Size = 0
			case "oversized":
				info.Size = objectRegistryLimit + 1
			case "read-error":
				body.reader = registryErrorReader{}
			case "close-error":
				body.close = func() error { return errors.New("private credential in close") }
			}
			client.getHook = func(_ context.Context, gotKey, version string) (io.ReadCloser, objectstore.Info, error) {
				if gotKey != key || version != "" {
					t.Error("Get was not current exact-key read")
				}
				if kind == "nil-body" {
					return nil, info, nil
				}
				if kind == "typed-nil-body" {
					return (*trackedRegistryBody)(nil), info, nil
				}
				return body, info, nil
			}
			got, metadata, err := a.Get(context.Background(), key)
			if kind != "nil-body" && kind != "typed-nil-body" && body.closes.Load() != 1 {
				t.Fatal("provider body not closed exactly once")
			}
			if kind != "valid" {
				if got != nil || metadata != (Metadata{}) || err == nil || strings.Contains(err.Error(), "private") {
					t.Fatal("invalid bytes or raw error exposed", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer got.Close()
			contents, err := io.ReadAll(got)
			if err != nil || !bytes.Equal(contents, raw) || metadata.Version != info.Version || metadata.Size != info.Size || !metadata.ServerTime.Equal(info.ServerTime) {
				t.Fatal("verified body identity changed", err)
			}
		})
	}
}

type registryErrorReader struct{}

func (registryErrorReader) Read([]byte) (int, error) {
	return 0, errors.New("private credential in read")
}

type registryPretendMissing struct{}

func (registryPretendMissing) Error() string { return "private arbitrary error" }
func (registryPretendMissing) Is(error) bool { return true }

type registryHybridError struct{ target error }

func (registryHybridError) Error() string   { return "private hybrid wrapper" }
func (registryHybridError) Is(error) bool   { return true }
func (e registryHybridError) Unwrap() error { return e.target }

type registryCyclicError struct{}

func (e *registryCyclicError) Error() string { return "cycle" }
func (e *registryCyclicError) Unwrap() error { return e }

func TestReaderObjectStoreOnlyMapsDefiniteProviderOutcomes(t *testing.T) {
	for _, kind := range []string{"sentinel", "wrapped", "joined", "mixed", "wrong-operation", "enoent", "pretend", "hybrid", "nested-hybrid", "cycle", "ordinary"} {
		t.Run(kind, func(t *testing.T) {
			a, client, key := objectAdapterFixture(t)
			transform := func(target error) error {
				switch kind {
				case "sentinel":
					return target
				case "wrapped":
					return fmt.Errorf("private wrapper: %w", target)
				case "joined":
					return errors.Join(target, io.ErrUnexpectedEOF)
				case "mixed":
					return errors.Join(objectstore.ErrNotFound, objectstore.ErrConflict)
				case "wrong-operation":
					if target == objectstore.ErrNotFound {
						return objectstore.ErrConflict
					}
					return objectstore.ErrNotFound
				case "enoent":
					return os.ErrNotExist
				case "pretend":
					return registryPretendMissing{}
				case "hybrid":
					return registryHybridError{target: target}
				case "nested-hybrid":
					return fmt.Errorf("wrapped: %w", registryHybridError{target: target})
				case "cycle":
					return &registryCyclicError{}
				default:
					return errors.New("private provider credential")
				}
			}
			body := &trackedRegistryBody{reader: bytes.NewReader(nil)}
			client.getHook = func(context.Context, string, string) (io.ReadCloser, objectstore.Info, error) {
				return body, objectstore.Info{}, transform(objectstore.ErrNotFound)
			}
			got, _, err := a.Get(context.Background(), key)
			want := ErrUnavailable
			if kind == "sentinel" || kind == "wrapped" {
				want = ErrNotFound
			}
			if got != nil || !errors.Is(err, want) || strings.Contains(err.Error(), "private") || body.closes.Load() != 1 {
				t.Fatal("incorrect not-found mapping or body ownership", err)
			}
			client.putHook = func(context.Context, string, io.ReadSeeker, int64, string, objectstore.Condition) (objectstore.Info, error) {
				return objectstore.Info{}, transform(objectstore.ErrConflict)
			}
			_, err = a.CompareAndSwap(context.Background(), key, "", []byte("data"))
			want = ErrUnavailable
			if kind == "sentinel" || kind == "wrapped" {
				want = ErrConflict
			}
			if !errors.Is(err, want) || strings.Contains(err.Error(), "private") {
				t.Fatal("incorrect conditional-write mapping", err)
			}
		})
	}
}

func TestReaderObjectStoreCopiesWriteAndEnforcesExactPrecondition(t *testing.T) {
	a, client, key := objectAdapterFixture(t)
	for _, version := range []string{"", "\"old\""} {
		raw := []byte("original control document")
		expected := append([]byte(nil), raw...)
		client.putHook = func(_ context.Context, gotKey string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
			for i := range raw {
				raw[i] = 'x'
			}
			got, err := io.ReadAll(body)
			if err != nil || !bytes.Equal(got, expected) || gotKey != key || size != int64(len(expected)) || digest != registryDigest(expected) || condition != (objectstore.Condition{Absent: version == "", Version: version}) {
				t.Error("CAS changed body, digest or condition")
			}
			return objectstore.Info{Size: size, SHA256: digest, Version: "\"new\"", ServerTime: time.Now().UTC()}, nil
		}
		if _, err := a.CompareAndSwap(context.Background(), key, version, raw); err != nil {
			t.Fatal(err)
		}
	}
	before := client.puts.Load()
	for _, input := range []struct {
		version string
		body    []byte
	}{{"", nil}, {"", make([]byte, objectRegistryLimit+1)}, {"invalid version", []byte("data")}} {
		if _, err := a.CompareAndSwap(context.Background(), key, input.version, input.body); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if client.puts.Load() != before {
		t.Fatal("invalid write reached provider")
	}
}

func TestReaderObjectStoreRejectsInvalidWriteConfirmationWithoutRetry(t *testing.T) {
	for _, kind := range []string{"missing-version", "same-version", "missing-time", "wrong-size", "wrong-digest", "cancelled", "unknown-commit"} {
		t.Run(kind, func(t *testing.T) {
			a, client, key := objectAdapterFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client.putHook = func(_ context.Context, _ string, _ io.ReadSeeker, size int64, digest string, _ objectstore.Condition) (objectstore.Info, error) {
				info := objectstore.Info{Size: size, SHA256: digest, Version: "\"new\"", ServerTime: time.Now().UTC()}
				switch kind {
				case "missing-version":
					info.Version = ""
				case "same-version":
					info.Version = "\"old\""
				case "missing-time":
					info.ServerTime = time.Time{}
				case "wrong-size":
					info.Size++
				case "wrong-digest":
					info.SHA256 = strings.Repeat("0", 64)
				case "cancelled":
					cancel()
				case "unknown-commit":
					return info, io.ErrUnexpectedEOF
				}
				return info, nil
			}
			metadata, err := a.CompareAndSwap(ctx, key, "\"old\"", []byte("control document"))
			if err == nil || errors.Is(err, ErrConflict) || metadata != (Metadata{}) || client.puts.Load() != 1 || client.gets.Load() != 0 {
				t.Fatal("invalid confirmation granted success or triggered retry", err)
			}
		})
	}
}

func TestReaderObjectStoreRegistryRoundTripAndAmbiguousPin(t *testing.T) {
	a, client, _ := objectAdapterFixture(t)
	r, err := New(a, DefaultConfig("snapshots/control", "tenant"))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := NewReference("tenant", "events", strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{Reference: ref, ContentSHA256: strings.Repeat("b", 64)}
	prepare(t, r, binding)
	lease := acquire(t, r, binding)
	if err := lease.Check(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	client.afterPut = func() error { return io.ErrUnexpectedEOF }
	client.mu.Unlock()
	if l, err := r.Acquire(context.Background(), binding); l != nil || !errors.Is(err, ErrUnavailable) {
		t.Fatal("ambiguous acquire exposed reader", err)
	}
	client.mu.Lock()
	client.afterPut = nil
	client.mu.Unlock()
	if len(contents(t, r, binding).Readers) != 1 {
		t.Fatal("ambiguous committed pin was erased")
	}
}

func TestReaderObjectStoreCancelsReturnedMemoryBodyAndPreCancelledIO(t *testing.T) {
	a, client, key := objectAdapterFixture(t)
	if _, err := a.CompareAndSwap(context.Background(), key, "", []byte("data")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	body, _, err := a.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if n, err := body.Read(make([]byte, 8)); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatal(n, err)
	}
	if err := body.Close(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	gets, puts := client.gets.Load(), client.puts.Load()
	if _, _, err := a.Get(ctx, key); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := a.CompareAndSwap(ctx, key, "", []byte("data")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if gets != client.gets.Load() || puts != client.puts.Load() {
		t.Fatal("pre-cancelled call reached provider")
	}
}

type blockedRegistryReader struct {
	reader   io.Reader
	entered  chan struct{}
	release  <-chan struct{}
	once     sync.Once
	finished atomic.Bool
}

func (r *blockedRegistryReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	n, err := r.reader.Read(p)
	r.finished.Store(true)
	return n, err
}

func TestReaderObjectStoreCancellationJoinsReadAndCloseBeforeReturningCapacity(t *testing.T) {
	for _, stuck := range []string{"read", "close", "neither"} {
		t.Run(stuck, func(t *testing.T) {
			a, client, key := objectAdapterFixture(t)
			config := DefaultConfig("snapshots/control", "tenant")
			config.MaxLeases = 1
			r, err := New(a, config)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := NewReference("tenant", "events", strings.Repeat("a", 32))
			if err != nil {
				t.Fatal(err)
			}
			binding := Binding{Reference: ref, ContentSHA256: strings.Repeat("b", 64)}
			prepare(t, r, binding)
			existing, info, err := client.get(key, "")
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(existing)
			if err != nil {
				t.Fatal(err)
			}
			if err = existing.Close(); err != nil {
				t.Fatal(err)
			}
			readRelease, closeRelease, closeEntered := make(chan struct{}), make(chan struct{}), make(chan struct{})
			reader := &blockedRegistryReader{reader: bytes.NewReader(raw), entered: make(chan struct{}), release: readRelease}
			var readOnce, closeOnce sync.Once
			unblockRead := func() { readOnce.Do(func() { close(readRelease) }) }
			unblockClose := func() { closeOnce.Do(func() { close(closeRelease) }) }
			var closeFinished atomic.Bool
			body := &trackedRegistryBody{reader: reader, close: func() error {
				close(closeEntered)
				if stuck != "read" {
					unblockRead()
				}
				if stuck == "close" {
					<-closeRelease
				}
				closeFinished.Store(true)
				return nil
			}}
			client.getHook = func(context.Context, string, string) (io.ReadCloser, objectstore.Info, error) { return body, info, nil }
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			received := false
			t.Cleanup(func() {
				cancel()
				unblockRead()
				unblockClose()
				if !received {
					select {
					case <-result:
					case <-time.After(time.Second):
						t.Error("provider work remained after test cleanup")
					}
				}
			})
			go func() {
				lease, err := r.Acquire(ctx, binding)
				if lease != nil {
					_ = lease.Close()
					err = errors.New("cancelled provider read exposed a lease")
				}
				result <- err
			}()
			select {
			case <-reader.entered:
			case <-time.After(time.Second):
				t.Fatal("provider read did not start")
			}
			cancel()
			select {
			case <-closeEntered:
			case <-time.After(time.Second):
				t.Fatal("cancellation did not initiate provider Close")
			}
			if stuck != "neither" {
				select {
				case err := <-result:
					received = true
					t.Fatal("Get returned before read/close quiescence", err)
				case <-time.After(20 * time.Millisecond):
				}
				calls := client.gets.Load()
				for range 128 {
					if l, err := r.Acquire(context.Background(), binding); l != nil || !errors.Is(err, ErrCapacity) {
						t.Fatal("stalled provider released local capacity", err)
					}
				}
				if client.gets.Load() != calls || len(r.leases) != 1 {
					t.Fatal("capacity refusal started more provider work")
				}
			}
			unblockRead()
			unblockClose()
			select {
			case err := <-result:
				received = true
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("joined cancellation did not return")
			}
			if !reader.finished.Load() || !closeFinished.Load() || body.closes.Load() != 1 || len(r.leases) != 0 {
				t.Fatal("read/close lifetime or capacity ownership was lost")
			}
		})
	}
}

func TestReaderObjectStoreCloseFailureCannotConfirmAbsence(t *testing.T) {
	a, client, key := objectAdapterFixture(t)
	body := &trackedRegistryBody{reader: bytes.NewReader(nil), close: func() error { return errors.New("private close failure") }}
	client.getHook = func(context.Context, string, string) (io.ReadCloser, objectstore.Info, error) {
		return body, objectstore.Info{}, objectstore.ErrNotFound
	}
	if got, metadata, err := a.Get(context.Background(), key); got != nil || metadata != (Metadata{}) || !errors.Is(err, ErrUnavailable) {
		t.Fatal("unclosed provider result confirmed absence", err)
	}
	if body.closes.Load() != 1 {
		t.Fatal("error body was not closed exactly once")
	}
}

func TestReaderObjectStoreCancellationAtGetAndCloseBoundariesCannotExposeBody(t *testing.T) {
	for _, boundary := range []string{"get", "close"} {
		t.Run(boundary, func(t *testing.T) {
			a, client, key := objectAdapterFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			raw := []byte("control document")
			body := &trackedRegistryBody{reader: bytes.NewReader(raw), close: func() error {
				if boundary == "close" {
					cancel()
				}
				return nil
			}}
			client.getHook = func(context.Context, string, string) (io.ReadCloser, objectstore.Info, error) {
				if boundary == "get" {
					cancel()
				}
				return body, objectstore.Info{Size: int64(len(raw)), SHA256: registryDigest(raw), Version: "\"v1\"", ServerTime: time.Now().UTC()}, nil
			}
			if got, metadata, err := a.Get(ctx, key); got != nil || metadata != (Metadata{}) || !errors.Is(err, context.Canceled) {
				t.Fatal("cancelled completion exposed verified body", err)
			}
			if body.closes.Load() != 1 {
				t.Fatal("cancelled completion did not join one body Close")
			}
		})
	}
}
