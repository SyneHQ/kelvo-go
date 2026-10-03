// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package readerlease

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }

type fakeObject struct {
	data    []byte
	version string
}
type fakeStore struct {
	mu                          sync.Mutex
	objects                     map[string]fakeObject
	version                     int
	now                         func() time.Time
	offset                      time.Duration
	missingDate, missingVersion bool
	getHook                     func(context.Context, string) error
	casHook                     func(context.Context, string, string, []byte) error
	afterCAS                    func(context.Context, string) error
	calls                       atomic.Int64
}

func (s *fakeStore) change(fn func(*fakeStore)) { s.mu.Lock(); defer s.mu.Unlock(); fn(s) }
func (s *fakeStore) metadata(o fakeObject) Metadata {
	m := Metadata{Version: o.version, Size: int64(len(o.data)), ServerTime: s.now().Add(s.offset).UTC()}
	if s.missingDate {
		m.ServerTime = time.Time{}
	}
	if s.missingVersion {
		m.Version = ""
	}
	return m
}
func (s *fakeStore) Get(ctx context.Context, key string) (io.ReadCloser, Metadata, error) {
	s.calls.Add(1)
	s.mu.Lock()
	hook := s.getHook
	s.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, key); err != nil {
			return nil, Metadata{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[key]
	if !ok {
		return nil, Metadata{}, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), o.data...))), s.metadata(o), nil
}
func (s *fakeStore) CompareAndSwap(ctx context.Context, key, version string, raw []byte) (Metadata, error) {
	s.calls.Add(1)
	s.mu.Lock()
	hook, after := s.casHook, s.afterCAS
	s.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, key, version, raw); err != nil {
			return Metadata{}, err
		}
	}
	s.mu.Lock()
	old, exists := s.objects[key]
	if (version == "" && exists) || (version != "" && (!exists || version != old.version)) {
		s.mu.Unlock()
		return Metadata{}, ErrConflict
	}
	s.version++
	o := fakeObject{data: append([]byte(nil), raw...), version: strconv.Itoa(s.version)}
	s.objects[key] = o
	meta := s.metadata(o)
	s.mu.Unlock()
	if after != nil {
		if err := after(ctx, key); err != nil {
			return Metadata{}, err
		}
	}
	return meta, nil
}

func testRegistry(t *testing.T) (*Registry, *fakeStore, *fakeClock, Binding) {
	t.Helper()
	clock := &fakeClock{now: time.Now()}
	store := &fakeStore{objects: map[string]fakeObject{}, now: clock.Now}
	cfg := DefaultConfig("snapshots", "tenant")
	r, err := New(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.now = clock.Now
	ref, err := NewReference("tenant", "events", strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	return r, store, clock, Binding{Reference: ref, ContentSHA256: strings.Repeat("b", 64)}
}
func prepare(t *testing.T, r *Registry, b Binding) {
	t.Helper()
	if err := r.Stage(context.Background(), b.Reference, strings.Repeat("c", 32)); err != nil {
		t.Fatal(err)
	}
	if err := r.Seal(context.Background(), b, strings.Repeat("c", 32)); err != nil {
		t.Fatal(err)
	}
}
func acquire(t *testing.T, r *Registry, b Binding) *Lease {
	t.Helper()
	l, err := r.Acquire(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}
func contents(t *testing.T, r *Registry, b Binding) document {
	t.Helper()
	s, err := r.read(context.Background(), b.Reference)
	if err != nil {
		t.Fatal(err)
	}
	return s.document
}

func TestReaderRegistryConstructorTimingAndScope(t *testing.T) {
	r, s, _, b := testRegistry(t)
	for name, change := range map[string]func(*Config){
		"zero ttl": func(c *Config) { c.LeaseDuration = 0 }, "long ttl": func(c *Config) { c.LeaseDuration = 2 * time.Hour },
		"late renewal": func(c *Config) { c.RenewInterval = c.LeaseDuration }, "unbounded operation": func(c *Config) { c.OperationTimeout = 0 },
		"renewal overlaps operation": func(c *Config) { c.RenewInterval = c.OperationTimeout }, "missing clock bound": func(c *Config) { c.ClockUncertainty = 0 },
		"insufficient deadline margin": func(c *Config) { c.LeaseDuration = 20 * time.Second; c.RenewInterval = 6 * time.Second },
		"unbounded readers":            func(c *Config) { c.MaxReaders = 129 }, "unbounded payload": func(c *Config) { c.MaxBytes = 1 << 20 },
		"unbounded local leases": func(c *Config) { c.MaxLeases = 129 }, "zero local leases": func(c *Config) { c.MaxLeases = 0 },
		"unbounded retries": func(c *Config) { c.MaxAttempts = 33 }, "traversal": func(c *Config) { c.Prefix = "../other" },
		"foreign tenant": func(c *Config) { c.Tenant = "../tenant" },
	} {
		t.Run(name, func(t *testing.T) {
			c := r.config
			change(&c)
			if _, err := New(s, c); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
	if _, err := New(nil, r.config); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	var missing *fakeStore
	if _, err := New(missing, r.config); !errors.Is(err, ErrInvalid) {
		t.Fatal("typed nil store accepted", err)
	}
	b.Tenant = "other"
	before := s.calls.Load()
	if _, err := r.Acquire(context.Background(), b); !errors.Is(err, ErrBinding) {
		t.Fatal(err)
	}
	if s.calls.Load() != before {
		t.Fatal("cross-tenant reference reached provider")
	}
}

func TestReaderRegistryStagesSealsAndFencesImmutableBinding(t *testing.T) {
	r, _, _, b := testRegistry(t)
	ctx := context.Background()
	owner := strings.Repeat("c", 32)
	if _, err := r.Acquire(ctx, b); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := r.Stage(ctx, b.Reference, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Acquire(ctx, b); !errors.Is(err, ErrUnsealed) {
		t.Fatal(err)
	}
	if err := r.Stage(ctx, b.Reference, strings.Repeat("d", 32)); !errors.Is(err, ErrFence) {
		t.Fatal(err)
	}
	if err := r.Seal(ctx, b, strings.Repeat("d", 32)); !errors.Is(err, ErrFence) {
		t.Fatal(err)
	}
	if err := r.Seal(ctx, b, owner); err != nil {
		t.Fatal(err)
	}
	before := contents(t, r, b).Sequence
	if err := r.Stage(ctx, b.Reference, owner); err != nil {
		t.Fatal(err)
	}
	if err := r.Seal(ctx, b, owner); err != nil {
		t.Fatal(err)
	}
	if contents(t, r, b).Sequence != before {
		t.Fatal("idempotent retry changed registry")
	}
	for _, change := range []func(*Binding){func(b *Binding) { b.ContentSHA256 = strings.Repeat("e", 64) }, func(b *Binding) { b.Incarnation = strings.Repeat("e", 64) }} {
		bad := b
		change(&bad)
		if _, err := r.Acquire(ctx, bad); !errors.Is(err, ErrBinding) {
			t.Fatal(err)
		}
		if err := r.Seal(ctx, bad, owner); !errors.Is(err, ErrBinding) {
			t.Fatal(err)
		}
	}
}

func TestReaderRegistryConcurrentCASAndCapacityPreservePins(t *testing.T) {
	r, s, _, b := testRegistry(t)
	prepare(t, r, b)
	r.config.MaxReaders = 2
	other, err := New(s, r.config)
	if err != nil {
		t.Fatal(err)
	}
	other.now = r.now
	var conflicts atomic.Int32
	s.change(func(s *fakeStore) {
		s.casHook = func(context.Context, string, string, []byte) error {
			if conflicts.Add(1) == 1 {
				return ErrConflict
			}
			return nil
		}
	})
	type outcome struct {
		lease *Lease
		err   error
	}
	results := make(chan outcome, 2)
	for _, registry := range []*Registry{r, other} {
		go func(registry *Registry) {
			l, e := registry.Acquire(context.Background(), b)
			results <- outcome{l, e}
		}(registry)
	}
	var leases []*Lease
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Error(result.err)
			continue
		}
		l := result.lease
		leases = append(leases, l)
		t.Cleanup(func() { _ = l.Close() })
	}
	if t.Failed() {
		return
	}
	if len(contents(t, r, b).Readers) != 2 {
		t.Fatal("concurrent pins lost")
	}
	if _, err = r.Acquire(context.Background(), b); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if err = leases[0].Close(); err != nil {
		t.Fatal(err)
	}
	if len(contents(t, r, b).Readers) != 1 {
		t.Fatal("release removed another reader")
	}
	if err = leases[0].Close(); err != nil {
		t.Fatal(err)
	}
	if err = leases[1].Check(); err != nil {
		t.Fatal(err)
	}
	s.change(func(s *fakeStore) {
		s.casHook = func(context.Context, string, string, []byte) error { return ErrConflict }
	})
	before := s.calls.Load()
	if _, err = r.Acquire(context.Background(), b); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if s.calls.Load()-before != int64(2*r.config.MaxAttempts) {
		t.Fatal("retry budget not enforced")
	}
	s.change(func(s *fakeStore) { s.casHook = nil })
}

func TestReaderRegistrySealRaceCannotChangeContent(t *testing.T) {
	r, s, _, b := testRegistry(t)
	owner := strings.Repeat("c", 32)
	if err := r.Stage(context.Background(), b.Reference, owner); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	s.change(func(s *fakeStore) {
		s.casHook = func(ctx context.Context, key, version string, raw []byte) error {
			var raced bool
			once.Do(func() {
				raced = true
				s.change(func(s *fakeStore) { s.casHook = nil })
				other := b
				other.ContentSHA256 = strings.Repeat("d", 64)
				if err := r.Seal(ctx, other, owner); err != nil {
					t.Error(err)
				}
			})
			if raced {
				return ErrConflict
			}
			return nil
		}
	})
	if err := r.Seal(context.Background(), b, owner); !errors.Is(err, ErrBinding) {
		t.Fatal(err)
	}
	if contents(t, r, b).ContentSHA256 != strings.Repeat("d", 64) {
		t.Fatal("seal race changed immutable content")
	}
}

func TestReaderRegistryUnknownAcquireOutcomeRetainsPinWithoutReader(t *testing.T) {
	r, s, _, b := testRegistry(t)
	prepare(t, r, b)
	s.change(func(s *fakeStore) { s.afterCAS = func(context.Context, string) error { return io.ErrUnexpectedEOF } })
	if l, err := r.Acquire(context.Background(), b); l != nil || !errors.Is(err, ErrUnavailable) {
		t.Fatal(l, err)
	}
	if len(r.leases) != 0 {
		t.Fatal("completed ambiguous acquisition retained local capacity")
	}
	s.change(func(s *fakeStore) { s.afterCAS = nil })
	if len(contents(t, r, b).Readers) != 1 {
		t.Fatal("ambiguous committed pin was erased")
	}
}

func TestReaderRegistryStrictPayloadAndProviderIdentity(t *testing.T) {
	for _, kind := range []string{"unknown", "duplicate", "alias", "extra-document", "oversized", "null", "missing-date", "missing-version", "enoent"} {
		t.Run(kind, func(t *testing.T) {
			r, s, _, b := testRegistry(t)
			prepare(t, r, b)
			key, _ := r.key(b.Reference)
			s.change(func(s *fakeStore) {
				o := s.objects[key]
				switch kind {
				case "unknown":
					o.data = append(o.data, []byte("unexpected: true\n")...)
				case "duplicate":
					o.data = append(o.data, []byte("version: 1\n")...)
				case "alias":
					o.data = []byte("version: &v 1\nsequence: *v\n")
				case "extra-document":
					o.data = append(o.data, []byte("---\nversion: 1\n")...)
				case "oversized":
					o.data = bytes.Repeat([]byte("x"), r.config.MaxBytes+1)
				case "null":
					o.data = []byte("null\n")
				case "missing-date":
					s.missingDate = true
				case "missing-version":
					s.missingVersion = true
				case "enoent":
					s.getHook = func(context.Context, string) error { return os.ErrNotExist }
				}
				s.objects[key] = o
			})
			if l, err := r.Acquire(context.Background(), b); l != nil || err == nil {
				t.Fatal("invalid provider state granted reader", err)
			}
			if len(r.leases) != 0 {
				t.Fatal("failed acquisition retained local capacity")
			}
		})
	}
}

func TestReaderRegistryRawENOENTCannotCreateProtectionMetadata(t *testing.T) {
	r, s, _, b := testRegistry(t)
	s.change(func(s *fakeStore) { s.getHook = func(context.Context, string) error { return os.ErrNotExist } })
	if err := r.Stage(context.Background(), b.Reference, strings.Repeat("c", 32)); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if s.calls.Load() != 1 || len(s.objects) != 0 {
		t.Fatal("raw ENOENT caused registry creation")
	}
}

func TestReaderRegistryRejectsScalarCoercionAndNoncanonicalFences(t *testing.T) {
	r, _, clock, b := testRegistry(t)
	prepare(t, r, b)
	d := contents(t, r, b)
	d.Sequence = 3
	d.Readers = []entry{{ID: strings.Repeat("d", 64), Owner: r.owner, Sequence: 2,
		RenewedAt: clock.Now().UTC(), ExpiresAt: clock.Now().Add(r.config.LeaseDuration).UTC()}}
	raw, err := encode(d, r.config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decode(raw, r.config); err != nil {
		t.Fatal("canonical registry rejected", err)
	}
	for name, replacement := range map[string][2]string{
		"fractional version":        {"version: 1\n", "version: 1.9\n"},
		"quoted version":            {"version: 1\n", "version: \"1\"\n"},
		"tagged fractional version": {"version: 1\n", "version: !!float 1.0\n"},
		"fractional document fence": {"sequence: 3\n", "sequence: 3.9\n"},
		"fractional reader fence":   {"sequence: 2\n", "sequence: 2.9\n"},
		"negative reader fence":     {"sequence: 2\n", "sequence: -2\n"},
		"signed document fence":     {"sequence: 3\n", "sequence: +3\n"},
		"hex document fence":        {"sequence: 3\n", "sequence: 0x3\n"},
		"octal document fence":      {"sequence: 3\n", "sequence: 03\n"},
		"separator document fence":  {"sequence: 3\n", "sequence: 3_0\n"},
		"overflow document fence":   {"sequence: 3\n", "sequence: 18446744073709551616\n"},
		"quoted yaml boolean":       {"sealed: true\n", "sealed: \"yes\"\n"},
		"yaml boolean alias":        {"sealed: true\n", "sealed: yes\n"},
		"capital boolean":           {"sealed: true\n", "sealed: True\n"},
		"missing boolean":           {"sealed: true\n", ""},
		"quoted timestamp":          {"renewed_at: " + d.Readers[0].RenewedAt.Format(time.RFC3339Nano), "renewed_at: \"" + d.Readers[0].RenewedAt.Format(time.RFC3339Nano) + "\""},
		"null reader list":          {string(raw[strings.Index(string(raw), "readers:"):]), "readers: null\n"},
	} {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(string(raw), replacement[0]) {
				t.Fatal("invalid mutation fixture")
			}
			mutated := []byte(strings.Replace(string(raw), replacement[0], replacement[1], 1))
			if _, err := decode(mutated, r.config); !errors.Is(err, ErrCorrupt) {
				t.Fatal("noncanonical durable fence accepted", err)
			}
		})
	}
}
