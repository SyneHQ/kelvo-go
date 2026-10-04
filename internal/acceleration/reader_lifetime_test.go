// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

type ownerTestResources struct {
	acquire             func(context.Context, context.Context, readerlease.Binding) (readerPin, error)
	acquires, closes    atomic.Int32
	closeStarted, quiet chan struct{}
	closeGate           <-chan struct{}
	closeError          error
	manualQuiet         bool
	once                sync.Once
}

func newOwnerTestResources() *ownerTestResources {
	return &ownerTestResources{closeStarted: make(chan struct{}), quiet: make(chan struct{})}
}

func (r *ownerTestResources) AcquireWithLifetime(ctx, custody context.Context, binding readerlease.Binding) (readerPin, error) {
	r.acquires.Add(1)
	if r.acquire != nil {
		return r.acquire(ctx, custody, binding)
	}
	return newOwnerTestPin(custody), nil
}

func (r *ownerTestResources) Close() error {
	r.closes.Add(1)
	r.once.Do(func() {
		close(r.closeStarted)
		if r.closeGate != nil {
			<-r.closeGate
		}
		if !r.manualQuiet {
			close(r.quiet)
		}
	})
	return r.closeError
}

func (r *ownerTestResources) Quiesced() <-chan struct{} { return r.quiet }

type ownerTestPin struct {
	ctx                 context.Context
	cancel              context.CancelCauseFunc
	checkFailure        atomic.Bool
	closes, renews      atomic.Int32
	closeStarted, quiet chan struct{}
	closeGate           <-chan struct{}
	closeError          error
	closeHook           func()
	manualQuiet         bool
	once                sync.Once
}

func newOwnerTestPin(custody context.Context) *ownerTestPin {
	ctx, cancel := context.WithCancelCause(custody)
	return &ownerTestPin{ctx: ctx, cancel: cancel, closeStarted: make(chan struct{}), quiet: make(chan struct{})}
}

func (p *ownerTestPin) Context() context.Context { return p.ctx }
func (p *ownerTestPin) Check() error {
	if p.checkFailure.Load() {
		return readerlease.ErrExpired
	}
	return context.Cause(p.ctx)
}
func (p *ownerTestPin) Quiesced() <-chan struct{} { return p.quiet }

func (p *ownerTestPin) renew() error {
	if err := p.Check(); err != nil {
		return err
	}
	p.renews.Add(1)
	return nil
}

func (p *ownerTestPin) Close() error {
	p.closes.Add(1)
	p.once.Do(func() {
		close(p.closeStarted)
		if p.closeHook != nil {
			p.closeHook()
		}
		p.cancel(readerlease.ErrClosed)
		if p.closeGate != nil {
			<-p.closeGate
		}
		if !p.manualQuiet {
			close(p.quiet)
		}
	})
	return p.closeError
}

func ownerTestSpec(t *testing.T) readerOwnerSpec {
	t.Helper()
	return readerOwnerSpec{tenant: "tenant", directory: t.TempDir(),
		location:            catalog.ObjectLocation{Provider: "s3", Endpoint: "https://storage.example.test", Bucket: "snapshots", Prefix: "snapshots", Region: "us-east-1"},
		readCredentials:     catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_READER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_READER_SECRET"},
		registryCredentials: catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_REGISTRY_ID", SecretAccessKeyEnv: "KELVO_SOURCE_REGISTRY_SECRET"},
		registry:            readerlease.DefaultConfig("snapshots", "tenant"),
		datasets:            []readerDatasetPolicy{{id: "trips", fingerprint: strings.Repeat("a", 64), maxAge: time.Hour, scan: catalog.SnapshotScanLimits{MaxRows: 1_000_000, MaxBytes: 256 << 20}}}}
}

func ownerTestBindings(count int) []readerlease.Binding {
	bindings := make([]readerlease.Binding, count)
	for i := range bindings {
		bindings[i] = readerlease.Binding{Reference: readerlease.Reference{Tenant: "tenant", Dataset: "trips", Generation: fmt.Sprintf("%032x", i+1), Incarnation: strings.Repeat("b", 64)}, ContentSHA256: strings.Repeat("c", 64)}
	}
	return bindings
}

func ownerTestAttach(t *testing.T, budget *ReaderBudget, spec readerOwnerSpec, resources *ownerTestResources) *ReaderOwner {
	t.Helper()
	owner, err := budget.newOwner(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err = owner.attach(resources); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = owner.Close(ctx)
		ownerTestWait(t, owner.Quiesced(), "owner cleanup")
	})
	return owner
}

func ownerTestBegin(t *testing.T, owner *ReaderOwner, ctx context.Context, count int) *ReadGuard {
	t.Helper()
	guard, err := owner.begin(ctx, ownerTestBindings(count))
	if err != nil {
		t.Fatal(err)
	}
	return guard
}

func ownerTestAcquire(t *testing.T, owner *ReaderOwner, ctx context.Context) (*ReadGuard, *ownerTestPin) {
	t.Helper()
	guard := ownerTestBegin(t, owner, ctx, 1)
	if err := guard.acquire(); err != nil {
		t.Fatal(err)
	}
	return guard, guard.pins[0].(*ownerTestPin)
}

func ownerTestWait(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal(label + " did not finish")
	}
}

func ownerTestPending(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(label + " finished without its cleanup proof")
	default:
	}
}

func ownerTestClose(t *testing.T, close func(context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return close(ctx)
}

func ownerTestTimeout(t *testing.T, close func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	bounded, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if err := close(bounded); !errors.Is(err, errReaderCleanupUnknown) {
		t.Fatal("bounded Close did not retain uncertainty", err)
	}
}

func TestReaderOwnerSpecImmutableAndRejectsInvalidScope(t *testing.T) {
	spec := ownerTestSpec(t)
	spec.datasets = append(spec.datasets, readerDatasetPolicy{id: "accounts", fingerprint: strings.Repeat("d", 64), maxAge: 2 * time.Hour, scan: spec.datasets[0].scan})
	budget := newReaderBudget()
	owner := ownerTestAttach(t, budget, spec, newOwnerTestResources())
	want := owner.spec
	want.datasets = append([]readerDatasetPolicy(nil), want.datasets...)
	spec.tenant, spec.directory, spec.location.Endpoint = "changed", "/changed", "https://changed.example.test"
	spec.readCredentials.AccessKeyIDEnv, spec.registryCredentials.AccessKeyIDEnv = "changed", "changed"
	spec.registry.LeaseDuration = time.Hour
	for i := range spec.datasets {
		spec.datasets[i] = readerDatasetPolicy{}
	}
	if !reflect.DeepEqual(owner.spec, want) || owner.spec.datasets[0].id != "accounts" {
		t.Fatal("caller mutation changed immutable owner scope")
	}
	for name, mutate := range map[string]func(*readerOwnerSpec){
		"tenant":               func(s *readerOwnerSpec) { s.tenant = "INVALID" },
		"directory":            func(s *readerOwnerSpec) { s.directory = "relative" },
		"endpoint":             func(s *readerOwnerSpec) { s.location.Endpoint = "https://storage.example.test/path" },
		"scope":                func(s *readerOwnerSpec) { s.registry.Tenant = "other" },
		"timing":               func(s *readerOwnerSpec) { s.registry.LeaseDuration++ },
		"credential_alias":     func(s *readerOwnerSpec) { s.registryCredentials = s.readCredentials },
		"credential_namespace": func(s *readerOwnerSpec) { s.registryCredentials.AccessKeyIDEnv = "UNSCOPED" },
		"dataset":              func(s *readerOwnerSpec) { s.datasets[0].id = "invalid-id" },
		"fingerprint":          func(s *readerOwnerSpec) { s.datasets[0].fingerprint = "short" },
		"scan_defaults":        func(s *readerOwnerSpec) { s.datasets[0].scan = catalog.SnapshotScanLimits{} },
		"age":                  func(s *readerOwnerSpec) { s.datasets[0].maxAge = 31 * 24 * time.Hour },
		"duplicate_dataset":    func(s *readerOwnerSpec) { s.datasets = append(s.datasets, s.datasets[0]) },
		"no_datasets":          func(s *readerOwnerSpec) { s.datasets = nil },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := ownerTestSpec(t)
			mutate(&invalid)
			if _, err := budget.newOwner(invalid); !errors.Is(err, errReaderInvalid) {
				t.Fatal("invalid scope was accepted", err)
			}
			if got := budget.snapshot(); got != (readerCounts{owners: 1}) {
				t.Fatal("invalid scope consumed capacity", got)
			}
		})
	}
}

func TestReaderGuardRejectsBindingsBeforeResourcesOrCapacity(t *testing.T) {
	budget, resources := newReaderBudget(), newOwnerTestResources()
	owner := ownerTestAttach(t, budget, ownerTestSpec(t), resources)
	for name, mutate := range map[string]func([]readerlease.Binding) []readerlease.Binding{
		"none":            func(_ []readerlease.Binding) []readerlease.Binding { return nil },
		"too_many":        func(_ []readerlease.Binding) []readerlease.Binding { return ownerTestBindings(65) },
		"foreign_tenant":  func(b []readerlease.Binding) []readerlease.Binding { b[0].Tenant = "foreign"; return b },
		"foreign_dataset": func(b []readerlease.Binding) []readerlease.Binding { b[0].Dataset = "foreign"; return b },
		"generation":      func(b []readerlease.Binding) []readerlease.Binding { b[0].Generation = "bad"; return b },
		"incarnation":     func(b []readerlease.Binding) []readerlease.Binding { b[0].Incarnation = "bad"; return b },
		"digest":          func(b []readerlease.Binding) []readerlease.Binding { b[0].ContentSHA256 = "bad"; return b },
		"duplicate":       func(b []readerlease.Binding) []readerlease.Binding { return append(b, b[0]) },
		"conflicting_digest": func(b []readerlease.Binding) []readerlease.Binding {
			next := b[0]
			next.ContentSHA256 = strings.Repeat("d", 64)
			return append(b, next)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := owner.begin(context.Background(), mutate(ownerTestBindings(1))); !errors.Is(err, errReaderInvalid) {
				t.Fatal("invalid binding accepted", err)
			}
		})
	}
	if _, err := owner.begin(nil, ownerTestBindings(1)); !errors.Is(err, errReaderInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := owner.begin(ctx, ownerTestBindings(1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if budget.snapshot() != (readerCounts{owners: 1}) || resources.acquires.Load() != 0 {
		t.Fatal("invalid work reached resource calls or changed capacity")
	}
}
