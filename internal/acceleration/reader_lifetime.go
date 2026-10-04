// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

const (
	readerOwnerLimit = 2
	readerGuardLimit = 128
	readerPinLimit   = 128
	readerBindLimit  = 64
	readerHoldLimit  = 4
)

var (
	errReaderInvalid        = errors.New("invalid reader lifetime configuration or operation")
	errReaderCapacity       = errors.New("reader lifetime capacity exhausted")
	errReaderNotActive      = errors.New("reader is not active")
	errReaderClosed         = errors.New("reader lifetime is closing")
	errReaderOwnerClosed    = errors.New("reader owner is draining")
	errReaderCleanupUnknown = errors.New("reader cleanup is uncertain")
)

// A budget spans the node's provider owners, guards and pending acquisitions.
// The fixed object runtime shares it across all managers and executor copies.
type ReaderBudget struct {
	mu     sync.Mutex
	counts readerCounts
}

type readerCounts struct{ owners, guards, pins int }

func newReaderBudget() *ReaderBudget { return &ReaderBudget{} }

func (b *ReaderBudget) snapshot() readerCounts {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.counts
}

func (b *ReaderBudget) reserveGuard(pins int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.counts.guards >= readerGuardLimit || pins > readerPinLimit-b.counts.pins {
		return false
	}
	b.counts.guards++
	b.counts.pins += pins
	return true
}

func (b *ReaderBudget) releaseGuard(pins int) {
	b.mu.Lock()
	b.counts.guards--
	b.counts.pins -= pins
	b.mu.Unlock()
}

// All fields are values except the bounded policy slice, copied at admission.
// Fingerprints come from catalog.DatasetFingerprint; no parallel fingerprint
// algorithm or mutable request catalog is retained here. Registry defaults are
// deliberately fixed until a separately reviewed configuration integration.
type readerOwnerSpec struct {
	tenant, directory                    string
	location                             catalog.ObjectLocation
	readCredentials, registryCredentials catalog.ObjectCredentials
	registry                             readerlease.Config
	datasets                             []readerDatasetPolicy
}

type readerDatasetPolicy struct {
	id, fingerprint string
	maxAge          time.Duration
	scan            catalog.SnapshotScanLimits
}

func (s readerOwnerSpec) immutable() (readerOwnerSpec, error) {
	if !storeTenantID.MatchString(s.tenant) || !filepath.IsAbs(s.directory) || filepath.Clean(s.directory) != s.directory ||
		len(s.directory) > 4096 || strings.ContainsAny(s.directory, "\x00\r\n") || s.location.Validate() != nil ||
		s.readCredentials.Validate(s.location.Provider) != nil || s.registryCredentials.Validate(s.location.Provider) != nil ||
		s.registry != readerlease.DefaultConfig(s.location.Prefix, s.tenant) || len(s.datasets) < 1 || len(s.datasets) > readerBindLimit {
		return readerOwnerSpec{}, errReaderInvalid
	}
	for _, read := range s.readCredentials.EnvironmentNames() {
		for _, registry := range s.registryCredentials.EnvironmentNames() {
			if read != "" && read == registry {
				return readerOwnerSpec{}, errReaderInvalid
			}
		}
	}
	s.datasets = slices.Clone(s.datasets)
	slices.SortFunc(s.datasets, func(a, b readerDatasetPolicy) int { return strings.Compare(a.id, b.id) })
	for i, dataset := range s.datasets {
		scan, err := dataset.scan.Effective()
		if !catalog.ValidID(dataset.id) || !storeDigest.MatchString(dataset.fingerprint) || dataset.maxAge <= 0 ||
			dataset.maxAge > 30*24*time.Hour || err != nil || scan != dataset.scan ||
			(i > 0 && s.datasets[i-1].id == dataset.id) {
			return readerOwnerSpec{}, errReaderInvalid
		}
	}
	return s, nil
}

func (s readerOwnerSpec) validBindings(bindings []readerlease.Binding) bool {
	if len(bindings) < 1 || len(bindings) > readerBindLimit {
		return false
	}
	for i, binding := range bindings {
		if binding.Tenant != s.tenant || !catalog.ValidID(binding.Dataset) || !storeGenerationID.MatchString(binding.Generation) ||
			!storeDigest.MatchString(binding.Incarnation) || !storeDigest.MatchString(binding.ContentSHA256) {
			return false
		}
		_, found := slices.BinarySearchFunc(s.datasets, binding.Dataset, func(d readerDatasetPolicy, id string) int { return strings.Compare(d.id, id) })
		if !found {
			return false
		}
		for _, prior := range bindings[:i] {
			// A conflicting digest for the same immutable identity is not a
			// second pin. Reject it along with byte-identical duplicates.
			if prior.Reference == binding.Reference {
				return false
			}
		}
	}
	return true
}

// Private resource contracts, including the node's real Registry adapter.
// Check/Context/Quiesced perform no I/O. Close is once-only and its return alone
// does not prove local quiescence. Quiesced joins admitted registry operations,
// renewals, provider methods, body reads/Close, upload reads and our callbacks.
// Standard HTTP internals belong to separately bounded node transports; this
// signal is not a promise that every HTTP goroutine exited or memory was erased.
type readerPin interface {
	Context() context.Context
	Check() error
	Close() error
	Quiesced() <-chan struct{}
}

type readerResources interface {
	AcquireWithLifetime(context.Context, context.Context, readerlease.Binding) (readerPin, error)
	Close() error
	Quiesced() <-chan struct{}
}

func nilReaderDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

type readerCallback struct {
	stop func() bool
	done chan struct{}
}

func readerAfter(ctx context.Context, fn func()) readerCallback {
	done := make(chan struct{})
	return readerCallback{stop: context.AfterFunc(ctx, func() { defer close(done); fn() }), done: done}
}

func (c readerCallback) join() {
	if c.stop != nil && !c.stop() {
		<-c.done
	}
}

// Bounded Close callers observe one retained finalizer, never create a waiter.
func validReaderCleanup(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	_, bounded := ctx.Deadline()
	return bounded
}

func readerWake(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

type readerOutcome struct {
	failure, cleanup error
	unknown          bool
}

func (o *readerOutcome) fail(err error) {
	if o.failure == nil {
		o.failure = err
	}
}

func (o *readerOutcome) cleanupFailed(err error) {
	if o.cleanup == nil {
		o.cleanup = err
	}
}

func (o readerOutcome) cleanupError() error {
	if o.unknown {
		return errors.Join(o.cleanup, errReaderCleanupUnknown)
	}
	return o.cleanup
}

func (o readerOutcome) err() error { return errors.Join(o.failure, o.cleanupError()) }
