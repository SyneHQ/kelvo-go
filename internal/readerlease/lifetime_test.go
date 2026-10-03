// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package readerlease

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type lifetimeValueKey string

func TestReaderLeaseAcquisitionContextEndsWithoutEndingCustody(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			r, s, b := shortRegistry(t)
			acquireValues := context.WithValue(context.Background(), lifetimeValueKey("request"), "request-only")
			acquireCtx, stopAcquire := context.WithCancel(acquireValues)
			if deadline {
				stopAcquire()
				acquireCtx, stopAcquire = context.WithTimeout(acquireValues, 100*time.Millisecond)
			}
			defer stopAcquire()
			custodyCtx, stopCustody := context.WithCancel(context.WithValue(context.Background(), lifetimeValueKey("owner"), "custody-only"))
			defer stopCustody()
			lease, err := r.AcquireWithLifetime(acquireCtx, custodyCtx, b)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			if lease.Context().Value(lifetimeValueKey("request")) != nil || lease.Context().Value(lifetimeValueKey("owner")) != "custody-only" {
				t.Fatal("lease retained acquisition values or lost custody values")
			}
			renewed := make(chan struct{})
			var once sync.Once
			s.change(func(s *fakeStore) {
				s.afterCAS = func(context.Context, string) error {
					if acquireCtx.Err() != nil {
						once.Do(func() { close(renewed) })
					}
					return nil
				}
			})
			if !deadline {
				stopAcquire()
			}
			select {
			case <-acquireCtx.Done():
			case <-time.After(time.Second):
				t.Fatal("acquisition context did not end")
			}
			select {
			case <-renewed:
			case <-time.After(time.Second):
				t.Fatal("pin did not renew after acquisition context ended")
			}
			if lease.Check() != nil || len(r.leases) != 1 {
				t.Fatal("request cancellation ended pin custody", lease.Check())
			}
			readers := contents(t, r, b).Readers
			if len(readers) != 1 || readers[0].Sequence < 2 {
				t.Fatal("renewal did not advance the retained pin")
			}
			select {
			case <-lease.Quiesced():
				t.Fatal("live custody was reported quiescent")
			default:
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
			waitLifetimeQuiesced(t, lease)
			if custodyCtx.Err() != nil {
				t.Fatal("closing one lease cancelled its parent owner")
			}
		})
	}
}

func TestReaderRegistryLifetimeRejectsInvalidAndCancelledContextsBeforeIO(t *testing.T) {
	r, s, _, b := testRegistry(t)
	prepare(t, r, b)
	cause := errors.New("custody stopped")
	stopped, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	for _, tc := range []struct {
		name             string
		acquire, custody context.Context
		want             error
	}{
		{"nil-acquire", nil, context.Background(), ErrInvalid},
		{"nil-custody", context.Background(), nil, ErrInvalid},
		{"cancelled-acquire", stopped, context.Background(), cause},
		{"cancelled-custody", context.Background(), stopped, cause},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := s.calls.Load()
			lease, err := r.AcquireWithLifetime(tc.acquire, tc.custody, b)
			if lease != nil || !errors.Is(err, tc.want) || s.calls.Load() != calls || len(r.leases) != 0 {
				t.Fatal("invalid acquisition reached provider or reserved custody", lease, err)
			}
		})
	}
}

func TestReaderRegistryLifetimeCancellationDuringProviderWorkRetainsCapacity(t *testing.T) {
	for _, phase := range []string{"get", "committed-cas"} {
		for _, cancelled := range []string{"acquire", "custody"} {
			t.Run(phase+"/"+cancelled, func(t *testing.T) {
				r, s, _, b := testRegistry(t)
				r.config.MaxLeases = 1
				r.leases = make(chan struct{}, 1)
				prepare(t, r, b)
				acquireCtx, stopAcquire := context.WithCancelCause(context.Background())
				custodyCtx, stopCustody := context.WithCancelCause(context.Background())
				defer stopAcquire(context.Canceled)
				defer stopCustody(context.Canceled)
				entered := make(chan context.Context, 1)
				release := make(chan struct{})
				var unblock sync.Once
				defer unblock.Do(func() { close(release) })
				hook := func(ctx context.Context, _ string) error {
					entered <- ctx
					<-release
					return nil
				}
				s.change(func(s *fakeStore) {
					if phase == "get" {
						s.getHook = hook
					} else {
						s.afterCAS = hook
					}
				})
				type result struct {
					lease *Lease
					err   error
				}
				done := make(chan result, 1)
				go func() { l, err := r.AcquireWithLifetime(acquireCtx, custodyCtx, b); done <- result{l, err} }()
				var operation context.Context
				select {
				case operation = <-entered:
				case <-time.After(time.Second):
					t.Fatal("acquisition did not reach provider")
				}
				cause := errors.New("selected lifetime stopped")
				if cancelled == "acquire" {
					stopAcquire(cause)
				} else {
					stopCustody(cause)
				}
				select {
				case <-operation.Done():
				case <-time.After(time.Second):
					t.Fatal("provider did not observe selected cancellation")
				}
				select {
				case <-done:
					t.Fatal("acquisition returned while provider work was still blocked")
				default:
				}
				assertLocalCapacityHeld(t, r, s, b)
				unblock.Do(func() { close(release) })
				select {
				case result := <-done:
					if result.lease != nil || !errors.Is(result.err, cause) {
						t.Fatal("cancelled acquisition granted a reader or lost its cause", result.lease, result.err)
					}
				case <-time.After(time.Second):
					t.Fatal("acquisition did not join provider work")
				}
				s.change(func(s *fakeStore) { s.getHook, s.afterCAS = nil, nil })
				wantPins := 0
				if phase == "committed-cas" {
					wantPins = 1
				}
				if len(contents(t, r, b).Readers) != wantPins || len(r.leases) != 0 {
					t.Fatal("uncertain commit or completed local custody was misrepresented")
				}
			})
		}
	}
}

// Block cause lookup only after cancellation, allowing the actual AfterFunc
// callback to be observed in progress while provider confirmation returns.
type lifetimeCauseGate struct {
	context.Context
	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *lifetimeCauseGate) Value(key any) any {
	if c.block.Load() {
		select {
		case <-c.Context.Done():
			c.once.Do(func() { close(c.entered) })
			<-c.release
		default:
		}
	}
	return c.Context.Value(key)
}

func TestReaderRegistryLifetimeJoinsStartedCancellationBeforeTransfer(t *testing.T) {
	testLifetimeCallbackJoin(t, false)
}

func TestReaderRegistryLifetimeFailureJoinsCancellationBeforeReleasingCapacity(t *testing.T) {
	// A failed provider confirmation never constructs newLease, so its context
	// cannot accidentally hide a missing join by blocking on the same cause.
	testLifetimeCallbackJoin(t, true)
}

func testLifetimeCallbackJoin(t *testing.T, failedConfirmation bool) {
	t.Helper()
	r, s, _, b := testRegistry(t)
	r.config.MaxLeases = 1
	r.leases = make(chan struct{}, 1)
	prepare(t, r, b)
	parent, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	custody := &lifetimeCauseGate{Context: parent, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseCause sync.Once
	defer releaseCause.Do(func() { close(custody.release) })
	committed, confirmed := make(chan struct{}), make(chan struct{})
	var releaseCAS sync.Once
	defer releaseCAS.Do(func() { close(confirmed) })
	s.change(func(s *fakeStore) {
		s.afterCAS = func(context.Context, string) error {
			close(committed)
			<-confirmed
			if failedConfirmation {
				return ErrUnavailable
			}
			return nil
		}
	})
	done := make(chan error, 1)
	go func() {
		l, err := r.AcquireWithLifetime(context.Background(), custody, b)
		if l != nil {
			_ = l.Close()
			err = errors.New("unexpected live lease")
		}
		done <- err
	}()
	select {
	case <-committed:
	case <-time.After(time.Second):
		t.Fatal("CAS did not commit")
	}
	custody.block.Store(true)
	cause := errors.New("custody cancelled at confirmation")
	cancel(cause)
	select {
	case <-custody.entered:
	case <-time.After(time.Second):
		t.Fatal("cancellation callback did not begin")
	}
	releaseCAS.Do(func() { close(confirmed) })
	select {
	case <-done:
		t.Fatal("acquisition outlived its started cancellation callback")
	case <-time.After(30 * time.Millisecond):
	}
	assertLocalCapacityHeld(t, r, s, b)
	releaseCause.Do(func() { close(custody.release) })
	select {
	case err := <-done:
		want := cause
		if failedConfirmation {
			want = ErrUnavailable
		}
		if !errors.Is(err, want) {
			t.Fatal("confirmation cancellation was not retained", err)
		}
	case <-time.After(time.Second):
		t.Fatal("callback was not joined")
	}
	s.change(func(s *fakeStore) { s.afterCAS = nil })
	if len(r.leases) != 0 || len(contents(t, r, b).Readers) != 1 {
		t.Fatal("cancelled late commit was treated as absent or live local custody")
	}
}

func TestReaderRegistryLifetimeHonorsEarlierCustodyDeadline(t *testing.T) {
	r, s, _, b := testRegistry(t)
	prepare(t, r, b)
	cause := errors.New("custody deadline reached")
	custody, cancel := context.WithTimeoutCause(context.Background(), 80*time.Millisecond, cause)
	defer cancel()
	deadline, _ := custody.Deadline()
	observed := make(chan time.Time, 1)
	s.change(func(s *fakeStore) {
		s.getHook = func(ctx context.Context, _ string) error {
			d, ok := ctx.Deadline()
			if !ok {
				return errors.New("provider context lost its deadline")
			}
			observed <- d
			<-ctx.Done()
			return ctx.Err()
		}
	})
	lease, err := r.AcquireWithLifetime(context.Background(), custody, b)
	if lease != nil || !errors.Is(err, cause) || len(r.leases) != 0 {
		t.Fatal("custody deadline was not enforced with its cause", lease, err)
	}
	select {
	case got := <-observed:
		if !got.Equal(deadline) {
			t.Fatal("provider did not receive the earlier custody deadline")
		}
	default:
		t.Fatal("provider deadline was not checked")
	}
}

type lifetimeBodyStore struct {
	Store
	block   atomic.Bool
	phase   string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *lifetimeBodyStore) Get(ctx context.Context, key string) (io.ReadCloser, Metadata, error) {
	body, metadata, err := s.Store.Get(ctx, key)
	if err != nil || !s.block.Load() {
		return body, metadata, err
	}
	return &lifetimeBlockedBody{ReadCloser: body, store: s}, metadata, nil
}

type lifetimeBlockedBody struct {
	io.ReadCloser
	store *lifetimeBodyStore
}

func (b *lifetimeBlockedBody) wait() {
	b.store.once.Do(func() { close(b.store.entered) })
	<-b.store.release
}

func (b *lifetimeBlockedBody) Read(p []byte) (int, error) {
	if b.store.phase == "read" {
		b.wait()
	}
	return b.ReadCloser.Read(p)
}

func (b *lifetimeBlockedBody) Close() error {
	if b.store.phase == "close" {
		b.wait()
	}
	return b.ReadCloser.Close()
}

func waitLifetimeQuiesced(t *testing.T, lease *Lease) {
	t.Helper()
	select {
	case <-lease.Quiesced():
	case <-time.After(time.Second):
		t.Fatal("lease did not report actual local quiescence")
	}
	if len(lease.registry.leases) != 0 {
		t.Fatal("quiescence was exposed before capacity was returned")
	}
}

func TestReaderLeaseLifetimeQuiescenceWaitsForProviderReadAndClose(t *testing.T) {
	for _, operation := range []string{"renewal", "release"} {
		for _, phase := range []string{"read", "close"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				r, s, b := shortRegistry(t)
				store := &lifetimeBodyStore{Store: s, phase: phase, entered: make(chan struct{}), release: make(chan struct{})}
				r.store = store
				custody, cancel := context.WithCancelCause(context.Background())
				defer cancel(context.Canceled)
				lease, err := r.AcquireWithLifetime(context.Background(), custody, b)
				if err != nil {
					t.Fatal(err)
				}
				var release sync.Once
				defer func() { release.Do(func() { close(store.release) }); _ = lease.Close(); waitLifetimeQuiesced(t, lease) }()
				if operation == "renewal" {
					store.block.Store(true)
					select {
					case <-store.entered:
					case <-time.After(time.Second):
						t.Fatal("renewal did not reach blocked provider body")
					}
				}
				cause := errors.New("owner cancelled")
				cancel(cause)
				if !errors.Is(lease.Check(), cause) {
					t.Fatal("custody loss did not cancel the successful lease")
				}
				if operation == "release" {
					// Renewal cannot accidentally consume the intended release
					// barrier. Join both workers while provider access is open.
					for _, done := range []<-chan struct{}{lease.renewDone, lease.watchDone} {
						select {
						case <-done:
						case <-time.After(time.Second):
							t.Fatal("lease workers did not stop before release barrier")
						}
					}
					store.block.Store(true)
				}
				started := time.Now()
				if err := lease.Close(); !errors.Is(err, ErrReleaseUnknown) || time.Since(started) > 500*time.Millisecond {
					t.Fatal("blocked provider defeated bounded close", err)
				}
				select {
				case <-store.entered:
				default:
					t.Fatal("close did not reach provider work")
				}
				select {
				case <-lease.Quiesced():
					t.Fatal("context cancellation or Close return falsely proved quiescence")
				default:
				}
				assertLocalCapacityHeld(t, r, s, b)
				release.Do(func() { close(store.release) })
				waitLifetimeQuiesced(t, lease)
				if !errors.Is(lease.Close(), ErrReleaseUnknown) {
					t.Fatal("later cleanup rewrote an uncertain Close result")
				}
			})
		}
	}
}

func TestReaderLeaseLifetimeConcurrentCloseAndCustodyLossReleaseOnce(t *testing.T) {
	r, s, _, b := testRegistry(t)
	prepare(t, r, b)
	custody, cancel := context.WithCancel(context.Background())
	defer cancel()
	lease, err := r.AcquireWithLifetime(context.Background(), custody, b)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	before := s.version
	s.mu.Unlock()
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			if err := lease.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Add(1)
	go func() { defer workers.Done(); <-start; cancel() }()
	close(start)
	workers.Wait()
	waitLifetimeQuiesced(t, lease)
	s.mu.Lock()
	after := s.version
	s.mu.Unlock()
	if after != before+1 || len(contents(t, r, b).Readers) != 0 {
		t.Fatal("concurrent close duplicated release or left a confirmed pin")
	}
}
