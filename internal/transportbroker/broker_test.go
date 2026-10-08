// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package transportbroker

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type openFunc func(context.Context, OpenRequest) (net.Conn, error)

func (f openFunc) Open(ctx context.Context, request OpenRequest) (net.Conn, error) {
	return f(ctx, request)
}

func binding() Binding {
	return Binding{Issuer: "application", Audience: "database-ingress", ClusterTenant: "cluster-a", ServicePrincipal: "analytics",
		Tenant: "customer-a", Source: "source-a", SourceRevision: strings.Repeat("a", 64), Authority: "database.internal:5432",
		Execution: Execution{Kind: "operation", ID: "operation-a", GrantSHA256: strings.Repeat("b", 64), Worker: "worker-a", Owner: strings.Repeat("c", 32), Claim: strings.Repeat("d", 32)},
		ExpiresAt: time.Now().Add(time.Minute)}
}

func fixture(t *testing.T, limits Limits, opener Opener) *Broker {
	t.Helper()
	b, err := New(limits, opener)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := b.Close(ctx); err != nil {
			t.Error("fixture did not join", err)
		}
	})
	return b
}

func admit(t *testing.T, b *Broker, value Binding) *Session {
	t.Helper()
	s, err := b.Admit(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func dial(t *testing.T, d Dialer, authority string) net.Conn {
	t.Helper()
	c, err := d.DialContext(context.Background(), "tcp", authority)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func pipe(t *testing.T) net.Conn {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	return a
}

func TestDriverCapabilityCannotChangeSourceOrExecution(t *testing.T) {
	var requests []OpenRequest
	b := fixture(t, Limits{MaxSessions: 2, MaxDataConnections: 2, MaxDataPerSession: 2}, openFunc(func(_ context.Context, r OpenRequest) (net.Conn, error) {
		requests = append(requests, r)
		return pipe(t), nil
	}))
	original := binding()
	s := admit(t, b, original)
	original.Source = "changed-after-admission"
	original.Execution.Claim = strings.Repeat("e", 32)
	d := s.DataDialer()
	for _, target := range []struct{ network, address string }{
		{"udp", binding().Authority}, {"tcp4", binding().Authority}, {"tcp", "127.0.0.1:5432"},
		{"tcp", "other.internal:5432"}, {"tcp", "database.internal:5433"}, {"tcp", "DATABASE.internal:5432"},
	} {
		if _, err := d.DialContext(context.Background(), target.network, target.address); !errors.Is(err, ErrScope) {
			t.Fatal("driver changed fixed routing scope", target, err)
		}
	}
	if len(requests) != 0 {
		t.Fatal("invalid driver target reached the opener")
	}
	first := dial(t, d, binding().Authority)
	second := dial(t, d, binding().Authority)
	if len(requests) != 2 || requests[0].Binding != s.binding || requests[1].Binding != s.binding || requests[0].Purpose != Data ||
		requests[0].ID == requests[1].ID || !hexValue(requests[0].ID, 64) || !hexValue(requests[1].ID, 64) {
		t.Fatal("physical opens lost immutable authority or unique identities")
	}
	if first.RemoteAddr().Network() != "tcp" || first.RemoteAddr().String() != binding().Authority || second.RemoteAddr().String() != binding().Authority {
		t.Fatal("driver observed the proxy address instead of original source authority")
	}
	if _, err := b.Admit(context.Background(), s.binding); !errors.Is(err, ErrScope) {
		t.Fatal("duplicate source attempt acquired another capacity reservation", err)
	}
	changed := s.binding
	changed.SourceRevision = strings.Repeat("f", 64)
	changed.Authority = "other.internal:5432"
	if _, err := b.Admit(context.Background(), changed); !errors.Is(err, ErrScope) {
		t.Fatal("active source attempt changed routing revision", err)
	}
	changed.Execution.GrantSHA256 = strings.Repeat("e", 64)
	if _, err := b.Admit(context.Background(), changed); !errors.Is(err, ErrScope) {
		t.Fatal("changed grant hash bypassed the frozen source attempt", err)
	}
}

func TestCancellationCapacityIsReservedForEachSourceAttempt(t *testing.T) {
	var requests []OpenRequest
	b := fixture(t, Limits{MaxSessions: 2, MaxDataConnections: 1, MaxDataPerSession: 1}, openFunc(func(_ context.Context, r OpenRequest) (net.Conn, error) {
		requests = append(requests, r)
		return pipe(t), nil
	}))
	a := admit(t, b, binding())
	other := binding()
	other.Tenant = "customer-b"
	other.Execution.ID = "operation-b"
	z := admit(t, b, other)
	data := dial(t, a.DataDialer(), binding().Authority)
	for _, s := range []*Session{a, z} {
		if _, err := s.DataDialer().DialContext(context.Background(), "tcp", s.binding.Authority); !errors.Is(err, ErrCapacity) {
			t.Fatal("data saturation did not hold", err)
		}
	}
	firstCancel := dial(t, a.CancellationDialer(), a.binding.Authority)
	_ = dial(t, z.CancellationDialer(), z.binding.Authority)
	if _, err := a.CancellationDialer().DialContext(context.Background(), "tcp", a.binding.Authority); !errors.Is(err, ErrCapacity) {
		t.Fatal("one source consumed another source's cancellation slot", err)
	}
	if got := b.Snapshot(); got.DataConnections != 1 || got.CancellationConnections != 2 || got.Sessions != 2 {
		t.Fatal("unexpected aggregate data/cancellation reservation", got)
	}
	if len(requests) != 3 || requests[1].Purpose != Cancellation || requests[2].Purpose != Cancellation ||
		requests[0].Binding != requests[1].Binding || requests[1].Binding == requests[2].Binding || requests[0].ID == requests[1].ID {
		t.Fatal("cancellation did not receive a separate exact-scope physical open")
	}
	if err := firstCancel.Close(); err != nil {
		t.Fatal(err)
	}
	_ = dial(t, a.CancellationDialer(), a.binding.Authority)
	if requests[3].ID == requests[1].ID {
		t.Fatal("replacement cancellation reused a physical ticket ID")
	}
	if err := data.Close(); err != nil {
		t.Fatal(err)
	}
	_ = dial(t, z.DataDialer(), z.binding.Authority)
}

func TestConcurrentCancellationFanoutSurvivesDataSaturation(t *testing.T) {
	requests := make(chan OpenRequest, 6)
	b := fixture(t, Limits{MaxSessions: 2, MaxDataConnections: 2, MaxDataPerSession: 2}, openFunc(func(_ context.Context, request OpenRequest) (net.Conn, error) {
		requests <- request
		return pipe(t), nil
	}))
	a := admit(t, b, binding())
	other := binding()
	other.Execution.ID = "another-operation"
	z := admit(t, b, other)
	_ = dial(t, a.DataDialer(), a.binding.Authority)
	_ = dial(t, a.DataDialer(), a.binding.Authority)
	if _, err := z.DataDialer().DialContext(context.Background(), "tcp", z.binding.Authority); !errors.Is(err, ErrCapacity) {
		t.Fatal("data pool was not saturated", err)
	}
	start, results := make(chan struct{}), make(chan error, 4)
	for _, s := range []*Session{a, a, z, z} {
		go func(s *Session) {
			<-start
			_, err := s.CancellationDialer().DialContext(context.Background(), "tcp", s.binding.Authority)
			results <- err
		}(s)
	}
	close(start)
	var fanoutErr error
	for range 4 {
		fanoutErr = errors.Join(fanoutErr, waitError(t, results))
	}
	if fanoutErr != nil {
		t.Fatal("a concurrent source cancellation lost its reserved slot", fanoutErr)
	}
	for _, s := range []*Session{a, z} {
		if _, err := s.CancellationDialer().DialContext(context.Background(), "tcp", s.binding.Authority); !errors.Is(err, ErrCapacity) {
			t.Fatal("source exceeded its cancellation reservation", err)
		}
	}
	if got := b.Snapshot(); got.DataConnections != 2 || got.CancellationConnections != 4 || got.Sessions != 2 {
		t.Fatal("fanout exceeded aggregate reservations", got)
	}
	ids, cancellations := map[string]bool{}, map[string]int{}
	for range 6 {
		var request OpenRequest
		select {
		case request = <-requests:
		case <-time.After(5 * time.Second):
			t.Fatal("physical-open evidence did not arrive before its deadline")
		}
		if ids[request.ID] {
			t.Fatal("concurrent cancellation reused a physical ticket ID")
		}
		ids[request.ID] = true
		if request.Purpose == Cancellation {
			cancellations[request.Binding.Execution.ID]++
		}
	}
	if cancellations[a.binding.Execution.ID] != 2 || cancellations[z.binding.Execution.ID] != 2 {
		t.Fatal("fanout changed source execution bindings", cancellations)
	}
}

func TestParentCancellationClosesOnlyItsSourceSession(t *testing.T) {
	b := fixture(t, Limits{MaxSessions: 2, MaxDataConnections: 2, MaxDataPerSession: 1}, openFunc(func(context.Context, OpenRequest) (net.Conn, error) { return pipe(t), nil }))
	ctx, cancel := context.WithCancel(context.Background())
	a, err := b.Admit(ctx, binding())
	if err != nil {
		t.Fatal(err)
	}
	other := binding()
	other.Tenant = "customer-b"
	z := admit(t, b, other)
	first := dial(t, a.DataDialer(), a.binding.Authority)
	second := dial(t, z.DataDialer(), z.binding.Authority)
	cancel()
	if err := a.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("closed")); err == nil {
		t.Fatal("closed source remained writable")
	}
	if err := second.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal("another tenant's source was closed", err)
	}
	if got := b.Snapshot(); got.Sessions != 1 || got.DataConnections != 1 {
		t.Fatal("parent cancellation changed foreign source reservations", got)
	}
}

func TestAuthorityExpiryClosesSocketsWithoutParentCancellation(t *testing.T) {
	closed, release := make(chan struct{}), make(chan struct{})
	close(release)
	b := fixture(t, Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, openFunc(func(context.Context, OpenRequest) (net.Conn, error) {
		return &controlledConn{Conn: pipe(t), closeStarted: closed, releaseClose: release}, nil
	}))
	value := binding()
	value.ExpiresAt = time.Now().Add(time.Second)
	s := admit(t, b, value)
	_ = dial(t, s.DataDialer(), value.Authority)
	waitSignal(t, closed)
	waitSignal(t, s.done)
	if _, err := s.CancellationDialer().DialContext(context.Background(), "tcp", value.Authority); !errors.Is(err, ErrClosed) {
		t.Fatal("cancellation bypassed expired source authority", err)
	}
	if got := b.Snapshot(); got.Sessions != 0 || got.DataConnections != 0 || got.CancellationConnections != 0 {
		t.Fatal("authority expiry did not join source custody", got)
	}
}

func TestLateOpenAndBlockedCloseRetainReservation(t *testing.T) {
	entered, releaseOpen, closeStarted, releaseClose := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		for _, signal := range []chan struct{}{releaseOpen, releaseClose} {
			select {
			case <-signal:
			default:
				close(signal)
			}
		}
	}()
	raw := &controlledConn{Conn: pipe(t), closeStarted: closeStarted, releaseClose: releaseClose}
	b := fixture(t, Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, openFunc(func(ctx context.Context, _ OpenRequest) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		<-releaseOpen
		return raw, nil
	}))
	s := admit(t, b, binding())
	done := make(chan error, 1)
	go func() {
		_, err := s.DataDialer().DialContext(context.Background(), "tcp", s.binding.Authority)
		done <- err
	}()
	waitSignal(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Close(ctx); !errors.Is(err, ErrCleanup) || !errors.Is(err, context.Canceled) {
		t.Fatal("pending open claimed joined cleanup", err)
	}
	if got := b.Snapshot(); got.Opening != 1 || got.Sessions != 1 || got.DataConnections != 1 {
		t.Fatal("pending open released capacity", got)
	}
	close(releaseOpen)
	waitSignal(t, closeStarted)
	if err := waitError(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal("late socket escaped closing source custody", err)
	}
	other := binding()
	other.Execution.ID = "another-operation"
	if _, err := b.Admit(context.Background(), other); !errors.Is(err, ErrCapacity) {
		t.Fatal("blocked socket close released session admission", err)
	}
	close(releaseClose)
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := b.Snapshot(); got.Sessions != 0 || got.DataConnections != 0 || got.Opening != 0 {
		t.Fatal("joined cleanup retained a completed reservation", got)
	}
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture event did not arrive before its deadline")
	}
}

func waitError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("fixture result did not arrive before its deadline")
		return nil
	}
}

type controlledConn struct {
	net.Conn
	closeStarted chan struct{}
	releaseClose chan struct{}
	closeError   error
	panicClose   bool
}

func (c *controlledConn) Close() error {
	if c.closeStarted != nil {
		close(c.closeStarted)
		<-c.releaseClose
	}
	_ = c.Conn.Close()
	if c.panicClose {
		panic("private provider detail")
	}
	return c.closeError
}

func TestCleanupUncertaintyDrainsDataButPreservesAcceptedCancellation(t *testing.T) {
	for _, mode := range []string{"close_error", "close_panic", "open_panic"} {
		t.Run(mode, func(t *testing.T) {
			var calls int
			b, err := New(Limits{MaxSessions: 2, MaxDataConnections: 2, MaxDataPerSession: 1}, openFunc(func(_ context.Context, r OpenRequest) (net.Conn, error) {
				calls++
				if r.Purpose == Cancellation {
					return pipe(t), nil
				}
				if mode == "open_panic" {
					panic("private provider detail")
				}
				return &controlledConn{Conn: pipe(t), closeError: errors.New("private provider detail"), panicClose: mode == "close_panic"}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			a := admit(t, b, binding())
			other := binding()
			other.Execution.ID = "other-operation"
			z := admit(t, b, other)
			c, err := a.DataDialer().DialContext(context.Background(), "tcp", a.binding.Authority)
			if mode == "open_panic" {
				if !errors.Is(err, ErrCleanup) {
					t.Fatal("provider panic did not retain unknown open custody", err)
				}
			} else if err != nil || !errors.Is(c.Close(), ErrCleanup) {
				t.Fatal("unknown close was accepted", err)
			}
			if _, err := z.DataDialer().DialContext(context.Background(), "tcp", z.binding.Authority); !errors.Is(err, ErrClosed) {
				t.Fatal("uncertain cleanup admitted more data work", err)
			}
			cancelConn := dial(t, z.CancellationDialer(), z.binding.Authority)
			if err := cancelConn.Close(); err != nil || calls != 2 {
				t.Fatal("accepted cancellation lost its reservation", err, calls)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := b.Close(ctx); !errors.Is(err, ErrCleanup) {
				t.Fatal("uncertain provider cleanup released broker custody", err)
			}
			got := b.Snapshot()
			if !got.Draining || !got.CleanupFailed || got.DataConnections != 1 || got.Sessions != 1 {
				t.Fatal("uncertain physical connection lost its reservation", got)
			}
		})
	}
}

func TestProviderFailureDoesNotRetryOrExposeDiagnostics(t *testing.T) {
	for _, mode := range []string{"error", "empty", "connection_and_error"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			b := fixture(t, Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, openFunc(func(context.Context, OpenRequest) (net.Conn, error) {
				calls++
				if mode == "connection_and_error" {
					return pipe(t), errors.New("private provider detail")
				}
				if mode == "empty" {
					return nil, nil
				}
				return nil, errors.New("private provider detail")
			}))
			s := admit(t, b, binding())
			if c, err := s.DataDialer().DialContext(context.Background(), "tcp", s.binding.Authority); c != nil || err != ErrOpen || calls != 1 {
				t.Fatal("provider error escaped or was retried", err, calls)
			}
			if err := s.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConcurrentDuplicateAdmissionProducesOneCapability(t *testing.T) {
	b := fixture(t, Limits{MaxSessions: 32, MaxDataConnections: 32, MaxDataPerSession: 1}, openFunc(func(context.Context, OpenRequest) (net.Conn, error) { return pipe(t), nil }))
	value := binding()
	start := make(chan struct{})
	results := make(chan error, 32)
	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, err := b.Admit(context.Background(), value)
			results <- err
		}()
	}
	close(start)
	group.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else if !errors.Is(err, ErrScope) {
			t.Fatal(err)
		}
	}
	if accepted != 1 || b.Snapshot().Sessions != 1 {
		t.Fatal("concurrent duplicate source gained more than one reservation", accepted)
	}
}

func TestHalfCloseKeepsReplyAndCustody(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		defer close(serverDone)
		peer, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer peer.Close()
		_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
		body, err := io.ReadAll(peer)
		if err == nil && string(body) != "request" {
			err = errors.New("request changed")
		}
		if err == nil {
			_, err = io.WriteString(peer, "reply")
		}
		serverDone <- err
	}()
	t.Cleanup(func() {
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			t.Error("TCP fixture did not join after broker cleanup")
		}
	})
	b := fixture(t, Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, openFunc(func(ctx context.Context, _ OpenRequest) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}))
	s := admit(t, b, binding())
	c := dial(t, s.DataDialer(), s.binding.Authority)
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(c, "request"); err != nil {
		t.Fatal(err)
	}
	if err := c.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(c)
	if err != nil || string(body) != "reply" {
		t.Fatal("half-close discarded source reply", err)
	}
	if err := waitError(t, serverDone); err != nil {
		t.Fatal(err)
	}
	if b.Snapshot().DataConnections != 1 {
		t.Fatal("EOF or half-close released physical connection custody")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
