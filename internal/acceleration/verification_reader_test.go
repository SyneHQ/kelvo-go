// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

type verificationTestClient struct {
	get                               func(context.Context, string, string) (io.ReadCloser, objectstore.Info, error)
	rangeGet                          func(context.Context, string, string, int64, int64) (io.ReadCloser, objectstore.Info, error)
	head                              func(context.Context, string, string) (objectstore.Info, error)
	gets, ranges, heads, puts, closes atomic.Int32
}

func (c *verificationTestClient) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	c.gets.Add(1)
	return c.get(ctx, key, version)
}
func (c *verificationTestClient) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, objectstore.Info, error) {
	c.ranges.Add(1)
	return c.rangeGet(ctx, key, version, offset, length)
}
func (c *verificationTestClient) Head(ctx context.Context, key, version string) (objectstore.Info, error) {
	c.heads.Add(1)
	return c.head(ctx, key, version)
}
func (c *verificationTestClient) Put(context.Context, string, io.ReadSeeker, int64, string, objectstore.Condition) (objectstore.Info, error) {
	c.puts.Add(1)
	return objectstore.Info{}, nil
}
func (c *verificationTestClient) Close() { c.closes.Add(1) }

type verificationTestBody struct {
	read          func([]byte) (int, error)
	close         func() error
	reads, closes atomic.Int32
}

func (b *verificationTestBody) Read(p []byte) (int, error) { b.reads.Add(1); return b.read(p) }
func (b *verificationTestBody) Close() error {
	b.closes.Add(1)
	if b.close != nil {
		return b.close()
	}
	return nil
}

func verificationBodyClient(body io.ReadCloser, err error) *verificationTestClient {
	return &verificationTestClient{get: func(context.Context, string, string) (io.ReadCloser, objectstore.Info, error) {
		return body, objectstore.Info{}, err
	}}
}

func verificationAwait[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("verification fixture did not hand back within five seconds")
		var zero T
		return zero
	}
}

func TestVerificationReaderCountsRootDescriptorAndRepeatedRanges(t *testing.T) {
	client := &verificationTestClient{
		get: func(_ context.Context, key, _ string) (io.ReadCloser, objectstore.Info, error) {
			return io.NopCloser(strings.NewReader(key)), objectstore.Info{}, nil
		},
		rangeGet: func(context.Context, string, string, int64, int64) (io.ReadCloser, objectstore.Info, error) {
			return objectstore.ExactRangeBody(io.NopCloser(strings.NewReader("abc")), 3), objectstore.Info{}, nil
		},
	}
	r := newVerificationReader(client, 32)
	for _, key := range []string{"one", "three"} {
		body, _, err := r.Get(context.Background(), key, "version")
		if err != nil {
			t.Fatal(err)
		}
		if data, err := io.ReadAll(body); err != nil || string(data) != key {
			t.Fatal("metadata read", err)
		}
		if err := body.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		body, _, err := r.GetRange(context.Background(), "payload", "version", 0, 3)
		if err != nil {
			t.Fatal(err)
		}
		if data, err := io.ReadAll(body); err != nil || string(data) != "abc" {
			t.Fatal("range read", err)
		}
		if err := body.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if r.Remaining() != 16 || r.Err() != nil {
		t.Fatal("cumulative bytes or EOF allowance lost", r.Remaining(), r.Err())
	}
	r.Close()
	if client.closes.Load() != 0 {
		t.Fatal("borrowed reader closed the shared provider")
	}
}

func TestVerificationReaderStopsAtActualByteBudget(t *testing.T) {
	var passedLength int
	body := &verificationTestBody{read: func(p []byte) (int, error) { passedLength = len(p); return copy(p, "abcdef"), nil }}
	client := verificationBodyClient(body, nil)
	r := newVerificationReader(client, 5)
	read, _, err := r.Get(context.Background(), "key", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := read.Read(make([]byte, 9)); n != 5 || err != nil || passedLength != 5 {
		t.Fatal("read exceeded allowance", n, err, passedLength)
	}
	if n, err := read.Read(make([]byte, 1)); n != 0 || !errors.Is(err, errVerificationLimit) || errors.Is(err, io.EOF) {
		t.Fatal("budget masqueraded as EOF", n, err)
	}
	if r.Remaining() != 0 || body.reads.Load() != 1 {
		t.Fatal("exhaustion delegated another read")
	}
	if _, _, err := r.Get(context.Background(), "other", "v1"); !errors.Is(err, errVerificationLimit) || client.gets.Load() != 1 {
		t.Fatal("budget reset between objects", err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVerificationReaderPreflightAndRangeAdmissionStayBounded(t *testing.T) {
	for _, want := range []int64{-1, 11, math.MaxInt64} {
		r := newVerificationReader(&verificationTestClient{}, 10)
		if !errors.Is(r.Preflight(want), errVerificationLimit) || r.Remaining() != 10 || !errors.Is(r.Preflight(0), errVerificationLimit) {
			t.Fatal("preflight wrapped, charged or cleared its failure", want)
		}
	}
	r := newVerificationReader(&verificationTestClient{}, 10)
	if r.Preflight(10) != nil || r.Preflight(10) != nil || r.Remaining() != 10 {
		t.Fatal("preflight charged declared bytes")
	}
	client := &verificationTestClient{}
	r = newVerificationReader(client, 3)
	if _, _, err := r.GetRange(context.Background(), "key", "v1", 0, 3); !errors.Is(err, errVerificationLimit) || client.ranges.Load() != 0 {
		t.Fatal("range opened without EOF headroom", err)
	}
	r = newVerificationReader(client, 10)
	if _, err := r.Put(context.Background(), "key", nil, 0, "", objectstore.Condition{}); err == nil || client.puts.Load() != 0 {
		t.Fatal("verification performed a write")
	}
}

func TestVerificationReaderRangeEOFMustFitRemainingHeadroom(t *testing.T) {
	for _, limit := range []int64{4, 5} {
		client := &verificationTestClient{rangeGet: func(context.Context, string, string, int64, int64) (io.ReadCloser, objectstore.Info, error) {
			return objectstore.ExactRangeBody(io.NopCloser(strings.NewReader("abc")), 3), objectstore.Info{}, nil
		}}
		r := newVerificationReader(client, limit)
		body, _, err := r.GetRange(context.Background(), "key", "v1", 0, 3)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(body)
		if string(data) != "abc" || r.Remaining() != limit-4 {
			t.Fatal("range accounting", string(data), r.Remaining())
		}
		if limit == 4 && !errors.Is(err, errVerificationLimit) || limit == 5 && err != nil {
			t.Fatal("unmetered or refused EOF probe", limit, err)
		}
		if err := body.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestVerificationReaderConcurrentReservationsRefundUnusedBytes(t *testing.T) {
	entered, release := make(chan int, 1), make(chan struct{})
	var releaseOnce sync.Once
	releaseRead := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseRead()
	first := &verificationTestBody{read: func(p []byte) (int, error) { entered <- len(p); <-release; return copy(p, "four"), nil }}
	second := &verificationTestBody{read: func(p []byte) (int, error) { return copy(p, "xy"), nil }}
	client := &verificationTestClient{get: func(_ context.Context, key, _ string) (io.ReadCloser, objectstore.Info, error) {
		if key == "first" {
			return first, objectstore.Info{}, nil
		}
		return second, objectstore.Info{}, nil
	}}
	r := newVerificationReader(client, 10)
	a, _, err := r.Get(context.Background(), "first", "v1")
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := r.Get(context.Background(), "second", "v1")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		n, err := a.Read(make([]byte, 8))
		if err == nil && n != 4 {
			err = errors.New("first read count")
		}
		done <- err
	}()
	if got := verificationAwait(t, entered); got != 8 {
		t.Fatal("first reservation", got)
	}
	if r.Remaining() != 2 || r.Preflight(2) != nil {
		t.Fatal("in-flight bytes were available twice")
	}
	n, secondErr := b.Read(make([]byte, 8))
	releaseRead()
	firstErr := verificationAwait(t, done)
	if n != 2 || secondErr != nil || firstErr != nil || r.Remaining() != 4 || r.Err() != nil {
		t.Fatal("concurrent reservation/refund", n, secondErr, firstErr, r.Remaining(), r.Err())
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVerificationReaderPartialAndNilBodiesRemainOwned(t *testing.T) {
	marker := errors.New("provider failed")
	for _, mode := range []string{"partial", "partial-close", "request-eof", "nil", "typed-nil"} {
		t.Run(mode, func(t *testing.T) {
			body := &verificationTestBody{read: func([]byte) (int, error) { return 0, io.EOF }}
			var input io.ReadCloser = body
			var requestErr error
			switch mode {
			case "partial":
				requestErr = marker
			case "partial-close":
				requestErr = marker
				body.close = func() error { return errors.New("private-close-detail") }
			case "request-eof":
				requestErr = io.EOF
			case "nil":
				input = nil
			case "typed-nil":
				input = (*verificationTestBody)(nil)
			}
			r := newVerificationReader(verificationBodyClient(input, requestErr), 10)
			got, _, err := r.Get(context.Background(), "key", "v1")
			if err == nil || r.Err() == nil {
				t.Fatal("invalid response accepted")
			}
			if requestErr != nil {
				if got == nil || !errors.Is(r.Err(), requestErr) {
					t.Fatal("partial ownership or error lost", err)
				}
				if err := got.Close(); mode == "partial-close" && !errors.Is(err, errReaderCleanupUnknown) || mode != "partial-close" && err != nil {
					t.Fatal(err)
				}
				if err := got.Close(); mode == "partial-close" && !errors.Is(err, errReaderCleanupUnknown) || mode != "partial-close" && err != nil {
					t.Fatal(err)
				}
				if !errors.Is(r.Err(), requestErr) || mode == "partial-close" && (!errors.Is(r.Err(), errReaderCleanupUnknown) || strings.Contains(r.Err().Error(), "private-close")) {
					t.Fatal("partial failure was cleared or cleanup leaked", r.Err())
				}
				if body.closes.Load() != 1 {
					t.Fatal("partial body not closed exactly once")
				}
			} else if got != nil {
				t.Fatal("nil response acquired a body")
			}
		})
	}
}

func TestVerificationReaderDataErrorsAndOnlyPlainEOF(t *testing.T) {
	marker := errors.New("provider read failed")
	for _, failure := range []error{io.EOF, fmt.Errorf("wrapped: %w", io.EOF), errors.Join(io.EOF, marker), errors.Join(ErrCorrupt, marker), io.ErrUnexpectedEOF} {
		body := &verificationTestBody{read: func(p []byte) (int, error) { return copy(p, "ok"), failure }}
		r := newVerificationReader(verificationBodyClient(body, nil), 10)
		got, _, err := r.Get(context.Background(), "key", "v1")
		if err != nil {
			t.Fatal(err)
		}
		n, err := got.Read(make([]byte, 8))
		if n != 2 || r.Remaining() != 8 {
			t.Fatal("data+error bytes not charged", n, r.Remaining())
		}
		if failure == io.EOF {
			if err != io.EOF || r.Err() != nil {
				t.Fatal("ordinary EOF became operational failure", err, r.Err())
			}
		} else if r.Err() == nil || !errors.Is(err, failure) {
			t.Fatal("operational read error disappeared", err)
		}
		if err := got.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestVerificationReaderPreservesErrorsMaskedByParquet(t *testing.T) {
	marker := errors.New("range provider failed")
	for _, kind := range []string{"provider", "budget", "close"} {
		t.Run(kind, func(t *testing.T) {
			want, limit := marker, int64(1024)
			client := &verificationTestClient{rangeGet: func(context.Context, string, string, int64, int64) (io.ReadCloser, objectstore.Info, error) {
				return nil, objectstore.Info{}, marker
			}}
			if kind == "budget" {
				want, limit = errVerificationLimit, 8
			}
			if kind == "close" {
				want = errReaderCleanupUnknown
				client.rangeGet = func(context.Context, string, string, int64, int64) (io.ReadCloser, objectstore.Info, error) {
					return &verificationTestBody{read: strings.NewReader("12345678").Read, close: func() error { return errors.New("private-close-detail") }}, objectstore.Info{Size: 100, Version: "v1", SHA256: "digest"}, nil
				}
			}
			r := newVerificationReader(client, limit)
			_, err := readParquetSchema(&schemaObjectReader{ctx: context.Background(), client: r, protected: true,
				snapshot: Snapshot{Bytes: 100, ObjectKey: "key", ObjectVersion: "v1", SHA256: "digest"}}, 100, nil)
			if !errors.Is(err, ErrCorrupt) || !errors.Is(r.Err(), want) || strings.Contains(r.Err().Error(), "private-close") {
				t.Fatal("Parquet concealed an operation failure", err, r.Err())
			}
			if kind == "budget" && client.ranges.Load() != 0 {
				t.Fatal("exhausted footer budget reached provider")
			}
		})
	}
}

func TestVerificationReaderCloseJoinsReadsAndSanitizesFailure(t *testing.T) {
	entered, closeEntered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseRead := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseRead()
	body := &verificationTestBody{read: func(p []byte) (int, error) { close(entered); <-release; return copy(p, "x"), nil },
		close: func() error { close(closeEntered); return errors.New("private-provider-close-detail") }}
	r := newVerificationReader(verificationBodyClient(body, nil), 10)
	got, _, err := r.Get(context.Background(), "key", "v1")
	if err != nil {
		t.Fatal(err)
	}
	readDone, closed := make(chan error, 1), make(chan error, 1)
	go func() { _, err := got.Read(make([]byte, 8)); readDone <- err }()
	verificationAwait(t, entered)
	go func() { closed <- got.Close() }()
	verificationAwait(t, closeEntered)
	select {
	case <-closed:
		t.Fatal("Close returned before body read handback")
	default:
	}
	releaseRead()
	verificationAwait(t, readDone)
	if err := verificationAwait(t, closed); !errors.Is(err, errReaderCleanupUnknown) {
		t.Fatal("Close failure was not classified", err)
	}
	if !errors.Is(r.Err(), errReaderCleanupUnknown) || strings.Contains(r.Err().Error(), "private-provider") || r.Remaining() != 9 {
		t.Fatal("cleanup error or read accounting lost", r.Err(), r.Remaining())
	}
	if err := got.Close(); !errors.Is(err, errReaderCleanupUnknown) || body.closes.Load() != 1 {
		t.Fatal("Close repeated provider work", err)
	}
}

func TestVerificationReaderChecksContextBeforeEveryBodyRead(t *testing.T) {
	marker := errors.New("lease lost")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	body := &verificationTestBody{read: func([]byte) (int, error) { return 0, nil }}
	r := newVerificationReader(verificationBodyClient(body, nil), 10)
	got, _, err := r.Get(ctx, "key", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := got.Read(make([]byte, 8)); n != 0 || err != nil || r.Remaining() != 10 {
		t.Fatal("zero-progress refund", n, err)
	}
	cancel(marker)
	if n, err := got.Read(make([]byte, 8)); n != 0 || !errors.Is(err, marker) || body.reads.Load() != 1 {
		t.Fatal("canceled zero-progress reader kept running", n, err)
	}
	if err := got.Close(); err != nil {
		t.Fatal(err)
	}
	deadline, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	client := &verificationTestClient{}
	r = newVerificationReader(client, 10)
	if _, _, err := r.Get(deadline, "key", "v1"); !errors.Is(err, context.DeadlineExceeded) || client.gets.Load() != 0 {
		t.Fatal("expired deadline performed I/O", err)
	}
}

func TestVerificationReaderRejectsInvalidDependenciesAndReadCounts(t *testing.T) {
	for _, limit := range []int64{0, -1, math.MaxInt64} {
		r := newVerificationReader(&verificationTestClient{}, limit)
		if _, _, err := r.Get(context.Background(), "key", "v1"); err == nil || r.Remaining() != 0 {
			t.Fatal("invalid meter performed I/O")
		}
	}
	for _, client := range []objectstore.RangeClient{nil, (*verificationTestClient)(nil)} {
		if r := newVerificationReader(client, 10); r.Err() == nil {
			t.Fatal("nil provider accepted")
		}
	}
	for _, count := range []int{-1, 11} {
		body := &verificationTestBody{read: func([]byte) (int, error) { return count, nil }}
		r := newVerificationReader(verificationBodyClient(body, nil), 10)
		got, _, err := r.Get(context.Background(), "key", "v1")
		if err != nil {
			t.Fatal(err)
		}
		if n, err := got.Read(make([]byte, 10)); n != 0 || err == nil || r.Remaining() != 0 {
			t.Fatal("invalid provider count escaped", n, err, r.Remaining())
		}
		if err := got.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
