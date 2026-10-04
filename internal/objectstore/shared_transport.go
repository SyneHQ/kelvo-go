// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

const sharedRequestLimit = 64
const sharedDialLimit = 4

var errTransportCapacity = errors.New("object transport capacity exhausted")

// SharedTransport belongs to one immutable node namespace. Credential-bearing
// clients borrow it; replacing a client cannot create another pool or bypass
// the global request/body and dialing bounds. Use separate data and registry
// instances so data traffic cannot consume registry renewal capacity.
//
// Quiescence joins admitted RoundTrip/body work, our dialing and cancellation
// callbacks. It does not join every net/http internal goroutine or erase
// retained Go-owned request data. No late upload callback can reach a reader
// after Client.Put has returned (see uploadBody).
type SharedTransport struct {
	location        catalog.ObjectLocation
	base            *http.Transport
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.Mutex
	closed          bool
	requests        int
	dials           int
	requestLifetime clientLifetime
	dialLifetime    clientLifetime
	work            sync.WaitGroup
	closeOne        sync.Once
	quiet           chan struct{}
	dial            func(context.Context, string, string) (net.Conn, error)
}

func NewSharedTransport(location catalog.ObjectLocation) (*SharedTransport, error) {
	if err := location.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &SharedTransport{location: location, ctx: ctx, cancel: cancel, quiet: make(chan struct{}),
		dial: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext}
	t.base = &http.Transport{Proxy: nil, DialContext: t.dialContext,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second, IdleConnTimeout: 90 * time.Second,
		MaxIdleConns: 4, MaxIdleConnsPerHost: 4, MaxConnsPerHost: 4,
		MaxResponseHeaderBytes: 64 << 10, DisableCompression: true}
	return t, nil
}

func (t *SharedTransport) reserve(dial bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return errClientClosed
	}
	count, maximum := &t.requests, sharedRequestLimit
	if dial {
		count, maximum = &t.dials, sharedDialLimit
	}
	if *count == maximum {
		return errTransportCapacity
	}
	*count++
	t.work.Add(1)
	return nil
}

func (t *SharedTransport) release(dial bool) {
	t.mu.Lock()
	if dial {
		t.dials--
	} else {
		t.requests--
	}
	t.mu.Unlock()
	t.work.Done()
}

// One bounded completion record remains charged through body Close and the
// cancellation callback. It cannot be released merely because RoundTrip ended.
func (t *SharedTransport) track(op *clientOperation, dial bool) {
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(t.ctx, func() { defer close(callbackDone); op.cancel() })
	go func() {
		<-op.done
		if !stop() {
			<-callbackDone
		}
		t.release(dial)
	}()
}

func (t *SharedTransport) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if err := t.reserve(true); err != nil {
		return nil, err
	}
	op, err := t.dialLifetime.begin(ctx)
	if err != nil {
		t.release(true)
		return nil, err
	}
	t.track(op, true)
	defer op.finish(true)
	conn, err := t.dial(op.ctx, network, address)
	if cause := context.Cause(op.ctx); cause != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, cause
	}
	if err != nil && conn != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, err
}

func (t *SharedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil || request.URL.Scheme+"://"+request.URL.Host != t.location.Endpoint {
		if request != nil && request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, errors.New("object transport refuses a different endpoint")
	}
	if err := t.reserve(false); err != nil {
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, err
	}
	op, err := t.requestLifetime.begin(request.Context())
	if err != nil {
		t.release(false)
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, err
	}
	t.track(op, false)
	defer op.finish(true)
	response, err := t.base.RoundTrip(request.WithContext(op.ctx))
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, err
	}
	if response == nil || response.Body == nil {
		return nil, errors.New("object transport returned an incomplete response")
	}
	response.Body, err = op.returnBody(response.Body)
	if err != nil {
		return nil, err
	}
	return response, nil
}

// Close seals admission and waits for owned work. A non-cooperative dial/body
// may keep it waiting; the owning runtime supplies bounded Close observation.
func (t *SharedTransport) Close() {
	t.closeOne.Do(func() {
		t.mu.Lock()
		t.closed = true
		t.mu.Unlock()
		t.cancel() // Signal every request and dial before joining either group.
		t.requestLifetime.close(func() {})
		t.dialLifetime.close(func() {})
		t.work.Wait()
		t.base.CloseIdleConnections()
		close(t.quiet)
	})
}

func (t *SharedTransport) Quiesced() <-chan struct{} { return t.quiet }

func (t *SharedTransport) client(location catalog.ObjectLocation) (*http.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || location != t.location {
		return nil, errors.New("object transport is closed or belongs to another namespace")
	}
	return &http.Client{Transport: t, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}, nil
}
