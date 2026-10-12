// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package connreuse lets an HTTP/1 client keep connections between requests
// without giving up what a fresh connection per request provided.
//
// Callers still validate the verified peer chain against the current time on
// every response. This package adds the other two properties:
//
//   - net/http never replays a request. On a reused connection it would
//     otherwise resend an idempotent request after an ambiguous failure, which
//     can consume a single-reader result twice.
//   - a connection carries no new request once MaxAge has passed since it was
//     dialed, so a peer is re-authenticated at a bounded interval.
package connreuse

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"time"
)

// Transport wraps an *http.Transport configured by New.
type Transport struct {
	base   *http.Transport
	maxAge time.Duration
	now    func() time.Time
}

type agedConn struct {
	net.Conn
	dialed time.Time
}

type noBody struct{}

func (noBody) Read([]byte) (int, error) { return 0, io.EOF }
func (noBody) Close() error             { return nil }

// New enables bounded reuse on base and returns the wrapper to use as the
// client's RoundTripper. base must dial with DialContext and must not have
// been used yet. idle bounds an unused connection and must stay below the
// server's own idle timeout; maxAge bounds a connection's total life.
func New(base *http.Transport, idle, maxAge time.Duration) (*Transport, error) {
	if base == nil || base.DialContext == nil || base.DialTLSContext != nil || idle <= 0 || maxAge < idle || base.MaxConnsPerHost < 1 {
		return nil, errors.New("invalid connection reuse configuration")
	}
	t := &Transport{base: base, maxAge: maxAge, now: time.Now}
	dial := base.DialContext
	base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &agedConn{Conn: conn, dialed: t.now()}, nil
	}
	base.DisableKeepAlives = false
	base.IdleConnTimeout = idle
	base.MaxIdleConnsPerHost = base.MaxConnsPerHost
	base.MaxIdleConns = base.MaxConnsPerHost
	return t, nil
}

func dialTime(conn net.Conn) (time.Time, bool) {
	if secured, ok := conn.(*tls.Conn); ok {
		conn = secured.NetConn()
	}
	aged, ok := conn.(*agedConn)
	if !ok {
		return time.Time{}, false
	}
	return aged.dialed, true
}

// RoundTrip sends one request. It never retries.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	var out *http.Request
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		// Called on this goroutine before the request is written. The header
		// makes this the connection's last request: the server answers with
		// the same header and closes, and the transport does not pool the
		// connection again. The transport works on a shallow copy of out, so
		// only the shared header map reaches it; setting out.Close would not.
		if dialed, ok := dialTime(info.Conn); !ok || t.now().Sub(dialed) >= t.maxAge {
			out.Header.Set("Connection", "close")
		}
	}}
	out = req.Clone(httptrace.WithClientTrace(req.Context(), trace))
	if out.Header == nil {
		out.Header = http.Header{}
	}
	// A request with a body that cannot be rewound is never replayed. A
	// bodiless request gets an empty body, which is sent as no body at all.
	if out.Body == nil || out.Body == http.NoBody {
		out.Body = noBody{}
	}
	out.GetBody = nil
	return t.base.RoundTrip(out)
}

// CloseIdleConnections closes every unused connection.
func (t *Transport) CloseIdleConnections() { t.base.CloseIdleConnections() }
