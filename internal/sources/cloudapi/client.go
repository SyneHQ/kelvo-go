// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package cloudapi contains the bounded HTTP transport used by cloud SQL APIs.
package cloudapi

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type Client struct {
	Origin *url.URL
	HTTP   *http.Client
	Token  string
	Limit  int64
}

func New(source catalog.Source, limits query.Limits) (*Client, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	raw := os.Getenv(source.URLEnv)
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, query.NewError("CONFIGURATION_ERROR", "Cloud source requires an HTTPS origin")
	}
	token := os.Getenv(source.TokenEnv)
	if token == "" || len(token) > 16<<10 || strings.ContainsAny(token, "\r\n") {
		return nil, query.NewError("CONFIGURATION_ERROR", "Cloud source token is unavailable")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.DisableCompression = true
	// Managed database endpoints vary in TLS 1.3 support. Require verified
	// TLS 1.2 or newer and let the handshake negotiate the newest common version.
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	tr.TLSHandshakeTimeout = 5 * time.Second
	tr.ResponseHeaderTimeout = limits.Timeout
	tr.MaxConnsPerHost = 2
	tr.MaxResponseHeaderBytes = 64 << 10
	return &Client{Origin: u, HTTP: &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, Token: token, Limit: min(32<<20, int64(limits.MemoryMB)<<18)}, nil
}
func (c *Client) Close() { c.HTTP.CloseIdleConnections() }

// Do never accepts an absolute URL or a redirect from a provider response.
// Decode into a fresh response struct each time to avoid retaining stale state.
func (c *Client) Do(ctx context.Context, method, path string, body any, headers map[string]string, out any) (int, int64, error) {
	var input io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, 0, query.NewError("INVALID_ARGUMENT", "Invalid cloud query request")
		}
		input = bytes.NewReader(b)
	}
	return c.do(ctx, method, path, input, "application/json", headers, out)
}

// DoText uses the same bounded, authenticated transport for protocols whose
// request body is SQL text rather than JSON (Trino and Presto).
func (c *Client) DoText(ctx context.Context, method, path, body string, headers map[string]string, out any) (int, int64, error) {
	return c.do(ctx, method, path, strings.NewReader(body), "text/plain; charset=utf-8", headers, out)
}

func (c *Client) do(ctx context.Context, method, path string, input io.Reader, contentType string, headers map[string]string, out any) (int, int64, error) {
	ref, err := url.Parse(path)
	if err != nil || ref.IsAbs() || ref.Host != "" || ref.Fragment != "" || !strings.HasPrefix(ref.Path, "/") || strings.HasPrefix(ref.Path, "//") {
		return 0, 0, query.NewError("QUERY_FAILED", "Invalid cloud pagination path")
	}
	u := *c.Origin
	u.Path = ref.Path
	u.RawPath = ref.RawPath
	u.RawQuery = ref.RawQuery
	r, err := http.NewRequestWithContext(ctx, method, u.String(), input)
	if err != nil {
		return 0, 0, query.NewError("QUERY_FAILED", "Cannot prepare cloud request")
	}
	r.Header.Set("Authorization", "Bearer "+c.Token)
	r.Header.Set("Content-Type", contentType)
	r.Header.Set("Accept", "application/json")
	for key, val := range headers {
		r.Header.Set(key, val)
	}
	response, err := c.HTTP.Do(r)
	if err != nil {
		if ctx.Err() != nil {
			return 0, 0, ctx.Err()
		}
		return 0, 0, query.NewError("QUERY_FAILED", "Cloud source request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response.StatusCode, 0, query.NewError("QUERY_FAILED", "Cloud source rejected request")
	}
	wire := &countReader{Reader: response.Body}
	var reader io.Reader = wire
	if response.Header.Get("Content-Encoding") == "gzip" {
		z, e := gzip.NewReader(wire)
		if e != nil {
			return response.StatusCode, 0, query.NewError("QUERY_FAILED", "Invalid compressed cloud response")
		}
		defer z.Close()
		reader = z
	} else if enc := response.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		return response.StatusCode, 0, query.NewError("QUERY_FAILED", "Unsupported cloud response encoding")
	}
	b, err := io.ReadAll(io.LimitReader(reader, c.Limit+1))
	if err != nil {
		if ctx.Err() != nil {
			return response.StatusCode, wire.n, ctx.Err()
		}
		return response.StatusCode, wire.n, query.NewError("QUERY_FAILED", "Cloud source returned an incomplete response")
	}
	if int64(len(b)) > c.Limit {
		return response.StatusCode, wire.n, query.NewError("RESOURCE_EXHAUSTED", "Cloud response exceeds its memory budget")
	}
	if out == nil {
		return response.StatusCode, wire.n, nil
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return response.StatusCode, wire.n, query.NewError("QUERY_FAILED", "Cloud source returned invalid JSON")
	}
	return response.StatusCode, wire.n, nil
}

type countReader struct {
	io.Reader
	n int64
}

func (r *countReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.n += int64(n)
	return n, err
}

func Poll(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func Source(c catalog.Config, id, kind string) (catalog.Source, error) {
	for _, s := range c.Sources {
		if s.ID == id && s.Type == kind {
			return s, nil
		}
	}
	return catalog.Source{}, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
}

func SingleSource(c catalog.Config, kind string) (catalog.Source, error) {
	var source catalog.Source
	count := 0
	for _, s := range c.Sources {
		if s.Type == kind {
			source = s
			count++
		}
	}
	if count != 1 {
		return source, query.NewError("CONFIGURATION_ERROR", "Native executor requires exactly one selected source")
	}
	return source, nil
}
