// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cloudapi

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func testClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	t.Setenv("CLOUD_TEST_ORIGIN", server.URL)
	t.Setenv("CLOUD_TEST_TOKEN", "private-token")
	c, err := New(catalog.Source{URLEnv: "CLOUD_TEST_ORIGIN", TokenEnv: "CLOUD_TEST_TOKEN"}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	c.HTTP.Transport = server.Client().Transport
	t.Cleanup(c.Close)
	return c
}
func TestTransportJSONAndCompression(t *testing.T) {
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-token" {
			t.Error("missing bearer")
		}
		w.Header().Set("Content-Encoding", "gzip")
		z := gzip.NewWriter(w)
		defer z.Close()
		_, _ = z.Write([]byte(`{"v":9223372036854775807}`))
	})
	var out map[string]any
	_, _, err := c.Do(context.Background(), "GET", "/test", nil, nil, &out)
	if err != nil || out["v"] != json.Number("9223372036854775807") {
		t.Fatalf("exact numeric decode: %v %v", out, err)
	}
	c.Limit = 5
	_, _, err = c.Do(context.Background(), "GET", "/test", nil, nil, &out)
	if query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("gzip expansion not bounded: %v", err)
	}
}
func TestTransportRejectsRedirectsPathsErrorsAndTrailingJSON(t *testing.T) {
	for _, test := range []struct {
		name, body string
		status     int
	}{{"redirect", "", 302}, {"rejected", "private-token", 403}, {"trailing", `{} {}`, 200}, {"malformed", `private-token`, 200}} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Location", "https://other.invalid/private-token")
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			})
			var out any
			_, _, err := c.Do(context.Background(), "GET", "/test", nil, nil, &out)
			if err == nil || strings.Contains(err.Error(), "private-token") || calls != 1 {
				t.Fatalf("unsafe failure: %v calls=%d", err, calls)
			}
		})
	}
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) { t.Error("unsafe path requested") })
	for _, path := range []string{"https://evil.invalid/", "//evil.invalid/", "relative", "/ok#fragment"} {
		if _, _, err := c.Do(context.Background(), "GET", path, nil, nil, nil); err == nil {
			t.Fatalf("accepted %q", path)
		}
	}
}
func TestClientConfigurationAndCancellation(t *testing.T) {
	t.Setenv("CLOUD_TEST_TOKEN", "token")
	for _, origin := range []string{"http://example.com", "https://u:p@example.com", "https://example.com/path", "https://example.com/?x=1", "https://example.com/#x"} {
		t.Setenv("CLOUD_TEST_ORIGIN", origin)
		if _, err := New(catalog.Source{URLEnv: "CLOUD_TEST_ORIGIN", TokenEnv: "CLOUD_TEST_TOKEN"}, query.DefaultLimits()); err == nil {
			t.Fatalf("accepted %s", origin)
		}
	}
	t.Setenv("CLOUD_TEST_ORIGIN", "https://example.com")
	c, err := New(catalog.Source{URLEnv: "CLOUD_TEST_ORIGIN", TokenEnv: "CLOUD_TEST_TOKEN"}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tr := c.HTTP.Transport.(*http.Transport)
	if tr.Proxy != nil || tr.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatal("unsafe transport defaults")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err = c.Do(ctx, "GET", "/", nil, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func TestTransportRawSQLUsesSameBoundaries(t *testing.T) {
	const sql = "SELECT 'literal' AS label, 9007199254740993 AS large"
	calls := 0
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != sql || r.Method != http.MethodPost || r.Header.Get("Content-Type") != "text/plain; charset=utf-8" || r.Header.Get("Authorization") != "Bearer private-token" || r.Header.Get("X-Trino-User") != "reader" {
			t.Error("SQL request changed")
		}
		_, _ = w.Write([]byte(`{"value":9007199254740993}`))
	})
	var out map[string]any
	if _, _, err := c.DoText(context.Background(), http.MethodPost, "/v1/statement", sql, map[string]string{"X-Trino-User": "reader"}, &out); err != nil || out["value"] != json.Number("9007199254740993") {
		t.Fatalf("raw transport result %v %v", out, err)
	}
	if _, _, err := c.DoText(context.Background(), http.MethodPost, "https://other.invalid/", sql, nil, nil); err == nil || calls != 1 {
		t.Fatal("raw transport bypassed origin restriction")
	}
}
