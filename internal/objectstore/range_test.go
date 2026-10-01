// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

func TestS3CompatibleRanges(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs"} {
		t.Run(provider, func(t *testing.T) {
			t.Setenv("KELVO_SOURCE_RANGE_ID", "fixture-id")
			t.Setenv("KELVO_SOURCE_RANGE_SECRET", "fixture-not-a-credential")
			version := `"version-one"`
			if provider == "gcs" {
				version = "123"
			}
			mode := "valid"
			foreignCalls := 0
			foreign := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { foreignCalls++; w.WriteHeader(500) }))
			defer foreign.Close()
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/snapshots/kelvo/data" || r.Header.Get("Range") != "bytes=2-5" {
					t.Error("unexpected range request")
					w.WriteHeader(400)
					return
				}
				condition := r.Header.Get("If-Match")
				if provider == "gcs" {
					condition = r.Header.Get("x-goog-if-generation-match")
				}
				if condition != version {
					t.Error("missing immutable version condition")
					w.WriteHeader(412)
					return
				}
				if mode == "redirect" {
					w.Header().Set("Location", foreign.URL)
					w.WriteHeader(307)
					return
				}
				if provider == "gcs" {
					w.Header().Set("x-goog-generation", version)
				} else {
					w.Header().Set("ETag", version)
				}
				if mode == "version" {
					if provider == "gcs" {
						w.Header().Set("x-goog-generation", "124")
					} else {
						w.Header().Set("ETag", `"version-two"`)
					}
				}
				w.Header().Set("Content-Length", "4")
				w.Header().Set("Content-Range", "bytes 2-5/10")
				if mode == "wrong-range" {
					w.Header().Set("Content-Range", "bytes 1-4/10")
				}
				if mode == "unknown-total" {
					w.Header().Set("Content-Range", "bytes 2-5/*")
				}
				if mode == "full" {
					w.WriteHeader(200)
				} else {
					w.WriteHeader(206)
				}
				if mode == "short" {
					_, _ = w.Write([]byte("cd"))
				} else {
					_, _ = w.Write([]byte("cdef"))
				}
			}))
			defer server.Close()
			location := catalog.ObjectLocation{Provider: provider, Endpoint: server.URL, Bucket: "snapshots", Prefix: "kelvo"}
			if provider == "s3" {
				location.Region = "us-east-1"
			}
			client, err := New(location, catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_RANGE_ID", SecretAccessKeyEnv: "KELVO_SOURCE_RANGE_SECRET"})
			if err != nil {
				t.Fatal(err)
			}
			c := client.(*s3Client)
			c.http.Transport = server.Client().Transport
			defer c.Close()
			for _, testMode := range []string{"valid", "wrong-range", "unknown-total", "version", "full", "short", "redirect"} {
				mode = testMode
				body, info, err := c.GetRange(context.Background(), "kelvo/data", version, 2, 4)
				var data []byte
				if err == nil {
					data, err = io.ReadAll(body)
					body.Close()
				}
				if mode == "valid" {
					if err != nil || string(data) != "cdef" || info.Size != 10 {
						t.Fatalf("valid range: %q, %+v, %v", data, info, err)
					}
				} else if err == nil {
					t.Errorf("accepted %s range", mode)
				}
			}
			if foreignCalls != 0 {
				t.Fatal("range request followed a redirect")
			}
			for _, bounds := range [][2]int64{{-1, 1}, {0, 0}, {MaxUploadBytes, 1}, {MaxUploadBytes - 1, 2}, {1, 1 << 62}} {
				if _, _, err := c.GetRange(context.Background(), "kelvo/data", version, bounds[0], bounds[1]); err == nil {
					t.Fatal("accepted invalid bounds", bounds)
				}
			}
		})
	}
}

func TestExactRangeBody(t *testing.T) {
	for _, tc := range []struct {
		body  string
		size  int64
		valid bool
	}{{"abcd", 4, true}, {"abc", 4, false}, {"abcde", 4, false}} {
		r := ExactRangeBody(io.NopCloser(strings.NewReader(tc.body)), tc.size)
		_, err := io.ReadAll(r)
		r.Close()
		if (err == nil) != tc.valid {
			t.Fatalf("body %q, size %d: %v", tc.body, tc.size, err)
		}
	}
}
