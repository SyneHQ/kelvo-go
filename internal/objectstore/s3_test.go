// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

func TestS3CompatibleConditionalPublication(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs"} {
		t.Run(provider, func(t *testing.T) {
			var mu sync.Mutex
			var data []byte
			var digest string
			generation := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.URL.Path != "/snapshots/kelvo/tenant/dataset/current.yaml" || r.URL.RawQuery != "" {
					t.Error("unexpected request target")
					w.WriteHeader(400)
					return
				}
				if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=fixture-id/") || !strings.Contains(r.Header.Get("Authorization"), "/s3/aws4_request") {
					t.Error("request was not SigV4 signed")
				}
				version := fmt.Sprintf(`"etag-%d"`, generation)
				if provider == "gcs" {
					version = strconv.Itoa(generation)
				}
				if r.Method == http.MethodPut {
					if provider == "gcs" {
						if r.Header.Get("If-Match") != "" || r.Header.Get("If-None-Match") != "" {
							t.Error("GCS used unsupported write ETag condition")
						}
						if r.Header.Get("x-goog-if-generation-match") != version {
							w.WriteHeader(412)
							return
						}
					} else if (generation == 0 && r.Header.Get("If-None-Match") != "*") || (generation > 0 && r.Header.Get("If-Match") != version) {
						w.WriteHeader(412)
						return
					}
					body, err := io.ReadAll(io.LimitReader(r.Body, 4097))
					if err != nil || len(body) > 4096 {
						t.Error("invalid upload body")
						w.WriteHeader(400)
						return
					}
					h := sha256.Sum256(body)
					if r.Header.Get("x-amz-content-sha256") != hex.EncodeToString(h[:]) {
						t.Error("payload signature hash mismatch")
					}
					data = body
					digest = hex.EncodeToString(h[:])
					generation++
					version = fmt.Sprintf(`"etag-%d"`, generation)
					if provider == "gcs" {
						version = strconv.Itoa(generation)
					}
				} else if generation == 0 {
					w.WriteHeader(404)
					return
				} else {
					condition := r.Header.Get("If-Match")
					if provider == "gcs" {
						condition = r.Header.Get("x-goog-if-generation-match")
					}
					if condition != "" && condition != version {
						w.WriteHeader(412)
						return
					}
				}
				if provider == "gcs" {
					w.Header().Set("x-goog-generation", version)
					w.Header().Set("x-goog-meta-kelvo-sha256", digest)
				} else {
					w.Header().Set("ETag", version)
					w.Header().Set("x-amz-meta-kelvo-sha256", digest)
				}
				if r.Method != http.MethodPut {
					w.Header().Set("Content-Length", strconv.Itoa(len(data)))
					if r.Method == http.MethodGet {
						w.Write(data)
					}
				}
			}))
			defer server.Close()
			t.Setenv("KELVO_SOURCE_OBJECT_READ_ID", "fixture-id")
			t.Setenv("KELVO_SOURCE_OBJECT_READ_SECRET", "fixture-secret-not-a-credential")
			location := catalog.ObjectLocation{Provider: provider, Endpoint: server.URL, Bucket: "snapshots", Prefix: "kelvo"}
			if provider == "s3" {
				location.Region = "us-east-1"
			}
			client, err := New(location, catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_OBJECT_READ_ID", SecretAccessKeyEnv: "KELVO_SOURCE_OBJECT_READ_SECRET"})
			if err != nil {
				t.Fatal(err)
			}
			c := client.(*s3Client)
			c.http.Transport = server.Client().Transport
			defer c.Close()
			ctx := context.Background()
			key := "kelvo/tenant/dataset/current.yaml"
			if _, err = c.Head(ctx, key, ""); !errors.Is(err, ErrNotFound) {
				t.Fatal("missing object", err)
			}
			body := []byte("committed: generation-one\n")
			h := sha256.Sum256(body)
			sum := hex.EncodeToString(h[:])
			first, err := c.Put(ctx, key, bytes.NewReader(body), int64(len(body)), sum, Condition{Absent: true})
			if err != nil || first.Version == "" {
				t.Fatal("create", first, err)
			}
			if _, err = c.Put(ctx, key, bytes.NewReader(body), int64(len(body)), sum, Condition{Absent: true}); !errors.Is(err, ErrConflict) {
				t.Fatal("create-only overwrite accepted", err)
			}
			reader, info, err := c.Get(ctx, key, first.Version)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(reader)
			reader.Close()
			if err != nil || !bytes.Equal(got, body) || info.Size != int64(len(body)) || info.SHA256 != sum {
				t.Fatal("read changed object", err)
			}
			second, err := c.Put(ctx, key, bytes.NewReader(body), int64(len(body)), sum, Condition{Version: first.Version})
			if err != nil || second.Version == first.Version {
				t.Fatal("conditional replacement", err)
			}
			if _, err = c.Put(ctx, key, bytes.NewReader(body), int64(len(body)), sum, Condition{Version: first.Version}); !errors.Is(err, ErrConflict) {
				t.Fatal("stale writer was not fenced", err)
			}
			if _, err = c.Head(ctx, key, first.Version); !errors.Is(err, ErrConflict) {
				t.Fatal("read revision was not checked", err)
			}
			if _, err = c.Head(ctx, "kelvo-other/tenant/data", ""); err == nil {
				t.Fatal("namespace escape accepted")
			}
		})
	}
}

func TestS3ClientRefusesRedirectsAndUnsafeConditions(t *testing.T) {
	redirected := false
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected = true; w.WriteHeader(200) }))
	defer target.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(307)
	}))
	defer server.Close()
	t.Setenv("KELVO_SOURCE_OBJECT_READ_ID", "fixture-id")
	t.Setenv("KELVO_SOURCE_OBJECT_READ_SECRET", "fixture-secret-not-a-credential")
	client, err := New(catalog.ObjectLocation{Provider: "s3", Endpoint: server.URL, Bucket: "snapshots", Prefix: "kelvo", Region: "us-east-1"}, catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_OBJECT_READ_ID", SecretAccessKeyEnv: "KELVO_SOURCE_OBJECT_READ_SECRET"})
	if err != nil {
		t.Fatal(err)
	}
	c := client.(*s3Client)
	c.http.Transport = server.Client().Transport
	defer c.Close()
	if _, err = c.Head(context.Background(), "kelvo/object", ""); err == nil || redirected {
		t.Fatal("followed credential redirect", err)
	}
	empty := sha256.Sum256(nil)
	for _, condition := range []Condition{{}, {Absent: true, Version: `"old"`}, {Version: "unquoted"}} {
		if _, err = c.Put(context.Background(), "kelvo/object", bytes.NewReader(nil), 0, hex.EncodeToString(empty[:]), condition); err == nil {
			t.Fatal("unsafe condition accepted")
		}
	}
}
