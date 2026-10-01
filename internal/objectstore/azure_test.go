// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

const azureTestDate = "Fri, 02 Oct 2026 12:00:00 GMT"

func azureTestSAS() string {
	values := url.Values{
		"sv":  {azureStorageVersion},
		"sp":  {"rcw"},
		"sr":  {"c"},
		"se":  {"2030-01-01T00:00:00Z"},
		"spr": {"https"},
		"sig": {base64.StdEncoding.EncodeToString([]byte("fixture-only Azure SAS signature"))},
	}
	return values.Encode()
}

func azureTestLocation(endpoint string) catalog.ObjectLocation {
	return catalog.ObjectLocation{Provider: "azure", Endpoint: endpoint, Account: "kelvofixture", Bucket: "snapshots", Prefix: "tenant-a"}
}

func azureTestClient(t *testing.T, handler http.HandlerFunc) (*azureClient, *httptest.Server) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	t.Setenv("KELVO_SOURCE_AZURE_TEST_SAS", azureTestSAS())
	client, err := newAzure(azureTestLocation(server.URL), catalog.ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_AZURE_TEST_SAS"})
	if err != nil {
		t.Fatal(err)
	}
	azure := client.(*azureClient)
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	azure.http.Transport.(*http.Transport).TLSClientConfig.RootCAs = pool
	t.Cleanup(azure.Close)
	return azure, server
}

func azureTestDigest(data string) string {
	digest := sha256.Sum256([]byte(data))
	return hex.EncodeToString(digest[:])
}

func azureTestHeaders(w http.ResponseWriter, data, version string) {
	w.Header().Set("ETag", version)
	w.Header().Set("Date", azureTestDate)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("x-ms-meta-kelvo-sha256", azureTestDigest(data))
}

func TestAzureImmutableUploadAndManifestCompareAndSwap(t *testing.T) {
	type object struct{ data, version, digest string }
	objects := map[string]object{}
	var lock sync.Mutex
	var sequence int
	client, _ := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/snapshots/tenant-a/") || r.Header.Get("Authorization") != "" || r.Header.Get("x-ms-version") != azureStorageVersion {
			t.Errorf("unexpected request boundary: method=%s path=%s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.URL.RawQuery != azureTestSAS() {
			t.Error("SAS query did not match the configured explicit token")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		lock.Lock()
		defer lock.Unlock()
		current, exists := objects[r.URL.Path]
		switch r.Method {
		case http.MethodPut:
			if r.Header.Get("x-ms-blob-type") != "BlockBlob" || r.ContentLength < 0 {
				t.Error("upload lacked explicit block-blob type or size")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if r.Header.Get("If-None-Match") == "*" {
				if exists {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			} else if r.Header.Get("If-Match") == "" || !exists || r.Header.Get("If-Match") != current.version {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil || int64(len(body)) != r.ContentLength || r.Header.Get("x-ms-meta-kelvo-sha256") != azureTestDigest(string(body)) {
				t.Error("upload changed the supplied bytes, size, or SHA256 metadata")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			sequence++
			current = object{data: string(body), version: fmt.Sprintf(`"version-%d"`, sequence), digest: azureTestDigest(string(body))}
			objects[r.URL.Path] = current
			w.Header().Set("ETag", current.version)
			w.Header().Set("Date", azureTestDate)
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet, http.MethodHead:
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if v := r.Header.Get("If-Match"); v != "" && v != current.version {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			azureTestHeaders(w, current.data, current.version)
			if r.Method == http.MethodGet {
				_, _ = io.WriteString(w, current.data)
			}
		default:
			t.Errorf("unexpected operation %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	ctx := context.Background()
	key := "tenant-a/orders/manifest.yaml"
	if _, err := client.Head(ctx, key, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing object: %v", err)
	}
	firstData := "generation: first\n"
	first, err := client.Put(ctx, key, strings.NewReader(firstData), int64(len(firstData)), azureTestDigest(firstData), Condition{Absent: true})
	if err != nil || first.Size != int64(len(firstData)) || first.SHA256 != azureTestDigest(firstData) || first.Version == "" {
		t.Fatalf("initial publication: %+v, %v", first, err)
	}
	date, _ := http.ParseTime(azureTestDate)
	if !first.ServerTime.Equal(date) {
		t.Fatal("publisher did not preserve server time")
	}
	if _, err = client.Put(ctx, key, strings.NewReader(firstData), int64(len(firstData)), azureTestDigest(firstData), Condition{Absent: true}); !errors.Is(err, ErrConflict) {
		t.Fatalf("create-only overwrite was accepted: %v", err)
	}
	reader, readInfo, err := client.Get(ctx, key, first.Version)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(got) != firstData || readInfo != first {
		t.Fatalf("read did not preserve bytes or metadata: %+v, %v", readInfo, err)
	}
	var successes, conflicts atomic.Int32
	var contenders sync.WaitGroup
	for _, data := range []string{"generation: second-a\n", "generation: second-b\n"} {
		contenders.Add(1)
		go func(data string) {
			defer contenders.Done()
			_, e := client.Put(ctx, key, strings.NewReader(data), int64(len(data)), azureTestDigest(data), Condition{Version: first.Version})
			if e == nil {
				successes.Add(1)
			} else if errors.Is(e, ErrConflict) {
				conflicts.Add(1)
			} else {
				t.Errorf("unexpected conditional publication failure: %v", e)
			}
		}(data)
	}
	contenders.Wait()
	if successes.Load() != 1 || conflicts.Load() != 1 {
		t.Fatalf("CAS admitted wrong number of writers: successes=%d conflicts=%d", successes.Load(), conflicts.Load())
	}
	if _, err = client.Head(ctx, key, first.Version); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale read precondition was accepted: %v", err)
	}
}

func TestAzureRefusesInvalidRequestsBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	client, _ := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusBadRequest) })
	ctx := context.Background()
	for _, key := range []string{"tenant-a", "tenant-b/orders/file", "tenant-a/../other/file", "tenant-a/orders/file?comp=list", "tenant-a/orders/%2e%2e/file", "tenant-a/orders/*", "tenant-a//file"} {
		if _, err := client.Head(ctx, key, ""); err == nil {
			t.Errorf("accepted invalid key %q", key)
		}
	}
	for _, version := range []string{"*", "unquoted", `W/"weak"`, "\"value\r\nheader\""} {
		if _, err := client.Head(ctx, "tenant-a/orders/file", version); err == nil {
			t.Error("accepted invalid version")
		}
	}
	for _, condition := range []Condition{{}, {Absent: true, Version: `"v1"`}, {Version: "not-an-etag"}} {
		if _, err := client.Put(ctx, "tenant-a/orders/file", strings.NewReader("data"), 4, azureTestDigest("data"), condition); err == nil {
			t.Errorf("accepted invalid condition %+v", condition)
		}
	}
	for _, size := range []int64{-1, 3, MaxUploadBytes + 1} {
		if _, err := client.Put(ctx, "tenant-a/orders/file", strings.NewReader("data"), size, azureTestDigest("data"), Condition{Absent: true}); err == nil {
			t.Errorf("accepted invalid size %d", size)
		}
	}
	if _, err := client.Put(ctx, "tenant-a/orders/file", strings.NewReader("data"), 4, "not-a-digest", Condition{Absent: true}); err == nil {
		t.Error("accepted invalid digest")
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid requests made %d network calls", calls.Load())
	}
}

func TestAzureErrorResponsesPreserveServerTime(t *testing.T) {
	date, _ := http.ParseTime(azureTestDate)
	for _, status := range []int{http.StatusNotFound, http.StatusConflict, http.StatusPreconditionFailed} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			client, _ := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Date", azureTestDate)
				w.WriteHeader(status)
			})
			want := ErrConflict
			if status == http.StatusNotFound {
				want = ErrNotFound
			}
			ctx, key := context.Background(), "tenant-a/orders/manifest.yaml"
			_, got, err := client.Get(ctx, key, "")
			if !errors.Is(err, want) || !got.ServerTime.Equal(date) {
				t.Fatalf("GET lost provider clock: %+v, %v", got, err)
			}
			got, err = client.Head(ctx, key, "")
			if !errors.Is(err, want) || !got.ServerTime.Equal(date) {
				t.Fatalf("HEAD lost provider clock: %+v, %v", got, err)
			}
			got, err = client.Put(ctx, key, strings.NewReader("data"), 4, azureTestDigest("data"), Condition{Absent: true})
			if !errors.Is(err, want) || !got.ServerTime.Equal(date) {
				t.Fatalf("PUT lost provider clock: %+v, %v", got, err)
			}
		})
	}
}

func TestAzureSASFormsAndInvalidConfiguration(t *testing.T) {
	location := azureTestLocation("https://kelvofixture.blob.core.windows.net")
	credentials := catalog.ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_AZURE_FORMS_SAS"}
	service, _ := url.ParseQuery(azureTestSAS())
	account, _ := url.ParseQuery(service.Encode())
	account.Del("sr")
	account.Set("ss", "b")
	account.Set("srt", "co")
	delegated, _ := url.ParseQuery(service.Encode())
	for key, value := range map[string]string{"skoid": "11111111-1111-1111-1111-111111111111", "sktid": "22222222-2222-2222-2222-222222222222", "skt": "2026-10-01T00:00:00Z", "ske": "2026-10-07T00:00:00Z", "sks": "b", "skv": azureStorageVersion} {
		delegated.Set(key, value)
	}
	for _, token := range []string{service.Encode(), "?" + service.Encode(), account.Encode(), delegated.Encode()} {
		t.Setenv(credentials.SASTokenEnv, token)
		client, err := newAzure(location, credentials)
		if err != nil {
			t.Fatalf("ordinary SAS form was rejected: %v", err)
		}
		client.Close()
	}
	for _, token := range []string{"", "https://other.invalid/?" + azureTestSAS(), azureTestSAS() + "&sig=duplicate", azureTestSAS() + "&comp=list", azureTestSAS() + "&secret=unrecognized", azureTestSAS() + "\r\n"} {
		t.Setenv(credentials.SASTokenEnv, token)
		if _, err := newAzure(location, credentials); err == nil || strings.Contains(err.Error(), "fixture-only") || strings.Contains(err.Error(), "sig=") {
			t.Fatal("invalid SAS was accepted or exposed")
		}
	}
	t.Setenv(credentials.SASTokenEnv, azureTestSAS())
	for _, endpoint := range []string{"http://kelvofixture.blob.core.windows.net", "https://user:secret@example.invalid", "https://example.invalid/path", "https://example.invalid?sig=secret"} {
		location.Endpoint = endpoint
		if _, err := newAzure(location, credentials); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("invalid origin was accepted or exposed")
		}
	}
}

func TestAzureResponseMetadataAndVersionValidation(t *testing.T) {
	cases := map[string]func(http.ResponseWriter){
		"missing date":      func(w http.ResponseWriter) { w.Header()["Date"] = nil },
		"missing version":   func(w http.ResponseWriter) { w.Header().Del("ETag") },
		"weak version":      func(w http.ResponseWriter) { w.Header().Set("ETag", `W/"weak"`) },
		"duplicate version": func(w http.ResponseWriter) { w.Header().Add("ETag", `"other"`) },
		"invalid date":      func(w http.ResponseWriter) { w.Header().Set("Date", "not-a-date") },
		"duplicate date":    func(w http.ResponseWriter) { w.Header().Add("Date", azureTestDate) },
		"invalid digest":    func(w http.ResponseWriter) { w.Header().Set("x-ms-meta-kelvo-sha256", "invalid") },
		"duplicate digest":  func(w http.ResponseWriter) { w.Header().Add("x-ms-meta-kelvo-sha256", azureTestDigest("data")) },
		"oversized object":  func(w http.ResponseWriter) { w.Header().Set("Content-Length", strconv.FormatInt(MaxUploadBytes+1, 10)) },
		"encoded object":    func(w http.ResponseWriter) { w.Header().Set("Content-Encoding", "gzip") },
	}
	for name, alter := range cases {
		t.Run(name, func(t *testing.T) {
			client, _ := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				azureTestHeaders(w, "data", `"v1"`)
				alter(w)
				w.WriteHeader(http.StatusOK)
			})
			if _, err := client.Head(context.Background(), "tenant-a/orders/file", ""); err == nil {
				t.Fatal("invalid response metadata was accepted")
			}
		})
	}
	client, _ := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		azureTestHeaders(w, "data", `"unexpected"`)
		w.WriteHeader(http.StatusOK)
	})
	if _, err := client.Head(context.Background(), "tenant-a/orders/file", `"v1"`); !errors.Is(err, ErrConflict) {
		t.Fatalf("server ignoring If-Match was accepted: %v", err)
	}
	if body, info, err := client.Get(context.Background(), "tenant-a/orders/file", `"v1"`); !errors.Is(err, ErrConflict) || body != nil || info.ServerTime.IsZero() {
		t.Fatalf("GET conflict lost provider time or returned an unpinned body: %+v, %v", info, err)
	}
}

func TestAzureStreamingGetAndTruncatedBody(t *testing.T) {
	release := make(chan struct{})
	client, _ := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		azureTestHeaders(w, "data", `"v1"`)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "data")
		case <-r.Context().Done():
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	body, info, err := client.Get(ctx, "tenant-a/orders/file", `"v1"`)
	close(release)
	if err != nil || info.Size != 4 {
		t.Fatalf("Get did not return before the body became available: %v", err)
	}
	got, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil || !bytes.Equal(got, []byte("data")) {
		t.Fatalf("streaming body: %v", err)
	}
	truncated, _ := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		azureTestHeaders(w, "data", `"v1"`)
		_, _ = io.WriteString(w, "da")
	})
	body, _, err = truncated.Get(context.Background(), "tenant-a/orders/file", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(body)
	_ = body.Close()
	if err == nil || strings.Contains(err.Error(), "sig=") {
		t.Fatal("truncated body was accepted or exposed credentials")
	}
}

func TestAzurePinnedRangeReadsAndResponseValidation(t *testing.T) {
	const payload = "0123456789abcdefghijklmnopqrstuvwxyz"
	var calls atomic.Int32
	client, _ := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodGet || r.Header.Get("Range") != "bytes=10-15" || r.Header.Get("If-Match") != `"immutable"` {
			t.Error("range read lost its exact interval or version precondition")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		azureTestHeaders(w, payload, `"immutable"`)
		w.Header().Set("Content-Length", "6")
		w.Header().Set("Content-Range", "bytes 10-15/36")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, payload[10:16])
	})
	ctx := context.Background()
	body, info, err := client.GetRange(ctx, "tenant-a/orders/file", `"immutable"`, 10, 6)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil || string(data) != "abcdef" || info.Size != 36 || info.Version != `"immutable"` || info.SHA256 != azureTestDigest(payload) {
		t.Fatalf("range changed data or full-object identity: %q, %+v, %v", data, info, err)
	}
	for _, interval := range [][2]int64{{-1, 1}, {0, 0}, {0, -1}, {MaxUploadBytes, 1}, {MaxUploadBytes - 1, 2}, {0, MaxUploadBytes + 1}} {
		if _, _, err := client.GetRange(ctx, "tenant-a/orders/file", `"immutable"`, interval[0], interval[1]); err == nil {
			t.Error("invalid range was accepted")
		}
	}
	if _, _, err := client.GetRange(ctx, "tenant-a/orders/file", "", 0, 1); err == nil {
		t.Error("unpinned range was accepted")
	}
	if calls.Load() != 1 {
		t.Fatal("invalid range reached the provider")
	}
	cases := map[string]func(http.ResponseWriter) int{
		"ignored range": func(w http.ResponseWriter) int { return http.StatusOK },
		"missing range": func(w http.ResponseWriter) int { w.Header().Del("Content-Range"); return http.StatusPartialContent },
		"wrong start": func(w http.ResponseWriter) int {
			w.Header().Set("Content-Range", "bytes 9-14/36")
			return http.StatusPartialContent
		},
		"wrong end": func(w http.ResponseWriter) int {
			w.Header().Set("Content-Range", "bytes 10-16/36")
			return http.StatusPartialContent
		},
		"unknown total": func(w http.ResponseWriter) int {
			w.Header().Set("Content-Range", "bytes 10-15/*")
			return http.StatusPartialContent
		},
		"impossible total": func(w http.ResponseWriter) int {
			w.Header().Set("Content-Range", "bytes 10-15/15")
			return http.StatusPartialContent
		},
		"duplicate range": func(w http.ResponseWriter) int {
			w.Header().Add("Content-Range", "bytes 10-15/37")
			return http.StatusPartialContent
		},
		"oversized response": func(w http.ResponseWriter) int {
			w.Header().Set("Content-Length", "7")
			return http.StatusPartialContent
		},
		"version changed": func(w http.ResponseWriter) int { w.Header().Set("ETag", `"changed"`); return http.StatusPartialContent },
		"encoding changed": func(w http.ResponseWriter) int {
			w.Header().Set("Content-Encoding", "gzip")
			return http.StatusPartialContent
		},
	}
	for name, alter := range cases {
		t.Run(name, func(t *testing.T) {
			invalid, _ := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				azureTestHeaders(w, "abcdef", `"immutable"`)
				w.Header().Set("Content-Range", "bytes 10-15/36")
				w.WriteHeader(alter(w))
			})
			if body, _, err := invalid.GetRange(ctx, "tenant-a/orders/file", `"immutable"`, 10, 6); err == nil || body != nil {
				t.Fatal("invalid partial response was accepted")
			}
		})
	}
	truncated, _ := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		azureTestHeaders(w, "abcdef", `"immutable"`)
		w.Header().Set("Content-Range", "bytes 10-15/36")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "abc")
	})
	body, _, err = truncated.GetRange(ctx, "tenant-a/orders/file", `"immutable"`, 10, 6)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(body)
	_ = body.Close()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated range was accepted: %v", err)
	}
}

type azureTestRoundTripper func(*http.Request) (*http.Response, error)

func (f azureTestRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestAzureRangeRejectsCustomTransportOverrun(t *testing.T) {
	client, _ := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) { t.Fatal("custom transport reached server") })
	client.http.Transport = azureTestRoundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusPartialContent, ContentLength: 6,
			Header: http.Header{"Content-Length": {"6"}, "Content-Range": {"bytes 10-15/36"}, "Etag": {`"immutable"`}, "Date": {azureTestDate}},
			Body:   io.NopCloser(strings.NewReader("abcdefunadvertised"))}, nil
	})
	body, _, err := client.GetRange(context.Background(), "tenant-a/orders/file", `"immutable"`, 10, 6)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	_ = body.Close()
	if err == nil || string(data) != "abcdef" {
		t.Fatalf("oversized body escaped the exact range: %q, %v", data, err)
	}
}

func TestAzureTLSRedirectCancellationAndErrorRedaction(t *testing.T) {
	var destinationCalls atomic.Int32
	destination := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { destinationCalls.Add(1) }))
	defer destination.Close()
	client, server := azureTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/redirect") {
			w.Header().Set("Location", destination.URL)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/wait") {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, r.URL.RawQuery)
	})
	if transport := client.http.Transport.(*http.Transport); transport.Proxy != nil || transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("transport permits ambient proxies or unverified TLS")
	}
	for _, key := range []string{"tenant-a/orders/redirect", "tenant-a/orders/denied"} {
		_, err := client.Head(context.Background(), key, "")
		if err == nil || strings.Contains(err.Error(), "sig=") || strings.Contains(err.Error(), "signature") || strings.Contains(err.Error(), server.URL) {
			t.Fatal("remote error was accepted or disclosed request credentials")
		}
		if body, _, err := client.GetRange(context.Background(), key, `"v1"`, 0, 1); err == nil || body != nil || strings.Contains(err.Error(), "sig=") {
			t.Fatal("range followed a redirect, returned a body, or disclosed its SAS")
		}
	}
	if destinationCalls.Load() != 0 {
		t.Fatal("redirected request reached another origin")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := client.Head(ctx, "tenant-a/orders/wait", ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation did not preserve the context error: %v", err)
	}
	untrusted, err := newAzure(azureTestLocation(server.URL), catalog.ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_AZURE_TEST_SAS"})
	if err != nil {
		t.Fatal(err)
	}
	defer untrusted.Close()
	if _, err := untrusted.Head(context.Background(), "tenant-a/orders/denied", ""); err == nil || strings.Contains(err.Error(), "sig=") {
		t.Fatal("untrusted TLS certificate was accepted or exposed the request URL")
	}
}
