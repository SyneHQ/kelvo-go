// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

type s3IdentityTransport func(*http.Request) (*http.Response, error)

func (f s3IdentityTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type s3IdentityBody struct {
	io.Reader
	reads, closes int
}

func (b *s3IdentityBody) Read(buffer []byte) (int, error) {
	b.reads++
	return b.Reader.Read(buffer)
}

func (b *s3IdentityBody) Close() error { b.closes++; return nil }

const s3IdentityDate = "Sat, 03 Oct 2026 12:00:00 GMT"

func s3IdentityFixture(t *testing.T, provider, operation string, alter func(*http.Response)) (*s3Client, *s3IdentityBody, string) {
	t.Helper()
	t.Setenv("KELVO_SOURCE_IDENTITY_ID", "fixture-id")
	t.Setenv("KELVO_SOURCE_IDENTITY_SECRET", "fixture-not-a-credential")
	location := catalog.ObjectLocation{Provider: provider, Endpoint: "https://objects.example.invalid", Bucket: "snapshots", Prefix: "kelvo"}
	if provider == "s3" {
		location.Region = "us-east-1"
	}
	client, err := New(location, catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_IDENTITY_ID", SecretAccessKeyEnv: "KELVO_SOURCE_IDENTITY_SECRET"})
	if err != nil {
		t.Fatal(err)
	}
	c := client.(*s3Client)
	t.Cleanup(c.Close)
	version := `"opaque-version-not-a-content-hash"`
	identity, checksum := "ETag", "x-amz-meta-kelvo-sha256"
	if provider == "gcs" {
		version, identity, checksum = "123", "x-goog-generation", "x-goog-meta-kelvo-sha256"
	}
	payload := "0123456789"
	status, length := http.StatusOK, int64(len(payload))
	if operation == "range" {
		payload, status, length = "2345", http.StatusPartialContent, 4
	}
	if operation == "head" {
		payload = ""
	}
	body := &s3IdentityBody{Reader: strings.NewReader(payload)}
	headers := http.Header{}
	headers.Set("Date", s3IdentityDate)
	headers.Set("Content-Length", strconv.FormatInt(length, 10))
	headers.Set("Content-Encoding", "identity")
	headers.Set(identity, version)
	headers.Set(checksum, strings.Repeat("a", 64))
	if operation == "range" {
		headers.Set("Content-Range", "bytes 2-5/10")
	}
	c.http.Transport = s3IdentityTransport(func(request *http.Request) (*http.Response, error) {
		condition := request.Header.Get("If-Match")
		if provider == "gcs" {
			condition = request.Header.Get("x-goog-if-generation-match")
		}
		method := http.MethodGet
		if operation == "head" {
			method = http.MethodHead
		}
		if condition != version || request.Method != method || request.URL.Path != "/snapshots/kelvo/data" ||
			(operation == "range" && request.Header.Get("Range") != "bytes=2-5") {
			t.Error("request lost provider method, exact key or immutable condition")
		}
		response := &http.Response{StatusCode: status, Header: headers.Clone(), ContentLength: length, Body: body, Request: request}
		if alter != nil {
			alter(response)
		}
		return response, nil
	})
	return c, body, version
}

func s3IdentityRead(c *s3Client, operation, version string) (io.ReadCloser, Info, error) {
	ctx := context.Background()
	switch operation {
	case "head":
		info, err := c.Head(ctx, "kelvo/data", version)
		return nil, info, err
	case "range":
		return c.GetRange(ctx, "kelvo/data", version, 2, 4)
	default:
		return c.Get(ctx, "kelvo/data", version)
	}
}

func TestS3ResponseIdentityRejectsDuplicateHeaders(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs"} {
		for _, operation := range []string{"get", "head", "range"} {
			fields := []string{"Date", "Content-Length", "Content-Encoding", "ETag", "x-amz-meta-kelvo-sha256"}
			if provider == "gcs" {
				fields[3], fields[4] = "x-goog-generation", "x-goog-meta-kelvo-sha256"
			}
			if operation == "range" {
				fields = append(fields, "Content-Range")
			}
			for _, field := range fields {
				for _, style := range []string{"identical-values", "conflicting-values", "identical-case-keys", "conflicting-case-keys", "empty-values"} {
					t.Run(provider+"/"+operation+"/"+field+"/"+style, func(t *testing.T) {
						c, observed, version := s3IdentityFixture(t, provider, operation, func(response *http.Response) {
							value := response.Header.Get(field)
							other := value
							if strings.HasPrefix(style, "conflicting") {
								other = "private-response-value"
								if field == "ETag" {
									other = `"private-response-value"`
								}
							}
							if style == "empty-values" {
								response.Header[http.CanonicalHeaderKey(field)] = nil
							} else if strings.HasSuffix(style, "case-keys") {
								response.Header[strings.ToUpper(field)] = []string{other}
							} else {
								response.Header[http.CanonicalHeaderKey(field)] = []string{value, other}
							}
						})
						body, info, err := s3IdentityRead(c, operation, version)
						if body != nil {
							_ = body.Close()
						}
						if err == nil || body != nil || info != (Info{}) || observed.reads != 0 || observed.closes != 1 {
							t.Fatalf("ambiguous response was trusted or body ownership leaked: info=%+v reads=%d closes=%d err=%v", info, observed.reads, observed.closes, err)
						}
						if strings.Contains(err.Error(), "private-response-value") || strings.Contains(err.Error(), "objects.example.invalid") {
							t.Fatal("private header or endpoint escaped in response error")
						}
					})
				}
			}
		}
	}
}

func TestS3ResponseIdentityPreservesValidProviderSemantics(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs"} {
		for _, operation := range []string{"get", "head", "range"} {
			for _, mode := range []string{"canonical", "lowercase", "uppercase", "optional-checksum-absent"} {
				t.Run(provider+"/"+operation+"/"+mode, func(t *testing.T) {
					c, observed, version := s3IdentityFixture(t, provider, operation, func(response *http.Response) {
						if mode == "optional-checksum-absent" {
							response.Header.Del("x-amz-meta-kelvo-sha256")
							response.Header.Del("x-goog-meta-kelvo-sha256")
						} else if mode != "canonical" {
							convert := strings.ToLower
							if mode == "uppercase" {
								convert = strings.ToUpper
							}
							converted := http.Header{}
							for key, values := range response.Header {
								converted[convert(key)] = values
							}
							response.Header = converted
						}
						response.Header["Set-Cookie"] = []string{"first=one", "second=two"}
					})
					body, info, err := s3IdentityRead(c, operation, version)
					if err != nil {
						t.Fatal(err)
					}
					date, _ := time.Parse(http.TimeFormat, s3IdentityDate)
					checksum := strings.Repeat("a", 64)
					if mode == "optional-checksum-absent" {
						checksum = ""
					}
					if info.Version != version || info.Size != 10 || info.SHA256 != checksum || !info.ServerTime.Equal(date) {
						t.Fatalf("valid provider metadata changed: %+v", info)
					}
					if operation != "head" {
						data, readErr := io.ReadAll(body)
						_ = body.Close()
						expected := "0123456789"
						if operation == "range" {
							expected = "2345"
						}
						if readErr != nil || string(data) != expected {
							t.Fatal("valid payload changed", readErr)
						}
					}
					if observed.closes != 1 {
						t.Fatal("valid response body was not released exactly once")
					}
				})
			}
		}
	}
}

func TestS3ResponseIdentityRejectsMismatchedLength(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs"} {
		for _, operation := range []string{"get", "head", "range"} {
			for _, length := range []string{"", "-1", "+10", "9", "9223372036854775808"} {
				t.Run(provider+"/"+operation+"/length="+length, func(t *testing.T) {
					c, observed, version := s3IdentityFixture(t, provider, operation, func(response *http.Response) {
						response.Header.Set("Content-Length", length)
					})
					body, _, err := s3IdentityRead(c, operation, version)
					if body != nil {
						_ = body.Close()
					}
					if err == nil || body != nil || observed.reads != 0 || observed.closes != 1 {
						t.Fatal("inconsistent response length was trusted", err)
					}
				})
			}
		}
	}
}

func TestS3ResponseIdentityPreservesConditionalErrorClock(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs"} {
		for _, status := range []int{http.StatusNotFound, http.StatusPreconditionFailed, http.StatusConflict} {
			for _, duplicate := range []bool{false, true} {
				t.Run(provider+"/"+strconv.Itoa(status)+"/duplicate="+strconv.FormatBool(duplicate), func(t *testing.T) {
					c, observed, version := s3IdentityFixture(t, provider, "head", func(response *http.Response) {
						response.StatusCode = status
						response.Header = http.Header{"Date": {s3IdentityDate}}
						if duplicate {
							response.Header["DATE"] = []string{s3IdentityDate}
						}
					})
					info, err := c.Head(context.Background(), "kelvo/data", version)
					if duplicate {
						if err == nil || !info.ServerTime.IsZero() {
							t.Fatal("ambiguous provider clock was trusted on an error response")
						}
					} else {
						expected := ErrConflict
						if status == http.StatusNotFound {
							expected = ErrNotFound
						}
						date, _ := time.Parse(http.TimeFormat, s3IdentityDate)
						if !errors.Is(err, expected) || !info.ServerTime.Equal(date) {
							t.Fatal("conditional status or provider clock changed", info, err)
						}
					}
					if observed.reads != 0 || observed.closes != 1 {
						t.Fatal("conditional error body was not closed without reading")
					}
				})
			}
		}
	}
}
