// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package awsapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

func source(t *testing.T, url string) catalog.Source {
	t.Helper()
	t.Setenv("KELVO_SOURCE_AWS_TEST_URL", url)
	t.Setenv("KELVO_SOURCE_AWS_TEST_ID", "explicit-access-id")
	t.Setenv("KELVO_SOURCE_AWS_TEST_SECRET", "explicit-secret")
	t.Setenv("KELVO_SOURCE_AWS_TEST_TOKEN", "session-token")
	return catalog.Source{URLEnv: "KELVO_SOURCE_AWS_TEST_URL", UsernameEnv: "KELVO_SOURCE_AWS_TEST_ID", PasswordEnv: "KELVO_SOURCE_AWS_TEST_SECRET", TokenEnv: "KELVO_SOURCE_AWS_TEST_TOKEN", Options: map[string]string{"region": "ap-south-1"}}
}
func TestExplicitCredentialsSigV4AndScope(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "ambient-forbidden")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
	t.Setenv("AWS_SESSION_TOKEN", "ambient-token")
	for _, service := range []string{"athena", "dynamodb"} {
		t.Run(service, func(t *testing.T) {
			var client *Client
			target := "AmazonAthena.GetQueryResults"
			contentType := "application/x-amz-json-1.1"
			if service == "dynamodb" {
				target = "DynamoDB_20120810.ExecuteStatement"
				contentType = "application/x-amz-json-1.0"
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				auth := r.Header.Get("Authorization")
				if r.Method != "POST" || r.URL.Path != "/" || r.URL.RawQuery != "" || r.Header.Get("X-Amz-Target") != target || r.Header.Get("Content-Type") != contentType || r.Header.Get("X-Amz-Security-Token") != "session-token" || !strings.Contains(auth, "/ap-south-1/"+service+"/aws4_request") || !strings.Contains(auth, "Credential=explicit-access-id/") || strings.Contains(string(raw), "explicit-secret") {
					t.Error("request scope, protocol, or credentials are incorrect")
				}
				stamp, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
				if err != nil {
					t.Error(err)
				}
				signed, _ := http.NewRequest(http.MethodPost, "https://"+r.Host+r.URL.RequestURI(), bytes.NewReader(raw))
				signedHeaders := strings.Split(strings.Split(auth, "SignedHeaders=")[1], ",")[0]
				for _, key := range strings.Split(signedHeaders, ";") {
					if key != "host" {
						signed.Header[http.CanonicalHeaderKey(key)] = r.Header.Values(key)
					}
				}
				if err = v4.NewSigner().SignHTTP(context.Background(), client.Credentials, signed, payloadHash(raw), service, "ap-south-1", stamp); err != nil || signed.Header.Get("Authorization") != auth {
					t.Error("signature does not verify")
				}
				fmt.Fprint(w, `{"ok":true}`)
			}))
			defer server.Close()
			var err error
			client, err = New(source(t, server.URL), query.DefaultLimits(), service)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client.HTTP.Transport = server.Client().Transport
			var out map[string]any
			if _, err = client.Do(context.Background(), target, map[string]string{"Statement": "SELECT * FROM example"}, &out); err != nil || out["ok"] != true {
				t.Fatalf("out=%v err=%v", out, err)
			}
		})
	}
}
func TestConfigurationAndAmbientCredentials(t *testing.T) {
	for _, url := range []string{"", "http://example.com", "https://user:password@example.com", "https://example.com/path", "https://example.com/?q=secret", "https://example.com/?", "https://example.com/#fragment", "https:opaque", "https://example.com/%2f"} {
		t.Run(url, func(t *testing.T) {
			if _, err := New(source(t, url), query.DefaultLimits(), "athena"); err == nil {
				t.Fatal("accepted invalid origin")
			}
		})
	}
	s := source(t, "https://athena.ap-south-1.amazonaws.com")
	s.TokenEnv = ""
	c, err := New(s, query.DefaultLimits(), "athena")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tr := c.HTTP.Transport.(*http.Transport)
	if tr.Proxy != nil || tr.TLSClientConfig.InsecureSkipVerify || tr.TLSClientConfig.MinVersion != tls.VersionTLS12 || !tr.DisableCompression {
		t.Fatal("unsafe transport defaults")
	}
	if c.Credentials.SessionToken != "" {
		t.Fatal("ambient token was used")
	}
	s.TokenEnv = "KELVO_SOURCE_AWS_MISSING_TEST_TOKEN"
	t.Setenv(s.TokenEnv, "")
	if _, err = New(s, query.DefaultLimits(), "athena"); err == nil {
		t.Fatal("missing configured token was ignored")
	}
	s.TokenEnv = ""
	t.Setenv("KELVO_SOURCE_AWS_TEST_SECRET", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
	if _, err = New(s, query.DefaultLimits(), "athena"); err == nil {
		t.Fatal("ambient secret was used")
	}
	t.Setenv("KELVO_SOURCE_AWS_TEST_SECRET", "explicit-secret")
	s.Options["region"] = "bad/region"
	if _, err = New(s, query.DefaultLimits(), "athena"); err == nil {
		t.Fatal("invalid signing region")
	}
}
func TestRedirectAndOperationIsolation(t *testing.T) {
	var escaped atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { escaped.Add(1) }))
	defer target.Close()
	for _, status := range []int{301, 302, 307, 308} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", target.URL+"/capture")
				w.WriteHeader(status)
			}))
			defer server.Close()
			c, err := New(source(t, server.URL), query.DefaultLimits(), "athena")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.HTTP.Transport = server.Client().Transport
			if _, err = c.Do(context.Background(), "AmazonAthena.StartQueryExecution", map[string]string{}, &struct{}{}); err == nil {
				t.Fatal("redirect accepted")
			}
			if _, err = c.Do(context.Background(), "DynamoDB_20120810.ExecuteStatement", map[string]string{}, &struct{}{}); err == nil {
				t.Fatal("cross-service operation accepted")
			}
			if calls.Load() != 1 {
				t.Fatal("execution replayed or invalid operation was sent")
			}
		})
	}
	if escaped.Load() != 0 {
		t.Fatal("credentials escaped configured origin")
	}
}
func TestResponsesBoundedAndSanitized(t *testing.T) {
	for _, test := range []struct {
		name, body, encoding string
		status               int
		code                 string
	}{
		{"oversize", `{"data":"` + strings.Repeat("x", 2048) + `"}`, "", 200, "RESOURCE_EXHAUSTED"},
		{"trailing", `{} {}`, "", 200, "QUERY_FAILED"}, {"null", `null`, "", 200, "QUERY_FAILED"}, {"malformed", `{`, "", 200, "QUERY_FAILED"}, {"encoded", `{}`, "gzip", 200, "QUERY_FAILED"}, {"provider secret", `explicit-secret session-token`, "", 403, "QUERY_FAILED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Encoding", test.encoding)
				w.WriteHeader(test.status)
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			l := query.DefaultLimits()
			l.MaxBytes = 1024
			c, err := New(source(t, server.URL), l, "athena")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.HTTP.Transport = server.Client().Transport
			_, err = c.Do(context.Background(), "AmazonAthena.GetQueryExecution", map[string]string{}, &struct{}{})
			if err == nil || query.PublicError(err).Code != test.code || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "session-token") {
				t.Fatalf("unsafe error: %v", err)
			}
		})
	}
}
func TestEmptyStopBodyAndCancellation(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Amz-Target") == "AmazonAthena.StopQueryExecution" {
			w.WriteHeader(200)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer server.Close()
	c, err := New(source(t, server.URL), query.DefaultLimits(), "athena")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.HTTP.Transport = server.Client().Transport
	if _, err = c.Do(context.Background(), "AmazonAthena.StopQueryExecution", map[string]string{}, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = c.Do(ctx, "AmazonAthena.GetQueryExecution", map[string]string{}, &struct{}{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline lost: %v", err)
	}
	var stats query.Stats
	l := query.DefaultLimits()
	l.MaxBytes = 1024
	if Account(&stats, 512, l) != nil || Account(&stats, 513, l) == nil {
		t.Fatal("aggregate response cap not enforced")
	}
}

func TestRejectsUnsafeEnvironmentAndConflictingSettings(t *testing.T) {
	for _, test := range []struct {
		name, service string
		change        func(*catalog.Source)
	}{
		{"runtime environment", "athena", func(s *catalog.Source) { s.URLEnv = "PATH" }},
		{"ambient credential", "athena", func(s *catalog.Source) { s.UsernameEnv = "AWS_ACCESS_KEY_ID" }},
		{"ambient session", "athena", func(s *catalog.Source) { s.TokenEnv = "AWS_SESSION_TOKEN" }},
		{"dsn", "athena", func(s *catalog.Source) { s.DSNEnv = "KELVO_SOURCE_OTHER_DSN" }},
		{"path", "dynamodb", func(s *catalog.Source) { s.Path = "/private/data" }},
		{"adapter", "athena", func(s *catalog.Source) { s.Adapter = "dbapi" }},
		{"option typo", "athena", func(s *catalog.Source) { s.Options["work_group"] = "analytics" }},
		{"cross-service option", "dynamodb", func(s *catalog.Source) { s.Options["workgroup"] = "analytics" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := source(t, "https://example.com")
			test.change(&s)
			if _, err := New(s, query.DefaultLimits(), test.service); err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" {
				t.Fatalf("configuration accepted: %v", err)
			}
		})
	}
}
func TestInvalidUTF8RejectedBeforeDecoding(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte{'{', '"', 'v', '"', ':', '"', 0xff, '"', '}'})
	}))
	defer server.Close()
	c, err := New(source(t, server.URL), query.DefaultLimits(), "athena")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.HTTP.Transport = server.Client().Transport
	var out map[string]string
	if _, err = c.Do(context.Background(), "AmazonAthena.GetQueryResults", map[string]string{}, &out); err == nil {
		t.Fatal("invalid UTF-8 was silently replaced")
	}
}
