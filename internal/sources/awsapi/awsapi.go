// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package awsapi provides bounded AWS JSON requests using explicit credentials.
package awsapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type Client struct {
	Origin          *url.URL
	HTTP            *http.Client
	Credentials     aws.Credentials
	Region, Service string
	Limit           int64
	signer          *v4.Signer
}

var regionName = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)+-[0-9]+$`)

func New(s catalog.Source, l query.Limits, service string) (*Client, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	if service != "athena" && service != "dynamodb" {
		return nil, query.NewError("CONFIGURATION_ERROR", "Unsupported AWS service")
	}
	if s.DSNEnv != "" || s.Path != "" || s.Adapter != "" {
		return nil, query.NewError("CONFIGURATION_ERROR", "AWS source has conflicting connection settings")
	}
	for _, name := range []string{s.URLEnv, s.UsernameEnv, s.PasswordEnv} {
		if len(name) > 256 || catalog.ValidateEnvironment(name) != nil {
			return nil, query.NewError("CONFIGURATION_ERROR", "AWS source requires dedicated source environment references")
		}
	}
	if s.TokenEnv != "" && (len(s.TokenEnv) > 256 || catalog.ValidateEnvironment(s.TokenEnv) != nil) {
		return nil, query.NewError("CONFIGURATION_ERROR", "AWS source requires a dedicated session-token environment reference")
	}
	for key, value := range s.Options {
		allowed := key == "region" || (service == "athena" && (key == "workgroup" || key == "database" || key == "output_location"))
		if !allowed || len(value) > 4096 {
			return nil, query.NewError("CONFIGURATION_ERROR", "AWS source has an unsupported option")
		}
	}
	u, err := url.Parse(os.Getenv(s.URLEnv))
	if s.URLEnv == "" || err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return nil, query.NewError("CONFIGURATION_ERROR", "AWS source requires an HTTPS origin")
	}
	region := s.Options["region"]
	if len(region) > 64 || !regionName.MatchString(region) {
		return nil, query.NewError("CONFIGURATION_ERROR", "AWS source requires a valid region")
	}
	id, secret := os.Getenv(s.UsernameEnv), os.Getenv(s.PasswordEnv)
	token := os.Getenv(s.TokenEnv)
	if s.UsernameEnv == "" || s.PasswordEnv == "" || id == "" || secret == "" || (s.TokenEnv != "" && token == "") || len(id) > 1024 || len(secret) > 4096 || len(token) > 16384 || strings.ContainsAny(id+secret+token, "\r\n\x00") {
		return nil, query.NewError("CONFIGURATION_ERROR", "AWS source credentials are unavailable")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.DisableCompression = true
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	tr.TLSHandshakeTimeout = 5 * time.Second
	tr.ResponseHeaderTimeout = l.Timeout
	tr.MaxConnsPerHost = 2
	tr.MaxResponseHeaderBytes = 64 << 10
	return &Client{Origin: u, HTTP: &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, Credentials: aws.Credentials{AccessKeyID: id, SecretAccessKey: secret, SessionToken: token}, Region: region, Service: service, Limit: min(32<<20, l.MaxBytes, int64(l.MemoryMB)<<18), signer: v4.NewSigner()}, nil
}
func (c *Client) Close() { c.HTTP.CloseIdleConnections() }

// Do makes one request to the configured origin. It never retries execution,
// follows redirects, loads ambient credentials, or accepts response URLs.
// A nil out permits the empty body returned by StopQueryExecution.
func (c *Client) Do(ctx context.Context, target string, body any, out any) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !c.allowedTarget(target) {
		return 0, query.NewError("PERMISSION_DENIED", "AWS operation is unavailable")
	}
	b, err := json.Marshal(body)
	if err != nil || len(b) > 256<<10 {
		return 0, query.NewError("INVALID_ARGUMENT", "Invalid AWS request")
	}
	u := *c.Origin
	u.Path = "/"
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(b))
	if err != nil {
		return 0, query.NewError("QUERY_FAILED", "Cannot prepare AWS request")
	}
	contentType := "application/x-amz-json-1.0"
	if c.Service == "athena" {
		contentType = "application/x-amz-json-1.1"
	}
	r.Header.Set("Content-Type", contentType)
	r.Header.Set("X-Amz-Target", target)
	if err = c.signer.SignHTTP(ctx, c.Credentials, r, payloadHash(b), c.Service, c.Region, time.Now()); err != nil {
		return 0, query.NewError("QUERY_FAILED", "Cannot sign AWS request")
	}
	response, err := c.HTTP.Do(r)
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, query.NewError("QUERY_FAILED", "AWS request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return 0, query.NewError("QUERY_FAILED", "AWS source rejected request")
	}
	if enc := response.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		return 0, query.NewError("QUERY_FAILED", "Unsupported AWS response encoding")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, c.Limit+1))
	n := int64(len(raw))
	if err != nil {
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
		return n, query.NewError("QUERY_FAILED", "AWS source returned an incomplete response")
	}
	if n > c.Limit {
		return n, query.NewError("RESOURCE_EXHAUSTED", "AWS response exceeds memory budget")
	}
	if out == nil {
		return n, nil
	}
	// AWS JSON responses are objects; null is not an empty successful response.
	if trimmed := bytes.TrimSpace(raw); !utf8.Valid(raw) || len(trimmed) == 0 || trimmed[0] != '{' {
		return n, query.NewError("QUERY_FAILED", "AWS source returned invalid JSON")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return n, query.NewError("QUERY_FAILED", "AWS source returned invalid JSON")
	}
	return n, nil
}
func (c *Client) allowedTarget(target string) bool {
	if c.Service == "dynamodb" {
		return target == "DynamoDB_20120810.ExecuteStatement"
	}
	if c.Service == "athena" {
		switch target {
		case "AmazonAthena.StartQueryExecution", "AmazonAthena.GetQueryExecution", "AmazonAthena.GetQueryResults", "AmazonAthena.StopQueryExecution":
			return true
		}
	}
	return false
}
func payloadHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// Account bounds aggregate network work, including empty pages and polling.
func Account(stats *query.Stats, n int64, limits query.Limits) error {
	stats.WireBytes += n
	if stats.WireBytes > limits.MaxBytes {
		return query.NewError("RESOURCE_EXHAUSTED", "AWS query exceeds response byte limit")
	}
	return nil
}
