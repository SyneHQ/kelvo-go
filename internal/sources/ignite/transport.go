// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package ignite

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type client struct {
	http               *http.Client
	endpoint           string
	username, password string
	responseLimit      int64
}

type response struct {
	Status   *int            `json:"successStatus"`
	Error    json.RawMessage `json:"error"`
	Response json.RawMessage `json:"response"`
}

func validText(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}

func newClient(source catalog.Source, limits query.Limits) (*client, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	for _, name := range []string{source.URLEnv, source.UsernameEnv, source.PasswordEnv} {
		if name == "" || catalog.ValidateEnvironment(name) != nil {
			return nil, query.NewError("CONFIGURATION_ERROR", "Ignite requires explicit URL, username and password environment references")
		}
	}
	origin, err := url.Parse(os.Getenv(source.URLEnv))
	if err != nil || origin.Scheme != "https" || origin.Hostname() == "" || origin.User != nil || origin.Opaque != "" || (origin.Path != "" && origin.Path != "/") || origin.RawPath != "" || origin.RawQuery != "" || origin.ForceQuery || origin.Fragment != "" {
		return nil, query.NewError("CONFIGURATION_ERROR", "Ignite requires a verified HTTPS origin")
	}
	username, password := os.Getenv(source.UsernameEnv), os.Getenv(source.PasswordEnv)
	if username == "" || password == "" || len(username) > 16<<10 || len(password) > 16<<10 || !validText(username) || !validText(password) {
		return nil, query.NewError("CONFIGURATION_ERROR", "Ignite credentials are unavailable")
	}
	origin.Path = "/ignite"
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:    5 * time.Second,
		ResponseHeaderTimeout:  limits.Timeout,
		IdleConnTimeout:        30 * time.Second,
		MaxConnsPerHost:        2,
		MaxIdleConnsPerHost:    2,
		MaxResponseHeaderBytes: 64 << 10,
		DisableCompression:     true,
	}
	return &client{http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, endpoint: origin.String(), username: username, password: password, responseLimit: min(32<<20, int64(limits.MemoryMB)<<18)}, nil
}

// post uses the documented form parameter names. Credentials and SQL never
// appear in the URL, redirects, proxy configuration, errors, or logs.
func (c *client) post(ctx context.Context, form url.Values, limit int64) (response, int64, error) {
	var result response
	input := make(url.Values, len(form)+2)
	for key, values := range form {
		input[key] = append([]string(nil), values...)
	}
	input.Set("ignite.login", c.username)
	input.Set("ignite.password", c.password)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, strings.NewReader(input.Encode()))
	if err != nil {
		return result, 0, query.NewError("QUERY_FAILED", "Cannot prepare Ignite request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return result, 0, ctx.Err()
		}
		return result, 0, query.NewError("QUERY_FAILED", "Ignite request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return result, 0, query.NewError("QUERY_FAILED", "Ignite rejected the request")
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return result, 0, query.NewError("QUERY_FAILED", "Ignite returned an unsupported response encoding")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	n := int64(len(body))
	if err != nil {
		if ctx.Err() != nil {
			return result, n, ctx.Err()
		}
		return result, n, query.NewError("QUERY_FAILED", "Ignite returned an incomplete response")
	}
	if n > limit {
		return result, n, query.NewError("RESOURCE_EXHAUSTED", "Ignite response exceeds its budget")
	}
	if err := decode(body, &result); err != nil {
		return result, n, query.NewError("QUERY_FAILED", "Ignite returned invalid JSON")
	}
	return result, n, nil
}

func decode(body []byte, out any) error {
	if !utf8.Valid(body) {
		return query.NewError("QUERY_FAILED", "Ignite returned invalid UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	if err := d.Decode(out); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return query.NewError("QUERY_FAILED", "Ignite returned trailing JSON")
	}
	return nil
}
