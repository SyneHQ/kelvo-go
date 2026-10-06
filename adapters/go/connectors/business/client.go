// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package business executes bounded requests against fixed provider origins.
package business

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
)

const maxResponse = 1 << 20

type Config struct{ Type, Environment, Token, ClientID, Endpoint, Project string }
type Client struct {
	config     Config
	endpoint   string
	http       *http.Client
	dispatched bool
	confirmed  bool
}
type Query = provider.Query

func Supported(kind string) bool { return provider.Supported(kind) }
func New(config Config) (*Client, error) {
	config.Type = strings.ToLower(config.Type)
	if strings.TrimSpace(config.Token) == "" {
		return nil, errors.New("access token or API key is required")
	}
	if config.Environment != "" && config.Environment != "production" && config.Environment != "sandbox" {
		return nil, errors.New("environment must be production or sandbox")
	}
	var endpoint string
	switch config.Type {
	case "posthog":
		if !provider.ValidPostHogOrigin(config.Endpoint) || !provider.ValidProject(config.Project) || config.Environment != "" || config.ClientID != "" {
			return nil, errors.New("invalid saved PostHog source")
		}
		endpoint = strings.TrimSuffix(config.Endpoint, "/")
	case "salesforce_data360":
		endpoint = "https://api.salesforce.com/platform/mcp/v1/data/data360"
		if config.Environment == "sandbox" {
			endpoint = "https://api.salesforce.com/platform/mcp/v1/data/sandbox/data360"
		}
	case "salesforce_tableau_next":
		endpoint = "https://api.salesforce.com/platform/mcp/v1/analytics/tableau-next"
		if config.Environment == "sandbox" {
			endpoint = "https://api.salesforce.com/platform/mcp/v1/sandbox/analytics/tableau-next"
		}
	case "daloopa":
		endpoint = "https://mcp.daloopa.com/server/mcp"
	case "motherduck":
		endpoint = "https://api.motherduck.com/mcp"
	case "ramp":
		endpoint = "https://api.ramp.com/developer/v1"
		if config.Environment == "sandbox" {
			endpoint = "https://demo-api.ramp.com/developer/v1"
		}
	default:
		return nil, errors.New("unsupported business connector")
	}
	// URLs are fixed, never caller-controlled. Do not forward credentials on redirects.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost, transport.MaxIdleConns = 1, 1, 1
	return &Client{config: config, endpoint: endpoint, http: &http.Client{Transport: transport, Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func parseQuery(raw string) (Query, error) { return provider.ParseQuery([]byte(raw)) }
func ReadOnly(kind, raw string) bool {
	op, _, err := provider.InvocationText(kind, raw)
	return err == nil && op == operations.NativeRead
}
func (c *Client) Query(ctx context.Context, raw string) ([]map[string]any, error) {
	c.dispatched, c.confirmed = false, false
	if c.config.Type == "posthog" {
		return c.posthog(ctx, []byte(raw))
	}
	q, err := parseQuery(raw)
	if err != nil {
		return nil, err
	}
	if c.config.Type == "ramp" {
		return c.ramp(ctx, q)
	}
	if q.Resource != "" {
		return nil, errors.New("resource queries are only supported for Ramp")
	}
	s := session{client: c}
	defer s.close()
	var initialized struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err = s.call(ctx, "initialize", map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{}, "clientInfo": map[string]string{"name": "kelvo-go", "version": "1.0"}}, &initialized); err != nil {
		return nil, err
	}
	switch initialized.ProtocolVersion {
	case "2025-03-26", "2025-06-18", "2025-11-25":
		s.protocol = initialized.ProtocolVersion
	default:
		return nil, errors.New("provider negotiated an unsupported MCP protocol")
	}
	if err = s.call(ctx, "notifications/initialized", nil, nil); err != nil {
		return nil, err
	}
	if q.Operation == "list_tools" {
		rows := []map[string]any{}
		cursor := ""
		seen := map[string]bool{}
		for page := 0; page < 100; page++ {
			params := map[string]any{}
			if cursor != "" {
				params["cursor"] = cursor
			}
			var result struct {
				Tools []map[string]any `json:"tools"`
				Next  string           `json:"nextCursor"`
			}
			if err = s.call(ctx, "tools/list", params, &result); err != nil {
				return nil, err
			}
			rows = append(rows, result.Tools...)
			if b, _ := json.Marshal(rows); len(b) > maxResponse {
				return nil, errors.New("tool catalog exceeds 1 MiB")
			}
			if result.Next == "" {
				return []map[string]any{{"tools": rows}}, nil
			}
			if seen[result.Next] {
				return nil, errors.New("provider repeated a discovery cursor")
			}
			seen[result.Next] = true
			cursor = result.Next
		}
		return nil, errors.New("too many tool pages")
	}
	if q.Arguments == nil {
		q.Arguments = map[string]json.RawMessage{}
	}
	var result map[string]any
	if err = s.call(ctx, "tools/call", map[string]any{"name": q.Tool, "arguments": q.Arguments}, &result); err != nil {
		return nil, err
	}
	if failed, _ := result["isError"].(bool); failed {
		return nil, errors.New("provider tool failed; check its arguments and account permissions")
	}
	c.confirmed = true
	// Preserve structured content, text, citations and pagination metadata intact.
	return []map[string]any{result}, nil
}
func (c *Client) request(ctx context.Context, method, endpoint string, body io.Reader, headers http.Header, expectedID ...int) ([]byte, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, nil, err
	}
	req.Header = headers.Clone()
	req.GetBody = nil
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, errors.New("provider request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("provider returned HTTP %d; check credentials, scopes and environment", resp.StatusCode)
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") && len(expectedID) > 0 && expectedID[0] > 0 {
		b, err := rpcEvent(io.LimitReader(resp.Body, maxResponse+1), expectedID[0])
		return b, resp.Header, err
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil {
		return nil, nil, errors.New("provider response could not be read")
	}
	if len(b) > maxResponse {
		return nil, nil, errors.New("provider response exceeds 1 MiB; narrow the query")
	}
	return b, resp.Header, nil
}
func (c *Client) headers() http.Header {
	h := http.Header{"Content-Type": {"application/json"}, "Accept": {"application/json, text/event-stream"}}
	if c.config.Type == "daloopa" {
		h.Set("X-API-KEY", c.config.Token)
	} else {
		h.Set("Authorization", "Bearer "+c.config.Token)
	}
	return h
}

type session struct {
	client       *Client
	id, protocol string
	seq          int
}

func (s *session) call(ctx context.Context, method string, params any, out any) error {
	s.seq++
	notification := strings.HasPrefix(method, "notifications/")
	payload := map[string]any{"jsonrpc": "2.0", "method": method}
	if !notification {
		payload["id"] = s.seq
	}
	if params != nil {
		payload["params"] = params
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	h := s.client.headers()
	if s.id != "" {
		h.Set("Mcp-Session-Id", s.id)
	}
	if s.protocol != "" {
		h.Set("MCP-Protocol-Version", s.protocol)
	}
	if method == "tools/call" {
		s.client.dispatched = true
	}
	b, headers, err := s.client.request(ctx, "POST", s.client.endpoint, bytes.NewReader(b), h, s.seq)
	if err != nil {
		return err
	}
	if id := headers.Get("Mcp-Session-Id"); id != "" {
		if len(id) > 512 || strings.ContainsAny(id, "\x00\r\n") || len(headers.Values("Mcp-Session-Id")) != 1 {
			return errors.New("invalid MCP session")
		}
		s.id = id
	}
	if notification {
		return nil
	}
	var response struct {
		ID     int             `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if json.Unmarshal(b, &response) != nil || response.ID != s.seq {
		return errors.New("invalid MCP response")
	}
	if len(response.Error) > 0 && string(response.Error) != "null" {
		return errors.New("provider MCP request failed; check tool schema and permissions")
	}
	if len(response.Result) == 0 {
		return errors.New("MCP result is missing")
	}
	if out != nil {
		return decodeExact(response.Result, out)
	}
	return nil
}
func rpcEvent(reader io.Reader, id int) ([]byte, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), maxResponse)
	var data []string
	match := func() []byte {
		v := []byte(strings.Join(data, "\n"))
		var r struct {
			ID int `json:"id"`
		}
		if json.Unmarshal(v, &r) == nil && r.ID == id {
			return v
		}
		return nil
	}
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			if v := match(); v != nil {
				return v, nil
			}
			data = nil
		} else if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if v := match(); v != nil {
		return v, nil
	}
	return nil, errors.New("MCP stream did not contain the requested result")
}
func (s *session) close() {
	if s.id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	h := s.client.headers()
	h.Set("Mcp-Session-Id", s.id)
	h.Set("MCP-Protocol-Version", s.protocol)
	_, _, _ = s.client.request(ctx, "DELETE", s.client.endpoint, nil, h)
}

var rampResources = provider.Resources()

func (c *Client) ramp(ctx context.Context, q Query) ([]map[string]any, error) {
	if q.Operation == "list_tools" { // A real authenticated probe, not a static catalog success.
		probe := q
		probe.Operation = ""
		probe.Resource = "transactions"
		probe.Params = map[string]string{"page_size": "1"}
		if _, err := c.rampPages(ctx, probe, true); err != nil {
			return nil, err
		}
		rows := []map[string]any{}
		for resource, scope := range rampResources {
			rows = append(rows, map[string]any{"resource": resource, "required_scope": scope, "query": map[string]any{"resource": resource, "params": map[string]string{}}})
		}
		return []map[string]any{{"resources": rows}}, nil
	}
	rows, err := c.rampPages(ctx, q, false)
	if err != nil {
		return nil, err
	}
	return []map[string]any{{"data": rows, "rowCount": len(rows)}}, nil
}
func (c *Client) rampPages(ctx context.Context, q Query, probe bool) ([]map[string]any, error) {
	scope, ok := rampResources[q.Resource]
	if !ok {
		return nil, errors.New("unsupported Ramp resource; use list_tools")
	}
	token := c.config.Token
	if c.config.ClientID != "" {
		values := url.Values{"grant_type": {"client_credentials"}, "scope": {scope}}
		req, _ := http.NewRequest("POST", c.endpoint+"/token", nil)
		req.SetBasicAuth(c.config.ClientID, c.config.Token)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		b, _, err := c.request(ctx, "POST", c.endpoint+"/token", strings.NewReader(values.Encode()), req.Header)
		if err != nil {
			return nil, err
		}
		var auth struct {
			Token string `json:"access_token"`
		}
		if json.Unmarshal(b, &auth) != nil || auth.Token == "" {
			return nil, errors.New("Ramp did not return an access token")
		}
		token = auth.Token
	}
	h := http.Header{"Authorization": {"Bearer " + token}, "Accept": {"application/json"}}
	values := url.Values{}
	for k, v := range q.Params {
		values.Set(k, v)
	}
	next := c.endpoint + "/" + q.Resource
	if len(values) > 0 {
		next += "?" + values.Encode()
	}
	base, _ := url.Parse(c.endpoint)
	rows := []map[string]any{}
	seen := map[string]bool{}
	totalBytes := 0
	for page := 0; page < 100; page++ {
		target, err := url.Parse(next)
		if err != nil {
			return nil, errors.New("invalid Ramp pagination URL")
		}
		target = base.ResolveReference(target)
		if target.Scheme != base.Scheme || target.Host != base.Host || target.User != nil || target.Path != base.Path+"/"+q.Resource {
			return nil, errors.New("Ramp pagination escaped the selected resource")
		}
		if seen[target.String()] {
			return nil, errors.New("Ramp repeated a page")
		}
		seen[target.String()] = true
		b, _, err := c.request(ctx, "GET", target.String(), nil, h)
		if err != nil {
			return nil, err
		}
		totalBytes += len(b)
		if totalBytes > maxResponse {
			return nil, errors.New("Ramp result exceeds 1 MiB; add filters")
		}
		var result struct {
			Data []map[string]any `json:"data"`
			Page struct {
				Next *string `json:"next"`
			} `json:"page"`
		}
		if decodeExact(b, &result) != nil || result.Data == nil {
			return nil, errors.New("invalid Ramp data response")
		}
		rows = append(rows, result.Data...)
		if len(rows) > 10000 {
			return nil, errors.New("Ramp query exceeds 10000 rows; add filters")
		}
		if probe {
			return rows, nil
		}
		if result.Page.Next == nil || *result.Page.Next == "" {
			return rows, nil
		}
		next = *result.Page.Next
	}
	return nil, errors.New("Ramp query exceeds 100 pages; add filters")
}

func decodeExact(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(out); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("multiple provider documents")
	}
	return nil
}
