// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package saas executes read-only analytics against explicitly scoped SaaS APIs.
package saas

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
)

const maxRows = 10000
const maxBytes = 8 << 20

type Driver struct{ Engine string }

func (d Driver) Capabilities() operations.Capabilities {
	return operations.Capabilities{Version: operations.Version, Engine: d.Engine, Operations: []operations.Capability{
		{Kind: operations.NativeRead, ParameterTypes: []string{"json"}, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.ConnectionTest, Idempotency: "none", Cancellation: "best_effort"},
	}}
}
func (d Driver) Open(ctx context.Context, c adapter.Connection) (adapter.Session, error) {
	if ctx == nil || ctx.Err() != nil || !provider.SaaS(d.Engine) || c.Engine != d.Engine || c.TenantID == "" || c.ConnectionID == "" || c.Revision == "" || c.Host != "" || c.Port != 0 || c.Password != "" || c.TLS != nil || c.Schema != "" || !provider.ValidSaaSAccount(d.Engine, c.Namespace) {
		return nil, adapter.ErrInvalid
	}
	if c.Token == "" || len(c.Token) > 32<<10 || strings.ContainsAny(c.Username, "\x00\r\n") || len(c.Username) > 4096 {
		return nil, adapter.ErrInvalid
	}
	for key := range c.Options {
		if d.Engine != "google_ads" || key != "login_customer_id" || !provider.ValidSaaSAccount("google_ads", c.Options[key]) {
			return nil, adapter.ErrInvalid
		}
	}
	if c.Endpoint != "" && d.Engine != "salesforce" || c.Username != "" && d.Engine != "google_ads" && d.Engine != "salesforce" {
		return nil, adapter.ErrInvalid
	}
	endpoint := ""
	switch d.Engine {
	case "stripe":
		endpoint = "https://api.stripe.com"
	case "ga4":
		endpoint = "https://analyticsdata.googleapis.com"
	case "google_ads":
		endpoint = "https://googleads.googleapis.com"
		if c.Username == "" {
			return nil, adapter.ErrInvalid
		}
	case "facebook_ads":
		endpoint = "https://graph.facebook.com"
	case "salesforce":
		if !provider.ValidSalesforceOrigin(c.Endpoint) {
			return nil, adapter.ErrInvalid
		}
		endpoint = strings.TrimRight(c.Endpoint, "/")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxConnsPerHost = 2
	transport.MaxIdleConnsPerHost = 1
	transport.MaxIdleConns = 2
	httpClient := &http.Client{Transport: transport, Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	s := &Session{engine: d.Engine, namespace: c.Namespace, endpoint: endpoint, oauthEndpoint: "https://oauth2.googleapis.com/token", token: c.Token, username: c.Username, loginCustomer: c.Options["login_customer_id"], http: httpClient}
	if err := s.validateCredentials(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

type Session struct {
	mu                                                                         sync.Mutex
	engine, namespace, endpoint, oauthEndpoint, token, username, loginCustomer string
	http                                                                       *http.Client
}

func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.http != nil {
		s.http.CloseIdleConnections()
		s.http = nil
	}
	s.token = ""
	s.username = ""
	return nil
}
func (s *Session) RunNative(ctx context.Context, n adapter.Native, sink adapter.Sink) (adapter.NativeResult, error) {
	result := adapter.NativeResult{Outcome: operations.Rejected, Effect: operations.EffectNone}
	if s == nil || ctx == nil || n.Validate() != nil || n.Kind != operations.NativeRead || n.Spec.Provider != s.engine || n.Spec.Command != "query" || n.Spec.ReturnResult || len(n.Spec.Parameters) != 1 || n.Spec.Parameters[0].Type != "json" || sink == nil {
		return result, adapter.ErrInvalid
	}
	q, err := provider.ParseSaaS(s.engine, n.Spec.Parameters[0].Value)
	if err != nil {
		return result, adapter.ErrInvalid
	}
	if ctx.Err() != nil {
		result.Outcome = operations.CancelledBeforeStart
		return result, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.http == nil {
		return result, adapter.ErrInvalid
	}
	limits := n.Limits
	limits.MaxRows = min(limits.MaxRows, maxRows)
	limits.MaxBytes = min(limits.MaxBytes, maxBytes)
	rows, err := s.query(ctx, q.SQL, limits)
	result.Outcome = operations.Failed
	if err != nil {
		return result, err
	}
	result.Stats, err = writeObjects(ctx, rows, limits, sink)
	if err == nil {
		result.Outcome = operations.Completed
	}
	return result, err
}
func (s *Session) Test(ctx context.Context) error {
	if s == nil || ctx == nil {
		return adapter.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.http == nil {
		return adapter.ErrInvalid
	}
	query := ""
	switch s.engine {
	case "stripe":
		query = "SELECT id FROM customers LIMIT 1"
	case "ga4":
		query = "SELECT sessions FROM ga4 LIMIT 1"
	case "google_ads":
		query = "SELECT customer.id FROM customer LIMIT 1"
	case "facebook_ads":
		query = "SELECT account_id FROM fb_ads LIMIT 1"
	case "salesforce":
		query = "SELECT Id FROM Organization LIMIT 1"
	}
	_, err := s.query(ctx, query, adapter.Limits{MaxRows: 1, MaxBytes: 1 << 20, BatchRows: 1})
	return err
}
func (s *Session) query(ctx context.Context, sql string, limits adapter.Limits) ([]map[string]any, error) {
	if limits.MaxRows < 1 || limits.MaxBytes < 1 {
		return nil, adapter.ErrLimit
	}
	switch s.engine {
	case "stripe":
		return s.stripe(ctx, sql, limits)
	case "ga4":
		return s.ga4(ctx, sql, limits)
	case "google_ads":
		return s.googleAds(ctx, sql, limits)
	case "facebook_ads":
		return s.facebook(ctx, sql, limits)
	case "salesforce":
		return s.salesforce(ctx, sql, limits)
	}
	return nil, adapter.ErrUnsupported
}
func envelope(raw []byte) (map[string]json.RawMessage, error) {
	var value map[string]json.RawMessage
	if decodeDocument(raw, &value) != nil || value == nil {
		return nil, adapter.ErrInvalid
	}
	return value, nil
}
