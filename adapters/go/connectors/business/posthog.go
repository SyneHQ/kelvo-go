// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package business

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
)

func (c *Client) posthog(ctx context.Context, raw []byte) ([]map[string]any, error) {
	q, kind, err := provider.ParsePostHog(raw)
	if err != nil {
		return nil, adapter.ErrInvalid
	}
	method, path, body := q.Method, q.Path, q.Body
	if q.SQL != "" || q.Query != nil {
		method, path = "POST", "/api/projects/:project_id/query/"
		var envelope map[string]json.RawMessage
		if q.SQL != "" {
			query, _ := json.Marshal(map[string]string{"kind": "HogQLQuery", "query": q.SQL})
			envelope = map[string]json.RawMessage{"query": query}
		} else if err := operations.DecodeStrict(q.Query, &envelope, operations.MaxRequestBytes); err != nil {
			return nil, adapter.ErrInvalid
		}
		// A detached provider query would outlive this bounded operation. Use
		// documented blocking execution and reject any asynchronous response.
		envelope["refresh"] = json.RawMessage(`"force_blocking"`)
		body, err = json.Marshal(envelope)
		if err != nil {
			return nil, adapter.ErrInvalid
		}
	}
	path, err = provider.PostHogPath(path, c.config.Project)
	if err != nil {
		return nil, adapter.ErrInvalid
	}
	endpoint := c.endpoint + path
	var total int64
	rows := []map[string]any{}
	seen := map[string]bool{}
	for page := 0; page < 100; page++ {
		if seen[endpoint] {
			return nil, errors.New("provider repeated a page")
		}
		seen[endpoint] = true
		req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, adapter.ErrInvalid
		}
		req.GetBody = nil // A mutation must never be replayed by the transport.
		req.Header.Set("Authorization", "Bearer "+c.config.Token)
		req.Header.Set("Content-Type", "application/json")
		if kind == operations.NativeExecute {
			c.dispatched = true
		}
		response, err := c.http.Do(req)
		if err != nil {
			return nil, errors.New("provider transport failed")
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 && kind == operations.NativeExecute {
			c.confirmed = true
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, int64(maxResponse)+1-total))
		response.Body.Close()
		if readErr != nil {
			return nil, errors.New("provider response incomplete")
		}
		total += int64(len(data))
		if total > maxResponse {
			return nil, adapter.ErrLimit
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, errors.New("provider request rejected")
		}
		if response.StatusCode == http.StatusNoContent {
			if kind != operations.NativeExecute || len(data) != 0 {
				return nil, adapter.ErrInvalid
			}
			return []map[string]any{}, nil
		}
		var envelope map[string]json.RawMessage
		if operations.DecodeStrict(data, &envelope, maxResponse) != nil || envelope == nil {
			return nil, errors.New("invalid provider response")
		}
		for _, field := range []string{"hasMore", "has_more", "has_more_data"} {
			if value := envelope[field]; value != nil {
				var more bool
				if json.Unmarshal(value, &more) != nil || more {
					return nil, errors.New("provider result incomplete; use bounded keyset queries")
				}
			}
		}
		if progress := envelope["query_status"]; len(progress) != 0 && string(progress) != "null" {
			var status struct {
				Complete bool `json:"complete"`
				Error    bool `json:"error"`
			}
			// Other provider status fields are informational; do not bind them to
			// execution semantics. The completed flag alone never confirms writes.
			if json.Unmarshal(progress, &status) != nil || !status.Complete || status.Error {
				return nil, errors.New("provider query incomplete")
			}
		}
		batch, err := posthogRows(envelope)
		if err != nil {
			return nil, err
		}
		if len(batch) > 10000-len(rows) {
			return nil, adapter.ErrLimit
		}
		rows = append(rows, batch...)
		var next string
		if value := envelope["next"]; len(value) > 0 && string(value) != "null" && json.Unmarshal(value, &next) != nil {
			return nil, adapter.ErrInvalid
		}
		if next == "" {
			return rows, nil
		}
		if method != "GET" || kind != operations.NativeRead {
			return nil, errors.New("provider query returned unsupported pagination")
		}
		// Pagination inherits the exact origin and saved project. Never follow
		// a provider-supplied link to another host or another project.
		nextURL, err := url.Parse(next)
		base, baseErr := url.Parse(c.endpoint)
		if err != nil || baseErr != nil {
			return nil, adapter.ErrInvalid
		}
		nextURL = base.ResolveReference(nextURL)
		if nextURL.Scheme != base.Scheme || nextURL.Host != base.Host || nextURL.User != nil || nextURL.RawPath != "" || nextURL.Fragment != "" {
			return nil, adapter.ErrInvalid
		}
		prefix := "/api/projects/" + c.config.Project + "/"
		if !strings.HasPrefix(nextURL.Path, prefix) {
			return nil, adapter.ErrInvalid
		}
		nextURL.Path = strings.Replace(nextURL.Path, prefix, "/api/projects/:project_id/", 1)
		path, err := provider.PostHogPath(nextURL.RequestURI(), c.config.Project)
		if err != nil {
			return nil, adapter.ErrInvalid
		}
		endpoint = c.endpoint + path
	}
	return nil, adapter.ErrLimit
}

func posthogRows(envelope map[string]json.RawMessage) ([]map[string]any, error) {
	var payload any
	data := envelope["results"]
	if data == nil {
		encoded, err := json.Marshal(envelope)
		if err != nil {
			return nil, adapter.ErrInvalid
		}
		var object map[string]any
		if operations.DecodeStrict(encoded, &object, maxResponse) != nil {
			return nil, adapter.ErrInvalid
		}
		return []map[string]any{object}, nil
	}
	if operations.DecodeStrict(data, &payload, maxResponse) != nil {
		return nil, adapter.ErrInvalid
	}
	values, ok := payload.([]any)
	if !ok {
		return nil, adapter.ErrInvalid
	}
	var columns []string
	if len(envelope["columns"]) != 0 && operations.DecodeStrict(envelope["columns"], &columns, maxResponse) != nil {
		return nil, adapter.ErrInvalid
	}
	seen := map[string]bool{}
	for _, column := range columns {
		if column == "" || seen[column] {
			return nil, errors.New("provider returned ambiguous columns")
		}
		seen[column] = true
	}
	rows := make([]map[string]any, 0, len(values))
	for _, value := range values {
		switch row := value.(type) {
		case map[string]any:
			rows = append(rows, row)
		case []any:
			if len(columns) == 0 || len(columns) != len(row) {
				return nil, adapter.ErrInvalid
			}
			object := make(map[string]any, len(columns))
			for i, column := range columns {
				object[column] = row[i]
			}
			rows = append(rows, object)
		default:
			return nil, adapter.ErrInvalid
		}
	}
	return rows, nil
}
