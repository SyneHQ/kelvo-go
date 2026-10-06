// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

// v25 is the documented current major version as of October 2026. Query text
// remains byte-for-byte intact, including literals and case-sensitive names.
func (s *Session) googleAds(ctx context.Context, query string, limits adapter.Limits) ([]map[string]any, error) {
	words := strings.Fields(query)
	if len(words) == 0 {
		return nil, adapter.ErrUnsupported
	}
	endpoint := s.endpoint + "/v25/customers/" + s.namespace + "/googleAds:search"
	body := map[string]any{"query": query}
	if strings.EqualFold(words[0], "FIND") {
		var err error
		body, err = keywordRequest(query)
		if err != nil {
			return nil, err
		}
		endpoint = s.endpoint + "/v25/customers/" + s.namespace + ":generateKeywordIdeas"
	} else if !strings.EqualFold(words[0], "SELECT") {
		return nil, adapter.ErrUnsupported
	}
	token, err := s.googleToken(ctx, "https://www.googleapis.com/auth/adwords")
	if err != nil {
		return nil, err
	}
	headers := bearer(token)
	headers.Set("developer-token", s.username)
	if s.loginCustomer != "" {
		headers.Set("login-customer-id", s.loginCustomer)
	}
	rows := []map[string]any{}
	b := &budget{limits: limits}
	seen := map[string]bool{}
	for page := 0; page < 100; page++ {
		raw, _ := json.Marshal(body)
		response, err := s.request(ctx, "POST", endpoint, raw, headers, b)
		if err != nil {
			return nil, err
		}
		object, err := envelope(response)
		if err != nil {
			return nil, err
		}
		batch := []map[string]any{}
		if item := object["results"]; item != nil {
			batch, err = objectRows(item)
			if err != nil {
				return nil, err
			}
		}
		if err = b.add(batch); err != nil {
			return nil, err
		}
		rows = append(rows, batch...)
		var next string
		if item := object["nextPageToken"]; item != nil && json.Unmarshal(item, &next) != nil {
			return nil, adapter.ErrInvalid
		}
		if next == "" {
			return rows, nil
		}
		if len(next) > 8192 || seen[next] || len(batch) == 0 {
			return nil, errors.New("provider pagination invalid")
		}
		seen[next] = true
		body["pageToken"] = next
	}
	return nil, adapter.ErrLimit
}
