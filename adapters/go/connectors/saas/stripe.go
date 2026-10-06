// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func validateLocalPlan(p selectPlan, fields map[string]bool) error {
	groups := map[string]bool{}
	aliases := map[string]bool{}
	aggregated := len(p.groups) > 0
	for _, name := range p.groups {
		if !fields[name] || groups[name] {
			return adapter.ErrUnsupported
		}
		groups[name] = true
	}
	for _, c := range p.columns {
		if !fields[c.field] && !(c.aggregate == "count" && c.field == "*") {
			return adapter.ErrUnsupported
		}
		aliases[c.alias] = true
		aggregated = aggregated || c.aggregate != ""
	}
	if p.star && aggregated {
		return adapter.ErrUnsupported
	}
	for _, c := range p.columns {
		if aggregated && c.aggregate == "" && !groups[c.field] {
			return adapter.ErrUnsupported
		}
	}
	for _, f := range p.filters {
		if !fields[f.field] {
			return adapter.ErrUnsupported
		}
	}
	for _, f := range p.having {
		if !aggregated || !aliases[f.field] {
			return adapter.ErrUnsupported
		}
	}
	for _, o := range p.orders {
		if aggregated && !aliases[o.field] || !aggregated && !fields[o.field] && !aliases[o.field] {
			return adapter.ErrUnsupported
		}
	}
	return nil
}
func (s *Session) stripe(ctx context.Context, query string, limits adapter.Limits) ([]map[string]any, error) {
	p, err := parseSelect(query)
	if err != nil {
		return nil, err
	}
	entity, ok := stripeEntities[p.table]
	if !ok {
		return nil, adapter.ErrUnsupported
	}
	if err = validateLocalPlan(p, entity.fields); err != nil {
		return nil, err
	}
	if p.limited && p.limit == 0 {
		return []map[string]any{}, nil
	}
	params := url.Values{"limit": {"100"}}
	headers := bearer(s.token)
	if s.namespace != "" {
		headers.Set("Stripe-Account", s.namespace)
	}
	// Only plain projections can stop at a requested LIMIT. Filtering, sorting
	// and aggregates must inspect the complete bounded input or fail.
	early := p.limited && len(p.filters) == 0 && len(p.groups) == 0 && len(p.orders) == 0 && len(p.having) == 0
	for _, c := range p.columns {
		early = early && c.aggregate == ""
	}
	inputLimits := limits
	inputLimits.MaxRows = maxRows
	b := &budget{limits: inputLimits}
	rows := []map[string]any{}
	seen := map[string]bool{}
	for page := 0; page < 100; page++ {
		if early {
			remaining := p.offset + p.limit - int64(len(rows))
			params.Set("limit", strconv.FormatInt(min(100, remaining), 10))
		}
		raw, err := s.request(ctx, "GET", s.endpoint+entity.endpoint+"?"+params.Encode(), nil, headers, b)
		if err != nil {
			return nil, err
		}
		object, err := envelope(raw)
		if err != nil {
			return nil, err
		}
		batch, err := objectRows(object["data"])
		if err != nil {
			return nil, err
		}
		if err = b.add(batch); err != nil {
			return nil, err
		}
		rows = append(rows, batch...)
		var more bool
		if json.Unmarshal(object["has_more"], &more) != nil {
			return nil, adapter.ErrInvalid
		}
		if !more || early && int64(len(rows)) >= p.offset+p.limit {
			result, err := evaluate(rows, p)
			if int64(len(result)) > limits.MaxRows {
				return nil, adapter.ErrLimit
			}
			return result, err
		}
		if len(batch) == 0 {
			return nil, errors.New("provider pagination invalid")
		}
		id, ok := batch[len(batch)-1]["id"].(string)
		if !ok || id == "" || len(id) > 256 || seen[id] {
			return nil, adapter.ErrInvalid
		}
		seen[id] = true
		params.Set("starting_after", id)
	}
	return nil, adapter.ErrLimit
}
