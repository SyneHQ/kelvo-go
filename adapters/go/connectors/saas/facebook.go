// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import (
	"context"
	"encoding/json"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

var facebookBreakdowns = map[string]bool{"country": true, "region": true, "impression_device": true, "device_platform": true, "publisher_platform": true, "platform_position": true, "age": true, "gender": true}

func facebookFields() map[string]gaField {
	fields := map[string]gaField{}
	for name := range facebookDimensions {
		fields[name] = gaField{}
	}
	for name := range facebookMetrics {
		fields[name] = gaField{metric: true}
	}
	return fields
}
func facebookRequest(p selectPlan) (url.Values, error) {
	if p.table != "fb_ads" || p.star || len(p.having) != 0 {
		return nil, adapter.ErrUnsupported
	}
	params := url.Values{"level": {"campaign"}, "date_preset": {"last_28d"}, "limit": {"100"}}
	fields := map[string]bool{}
	breakdowns := []string{}
	selected := map[string]bool{}
	for _, c := range p.columns {
		if c.aggregate != "" || selected[c.field] {
			return nil, adapter.ErrUnsupported
		}
		selected[c.field] = true
		if facebookMetrics[c.field] {
			fields[c.field] = true
		} else if name := facebookDimensions[c.field]; name != "" {
			if facebookBreakdowns[c.field] {
				breakdowns = append(breakdowns, c.field)
			} else {
				fields[name] = true
			}
			if c.field == "date" {
				params.Set("time_increment", "1")
			}
		} else {
			return nil, adapter.ErrUnsupported
		}
	}
	groups := map[string]bool{}
	for _, g := range p.groups {
		if !selected[g] || facebookDimensions[g] == "" || groups[g] {
			return nil, adapter.ErrUnsupported
		}
		groups[g] = true
	}
	if len(groups) > 0 {
		for _, c := range p.columns {
			if facebookDimensions[c.field] != "" && !groups[c.field] {
				return nil, adapter.ErrUnsupported
			}
		}
	}
	for _, o := range p.orders {
		found := false
		for _, c := range p.columns {
			if c.alias == o.field || c.field == o.field {
				found = true
			}
		}
		if !found {
			return nil, adapter.ErrUnsupported
		}
	}
	filters := []map[string]any{}
	dateSet, levelSet := false, false
	for _, f := range p.filters {
		if f.field == "date" {
			if dateSet {
				return nil, adapter.ErrUnsupported
			}
			start, end, err := dateRange(f)
			if err != nil {
				return nil, err
			}
			raw, _ := json.Marshal(map[string]string{"since": start, "until": end})
			params.Del("date_preset")
			params.Set("time_range", string(raw))
			dateSet = true
			continue
		}
		if f.field == "level" {
			if levelSet || f.op != "=" || len(f.values) != 1 {
				return nil, adapter.ErrUnsupported
			}
			value, ok := f.values[0].(string)
			if !ok || value != "account" && value != "campaign" && value != "adset" && value != "ad" {
				return nil, adapter.ErrUnsupported
			}
			params.Set("level", value)
			levelSet = true
			continue
		}
		name := map[string]string{"account_id": "account.id", "campaign_id": "campaign.id", "campaign_name": "campaign.name", "adset_id": "adset.id", "adset_name": "adset.name", "ad_id": "ad.id", "ad_name": "ad.name"}[f.field]
		if name == "" {
			return nil, adapter.ErrUnsupported
		}
		op := map[string]string{"=": "EQUAL", "!=": "NOT_EQUAL", "in": "IN"}[f.op]
		if op == "" {
			return nil, adapter.ErrUnsupported
		}
		values := []string{}
		for _, v := range f.values {
			value, ok := v.(string)
			if !ok {
				return nil, adapter.ErrUnsupported
			}
			values = append(values, value)
		}
		var value any = values
		if f.op != "in" {
			value = values[0]
		}
		filters = append(filters, map[string]any{"field": name, "operator": op, "value": value})
	}
	if len(filters) > 0 {
		raw, _ := json.Marshal(filters)
		params.Set("filtering", string(raw))
	}
	list := []string{}
	for field := range fields {
		list = append(list, field)
	}
	sort.Strings(list)
	sort.Strings(breakdowns)
	params.Set("fields", strings.Join(list, ","))
	if len(breakdowns) > 0 {
		params.Set("breakdowns", strings.Join(breakdowns, ","))
	}
	return params, nil
}
func facebookRows(rows []map[string]any, p selectPlan) ([]map[string]any, error) {
	result := make([]map[string]any, 0, len(rows))
	for _, raw := range rows {
		row := map[string]any{}
		for _, c := range p.columns {
			field := c.field
			if facebookDimensions[field] != "" {
				field = facebookDimensions[field]
			}
			value := raw[field]
			if facebookMetrics[c.field] && c.field != "actions" && c.field != "action_values" && value != nil {
				if text, ok := value.(string); ok {
					if !jsonNumber.MatchString(text) || len(text) > 128 {
						return nil, adapter.ErrInvalid
					}
					value = json.Number(text)
				} else if _, ok := value.(json.Number); !ok {
					return nil, adapter.ErrInvalid
				}
			}
			row[c.field] = value
		}
		result = append(result, row)
	}
	return result, nil
}
func (s *Session) facebook(ctx context.Context, query string, limits adapter.Limits) ([]map[string]any, error) {
	if rows, handled, err := schemaCommand(query, "fb_ads", s.namespace, facebookFields()); handled {
		if int64(len(rows)) > limits.MaxRows {
			return nil, adapter.ErrLimit
		}
		return rows, err
	}
	p, err := parseSelect(query)
	if err != nil {
		return nil, err
	}
	params, err := facebookRequest(p)
	if err != nil {
		return nil, err
	}
	if p.limited && p.limit == 0 {
		return []map[string]any{}, nil
	}
	path := "/v24.0/act_" + strings.TrimPrefix(s.namespace, "act_") + "/insights"
	rows := []map[string]any{}
	inputLimits := limits
	inputLimits.MaxRows = maxRows
	b := &budget{limits: inputLimits}
	seen := map[string]bool{}
	for page := 0; page < 100; page++ {
		if p.limited && len(p.orders) == 0 {
			params.Set("limit", strconv.FormatInt(min(100, p.offset+p.limit-int64(len(rows))), 10))
		}
		raw, err := s.request(ctx, "GET", s.endpoint+path+"?"+params.Encode(), nil, bearer(s.token), b)
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
		batch, err = facebookRows(batch, p)
		if err != nil {
			return nil, err
		}
		rows = append(rows, batch...)
		var paging struct {
			Next    string `json:"next"`
			Cursors struct {
				After string `json:"after"`
			} `json:"cursors"`
		}
		if object["paging"] != nil && json.Unmarshal(object["paging"], &paging) != nil {
			return nil, adapter.ErrInvalid
		}
		if paging.Next == "" || p.limited && len(p.orders) == 0 && int64(len(rows)) >= p.offset+p.limit {
			local := p
			local.filters = nil
			local.groups = nil
			result, err := evaluate(rows, local)
			if int64(len(result)) > limits.MaxRows {
				return nil, adapter.ErrLimit
			}
			return result, err
		}
		if _, err = nextAtOrigin(paging.Next, s.endpoint, path); err != nil {
			return nil, err
		}
		after := paging.Cursors.After
		if after == "" || len(after) > 4096 || seen[after] || len(batch) == 0 {
			return nil, adapter.ErrInvalid
		}
		seen[after] = true
		params.Set("after", after)
	}
	return nil, adapter.ErrLimit
}
