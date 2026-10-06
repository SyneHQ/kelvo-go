// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

var keywordPrefix = regexp.MustCompile(`(?i)^\s*FIND\s+KEYWORDS\s+WHERE\s+`)

func keywordRequest(query string) (map[string]any, error) {
	match := keywordPrefix.FindStringIndex(query)
	if match == nil {
		return nil, adapter.ErrUnsupported
	}
	p, err := parseSelect("SELECT keywords FROM keyword_ideas WHERE " + query[match[1]:])
	if err != nil || len(p.orders) > 0 || len(p.groups) > 0 || len(p.having) > 0 || p.limited {
		return nil, adapter.ErrUnsupported
	}
	values := map[string]string{}
	keywords := []string{}
	seen := map[string]bool{}
	for _, f := range p.filters {
		if seen[f.field] {
			return nil, adapter.ErrUnsupported
		}
		seen[f.field] = true
		if f.field == "keywords" {
			if f.op != "in" || len(f.values) > 20 {
				return nil, adapter.ErrUnsupported
			}
			for _, v := range f.values {
				text, ok := v.(string)
				if !ok || strings.TrimSpace(text) == "" || len(text) > 256 {
					return nil, adapter.ErrUnsupported
				}
				keywords = append(keywords, text)
			}
			continue
		}
		switch f.field {
		case "page_url", "start_year", "start_month", "end_year", "end_month":
		default:
			return nil, adapter.ErrUnsupported
		}
		if f.op != "=" || len(f.values) != 1 {
			return nil, adapter.ErrUnsupported
		}
		value, ok := f.values[0].(string)
		if !ok {
			return nil, adapter.ErrUnsupported
		}
		values[f.field] = value
	}
	body := map[string]any{"language": "languageConstants/1000", "geoTargetConstants": []string{"geoTargetConstants/2840"}, "keywordPlanNetwork": "GOOGLE_SEARCH_AND_PARTNERS", "includeAdultKeywords": false, "pageSize": 100}
	page := values["page_url"]
	if page != "" {
		u, err := url.Parse(page)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" || len(page) > 2048 {
			return nil, adapter.ErrInvalid
		}
	}
	if len(keywords) > 0 && page != "" {
		body["keywordAndUrlSeed"] = map[string]any{"keywords": keywords, "url": page}
	} else if len(keywords) > 0 {
		body["keywordSeed"] = map[string]any{"keywords": keywords}
	} else if page != "" {
		body["urlSeed"] = map[string]any{"url": page}
	} else {
		return nil, adapter.ErrInvalid
	}
	if values["start_year"] != "" || values["end_year"] != "" || values["start_month"] != "" || values["end_month"] != "" {
		dates := []time.Time{}
		dateParts := []map[string]any{}
		for _, prefix := range []string{"start", "end"} {
			year, err := strconv.Atoi(values[prefix+"_year"])
			if err != nil || year < 2000 || year > 2100 {
				return nil, adapter.ErrInvalid
			}
			month, err := time.Parse("January", strings.ToLower(values[prefix+"_month"]))
			if err != nil {
				return nil, adapter.ErrInvalid
			}
			dates = append(dates, time.Date(year, month.Month(), 1, 0, 0, 0, 0, time.UTC))
			dateParts = append(dateParts, map[string]any{"year": year, "month": strings.ToUpper(month.Format("January"))})
		}
		if dates[0].After(dates[1]) {
			return nil, adapter.ErrInvalid
		}
		body["historicalMetricsOptions"] = map[string]any{"yearMonthRange": map[string]any{"start": dateParts[0], "end": dateParts[1]}}
	}
	return body, nil
}
