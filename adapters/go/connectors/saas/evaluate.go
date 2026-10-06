// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import (
	"encoding/json"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func exactNumber(v any) (*big.Rat, bool) {
	n, ok := v.(json.Number)
	if !ok || len(n) > 128 || !jsonNumber.MatchString(string(n)) {
		return nil, false
	}
	if at := strings.IndexAny(string(n), "eE"); at >= 0 {
		exponent, err := strconv.Atoi(string(n)[at+1:])
		if err != nil || exponent < -128 || exponent > 128 {
			return nil, false
		}
	}
	x, ok := new(big.Rat).SetString(string(n))
	return x, ok
}
func compare(a, b any) (int, error) {
	if a == nil && b == nil {
		return 0, nil
	}
	if a == nil {
		return -1, nil
	}
	if b == nil {
		return 1, nil
	}
	if x, ok := exactNumber(a); ok {
		y, ok := exactNumber(b)
		if !ok {
			return 0, adapter.ErrUnsupported
		}
		return x.Cmp(y), nil
	}
	switch x := a.(type) {
	case string:
		y, ok := b.(string)
		if !ok {
			return 0, adapter.ErrUnsupported
		}
		return strings.Compare(x, y), nil
	case bool:
		y, ok := b.(bool)
		if !ok {
			return 0, adapter.ErrUnsupported
		}
		if x == y {
			return 0, nil
		}
		if x {
			return 1, nil
		}
		return -1, nil
	}
	return 0, adapter.ErrUnsupported
}
func matches(row map[string]any, conditions []predicate) (bool, error) {
	for _, p := range conditions {
		value := row[p.field]
		if p.op == "is null" {
			if value != nil {
				return false, nil
			}
			continue
		}
		if p.op == "is not null" {
			if value == nil {
				return false, nil
			}
			continue
		}
		if value == nil {
			return false, nil
		}
		if p.op == "in" {
			found := false
			for _, item := range p.values {
				cmp, err := compare(value, item)
				if err != nil {
					return false, err
				}
				found = found || cmp == 0
			}
			if !found {
				return false, nil
			}
			continue
		}
		cmp, err := compare(value, p.values[0])
		if err != nil {
			return false, err
		}
		match := false
		switch p.op {
		case "=":
			match = cmp == 0
		case "!=":
			match = cmp != 0
		case "<":
			match = cmp < 0
		case "<=":
			match = cmp <= 0
		case ">":
			match = cmp > 0
		case ">=":
			match = cmp >= 0
		case "between":
			upper, err := compare(value, p.values[1])
			if err != nil {
				return false, err
			}
			match = cmp >= 0 && upper <= 0
		default:
			return false, adapter.ErrUnsupported
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}
func decimal(r *big.Rat) (json.Number, error) {
	den := new(big.Int).Set(r.Denom())
	scale := 0
	for _, factor := range []int64{2, 5} {
		count := 0
		for new(big.Int).Mod(den, big.NewInt(factor)).Sign() == 0 {
			den.Div(den, big.NewInt(factor))
			count++
			if count > 256 {
				return "", adapter.ErrLimit
			}
		}
		scale = max(scale, count)
	}
	if den.Cmp(big.NewInt(1)) != 0 {
		return "", adapter.ErrUnsupported
	}
	return json.Number(r.FloatString(scale)), nil
}
func aggregate(rows []map[string]any, c projection) (any, error) {
	count := int64(0)
	var chosen any
	sum := new(big.Rat)
	for _, row := range rows {
		value := row[c.field]
		if c.aggregate == "count" && c.field == "*" {
			count++
			continue
		}
		if value == nil {
			continue
		}
		count++
		switch c.aggregate {
		case "count":
		case "sum":
			x, ok := exactNumber(value)
			if !ok {
				return nil, adapter.ErrUnsupported
			}
			sum.Add(sum, x)
		case "min", "max":
			if chosen == nil {
				chosen = value
				continue
			}
			cmp, err := compare(value, chosen)
			if err != nil {
				return nil, err
			}
			if c.aggregate == "min" && cmp < 0 || c.aggregate == "max" && cmp > 0 {
				chosen = value
			}
		default:
			return nil, adapter.ErrUnsupported
		}
	}
	if c.aggregate == "count" {
		return json.Number(strconv.FormatInt(count, 10)), nil
	}
	if count == 0 {
		return nil, nil
	}
	if c.aggregate == "sum" {
		return decimal(sum)
	}
	return chosen, nil
}
func evaluate(rows []map[string]any, p selectPlan) ([]map[string]any, error) {
	filtered := []map[string]any{}
	for _, row := range rows {
		ok, err := matches(row, p.filters)
		if err != nil {
			return nil, err
		}
		if ok {
			filtered = append(filtered, row)
		}
	}
	aggregated := len(p.groups) > 0
	for _, c := range p.columns {
		aggregated = aggregated || c.aggregate != ""
	}
	if aggregated {
		groups := map[string][]map[string]any{}
		keys := []string{}
		if len(p.groups) == 0 {
			keys = append(keys, "")
			groups[""] = filtered
		}
		for _, row := range filtered {
			if len(p.groups) == 0 {
				break
			}
			values := []any{}
			for _, name := range p.groups {
				value := row[name]
				if n, ok := exactNumber(value); ok {
					value = map[string]string{"number": n.RatString()}
				}
				values = append(values, value)
			}
			raw, err := json.Marshal(values)
			if err != nil {
				return nil, adapter.ErrInvalid
			}
			key := string(raw)
			if _, ok := groups[key]; !ok {
				keys = append(keys, key)
			}
			groups[key] = append(groups[key], row)
		}
		filtered = []map[string]any{}
		for _, key := range keys {
			row := map[string]any{}
			for _, c := range p.columns {
				if c.aggregate != "" {
					value, err := aggregate(groups[key], c)
					if err != nil {
						return nil, err
					}
					row[c.alias] = value
				} else if len(groups[key]) > 0 {
					row[c.alias] = groups[key][0][c.field]
				}
			}
			ok, err := matches(row, p.having)
			if err != nil {
				return nil, err
			}
			if ok {
				filtered = append(filtered, row)
			}
		}
	} else if len(p.having) != 0 {
		return nil, adapter.ErrUnsupported
	}
	var sortErr error
	sort.SliceStable(filtered, func(i, j int) bool {
		for _, order := range p.orders {
			name := order.field
			if !aggregated {
				for _, c := range p.columns {
					if c.alias == name {
						name = c.field
						break
					}
				}
			}
			cmp, err := compare(filtered[i][name], filtered[j][name])
			if err != nil {
				sortErr = err
				return false
			}
			if cmp == 0 {
				continue
			}
			if order.desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	if sortErr != nil {
		return nil, sortErr
	}
	start := min(int64(len(filtered)), p.offset)
	end := int64(len(filtered))
	if p.limited {
		end = min(end, start+p.limit)
	}
	filtered = filtered[start:end]
	if aggregated || p.star {
		return filtered, nil
	}
	result := make([]map[string]any, 0, len(filtered))
	for _, row := range filtered {
		value := map[string]any{}
		for _, c := range p.columns {
			value[c.alias] = row[c.field]
		}
		result = append(result, value)
	}
	return result, nil
}
