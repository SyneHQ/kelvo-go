// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

type gaField struct {
	metric      bool
	description string
}

func (s *Session) gaMetadata(ctx context.Context, token string, b *budget) (map[string]gaField, error) {
	raw, err := s.request(ctx, "GET", s.endpoint+"/v1beta/properties/"+s.namespace+"/metadata", nil, bearer(token), b)
	if err != nil {
		return nil, err
	}
	object, err := envelope(raw)
	if err != nil {
		return nil, err
	}
	fields := map[string]gaField{}
	for _, kind := range []string{"dimensions", "metrics"} {
		rows, err := objectRows(object[kind])
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			name, ok := row["apiName"].(string)
			if !ok || !fieldName.MatchString(name) {
				continue
			}
			desc, _ := row["description"].(string)
			fields[name] = gaField{metric: kind == "metrics", description: desc}
		}
	}
	return fields, nil
}
func schemaCommand(query, table, account string, fields map[string]gaField) ([]map[string]any, bool, error) {
	words := strings.Fields(strings.ToLower(strings.TrimSuffix(strings.TrimSpace(query), ";")))
	if len(words) == 0 {
		return nil, false, nil
	}
	if words[0] == "select" {
		return nil, false, nil
	}
	if len(words) == 2 && words[0] == "show" {
		switch words[1] {
		case "tables":
			return []map[string]any{{"table": table}}, true, nil
		case "schemas":
			return []map[string]any{{"schema": "default"}}, true, nil
		case "databases":
			return []map[string]any{{"database": account}}, true, nil
		}
	}
	kind := "fields"
	limit := maxRows
	if words[0] == "list" && (len(words) == 2 || len(words) == 4) {
		kind = words[1]
		if kind != "fields" && kind != "dimensions" && kind != "metrics" {
			return nil, true, adapter.ErrUnsupported
		}
		if len(words) == 4 {
			n, err := strconv.Atoi(words[3])
			if words[2] != "limit" || err != nil || n < 0 || n > maxRows {
				return nil, true, adapter.ErrUnsupported
			}
			limit = n
		}
	} else if (words[0] == "describe" || words[0] == "desc") && (len(words) == 2 && words[1] == table || len(words) == 3 && words[1] == "table" && words[2] == table) {
	} else {
		return nil, true, adapter.ErrUnsupported
	}
	names := []string{}
	for name, f := range fields {
		if kind == "dimensions" && f.metric || kind == "metrics" && !f.metric {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	rows := []map[string]any{}
	for _, name := range names[:min(len(names), limit)] {
		kind := "dimension"
		if fields[name].metric {
			kind = "metric"
		}
		rows = append(rows, map[string]any{"name": name, "kind": kind, "description": fields[name].description})
	}
	return rows, true, nil
}
func dateRange(p predicate) (string, string, error) {
	if p.op != "between" || len(p.values) != 2 {
		return "", "", adapter.ErrUnsupported
	}
	start, ok := p.values[0].(string)
	if !ok {
		return "", "", adapter.ErrUnsupported
	}
	end, ok := p.values[1].(string)
	if !ok {
		return "", "", adapter.ErrUnsupported
	}
	a, err := time.Parse("2006-01-02", start)
	if err != nil {
		return "", "", adapter.ErrInvalid
	}
	z, err := time.Parse("2006-01-02", end)
	if err != nil || a.After(z) {
		return "", "", adapter.ErrInvalid
	}
	return start, end, nil
}
func gaFilter(p predicate, metric bool) (map[string]any, error) {
	filter := map[string]any{"fieldName": p.field}
	negate := p.op == "!="
	if metric {
		if len(p.values) != 1 {
			return nil, adapter.ErrUnsupported
		}
		number, ok := p.values[0].(json.Number)
		if !ok {
			return nil, adapter.ErrUnsupported
		}
		op := map[string]string{"=": "EQUAL", "!=": "EQUAL", "<": "LESS_THAN", "<=": "LESS_THAN_OR_EQUAL", ">": "GREATER_THAN", ">=": "GREATER_THAN_OR_EQUAL"}[p.op]
		if op == "" {
			return nil, adapter.ErrUnsupported
		}
		value := map[string]any{"doubleValue": number}
		if !strings.ContainsAny(string(number), ".eE") {
			if _, err := strconv.ParseInt(string(number), 10, 64); err != nil {
				return nil, adapter.ErrUnsupported
			}
			value = map[string]any{"int64Value": string(number)}
		}
		filter["numericFilter"] = map[string]any{"operation": op, "value": value}
	} else {
		values := []string{}
		for _, value := range p.values {
			str, ok := value.(string)
			if !ok {
				return nil, adapter.ErrUnsupported
			}
			values = append(values, str)
		}
		if p.op == "in" {
			filter["inListFilter"] = map[string]any{"values": values, "caseSensitive": true}
		} else if (p.op == "=" || p.op == "!=") && len(values) == 1 {
			filter["stringFilter"] = map[string]any{"matchType": "EXACT", "value": values[0], "caseSensitive": true}
		} else {
			return nil, adapter.ErrUnsupported
		}
	}
	expr := map[string]any{"filter": filter}
	if negate {
		return map[string]any{"notExpression": expr}, nil
	}
	return expr, nil
}
func gaRequest(p selectPlan, fields map[string]gaField) (map[string]any, []projection, []projection, error) {
	if p.table != "ga4" || p.star {
		return nil, nil, nil, adapter.ErrUnsupported
	}
	request := map[string]any{}
	dims := []projection{}
	metrics := []projection{}
	selected := map[string]bool{}
	aliases := map[string]string{}
	for _, c := range p.columns {
		f, ok := fields[c.field]
		if !ok || c.aggregate != "" || selected[c.field] {
			return nil, nil, nil, adapter.ErrUnsupported
		}
		selected[c.field] = true
		aliases[c.alias] = c.field
		if f.metric {
			metrics = append(metrics, c)
		} else {
			dims = append(dims, c)
		}
	}
	if len(dims) > 9 || len(metrics) == 0 || len(metrics) > 10 {
		return nil, nil, nil, adapter.ErrUnsupported
	}
	if len(p.groups) > 0 {
		groups := map[string]bool{}
		for _, name := range p.groups {
			if groups[name] || !selected[name] || fields[name].metric {
				return nil, nil, nil, adapter.ErrUnsupported
			}
			groups[name] = true
		}
		if len(groups) != len(dims) {
			return nil, nil, nil, adapter.ErrUnsupported
		}
	}
	dimNames := []map[string]string{}
	metricNames := []map[string]string{}
	for _, c := range dims {
		dimNames = append(dimNames, map[string]string{"name": c.field})
	}
	for _, c := range metrics {
		metricNames = append(metricNames, map[string]string{"name": c.field})
	}
	request["dimensions"] = dimNames
	request["metrics"] = metricNames
	start, end := "28daysAgo", "yesterday"
	dateSet := false
	df := []map[string]any{}
	mf := []map[string]any{}
	for _, p := range append(append([]predicate{}, p.filters...), p.having...) {
		if p.field == "date" && p.op == "between" {
			if dateSet {
				return nil, nil, nil, adapter.ErrUnsupported
			}
			var err error
			start, end, err = dateRange(p)
			if err != nil {
				return nil, nil, nil, err
			}
			dateSet = true
			continue
		}
		if field := aliases[p.field]; field != "" {
			p.field = field
		}
		field, ok := fields[p.field]
		if !ok {
			return nil, nil, nil, adapter.ErrUnsupported
		}
		f, err := gaFilter(p, field.metric)
		if err != nil {
			return nil, nil, nil, err
		}
		if field.metric {
			mf = append(mf, f)
		} else {
			df = append(df, f)
		}
	}
	request["dateRanges"] = []map[string]string{{"startDate": start, "endDate": end}}
	if len(df) > 0 {
		request["dimensionFilter"] = map[string]any{"andGroup": map[string]any{"expressions": df}}
	}
	if len(mf) > 0 {
		request["metricFilter"] = map[string]any{"andGroup": map[string]any{"expressions": mf}}
	}
	orders := []map[string]any{}
	for _, o := range p.orders {
		name := o.field
		if aliases[name] != "" {
			name = aliases[name]
		}
		if !selected[name] {
			return nil, nil, nil, adapter.ErrUnsupported
		}
		order := map[string]any{"desc": o.desc}
		if fields[name].metric {
			order["metric"] = map[string]string{"metricName": name}
		} else {
			order["dimension"] = map[string]string{"dimensionName": name, "orderType": "ALPHANUMERIC"}
		}
		orders = append(orders, order)
	}
	if len(orders) > 0 {
		request["orderBys"] = orders
	}
	return request, dims, metrics, nil
}
func gaRows(object map[string]json.RawMessage, dims, metrics []projection) ([]map[string]any, error) {
	var dh []struct {
		Name string `json:"name"`
	}
	var mh []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if object["dimensionHeaders"] != nil && json.Unmarshal(object["dimensionHeaders"], &dh) != nil {
		return nil, adapter.ErrInvalid
	}
	if json.Unmarshal(object["metricHeaders"], &mh) != nil || len(dh) != len(dims) || len(mh) != len(metrics) {
		return nil, adapter.ErrInvalid
	}
	for i, h := range dh {
		if h.Name != dims[i].field {
			return nil, adapter.ErrInvalid
		}
	}
	for i, h := range mh {
		if h.Name != metrics[i].field {
			return nil, adapter.ErrInvalid
		}
	}
	if object["rows"] == nil {
		return []map[string]any{}, nil
	}
	var values []struct {
		Dimensions []struct {
			Value string `json:"value"`
		} `json:"dimensionValues"`
		Metrics []struct {
			Value string `json:"value"`
		} `json:"metricValues"`
	}
	if json.Unmarshal(object["rows"], &values) != nil {
		return nil, adapter.ErrInvalid
	}
	rows := []map[string]any{}
	for _, v := range values {
		if len(v.Dimensions) != len(dims) || len(v.Metrics) != len(metrics) {
			return nil, adapter.ErrInvalid
		}
		row := map[string]any{}
		for i, d := range v.Dimensions {
			row[dims[i].alias] = d.Value
		}
		for i, m := range v.Metrics {
			if !jsonNumber.MatchString(m.Value) || len(m.Value) > 128 {
				return nil, adapter.ErrInvalid
			}
			row[metrics[i].alias] = json.Number(m.Value)
		}
		rows = append(rows, row)
	}
	return rows, nil
}
func (s *Session) ga4(ctx context.Context, query string, limits adapter.Limits) ([]map[string]any, error) {
	token, err := s.googleToken(ctx, "https://www.googleapis.com/auth/analytics.readonly")
	if err != nil {
		return nil, err
	}
	b := &budget{limits: limits}
	fields, err := s.gaMetadata(ctx, token, b)
	if err != nil {
		return nil, err
	}
	if rows, handled, err := schemaCommand(query, "ga4", s.namespace, fields); handled {
		if int64(len(rows)) > limits.MaxRows {
			return nil, adapter.ErrLimit
		}
		return rows, err
	}
	p, err := parseSelect(query)
	if err != nil {
		return nil, err
	}
	request, dims, metrics, err := gaRequest(p, fields)
	if err != nil {
		return nil, err
	}
	if p.limited && p.limit == 0 {
		return []map[string]any{}, nil
	}
	rows := []map[string]any{}
	total := int64(-1)
	for page := 0; page < 100; page++ {
		count := int64(1000)
		if p.limited {
			count = min(count, p.limit-int64(len(rows)))
		}
		request["limit"] = strconv.FormatInt(count, 10)
		request["offset"] = strconv.FormatInt(p.offset+int64(len(rows)), 10)
		body, _ := json.Marshal(request)
		raw, err := s.request(ctx, "POST", s.endpoint+"/v1beta/properties/"+s.namespace+":runReport", body, bearer(token), b)
		if err != nil {
			return nil, err
		}
		object, err := envelope(raw)
		if err != nil {
			return nil, err
		}
		var rowCount int64
		if object["rowCount"] != nil && json.Unmarshal(object["rowCount"], &rowCount) != nil || rowCount < 0 {
			return nil, adapter.ErrInvalid
		}
		if total != -1 && total != rowCount {
			return nil, adapter.ErrInvalid
		}
		total = rowCount
		batch, err := gaRows(object, dims, metrics)
		if err != nil {
			return nil, err
		}
		if err = b.add(batch); err != nil {
			return nil, err
		}
		rows = append(rows, batch...)
		if p.offset+int64(len(rows)) >= total || p.limited && int64(len(rows)) >= p.limit {
			return rows, nil
		}
		if len(batch) == 0 {
			return nil, adapter.ErrInvalid
		}
	}
	return nil, adapter.ErrLimit
}
