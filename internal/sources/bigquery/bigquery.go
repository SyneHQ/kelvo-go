// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package bigquery implements bounded, paginated GoogleSQL jobs over HTTPS.
package bigquery

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/apache/arrow-go/v18/arrow"
)

type Engine struct {
	source catalog.Source
	limits query.Limits
	client *cloudapi.Client
}

var component = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func New(c catalog.Config, l query.Limits) (*Engine, error) {
	s, err := cloudapi.SingleSource(c, "bigquery")
	if err != nil {
		return nil, err
	}
	if !component.MatchString(s.Options["project"]) || !component.MatchString(s.Options["location"]) {
		return nil, query.NewError("CONFIGURATION_ERROR", "BigQuery requires project and location")
	}
	for k, v := range s.Options {
		switch k {
		case "project", "location":
		case "dataset":
			if !component.MatchString(v) {
				return nil, query.NewError("CONFIGURATION_ERROR", "Invalid BigQuery dataset")
			}
		case "maximum_bytes_billed":
			if n, e := strconv.ParseInt(v, 10, 64); e != nil || n < 1 {
				return nil, query.NewError("CONFIGURATION_ERROR", "Invalid BigQuery billing limit")
			}
		default:
			return nil, query.NewError("CONFIGURATION_ERROR", "Unknown BigQuery option")
		}
	}
	client, err := cloudapi.New(s, l)
	if err != nil {
		return nil, err
	}
	return &Engine{s, l, client}, nil
}
func (e *Engine) Close() error { e.client.Close(); return nil }

type jobRef struct {
	Project  string `json:"projectId"`
	ID       string `json:"jobId"`
	Location string `json:"location"`
}
type field struct {
	Name               string  `json:"name"`
	Type               string  `json:"type"`
	Mode               string  `json:"mode"`
	Precision          string  `json:"precision"`
	Scale              string  `json:"scale"`
	TimestampPrecision string  `json:"timestampPrecision"`
	Fields             []field `json:"fields"`
}
type result struct {
	Ref      jobRef `json:"jobReference"`
	Complete bool   `json:"jobComplete"`
	Schema   struct {
		Fields []field `json:"fields"`
	} `json:"schema"`
	Rows []struct {
		Fields []struct {
			Value any `json:"v"`
		} `json:"f"`
	} `json:"rows"`
	Total  string `json:"totalRows"`
	Next   string `json:"pageToken"`
	Errors []any  `json:"errors"`
}

func (e *Engine) Execute(parent context.Context, r query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	defer func() { stats.Backend = "bigquery"; stats.DurationNS = time.Since(started).Nanoseconds() }()
	if r.Mode != "native" || r.ConnectionID != e.source.ID || len(r.Sources) != 0 || sink == nil {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if err = query.ValidateRequest(r); err != nil {
		return stats, err
	}
	if r.Mongo != nil || len(r.Parameters) != 0 {
		return stats, query.NewError("UNSUPPORTED", "BigQuery requires SQL without parameters")
	}
	sql, err := sqlguard.ReadOnly(r.SQL)
	if err != nil {
		return stats, err
	}
	ctx, stop := context.WithTimeout(parent, e.limits.Timeout)
	defer stop()
	// A client-generated ID permits cancellation even if the submission response
	// is lost. Submission is never replayed automatically.
	ref := jobRef{e.source.Options["project"], fmt.Sprintf("kelvo_%x", rand.Text()), e.source.Options["location"]}
	base := "/bigquery/v2/projects/" + ref.Project
	q := map[string]any{"query": sql, "useLegacySql": false}
	if v := e.source.Options["dataset"]; v != "" {
		q["defaultDataset"] = map[string]string{"projectId": ref.Project, "datasetId": v}
	}
	if v := e.source.Options["maximum_bytes_billed"]; v != "" {
		q["maximumBytesBilled"] = v
	}
	body := map[string]any{"jobReference": ref, "configuration": map[string]any{"query": q, "jobTimeoutMs": strconv.FormatInt(e.limits.Timeout.Milliseconds(), 10)}}
	defer func() {
		if err != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _, _ = e.client.Do(cleanup, http.MethodPost, base+"/jobs/"+ref.ID+"/cancel?location="+url.QueryEscape(ref.Location), nil, nil, nil)
		}
	}()
	var submitted struct {
		Ref    jobRef `json:"jobReference"`
		Status struct {
			Error any `json:"errorResult"`
		} `json:"status"`
	}
	_, n, err := e.client.Do(ctx, http.MethodPost, base+"/jobs", body, nil, &submitted)
	stats.WireBytes += n
	if err != nil {
		return stats, err
	}
	if submitted.Ref != ref || submitted.Status.Error != nil {
		return stats, query.NewError("QUERY_FAILED", "BigQuery rejected the query job")
	}
	var schema *arrow.Schema
	var writer *rowarrow.Writer
	defer func() {
		if writer != nil {
			writer.Close()
		}
	}()
	var total, rows int64
	next := ""
	pages := 0
	for {
		values := url.Values{"location": {ref.Location}, "maxResults": {strconv.FormatInt(min(1000, e.limits.MaxRows+1), 10)}, "timeoutMs": {"1000"}, "formatOptions.useInt64Timestamp": {"true"}}
		if next != "" {
			values.Set("pageToken", next)
		}
		var page result
		_, n, err = e.client.Do(ctx, http.MethodGet, base+"/queries/"+ref.ID+"?"+values.Encode(), nil, nil, &page)
		stats.WireBytes += n
		if err != nil {
			return stats, err
		}
		if page.Ref != ref || len(page.Errors) != 0 {
			return stats, query.NewError("QUERY_FAILED", "BigQuery returned an invalid or failed query result")
		}
		if !page.Complete {
			if writer != nil || len(page.Rows) != 0 || page.Next != "" {
				return stats, query.NewError("QUERY_FAILED", "BigQuery returned inconsistent job state")
			}
			if err = cloudapi.Poll(ctx); err != nil {
				return stats, err
			}
			continue
		}
		pages++
		if pages > 100000 {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "BigQuery pagination exceeds its limit")
		}
		if schema == nil {
			total, err = strconv.ParseInt(page.Total, 10, 64)
			if err != nil || total < 0 {
				return stats, query.NewError("QUERY_FAILED", "BigQuery returned an invalid row count")
			}
			if total > e.limits.MaxRows {
				return stats, query.NewError("RESOURCE_EXHAUSTED", "BigQuery result exceeds row limit")
			}
			schema, err = makeSchema(page.Schema.Fields)
			if err != nil {
				return stats, err
			}
			writer, err = rowarrow.NewWriter(schema, e.limits, sink)
			if err != nil {
				return stats, err
			}
			stats.PrepareNS = time.Since(started).Nanoseconds()
		} else {
			if page.Total != "" && page.Total != strconv.FormatInt(total, 10) {
				return stats, query.NewError("QUERY_FAILED", "BigQuery changed the result count")
			}
			if len(page.Schema.Fields) > 0 {
				other, er := makeSchema(page.Schema.Fields)
				if er != nil || !schema.Equal(other) {
					return stats, query.NewError("QUERY_FAILED", "BigQuery changed the result schema")
				}
			}
		}
		for _, item := range page.Rows {
			if err = ctx.Err(); err != nil {
				return stats, err
			}
			if rows >= total || len(item.Fields) != schema.NumFields() {
				return stats, query.NewError("QUERY_FAILED", "BigQuery returned inconsistent rows")
			}
			input := make([]any, len(item.Fields))
			for i, cell := range item.Fields {
				input[i] = cell.Value
			}
			row, er := makeRow(schema, input)
			if er != nil {
				return stats, er
			}
			if err = writer.Write(row); err != nil {
				return stats, err
			}
			rows++
		}
		if page.Next == "" {
			break
		}
		if len(page.Next) > 16384 || page.Next == next || len(page.Rows) == 0 || rows >= total {
			return stats, query.NewError("QUERY_FAILED", "BigQuery returned invalid pagination")
		}
		next = page.Next
	}
	if rows != total {
		return stats, query.NewError("QUERY_FAILED", "BigQuery result is incomplete")
	}
	written, err := writer.Finish()
	stats.Rows, stats.Bytes, stats.Batches = written.Rows, written.Bytes, written.Batches
	return stats, err
}

func makeSchema(fields []field) (*arrow.Schema, error) {
	cols := make([]cloudapi.Column, len(fields))
	for i, f := range fields {
		if f.Mode != "" && f.Mode != "NULLABLE" && f.Mode != "REQUIRED" || len(f.Fields) != 0 {
			return nil, query.NewError("UNSUPPORTED", "BigQuery nested or repeated fields are unsupported")
		}
		c := cloudapi.Column{Name: f.Name, Type: strings.ToUpper(f.Type)}
		switch c.Type {
		case "INTEGER", "INT64":
			c.Type = "BIGINT"
		case "FLOAT", "FLOAT64":
			c.Type = "DOUBLE"
		case "BYTES":
			c.Type = "BINARY"
		case "DATETIME":
			c.Type = "TIMESTAMP_NTZ"
		case "NUMERIC", "BIGNUMERIC":
			c.Type, c.Precision, c.Scale = "DECIMAL", 38, 9
			if strings.ToUpper(f.Type) == "BIGNUMERIC" {
				c.Precision, c.Scale = 76, 38
			}
			if f.Precision != "" {
				var err error
				c.Precision, err = strconv.Atoi(f.Precision)
				if err != nil {
					return nil, query.NewError("UNSUPPORTED", "Invalid BigQuery decimal metadata")
				}
				c.Scale = 0
			}
			if f.Scale != "" {
				var err error
				c.Scale, err = strconv.Atoi(f.Scale)
				if err != nil || f.Precision == "" {
					return nil, query.NewError("UNSUPPORTED", "Invalid BigQuery decimal metadata")
				}
			}
		case "TIMESTAMP":
			if f.TimestampPrecision != "" && f.TimestampPrecision != "6" {
				return nil, query.NewError("UNSUPPORTED", "BigQuery sub-microsecond timestamps require another representation")
			}
		}
		cols[i] = c
	}
	s, err := cloudapi.Schema(cols)
	if err != nil {
		return nil, err
	}
	out := s.Fields()
	for i := range out {
		if t, ok := out[i].Type.(*arrow.TimestampType); ok {
			out[i].Type = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: t.TimeZone}
		}
		out[i].Nullable = fields[i].Mode != "REQUIRED"
	}
	return arrow.NewSchema(out, nil), nil
}

func makeRow(schema *arrow.Schema, input []any) ([]any, error) {
	// The BigQuery INT64 timestamp output setting carries exact microseconds.
	// Convert those to an ISO string before the common scalar parser runs.
	for i, v := range input {
		if v == nil {
			continue
		}
		if t, ok := schema.Field(i).Type.(*arrow.TimestampType); ok && t.TimeZone != "" {
			s, ok := v.(string)
			n, err := strconv.ParseInt(s, 10, 64)
			if !ok || err != nil {
				return nil, query.NewError("UNSUPPORTED", "BigQuery timestamp precision is unsupported")
			}
			input[i] = time.UnixMicro(n).UTC().Format(time.RFC3339Nano)
		}
	}
	return cloudapi.Row(schema, input, "bigquery")
}
