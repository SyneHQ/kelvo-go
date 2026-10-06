// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package elasticsearch implements the read-only Elasticsearch SQL API.
package elasticsearch

import (
	"context"
	"encoding/base64"
	"math"
	"net/http"
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
	auth   string
}

func New(c catalog.Config, l query.Limits) (*Engine, error) {
	return newEngine(c, l, cloudapi.New)
}

// NewResolved opens one request-owned source without reading ambient credentials.
func NewResolved(c catalog.Config, l query.Limits, credentials cloudapi.Credentials) (*Engine, error) {
	return newEngine(c, l, func(s catalog.Source, limits query.Limits) (*cloudapi.Client, error) {
		return cloudapi.NewResolved(s, limits, credentials)
	})
}

// NewResolvedBasic keeps basic authentication distinct from API-key and bearer modes.
func NewResolvedBasic(c catalog.Config, l query.Limits, credentials cloudapi.Credentials, username, password string) (*Engine, error) {
	if username == "" || password == "" || strings.ContainsAny(username, ":\r\n\x00") || strings.ContainsAny(password, "\r\n\x00") || len(username) > 4096 || len(password) > 4096 {
		return nil, query.NewError("CONFIGURATION_ERROR", "Elasticsearch requires explicit basic credentials")
	}
	credentials.Token = base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	e, err := NewResolved(c, l, credentials)
	if err == nil {
		e.auth = "Basic"
	}
	return e, err
}

func newEngine(c catalog.Config, l query.Limits, open cloudapi.Factory) (*Engine, error) {
	s, err := cloudapi.SingleSource(c, "elasticsearch")
	if err != nil {
		return nil, err
	}
	auth := "ApiKey"
	for k, v := range s.Options {
		if k != "authentication" || v != "ApiKey" && v != "Bearer" {
			return nil, query.NewError("CONFIGURATION_ERROR", "Elasticsearch accepts authentication ApiKey or Bearer")
		}
		auth = v
	}
	client, err := open(s, l)
	if err != nil {
		return nil, err
	}
	return &Engine{s, l, client, auth}, nil
}
func (e *Engine) Close() error { e.client.Close(); return nil }

type column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type response struct {
	Columns []column `json:"columns"`
	Rows    [][]any  `json:"rows"`
	Cursor  string   `json:"cursor"`
	Partial bool     `json:"is_partial"`
	Running bool     `json:"is_running"`
	ID      string   `json:"id"`
	Error   any      `json:"error"`
}

func (e *Engine) Execute(parent context.Context, r query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	defer func() { stats.Backend = "elasticsearch"; stats.DurationNS = time.Since(started).Nanoseconds() }()
	if r.Mode != "native" || r.ConnectionID != e.source.ID || len(r.Sources) != 0 || sink == nil {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if err = query.ValidateRequest(r); err != nil {
		return stats, err
	}
	if r.Mongo != nil {
		return stats, query.NewError("UNSUPPORTED", "Elasticsearch requires SQL")
	}
	values, err := r.Values()
	if err != nil {
		return stats, err
	}
	for i, value := range values {
		if n, ok := value.(uint64); ok {
			if n > math.MaxInt64 {
				return stats, query.NewError("UNSUPPORTED", "Elasticsearch SQL parameters require signed 64-bit integers")
			}
			values[i] = int64(n)
		}
	}
	sql, err := sqlguard.ReadOnly(r.SQL)
	if err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	headers := map[string]string{"Authorization": e.auth + " " + e.client.Token}
	// Synchronous API only. Partial/async replies are never accepted as complete.
	body := map[string]any{"query": sql, "fetch_size": min(1000, e.limits.MaxRows+1), "field_multi_value_leniency": false, "allow_partial_search_results": false, "columnar": false, "time_zone": "UTC", "request_timeout": strconv.FormatInt(e.limits.Timeout.Milliseconds(), 10) + "ms", "page_timeout": "30s"}
	if len(values) > 0 {
		body["params"] = values
	}
	cursor := ""
	defer func() {
		if cursor != "" {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, _, _ = e.client.Do(cleanup, http.MethodPost, "/_sql/close", map[string]string{"cursor": cursor}, headers, nil)
		}
	}()
	var schema *arrow.Schema
	var writer *rowarrow.Writer
	defer func() {
		if writer != nil {
			writer.Close()
		}
	}()
	for pages := 0; ; pages++ {
		if pages >= 100000 {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Elasticsearch pagination exceeds its limit")
		}
		var page response
		_, n, er := e.client.Do(ctx, http.MethodPost, "/_sql?format=json", body, headers, &page)
		stats.WireBytes += n
		if er != nil {
			return stats, er
		}
		// Capture a valid returned cursor before any row/schema checks so a failed
		// conversion still closes the provider cursor.
		if len(page.Cursor) > 64<<10 {
			return stats, query.NewError("QUERY_FAILED", "Elasticsearch cursor exceeds its limit")
		}
		previous := cursor
		if page.Cursor != "" {
			cursor = page.Cursor
		}
		if page.Error != nil || page.Partial || page.Running || page.ID != "" {
			return stats, query.NewError("QUERY_FAILED", "Elasticsearch returned an incomplete or failed result")
		}
		if schema == nil {
			schema, err = makeSchema(page.Columns)
			if err != nil {
				return stats, err
			}
			writer, err = rowarrow.NewWriter(schema, e.limits, sink)
			if err != nil {
				return stats, err
			}
			stats.PrepareNS = time.Since(started).Nanoseconds()
		} else if len(page.Columns) > 0 {
			other, er := makeSchema(page.Columns)
			if er != nil || !schema.Equal(other) {
				return stats, query.NewError("QUERY_FAILED", "Elasticsearch changed result schema")
			}
		}
		for _, input := range page.Rows {
			if err = ctx.Err(); err != nil {
				return stats, err
			}
			row, er := cloudapi.Row(schema, input, "elasticsearch")
			if er != nil {
				return stats, er
			}
			if err = writer.Write(row); err != nil {
				return stats, err
			}
		}
		if page.Cursor == "" {
			cursor = ""
			break
		}
		// Elasticsearch can reuse an opaque cursor while advancing server state.
		// Reusing one without making progress cannot be a successful next page.
		if len(page.Rows) == 0 && page.Cursor == previous {
			return stats, query.NewError("QUERY_FAILED", "Elasticsearch cursor made no progress")
		}
		body = map[string]any{"cursor": page.Cursor, "columnar": false, "time_zone": "UTC"}
	}
	written, err := writer.Finish()
	stats.Rows, stats.Bytes, stats.Batches = written.Rows, written.Bytes, written.Batches
	return stats, err
}

func makeSchema(columns []column) (*arrow.Schema, error) {
	cols := make([]cloudapi.Column, len(columns))
	for i, c := range columns {
		kind := strings.ToLower(c.Type)
		mapped := ""
		switch kind {
		case "boolean":
			mapped = "BOOLEAN"
		case "byte":
			mapped = "TINYINT"
		case "short":
			mapped = "SMALLINT"
		case "integer":
			mapped = "INT"
		case "long":
			mapped = "BIGINT"
		case "float", "half_float":
			mapped = "DOUBLE"
		case "double", "scaled_float":
			mapped = "DOUBLE"
		case "keyword", "text", "ip", "version":
			mapped = "STRING"
		case "binary":
			mapped = "BINARY"
		case "datetime", "date", "date_nanos":
			mapped = "TIMESTAMP"
		default:
			return nil, query.NewError("UNSUPPORTED", "Elasticsearch result type is unsupported")
		}
		cols[i] = cloudapi.Column{Name: c.Name, Type: mapped}
	}
	s, err := cloudapi.Schema(cols)
	if err != nil {
		return nil, err
	}
	fields := s.Fields()
	for i, c := range columns {
		if c.Type == "float" || c.Type == "half_float" {
			fields[i].Type = arrow.PrimitiveTypes.Float32
		}
		fields[i].Metadata = arrow.MetadataFrom(map[string]string{"native_type": c.Type})
	}
	return arrow.NewSchema(fields, nil), nil
}
