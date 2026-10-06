// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package ignite implements Apache Ignite 2's SQL fields REST protocol.
package ignite

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/apache/arrow-go/v18/arrow"
)

type Engine struct {
	source    catalog.Source
	limits    query.Limits
	client    *client
	cacheName string
	maxPages  int
}

func New(c catalog.Config, limits query.Limits) (*Engine, error) {
	return newEngine(c, limits, newClient)
}
func NewResolved(c catalog.Config, limits query.Limits, credentials Credentials) (*Engine, error) {
	return newEngine(c, limits, func(s catalog.Source, l query.Limits) (*client, error) { return newResolvedClient(s, l, credentials) })
}
func newEngine(c catalog.Config, limits query.Limits, open func(catalog.Source, query.Limits) (*client, error)) (*Engine, error) {
	source, err := cloudapi.SingleSource(c, "ignite")
	if err != nil {
		return nil, err
	}
	if !catalog.ValidID(source.ID) || source.Adapter != "" || source.DSNEnv != "" || source.TokenEnv != "" || source.Path != "" {
		return nil, query.NewError("CONFIGURATION_ERROR", "Ignite requires a native REST source")
	}
	cache := ""
	for key, value := range source.Options {
		if key != "cache_name" || len(value) == 0 || len(value) > 4096 || !validText(value) {
			return nil, query.NewError("CONFIGURATION_ERROR", "Ignite accepts only a nonempty cache_name option")
		}
		cache = value
	}
	if cache == "" {
		return nil, query.NewError("CONFIGURATION_ERROR", "Ignite requires cache_name")
	}
	cl, err := open(source, limits)
	if err != nil {
		return nil, err
	}
	return &Engine{source: source, limits: limits, client: cl, cacheName: cache, maxPages: 100000}, nil
}

func (e *Engine) Close() error { e.client.http.CloseIdleConnections(); return nil }

type column struct {
	Name   string `json:"fieldName"`
	Type   string `json:"fieldTypeName"`
	Schema string `json:"schemaName"`
	Table  string `json:"typeName"`
}

type page struct {
	Columns []column `json:"fieldsMetadata"`
	Rows    [][]any  `json:"items"`
	Last    *bool    `json:"last"`
	QueryID *int64   `json:"queryId"`
}

func (e *Engine) Execute(parent context.Context, request query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	defer func() { stats.Backend = "ignite"; stats.DurationNS = time.Since(started).Nanoseconds() }()
	if request.Mode != "native" || request.ConnectionID != e.source.ID || len(request.Sources) != 0 || sink == nil {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if err = query.ValidateRequest(request); err != nil {
		return stats, err
	}
	// Ignite 2's REST argN parameters are untyped strings. Refuse typed
	// parameters rather than changing NULL/numeric semantics or interpolating SQL.
	if request.Mongo != nil || len(request.Parameters) != 0 {
		return stats, query.NewError("UNSUPPORTED", "Ignite REST requires SQL without parameters")
	}
	sql, err := sqlguard.ReadOnly(request.SQL)
	if err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	pageSize := min(int64(1000), e.limits.MaxRows+1)
	form := url.Values{"cmd": {"qryfldexe"}, "qry": {sql}, "pageSize": {strconv.FormatInt(pageSize, 10)}}
	if e.cacheName != "" {
		form.Set("cacheName", e.cacheName)
	}
	var cursor *int64
	defer func() {
		if cursor != nil {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			// The endpoint owns these cursor IDs; neither server-provided URLs nor
			// session tokens influence the destination or authentication.
			_, _, _ = e.client.post(cleanup, url.Values{"cmd": {"qrycls"}, "qryId": {strconv.FormatInt(*cursor, 10)}}, e.client.responseLimit)
		}
	}()
	var schema *arrow.Schema
	var writer *rowarrow.Writer
	defer func() {
		if writer != nil {
			writer.Close()
		}
	}()
	wireLimit := e.limits.MaxBytes*4 + e.client.responseLimit
	for count := 0; ; count++ {
		if count >= e.maxPages || stats.WireBytes >= wireLimit {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Ignite pagination exceeds its budget")
		}
		envelope, bytes, callErr := e.client.post(ctx, form, min(e.client.responseLimit, wireLimit-stats.WireBytes))
		stats.WireBytes += bytes
		if callErr != nil {
			return stats, callErr
		}
		var result page
		decodeErr := decode(envelope.Response, &result)
		// Capture the initial handle even when the response's status or schema
		// fails validation, so those paths still release server resources.
		if cursor == nil && result.QueryID != nil {
			cursor = result.QueryID
		}
		if envelope.Status == nil || *envelope.Status != 0 || !emptyError(envelope.Error) {
			return stats, query.NewError("QUERY_FAILED", "Ignite rejected the query")
		}
		if decodeErr != nil || result.QueryID == nil || result.Last == nil || result.Rows == nil || *cursor != *result.QueryID {
			return stats, query.NewError("QUERY_FAILED", "Ignite returned an invalid query page")
		}
		if int64(len(result.Rows)) > pageSize {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Ignite returned more rows than requested")
		}
		if schema == nil {
			schema, err = makeSchema(result.Columns)
			if err != nil {
				return stats, err
			}
			writer, err = rowarrow.NewWriter(schema, e.limits, sink)
			if err != nil {
				return stats, err
			}
			stats.PrepareNS = time.Since(started).Nanoseconds()
		} else if len(result.Columns) != 0 {
			next, schemaErr := makeSchema(result.Columns)
			if schemaErr != nil || !schema.Equal(next) {
				return stats, query.NewError("QUERY_FAILED", "Ignite changed result schema")
			}
		}
		for _, values := range result.Rows {
			if err = ctx.Err(); err != nil {
				return stats, err
			}
			row, rowErr := makeRow(schema, values)
			if rowErr != nil {
				return stats, rowErr
			}
			if err = writer.Write(row); err != nil {
				return stats, err
			}
		}
		if *result.Last {
			// Ignite closes exhausted cursors itself.
			cursor = nil
			break
		}
		if len(result.Rows) == 0 {
			return stats, query.NewError("QUERY_FAILED", "Ignite cursor made no progress")
		}
		form = url.Values{"cmd": {"qryfetch"}, "qryId": {strconv.FormatInt(*cursor, 10)}, "pageSize": {strconv.FormatInt(pageSize, 10)}}
	}
	if err = ctx.Err(); err != nil {
		return stats, err
	}
	written, err := writer.Finish()
	stats.Rows, stats.Bytes, stats.Batches = written.Rows, written.Bytes, written.Batches
	return stats, err
}

func emptyError(raw json.RawMessage) bool {
	return string(raw) == "null" || string(raw) == `""`
}
