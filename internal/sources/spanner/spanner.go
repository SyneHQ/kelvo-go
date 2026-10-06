// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package spanner executes bounded, single-use read-only SQL through Spanner REST.
package spanner

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
)

type Engine struct {
	source   catalog.Source
	limits   query.Limits
	client   *cloudapi.Client
	database string
}

var component = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var sessionID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

func New(c catalog.Config, limits query.Limits) (*Engine, error) {
	return newEngine(c, limits, cloudapi.New)
}

// NewResolved opens one request-owned source without reading ambient credentials.
func NewResolved(c catalog.Config, limits query.Limits, credentials cloudapi.Credentials) (*Engine, error) {
	return newEngine(c, limits, func(s catalog.Source, l query.Limits) (*cloudapi.Client, error) {
		return cloudapi.NewResolved(s, l, credentials)
	})
}

func newEngine(c catalog.Config, limits query.Limits, open cloudapi.Factory) (*Engine, error) {
	source, err := cloudapi.SingleSource(c, "spanner")
	if err != nil {
		return nil, err
	}
	for _, key := range []string{"project", "instance", "database"} {
		if !component.MatchString(source.Options[key]) {
			return nil, query.NewError("CONFIGURATION_ERROR", "Spanner requires valid project, instance and database options")
		}
	}
	for key := range source.Options {
		if key != "project" && key != "instance" && key != "database" {
			return nil, query.NewError("CONFIGURATION_ERROR", "Unknown Spanner option")
		}
	}
	client, err := open(source, limits)
	if err != nil {
		return nil, err
	}
	// executeSql is not a streaming API. Bound its complete decoded JSON reply,
	// independently of the output row budget and the provider's 10 MiB limit.
	client.Limit = min(client.Limit, 10<<20)
	database := "projects/" + source.Options["project"] + "/instances/" + source.Options["instance"] + "/databases/" + source.Options["database"]
	return &Engine{source: source, limits: limits, client: client, database: database}, nil
}

func (e *Engine) Close() error { e.client.Close(); return nil }

type resultSet struct {
	Metadata struct {
		RowType struct {
			Fields []field `json:"fields"`
		} `json:"rowType"`
	} `json:"metadata"`
	Rows json.RawMessage `json:"rows"`
}

func (e *Engine) Execute(parent context.Context, request query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	defer func() { stats.Backend = "spanner"; stats.DurationNS = time.Since(started).Nanoseconds() }()
	if request.Mode != "native" || request.ConnectionID != e.source.ID || len(request.Sources) != 0 || sink == nil {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if err = query.ValidateRequest(request); err != nil {
		return stats, err
	}
	if request.Mongo != nil || len(request.Parameters) != 0 {
		return stats, query.NewError("UNSUPPORTED", "Spanner requires SQL without parameters")
	}
	sql, err := sqlguard.ReadOnly(request.SQL)
	if err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	var session struct {
		Name string `json:"name"`
	}
	_, n, err := e.client.Do(ctx, http.MethodPost, "/v1/"+e.database+"/sessions", map[string]any{"session": map[string]any{}}, nil, &session)
	stats.WireBytes += n
	if err != nil {
		return stats, err
	}
	prefix := e.database + "/sessions/"
	if !strings.HasPrefix(session.Name, prefix) || !sessionID.MatchString(strings.TrimPrefix(session.Name, prefix)) {
		return stats, query.NewError("QUERY_FAILED", "Spanner returned an invalid session identity")
	}
	path := "/v1/" + session.Name
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		status, received, cleanupErr := e.client.Do(cleanup, http.MethodDelete, path, nil, nil, nil)
		stats.WireBytes += received
		if cleanupErr != nil && status != http.StatusNotFound && err == nil {
			err = query.NewError("QUERY_FAILED", "Spanner session cleanup failed")
		}
	}()
	body := map[string]any{
		"sql":         sql,
		"queryMode":   "NORMAL",
		"transaction": map[string]any{"singleUse": map[string]any{"readOnly": map[string]bool{"strong": true}}},
	}
	var result resultSet
	_, n, err = e.client.Do(ctx, http.MethodPost, path+":executeSql", body, nil, &result)
	stats.WireBytes += n
	if err != nil {
		return stats, err
	}
	schema, err := makeSchema(result.Metadata.RowType.Fields)
	if err != nil {
		return stats, err
	}
	if len(result.Rows) == 0 {
		result.Rows = json.RawMessage("[]")
	}
	decoder := json.NewDecoder(bytes.NewReader(result.Rows))
	decoder.UseNumber()
	if token, decodeErr := decoder.Token(); decodeErr != nil || token != json.Delim('[') {
		return stats, query.NewError("QUERY_FAILED", "Spanner returned invalid result rows")
	}
	writer, err := rowarrow.NewWriter(schema, e.limits, sink)
	if err != nil {
		return stats, err
	}
	defer writer.Close()
	stats.PrepareNS = time.Since(started).Nanoseconds()
	for decoder.More() {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		var row []any
		if decoder.Decode(&row) != nil {
			return stats, query.NewError("QUERY_FAILED", "Spanner returned invalid result rows")
		}
		values, convertErr := makeRow(result.Metadata.RowType.Fields, row)
		if convertErr != nil {
			return stats, convertErr
		}
		if err = writer.Write(values); err != nil {
			return stats, err
		}
	}
	if token, decodeErr := decoder.Token(); decodeErr != nil || token != json.Delim(']') {
		return stats, query.NewError("QUERY_FAILED", "Spanner returned incomplete result rows")
	}
	written, err := writer.Finish()
	stats.Rows, stats.Bytes, stats.Batches = written.Rows, written.Bytes, written.Batches
	return stats, err
}
