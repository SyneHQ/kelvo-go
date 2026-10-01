// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package snowflake implements the Snowflake SQL API v2.
package snowflake

import (
	"context"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/google/uuid"
)

type Engine struct {
	source  catalog.Source
	limits  query.Limits
	client  *cloudapi.Client
	headers map[string]string
}

var handlePattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,128}$`)

const endpoint = "/api/v2/statements"

func New(c catalog.Config, l query.Limits) (*Engine, error) {
	s, err := cloudapi.SingleSource(c, "snowflake")
	if err != nil {
		return nil, err
	}
	typ := s.Options["token_type"]
	if typ == "" {
		typ = "OAUTH"
	}
	if typ != "OAUTH" && typ != "KEYPAIR_JWT" && typ != "PROGRAMMATIC_ACCESS_TOKEN" {
		return nil, query.NewError("CONFIGURATION_ERROR", "Unsupported Snowflake token_type")
	}
	client, err := cloudapi.New(s, l)
	if err != nil {
		return nil, err
	}
	return &Engine{s, l, client, map[string]string{"X-Snowflake-Authorization-Token-Type": typ}}, nil
}
func (e *Engine) Close() error { e.client.Close(); return nil }

type response struct {
	Handle   string   `json:"statementHandle"`
	Handles  []string `json:"statementHandles"`
	Code     string   `json:"code"`
	Data     [][]any  `json:"data"`
	Metadata struct {
		Rows    int64  `json:"numRows"`
		Format  string `json:"format"`
		Columns []struct {
			Name      string `json:"name"`
			Type      string `json:"type"`
			Precision int    `json:"precision"`
			Scale     int    `json:"scale"`
		} `json:"rowType"`
		Partitions []struct {
			Rows int64 `json:"rowCount"`
		} `json:"partitionInfo"`
	} `json:"resultSetMetaData"`
}

func (e *Engine) Execute(parent context.Context, r query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	if r.Mode != "native" || r.ConnectionID != e.source.ID || len(r.Sources) != 0 {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if r.Mongo != nil || len(r.Parameters) > 0 {
		return stats, query.NewError("UNSUPPORTED", "Snowflake native queries currently require SQL without parameters")
	}
	sql, err := sqlguard.ReadOnly(r.SQL)
	if err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	body := map[string]any{"statement": sql, "timeout": int64((e.limits.Timeout + time.Second - 1) / time.Second), "parameters": map[string]string{"MULTI_STATEMENT_COUNT": "1"}}
	for _, key := range []string{"database", "schema", "warehouse", "role"} {
		if v := e.source.Options[key]; v != "" {
			body[key] = v
		}
	}
	var result response
	status, wire, err := e.client.Do(ctx, http.MethodPost, endpoint+"?async=true&requestId="+uuid.NewString(), body, e.headers, &result)
	if err != nil {
		return stats, err
	}
	id := result.Handle
	if !handlePattern.MatchString(id) {
		return stats, query.NewError("QUERY_FAILED", "Snowflake returned an invalid statement handle")
	}
	defer func() {
		if err != nil {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, _, _ = e.client.Do(cleanup, http.MethodPost, endpoint+"/"+id+"/cancel", nil, e.headers, nil)
		}
	}()
	for status == http.StatusAccepted {
		if err = cloudapi.Poll(ctx); err != nil {
			return stats, err
		}
		var next response
		var n int64
		status, n, err = e.client.Do(ctx, http.MethodGet, endpoint+"/"+id, nil, e.headers, &next)
		wire += n
		if err != nil {
			return stats, err
		}
		if next.Handle != id {
			return stats, query.NewError("QUERY_FAILED", "Snowflake changed the statement handle")
		}
		result = next
	}
	if status != http.StatusOK || result.Code != "090001" || len(result.Handles) > 0 {
		return stats, query.NewError("QUERY_FAILED", "Snowflake statement did not succeed as one result set")
	}
	m := result.Metadata
	if m.Rows > e.limits.MaxRows {
		return stats, query.NewError("RESOURCE_EXHAUSTED", "Snowflake result exceeds row limit")
	}
	if m.Rows < 0 || len(m.Partitions) < 1 || int64(len(m.Partitions)) > e.limits.MaxRows+1 || m.Format != "jsonv2" {
		return stats, query.NewError("QUERY_FAILED", "Snowflake returned invalid result metadata")
	}
	var total int64
	for _, part := range m.Partitions {
		if part.Rows < 0 || part.Rows > m.Rows-total {
			return stats, query.NewError("QUERY_FAILED", "Snowflake returned inconsistent partition metadata")
		}
		total += part.Rows
	}
	if total != m.Rows {
		return stats, query.NewError("QUERY_FAILED", "Snowflake partition metadata is incomplete")
	}
	columns := make([]cloudapi.Column, len(m.Columns))
	for i, c := range m.Columns {
		columns[i] = cloudapi.Column{Name: c.Name, Type: c.Type, Precision: c.Precision, Scale: c.Scale}
	}
	schema, err := cloudapi.Schema(columns, "snowflake")
	if err != nil {
		return stats, err
	}
	writer, err := rowarrow.NewWriter(schema, e.limits, sink)
	if err != nil {
		return stats, err
	}
	defer writer.Close()
	data := result.Data
	for index, part := range m.Partitions {
		if int64(len(data)) != part.Rows {
			return stats, query.NewError("QUERY_FAILED", "Snowflake partition row count is inconsistent")
		}
		for _, input := range data {
			if err = ctx.Err(); err != nil {
				return stats, err
			}
			row, er := cloudapi.Row(schema, input, "snowflake")
			if er != nil {
				return stats, er
			}
			if err = writer.Write(row); err != nil {
				return stats, err
			}
		}
		if index == len(m.Partitions)-1 {
			break
		}
		var next response
		var n int64
		status, n, err = e.client.Do(ctx, http.MethodGet, endpoint+"/"+id+"?partition="+strconv.Itoa(index+1), nil, e.headers, &next)
		wire += n
		if err != nil {
			return stats, err
		}
		if status != http.StatusOK || (next.Handle != "" && next.Handle != id) || (next.Code != "" && next.Code != "090001") || len(next.Handles) > 0 {
			return stats, query.NewError("QUERY_FAILED", "Snowflake returned an invalid partition")
		}
		data = next.Data
	}
	stats, err = writer.Finish()
	stats.Backend = "snowflake"
	stats.WireBytes = wire
	stats.DurationNS = time.Since(started).Nanoseconds()
	return stats, err
}
