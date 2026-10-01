// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package databricks implements bounded Statement Execution API queries.
package databricks

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
)

type Engine struct {
	source catalog.Source
	limits query.Limits
	client *cloudapi.Client
}

var statementID = regexp.MustCompile(`^[A-Za-z0-9-]{1,128}$`)

const endpoint = "/api/2.0/sql/statements"

func New(c catalog.Config, l query.Limits) (*Engine, error) {
	s, err := cloudapi.SingleSource(c, "databricks")
	if err != nil {
		return nil, err
	}
	if !statementID.MatchString(s.Options["warehouse_id"]) {
		return nil, query.NewError("CONFIGURATION_ERROR", "Databricks requires warehouse_id")
	}
	client, err := cloudapi.New(s, l)
	if err != nil {
		return nil, err
	}
	return &Engine{s, l, client}, nil
}
func (e *Engine) Close() error { e.client.Close(); return nil }

type chunk struct {
	Index     int     `json:"chunk_index"`
	Offset    int64   `json:"row_offset"`
	Count     int64   `json:"row_count"`
	Data      [][]any `json:"data_array"`
	Next      string  `json:"next_chunk_internal_link"`
	NextIndex *int    `json:"next_chunk_index"`
}
type response struct {
	ID     string `json:"statement_id"`
	Status struct {
		State string `json:"state"`
	} `json:"status"`
	Manifest struct {
		Format string `json:"format"`
		Schema struct {
			Columns []struct {
				Name      string `json:"name"`
				Type      string `json:"type_name"`
				Precision int    `json:"type_precision"`
				Scale     int    `json:"type_scale"`
			} `json:"columns"`
		} `json:"schema"`
		Rows      int64 `json:"total_row_count"`
		Chunks    int   `json:"total_chunk_count"`
		Truncated bool  `json:"truncated"`
	} `json:"manifest"`
	Result chunk `json:"result"`
}

func (e *Engine) Execute(parent context.Context, r query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	if r.Mode != "native" || r.ConnectionID != e.source.ID || len(r.Sources) != 0 {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if r.Mongo != nil || len(r.Parameters) > 0 {
		return stats, query.NewError("UNSUPPORTED", "Databricks native queries currently require SQL without parameters")
	}
	sql, err := sqlguard.ReadOnly(r.SQL)
	if err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	body := map[string]any{"statement": sql, "warehouse_id": e.source.Options["warehouse_id"], "format": "JSON_ARRAY", "disposition": "INLINE", "wait_timeout": "0s", "row_limit": e.limits.MaxRows + 1, "byte_limit": min(int64(25<<20), e.limits.MaxBytes, e.client.Limit)}
	for _, key := range []string{"catalog", "schema"} {
		if v := e.source.Options[key]; v != "" {
			body[key] = v
		}
	}
	var result response
	_, wire, err := e.client.Do(ctx, http.MethodPost, endpoint, body, nil, &result)
	if err != nil {
		return stats, err
	}
	id := result.ID
	if !statementID.MatchString(id) {
		return stats, query.NewError("QUERY_FAILED", "Databricks returned an invalid statement handle")
	}
	// Cancellation has its own small deadline because the query context may have expired.
	defer func() {
		if err != nil {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, _, _ = e.client.Do(cleanup, http.MethodPost, endpoint+"/"+id+"/cancel", nil, nil, nil)
		}
	}()
	for result.Status.State == "PENDING" || result.Status.State == "RUNNING" {
		if err = cloudapi.Poll(ctx); err != nil {
			return stats, err
		}
		var next response
		var n int64
		_, n, err = e.client.Do(ctx, http.MethodGet, endpoint+"/"+id, nil, nil, &next)
		wire += n
		if err != nil {
			return stats, err
		}
		if next.ID != id {
			return stats, query.NewError("QUERY_FAILED", "Databricks changed the statement handle")
		}
		result = next
	}
	if result.Status.State != "SUCCEEDED" {
		return stats, query.NewError("QUERY_FAILED", "Databricks statement did not succeed")
	}
	m := result.Manifest
	if m.Truncated || m.Rows > e.limits.MaxRows {
		return stats, query.NewError("RESOURCE_EXHAUSTED", "Databricks result was truncated or exceeds row limit")
	}
	if m.Rows < 0 || m.Chunks < 0 || int64(m.Chunks) > e.limits.MaxRows+1 || m.Format != "JSON_ARRAY" {
		return stats, query.NewError("QUERY_FAILED", "Databricks returned invalid result metadata")
	}
	columns := make([]cloudapi.Column, len(m.Schema.Columns))
	for i, c := range m.Schema.Columns {
		columns[i] = cloudapi.Column{Name: c.Name, Type: c.Type, Precision: c.Precision, Scale: c.Scale}
	}
	schema, err := cloudapi.Schema(columns, "databricks")
	if err != nil {
		return stats, err
	}
	writer, err := rowarrow.NewWriter(schema, e.limits, sink)
	if err != nil {
		return stats, err
	}
	defer writer.Close()
	var rows int64
	current := result.Result
	for index := 0; index < max(1, m.Chunks); index++ {
		if current.Index != index || current.Offset != rows || current.Count != int64(len(current.Data)) || current.Count < 0 || current.Count > m.Rows-rows {
			return stats, query.NewError("QUERY_FAILED", "Databricks returned inconsistent result chunks")
		}
		for _, input := range current.Data {
			if err = ctx.Err(); err != nil {
				return stats, err
			}
			row, er := cloudapi.Row(schema, input, "databricks")
			if er != nil {
				return stats, er
			}
			if err = writer.Write(row); err != nil {
				return stats, err
			}
		}
		rows += current.Count
		if index == max(1, m.Chunks)-1 {
			if current.Next != "" || current.NextIndex != nil {
				return stats, query.NewError("QUERY_FAILED", "Databricks returned unexpected pagination")
			}
			break
		}
		// The documented internal route is restricted to this statement and next chunk.
		expected := endpoint + "/" + id + "/result/chunks/" + strconv.Itoa(index+1)
		if current.Next != "" && current.Next != expected {
			return stats, query.NewError("QUERY_FAILED", "Databricks returned an invalid pagination path")
		}
		if current.NextIndex != nil && *current.NextIndex != index+1 {
			return stats, query.NewError("QUERY_FAILED", "Databricks returned an invalid chunk index")
		}
		if current.Next == "" && current.NextIndex == nil {
			return stats, query.NewError("QUERY_FAILED", "Databricks result is missing a chunk")
		}
		var next chunk
		var n int64
		_, n, err = e.client.Do(ctx, http.MethodGet, expected, nil, nil, &next)
		wire += n
		if err != nil {
			return stats, err
		}
		current = next
	}
	if rows != m.Rows {
		return stats, query.NewError("QUERY_FAILED", "Databricks result is incomplete")
	}
	stats, err = writer.Finish()
	stats.Backend = "databricks"
	stats.WireBytes = wire
	stats.DurationNS = time.Since(started).Nanoseconds()
	return stats, err
}
