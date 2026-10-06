// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package athena implements source-bound Athena query execution and pagination.
package athena

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/awsapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/apache/arrow-go/v18/arrow"
)

type Engine struct {
	s catalog.Source
	l query.Limits
	c *awsapi.Client
}

var workgroupName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,128}$`)
var executionID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

func New(c catalog.Config, l query.Limits) (*Engine, error) { return newEngine(c, l, awsapi.New) }

func NewResolved(c catalog.Config, l query.Limits, credentials awsapi.Credentials) (*Engine, error) {
	return newEngine(c, l, func(s catalog.Source, l query.Limits, service string) (*awsapi.Client, error) {
		return awsapi.NewResolved(s, l, service, credentials)
	})
}
func newEngine(c catalog.Config, l query.Limits, open awsapi.Factory) (*Engine, error) {
	s, err := cloudapi.SingleSource(c, "athena")
	if err != nil {
		return nil, err
	}
	output, parseErr := url.Parse(s.Options["output_location"])
	if !workgroupName.MatchString(s.Options["workgroup"]) || s.Options["database"] == "" || len(s.Options["database"]) > 255 || strings.ContainsAny(s.Options["database"], "\r\n\x00") || parseErr != nil || output.Scheme != "s3" || output.Hostname() == "" || output.User != nil || output.RawQuery != "" || output.Fragment != "" || len(s.Options["output_location"]) > 1024 {
		return nil, query.NewError("CONFIGURATION_ERROR", "Athena requires workgroup, database and an S3 output_location")
	}
	client, err := open(s, l, "athena")
	if err != nil {
		return nil, err
	}
	return &Engine{s, l, client}, nil
}
func (e *Engine) Close() error { e.c.Close(); return nil }

type cell struct {
	VarCharValue *string `json:"VarCharValue"`
}
type athenaRow struct {
	Data []cell `json:"Data"`
}
type athenaColumn struct {
	Name      string `json:"Name"`
	Label     string `json:"Label"`
	Type      string `json:"Type"`
	Precision int    `json:"Precision"`
	Scale     int    `json:"Scale"`
}
type resultSet struct {
	Metadata struct {
		Columns []athenaColumn `json:"ColumnInfo"`
	} `json:"ResultSetMetadata"`
	Rows []athenaRow `json:"Rows"`
}
type result struct {
	ResultSet *resultSet `json:"ResultSet"`
	NextToken string     `json:"NextToken"`
}

func (e *Engine) Execute(parent context.Context, r query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	defer func() { stats.Backend = "athena"; stats.DurationNS = time.Since(started).Nanoseconds() }()
	if err = query.ValidateRequest(r); err != nil {
		return
	}
	if r.Mode != "native" || r.ConnectionID != e.s.ID || sink == nil {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if len(r.Parameters) > 0 || r.Mongo != nil {
		return stats, query.NewError("UNSUPPORTED", "Athena requires SQL without parameters")
	}
	sql, err := sqlguard.ReadOnly(r.SQL)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, e.l.Timeout)
	defer cancel()
	token := make([]byte, 32)
	if _, err = rand.Read(token); err != nil {
		return stats, query.NewError("QUERY_FAILED", "Cannot prepare Athena request")
	}
	var startOut struct {
		ID string `json:"QueryExecutionId"`
	}
	body := map[string]any{"ClientRequestToken": hex.EncodeToString(token), "QueryString": sql, "WorkGroup": e.s.Options["workgroup"], "QueryExecutionContext": map[string]string{"Database": e.s.Options["database"]}, "ResultConfiguration": map[string]string{"OutputLocation": e.s.Options["output_location"]}, "ResultReuseConfiguration": map[string]any{"ResultReuseByAgeConfiguration": map[string]bool{"Enabled": false}}}
	n, err := e.c.Do(ctx, "AmazonAthena.StartQueryExecution", body, &startOut)
	if err != nil {
		_ = awsapi.Account(&stats, n, e.l)
		return
	}
	if !executionID.MatchString(startOut.ID) {
		return stats, query.NewError("QUERY_FAILED", "Athena returned an invalid query ID")
	}
	// Cleanup has an independent, bounded context so cancellation of the caller
	// cannot prevent StopQueryExecution. Never resubmit StartQueryExecution.
	defer func() {
		if err != nil {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, _ = e.c.Do(cleanup, "AmazonAthena.StopQueryExecution", map[string]string{"QueryExecutionId": startOut.ID}, nil)
		}
	}()
	if err = awsapi.Account(&stats, n, e.l); err != nil {
		return
	}
	for polls := 0; ; polls++ {
		if polls >= 10000 {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Athena polling exceeds limit")
		}
		var out struct {
			QueryExecution *struct {
				ID     string `json:"QueryExecutionId"`
				Status struct {
					State string `json:"State"`
				} `json:"Status"`
			} `json:"QueryExecution"`
		}
		n, err = e.c.Do(ctx, "AmazonAthena.GetQueryExecution", map[string]string{"QueryExecutionId": startOut.ID}, &out)
		budgetErr := awsapi.Account(&stats, n, e.l)
		if err != nil {
			return
		}
		if budgetErr != nil {
			return stats, budgetErr
		}
		if out.QueryExecution == nil || (out.QueryExecution.ID != "" && out.QueryExecution.ID != startOut.ID) {
			return stats, query.NewError("QUERY_FAILED", "Athena returned an invalid execution status")
		}
		switch out.QueryExecution.Status.State {
		case "SUCCEEDED":
		case "QUEUED", "RUNNING":
			if err = cloudapi.Poll(ctx); err != nil {
				return
			}
			continue
		case "FAILED", "CANCELLED":
			return stats, query.NewError("QUERY_FAILED", "Athena query did not succeed")
		default:
			return stats, query.NewError("QUERY_FAILED", "Athena returned an invalid execution state")
		}
		break
	}
	var schema *arrow.Schema
	var writer *rowarrow.Writer
	defer func() {
		if writer != nil {
			writer.Close()
		}
	}()
	next := ""
	seen := map[[32]byte]bool{}
	for page := 0; ; page++ {
		if page >= 10000 {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Athena pagination exceeds limit")
		}
		body := map[string]any{"QueryExecutionId": startOut.ID, "QueryResultType": "DATA_ROWS", "MaxResults": min(int64(1000), e.l.MaxRows+1)}
		if next != "" {
			body["NextToken"] = next
		}
		var out result
		n, err = e.c.Do(ctx, "AmazonAthena.GetQueryResults", body, &out)
		budgetErr := awsapi.Account(&stats, n, e.l)
		if err != nil {
			return
		}
		if budgetErr != nil {
			return stats, budgetErr
		}
		if out.ResultSet == nil {
			return stats, query.NewError("QUERY_FAILED", "Athena omitted the result set")
		}
		if schema == nil {
			schema, err = makeSchema(out.ResultSet.Metadata.Columns)
			if err != nil {
				return
			}
			writer, err = rowarrow.NewWriter(schema, e.l, sink)
			if err != nil {
				return
			}
			stats.PrepareNS = time.Since(started).Nanoseconds()
		} else if len(out.ResultSet.Metadata.Columns) > 0 {
			other, schemaErr := makeSchema(out.ResultSet.Metadata.Columns)
			if schemaErr != nil || !schema.Equal(other) {
				return stats, query.NewError("QUERY_FAILED", "Athena changed result schema")
			}
		}
		for i, row := range out.ResultSet.Rows {
			if err = ctx.Err(); err != nil {
				return
			}
			if len(row.Data) != schema.NumFields() {
				return stats, query.NewError("QUERY_FAILED", "Athena returned an invalid row")
			}
			// The header belongs to the first API page only. An empty first page
			// must never cause the next page's first data row to be discarded.
			if page == 0 && i == 0 {
				for j, c := range row.Data {
					if c.VarCharValue == nil || (*c.VarCharValue != schema.Field(j).Name && (out.ResultSet.Metadata.Columns[j].Label == "" || *c.VarCharValue != out.ResultSet.Metadata.Columns[j].Label)) {
						return stats, query.NewError("QUERY_FAILED", "Athena returned an invalid result header")
					}
				}
				continue
			}
			values := make([]any, len(row.Data))
			for j, c := range row.Data {
				if c.VarCharValue != nil {
					values[j], err = convert(schema.Field(j).Type, *c.VarCharValue)
					if err != nil {
						return
					}
				}
			}
			if err = writer.Write(values); err != nil {
				return
			}
		}
		next = out.NextToken
		if next == "" {
			break
		}
		digest := sha256.Sum256([]byte(next))
		if len(next) > 1024 || seen[digest] {
			return stats, query.NewError("QUERY_FAILED", "Athena returned invalid pagination")
		}
		seen[digest] = true
	}
	if err = ctx.Err(); err != nil {
		return
	}
	finished, err := writer.Finish()
	stats.Rows, stats.Bytes, stats.Batches = finished.Rows, finished.Bytes, finished.Batches
	return
}
