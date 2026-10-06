// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package dynamodb executes read-only PartiQL and preserves AttributeValue JSON.
package dynamodb

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"regexp"
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

func New(c catalog.Config, l query.Limits) (*Engine, error) { return newEngine(c, l, awsapi.New) }

func NewResolved(c catalog.Config, l query.Limits, credentials awsapi.Credentials) (*Engine, error) {
	return newEngine(c, l, func(s catalog.Source, l query.Limits, service string) (*awsapi.Client, error) {
		return awsapi.NewResolved(s, l, service, credentials)
	})
}
func newEngine(c catalog.Config, l query.Limits, open awsapi.Factory) (*Engine, error) {
	s, err := cloudapi.SingleSource(c, "dynamodb")
	if err != nil {
		return nil, err
	}
	client, err := open(s, l, "dynamodb")
	if err != nil {
		return nil, err
	}
	return &Engine{s, l, client}, nil
}
func (e *Engine) Close() error { e.c.Close(); return nil }

var selectStart = regexp.MustCompile(`(?is)^(?:\s|--[^\n]*(?:\n|$)|/\*.*?\*/)*select\b`)
var documentSchema = arrow.NewSchema([]arrow.Field{{Name: "document", Type: arrow.BinaryTypes.Binary, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"encoding": "dynamodb-attributevalue-json"})}}, nil)

type response struct {
	Items            []json.RawMessage          `json:"Items"`
	NextToken        string                     `json:"NextToken"`
	LastEvaluatedKey map[string]json.RawMessage `json:"LastEvaluatedKey"`
}

func (e *Engine) Execute(parent context.Context, r query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	defer func() {
		stats.Backend = "dynamodb"
		stats.DurationNS = time.Since(started).Nanoseconds()
		stats.EngineStreaming = true
	}()
	if err = query.ValidateRequest(r); err != nil {
		return
	}
	if r.Mode != "native" || r.ConnectionID != e.s.ID || sink == nil {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if len(r.Parameters) > 0 || r.Mongo != nil {
		return stats, query.NewError("UNSUPPORTED", "DynamoDB requires PartiQL without parameters")
	}
	sql, err := sqlguard.ReadOnly(r.SQL)
	if err != nil {
		return
	}
	if len(sql) > 8192 || !selectStart.MatchString(sql) {
		return stats, query.NewError("INVALID_ARGUMENT", "DynamoDB requires one PartiQL SELECT statement of at most 8192 bytes")
	}
	ctx, cancel := context.WithTimeout(parent, e.l.Timeout)
	defer cancel()
	if err = ctx.Err(); err != nil {
		return
	}
	writer, err := rowarrow.NewWriter(documentSchema, e.l, sink)
	if err != nil {
		return
	}
	defer writer.Close()
	next := ""
	seen := map[[32]byte]bool{}
	for page := 0; ; page++ {
		if page >= 10000 {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "DynamoDB pagination exceeds limit")
		}
		body := map[string]any{"Statement": sql, "Limit": min(int64(1000), e.l.MaxRows+1), "ReturnConsumedCapacity": "NONE"}
		if next != "" {
			body["NextToken"] = next
		}
		var out response
		n, callErr := e.c.Do(ctx, "DynamoDB_20120810.ExecuteStatement", body, &out)
		budgetErr := awsapi.Account(&stats, n, e.l)
		if callErr != nil {
			return stats, callErr
		}
		if budgetErr != nil {
			return stats, budgetErr
		}
		if page == 0 {
			stats.PrepareNS = time.Since(started).Nanoseconds()
		}
		// ExecuteStatement has no ExclusiveStartKey request field. Never invent a
		// continuation or quietly return a partial result if no token was supplied.
		if len(out.LastEvaluatedKey) > 0 && out.NextToken == "" {
			return stats, query.NewError("UNSUPPORTED", "DynamoDB returned LastEvaluatedKey without an executable NextToken")
		}
		for _, item := range out.Items {
			if err = ctx.Err(); err != nil {
				return
			}
			if !validDocument(item) {
				return stats, query.NewError("QUERY_FAILED", "DynamoDB returned an invalid AttributeValue document")
			}
			// Raw bytes retain numbers as exact strings, typed NULL, binary base64,
			// nested lists/maps and sets, even when documents have different shapes.
			if err = writer.Write([]any{[]byte(item)}); err != nil {
				return
			}
		}
		next = out.NextToken
		if next == "" {
			break
		}
		digest := sha256.Sum256([]byte(next))
		if len(next) > 32768 || seen[digest] {
			return stats, query.NewError("QUERY_FAILED", "DynamoDB returned invalid pagination")
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
