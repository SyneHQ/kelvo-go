// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package trino implements bounded Trino and Presto statement-protocol reads.
package trino

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
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
	source  catalog.Source
	limits  query.Limits
	client  *cloudapi.Client
	headers map[string]string
}

var safeSegment = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var safeContext = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,127}$`)

func New(config catalog.Config, limits query.Limits) (*Engine, error) {
	var source catalog.Source
	count := 0
	for _, candidate := range config.Sources {
		if candidate.Type == "trino" || candidate.Type == "presto" {
			source = candidate
			count++
		}
	}
	if count != 1 || !catalog.ValidID(source.ID) {
		return nil, query.NewError("CONFIGURATION_ERROR", "Trino/Presto requires one selected source")
	}
	for _, name := range []string{source.URLEnv, source.TokenEnv, source.UsernameEnv} {
		if name == "" || catalog.ValidateEnvironment(name) != nil {
			return nil, query.NewError("CONFIGURATION_ERROR", "Trino/Presto requires URL, token and username environment references")
		}
	}
	username := os.Getenv(source.UsernameEnv)
	if username == "" || len(username) > 256 || strings.TrimSpace(username) != username || strings.ContainsAny(username, "\r\n\x00\t") {
		return nil, query.NewError("CONFIGURATION_ERROR", "Trino/Presto username is unavailable or invalid")
	}
	prefix := "X-Trino-"
	if source.Type == "presto" {
		prefix = "X-Presto-"
	}
	headers := map[string]string{prefix + "User": username, prefix + "Source": "kelvo-go", prefix + "Time-Zone": "UTC"}
	if source.Type == "trino" {
		headers[prefix+"Client-Capabilities"] = "PARAMETRIC_DATETIME"
	}
	for key, value := range source.Options {
		if (key != "catalog" && key != "schema") || !safeContext.MatchString(value) {
			return nil, query.NewError("CONFIGURATION_ERROR", "Trino/Presto only accepts simple catalog and schema options")
		}
		if key == "catalog" {
			headers[prefix+"Catalog"] = value
		} else {
			headers[prefix+"Schema"] = value
		}
	}
	client, err := cloudapi.New(source, limits)
	if err != nil {
		return nil, err
	}
	return &Engine{source: source, limits: limits, client: client, headers: headers}, nil
}

func (e *Engine) Close() error { e.client.Close(); return nil }

type column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type response struct {
	ID          string          `json:"id"`
	Next        string          `json:"nextUri"`
	Columns     []column        `json:"columns"`
	Data        [][]any         `json:"data"`
	Error       json.RawMessage `json:"error"`
	BinaryData  json.RawMessage `json:"binaryData"`
	UpdateType  string          `json:"updateType"`
	UpdateCount *json.Number    `json:"updateCount"`
}

func (e *Engine) Execute(parent context.Context, request query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	stats.Backend, stats.EngineStreaming = e.source.Type, true
	defer func() {
		stats.DurationNS = time.Since(started).Nanoseconds()
		if err != nil {
			err = query.PublicError(err)
		}
	}()
	if sink == nil || request.Mode != "native" || request.ConnectionID != e.source.ID || len(request.Sources) != 0 {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if request.Mongo != nil || len(request.Parameters) != 0 {
		return stats, query.NewError("UNSUPPORTED", "Trino/Presto requires SQL without parameters")
	}
	sql, err := sqlguard.ReadOnly(request.SQL)
	if err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	var result response
	status, wire, err := e.client.DoText(ctx, http.MethodPost, "/v1/statement", sql, e.headers, &result)
	stats.WireBytes = wire
	if err != nil {
		return stats, err
	}
	if status != http.StatusOK || !safeSegment.MatchString(result.ID) {
		return stats, query.NewError("QUERY_FAILED", "Trino/Presto returned an invalid query handle")
	}
	id := result.ID
	cleanupPath := ""
	defer func() {
		if err != nil && cleanupPath != "" {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, _, _ = e.client.Do(cleanup, http.MethodDelete, cleanupPath, nil, e.headers, nil)
		}
	}()
	var schema *arrow.Schema
	var writer *rowarrow.Writer
	defer func() {
		if writer != nil {
			writer.Close()
		}
	}()
	seen := map[[32]byte]bool{}
	for pages := 0; ; pages++ {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		if result.ID != id {
			return stats, query.NewError("QUERY_FAILED", "Trino/Presto changed the query handle")
		}
		next := ""
		if result.Next != "" {
			next, err = e.nextPath(result.Next, id)
			if err != nil {
				return stats, err
			}
			cleanupPath = next
		}
		if nonNullJSON(result.BinaryData) {
			return stats, query.NewError("UNSUPPORTED", "Trino/Presto binary or spooled results are unsupported")
		}
		if nonNullJSON(result.Error) || result.UpdateType != "" || result.UpdateCount != nil {
			return stats, query.NewError("QUERY_FAILED", "Trino/Presto query failed or returned a mutation result")
		}
		if len(result.Columns) > 0 {
			incoming, er := resultSchema(result.Columns)
			if er != nil {
				return stats, er
			}
			if schema != nil && !schema.Equal(incoming) {
				return stats, query.NewError("QUERY_FAILED", "Trino/Presto changed the result schema")
			}
			if schema == nil {
				schema = incoming
				writer, err = rowarrow.NewWriter(schema, e.limits, sink)
				if err != nil {
					return stats, err
				}
				stats.PrepareNS = time.Since(started).Nanoseconds()
			}
		}
		if len(result.Data) > 0 && writer == nil {
			return stats, query.NewError("QUERY_FAILED", "Trino/Presto returned data without a schema")
		}
		for _, values := range result.Data {
			if err = ctx.Err(); err != nil {
				return stats, err
			}
			row, er := resultRow(schema, values)
			if er != nil {
				return stats, er
			}
			if err = writer.Write(row); err != nil {
				return stats, err
			}
		}
		if next == "" {
			cleanupPath = ""
			break
		}
		key := sha256.Sum256([]byte(next))
		if seen[key] || pages >= 8191 {
			return stats, query.NewError("QUERY_FAILED", "Trino/Presto pagination repeated or exceeded its limit")
		}
		seen[key] = true
		if len(result.Data) == 0 {
			if err = cloudapi.Poll(ctx); err != nil {
				return stats, err
			}
		}
		var page response
		status, wire, err = e.client.Do(ctx, http.MethodGet, next, nil, e.headers, &page)
		stats.WireBytes += wire
		if err != nil {
			return stats, err
		}
		if status != http.StatusOK {
			return stats, query.NewError("QUERY_FAILED", "Trino/Presto returned an invalid page status")
		}
		result = page
	}
	if writer == nil {
		return stats, query.NewError("QUERY_FAILED", "Trino/Presto completed without a result schema")
	}
	delivered, err := writer.Finish()
	stats.Rows, stats.Bytes, stats.Batches = delivered.Rows, delivered.Bytes, delivered.Batches
	return stats, err
}

func nonNullJSON(value json.RawMessage) bool {
	return len(value) > 0 && strings.TrimSpace(string(value)) != "null"
}

func (e *Engine) nextPath(raw, id string) (string, error) {
	deny := func() (string, error) {
		return "", query.NewError("QUERY_FAILED", "Trino/Presto returned an invalid pagination location")
	}
	if len(raw) > 2048 {
		return deny()
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || !strings.EqualFold(u.Host, e.client.Origin.Host) || u.User != nil || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || strings.Contains(u.EscapedPath(), "%") {
		return deny()
	}
	parts := strings.Split(u.Path, "/")
	if len(parts) < 6 || parts[0] != "" || parts[1] != "v1" || parts[2] != "statement" || (parts[3] != "queued" && parts[3] != "executing") || parts[4] != id {
		return deny()
	}
	if e.source.Type == "trino" {
		if len(parts) != 7 || !safeSegment.MatchString(parts[5]) || u.RawQuery != "" {
			return deny()
		}
	} else {
		if len(parts) != 6 {
			return deny()
		}
		params, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			return deny()
		}
		for name, values := range params {
			if name != "slug" || len(values) != 1 || !safeSegment.MatchString(values[0]) {
				return deny()
			}
		}
	}
	token := parts[len(parts)-1]
	if token == "" || len(token) > 19 || strings.Trim(token, "0123456789") != "" {
		return deny()
	}
	if _, err := strconv.ParseInt(token, 10, 64); err != nil {
		return deny()
	}
	return u.RequestURI(), nil
}
