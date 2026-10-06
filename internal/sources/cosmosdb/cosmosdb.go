// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package cosmosdb reads bounded Cosmos DB for NoSQL query pages as exact JSON.
package cosmosdb

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"regexp"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/apache/arrow-go/v18/arrow"
)

type Engine struct {
	source          catalog.Source
	limits          query.Limits
	client          *cloudapi.Client
	resource        string
	key             []byte
	maxPages        int
	maxRequestUnits int64
	now             func() time.Time
}

var component = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var positiveInteger = regexp.MustCompile(`^[1-9][0-9]*$`)
var chargeNumber = regexp.MustCompile(`^[0-9]+(?:\.[0-9]{1,6})?$`)

func New(c catalog.Config, limits query.Limits) (*Engine, error) {
	return newEngine(c, limits, cloudapi.New)
}
func NewResolved(c catalog.Config, limits query.Limits, credentials cloudapi.Credentials) (*Engine, error) {
	return newEngine(c, limits, func(s catalog.Source, l query.Limits) (*cloudapi.Client, error) {
		return cloudapi.NewResolved(s, l, credentials)
	})
}
func NewDiscoveryResolved(c catalog.Config, limits query.Limits, credentials cloudapi.Credentials) (*Engine, error) {
	return newConfigured(c, limits, func(s catalog.Source, l query.Limits) (*cloudapi.Client, error) {
		return cloudapi.NewResolved(s, l, credentials)
	}, false)
}
func newEngine(c catalog.Config, limits query.Limits, open cloudapi.Factory) (*Engine, error) {
	return newConfigured(c, limits, open, true)
}
func newConfigured(c catalog.Config, limits query.Limits, open cloudapi.Factory, requireContainer bool) (*Engine, error) {
	source, err := cloudapi.SingleSource(c, "cosmosdb")
	if err != nil {
		return nil, err
	}
	if !component.MatchString(source.Options["database"]) || (requireContainer || source.Options["container"] != "") && !component.MatchString(source.Options["container"]) {
		return nil, query.NewError("CONFIGURATION_ERROR", "Cosmos DB requires valid database and container options")
	}
	if source.Options["auth"] != "aad" && source.Options["auth"] != "master_key" {
		return nil, query.NewError("CONFIGURATION_ERROR", "Cosmos DB auth must be aad or master_key")
	}
	maxPages, maxRU := 1000, int64(10000)
	for key, value := range source.Options {
		switch key {
		case "database", "container", "auth":
		case "max_pages", "max_request_units":
			n, parseErr := strconv.ParseInt(value, 10, 64)
			limit := int64(10000)
			if key == "max_request_units" {
				limit = 1000000
			}
			if parseErr != nil || !positiveInteger.MatchString(value) || n > limit {
				return nil, query.NewError("CONFIGURATION_ERROR", "Invalid Cosmos DB query budget")
			}
			if key == "max_pages" {
				maxPages = int(n)
			} else {
				maxRU = n
			}
		default:
			return nil, query.NewError("CONFIGURATION_ERROR", "Unknown Cosmos DB option")
		}
	}
	client, err := open(source, limits)
	if err != nil {
		return nil, err
	}
	var key []byte
	if source.Options["auth"] == "master_key" {
		key, err = base64.StdEncoding.Strict().DecodeString(client.Token)
		if err != nil || len(key) < 32 || len(key) > 256 {
			client.Close()
			return nil, query.NewError("CONFIGURATION_ERROR", "Cosmos DB master key must be valid base64 key material")
		}
	}
	return &Engine{source: source, limits: limits, client: client, resource: "dbs/" + source.Options["database"] + "/colls/" + source.Options["container"], key: key, maxPages: maxPages, maxRequestUnits: maxRU, now: time.Now}, nil
}

func (e *Engine) Close() error { e.client.Close(); return nil }

type queryPage struct {
	Count     *int64          `json:"_count"`
	Documents json.RawMessage `json:"Documents"`
}

func (e *Engine) Execute(parent context.Context, request query.Request, sink query.Sink) (stats query.Stats, err error) {
	if !component.MatchString(e.source.Options["container"]) {
		return stats, query.NewError("INVALID_ARGUMENT", "Cosmos query requires a selected container")
	}
	started := time.Now()
	defer func() { stats.Backend = "cosmosdb"; stats.DurationNS = time.Since(started).Nanoseconds() }()
	if request.Mode != "native" || request.ConnectionID != e.source.ID || len(request.Sources) != 0 || sink == nil {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if err = query.ValidateRequest(request); err != nil {
		return stats, err
	}
	if request.Mongo != nil || len(request.Parameters) != 0 {
		return stats, query.NewError("UNSUPPORTED", "Cosmos DB requires SQL without parameters")
	}
	sql, err := simpleQuery(request.SQL)
	if err != nil {
		return stats, err
	}
	body, err := json.Marshal(map[string]any{"query": sql, "parameters": []any{}})
	if err != nil {
		return stats, query.NewError("INVALID_ARGUMENT", "Invalid Cosmos DB query")
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	schema := arrow.NewSchema([]arrow.Field{{Name: "document", Type: arrow.BinaryTypes.Binary, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"source_type": "cosmosdb", "native_type": "JSON", "encoding": "json", "representation": "source_json_value"})}}, nil)
	writer, err := rowarrow.NewWriter(schema, e.limits, sink)
	if err != nil {
		return stats, err
	}
	defer writer.Close()
	seen := make(map[[32]byte]struct{})
	continuation, sessionToken := "", ""
	usedRU := new(big.Rat)
	budgetRU := new(big.Rat).SetInt64(e.maxRequestUnits)
	pageSize := min(int64(1000), e.limits.MaxRows+1)
	for pageNumber := 1; ; pageNumber++ {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		if pageNumber > e.maxPages {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Cosmos DB page budget exceeded")
		}
		var page queryPage
		headers, received, callErr := e.query(ctx, body, pageSize, continuation, sessionToken, &page)
		stats.WireBytes += received
		if callErr != nil {
			return stats, callErr
		}
		charge, chargeErr := singleHeader(headers, "x-ms-request-charge")
		if chargeErr != nil || len(charge) > 20 || !chargeNumber.MatchString(charge) {
			return stats, query.NewError("QUERY_FAILED", "Cosmos DB returned invalid request charge")
		}
		ru, ok := new(big.Rat).SetString(charge)
		if !ok {
			return stats, query.NewError("QUERY_FAILED", "Cosmos DB returned invalid request charge")
		}
		usedRU.Add(usedRU, ru)
		if usedRU.Cmp(budgetRU) > 0 {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Cosmos DB request unit budget exceeded")
		}
		if page.Count == nil || *page.Count < 0 || *page.Count > pageSize || len(page.Documents) < 2 || page.Documents[0] != '[' {
			return stats, query.NewError("QUERY_FAILED", "Cosmos DB returned invalid query results")
		}
		var documents []json.RawMessage
		if json.Unmarshal(page.Documents, &documents) != nil || int64(len(documents)) != *page.Count {
			return stats, query.NewError("QUERY_FAILED", "Cosmos DB result count is inconsistent")
		}
		if pageNumber == 1 {
			stats.PrepareNS = time.Since(started).Nanoseconds()
		}
		for _, document := range documents {
			if err = ctx.Err(); err != nil {
				return stats, err
			}
			// RawMessage retains numeric lexemes, nested structure, and JSON null;
			// JSON null is a JSON value, not an absent Arrow cell.
			if err = writer.Write([]any{[]byte(document)}); err != nil {
				return stats, err
			}
		}
		next, headerErr := singleHeader(headers, "x-ms-continuation")
		if headerErr != nil {
			return stats, headerErr
		}
		latestSession, headerErr := singleHeader(headers, "x-ms-session-token")
		if headerErr != nil {
			return stats, headerErr
		}
		if latestSession != "" {
			sessionToken = latestSession
		}
		if next == "" {
			break
		}
		hash := sha256.Sum256([]byte(next))
		if _, exists := seen[hash]; exists {
			return stats, query.NewError("QUERY_FAILED", "Cosmos DB repeated a continuation token")
		}
		seen[hash] = struct{}{}
		continuation = next
	}
	written, err := writer.Finish()
	stats.Rows, stats.Bytes, stats.Batches = written.Rows, written.Bytes, written.Batches
	return stats, err
}
