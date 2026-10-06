// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package d1 implements bounded Cloudflare D1 raw-result API queries.
package d1

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

var accountID = regexp.MustCompile(`^[a-fA-F0-9]{32}$`)
var databaseID = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)

type Engine struct {
	sourceID string
	path     string
	limits   query.Limits
	client   *cloudapi.Client
}

func New(config catalog.Config, limits query.Limits) (*Engine, error) {
	return newEngine(config, limits, func(s catalog.Source, l query.Limits) (*cloudapi.Client, error) {
		if catalog.ValidateEnvironment(s.URLEnv) != nil || catalog.ValidateEnvironment(s.TokenEnv) != nil {
			return nil, query.NewError("CONFIGURATION_ERROR", "D1 requires permitted credential references")
		}
		return cloudapi.New(s, l)
	})
}

// NewResolved opens one request-owned source without reading ambient credentials.
func NewResolved(config catalog.Config, limits query.Limits, credentials cloudapi.Credentials) (*Engine, error) {
	return newEngine(config, limits, func(s catalog.Source, l query.Limits) (*cloudapi.Client, error) {
		return cloudapi.NewResolved(s, l, credentials)
	})
}

func newEngine(config catalog.Config, limits query.Limits, open cloudapi.Factory) (*Engine, error) {
	source, err := cloudapi.SingleSource(config, "d1")
	if err != nil {
		return nil, err
	}
	if !catalog.ValidID(source.ID) || !accountID.MatchString(source.Options["account_id"]) || !databaseID.MatchString(source.Options["database_id"]) {
		return nil, query.NewError("CONFIGURATION_ERROR", "D1 requires a source ID, account ID, database ID and permitted credential references")
	}
	client, err := open(source, limits)
	if err != nil {
		return nil, err
	}
	// /raw preserves SQL column order, duplicate labels, and empty-result
	client.Limit = min(client.Limit, limits.MaxBytes)
	// metadata without creating one Go map for every returned row.
	path := "/client/v4/accounts/" + source.Options["account_id"] + "/d1/database/" + source.Options["database_id"] + "/raw"
	return &Engine{sourceID: source.ID, path: path, limits: limits, client: client}, nil
}

func (e *Engine) Close() error { e.client.Close(); return nil }

type response struct {
	Success bool              `json:"success"`
	Errors  []json.RawMessage `json:"errors"`
	Result  []struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
		Results struct {
			Columns []string `json:"columns"`
			Rows    [][]any  `json:"rows"`
		} `json:"results"`
	} `json:"result"`
}

func (e *Engine) Execute(parent context.Context, request query.Request, sink query.Sink) (query.Stats, error) {
	if err := query.ValidateRequest(request); err != nil {
		return query.Stats{}, err
	}
	return e.ExecuteBound(parent, request, cloudapi.OperationParameters(request.Parameters), sink)
}

func (e *Engine) ExecuteBound(parent context.Context, request query.Request, parameters []operations.Parameter, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	stats.Backend = "d1"
	// The API returns a bounded JSON result before Arrow delivery starts.
	stats.EngineStreaming = false
	defer func() { stats.DurationNS = time.Since(started).Nanoseconds() }()
	request.Parameters = nil
	if err := query.ValidateRequest(request); err != nil {
		return stats, err
	}
	if request.Mode != "native" || request.ConnectionID != e.sourceID || sink == nil {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if request.Mongo != nil {
		return stats, query.NewError("UNSUPPORTED", "D1 native queries require SQL")
	}
	bound, err := cloudapi.D1Parameters(parameters)
	if err != nil {
		return stats, err
	}
	sql, err := sqlguard.ReadOnly(request.SQL)
	if err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	boundedSQL := "SELECT * FROM (" + sql + "\n) AS kelvo_result LIMIT " + strconv.FormatInt(e.limits.MaxRows+1, 10)
	var result response
	body := map[string]any{"sql": boundedSQL}
	if len(bound) != 0 {
		body["params"] = bound
	}
	_, wire, err := e.client.Do(ctx, http.MethodPost, e.path, body, nil, &result)
	stats.WireBytes = wire
	if err != nil {
		return stats, query.PublicError(err)
	}
	if !result.Success || len(result.Errors) != 0 || len(result.Result) != 1 || !result.Result[0].Success || result.Result[0].Error != "" {
		return stats, query.NewError("QUERY_FAILED", "D1 statement did not succeed as one result set")
	}
	data := result.Result[0].Results
	if int64(len(data.Rows)) > e.limits.MaxRows {
		return stats, query.NewError("RESOURCE_EXHAUSTED", "D1 result exceeds row limit")
	}
	schema, err := resultSchema(ctx, data.Columns, data.Rows)
	if err != nil {
		return stats, err
	}
	stats.PrepareNS = time.Since(started).Nanoseconds()
	writer, err := rowarrow.NewWriter(schema, e.limits, sink)
	if err != nil {
		return stats, query.PublicError(err)
	}
	defer writer.Close()
	for _, input := range data.Rows {
		if err := ctx.Err(); err != nil {
			return stats, query.PublicError(err)
		}
		row := make([]any, len(input))
		for i, value := range input {
			row[i], err = convert(value, schema.Field(i).Type)
			if err != nil {
				return stats, err
			}
		}
		if err := writer.Write(row); err != nil {
			return stats, query.PublicError(err)
		}
	}
	delivered, err := writer.Finish()
	stats.Rows, stats.Bytes, stats.Batches = delivered.Rows, delivered.Bytes, delivered.Batches
	if err != nil {
		return stats, query.PublicError(err)
	}
	return stats, nil
}

func resultSchema(ctx context.Context, columns []string, rows [][]any) (*arrow.Schema, error) {
	if len(columns) < 1 || len(columns) > 4096 {
		return nil, invalidResult()
	}
	fields := make([]arrow.Field, len(columns))
	for i, name := range columns {
		if len(name) > 1024 {
			return nil, invalidResult()
		}
		fields[i] = arrow.Field{Name: name, Type: arrow.Null, Nullable: true}
	}
	// SQLite columns can change type across rows. Inspect the complete bounded
	// response before emitting a schema; never convert a later mismatch to NULL.
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, query.PublicError(err)
		}
		if len(row) != len(fields) {
			return nil, invalidResult()
		}
		for i, value := range row {
			typ, err := valueType(value)
			if err != nil {
				return nil, err
			}
			if fields[i].Type.ID() == arrow.NULL {
				fields[i].Type = typ
				continue
			}
			if typ.ID() == arrow.NULL || typ.ID() == fields[i].Type.ID() {
				continue
			}
			if (typ.ID() == arrow.INT64 && fields[i].Type.ID() == arrow.FLOAT64) || (typ.ID() == arrow.FLOAT64 && fields[i].Type.ID() == arrow.INT64) {
				fields[i].Type = arrow.PrimitiveTypes.Float64
				continue
			}
			return nil, query.NewError("UNSUPPORTED", "D1 returned heterogeneous column types; use explicit SQL casts")
		}
	}
	// Validate numeric promotions before publishing any result. Other values
	// were checked above; do not allocate binary copies during schema analysis.
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, query.PublicError(err)
		}
		for i, value := range row {
			if fields[i].Type.ID() == arrow.FLOAT64 {
				if _, err := convert(value, fields[i].Type); err != nil {
					return nil, err
				}
			}
		}
	}
	return arrow.NewSchema(fields, nil), nil
}

func valueType(value any) (arrow.DataType, error) {
	switch value := value.(type) {
	case nil:
		return arrow.Null, nil
	case bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case string:
		return arrow.BinaryTypes.String, nil
	case json.Number:
		if _, err := strconv.ParseInt(value.String(), 10, 64); err == nil {
			return arrow.PrimitiveTypes.Int64, nil
		}
		// A JSON integer outside int64 must not silently round into a float.
		if !containsFraction(value.String()) {
			return nil, query.NewError("UNSUPPORTED", "D1 integer exceeds int64; select it as text")
		}
		if number, err := strconv.ParseFloat(value.String(), 64); err == nil && !math.IsNaN(number) && !math.IsInf(number, 0) {
			return arrow.PrimitiveTypes.Float64, nil
		}
		return nil, invalidResult()
	case []any:
		for _, item := range value {
			if _, err := binaryByte(item); err != nil {
				return nil, err
			}
		}
		return arrow.BinaryTypes.Binary, nil
	default:
		return nil, query.NewError("UNSUPPORTED", "D1 returned an unsupported value type")
	}
}

func containsFraction(value string) bool {
	for _, c := range value {
		if c == '.' || c == 'e' || c == 'E' {
			return true
		}
	}
	return false
}

func convert(value any, typ arrow.DataType) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch typ.ID() {
	case arrow.BOOL:
		if b, ok := value.(bool); ok {
			return b, nil
		}
	case arrow.STRING:
		if s, ok := value.(string); ok {
			return s, nil
		}
	case arrow.INT64:
		if number, ok := value.(json.Number); ok {
			if integer, err := strconv.ParseInt(number.String(), 10, 64); err == nil {
				return integer, nil
			}
		}
	case arrow.FLOAT64:
		if number, ok := value.(json.Number); ok {
			if integer, err := strconv.ParseInt(number.String(), 10, 64); err == nil && (integer > 1<<53 || integer < -(1<<53)) {
				return nil, query.NewError("UNSUPPORTED", "D1 numeric promotion could lose integer precision; use explicit SQL casts")
			}
			if f, err := strconv.ParseFloat(number.String(), 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
				return f, nil
			}
		}
	case arrow.BINARY:
		if items, ok := value.([]any); ok {
			b := make([]byte, len(items))
			for i, item := range items {
				value, err := binaryByte(item)
				if err != nil {
					return nil, err
				}
				b[i] = value
			}
			return b, nil
		}
	}
	return nil, invalidResult()
}

func binaryByte(item any) (byte, error) {
	number, ok := item.(json.Number)
	if !ok {
		return 0, invalidResult()
	}
	b, err := strconv.ParseUint(number.String(), 10, 8)
	if err != nil {
		return 0, invalidResult()
	}
	return byte(b), nil
}

func invalidResult() error { return query.NewError("QUERY_FAILED", "D1 returned invalid result data") }
