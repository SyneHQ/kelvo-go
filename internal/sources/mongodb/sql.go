// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"encoding/json"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/synehq/zero-sql/pkg/zerosql"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func sqlRequest(request query.Request) (*query.MongoRequest, error) {
	if request.Mongo != nil || len(request.Parameters) != 0 {
		return nil, query.NewError("UNSUPPORTED", "MongoDB SQL requires SQL without parameters or an aggregation payload")
	}
	// Use only the additive raw-parser API: the legacy converter rewrites CAST
	// expressions and supports constructs whose SQL semantics are unverified.
	converted, err := zerosql.New(nil).ConvertReadOnlySQLToMongoWithCollection(request.SQL)
	if err != nil || converted == nil {
		return nil, query.NewError("UNSUPPORTED", "MongoDB SQL syntax is unsupported; use a supported SELECT or native aggregation")
	}
	command := &query.MongoRequest{Collection: converted.Collection, Pipeline: make([]json.RawMessage, 0, len(converted.Pipeline))}
	for _, stage := range converted.Pipeline {
		normalized, err := exactSQLNumbers(stage)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(normalized)
		if err != nil {
			return nil, query.NewError("UNSUPPORTED", "MongoDB SQL could not be represented as a pipeline")
		}
		command.Pipeline = append(command.Pipeline, encoded)
	}
	// The caller always runs readPipeline next. Converted SQL receives the same
	// collection, JavaScript/write, stage count, byte and depth gates as raw input.
	return command, nil
}

func exactSQLNumbers(value any) (any, error) {
	switch value := value.(type) {
	case json.Number:
		// A SQL decimal literal must not become binary float64 through JSON.
		if _, err := bson.ParseDecimal128(value.String()); err != nil {
			return nil, query.NewError("UNSUPPORTED", "MongoDB SQL decimal exceeds Decimal128 precision or range")
		}
		return map[string]string{"$numberDecimal": value.String()}, nil
	case float32, float64:
		return nil, query.NewError("UNSUPPORTED", "MongoDB SQL conversion produced an inexact numeric literal")
	case map[string]interface{}:
		out := make(map[string]interface{}, len(value))
		for key, item := range value {
			normalized, err := exactSQLNumbers(item)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	case []map[string]interface{}:
		out := make([]interface{}, len(value))
		for i, item := range value {
			normalized, err := exactSQLNumbers(item)
			if err != nil {
				return nil, err
			}
			out[i] = normalized
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, len(value))
		for i, item := range value {
			normalized, err := exactSQLNumbers(item)
			if err != nil {
				return nil, err
			}
			out[i] = normalized
		}
		return out, nil
	default:
		return value, nil
	}
}
