// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func readPipeline(request *query.MongoRequest, maxRows int64) (mongo.Pipeline, error) {
	if request == nil || request.Collection == "" || len(request.Collection) > 120 || !utf8.ValidString(request.Collection) || strings.ContainsAny(request.Collection, "$\x00") || strings.HasPrefix(request.Collection, "system.") || len(request.Pipeline) > 128 {
		return nil, query.NewError("INVALID_ARGUMENT", "MongoDB collection or pipeline is invalid")
	}
	pipeline := make(mongo.Pipeline, 0, len(request.Pipeline)+1)
	total := 0
	for _, encoded := range request.Pipeline {
		total += len(encoded)
		if total > 128<<10 {
			return nil, query.NewError("INVALID_ARGUMENT", "MongoDB pipeline exceeds size limit")
		}
		if err := validateJSONDepth(encoded); err != nil {
			return nil, err
		}
		var stage bson.D
		if err := bson.UnmarshalExtJSON(encoded, false, &stage); err != nil || len(stage) != 1 || !strings.HasPrefix(stage[0].Key, "$") {
			return nil, query.NewError("INVALID_ARGUMENT", "MongoDB requires one operator per pipeline stage")
		}
		if err := validateBSON(stage, 0); err != nil {
			return nil, err
		}
		pipeline = append(pipeline, stage)
	}
	// Read one extra result to distinguish overflow from successful completion.
	pipeline = append(pipeline, bson.D{{Key: "$limit", Value: maxRows + 1}})
	return pipeline, nil
}

// Bound nesting before the Extended JSON decoder allocates nested BSON values.
// UseNumber prevents a canonical BSON number from passing through float64.
func validateJSONDepth(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	depth := 0
	for {
		token, err := decoder.Token()
		if err == io.EOF && depth == 0 {
			return nil
		}
		if err != nil {
			return query.NewError("INVALID_ARGUMENT", "Invalid MongoDB pipeline JSON")
		}
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
			if depth > 64 {
				return query.NewError("INVALID_ARGUMENT", "MongoDB pipeline exceeds nesting limit")
			}
		}
	}
}

func validateBSON(value any, depth int) error {
	if depth > 64 {
		return query.NewError("INVALID_ARGUMENT", "MongoDB pipeline exceeds nesting limit")
	}
	switch value := value.(type) {
	case bson.D:
		for _, field := range value {
			switch field.Key {
			case "$out", "$merge", "$where", "$function", "$accumulator", "$changeStream":
				return query.NewError("PERMISSION_DENIED", "MongoDB writes, executable JavaScript and change streams are unavailable")
			}
			if err := validateBSON(field.Value, depth+1); err != nil {
				return err
			}
		}
	case bson.A:
		for _, item := range value {
			if err := validateBSON(item, depth+1); err != nil {
				return err
			}
		}
	case bson.JavaScript, bson.CodeWithScope:
		return query.NewError("PERMISSION_DENIED", "MongoDB executable JavaScript is unavailable")
	}
	return nil
}
