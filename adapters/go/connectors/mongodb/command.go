// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
	native "github.com/SYNEHQ/kelvo-go/internal/sources/mongodb"
	"github.com/SYNEHQ/kelvo-go/operations"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type commandInput struct {
	Collection string            `json:"collection"`
	Filter     json.RawMessage   `json:"filter,omitempty"`
	Projection json.RawMessage   `json:"projection,omitempty"`
	Sort       json.RawMessage   `json:"sort,omitempty"`
	Pipeline   []json.RawMessage `json:"pipeline,omitempty"`
	Document   json.RawMessage   `json:"document,omitempty"`
	Documents  []json.RawMessage `json:"documents,omitempty"`
	Update     json.RawMessage   `json:"update,omitempty"`
	Name       string            `json:"name,omitempty"`
	Keys       json.RawMessage   `json:"keys,omitempty"`
	Unique     *bool             `json:"unique,omitempty"`
	Upsert     *bool             `json:"upsert,omitempty"`
}

type commandPlan struct {
	command, collection, name string
	pipeline                  mongo.Pipeline
	filter, document, update  bson.D
	documents                 []any
	keys                      bson.D
	unique, upsert            bool
}

func parseCommand(request adapter.Native) (commandPlan, error) {
	p := commandPlan{command: request.Spec.Command}
	if request.Validate() != nil || request.Spec.Provider != "mongodb" || len(request.Spec.Parameters) != 1 || request.Spec.Parameters[0].Type != "json" {
		return p, adapter.ErrInvalid
	}
	raw := request.Spec.Parameters[0].Value
	var input commandInput
	if operations.DecodeStrict(raw, &input, 16<<10) != nil || !validCollection(input.Collection) {
		return p, adapter.ErrInvalid
	}
	// Reject irrelevant fields too: a read can never smuggle write options, and
	// a caller does not silently lose a requested predicate or update option.
	allowed := map[string]bool{"collection": true}
	allow := func(keys ...string) {
		for _, key := range keys {
			allowed[key] = true
		}
	}
	read := false
	switch p.command {
	case "aggregate":
		read = true
		allow("pipeline")
	case "find", "find_one":
		read = true
		allow("filter", "projection", "sort")
	case "count":
		read = true
		allow("filter")
	case "list_indexes":
		read = true
	case "insert_one":
		allow("document")
	case "insert_many":
		allow("documents")
	case "update_one", "update_many":
		allow("filter", "update", "upsert")
	case "delete_one", "delete_many":
		allow("filter")
	case "create_collection", "drop_collection":
	case "create_index":
		allow("keys", "name", "unique")
	case "drop_index":
		allow("name")
	default:
		return p, adapter.ErrUnsupported
	}
	if read != (request.Kind == operations.NativeRead) {
		return p, adapter.ErrUnsupported
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return p, adapter.ErrInvalid
	}
	for key, value := range fields {
		if !allowed[key] || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return p, adapter.ErrInvalid
		}
	}
	p.collection, p.name = input.Collection, input.Name
	if input.Upsert != nil {
		p.upsert = *input.Upsert
	}
	if input.Unique != nil {
		p.unique = *input.Unique
	}
	var err error
	if input.Filter != nil {
		p.filter, err = document(input.Filter)
		if err != nil {
			return p, err
		}
	} else {
		p.filter = bson.D{}
	}
	if read {
		if p.command == "list_indexes" {
			return p, nil
		}
		pipeline := input.Pipeline
		if p.command != "aggregate" {
			pipeline = make([]json.RawMessage, 0, 5)
			if input.Filter != nil {
				pipeline = append(pipeline, stage("$match", input.Filter))
			}
			if input.Sort != nil {
				pipeline = append(pipeline, stage("$sort", input.Sort))
			}
			if input.Projection != nil {
				pipeline = append(pipeline, stage("$project", input.Projection))
			}
			if p.command == "find_one" {
				pipeline = append(pipeline, json.RawMessage(`{"$limit":1}`))
			}
			if p.command == "count" {
				pipeline = append(pipeline, json.RawMessage(`{"$count":"count"}`))
			}
		}
		for _, encoded := range pipeline {
			if exactJSONNumbers(encoded) != nil {
				return p, adapter.ErrInvalid
			}
		}
		p.pipeline, err = native.ReadPipeline(p.collection, pipeline, request.Limits.MaxRows)
		if err != nil {
			return p, adapter.ErrUnsupported
		}
		if err = readStages(p.pipeline, 0); err != nil {
			return p, err
		}
		return p, nil
	}
	switch p.command {
	case "insert_one":
		p.document, err = document(input.Document)
	case "insert_many":
		if len(input.Documents) < 1 || len(input.Documents) > 100 {
			return p, adapter.ErrInvalid
		}
		for _, raw := range input.Documents {
			var doc bson.D
			doc, err = document(raw)
			if err != nil {
				return p, err
			}
			p.documents = append(p.documents, doc)
		}
	case "update_one", "update_many":
		if input.Filter == nil {
			return p, adapter.ErrInvalid
		}
		p.update, err = document(input.Update)
		if err == nil {
			err = updateOperators(p.update)
		}
	case "delete_one", "delete_many":
		if input.Filter == nil {
			return p, adapter.ErrInvalid
		}
	case "create_index":
		p.keys, err = document(input.Keys)
		if err == nil {
			err = indexKeys(p.keys)
		}
		fallthrough
	case "drop_index":
		if p.name == "" || len(p.name) > 120 || strings.ContainsAny(p.name, "*$\x00\r\n") || p.name == "_id_" {
			return p, adapter.ErrInvalid
		}
	}
	return p, err
}

func stage(operator string, value json.RawMessage) json.RawMessage {
	key, _ := json.Marshal(operator)
	out := make(json.RawMessage, 0, len(key)+len(value)+3)
	out = append(out, '{')
	out = append(out, key...)
	out = append(out, ':')
	out = append(out, value...)
	return append(out, '}')
}

func exactJSONNumbers(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return adapter.ErrInvalid
		}
		if n, ok := token.(json.Number); ok {
			if _, err := strconv.ParseInt(n.String(), 10, 64); err != nil {
				// Fractions, doubles, decimals and larger integers require their
				// explicit canonical Extended JSON representation.
				return adapter.ErrInvalid
			}
		}
	}
}

func document(raw json.RawMessage) (bson.D, error) {
	var doc bson.D
	if len(raw) == 0 || exactJSONNumbers(raw) != nil || bson.UnmarshalExtJSON(raw, false, &doc) != nil {
		return nil, adapter.ErrInvalid
	}
	if err := safeBSON(doc, 0); err != nil {
		return nil, err
	}
	return doc, nil
}

func safeBSON(value any, depth int) error {
	if depth > 24 {
		return adapter.ErrInvalid
	}
	switch value := value.(type) {
	case bson.D:
		for _, field := range value {
			switch field.Key {
			case "$out", "$merge", "$where", "$function", "$accumulator", "$changeStream", "$db":
				return adapter.ErrUnsupported
			}
			if err := safeBSON(field.Value, depth+1); err != nil {
				return err
			}
		}
	case bson.A:
		for _, item := range value {
			if err := safeBSON(item, depth+1); err != nil {
				return err
			}
		}
	case bson.JavaScript, bson.CodeWithScope:
		return adapter.ErrUnsupported
	}
	return nil
}

func updateOperators(update bson.D) error {
	if len(update) == 0 {
		return adapter.ErrInvalid
	}
	for _, field := range update {
		switch field.Key {
		case "$set", "$unset", "$inc", "$mul", "$min", "$max", "$rename", "$setOnInsert", "$push", "$pull", "$addToSet", "$pop", "$currentDate", "$bit":
		default:
			return adapter.ErrUnsupported
		}
		if _, ok := field.Value.(bson.D); !ok {
			return adapter.ErrInvalid
		}
	}
	return nil
}

func indexKeys(keys bson.D) error {
	if len(keys) < 1 || len(keys) > 32 {
		return adapter.ErrInvalid
	}
	for _, key := range keys {
		if key.Key == "" || strings.ContainsAny(key.Key, "$\x00\r\n") {
			return adapter.ErrInvalid
		}
		switch v := key.Value.(type) {
		case int32:
			if v != 1 && v != -1 {
				return adapter.ErrUnsupported
			}
		case int64:
			if v != 1 && v != -1 {
				return adapter.ErrUnsupported
			}
		default:
			return adapter.ErrUnsupported
		}
	}
	return nil
}

func readStages(pipeline mongo.Pipeline, depth int) error {
	if depth > 16 || len(pipeline) > 129 {
		return adapter.ErrInvalid
	}
	for _, stage := range pipeline {
		if len(stage) != 1 {
			return adapter.ErrInvalid
		}
		if err := safeBSON(stage, 0); err != nil {
			return err
		}
		op, value := stage[0].Key, stage[0].Value
		switch op {
		case "$match", "$project", "$addFields", "$set", "$unset", "$sort", "$skip", "$limit", "$group", "$unwind", "$count", "$replaceRoot", "$replaceWith", "$sortByCount", "$bucket", "$bucketAuto", "$sample", "$setWindowFields", "$densify", "$fill":
		case "$lookup", "$unionWith":
			if name, ok := value.(string); ok && op == "$unionWith" {
				if !validCollection(name) {
					return adapter.ErrInvalid
				}
				continue
			}
			doc, ok := value.(bson.D)
			if !ok {
				return adapter.ErrUnsupported
			}
			hasCollection := false
			for _, field := range doc {
				switch field.Key {
				case "from", "coll":
					name, ok := field.Value.(string)
					if !ok || !validCollection(name) || op == "$lookup" && field.Key != "from" || op == "$unionWith" && field.Key != "coll" {
						return adapter.ErrUnsupported
					}
					hasCollection = true
				case "pipeline":
					if err := nestedStages(field.Value, depth+1); err != nil {
						return err
					}
				case "localField", "foreignField", "as", "let":
					if op != "$lookup" {
						return adapter.ErrUnsupported
					}
				default:
					return adapter.ErrUnsupported
				}
			}
			if !hasCollection {
				return adapter.ErrUnsupported
			}
		case "$facet":
			doc, ok := value.(bson.D)
			if !ok {
				return adapter.ErrInvalid
			}
			for _, field := range doc {
				if err := nestedStages(field.Value, depth+1); err != nil {
					return err
				}
			}
		default:
			return adapter.ErrUnsupported
		}
	}
	return nil
}

func nestedStages(value any, depth int) error {
	array, ok := value.(bson.A)
	if !ok {
		return adapter.ErrInvalid
	}
	pipeline := make(mongo.Pipeline, 0, len(array))
	for _, item := range array {
		doc, ok := item.(bson.D)
		if !ok {
			return adapter.ErrInvalid
		}
		pipeline = append(pipeline, doc)
	}
	return readStages(pipeline, depth)
}
