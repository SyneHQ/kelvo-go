// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"context"
	"encoding/json"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// SQLRead exposes the existing exact, bounded SELECT compiler to optional
// resolved-credential adapters. It never opens a connection or reads secrets.
func SQLRead(sql string) (*query.MongoRequest, error) {
	return sqlRequest(query.Request{SQL: sql})
}

// ReadPipeline applies the same BSON/depth/size/write gates as the native path.
// Optional adapters additionally whitelist the provider stages they implement.
func ReadPipeline(collection string, pipeline []json.RawMessage, maxRows int64) (mongo.Pipeline, error) {
	if maxRows < 1 || maxRows > 1_000_000 {
		return nil, query.NewError("INVALID_ARGUMENT", "MongoDB result row limit is invalid")
	}
	return readPipeline(&query.MongoRequest{Collection: collection, Pipeline: pipeline}, maxRows)
}

// DocumentCursor yields borrowed BSON. StreamDocuments copies it into Arrow
// before advancing; exact types and absent fields are never inferred or lost.
type DocumentCursor interface {
	Next(context.Context) bool
	Document() bson.Raw
	Err() error
}

func StreamDocuments(ctx context.Context, cursor DocumentCursor, limits query.Limits, sink query.Sink, stats *query.Stats) error {
	if ctx == nil || cursor == nil || sink == nil || stats == nil || limits.Validate() != nil {
		return query.NewError("INVALID_ARGUMENT", "Invalid MongoDB result stream")
	}
	return streamCursor(ctx, cursor, limits, sink, stats)
}
