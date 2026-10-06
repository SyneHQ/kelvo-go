// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"encoding/json"
	"time"

	"github.com/SYNEHQ/kelvo-go/watch"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func watchDocument(raw bson.Raw) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage(`null`), nil
	}
	encoded, err := bson.MarshalExtJSON(raw, true, false)
	return json.RawMessage(encoded), err
}

func decodeWatchEvent(scope watch.Scope, raw bson.Raw, maximum int64) (watch.Event, bson.Raw, error) {
	if len(raw) == 0 || int64(len(raw)) > maximum || raw.Validate() != nil {
		return watch.Event{}, nil, watch.ErrLimit
	}
	var change struct {
		ID        bson.Raw `bson:"_id"`
		Operation string   `bson:"operationType"`
		Document  bson.Raw `bson:"fullDocument"`
		Before    bson.Raw `bson:"fullDocumentBeforeChange"`
		Key       bson.Raw `bson:"documentKey"`
		Update    bson.Raw `bson:"updateDescription"`
		Namespace struct {
			Database   string `bson:"db"`
			Collection string `bson:"coll"`
		} `bson:"ns"`
		ClusterTime bson.Timestamp `bson:"clusterTime"`
		WallTime    time.Time      `bson:"wallTime"`
	}
	if bson.Unmarshal(raw, &change) != nil {
		return watch.Event{}, nil, watch.ErrInvalid
	}
	switch change.Operation {
	case "invalidate", "drop", "rename", "dropDatabase":
		return watch.Event{}, nil, watch.ErrConflict
	}
	if change.Namespace.Database != scope.Database || change.Namespace.Collection != scope.Table {
		return watch.Event{}, nil, watch.ErrConflict
	}
	if _, err := encodeWatchToken(change.ID); err != nil {
		return watch.Event{}, nil, err
	}
	if len(change.Key) == 0 {
		return watch.Event{}, nil, watch.ErrInvalid
	}
	event := watch.Event{ID: watchEventID(change.ID), Timestamp: change.WallTime.UTC(), Data: json.RawMessage(`null`), OldData: json.RawMessage(`null`)}
	if event.Timestamp.IsZero() && change.ClusterTime.T > 0 {
		event.Timestamp = time.Unix(int64(change.ClusterTime.T), 0).UTC()
	}
	var data, old bson.Raw
	switch change.Operation {
	case "insert":
		if len(change.Document) == 0 {
			return watch.Event{}, nil, watch.ErrInvalid
		}
		event.Operation, event.DataKind, data = "INSERT", "document", change.Document
	case "update", "replace":
		event.Operation = "UPDATE"
		if len(change.Document) > 0 {
			data, event.DataKind = change.Document, "document"
		} else if change.Operation == "update" && len(change.Update) > 0 {
			encoded, err := bson.Marshal(bson.D{{Key: "documentKey", Value: change.Key}, {Key: "updateDescription", Value: change.Update}})
			if err != nil {
				return watch.Event{}, nil, err
			}
			data, event.DataKind = bson.Raw(encoded), "update_delta"
		} else {
			data, event.DataKind = change.Key, "document_key"
		}
		if len(change.Before) > 0 {
			old, event.OldDataKind = change.Before, "document"
		}
	case "delete":
		event.Operation = "DELETE"
		if len(change.Before) > 0 {
			old, event.OldDataKind = change.Before, "document"
		} else {
			old, event.OldDataKind = change.Key, "document_key"
		}
	default:
		return watch.Event{}, nil, watch.ErrInvalid
	}
	var err error
	event.Data, err = watchDocument(data)
	if err != nil {
		return watch.Event{}, nil, err
	}
	event.OldData, err = watchDocument(old)
	if err != nil {
		return watch.Event{}, nil, err
	}
	// The original envelope remains available for the established filter
	// vocabulary. Canonical EJSON preserves BSON integer/decimal/binary types.
	context := bson.D{{Key: "operationType", Value: change.Operation}, {Key: "ns", Value: bson.D{{Key: "db", Value: scope.Database}, {Key: "coll", Value: scope.Table}}}, {Key: "documentKey", Value: change.Key}}
	for _, field := range []struct {
		name string
		raw  bson.Raw
	}{{"fullDocument", change.Document}, {"fullDocumentBeforeChange", change.Before}, {"updateDescription", change.Update}} {
		if _, lookupErr := raw.LookupErr(field.name); lookupErr == nil {
			var value any = field.raw
			if len(field.raw) == 0 {
				value = nil
			}
			context = append(context, bson.E{Key: field.name, Value: value})
		}
	}
	event.Context, err = bson.MarshalExtJSON(context, true, false)
	if err != nil {
		return watch.Event{}, nil, err
	}
	if event.Validate() != nil {
		return watch.Event{}, nil, watch.ErrInvalid
	}
	encoded, err := json.Marshal(event)
	if err != nil || int64(len(encoded)) > maximum {
		return watch.Event{}, nil, watch.ErrLimit
	}
	return event, append(bson.Raw(nil), change.ID...), nil
}
