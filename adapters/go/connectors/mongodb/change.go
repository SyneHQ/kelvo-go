// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func (s *Session) executeDetailed(ctx context.Context, plan commandPlan) (adapter.NativeResult, bson.D, error) {
	result := adapter.NativeResult{Outcome: operations.OutcomeUnknown, Effect: operations.EffectUnknown}
	collection := s.client.Database(s.database).Collection(plan.collection)
	details := bson.D{{Key: "acknowledged", Value: true}}
	var affected *int64
	var err error
	switch plan.command {
	case "insert_one":
		var inserted *mongo.InsertOneResult
		inserted, err = collection.InsertOne(ctx, plan.document)
		if err == nil {
			details = append(details, bson.E{Key: "insertedId", Value: inserted.InsertedID})
		}
		count := int64(1)
		affected = &count
	case "insert_many":
		var inserted *mongo.InsertManyResult
		inserted, err = collection.InsertMany(ctx, plan.documents, options.InsertMany().SetOrdered(true))
		if err == nil {
			details = append(details, bson.E{Key: "insertedIds", Value: inserted.InsertedIDs})
		}
		count := int64(len(plan.documents))
		affected = &count
	case "update_one", "update_many":
		var update *mongo.UpdateResult
		if plan.command == "update_one" {
			update, err = collection.UpdateOne(ctx, plan.filter, plan.update, options.UpdateOne().SetUpsert(plan.upsert))
		} else {
			update, err = collection.UpdateMany(ctx, plan.filter, plan.update, options.UpdateMany().SetUpsert(plan.upsert))
		}
		if err == nil {
			details = append(details, bson.E{Key: "matchedCount", Value: update.MatchedCount}, bson.E{Key: "modifiedCount", Value: update.ModifiedCount}, bson.E{Key: "upsertedCount", Value: update.UpsertedCount}, bson.E{Key: "upsertedId", Value: update.UpsertedID})
			count := update.ModifiedCount + update.UpsertedCount
			affected = &count
		}
	case "delete_one", "delete_many":
		var deleted *mongo.DeleteResult
		if plan.command == "delete_one" {
			deleted, err = collection.DeleteOne(ctx, plan.filter)
		} else {
			deleted, err = collection.DeleteMany(ctx, plan.filter)
		}
		if err == nil {
			affected = &deleted.DeletedCount
			details = append(details, bson.E{Key: "deletedCount", Value: deleted.DeletedCount})
		}
	case "create_collection":
		err = s.client.Database(s.database).CreateCollection(ctx, plan.collection)
	case "drop_collection":
		err = collection.Drop(ctx)
	case "create_index":
		_, err = collection.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: plan.keys, Options: options.Index().SetName(plan.name).SetUnique(plan.unique)})
	case "drop_index":
		err = collection.Indexes().DropOne(ctx, plan.name)
	default:
		return adapter.NativeResult{Outcome: operations.Rejected, Effect: operations.EffectNone}, nil, adapter.ErrUnsupported
	}
	// Every command is sent once. A source or network error after dispatch may
	// hide an applied or partial write, so it never authorizes replay.
	if err != nil {
		return result, nil, err
	}
	result.Outcome, result.Effect, result.AffectedRows = operations.Completed, operations.EffectCommitted, affected
	return result, details, nil
}
