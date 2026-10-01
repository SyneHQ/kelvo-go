// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestSQLConversionRejectsUnsupportedQueries(t *testing.T) {
	for _, sql := range []string{
		"SELECT CAST(amount AS INT) FROM orders", "SELECT * FROM a JOIN b ON a.id=b.id", "WITH x AS (SELECT * FROM a) SELECT * FROM x",
		"DELETE FROM orders", "INSERT INTO orders VALUES (1)", "SELECT * FROM orders ORDER BY amount, _id", "SELECT DISTINCT region FROM orders",
		"SELECT region FROM orders GROUP BY region HAVING COUNT(*) > 1", "SELECT * FROM orders; SELECT * FROM other",
	} {
		_, err := sqlRequest(query.Request{SQL: sql})
		assertCode(t, err, "UNSUPPORTED")
	}
	_, err := sqlRequest(query.Request{SQL: "SELECT * FROM orders", Mongo: &query.MongoRequest{Collection: "orders"}})
	assertCode(t, err, "UNSUPPORTED")
	_, err = exactSQLNumbers(map[string]interface{}{"$facet": []map[string]interface{}{{"unsafe": float64(1.5)}}})
	assertCode(t, err, "UNSUPPORTED")
}

func TestSQLConversionPassesSharedReadOnlyGate(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM orders", "SELECT region, SUM(amount) AS total FROM orders GROUP BY region ORDER BY total DESC",
		"SELECT COUNT(*) AS row_count, SUM(amount) AS total FROM orders WHERE amount NOT IN (1, NULL, 3)",
	} {
		command, err := sqlRequest(query.Request{SQL: sql})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := readPipeline(command, 100); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMongoLiveSQLSemantics(t *testing.T) {
	adminURI, readerURI := os.Getenv("KELVO_TEST_MONGO_ADMIN_URI"), os.Getenv("KELVO_TEST_MONGO_READER_URI")
	if adminURI == "" || readerURI == "" {
		t.Skip("set dedicated live MongoDB fixture URIs")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := mongo.Connect(options.Client().ApplyURI(adminURI))
	if err != nil {
		t.Fatal("fixture client initialization failed")
	}
	defer admin.Disconnect(context.Background())
	collectionName := "sql_" + bson.NewObjectID().Hex()
	collection := admin.Database("kelvo_native_test").Collection(collectionName)
	defer collection.Drop(context.Background())
	decimal, _ := bson.ParseDecimal128("123456789.01234567890123456789")
	_, err = collection.InsertMany(ctx, []any{
		bson.M{"_id": int32(1), "status": "paid", "region": "east", "amount": int64(10), "label": "a.b", "note": "$cash", "large": int64(9007199254740993), "precise": decimal},
		bson.M{"_id": int32(2), "status": "paid", "region": "east", "amount": int64(20), "label": "axb", "note": "plain", "large": int64(9223372036854775807)},
		bson.M{"_id": int32(3), "status": "pending", "region": "west", "amount": int32(5), "label": "acb", "note": "plain"},
		bson.M{"_id": int32(4), "status": "paid", "region": "west", "amount": int32(30), "label": "a.b", "note": "$cash"},
		bson.M{"_id": int32(5), "region": "empty", "amount": nil, "label": "a.b\n", "nullable_group": nil},
		bson.M{"_id": int32(6), "region": "empty", "label": "a\nb"},
	})
	if err != nil {
		t.Fatal("SQL fixture insert failed")
	}
	t.Setenv("KELVO_SOURCE_MONGO_DSN", readerURI)
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "mongo", Type: "mongodb", DSNEnv: "KELVO_SOURCE_MONGO_DSN", Options: map[string]string{"database": "kelvo_native_test"}}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	run := func(t *testing.T, sql string) []bson.Raw {
		t.Helper()
		sink := new(bsonSink)
		_, err := e.Execute(ctx, query.Request{Mode: "native", ConnectionID: "mongo", SQL: fmt.Sprintf(sql, collectionName)}, sink)
		if err != nil {
			t.Fatal(err)
		}
		return sink.documents
	}
	for _, test := range []struct {
		name, sql string
		ids       []int32
	}{
		{"filter alias order limit", "SELECT o._id, o.amount AS value FROM %s o WHERE o.status = 'paid' ORDER BY value DESC LIMIT 2", []int32{4, 2}},
		{"offset", "SELECT _id FROM %s ORDER BY _id LIMIT 2 OFFSET 1", []int32{2, 3}},
		{"LIKE metacharacter", "SELECT _id FROM %s WHERE label LIKE 'a.b' ORDER BY _id", []int32{1, 4}},
		{"LIKE wildcard newline", "SELECT _id FROM %s WHERE label LIKE 'a_b' ORDER BY _id", []int32{1, 2, 3, 4, 6}},
		{"NOT LIKE", "SELECT _id FROM %s WHERE label NOT LIKE 'a.b' ORDER BY _id", []int32{2, 3, 5, 6}},
		{"dollar literal", "SELECT _id FROM %s WHERE note = '$cash' ORDER BY _id", []int32{1, 4}},
		{"large integer", "SELECT _id FROM %s WHERE large = 9007199254740993", []int32{1}},
		{"decimal literal", "SELECT _id FROM %s WHERE precise = 123456789.01234567890123456789", []int32{1}},
		{"null equality unknown", "SELECT _id FROM %s WHERE amount = NULL", nil},
		{"not null equality unknown", "SELECT _id FROM %s WHERE NOT (amount = NULL)", nil},
		{"null and missing", "SELECT _id FROM %s WHERE amount IS NULL ORDER BY _id", []int32{5, 6}},
		{"not equal skips null", "SELECT _id FROM %s WHERE NOT (amount = 10) ORDER BY _id", []int32{2, 3, 4}},
		{"NOT IN null unknown", "SELECT _id FROM %s WHERE amount NOT IN (10, NULL)", nil},
		{"IN null match", "SELECT _id FROM %s WHERE amount IN (10, NULL)", []int32{1}},
		{"between", "SELECT _id FROM %s WHERE amount BETWEEN 10 AND 20 ORDER BY _id", []int32{1, 2}},
		{"not between", "SELECT _id FROM %s WHERE amount NOT BETWEEN 10 AND 20 ORDER BY _id", []int32{3, 4}},
		{"unknown OR true", "SELECT _id FROM %s WHERE amount = NULL OR _id = 1", []int32{1}},
		{"limit zero", "SELECT _id FROM %s LIMIT 0", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			documents := run(t, test.sql)
			var ids []int32
			for _, document := range documents {
				ids = append(ids, document.Lookup("_id").Int32())
			}
			if !reflect.DeepEqual(ids, test.ids) {
				t.Fatalf("ids=%v, want %v", ids, test.ids)
			}
		})
	}
	t.Run("literal projection", func(t *testing.T) {
		documents := run(t, "SELECT '$cash' AS literal, 9007199254740993 AS large, 123456789.01234567890123456789 AS precise FROM %s LIMIT 1")
		if len(documents) != 1 || documents[0].Lookup("literal").StringValue() != "$cash" || documents[0].Lookup("large").Int64() != 9007199254740993 || documents[0].Lookup("precise").Decimal128() != decimal {
			t.Fatal("literal projection changed values")
		}
	})
	t.Run("missing projection becomes null", func(t *testing.T) {
		documents := run(t, "SELECT amount FROM %s WHERE _id = 6")
		if len(documents) != 1 || documents[0].Lookup("amount").Type != bson.TypeNull {
			t.Fatal("missing field did not become SQL null")
		}
	})
	t.Run("group aggregate null semantics", func(t *testing.T) {
		documents := run(t, "SELECT region AS area, COUNT(*) AS row_count, COUNT(amount) AS present, SUM(amount) AS total, AVG(amount) AS average, MIN(amount) AS low, MAX(amount) AS high FROM %s GROUP BY region ORDER BY area")
		if len(documents) != 3 {
			t.Fatalf("group count=%d", len(documents))
		}
		for i, expected := range []struct {
			area                      string
			present                   int64
			total, average, low, high string
		}{{"east", 2, "30", "15", "10", "20"}, {"empty", 0, "", "", "", ""}, {"west", 2, "35", "17.5", "5", "30"}} {
			d := documents[i]
			if d.Lookup("area").StringValue() != expected.area || integerValue(t, d.Lookup("row_count")) != 2 || integerValue(t, d.Lookup("present")) != expected.present {
				t.Fatal("incorrect group or COUNT")
			}
			for field, value := range map[string]string{"total": expected.total, "average": expected.average, "low": expected.low, "high": expected.high} {
				assertDecimalOrNull(t, d.Lookup(field), value)
			}
		}
	})
	t.Run("empty global aggregates", func(t *testing.T) {
		documents := run(t, "SELECT COUNT(*) AS row_count, COUNT(amount) AS present, SUM(amount) AS total, AVG(amount) AS average, MIN(amount) AS low, MAX(amount) AS high, '$cash' AS literal FROM %s WHERE _id < 0")
		if len(documents) != 1 {
			t.Fatalf("empty global rows=%d", len(documents))
		}
		if integerValue(t, documents[0].Lookup("row_count")) != 0 || integerValue(t, documents[0].Lookup("present")) != 0 || documents[0].Lookup("literal").StringValue() != "$cash" {
			t.Fatal("empty aggregate defaults incorrect")
		}
		for _, field := range []string{"total", "average", "low", "high"} {
			assertDecimalOrNull(t, documents[0].Lookup(field), "")
		}
	})
	t.Run("empty grouped aggregate", func(t *testing.T) {
		if got := run(t, "SELECT region, COUNT(*) AS row_count FROM %s WHERE _id < 0 GROUP BY region"); len(got) != 0 {
			t.Fatal("empty grouped query emitted a row")
		}
	})
	t.Run("global aggregate large integer sum", func(t *testing.T) {
		documents := run(t, "SELECT SUM(large) AS total, COUNT(*) AS row_count FROM %s")
		if len(documents) != 1 || integerValue(t, documents[0].Lookup("row_count")) != 6 {
			t.Fatal("global count incorrect")
		}
		assertDecimalOrNull(t, documents[0].Lookup("total"), "9232379236109516800")
	})
	t.Run("null and missing group keys", func(t *testing.T) {
		documents := run(t, "SELECT nullable_group AS key_value, COUNT(*) AS row_count FROM %s GROUP BY nullable_group")
		if len(documents) != 1 || documents[0].Lookup("key_value").Type != bson.TypeNull || integerValue(t, documents[0].Lookup("row_count")) != 6 {
			t.Fatal("NULL and missing did not form one group")
		}
	})
	t.Run("incompatible comparisons fail explicitly", func(t *testing.T) {
		for _, sql := range []string{"SELECT _id FROM " + collectionName + " WHERE amount > 'hello'", "SELECT _id FROM " + collectionName + " WHERE amount LIKE '10'"} {
			_, err := e.Execute(ctx, query.Request{Mode: "native", ConnectionID: "mongo", SQL: sql}, new(bsonSink))
			assertCode(t, err, "QUERY_FAILED")
		}
	})
	t.Run("nonnumeric aggregate fails explicitly", func(t *testing.T) {
		bad := admin.Database("kelvo_native_test").Collection(collectionName + "_bad")
		defer bad.Drop(context.Background())
		if _, err := bad.InsertOne(ctx, bson.M{"amount": "10"}); err != nil {
			t.Fatal("invalid-type fixture insert failed")
		}
		_, err := e.Execute(ctx, query.Request{Mode: "native", ConnectionID: "mongo", SQL: "SELECT SUM(amount) AS total FROM " + bad.Name()}, new(bsonSink))
		assertCode(t, err, "QUERY_FAILED")
	})
}

func integerValue(t *testing.T, value bson.RawValue) int64 {
	t.Helper()
	switch value.Type {
	case bson.TypeInt32:
		return int64(value.Int32())
	case bson.TypeInt64:
		return value.Int64()
	default:
		t.Fatal("integer changed BSON type")
		return 0
	}
}

func assertDecimalOrNull(t *testing.T, value bson.RawValue, expected string) {
	t.Helper()
	if expected == "" {
		if value.Type != bson.TypeNull {
			t.Fatalf("expected NULL, got %v", value)
		}
		return
	}
	if value.Type != bson.TypeDecimal128 || value.Decimal128().String() != expected {
		t.Fatalf("expected Decimal128 %s, got %v", expected, value)
	}
}

// Keep canonical numbers inside the same JSON/Extended JSON path as native
// requests, including typed facet stage slices from the SQL converter.
func TestExactSQLNumberNormalization(t *testing.T) {
	normalized, err := exactSQLNumbers([]map[string]interface{}{{"value": json.Number("123456789.01234567890123456789")}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(normalized)
	if err != nil || string(encoded) != `[{"value":{"$numberDecimal":"123456789.01234567890123456789"}}]` {
		t.Fatalf("exact number=%s, %v", encoded, err)
	}
}
