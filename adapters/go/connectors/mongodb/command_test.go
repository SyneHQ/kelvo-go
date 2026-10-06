package mongodb

import (
	"encoding/json"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func nativeRequest(command, raw string, write bool) adapter.Native {
	kind := operations.NativeRead
	if write {
		kind = operations.NativeExecute
	}
	return adapter.Native{Kind: kind, Spec: operations.NativeSpec{Provider: "mongodb", Command: command, Parameters: []operations.Parameter{{Type: "json", Value: json.RawMessage(raw)}}}, Limits: adapter.Limits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 10}}
}

func TestNativeCommandsKeepExplicitReadWriteBoundary(t *testing.T) {
	for _, tc := range []struct {
		command, raw string
		write        bool
	}{
		{"aggregate", `{"collection":"orders","pipeline":[{"$match":{"active":true}}]}`, false},
		{"find", `{"collection":"orders","filter":{},"sort":{"id":-1},"projection":{"id":1}}`, false},
		{"find_one", `{"collection":"orders"}`, false},
		{"count", `{"collection":"orders","filter":{}}`, false},
		{"list_indexes", `{"collection":"orders"}`, false},
		{"insert_one", `{"collection":"orders","document":{"id":{"$numberLong":"9007199254740993"},"amount":{"$numberDecimal":"1.23"}}}`, true},
		{"insert_many", `{"collection":"orders","documents":[{"id":1},{"id":2}]}`, true},
		{"update_one", `{"collection":"orders","filter":{"id":1},"update":{"$set":{"x":null}},"upsert":true}`, true},
		{"update_many", `{"collection":"orders","filter":{},"update":{"$inc":{"n":1}}}`, true},
		{"delete_one", `{"collection":"orders","filter":{"id":1}}`, true},
		{"delete_many", `{"collection":"orders","filter":{}}`, true},
		{"create_collection", `{"collection":"orders"}`, true},
		{"drop_collection", `{"collection":"orders"}`, true},
		{"create_index", `{"collection":"orders","keys":{"customer_id":1,"created_at":-1},"name":"customer_created","unique":false}`, true},
		{"drop_index", `{"collection":"orders","name":"customer_created"}`, true},
	} {
		t.Run(tc.command, func(t *testing.T) {
			plan, err := parseCommand(nativeRequest(tc.command, tc.raw, tc.write))
			if err != nil || plan.collection != "orders" {
				t.Fatal(plan, err)
			}
			if _, err := parseCommand(nativeRequest(tc.command, tc.raw, !tc.write)); err == nil {
				t.Fatal("operation kind did not constrain command")
			}
		})
	}
}

func TestNativeRejectsAmbiguityScopeEscapeAndExecutablePayloads(t *testing.T) {
	for _, tc := range []struct {
		command, raw string
		write        bool
	}{
		{"run_command", `{"collection":"orders"}`, false},
		{"find", `{"collection":"system.users"}`, false},
		{"find", `{"collection":"orders","database":"other"}`, false},
		{"find", `{"collection":"orders","Collection":"other"}`, false},
		{"find", `{"collection":"orders","filter":null}`, false},
		{"find", `{"collection":"orders","filter":{"x":1,"x":2}}`, false},
		{"find", `{"collection":"orders","upsert":true}`, false},
		{"find", `{"collection":"orders","filter":{"$where":"true"}}`, false},
		{"aggregate", `{"collection":"orders","pipeline":[{"$out":"stolen"}]}`, false},
		{"aggregate", `{"collection":"orders","pipeline":[{"$facet":{"a":[{"$merge":"stolen"}]}}]}`, false},
		{"aggregate", `{"collection":"orders","pipeline":[{"$lookup":{"from":{"db":"other","coll":"orders"},"as":"joined"}}]}`, false},
		{"aggregate", `{"collection":"orders","pipeline":[{"$unionWith":{"coll":"orders","db":"other"}}]}`, false},
		{"aggregate", `{"collection":"orders","pipeline":[{"$currentOp":{}}]}`, false},
		{"aggregate", `{"collection":"orders","pipeline":[{"$set":{"x":{"$function":{"body":"x","args":[],"lang":"js"}}}}]}`, false},
		{"insert_one", `{"collection":"orders","document":{"x":1.23}}`, true},
		{"insert_one", `{"collection":"orders","document":{"x":9223372036854775808}}`, true},
		{"insert_one", `{"collection":"orders","document":{"x":{"$code":"f()"}}}`, true},
		{"insert_many", `{"collection":"orders","documents":[]}`, true},
		{"update_many", `{"collection":"orders","update":{"$set":{"x":1}}}`, true},
		{"update_one", `{"collection":"orders","filter":{},"update":{"x":1}}`, true},
		{"delete_many", `{"collection":"orders"}`, true},
		{"create_index", `{"collection":"orders","keys":{"id":"text"},"name":"id_text"}`, true},
		{"drop_index", `{"collection":"orders","name":"*"}`, true},
	} {
		if _, err := parseCommand(nativeRequest(tc.command, tc.raw, tc.write)); err == nil {
			t.Fatalf("accepted %s %s", tc.command, tc.raw)
		}
	}
}

func TestNativePreservesExtendedJSONTypesAndInput(t *testing.T) {
	raw := `{"collection":"orders","document":{"integer":{"$numberLong":"9007199254740993"},"decimal":{"$numberDecimal":"12345678901234567890.123"},"null":null,"object":{"$oid":"507f1f77bcf86cd799439011"},"date":{"$date":{"$numberLong":"1700000000000"}},"binary":{"$binary":{"base64":"AAEC","subType":"80"}}}}`
	request := nativeRequest("insert_one", raw, true)
	plan, err := parseCommand(request)
	if err != nil {
		t.Fatal(err)
	}
	if string(request.Spec.Parameters[0].Value) != raw {
		t.Fatal("mutated signed input")
	}
	if plan.document[0].Value != int64(9007199254740993) {
		t.Fatal(plan.document[0])
	}
	if _, ok := plan.document[1].Value.(bson.Decimal128); !ok {
		t.Fatal("decimal type lost")
	}
	if plan.document[2].Value != nil {
		t.Fatal("null lost")
	}
	if _, ok := plan.document[3].Value.(bson.ObjectID); !ok {
		t.Fatal("object id lost")
	}
	if _, ok := plan.document[4].Value.(bson.DateTime); !ok {
		t.Fatal("date lost")
	}
	if value, ok := plan.document[5].Value.(bson.Binary); !ok || value.Subtype != 0x80 {
		t.Fatal("binary subtype lost")
	}
	read := nativeRequest("find", `{"collection":"orders","filter":{"id":1}}`, false)
	before := string(read.Spec.Parameters[0].Value)
	plan, err = parseCommand(read)
	if err != nil || string(read.Spec.Parameters[0].Value) != before || len(plan.pipeline) != 2 {
		t.Fatal(plan, err)
	}
	last := plan.pipeline[len(plan.pipeline)-1]
	if last[0].Key != "$limit" || last[0].Value != int64(101) {
		t.Fatal("overflow detection limit missing", last)
	}
}

func TestNativePreservesCaseSensitiveFields(t *testing.T) {
	raw := `{"collection":"orders.archive","document":{"Name":"UPPER","name":"lower"}}`
	plan, err := parseCommand(nativeRequest("insert_one", raw, true))
	if err != nil || len(plan.document) != 2 || plan.document[0].Key != "Name" || plan.document[1].Key != "name" || plan.collection != "orders.archive" {
		t.Fatal("case-sensitive document or collection changed", err)
	}
}

func TestNativeAllowsBoundedSameDatabaseJoin(t *testing.T) {
	for _, pipeline := range []string{
		`[{"$lookup":{"from":"customers","localField":"customer_id","foreignField":"id","as":"customer"}}]`,
		`[{"$unionWith":{"coll":"archive","pipeline":[{"$match":{"active":true}}]}}]`,
		`[{"$facet":{"totals":[{"$group":{"_id":null,"n":{"$sum":1}}}]}}]`,
	} {
		if _, err := parseCommand(nativeRequest("aggregate", `{"collection":"orders","pipeline":`+pipeline+`}`, false)); err != nil {
			t.Fatal(err)
		}
	}
}
