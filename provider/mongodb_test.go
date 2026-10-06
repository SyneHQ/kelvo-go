package provider

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestMongoTextClassifiesJSONCommands(t *testing.T) {
	for _, tc := range []struct {
		text, command string
		write         bool
	}{
		{`db.orders.find({"name":"Keep Case"})`, "find", false},
		{`db.orders.findOne({"id":{"$numberLong":"9007199254740993"}});`, "find_one", false},
		{`db.orders.aggregate([{"$match":{"n":1}}])`, "aggregate", false},
		{`{"collection":"orders","pipeline":[]}`, "aggregate", false},
		{`db.orders.countDocuments({})`, "count", false},
		{`db.orders.getIndexes()`, "list_indexes", false},
		{`db.orders.insertOne({"amount":{"$numberDecimal":"1.23"}})`, "insert_one", true},
		{`db.orders.insertMany([{"id":1},{"id":2}])`, "insert_many", true},
		{`db.orders.updateOne({"id":1},{"$set":{"n":2}},{"upsert":true})`, "update_one", true},
		{`db.orders.updateMany({},{"$inc":{"n":1}})`, "update_many", true},
		{`db.orders.deleteOne({"id":1})`, "delete_one", true},
		{`db.orders.deleteMany({"n":1})`, "delete_many", true},
		{`db.createCollection("orders")`, "create_collection", true},
		{`db.orders.drop()`, "drop_collection", true},
		{`db.orders.createIndex({"id":1},{"name":"id_index","unique":true})`, "create_index", true},
		{`db.orders.dropIndex("id_index")`, "drop_index", true},
		{`{"command":"insert_one","collection":"orders","document":{"Name":"upper","name":"lower"}}`, "insert_one", true},
	} {
		t.Run(tc.text, func(t *testing.T) {
			kind, spec, err := InvocationText("mongodb", tc.text)
			if err != nil || kind.Mutating() != tc.write || spec == nil || spec.Provider != "mongodb" || spec.Command != tc.command || spec.ReturnResult != tc.write || len(spec.Parameters) != 1 {
				t.Fatal(kind, spec, err)
			}
			var value map[string]json.RawMessage
			if DecodeDocument(spec.Parameters[0].Value, &value, 16<<10) != nil || string(value["collection"]) != `"orders"` {
				t.Fatal("collection scope changed")
			}
		})
	}
}

func TestMongoFindChainKeepsExactPredicateAndOrder(t *testing.T) {
	kind, spec, err := MongoInvocationText(`db.orders.find({"id":{"$numberLong":"9007199254740993"}},{"id":1}).sort({"id":-1}).skip(2).limit(5)`)
	if err != nil || kind != operations.NativeRead || spec.Command != "aggregate" {
		t.Fatal(kind, spec, err)
	}
	want := `{"collection":"orders","pipeline":[{"$match":{"id":{"$numberLong":"9007199254740993"}}},{"$sort":{"id":-1}},{"$skip":2},{"$limit":5},{"$project":{"id":1}}]}`
	if string(spec.Parameters[0].Value) != want {
		t.Fatal("query semantics changed", string(spec.Parameters[0].Value))
	}
}

func TestMongoTextRejectsExecutableShellAndAmbiguity(t *testing.T) {
	for _, text := range []string{
		`db.orders.find({id:1})`, `db.orders.find({'id':1})`,
		`db.orders.find({"id":ObjectId("a")})`, `db.orders.find().forEach(printjson)`,
		`db.orders.find({});db.other.drop()`, `db.getSiblingDB("other").orders.find({})`,
		`db.adminCommand({"shutdown":1})`, `db.orders.mapReduce("x","y")`,
		`db.orders.find().limit(0)`, `db.orders.find().skip(-1)`, `db.orders.find().limit(2).skip(1)`,
		`db.orders.find().sort({"n":1}).sort({"id":1})`, `db.orders.find().limit(2).sort({"id":1})`,
		`db.orders.updateOne({},{"$set":{"x":1}},{"upsert":true,"unexpected":1})`,
		`{"command":"find","command":"delete_one","collection":"orders"}`,
		`{"command":"find","collection":"orders","filter":{"id":1,"id":2}}`,
		`{"command":"find","collection":"system.users"}`, `[]`, `null`, `SELECT * FROM orders`, strings.Repeat("x", 16<<10+1),
	} {
		if _, _, err := MongoInvocationText(text); err == nil {
			t.Fatal("unsafe or unsupported shell accepted", text)
		}
	}
}
