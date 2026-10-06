package mongodb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// The opt-in fixture harness supplies random test credentials only over an
// inherited pipe. No source credentials are accepted from argv or environment.
func TestMongoLiveAdapter(t *testing.T) {
	fd := os.Getenv("KELVO_MONGODB_LIVE_FD")
	if fd == "" {
		t.Skip("isolated MongoDB fixture not configured")
	}
	n, err := strconv.Atoi(fd)
	if err != nil || n < 3 {
		t.Fatal("invalid fixture input pipe")
	}
	input := os.NewFile(uintptr(n), "fixture-input")
	defer input.Close()
	raw, err := io.ReadAll(io.LimitReader(input, 64<<10))
	if err != nil {
		t.Fatal("fixture input unavailable")
	}
	defer clear(raw)
	var fixture struct {
		Host                                         string
		Port                                         int
		Username, Password, Database, CA, ServerName string
		ReplicaSet                                   bool
	}
	if json.Unmarshal(raw, &fixture) != nil {
		t.Fatal("invalid fixture configuration")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(fixture.CA)) {
		t.Fatal("invalid fixture CA")
	}
	connection := adapter.Connection{Engine: "mongodb", TenantID: "live-test", ConnectionID: "live-source", Revision: "live-revision", Namespace: fixture.Database, Username: fixture.Username, Password: fixture.Password, Endpoint: "mongodb://" + net.JoinHostPort(fixture.Host, strconv.Itoa(fixture.Port)) + "/?tls=true&authSource=admin&directConnection=true", TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: fixture.ServerName}}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	opened, err := (Driver{}).Open(ctx, connection)
	if err != nil {
		t.Fatal("fixture source open failed")
	}
	session := opened.(*Session)
	defer session.Close()
	execute := func(command, raw string) {
		t.Helper()
		result, err := session.RunNative(ctx, nativeRequest(command, raw, true), nil)
		if err != nil || result.Outcome != operations.Completed || result.Effect != operations.EffectCommitted {
			var provider mongo.CommandError
			_ = errors.As(err, &provider)
			t.Fatalf("native %s did not acknowledge completion (error_type=%T provider_code=%d outcome=%s)", command, err, provider.Code, result.Outcome)
		}
	}
	execute("create_collection", `{"collection":"orders"}`)
	execute("insert_many", `{"collection":"orders","documents":[{"id":1,"amount":{"$numberDecimal":"1.23"},"exact":{"$numberLong":"9007199254740993"},"nil":null,"binary":{"$binary":{"base64":"AAEC","subType":"80"}}},{"id":2,"amount":{"$numberDecimal":"2.34"}},{"id":3,"amount":{"$numberDecimal":"3.45"}}]}`)
	read := func(command, raw string) *bsonSink {
		t.Helper()
		sink := &bsonSink{}
		result, err := session.RunNative(ctx, nativeRequest(command, raw, false), sink)
		if err != nil || result.Outcome != operations.Completed || result.Effect != operations.EffectNone {
			t.Fatalf("native %s read failed", command)
		}
		return sink
	}
	rows := read("find", `{"collection":"orders","filter":{"id":1}}`)
	if len(rows.documents) != 1 {
		t.Fatal("find returned incorrect row count")
	}
	doc := bson.Raw(rows.documents[0])
	if doc.Lookup("exact").Int64() != 9007199254740993 || doc.Lookup("nil").Type != bson.TypeNull {
		t.Fatal("exact integer or null changed")
	}
	if doc.Lookup("amount").Decimal128().String() != "1.23" {
		t.Fatal("decimal changed")
	}
	subtype, data := doc.Lookup("binary").Binary()
	if subtype != 0x80 || len(data) != 3 {
		t.Fatal("binary subtype changed")
	}
	sqlSink := &bsonSink{}
	stats, err := session.Query(ctx, adapter.Query{Statement: "SELECT id FROM orders WHERE amount > 1.23 ORDER BY id LIMIT 2", MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 1}, sqlSink)
	if err != nil || stats.Rows != 2 || len(sqlSink.documents) != 2 {
		t.Fatal("SQL SELECT workflow failed")
	}
	for i, raw := range sqlSink.documents {
		if bson.Raw(raw).Lookup("id").Int32() != int32(i+2) {
			t.Fatal("SQL ordered result differs from source")
		}
	}
	execute("update_one", `{"collection":"orders","filter":{"id":1},"update":{"$set":{"amount":{"$numberDecimal":"9.99"}}}}`)
	rows = read("find_one", `{"collection":"orders","filter":{"id":1}}`)
	if len(rows.documents) != 1 || bson.Raw(rows.documents[0]).Lookup("amount").Decimal128().String() != "9.99" {
		t.Fatal("update was not visible")
	}
	execute("create_index", `{"collection":"orders","keys":{"id":1},"name":"id_lookup","unique":true}`)
	if len(read("list_indexes", `{"collection":"orders"}`).documents) != 2 {
		t.Fatal("index not visible")
	}
	execute("drop_index", `{"collection":"orders","name":"id_lookup"}`)
	execute("delete_one", `{"collection":"orders","filter":{"id":3}}`)
	counts := read("count", `{"collection":"orders"}`)
	if len(counts.documents) != 1 || bson.Raw(counts.documents[0]).Lookup("count").Int64() != 2 {
		t.Fatal("count differs after delete")
	}
	zero := read("count", `{"collection":"orders","filter":{"id":99}}`)
	if len(zero.documents) != 1 || bson.Raw(zero.documents[0]).Lookup("count").Int64() != 0 {
		t.Fatal("empty count is not zero")
	}
	invalid := nativeRequest("aggregate", `{"collection":"orders","pipeline":[{"$out":"other"}]}`, false)
	if _, err := session.RunNative(ctx, invalid, &bsonSink{}); err == nil {
		t.Fatal("read accepted mutation")
	}
	mutation := nativeRequest("insert_one", `{"collection":"orders","document":{"id":101}}`, true)
	mutation.Spec.ReturnResult = true
	ack := &bsonSink{}
	confirmed, err := session.RunNative(ctx, mutation, ack)
	if err != nil || confirmed.Outcome != operations.Completed || confirmed.Effect != operations.EffectCommitted || len(ack.documents) != 1 {
		t.Fatal("mutation result not acknowledged")
	}
	ackDoc := bson.Raw(ack.documents[0])
	if !ackDoc.Lookup("acknowledged").Boolean() || ackDoc.Lookup("insertedId").Type != bson.TypeObjectID {
		t.Fatal("inserted identity missing")
	}
	mutation.Spec.Parameters[0].Value = json.RawMessage(`{"collection":"orders","document":{"id":102}}`)
	confirmed, err = session.RunNative(ctx, mutation, mongoFailedResultSink{})
	if err == nil || confirmed.Outcome != operations.Completed || confirmed.Effect != operations.EffectCommitted {
		t.Fatal("delivery failure lost committed outcome")
	}
	if len(read("find", `{"collection":"orders","filter":{"id":102}}`).documents) != 1 {
		t.Fatal("write was not committed before result failure")
	}
	execute("drop_collection", `{"collection":"orders"}`)
	t.Log("verified TLS, native create/insert/read/update/index/delete/drop, exact BSON and SQL SELECT")
	if fixture.ReplicaSet {
		testMongoWatchLive(t, session)
	}
}

type mongoFailedResultSink struct{}

func (mongoFailedResultSink) Schema(*arrow.Schema) error { return errors.New("result delivery closed") }
func (mongoFailedResultSink) Write(arrow.RecordBatch) error {
	return errors.New("result delivery closed")
}
