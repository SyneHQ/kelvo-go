package mongodb

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	native "github.com/SYNEHQ/kelvo-go/internal/sources/mongodb"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func mongoConnection() adapter.Connection {
	return adapter.Connection{Engine: "mongodb", TenantID: "tenant", ConnectionID: "saved", Revision: "revision", Namespace: "app", Username: "reader", Password: "test-only", Endpoint: "mongodb://db.example:27017/?tls=true&authSource=admin", TLS: &tls.Config{MinVersion: tls.VersionTLS12}}
}

func TestMongoConnectionDisablesReplayAndVerifiesTLS(t *testing.T) {
	connection := mongoConnection()
	opts, err := ConnectionOptions(connection)
	if err != nil {
		t.Fatal(err)
	}
	if opts.RetryReads == nil || *opts.RetryReads || opts.RetryWrites == nil || *opts.RetryWrites || opts.MaxPoolSize == nil || *opts.MaxPoolSize != 1 || opts.Auth == nil || opts.Auth.Username != "reader" || opts.Auth.Password != "test-only" || opts.TLSConfig == nil || opts.TLSConfig.InsecureSkipVerify || opts.TLSConfig.ServerName != "" {
		t.Fatal("unsafe connection defaults")
	}
	opts.TLSConfig.MinVersion = tls.VersionTLS13
	if connection.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatal("caller TLS config mutated")
	}
	for _, suffix := range []string{"&tlsInsecure=true", "&tlsCAFile=/tmp/secret", "&authMechanism=MONGODB-AWS", "&retryWrites=true", "&tls=true", "&loadBalanced=true"} {
		bad := connection
		bad.Endpoint += suffix
		if _, err := ConnectionOptions(bad); err == nil {
			t.Fatal("unsafe option accepted", suffix)
		}
	}
	for _, endpoint := range []string{"mongodb://reader:test@db.example/?tls=true&authSource=admin", "mongodb://db.example/app?tls=true&authSource=admin", "mongodb://db.example/?tls=false&authSource=admin", "mongodb://db.example/?tls=true&authSource=", "mongodb://db.example/?tls=true&authSource=bad%2Fname", "mongodb://db.example/?tls=true&authSource=%24external", "mongodb+srv://127.0.0.1/?tls=true&authSource=admin", "mongodb://one.example,two.example/?tls=true&authSource=admin"} {
		bad := connection
		bad.Endpoint = endpoint
		if _, err := ConnectionOptions(bad); err == nil {
			t.Fatal("invalid endpoint accepted", endpoint)
		}
	}
	bad := connection
	bad.TLS = connection.TLS.Clone()
	bad.TLS.InsecureSkipVerify = true
	if _, err := ConnectionOptions(bad); err == nil {
		t.Fatal("unverified TLS accepted")
	}
	connection.Endpoint = "mongodb://db.example/?tls=true&authSource=app"
	opts, err = ConnectionOptions(connection)
	if err != nil || opts.Auth == nil || opts.Auth.AuthSource != "app" || opts.Auth.Username != connection.Username || opts.RetryWrites == nil || *opts.RetryWrites {
		t.Fatal("explicit authentication database changed", err)
	}
}

type bsonSink struct {
	schema    *arrow.Schema
	documents [][]byte
	fail      error
	writes    int
}

func (s *bsonSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *bsonSink) Write(batch arrow.RecordBatch) error {
	s.writes++
	if s.fail != nil {
		return s.fail
	}
	values := batch.Column(0).(*array.Binary)
	for i := 0; i < values.Len(); i++ {
		s.documents = append(s.documents, bytes.Clone(values.Value(i)))
	}
	return nil
}

func TestMongoReadReusesExactBSONTransport(t *testing.T) {
	decimal, _ := bson.ParseDecimal128("12345678901234567890.123")
	raw, err := bson.Marshal(bson.D{{Key: "n", Value: int64(9007199254740993)}, {Key: "d", Value: decimal}, {Key: "nil", Value: nil}, {Key: "oid", Value: bson.NewObjectID()}, {Key: "binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 2}}}})
	if err != nil {
		t.Fatal(err)
	}
	sink := &bsonSink{}
	stats, err := streamDocuments(context.Background(), &singleDocument{raw: raw}, adapter.Limits{MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 1}, sink)
	if err != nil || stats.Rows != 1 || len(sink.documents) != 1 || !bytes.Equal(sink.documents[0], raw) {
		t.Fatal(stats, err)
	}
	if sink.schema.Field(0).Name != "document_bson" {
		t.Fatal("BSON schema changed")
	}
	failing := &bsonSink{fail: errors.New("stop")}
	if _, err := streamDocuments(context.Background(), &singleDocument{raw: raw}, adapter.Limits{MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 1}, failing); err == nil || failing.writes != 1 {
		t.Fatal("sink failure ignored")
	}
}

func TestMongoSQLOnlyUsesVerifiedSelectSubset(t *testing.T) {
	for _, statement := range []string{"INSERT INTO orders VALUES (1)", "DELETE FROM orders", "SELECT * FROM orders; DROP TABLE orders", "SELECT * FROM orders WHERE id=CAST(1 AS INTEGER)"} {
		if _, err := native.SQLRead(statement); err == nil {
			t.Fatal("unverified SQL accepted", statement)
		}
	}
	request, err := native.SQLRead("SELECT id FROM orders WHERE amount > 1.23 LIMIT 5")
	if err != nil {
		t.Fatal(err)
	}
	pipeline, err := native.ReadPipeline(request.Collection, request.Pipeline, 100)
	if err != nil || readStages(pipeline, 0) != nil {
		t.Fatal(err)
	}
	found := false
	for _, stage := range request.Pipeline {
		found = found || strings.Contains(string(stage), "$numberDecimal")
	}
	if !found {
		t.Fatal("SQL decimal went through binary float")
	}
}

func TestMongoMetadataEnforcesSelectedScope(t *testing.T) {
	s := &Session{database: "app"}
	limits := adapter.Limits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 10}
	for _, spec := range []operations.MetadataSpec{{Object: "tables", Target: operations.ObjectRef{Catalog: "other"}, Limit: 10}, {Object: "tables", Target: operations.ObjectRef{Schema: "other"}, Limit: 10}, {Object: "tables", Target: operations.ObjectRef{Name: "system.users"}, Limit: 10}, {Object: "columns", Limit: 10}, {Object: "tables", Cursor: "01", Limit: 10}, {Object: "tables", Cursor: "10001", Limit: 10}} {
		if _, err := s.metadataScope(spec, limits); err == nil {
			t.Fatal("invalid metadata scope accepted", spec)
		}
	}
}
