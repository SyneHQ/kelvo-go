package cassandra

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gocql/gocql"
)

type executionBackend struct {
	*fakeBackend
	statements []string
	parameters [][]any
	errors     []error
}

func (b *executionBackend) Execute(_ context.Context, statement string, parameters []any) error {
	b.statements = append(b.statements, statement)
	b.parameters = append(b.parameters, parameters)
	if len(b.errors) >= len(b.statements) {
		return b.errors[len(b.statements)-1]
	}
	return nil
}

type requestFailure int

func (e requestFailure) Code() int       { return int(e) }
func (e requestFailure) Message() string { return "secret provider statement and credentials" }
func (e requestFailure) Error() string   { return e.Message() }

func TestChangesPreserveBindingsAndNeverReplay(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		outcome string
	}{
		{"ack", nil, "succeeded"},
		{"syntax", requestFailure(gocql.ErrCodeSyntax), "failed"},
		{"denied", requestFailure(gocql.ErrCodeUnauthorized), "failed"},
		{"invalid", requestFailure(gocql.ErrCodeInvalid), "failed"},
		{"timeout", requestFailure(gocql.ErrCodeWriteTimeout), "unknown"},
		{"unavailable", requestFailure(gocql.ErrCodeUnavailable), "unknown"},
		{"network", errors.New("secret connection"), "unknown"},
		{"cancel", context.Canceled, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &executionBackend{fakeBackend: &fakeBackend{}, errors: []error{tc.err}}
			s := &Session{backend: b}
			p := []operations.Parameter{{Type: "int64", Value: json.RawMessage(`"9007199254740993"`)}, {Type: "binary", Value: json.RawMessage(`"AP8="`)}}
			result, err := s.Execute(context.Background(), adapter.Change{Statements: []string{"INSERT INTO events(id, payload) VALUES(?, ?)"}, Parameters: [][]operations.Parameter{p}})
			if result.Outcome != tc.outcome || result.Attempted != 1 || len(b.statements) != 1 || result.AffectedRows != nil || (err == nil) != (tc.err == nil) || err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal(result, err, b.statements)
			}
			if !reflect.DeepEqual(b.parameters[0], []any{int64(9007199254740993), []byte{0, 255}}) {
				t.Fatal(b.parameters)
			}
		})
	}
}

func TestChangeValidationBeforeAnyBatchEffect(t *testing.T) {
	for _, statement := range []string{
		"USE other", "BEGIN BATCH UPDATE t SET n=n+1 WHERE id=1; APPLY BATCH", "GRANT ALL ON KEYSPACE t TO x", "CREATE KEYSPACE other", "CREATE FUNCTION f() RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'x'",
		"UPDATE t SET v=1 WHERE id=1 IF v=0", "INSERT INTO t(id) VALUES(1) IF NOT EXISTS", "DELETE FROM t; DROP TABLE t", "UPDATE t SET v=? WHERE id=1", "DELETE FROM t -- comment", "DELETE FROM t WHERE v='unterminated",
	} {
		b := &executionBackend{fakeBackend: &fakeBackend{}}
		s := &Session{backend: b}
		result, err := s.Execute(context.Background(), adapter.Change{Statements: []string{"UPDATE t SET n=n+1 WHERE id=1", statement}, Parameters: [][]operations.Parameter{nil, nil}})
		if err == nil || result.Attempted != 0 || len(b.statements) != 0 {
			t.Fatal(statement, result, err)
		}
	}
	for _, statement := range []string{"INSERT INTO t(id,v) VALUES(1,'it''s;fine')", `UPDATE "t" SET "v"=1 WHERE "id"=1`, "DELETE FROM t WHERE id=1;", "TRUNCATE TABLE t", "CREATE TABLE IF NOT EXISTS t (id bigint PRIMARY KEY)", "ALTER TABLE t ADD n text", "DROP TABLE t", "CREATE MATERIALIZED VIEW v AS SELECT * FROM t WHERE id IS NOT NULL PRIMARY KEY (id)"} {
		if !changeCQL(statement, 0) {
			t.Errorf("valid statement rejected: %s", statement)
		}
	}
}

func TestChangePartialCompletionAndPreDispatchCancellation(t *testing.T) {
	b := &executionBackend{fakeBackend: &fakeBackend{}, errors: []error{nil, errors.New("reset")}}
	s := &Session{backend: b}
	change := adapter.Change{Statements: []string{"DELETE FROM t WHERE id=1", "DELETE FROM t WHERE id=2", "DELETE FROM t WHERE id=3"}, Parameters: [][]operations.Parameter{nil, nil, nil}}
	result, err := s.Execute(context.Background(), change)
	if err == nil || result.Outcome != "unknown" || result.Completed != 1 || result.Attempted != 2 || len(b.statements) != 2 {
		t.Fatal(result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = s.Execute(ctx, change)
	if !errors.Is(err, context.Canceled) || result.Attempted != 0 || result.Outcome != "failed" || len(b.statements) != 2 {
		t.Fatal(result, err)
	}
	for _, invalid := range []adapter.Change{
		{Statements: change.Statements, Parameters: change.Parameters, Transaction: true},
		{Statements: change.Statements, Parameters: change.Parameters, Role: "admin"},
		{Statements: change.Statements, Parameters: change.Parameters, Isolation: "serializable"},
		{Statements: []string{"INSERT INTO t(v) VALUES(?)"}, Parameters: [][]operations.Parameter{{{Type: "timestamp", Value: json.RawMessage(`"2026-01-01T00:00:00.000001Z"`)}}}},
	} {
		if result, err := s.Execute(context.Background(), invalid); err == nil || result.Attempted != 0 {
			t.Fatal(result, err)
		}
	}
}

type metadataBackend struct {
	*fakeBackend
	statement  string
	parameters []any
}

func (b *metadataBackend) Query(_ context.Context, statement string, parameters []any, _ int) iterator {
	b.statement, b.parameters = statement, parameters
	b.queries++
	return b.iter
}

func TestMetadataIsScopedBoundedAndTyped(t *testing.T) {
	b := &metadataBackend{fakeBackend: &fakeBackend{iter: &fakeIterator{rows: []map[string]any{
		{"keyspace_name": "tenant", "table_name": "events", "column_name": "id", "type": "bigint", "kind": "partition_key"},
		{"keyspace_name": "tenant", "table_name": "events", "column_name": "payload", "type": "blob", "kind": "regular"},
	}}}}
	s := &Session{backend: b, namespace: "tenant"}
	spec := operations.MetadataSpec{Object: "columns", Target: operations.ObjectRef{Catalog: "tenant", Name: "events"}, Cursor: "1", Limit: 1}
	var name string
	stats, err := s.Inspect(context.Background(), spec, adapter.Limits{MaxRows: 1, MaxBytes: 4096, BatchRows: 1}, testSink{write: func(batch arrow.RecordBatch) error {
		name = batch.Column(2).(*array.String).Value(0)
		if batch.Column(4).(*array.Int64).Value(0) != 2 || batch.Column(5).(*array.String).Value(0) != "YES" {
			t.Fatal(batch)
		}
		return nil
	}})
	if err != nil || stats.Rows != 1 || name != "payload" || b.iter.closed != 1 || strings.Contains(b.statement, "events") || !reflect.DeepEqual(b.parameters, []any{"tenant", "events", 2}) {
		t.Fatal(stats, err, name, b.statement, b.parameters)
	}
	for _, invalid := range []operations.MetadataSpec{
		{Object: "tables", Target: operations.ObjectRef{Catalog: "other"}, Limit: 1},
		{Object: "columns", Target: operations.ObjectRef{Schema: "other"}, Limit: 1},
		{Object: "tables", Cursor: "01", Limit: 1}, {Object: "tables", Cursor: "10001", Limit: 1},
		{Object: "indexes", Limit: 1},
	} {
		if _, _, _, err := s.metadataQuery(invalid); err == nil {
			t.Fatal(invalid)
		}
	}
}

func TestMetadataRejectsUnscopedSourceRows(t *testing.T) {
	b := &metadataBackend{fakeBackend: &fakeBackend{iter: &fakeIterator{rows: []map[string]any{{"keyspace_name": "other", "table_name": "secrets"}}}}}
	s := &Session{backend: b, namespace: "tenant"}
	stats, err := s.Inspect(context.Background(), operations.MetadataSpec{Object: "tables", Limit: 10}, adapter.Limits{MaxRows: 10, MaxBytes: 4096, BatchRows: 1}, discardSink{})
	if err == nil || stats.Rows != 0 || b.iter.closed != 1 {
		t.Fatal(stats, err)
	}
}

func TestConnectionTestNeedsAResultAndPreservesReadGuard(t *testing.T) {
	b := &fakeBackend{iter: &fakeIterator{rows: []map[string]any{{"id": int64(1)}}}}
	if err := (&Session{backend: b}).Test(context.Background()); err != nil || b.queries != 1 || b.iter.closed != 1 {
		t.Fatal(err, b)
	}
	if err := (&Session{backend: &fakeBackend{iter: &fakeIterator{}}}).Test(context.Background()); err == nil {
		t.Fatal("empty connection response accepted")
	}
	if readCQL("UPDATE t SET n=n+1 WHERE id=1") {
		t.Fatal("write accepted by read path")
	}
}
