package oracle

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestObjectSnapshotBindsSchemaBeforeLimitAndStreamsVerifiedDocument(t *testing.T) {
	db, mock := oracleMock(t)
	oracleBegin(mock)
	query := strings.Replace(oracleObjects, "FETCH FIRST 501", "FETCH FIRST 2", 1)
	query = strings.Replace(query, "ORDER BY o.OWNER", "AND o.OWNER=:selected_schema ORDER BY o.OWNER", 1)
	mock.ExpectQuery(query).WithArgs("PACKAGE", "", "Mixed.Owner").WillReturnRows(sqlmock.NewRows([]string{"id", "schema", "name"}).AddRow("42", "Mixed.Owner", "pkg").AddRow("43", "Mixed.Owner", "another"))
	mock.ExpectRollback()
	s := &Session{Session: &sqlsession.Session{Pool: db, Engine: "oracle"}, database: "PDB", schema: "Mixed.Owner"}
	sink := &oracleSink{}
	defer sink.close()
	stats, err := s.Inspect(context.Background(), operations.MetadataSpec{Object: "objects", ObjectKind: "package", Limit: 1}, adapter.Limits{MaxRows: 1, MaxBytes: 4096, BatchRows: 1}, sink)
	if err != nil || stats.Rows != 1 || len(sink.records) != 1 {
		t.Fatal(stats, err)
	}
	var result Result
	if json.Unmarshal([]byte(sink.records[0].Column(0).(*array.String).Value(0)), &result) != nil || len(result.Objects) != 1 || !result.Truncated || result.Objects[0]["database"] != "PDB" {
		t.Fatal("object snapshot changed")
	}
}

func TestObjectSnapshotScopeAndBudgetDenialBeforeQuery(t *testing.T) {
	db, _ := oracleMock(t)
	s := &Session{Session: &sqlsession.Session{Pool: db, Engine: "oracle"}, database: "PDB", schema: "APP"}
	for _, spec := range []operations.MetadataSpec{
		{Object: "objects", ObjectKind: "package", Limit: 501},
		{Object: "objects", ObjectKind: "package", Limit: 10, Cursor: "1"},
		{Object: "objects", ObjectKind: "package", Limit: 10, Target: operations.ObjectRef{Schema: "other"}},
		{Object: "objects", ObjectKind: "package", Limit: 10, Target: operations.ObjectRef{Catalog: "other"}},
		{Object: "objects", ObjectKind: "execute", Limit: 10},
	} {
		sink := &oracleSink{}
		_, err := s.Inspect(context.Background(), spec, adapter.Limits{MaxRows: 10, MaxBytes: 4096, BatchRows: 1}, sink)
		sink.close()
		if err == nil {
			t.Fatal("invalid object snapshot accepted")
		}
	}
}
