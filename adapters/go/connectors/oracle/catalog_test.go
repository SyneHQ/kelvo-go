package oracle

import (
	"context"
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"strings"
	"testing"
)

func oracleMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		db.Close()
	})
	return db, mock
}
func oracleBegin(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec("SET TRANSACTION READ ONLY").WillReturnResult(sqlmock.NewResult(0, 0))
}
func oracleCatalogRows(kind, name string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "schema", "name", "status", "enabled", "trigger_scope", "table_schema", "table_name"}).AddRow("42", "Mixed.Owner", name, "INVALID", "disabled", "SCHEMA", nil, nil)
}
func oracleDetailBase(mock sqlmock.Sqlmock, kind, name, query string, source string) {
	oracleBegin(mock)
	mock.ExpectQuery(oracleObjects).WithArgs(kind, "42").WillReturnRows(oracleCatalogRows(kind, name))
	args := mock.ExpectQuery(query)
	if query == oracleSource {
		args.WithArgs("Mixed.Owner", name, kind)
	} else {
		args.WithArgs("Mixed.Owner", name)
	}
	args.WillReturnRows(sqlmock.NewRows([]string{"TEXT"}).AddRow(source))
	mock.ExpectQuery(oracleErrors).WithArgs("Mixed.Owner", name, kind).WillReturnRows(sqlmock.NewRows([]string{"line", "position", "text", "severity"}).AddRow("4", "7", "PLS-00201: identifier must be declared", "ERROR"))
}
func oracleNoDeps(mock sqlmock.Sqlmock, kind string) {
	mock.ExpectQuery(oracleDependencies).WithArgs("42", kind).WillReturnRows(sqlmock.NewRows([]string{"id", "schema", "name", "object_type", "database_link", "role"}))
}

func TestOracleFailsClosedWithoutReadOnlyTransaction(t *testing.T) {
	db, mock := oracleMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("SET TRANSACTION READ ONLY").WillReturnError(errors.New("read-only unavailable"))
	mock.ExpectRollback()
	if _, err := Inspect(context.Background(), db, "oracle", "PDB", "package", ""); err == nil {
		t.Fatal("must not inspect without read-only mode")
	}
}
func TestOracleListUsesBoundNativeIdentityAndNoDefinitions(t *testing.T) {
	db, mock := oracleMock(t)
	oracleBegin(mock)
	id := "42' OR 1=1 --"
	mock.ExpectQuery(oracleObjects).WithArgs("PACKAGE", id).WillReturnRows(sqlmock.NewRows([]string{"id", "schema", "name"}))
	mock.ExpectRollback()
	result, err := Inspect(context.Background(), db, "ORACLE", "PDB", "package", id)
	if err != nil || len(result.Objects) != 0 {
		t.Fatalf("%+v %v", result, err)
	}
}
func TestOraclePackageSourceOverloadsAndDependencies(t *testing.T) {
	db, mock := oracleMock(t)
	source := "PACKAGE \"Odd.Package\" AS\n" + strings.Repeat("-- comment\n", 700) + "END;\n"
	oracleDetailBase(mock, "PACKAGE", "Odd.Package", oracleSource, source)
	mock.ExpectQuery(oracleMembers).WithArgs("Mixed.Owner", "Odd.Package", "PACKAGE").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "overload", "authid"}).AddRow("1", "RUN", "1", "CURRENT_USER").AddRow("2", "RUN", "2", "CURRENT_USER").AddRow("3", "EMPTY", nil, "CURRENT_USER"))
	mock.ExpectQuery(oracleArguments).WithArgs("Mixed.Owner", "Odd.Package", "PACKAGE").WillReturnRows(sqlmock.NewRows([]string{"member_id", "position", "name", "mode", "data_type", "defaulted", "type_owner", "type_name", "type_subname", "type_link"}).AddRow("1", "0", nil, "OUT", "NUMBER", nil, nil, nil, nil, nil).AddRow("1", "1", "ARG", "IN", "NUMBER", "Y", nil, nil, nil, nil).AddRow("2", "1", "ARG", "IN/OUT", "PL/SQL RECORD", "N", "Mixed.Owner", "Odd.Package", "T_ROW", nil))
	mock.ExpectQuery(oracleDependencies).WithArgs("42", "PACKAGE").WillReturnRows(sqlmock.NewRows([]string{"id", "schema", "name", "object_type", "database_link", "role"}).AddRow("43", "Mixed.Owner", "Odd.Package", "PACKAGE BODY", nil, "used_by").AddRow(nil, "REMOTE", "SALES", "TABLE", "WAREHOUSE_LINK", "uses").AddRow(nil, "SYS", "DUAL", "SYNONYM", nil, "uses"))
	mock.ExpectQuery(oracleLinkedObject).WithArgs("Mixed.Owner", "Odd.Package", "PACKAGE BODY").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("43"))
	mock.ExpectRollback()
	result, err := Inspect(context.Background(), db, "oracle", "PDB", "package", "42")
	if err != nil {
		t.Fatal(err)
	}
	object := result.Objects[0]
	if object["definition"] != source || object["database"] != "PDB" {
		t.Fatal("source or service lost")
	}
	members := object["members"].([]RoutineMember)
	if len(members) != 3 || members[0].Signature != "ARG IN NUMBER DEFAULT …" || members[0].ReturnType != "NUMBER" || members[1].Signature != "ARG IN/OUT Mixed.Owner.Odd.Package.T_ROW" || members[2].Signature != "" || members[0].ID == members[1].ID {
		t.Fatalf("members: %+v", members)
	}
	if len(result.Relations) != 3 || !result.Relations[1].Unavailable || result.Relations[1].DatabaseLink != "WAREHOUSE_LINK" || result.Relations[2].Kind != "other" {
		t.Fatalf("relations: %+v", result.Relations)
	}
	if object["diagnostics"].([]Diagnostic)[0].Line != 4 {
		t.Fatal("lost error position")
	}
}
func TestOracleSchemaTriggerHasNoInventedTable(t *testing.T) {
	db, mock := oracleMock(t)
	oracleDetailBase(mock, "TRIGGER", "DDL_LOG", oracleSource, "TRIGGER DDL_LOG AFTER DDL ON SCHEMA BEGIN NULL; END;")
	oracleNoDeps(mock, "TRIGGER")
	mock.ExpectRollback()
	result, err := Inspect(context.Background(), db, "oracle", "PDB", "trigger", "42")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Relations) != 0 || result.Objects[0]["status"] != "INVALID" || result.Objects[0]["enabled"] != "disabled" {
		t.Fatal("trigger scope or status conflated")
	}
}
func TestOracleViewReadsFullLongNotTextVC(t *testing.T) {
	db, mock := oracleMock(t)
	source := "SELECT '" + strings.Repeat("x", 9000) + "' FROM dual"
	oracleDetailBase(mock, "VIEW", "Long View", oracleViewSource, source)
	oracleNoDeps(mock, "VIEW")
	mock.ExpectRollback()
	result, err := Inspect(context.Background(), db, "oracle", "PDB", "view", "42")
	if err != nil || result.Objects[0]["definition"] != source {
		t.Fatalf("view truncated: %v", err)
	}
}
func TestOracleSourceBoundsAndScanFailure(t *testing.T) {
	for _, scanFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "oversized", true: "scan failure"}[scanFailure], func(t *testing.T) {
			db, mock := oracleMock(t)
			mock.ExpectBegin()
			rows := sqlmock.NewRows([]string{"TEXT"}).AddRow(strings.Repeat("x", (1<<20)+1))
			if scanFailure {
				rows = sqlmock.NewRows([]string{"TEXT"}).AddRow("source").RowError(0, errors.New("read failed"))
			}
			mock.ExpectQuery(oracleSource).WillReturnRows(rows)
			mock.ExpectRollback()
			tx, _ := db.Begin()
			defer tx.Rollback()
			source, large, err := readOracleSource(context.Background(), tx, oracleSource)
			if source != "" || large == scanFailure || (scanFailure && err == nil) {
				t.Fatalf("partial SQL exposed: %t %v", large, err)
			}
		})
	}
}
func TestOracleUnsupportedKindNeverBegins(t *testing.T) {
	db, _ := oracleMock(t)
	if _, err := Inspect(context.Background(), db, "oracle", "PDB", "execute", ""); err != ErrUnsupported {
		t.Fatal(err)
	}
}
