package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"
)

// CATALOG_ORACLE_TEST_DSN must use a disposable, non-system fixture owner.
func TestOracleObjectsLive(t *testing.T) {
	dsn := os.Getenv("CATALOG_ORACLE_TEST_DSN")
	if dsn == "" {
		t.Skip("CATALOG_ORACLE_TEST_DSN is not configured")
	}
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var owner, service string
	if err = db.QueryRowContext(ctx, "SELECT USER, SYS_CONTEXT('USERENV','SERVICE_NAME') FROM SYS.DUAL").Scan(&owner, &service); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(owner, "CODEX_CATALOG_") {
		t.Fatal("requires a disposable CODEX_CATALOG_ owner")
	}
	suffix := fmt.Sprint(time.Now().UnixNano())
	tableName, packageName, triggerName, viewName, invalidName, procedureName, mviewName := "Events_"+suffix, "Package_"+suffix, "Trigger_"+suffix, "View_"+suffix, "Invalid_"+suffix, "Procedure_"+suffix, "MView_"+suffix
	quote := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
	cleanup := []string{}
	defer func() {
		for i := len(cleanup) - 1; i >= 0; i-- {
			if _, err := db.Exec(cleanup[i]); err != nil {
				t.Errorf("fixture cleanup: %v", err)
			}
		}
	}()
	setup := []struct{ query, drop string }{
		{"CREATE TABLE " + quote(tableName) + " (ID NUMBER PRIMARY KEY, VALUE NUMBER)", "DROP TABLE " + quote(tableName) + " PURGE"},
		{"CREATE OR REPLACE PACKAGE " + quote(packageName) + " AUTHID CURRENT_USER AS\n" + strings.Repeat("-- long source test\n", 2200) + "FUNCTION convert_value(a NUMBER) RETURN NUMBER;\n FUNCTION convert_value(a VARCHAR2) RETURN VARCHAR2;\n PROCEDURE do_nothing;\nEND;", "DROP PACKAGE " + quote(packageName)},
		{"CREATE OR REPLACE PACKAGE BODY " + quote(packageName) + " AS\n FUNCTION convert_value(a NUMBER) RETURN NUMBER IS BEGIN RETURN a+1; END;\n FUNCTION convert_value(a VARCHAR2) RETURN VARCHAR2 IS BEGIN RETURN a; END;\n PROCEDURE do_nothing IS BEGIN NULL; END;\n END;", ""},
		{"CREATE OR REPLACE TRIGGER " + quote(triggerName) + " BEFORE INSERT ON " + quote(tableName) + " FOR EACH ROW BEGIN :NEW.VALUE := 9; END;", "DROP TRIGGER " + quote(triggerName)},
		{"ALTER TRIGGER " + quote(triggerName) + " DISABLE", ""},
		{"CREATE OR REPLACE VIEW " + quote(viewName) + " AS SELECT ID,VALUE FROM " + quote(tableName), "DROP VIEW " + quote(viewName)},
		{"CREATE OR REPLACE PROCEDURE " + quote(procedureName) + " (a IN NUMBER DEFAULT 1) AUTHID DEFINER AS BEGIN NULL; END;", "DROP PROCEDURE " + quote(procedureName)},
		{"CREATE MATERIALIZED VIEW " + quote(mviewName) + " BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS SELECT ID,VALUE FROM " + quote(tableName), "DROP MATERIALIZED VIEW " + quote(mviewName)},
	}
	for _, item := range setup {
		if _, err := db.ExecContext(ctx, item.query); err != nil {
			t.Fatal(err)
		}
		if item.drop != "" {
			cleanup = append(cleanup, item.drop)
		}
	}
	// Oracle creates INVALID objects while returning ORA-24344 to some drivers.
	_, err = db.ExecContext(ctx, "CREATE OR REPLACE FUNCTION "+quote(invalidName)+" RETURN NUMBER AS BEGIN RETURN missing_catalog_symbol; END;")
	if err != nil && !strings.Contains(err.Error(), "ORA-24344") {
		t.Fatal(err)
	}
	cleanup = append(cleanup, "DROP FUNCTION "+quote(invalidName))
	expected := map[string]string{"function": invalidName, "procedure": procedureName, "package": packageName, "package_body": packageName, "trigger": triggerName, "view": viewName, "materialized_view": mviewName}
	for _, kind := range Kinds("oracle") {
		list, err := Inspect(ctx, db, "oracle", service, kind, "")
		if err != nil {
			t.Fatalf("%s list: %v", kind, err)
		}
		found := false
		for _, object := range list.Objects {
			if object["name"] != expected[kind] {
				continue
			}
			found = true
			if object["definition"] != nil {
				t.Fatal("list eagerly loads source")
			}
			detail, err := Inspect(ctx, db, "oracle", service, kind, object["id"].(string))
			if err != nil {
				t.Fatalf("%s detail: %v", kind, err)
			}
			row := detail.Objects[0]
			if row["definition"] == nil {
				t.Fatalf("%s missing source", kind)
			}
			if kind == "package" {
				members := row["members"].([]RoutineMember)
				if len(members) != 3 || len(row["definition"].(string)) < 32767 || members[0].ID == members[1].ID {
					t.Fatalf("package metadata lost: %+v", members)
				}
			}
			if kind == "function" && (row["status"] != "INVALID" || len(row["diagnostics"].([]Diagnostic)) == 0) {
				t.Fatal("missing compilation diagnostics")
			}
			if kind == "trigger" && (row["enabled"] != "disabled" || len(detail.Relations) == 0) {
				t.Fatal("missing trigger state/attachment")
			}
			if (kind == "view" || kind == "materialized_view") && len(detail.Relations) == 0 {
				t.Fatal("missing relation dependencies")
			}
		}
		if !found {
			t.Fatalf("%s fixture not visible", kind)
		}
	}
	missing, err := Inspect(ctx, db, "oracle", service, "package", "1' OR 1=1 --")
	if err != nil || len(missing.Objects) != 0 {
		t.Fatal("unsafe native identity")
	}
	var count int
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(tableName)).Scan(&count); err != nil || count != 0 {
		t.Fatal("inspection executed code")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	_, writeErr := tx.ExecContext(ctx, "INSERT INTO "+quote(tableName)+" (ID,VALUE) VALUES (1,1)")
	tx.Rollback()
	if writeErr == nil || !strings.Contains(writeErr.Error(), "ORA-01456") {
		t.Fatalf("read-only transaction did not reject DML: %v", writeErr)
	}
}
