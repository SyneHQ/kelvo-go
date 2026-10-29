//go:build !slim

package runtime

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestExecuteReturnsTheSourcesStatementError(t *testing.T) {
	notNull := &pgconn.PgError{Severity: "ERROR", Code: "23502", Message: "null value in column \"name\" of relation \"meal_items\"\nviolates not-null constraint",
		Detail: "Failing row contains (1, null).", SchemaName: "public", TableName: "meal_items", ColumnName: "name"}
	r := runtimeRequest(t, operations.StatementExecute)
	r.Request.Spec.Statement.SQL = ""
	r.Request.Spec.Statement.Batch = &operations.BatchSpec{Statements: []operations.BoundStatement{{SQL: "UPDATE a SET n=1"}, {SQL: "INSERT INTO meal_items (id) VALUES (1)"}}}
	r.RequestSHA256, _ = operations.Digest(r.Request)
	s := &processSession{errorResult: fmt.Errorf("statement 2: %w", notNull), result: adapter.ChangeResult{Outcome: "failed", Attempted: 2}}
	receipt, err := Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
		return s, nil
	}}.execute(context.Background(), r, io.Discard)
	if err == nil || receipt.ErrorCode != "SOURCE_FAILED" || receipt.Validate() != nil {
		t.Fatalf("receipt = %+v, err = %v", receipt, err)
	}
	got := receipt.SourceError
	if got == nil || got.SQLState != "23502" || got.Table != "meal_items" || got.Column != "name" || got.Detail != notNull.Detail ||
		strings.ContainsAny(got.Message, "\r\n") || got.Statement == nil || *got.Statement != 1 {
		t.Fatalf("source error = %+v", got)
	}
}

func TestSourceErrorWithholdsConnectionAndServerFailures(t *testing.T) {
	for _, err := range []error{
		&pgconn.PgError{Severity: "FATAL", Code: "53300", Message: "too many connections for role \"app\""},
		&pgconn.PgError{Severity: "ERROR", Code: "28000", Message: "role \"app\" is not permitted to log in"},
		&pgconn.PgError{Severity: "ERROR", Code: "57P01", Message: "terminating connection due to administrator command"},
		&pgconn.PgError{Severity: "ERROR", Code: "08006", Message: "connection failure"},
		&mysqldriver.MySQLError{Number: 1045, SQLState: [5]byte{'2', '8', '0', '0', '0'}, Message: "Access denied for user 'app'@'10.0.0.1'"},
		fmt.Errorf("dial tcp 10.0.0.1:5432: connect: connection refused"),
	} {
		if got := sourceError(err, nil); got != nil {
			t.Fatalf("%v crossed as %+v", err, got)
		}
	}
	if got := sourceError(&mysqldriver.MySQLError{Number: 1062, SQLState: [5]byte{'2', '3', '0', '0', '0'}, Message: "Duplicate entry '7' for key 'PRIMARY'"}, nil); got == nil || got.SQLState != "23000" {
		t.Fatalf("mysql statement error = %+v", got)
	}
	long := sourceText(strings.Repeat("é", operations.MaxSourceErrorText))
	if len(long) > operations.MaxSourceErrorText || !strings.HasPrefix(long, "é") || strings.HasSuffix(long, "\xc3") {
		t.Fatal("long text not trimmed on a rune boundary")
	}
}

// Real-PostgreSQL gate: the connector stack must keep the driver's error so a
// refused INSERT reaches the receipt with its SQLSTATE and constraint.
func TestPostgresRefusedInsertReachesTheReceipt(t *testing.T) {
	dsn := os.Getenv("KELVO_TEST_WARM_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set KELVO_TEST_WARM_POSTGRES_DSN for the real PostgreSQL gate")
	}
	database := "postgres"
	if u, err := url.Parse(dsn); err == nil && strings.Trim(u.Path, "/") != "" {
		database = strings.Trim(u.Path, "/")
	}
	run := func(sql string) operations.Receipt {
		r := runtimeRequest(t, operations.StatementExecute)
		r.Request.Connection = operations.ConnectionRef{ID: "saved-1", Database: database, Schema: "public"}
		r.Request.Spec.Statement.SQL = sql
		r.Request.Spec.Statement.Transaction = operations.TransactionAutocommit
		r.RequestSHA256, _ = operations.Digest(r.Request)
		r.Source = adapter.ConnectionSpec{Engine: "postgres", DSN: dsn, TenantID: "tenant-1", ConnectionID: "saved-1", Revision: "revision-1", Database: database, Schema: "public"}
		receipt, _ := Runner{}.execute(context.Background(), r, io.Discard)
		return receipt
	}
	conn, err := pgconn.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS kelvo_source_error_probe (id int PRIMARY KEY, name text NOT NULL)").ReadAll(); err != nil {
		t.Fatal(err)
	}
	receipt := run("INSERT INTO kelvo_source_error_probe (id) VALUES (1)")
	if receipt.Outcome != operations.Failed || receipt.Validate() != nil || receipt.SourceError == nil || receipt.SourceError.SQLState != "23502" || receipt.SourceError.Column != "name" {
		t.Fatalf("receipt = %+v source = %+v", receipt, receipt.SourceError)
	}
}
