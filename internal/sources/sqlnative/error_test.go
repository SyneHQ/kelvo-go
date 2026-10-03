// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

var privateDriverError = errors.New("private-password private-host SELECT private-table")

type errorScenario struct {
	stage                                     string
	err                                       error
	cancel                                    context.CancelFunc
	mapped                                    int
	rowsClosed, connectionsClosed, rolledBack bool
}

func (s *errorScenario) failure(stage string) error {
	if s.stage != stage {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	return fmt.Errorf("private wrapper: %w", s.err)
}

type errorConnector struct{ scenario *errorScenario }

func (c errorConnector) Connect(context.Context) (driver.Conn, error) {
	if err := c.scenario.failure("connect"); err != nil {
		return nil, err
	}
	return &errorConn{c.scenario}, nil
}
func (c errorConnector) Driver() driver.Driver { return errorDriver{} }

type errorDriver struct{}

func (errorDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unused") }

type errorConn struct{ scenario *errorScenario }

func (*errorConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (c *errorConn) Close() error                      { c.scenario.connectionsClosed = true; return nil }
func (*errorConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func (c *errorConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if err := c.scenario.failure("begin"); err != nil {
		return nil, err
	}
	return &errorTx{c.scenario}, nil
}
func (c *errorConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	if err := c.scenario.failure("read-only"); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}
func (c *errorConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if err := c.scenario.failure("query"); err != nil {
		return nil, err
	}
	return &errorRows{scenario: c.scenario}, nil
}

type errorTx struct{ scenario *errorScenario }

func (*errorTx) Commit() error      { return errors.New("commit forbidden") }
func (tx *errorTx) Rollback() error { tx.scenario.rolledBack = true; return nil }

type errorRows struct {
	scenario *errorScenario
	sent     bool
	columns  int
}

func (r *errorRows) Columns() []string {
	r.columns++
	// Changing driver column arity after ColumnTypes makes database/sql.Scan
	// reject the destination width without trusting fabricated driver text.
	if r.scenario.stage == "scan" && r.columns > 1 {
		return []string{"value", "unexpected"}
	}
	return []string{"value"}
}
func (*errorRows) ColumnTypeDatabaseTypeName(int) string { return "INT4" }
func (*errorRows) ColumnTypeNullable(int) (bool, bool)   { return true, true }
func (r *errorRows) Close() error                        { r.scenario.rowsClosed = true; return nil }
func (r *errorRows) Next(values []driver.Value) error {
	if r.sent {
		return io.EOF
	}
	r.sent = true
	if err := r.scenario.failure("rows"); err != nil {
		return err
	}
	values[0] = int64(7)
	return nil
}

type errorSink struct{ schemaErr, writeErr error }

func (s *errorSink) Schema(*arrow.Schema) error    { return s.schemaErr }
func (s *errorSink) Write(arrow.RecordBatch) error { return s.writeErr }

func errorEngine(t *testing.T, s *errorScenario) *Engine {
	t.Helper()
	t.Setenv("KELVO_SOURCE_ERROR_FIXTURE_DSN", "private-dsn")
	dialect := Dialect{SourceType: "fake", DriverName: "injected-error-fixture", ReadOnlyOption: true, ReadOnlySession: "SET TRANSACTION READ ONLY"}
	dialect.ValidateDSN = func(string) error { return s.failure("validate") }
	dialect.OpenDB = func(string) (*sql.DB, error) {
		if err := s.failure("open"); err != nil {
			return nil, err
		}
		return sql.OpenDB(errorConnector{s}), nil
	}
	dialect.ErrorCode = func(err error) string {
		s.mapped++
		if errors.Is(err, s.err) {
			return "PERMISSION_DENIED"
		}
		return ""
	}
	engine, err := New(catalog.Config{Sources: []catalog.Source{{ID: "raw", Type: "fake", DSNEnv: "KELVO_SOURCE_ERROR_FIXTURE_DSN"}}}, query.DefaultLimits(), dialect)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}
func executeErrorFixture(t *testing.T, engine *Engine, ctx context.Context, sink query.Sink) error {
	t.Helper()
	_, err := engine.Execute(ctx, query.Request{Mode: "native", ConnectionID: "raw", SQL: "SELECT value FROM fixture"}, sink)
	return err
}
func expectSafeSourceError(t *testing.T, err error, code, message string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
	actual := query.PublicError(err)
	if actual.Code != code || actual.Message != message {
		t.Fatalf("wrong public error: code=%s message=%q", actual.Code, actual.Message)
	}
	if strings.Contains(err.Error(), "private") || errors.Is(err, privateDriverError) {
		t.Fatal("raw driver error escaped")
	}
}

func TestNativeErrorMappingAcrossExecutionBoundaries(t *testing.T) {
	for _, tc := range []struct{ stage, message string }{
		{"validate", "Source connection configuration is invalid"},
		{"open", "Could not initialize source connection"},
		{"connect", "Could not connect to source"},
		{"begin", "Could not begin read-only source transaction"},
		{"read-only", "Could not enforce read-only source transaction"},
		{"query", "Source rejected query"},
		{"rows", "Source query did not complete"},
	} {
		t.Run(tc.stage, func(t *testing.T) {
			s := &errorScenario{stage: tc.stage, err: privateDriverError}
			engine := errorEngine(t, s)
			err := executeErrorFixture(t, engine, context.Background(), &errorSink{})
			expectSafeSourceError(t, err, "PERMISSION_DENIED", tc.message)
			if s.mapped != 1 {
				t.Fatalf("mapper called %d times", s.mapped)
			}
			if (tc.stage == "read-only" || tc.stage == "query" || tc.stage == "rows") && !s.rolledBack {
				t.Fatal("failed transaction not rolled back")
			}
			if tc.stage == "rows" && !s.rowsClosed {
				t.Fatal("failed rows not closed")
			}
		})
	}
}
func TestNativeScanFailureIsSanitized(t *testing.T) {
	s := &errorScenario{stage: "scan", err: privateDriverError}
	engine := errorEngine(t, s)
	err := executeErrorFixture(t, engine, context.Background(), &errorSink{})
	expectSafeSourceError(t, err, "QUERY_FAILED", "Source returned an invalid row")
	if !s.rowsClosed || !s.rolledBack {
		t.Fatal("scan failure leaked transaction resources")
	}
}
func TestNativeContextPrecedesClassificationAtEveryBoundary(t *testing.T) {
	for _, stage := range []string{"validate", "open", "connect", "begin", "read-only", "query", "rows"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := &errorScenario{stage: stage, err: privateDriverError, cancel: cancel}
			engine := errorEngine(t, s)
			err := executeErrorFixture(t, engine, ctx, &errorSink{})
			if query.PublicError(err).Code != "CANCELLED" || s.mapped != 0 {
				t.Fatalf("cancellation lost: code=%s mapper_calls=%d", query.PublicError(err).Code, s.mapped)
			}
		})
	}
}
func TestNativeTrustedCallbackAndSinkErrorsSurvive(t *testing.T) {
	for _, stage := range []string{"validate", "open", "schema", "write"} {
		t.Run(stage, func(t *testing.T) {
			intended := query.NewError("SCHEMA_MISMATCH", "Schema contract changed")
			s := &errorScenario{stage: stage, err: intended}
			engine := errorEngine(t, s)
			sink := &errorSink{}
			if stage == "schema" {
				sink.schemaErr = fmt.Errorf("private wrapper: %w", intended)
			}
			if stage == "write" {
				sink.writeErr = fmt.Errorf("private wrapper: %w", intended)
			}
			err := executeErrorFixture(t, engine, context.Background(), sink)
			expectSafeSourceError(t, err, "SCHEMA_MISMATCH", "Schema contract changed")
			if s.mapped != 0 {
				t.Fatal("trusted error passed to driver classifier")
			}
		})
	}
}
func TestNativeSourceCodeAllowlistAndCancellation(t *testing.T) {
	stages := []string{"Could not initialize source connection", "Could not connect to source", "Could not begin read-only source transaction", "Could not enforce read-only source transaction", "Source rejected query", "Source returned an invalid result", "Source returned an invalid row", "Source query did not complete"}
	// database/sql's driver column metadata API cannot return errors directly;
	// exercise the ColumnTypes error sanitization contract alongside every other
	// stage here, while executable driver fixtures cover attainable driver errors.
	for _, message := range stages {
		for _, code := range []string{"UNAUTHENTICATED", "PERMISSION_DENIED", "UNAVAILABLE", "INVALID_ARGUMENT", "CONFIGURATION_ERROR", "RESOURCE_EXHAUSTED", "NOT_SUPPORTED", "CANCELLED", "DEADLINE_EXCEEDED", "UNKNOWN_PRIVATE_CODE", ""} {
			engine := &Engine{dialect: Dialect{ErrorCode: func(error) string { return code }}}
			expected := code
			if code == "UNKNOWN_PRIVATE_CODE" || code == "" {
				expected = "QUERY_FAILED"
			}
			expectSafeSourceError(t, engine.sourceError(context.Background(), fmt.Errorf("wrapped: %w", privateDriverError), message), expected, message)
		}
	}
	engine := &Engine{dialect: Dialect{ErrorCode: func(error) string { t.Fatal("context must precede mapper"); return "" }}}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		err := engine.sourceError(context.Background(), fmt.Errorf("private wrapper: %w", cause), "stage")
		if query.PublicError(err).Code != query.PublicError(cause).Code {
			t.Fatal("wrapped context lost")
		}
	}
	// A driver is not allowed to inject a public message through a typed error.
	untrusted := query.NewError("UNAUTHENTICATED", "private driver message")
	engine.dialect.ErrorCode = nil
	expectSafeSourceError(t, engine.sourceError(context.Background(), untrusted, "static stage"), "QUERY_FAILED", "static stage")
}
func TestNativeLocalResultErrorsAreContextFirstAndNeverDriverMapped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	intended := query.NewError("RESOURCE_EXHAUSTED", "Result limit reached")
	err := localResultError(ctx, fmt.Errorf("private wrapper: %w", intended), "static stage")
	if query.PublicError(err).Code != "CANCELLED" {
		t.Fatal("local cancellation lost")
	}
	expectSafeSourceError(t, localResultError(context.Background(), privateDriverError, "static stage"), "QUERY_FAILED", "static stage")
}
