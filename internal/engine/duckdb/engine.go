//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package duckdb implements the developer-preview DuckDB execution adapter.
package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	arrowutil "github.com/apache/arrow-go/v18/arrow/util"

	duck "github.com/duckdb/duckdb-go/v2"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// Engine creates a fresh DuckDB database instance for every execution. It is a
// single-trust-domain developer-preview adapter, not a SQL sandbox.
type Engine struct {
	config catalog.Config
	limits query.Limits
}

func New(config catalog.Config, limits query.Limits) (*Engine, error) {
	if limits.MaxRows < 1 || limits.MaxBytes < 1 || limits.Timeout <= 0 || limits.MemoryMB < 1 || limits.Threads < 1 || limits.MaxTempMB < 1 {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid DuckDB execution limits")
	}
	if config.ExtensionDirectory != "" && !filepath.IsAbs(config.ExtensionDirectory) {
		return nil, query.NewError("INVALID_ARGUMENT", "Extension directory must be absolute")
	}
	for _, source := range config.Sources {
		if !catalog.ValidID(source.ID) {
			return nil, query.NewError("INVALID_ARGUMENT", "Source ID is invalid")
		}
		if err := source.ValidateFederation(); err != nil {
			return nil, query.NewError("CONFIGURATION_ERROR", "Invalid custom federation source")
		}
		if err := validateObjectSource(source); err != nil {
			return nil, err
		}
	}
	return &Engine{config: config, limits: limits}, nil
}

func (e *Engine) Execute(parent context.Context, req query.Request, sink query.Sink) (stats query.Stats, err error) {
	stats.Backend = "duckdb"
	stats.EngineStreaming = false // v2.10506.0 uses DuckDB's non-streaming pending result API.
	started := time.Now()
	defer func() { stats.DurationNS = time.Since(started).Nanoseconds() }()

	if sink == nil || strings.TrimSpace(req.SQL) == "" {
		return stats, query.NewError("INVALID_ARGUMENT", "Query and result sink are required")
	}
	// The original public DuckDB API predates mode. Preserve its implicit federation.
	if req.Mode == "" {
		req.Mode = "federated"
	}
	if err := query.ValidateRequest(req); err != nil {
		return stats, err
	}
	if req.Mode != "federated" {
		return stats, query.NewError("INVALID_ARGUMENT", "DuckDB requires federated execution mode")
	}
	sources, selectErr := e.config.Select(req.Sources)
	if selectErr != nil {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if err := validateObjectCombination(sources); err != nil {
		return stats, err
	}
	if containsDeniedCapability(req.SQL) {
		return stats, query.NewError("PERMISSION_DENIED", "Query uses a capability unavailable to this execution")
	}
	values, valueErr := req.Values()
	if valueErr != nil {
		return stats, valueErr
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()

	tempDir, tempErr := os.MkdirTemp("", "kelvo-duckdb-")
	if tempErr != nil {
		return stats, query.NewError("QUERY_FAILED", "Could not prepare query workspace")
	}
	defer os.RemoveAll(tempDir)

	db, openErr := sql.Open("duckdb", ":memory:")
	if openErr != nil {
		return stats, query.NewError("QUERY_FAILED", "Could not initialize query engine")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)

	conn, connErr := db.Conn(ctx)
	if connErr != nil {
		return stats, publicError(connErr)
	}
	defer conn.Close()
	bindings := &federationBindings{}
	defer func() {
		// Native Arrow scan callbacks remain referenced by bound views until the
		// connection/database have both released their plans.
		closeErr := errors.Join(conn.Close(), db.Close())
		bindings.Close()
		stats.Federation = bindings.stats()
		for _, scan := range stats.Federation {
			stats.SourceWireBytes += scan.SourceWireBytes
		}
		// Stream-release callbacks run during query/connection teardown. A
		// contained callback failure must prevent a successful Arrow export.
		if err == nil {
			err = publicError(bindings.callbackError(closeErr))
		}
	}()

	prepareStarted := time.Now()
	err = conn.Raw(func(raw any) error {
		driverConn, ok := raw.(driver.Conn)
		if !ok {
			return errors.New("unexpected DuckDB driver connection")
		}
		if err := configure(ctx, raw, e.config.ExtensionDirectory, tempDir, e.limits); err != nil {
			return err
		}
		if err := attachSources(ctx, raw, sources, e.config.ExtensionDirectory, tempDir); err != nil {
			return err
		}
		if err := bindings.attach(ctx, raw, sources, e.limits); err != nil {
			return err
		}
		if err := lockSourceAccess(ctx, raw, sources, tempDir); err != nil {
			return err
		}
		if err := validateReadOnly(ctx, raw, req.SQL); err != nil {
			return err
		}
		bounded, err := boundedSelect(normalizeValidatedSelect(req.SQL), e.limits.MaxRows)
		if err != nil {
			return err
		}
		stats.PrepareNS = time.Since(prepareStarted).Nanoseconds()
		return deliver(ctx, driverConn, bounded, values, e.limits, sink, &stats)
	})
	if err = bindings.callbackError(err); err != nil {
		return stats, publicError(err)
	}
	return stats, nil
}

func configure(ctx context.Context, raw any, extensionDir, tempDir string, limits query.Limits) error {
	exec, ok := raw.(driver.ExecerContext)
	if !ok {
		return errors.New("DuckDB driver does not support trusted configuration")
	}
	settings := []string{
		"SET autoinstall_known_extensions = false",
		"SET autoload_known_extensions = false",
		"SET allow_community_extensions = false",
		"SET allow_unredacted_secrets = false",
		"SET allow_persistent_secrets = false",
		"SET enable_logging = false",
		"SET memory_limit = '" + strconv.Itoa(limits.MemoryMB) + "MB'",
		"SET threads = " + strconv.Itoa(limits.Threads),
		"SET temp_directory = '" + quoteLiteral(tempDir) + "'",
		"SET max_temp_directory_size = '" + strconv.Itoa(limits.MaxTempMB) + "MB'",
	}
	if extensionDir != "" {
		settings = append(settings, "SET extension_directory = '"+quoteLiteral(extensionDir)+"'")
	}
	for _, statement := range settings {
		if _, err := exec.ExecContext(ctx, statement, nil); err != nil {
			return fmt.Errorf("trusted engine configuration failed: %w", err)
		}
	}
	return nil
}

func attachSources(ctx context.Context, raw any, sources []catalog.Source, extensionDir, tempDir string) error {
	exec, ok := raw.(driver.ExecerContext)
	if !ok {
		return errors.New("DuckDB driver does not support trusted source setup")
	}
	for _, source := range sources {
		if source.Adapter != "" {
			return errors.New("source adapters are unavailable to DuckDB federation")
		}
		if source.Federation != nil {
			continue
		}
		id := quoteIdentifier(source.ID)
		var statement string
		switch source.Type {
		case "csv":
			statement = "CREATE VIEW " + id + " AS SELECT * FROM read_csv_auto('" + quoteLiteral(source.Path) + "')"
		case "parquet":
			if source.Range != nil {
				if err := prepareObjectRange(ctx, exec, source, extensionDir, tempDir); err != nil {
					return err
				}
			}
			statement = "CREATE VIEW " + id + " AS SELECT * FROM read_parquet('" + quoteLiteral(source.Path) + "')"
		case "sqlite":
			if err := loadApprovedExtension(ctx, exec, source.Type, extensionDir, tempDir); err != nil {
				return err
			}
			statement = "ATTACH '" + quoteLiteral(source.Path) + "' AS " + id + " (TYPE SQLITE, READ_ONLY)"
		case "duckdb":
			statement = "ATTACH '" + quoteLiteral(source.Path) + "' AS " + id + " (READ_ONLY)"
		case "postgres", "mysql":
			dsn := os.Getenv(source.DSNEnv)
			if dsn == "" {
				return errors.New("source credentials are unavailable")
			}
			if err := loadApprovedExtension(ctx, exec, source.Type, extensionDir, tempDir); err != nil {
				return err
			}
			secretName := "kelvo_source_" + source.ID
			secretSQL, publicPath, err := sourceSecret(source.Type, secretName, dsn)
			if err != nil {
				return err
			}
			if _, err := exec.ExecContext(ctx, secretSQL, nil); err != nil {
				// Driver errors can echo CREATE SECRET and its private values.
				return errors.New("trusted source secret could not be prepared")
			}
			statement = "ATTACH '" + quoteLiteral(publicPath) + "' AS " + id + " (TYPE " + source.Type + ", SECRET " + quoteIdentifier(secretName) + ", READ_ONLY)"
		default:
			return errors.New("unsupported source")
		}
		if _, err := exec.ExecContext(ctx, statement, nil); err != nil {
			return fmt.Errorf("trusted source setup failed: %w", err)
		}
	}
	return nil
}

// lockSourceAccess runs only after Kelvo has registered every trusted source.
// File-backed requests are then limited to the exact source paths and temporary
// workspace. Network connectors need external access for their already-attached
// remote scans, so this developer-preview adapter leaves it enabled for those
// requests; it is not a security boundary or SQL sandbox.
func lockSourceAccess(ctx context.Context, raw any, sources []catalog.Source, tempDir string) error {
	if err := validateObjectCombination(sources); err != nil {
		return err
	}
	exec, ok := raw.(driver.ExecerContext)
	if !ok {
		return errors.New("DuckDB driver does not support source lockdown")
	}
	paths := []string{tempDir}
	networkSource := false
	for _, source := range sources {
		if source.Adapter != "" {
			return errors.New("source adapters are unavailable to DuckDB federation")
		}
		switch source.Type {
		case "csv", "parquet", "duckdb", "sqlite":
			paths = append(paths, source.Path)
		case "postgres", "mysql":
			// Custom adapters perform source I/O in Go; only the legacy DuckDB
			// extensions require broader native external access.
			networkSource = networkSource || source.Federation == nil
		}
	}
	if !networkSource {
		quoted := make([]string, 0, len(paths))
		for _, path := range paths {
			quoted = append(quoted, "'"+quoteLiteral(path)+"'")
		}
		if _, err := exec.ExecContext(ctx, "SET allowed_paths = ["+strings.Join(quoted, ", ")+"]", nil); err != nil {
			return fmt.Errorf("trusted source allowlist failed: %w", err)
		}
		// DuckDB creates temporary files beneath this directory, so allow the
		// directory itself without broadening access to any source parent.
		if _, err := exec.ExecContext(ctx, "SET allowed_directories = ['"+quoteLiteral(tempDir)+"']", nil); err != nil {
			return fmt.Errorf("trusted workspace allowlist failed: %w", err)
		}
		if _, err := exec.ExecContext(ctx, "SET enable_external_access = false", nil); err != nil {
			return fmt.Errorf("trusted source lockdown failed: %w", err)
		}
	}
	if _, err := exec.ExecContext(ctx, "SET lock_configuration = true", nil); err != nil {
		return fmt.Errorf("trusted configuration lock failed: %w", err)
	}
	return nil
}

func loadApprovedExtension(ctx context.Context, exec driver.ExecerContext, sourceType, extensionDir, tempDir string) error {
	if extensionDir == "" {
		return errors.New("approved extension directory is required")
	}
	canonicalName := map[string]string{
		"postgres": "postgres_scanner",
		"mysql":    "mysql_scanner",
		"sqlite":   "sqlite_scanner",
		"httpfs":   "httpfs",
	}[sourceType]
	if canonicalName == "" {
		return errors.New("unsupported approved extension")
	}
	canonicalPath := filepath.Join(extensionDir, canonicalName+".duckdb_extension")
	loadPath := canonicalPath
	if _, err := os.Stat(canonicalPath); err != nil {
		// Some approved catalogs retain a source-type filename even though the
		// extension entrypoint is named *_scanner. LOAD derives the entrypoint
		// from its filename, so give that trusted file a canonical private alias.
		legacyPath := filepath.Join(extensionDir, sourceType+".duckdb_extension")
		if _, legacyErr := os.Stat(legacyPath); legacyErr != nil {
			return errors.New("approved extension is unavailable")
		}
		loadPath = filepath.Join(tempDir, canonicalName+".duckdb_extension")
		if err := os.Symlink(legacyPath, loadPath); err != nil && !errors.Is(err, os.ErrExist) {
			return errors.New("approved extension could not be prepared")
		}
	}
	_, err := exec.ExecContext(ctx, "LOAD '"+quoteLiteral(loadPath)+"'", nil)
	if err != nil {
		return fmt.Errorf("approved extension could not be loaded: %w", err)
	}
	return nil
}

func validateReadOnly(ctx context.Context, raw any, sqlText string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	preparer, ok := raw.(driver.Conn)
	if !ok {
		return errors.New("DuckDB parser introspection is unavailable")
	}
	// The pinned driver's PrepareContext executes all statements preceding the
	// final one. Prepare extracts the SQL with DuckDB's own parser and rejects
	// any count other than one before preparing or executing a statement. Do not
	// replace this with PrepareContext or rely on a hand-written SQL lexer.
	// The worker process deadline bounds preparation, which lacks a context API.
	stmt, err := preparer.Prepare(sqlText)
	if err != nil {
		return query.NewError("PERMISSION_DENIED", "Only one valid read-only SELECT statement is permitted")
	}
	defer stmt.Close()
	if err := ctx.Err(); err != nil {
		return err
	}
	classified, ok := stmt.(interface{ StatementType() (duck.StmtType, error) })
	if !ok {
		return errors.New("DuckDB statement introspection is unavailable")
	}
	kind, err := classified.StatementType()
	if err != nil {
		return fmt.Errorf("SQL could not be classified: %w", err)
	}
	if kind != duck.STATEMENT_TYPE_SELECT {
		return query.NewError("PERMISSION_DENIED", "Only one read-only SELECT statement is permitted")
	}
	return nil
}

// This conservative preflight guard blocks user-invoked file/network/extension
// entry points before source setup. It is defense in depth; SQL classification
// and this denylist do not sandbox native code or every table-function alias.
// Shared deployments must execute workers inside an OS sandbox.
func containsDeniedCapability(sqlText string) bool {
	denied := map[string]struct{}{
		"attach": {}, "detach": {}, "install": {}, "load": {}, "pragma": {},
		"insert": {}, "update": {}, "delete": {}, "merge": {}, "create": {},
		"drop": {}, "alter": {}, "copy": {}, "vacuum": {}, "call": {},
		"set": {}, "reset": {}, "begin": {}, "commit": {}, "rollback": {},
		"read_csv": {}, "read_csv_auto": {}, "read_parquet": {}, "read_json": {},
		"glob": {}, "http_get": {}, "load_extension": {}, "query": {}, "query_table": {},
		"read_blob": {}, "read_text": {}, "postgres_query": {}, "mysql_query": {},
		"duckdb_secrets": {}, "getenv": {}, "postgres_scan": {}, "mysql_scan": {}, "sqlite_scan": {},
		"duckdb_logs": {}, "duckdb_logs_parsed": {}, "write_log": {},
		"read_json_auto": {}, "read_ndjson": {}, "read_ndjson_auto": {},
		"read_json_objects": {}, "read_json_objects_auto": {}, "read_ndjson_objects": {},
		"arrow_scan": {}, "arrow_scan_dumb": {},
		"parquet_scan": {}, "csv_scan": {}, "json_scan": {}, "sniff_csv": {},
		"postgres_scan_pushdown": {}, "postgres_execute": {}, "mysql_execute": {},
		"mysql_bind_params": {}, "mysql_create_params": {}, "mysql_pin_connection": {},
		"mysql_close_pinned_connection": {}, "mysql_configure_pool": {},
		"postgres_attach": {}, "postgres_clear_cache": {}, "mysql_clear_cache": {},
		"sqlite_attach": {}, "sqlite_query": {}, "sqlite_execute": {},
	}
	for _, token := range strings.FieldsFunc(strings.ToLower(sqlText), func(r rune) bool { return !(r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9') }) {
		if _, found := denied[token]; found {
			return true
		}
	}
	return false
}

// normalizeValidatedSelect removes only an exact terminal semicolon after the
// driver has verified that the input contains one SELECT statement. It does not
// attempt to parse comments or quoted literals; non-terminal semicolons remain
// for the driver validation and wrapper to reject.
func normalizeValidatedSelect(sqlText string) string {
	sqlText = strings.TrimSpace(sqlText)
	if strings.HasSuffix(sqlText, ";") {
		return strings.TrimSpace(strings.TrimSuffix(sqlText, ";"))
	}
	return sqlText
}

func boundedSelect(sqlText string, maxRows int64) (string, error) {
	// Statement classification above rejects non-SELECT and multi-statement input.
	// The wrapper makes overflow observable: maxRows+1 produces an explicit failure in deliver.
	if maxRows == int64(^uint64(0)>>1) {
		return "", query.NewError("INVALID_ARGUMENT", "Row limit is too large")
	}
	return "SELECT * FROM (" + sqlText + "\n) AS kelvo_result LIMIT " + strconv.FormatInt(maxRows+1, 10), nil
}

func deliver(ctx context.Context, driverConn driver.Conn, sqlText string, values []any, limits query.Limits, sink query.Sink, stats *query.Stats) error {
	arrowQuery, err := duck.NewArrowFromConn(driverConn)
	if err != nil {
		return err
	}
	reader, err := arrowQuery.QueryContext(ctx, sqlText, values...)
	if err != nil {
		return err
	}
	defer reader.Release()
	if err := sink.Schema(reader.Schema()); err != nil {
		return err
	}
	for reader.Next() {
		record := reader.RecordBatch()
		if record == nil {
			return errors.New("DuckDB returned an empty result batch")
		}
		nextRows := stats.Rows + record.NumRows()
		nextBytes := stats.Bytes + arrowutil.TotalRecordSize(record)
		if nextRows > limits.MaxRows {
			return query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds row limit")
		}
		if nextBytes > limits.MaxBytes {
			return query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds byte limit")
		}
		if err := sink.Write(record); err != nil {
			return err
		}
		stats.Rows, stats.Bytes, stats.Batches = nextRows, nextBytes, stats.Batches+1
	}
	return reader.Err()
}

func quoteLiteral(s string) string    { return strings.ReplaceAll(s, "'", "''") }
func quoteIdentifier(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func publicError(err error) error {
	if err == nil {
		return nil
	}
	var public *query.Error
	if errors.As(err, &public) {
		return public
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return query.NewError("DEADLINE_EXCEEDED", "Query deadline exceeded")
	}
	if errors.Is(err, context.Canceled) {
		return query.NewError("CANCELLED", "Query cancelled")
	}
	return query.NewError("QUERY_FAILED", "Query failed")
}

var _ query.Executor = (*Engine)(nil)
