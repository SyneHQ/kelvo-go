//go:build duckdb_arrow

package sheets

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/files"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func (s *Session) execute(ctx context.Context, q adapter.Query, metadata *operations.MetadataSpec, sink adapter.Sink) (adapter.QueryStats, error) {
	tables, err := s.snapshot(ctx)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	dir, err := os.MkdirTemp("", "kelvo-sheets-")
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "snapshot.duckdb")
	if err = writeSnapshot(ctx, path, tables, s.limits.MemoryMB, s.limits.Threads); err != nil {
		return adapter.QueryStats{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil || int64(len(raw)) > filesnapshot.MaxBytes {
		return adapter.QueryStats{}, adapter.ErrLimit
	}
	sum := sha256.Sum256(raw)
	file, err := os.Open(path)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	session, err := files.Open(ctx, file, path, "google_sheets", filesnapshot.Descriptor{Version: 1, Format: "duckdb", Bytes: int64(len(raw)), SHA256: hex.EncodeToString(sum[:])}, s.limits)
	clear(raw)
	if err != nil {
		file.Close()
		return adapter.QueryStats{}, err
	}
	defer session.Close()
	if metadata != nil {
		return session.Inspect(ctx, *metadata, adapter.Limits{MaxRows: q.MaxRows, MaxBytes: q.MaxBytes, BatchRows: q.BatchRows}, sink)
	}
	return session.Query(ctx, q, sink)
}
func writeSnapshot(ctx context.Context, path string, tables []table, memoryMB, threads int) (err error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := db.Close(); err == nil {
			err = closeErr
		}
	}()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, q := range []string{"SET memory_limit='" + strconv.Itoa(memoryMB) + "MB'", "SET threads=" + strconv.Itoa(threads), "SET enable_external_access=false", "SET lock_configuration=true"} {
		if _, err = db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	quote := func(value string) string { return "\"" + strings.ReplaceAll(value, "\"", "\"\"") + "\"" }
	for _, table := range tables {
		columns := make([]string, len(table.Columns))
		placeholders := make([]string, len(columns))
		for i, name := range table.Columns {
			columns[i] = quote(name) + " VARCHAR"
			placeholders[i] = "?"
		}
		if _, err = tx.ExecContext(ctx, "CREATE TABLE "+quote(table.Name)+" ("+strings.Join(columns, ",")+")"); err != nil {
			return err
		}
		statement, err := tx.PrepareContext(ctx, "INSERT INTO "+quote(table.Name)+" VALUES ("+strings.Join(placeholders, ",")+")")
		if err != nil {
			return err
		}
		for _, row := range table.Rows {
			if _, err = statement.ExecContext(ctx, row...); err != nil {
				statement.Close()
				return err
			}
		}
		if err = statement.Close(); err != nil {
			return err
		}
	}
	return tx.Commit()
}
