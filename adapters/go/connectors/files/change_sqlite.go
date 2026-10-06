//go:build cgo

package files

import (
	"context"
	"database/sql"
)

func (s *Session) openMutation(ctx context.Context, path string) (*sql.DB, error) {
	if s.descriptor.Format == "duckdb" {
		return s.openDuckMutation(ctx, path)
	}
	db := sql.OpenDB(sqliteConnector{path: path, maxBytes: s.limits.MaxBytes, memoryMB: s.limits.MemoryMB, write: true})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}
