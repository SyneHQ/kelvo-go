//go:build duckdb_arrow

package files

import (
	"context"
	"database/sql"
	"strconv"
)

func (s *Session) openDuckMutation(ctx context.Context, path string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, q := range []string{"SET autoinstall_known_extensions=false", "SET autoload_known_extensions=false", "SET allow_community_extensions=false", "SET memory_limit='" + strconv.Itoa(s.limits.MemoryMB) + "MB'", "SET threads=" + strconv.Itoa(s.limits.Threads), "SET max_temp_directory_size='" + strconv.Itoa(s.limits.MaxTempMB) + "MB'", "SET enable_external_access=false", "SET lock_configuration=true"} {
		if _, err = db.ExecContext(ctx, q); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}
