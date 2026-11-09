//go:build duckdb_arrow

package sheets

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/files"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func (s *Session) execute(ctx context.Context, q adapter.Query, metadata *operations.MetadataSpec, sink adapter.Sink) (adapter.QueryStats, error) {
	path, sum, size, release, err := s.materialize(ctx, false)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer release()
	file, err := os.Open(path)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	session, err := files.Open(ctx, file, path, "google_sheets", filesnapshot.Descriptor{Version: 1, Format: "duckdb", Bytes: size, SHA256: sum}, s.limits)
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

// refresh is Test(): with a usable cache it rebuilds and republishes the
// snapshot; otherwise it only proves the tabs are readable, as before.
func (s *Session) refresh(ctx context.Context) error {
	if cacheFromEnv() == nil || !serviceAccount(s.credential) {
		_, _, err := s.tabs(ctx)
		return err
	}
	_, _, _, release, err := s.materialize(ctx, true)
	if err == nil {
		release()
	}
	return err
}

// materialize returns a verified snapshot path and its digest. With a cache and
// a successful Drive version check it reuses (or builds once and publishes) the
// shared entry and holds a shared lock on it until release; otherwise it builds
// a private snapshot exactly as before.
func (s *Session) materialize(ctx context.Context, force bool) (path, sum string, size int64, release func(), err error) {
	cache := cacheFromEnv()
	version, ok := "", false
	if cache != nil {
		version, ok = s.driveVersion(ctx)
	}
	if !ok {
		dir, err := os.MkdirTemp("", "kelvo-sheets-")
		if err != nil {
			return "", "", 0, nil, err
		}
		path, sum, size, err = s.build(ctx, dir)
		if err != nil {
			os.RemoveAll(dir)
			return "", "", 0, nil, err
		}
		return path, sum, size, func() { os.RemoveAll(dir) }, nil
	}
	key := s.cacheKey(version)
	// Readers share the entry; a miss upgrades to exclusive so concurrent
	// processes build once and the waiters reuse the published result.
	lock, err := cache.lock(key, syscall.LOCK_SH)
	if err != nil {
		return "", "", 0, nil, err
	}
	release = func() { lock.Close() }
	fresh := func() (cacheMeta, bool) {
		meta, ok := cache.meta(key)
		return meta, ok && !force && meta.SpreadsheetID == s.id && meta.Version == version
	}
	meta, hit := fresh()
	if !hit {
		if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err == nil {
			meta, hit = fresh()
		}
		if err == nil && !hit {
			meta, err = s.buildEntry(ctx, cache, key, version)
		}
		if err == nil {
			err = syscall.Flock(int(lock.Fd()), syscall.LOCK_SH)
		}
		if err != nil {
			release()
			return "", "", 0, nil, err
		}
	}
	if hit {
		now := time.Now()
		os.Chtimes(filepath.Join(cache.dir, key, "meta.json"), now, now) // last used, for LRU
	}
	path = filepath.Join(cache.dir, key, "snapshot.duckdb")
	if !hit {
		cache.evict(key, s.id, version)
	}
	return path, meta.SHA256, meta.Bytes, release, nil
}

func (s *Session) buildEntry(ctx context.Context, cache *sheetCache, key, version string) (cacheMeta, error) {
	tmp, err := os.MkdirTemp(cache.dir, ".tmp-")
	if err != nil {
		return cacheMeta{}, err
	}
	defer os.RemoveAll(tmp)
	_, sum, size, err := s.build(ctx, tmp)
	if err != nil {
		return cacheMeta{}, err
	}
	meta := cacheMeta{SpreadsheetID: s.id, Version: version, BuiltAt: time.Now().UTC(), Bytes: size, SHA256: sum}
	return meta, cache.publish(key, tmp, meta)
}

// build fetches the sheet and writes dir/snapshot.duckdb, fsynced.
func (s *Session) build(ctx context.Context, dir string) (path, sum string, size int64, err error) {
	tables, err := s.snapshot(ctx)
	if err != nil {
		return "", "", 0, err
	}
	path = filepath.Join(dir, "snapshot.duckdb")
	if err = writeSnapshot(ctx, path, tables, s.limits.MemoryMB, s.limits.Threads); err != nil {
		return "", "", 0, err
	}
	file, err := os.Open(path)
	if err != nil {
		return "", "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err = io.Copy(hash, file)
	if err != nil || size > filesnapshot.MaxBytes {
		return "", "", 0, adapter.ErrLimit
	}
	if err = file.Sync(); err != nil {
		return "", "", 0, err
	}
	return path, hex.EncodeToString(hash.Sum(nil)), size, nil
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
