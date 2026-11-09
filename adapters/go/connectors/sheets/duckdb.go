//go:build duckdb_arrow

package sheets

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
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
	for retry := false; ; retry = true {
		snap, err := s.materialize(ctx, retry)
		if err != nil {
			return adapter.QueryStats{}, err
		}
		defer snap.release()
		file, err := os.Open(snap.path)
		if err != nil {
			return adapter.QueryStats{}, err
		}
		session, err := files.Open(ctx, file, snap.path, "google_sheets", filesnapshot.Descriptor{Version: 1, Format: "duckdb", Bytes: snap.size, SHA256: snap.sum}, s.limits)
		if err != nil {
			file.Close()
			if snap.drop != nil && !retry && errors.Is(err, adapter.ErrInvalid) {
				snap.release() // a corrupt cached entry: drop it and rebuild once
				snap.drop()
				continue
			}
			return adapter.QueryStats{}, err
		}
		defer session.Close()
		if metadata != nil {
			return session.Inspect(ctx, *metadata, adapter.Limits{MaxRows: q.MaxRows, MaxBytes: q.MaxBytes, BatchRows: q.BatchRows}, sink)
		}
		return session.Query(ctx, q, sink)
	}
}

// refresh is Test(): with a usable cache it rebuilds and republishes the
// snapshot; otherwise it only proves the tabs are readable, as before.
func (s *Session) refresh(ctx context.Context) error {
	if cacheFromEnv() == nil || !serviceAccount(s.credential) {
		_, _, err := s.tabs(ctx)
		return err
	}
	snap, err := s.materialize(ctx, true)
	if err == nil {
		snap.release()
	}
	return err
}

type materialized struct {
	path, sum string
	size      int64
	release   func()
	drop      func() // set on a cache hit: removes the entry
}

// materialize returns a snapshot path and its digest. With a cache and a
// successful Drive version check it reuses (or builds once and publishes) the
// shared entry, holding a shared lock until release. Any cache I/O failure
// falls back to a private build, exactly as before; only Sheets fetch and
// context errors are returned.
func (s *Session) materialize(ctx context.Context, force bool) (materialized, error) {
	var tables []table
	if cache := cacheFromEnv(); cache != nil {
		if version, ok := s.driveVersion(ctx); ok {
			snap, fetched, err := s.fromCache(ctx, cache, s.cacheKey(version), version, force)
			if err != nil || snap.release != nil {
				return snap, err
			}
			tables = fetched
		}
	}
	var err error
	if tables == nil {
		if tables, err = s.snapshot(ctx); err != nil {
			return materialized{}, err
		}
	}
	dir, err := os.MkdirTemp("", "kelvo-sheets-")
	if err != nil {
		return materialized{}, err
	}
	path, sum, size, err := s.build(ctx, dir, tables)
	if err != nil {
		os.RemoveAll(dir)
		return materialized{}, err
	}
	return materialized{path: path, sum: sum, size: size, release: func() { os.RemoveAll(dir) }}, nil
}

// fromCache returns a held entry, or a zero value (plus any tables it already
// fetched) when the caller should fall back. err is fatal: fetch or context.
func (s *Session) fromCache(ctx context.Context, cache *sheetCache, key, version string, force bool) (materialized, []table, error) {
	lock, err := cache.lock(ctx, key, syscall.LOCK_SH)
	if err != nil {
		return materialized{}, nil, ctx.Err()
	}
	fresh := func() (cacheMeta, bool) {
		meta, ok := cache.meta(key)
		return meta, ok && !force && meta.SpreadsheetID == s.id && meta.Version == version
	}
	var tables []table
	meta, hit := fresh()
	if !hit {
		// Upgrade so concurrent processes build once and the waiters reuse it.
		if cache.relock(ctx, lock, key, syscall.LOCK_EX) != nil {
			lock.Close()
			return materialized{}, nil, ctx.Err()
		}
		if meta, hit = fresh(); !hit {
			if tables, err = s.snapshot(ctx); err != nil {
				lock.Close()
				return materialized{}, nil, err
			}
			if meta, err = s.buildEntry(ctx, cache, key, version, tables); err != nil {
				lock.Close()
				return materialized{}, tables, ctx.Err()
			}
		}
		if cache.relock(ctx, lock, key, syscall.LOCK_SH) != nil {
			lock.Close()
			return materialized{}, tables, ctx.Err()
		}
	}
	snap := materialized{path: filepath.Join(cache.dir, key, "snapshot.duckdb"), sum: meta.SHA256, size: meta.Bytes, release: func() { lock.Close() }}
	if tables == nil {
		now := time.Now()
		os.Chtimes(filepath.Join(cache.dir, key, "meta.json"), now, now) // last used, for LRU
		snap.drop = func() { cache.remove(key) }
	} else {
		cache.evict(key, s.id, version)
	}
	return snap, nil, nil
}

func (s *Session) buildEntry(ctx context.Context, cache *sheetCache, key, version string, tables []table) (cacheMeta, error) {
	tmp, err := os.MkdirTemp(cache.dir, ".tmp-")
	if err != nil {
		return cacheMeta{}, err
	}
	defer os.RemoveAll(tmp)
	_, sum, size, err := s.build(ctx, tmp, tables)
	if err != nil {
		return cacheMeta{}, err
	}
	meta := cacheMeta{SpreadsheetID: s.id, Version: version, BuiltAt: time.Now().UTC(), Bytes: size, SHA256: sum}
	return meta, cache.publish(key, tmp, meta)
}

// build writes dir/snapshot.duckdb, fsynced, and returns its digest.
func (s *Session) build(ctx context.Context, dir string, tables []table) (path, sum string, size int64, err error) {
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
