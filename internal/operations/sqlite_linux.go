//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

// SQLiteBackend holds an exclusive process lock for the database lifetime.
// Each successful statement commits with synchronous=FULL. Only a proven CAS
// conflict returns ErrRevision. An uncertain acknowledgement is unavailable.
type SQLiteBackend struct {
	db        *sql.DB
	dir, lock *os.File
	policy    Policy
	mu        sync.Mutex
}

func OpenSQLite(ctx context.Context, directory string, policy Policy, authoritySHA256 string) (_ *SQLiteBackend, resultErr error) {
	if ctx == nil || policy.Validate() != nil || policy.Shards > 256 || len(authoritySHA256) != 64 {
		return nil, ErrInvalid
	}
	b := &SQLiteBackend{policy: policy}
	defer func() {
		if resultErr != nil {
			_ = b.Close()
		}
	}()
	var err error
	b.dir, err = sqliteDirectory(directory)
	if err != nil {
		return nil, ErrUnavailable
	}
	b.lock, err = sqliteFile(b.dir, ".lock", unix.O_RDWR|unix.O_CREAT)
	if err != nil {
		return nil, ErrUnavailable
	}
	if err = unix.Flock(int(b.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, ErrUnavailable
	}
	entries, err := b.dir.ReadDir(-1)
	if err != nil {
		return nil, ErrUnavailable
	}
	for _, entry := range entries {
		switch entry.Name() {
		case ".lock", "operations.sqlite", "operations.sqlite-wal", "operations.sqlite-shm":
			f, err := sqliteFile(b.dir, entry.Name(), unix.O_RDWR)
			if err != nil {
				return nil, ErrUnavailable
			}
			_ = f.Close()
		default:
			return nil, ErrInvalid
		}
	}
	f, err := sqliteFile(b.dir, "operations.sqlite", unix.O_RDWR|unix.O_CREAT)
	if err != nil {
		return nil, ErrUnavailable
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, ErrUnavailable
	}
	fresh := info.Size() == 0
	if err = errors.Join(f.Sync(), f.Close(), b.dir.Sync()); err != nil {
		return nil, ErrUnavailable
	}
	b.db, err = sql.Open("sqlite", filepath.Join(directory, "operations.sqlite"))
	if err != nil {
		return nil, ErrUnavailable
	}
	b.db.SetMaxOpenConns(1)
	b.db.SetMaxIdleConns(1)
	for _, statement := range []string{
		"PRAGMA busy_timeout=1000", "PRAGMA trusted_schema=OFF", "PRAGMA foreign_keys=ON",
		"PRAGMA synchronous=FULL", "PRAGMA journal_mode=WAL", "PRAGMA secure_delete=ON",
		"PRAGMA wal_autocheckpoint=32", "PRAGMA journal_size_limit=1048576",
		fmt.Sprintf("PRAGMA max_page_count=%d", (int64(policy.Shards)*MaxDocumentBytes*3+(4<<20))/4096),
	} {
		if _, err = b.db.ExecContext(ctx, statement); err != nil {
			return nil, ErrUnavailable
		}
	}
	var check string
	if err = b.db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&check); err != nil || check != "ok" {
		return nil, ErrUnavailable
	}
	metadata, _ := json.Marshal(struct {
		Version         int
		Policy          Policy
		AuthoritySHA256 string
	}{1, policy, authoritySHA256})
	if fresh {
		tx, err := b.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, ErrUnavailable
		}
		defer tx.Rollback()
		for _, statement := range []string{
			"CREATE TABLE metadata (id INTEGER PRIMARY KEY CHECK (id=1), value BLOB NOT NULL)",
			"CREATE TABLE kv (key TEXT PRIMARY KEY, revision INTEGER NOT NULL CHECK (revision>0), value BLOB NOT NULL CHECK (length(value)<=524288)) WITHOUT ROWID",
		} {
			if _, err = tx.ExecContext(ctx, statement); err != nil {
				return nil, ErrUnavailable
			}
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO metadata (id,value) VALUES (1,?)", metadata); err != nil {
			return nil, ErrUnavailable
		}
		if err = tx.Commit(); err != nil {
			return nil, ErrUnavailable
		}
	} else {
		var stored []byte
		if err = b.db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE id=1").Scan(&stored); err != nil {
			return nil, ErrUnavailable
		}
		if !bytes.Equal(stored, metadata) {
			return nil, ErrConflict
		}
	}
	// Reject foreign keys and oversized rows before any recovery dispatch.
	rows, err := b.db.QueryContext(ctx, "SELECT key, revision, length(value) FROM kv")
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var key string
		var revision, size int64
		if rows.Scan(&key, &revision, &size) != nil || !b.validKey(key) || revision < 1 || size < 1 || size > MaxDocumentBytes {
			return nil, ErrUnavailable
		}
		count++
	}
	if rows.Err() != nil || count > policy.Shards {
		return nil, ErrUnavailable
	}
	return b, nil
}

func (b *SQLiteBackend) validKey(key string) bool {
	prefix := "operation." + b.policy.Namespace + "."
	if !strings.HasPrefix(key, prefix) || len(key) != len(prefix)+4 {
		return false
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(key, prefix), 16, 16)
	return err == nil && int(n) < b.policy.Shards && key == fmt.Sprintf("%s%04x", prefix, n)
}

func (b *SQLiteBackend) Get(ctx context.Context, key string) (Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db == nil {
		return Entry{}, ErrUnavailable
	}
	if !b.validKey(key) {
		return Entry{}, ErrInvalid
	}
	var entry Entry
	var revision int64
	err := b.db.QueryRowContext(ctx, "SELECT value,revision FROM kv WHERE key=?", key).Scan(&entry.Value, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, ErrMissing
	}
	if err != nil || revision < 1 || len(entry.Value) < 1 || len(entry.Value) > MaxDocumentBytes {
		return Entry{}, ErrUnavailable
	}
	entry.Revision = uint64(revision)
	return entry, nil
}

func (b *SQLiteBackend) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db == nil {
		return 0, ErrUnavailable
	}
	if !b.validKey(key) || len(value) < 1 || len(value) > MaxDocumentBytes {
		return 0, ErrInvalid
	}
	result, err := b.db.ExecContext(ctx, "INSERT INTO kv (key,revision,value) VALUES (?,1,?) ON CONFLICT(key) DO NOTHING", key, value)
	return sqliteAcknowledgement(result, err, 1)
}

func (b *SQLiteBackend) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db == nil {
		return 0, ErrUnavailable
	}
	if !b.validKey(key) || len(value) < 1 || len(value) > MaxDocumentBytes || revision == 0 || revision >= math.MaxInt64 {
		return 0, ErrInvalid
	}
	result, err := b.db.ExecContext(ctx, "UPDATE kv SET value=?,revision=revision+1 WHERE key=? AND revision=?", value, key, int64(revision))
	return sqliteAcknowledgement(result, err, revision+1)
}

func sqliteAcknowledgement(result sql.Result, err error, revision uint64) (uint64, error) {
	if err != nil {
		return 0, ErrUnavailable
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, ErrUnavailable
	}
	if n == 0 {
		return 0, ErrRevision
	}
	if n != 1 {
		return 0, ErrUnavailable
	}
	return revision, nil
}

func (b *SQLiteBackend) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var err error
	if b.db != nil {
		err = b.db.Close()
		b.db = nil
	}
	if b.lock != nil {
		err = errors.Join(err, b.lock.Close())
		b.lock = nil
	}
	if b.dir != nil {
		err = errors.Join(err, b.dir.Close())
		b.dir = nil
	}
	return err
}

func sqliteDirectory(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || len(path) > 4096 {
		return nil, ErrInvalid
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, name := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		var st unix.Stat_t
		if unix.Fstat(fd, &st) != nil || (st.Uid != 0 && st.Uid != uint32(os.Geteuid())) || (st.Mode&0022 != 0 && !(st.Uid == 0 && st.Mode&unix.S_ISVTX != 0)) {
			unix.Close(fd)
			return nil, ErrInvalid
		}
		next, e := unix.Openat(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if errors.Is(e, unix.ENOENT) {
			e = unix.Mkdirat(fd, name, 0700)
			if e == nil {
				e = unix.Fsync(fd)
			}
			if e == nil {
				next, e = unix.Openat(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			}
		}
		unix.Close(fd)
		if e != nil {
			return nil, e
		}
		fd = next
	}
	f := os.NewFile(uintptr(fd), path)
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Uid != uint32(os.Geteuid()) || st.Mode&0077 != 0 {
		f.Close()
		return nil, ErrInvalid
	}
	return f, nil
}

func sqliteFile(dir *os.File, name string, flags int) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), name, flags|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name))
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != uint32(os.Geteuid()) || st.Mode&0077 != 0 || st.Nlink != 1 {
		f.Close()
		return nil, ErrInvalid
	}
	return f, nil
}
