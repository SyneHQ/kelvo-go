//go:build cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package files

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/SYNEHQ/kelvo-go/operations"
	sqlite "github.com/mattn/go-sqlite3"
)

// SQLite runs against an immutable copy of the verified descriptor. No upload
// path, SQL URI, extension or ambient database can enter the connector.
type sqliteConnector struct {
	path     string
	maxBytes int64
	memoryMB int
	metadata bool
	write    bool
}

func (c sqliteConnector) Driver() driver.Driver { return &sqlite.SQLiteDriver{} }
func (c sqliteConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	d := &sqlite.SQLiteDriver{ConnectHook: func(conn *sqlite.SQLiteConn) error {
		queryOnly := "ON"
		if c.write {
			queryOnly = "OFF"
		}
		for _, q := range []string{"PRAGMA query_only=" + queryOnly, "PRAGMA trusted_schema=OFF", "PRAGMA mmap_size=0", "PRAGMA temp_store=MEMORY", "PRAGMA cache_size=-" + strconv.Itoa(max(16, min(c.memoryMB*1024/4, 16384)))} {
			if _, err := conn.Exec(q, nil); err != nil {
				return err
			}
		}
		conn.SetLimit(sqlite.SQLITE_LIMIT_LENGTH, int(min(c.maxBytes, 16<<20)))
		conn.SetLimit(sqlite.SQLITE_LIMIT_SQL_LENGTH, 64<<10)
		conn.SetLimit(sqlite.SQLITE_LIMIT_COLUMN, 2000)
		conn.SetLimit(sqlite.SQLITE_LIMIT_ATTACHED, 0)
		conn.RegisterAuthorizer(func(action int, first, second, database string) int {
			if c.write {
				switch action {
				case sqlite.SQLITE_INSERT, sqlite.SQLITE_UPDATE, sqlite.SQLITE_DELETE, sqlite.SQLITE_CREATE_INDEX, sqlite.SQLITE_CREATE_TABLE, sqlite.SQLITE_CREATE_TRIGGER, sqlite.SQLITE_CREATE_VIEW, sqlite.SQLITE_DROP_INDEX, sqlite.SQLITE_DROP_TABLE, sqlite.SQLITE_DROP_TRIGGER, sqlite.SQLITE_DROP_VIEW, sqlite.SQLITE_ALTER_TABLE, sqlite.SQLITE_REINDEX, sqlite.SQLITE_ANALYZE, sqlite.SQLITE_TRANSACTION, sqlite.SQLITE_SAVEPOINT:
					return sqlite.SQLITE_OK
				}
			}
			switch action {
			case sqlite.SQLITE_SELECT, 33: // SQLITE_RECURSIVE (not exported by the driver).
				return sqlite.SQLITE_OK
			case sqlite.SQLITE_READ:
				if database == "main" || database == "temp" || database == "" {
					return sqlite.SQLITE_OK
				}
			case sqlite.SQLITE_FUNCTION:
				switch strings.ToLower(second) {
				case "load_extension", "readfile", "writefile", "fts3_tokenizer":
					return sqlite.SQLITE_DENY
				default:
					return sqlite.SQLITE_OK
				}
			case sqlite.SQLITE_PRAGMA:
				if c.metadata && first == "table_xinfo" {
					return sqlite.SQLITE_OK
				}
			}
			return sqlite.SQLITE_DENY
		})
		return nil
	}}
	options := url.Values{"mode": {"ro"}, "immutable": {"1"}}
	if c.write {
		options = url.Values{"mode": {"rw"}, "_journal_mode": {"DELETE"}, "_synchronous": {"FULL"}}
	}
	return d.Open("file:" + filepath.ToSlash(c.path) + "?" + options.Encode())
}

func (s *Session) sqlitePool(ctx context.Context, maximum int64, metadata bool) (*sql.DB, func(), error) {
	dir, err := os.MkdirTemp("", "kelvo-sqlite-")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	path := filepath.Join(dir, "source.sqlite")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	_, err = s.file.Seek(0, io.SeekStart)
	if err == nil {
		var count int64
		count, err = io.CopyN(file, s.file, s.descriptor.Bytes)
		if count != s.descriptor.Bytes && err == nil {
			err = io.ErrUnexpectedEOF
		}
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	db := sql.OpenDB(sqliteConnector{path: path, maxBytes: maximum, memoryMB: s.limits.MemoryMB, metadata: metadata})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	return db, func() { _ = db.Close(); cleanup() }, nil
}
func (s *Session) readSQLite(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	statement, err := sqlguard.ReadOnly(q.Statement)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	return s.sqliteRead(ctx, statement, q, false, sink)
}
func (s *Session) sqliteRead(ctx context.Context, statement string, q adapter.Query, metadata bool, sink adapter.Sink) (stats adapter.QueryStats, err error) {
	started := time.Now()
	defer func() { stats.Elapsed = time.Since(started) }()
	if s.file == nil || ctx == nil || sink == nil || q.Validate() != nil {
		return stats, adapter.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, s.limits.Timeout)
	defer cancel()
	db, close, err := s.sqlitePool(ctx, q.MaxBytes, metadata)
	if err != nil {
		return stats, err
	}
	defer close()
	args, err := adapter.SQLParameters(q.Parameters)
	if err != nil {
		return stats, err
	}
	rows, err := db.QueryContext(ctx, statement, args...)
	if err != nil {
		return stats, err
	}
	defer rows.Close()
	return streamSQLiteRows(ctx, rows, q, sink)
}
func (s *Session) inspectSQLite(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	if ctx == nil || sink == nil || spec.Limit < 1 || spec.Limit > 10000 || spec.Target.Catalog != "" && spec.Target.Catalog != s.name || spec.Target.Schema != "" && spec.Target.Schema != "main" {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	offset := int64(0)
	if spec.Cursor != "" {
		var err error
		offset, err = strconv.ParseInt(spec.Cursor, 10, 32)
		if err != nil || offset < 0 || offset > 1000000 || strconv.FormatInt(offset, 10) != spec.Cursor {
			return adapter.QueryStats{}, adapter.ErrInvalid
		}
	}
	var statement string
	var params []operations.Parameter
	add := func(kind string, value any) { params = append(params, jsonParameter(kind, value)) }
	switch spec.Object {
	case "catalogs", "databases":
		statement = "SELECT ? AS catalog"
		add("string", s.name)
	case "schemas":
		statement = "SELECT ? AS catalog,'main' AS schema_name"
		add("string", s.name)
	case "tables":
		statement = "SELECT ? AS catalog,'main' AS schema_name,name,type FROM sqlite_schema WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' AND (?='' OR name=?) ORDER BY name"
		add("string", s.name)
		add("string", spec.Target.Name)
		add("string", spec.Target.Name)
	case "columns":
		if spec.Target.Name == "" {
			return adapter.QueryStats{}, adapter.ErrInvalid
		}
		statement = "SELECT 'main' AS schema_name,? AS table_name,name,type,cid+1 AS position,CASE WHEN \"notnull\"=1 THEN 'NO' ELSE 'YES' END AS nullable,dflt_value AS default_value FROM pragma_table_xinfo(?) WHERE hidden<>1 ORDER BY cid"
		add("string", spec.Target.Name)
		add("string", spec.Target.Name)
	default:
		return adapter.QueryStats{}, adapter.ErrUnsupported
	}
	statement += " LIMIT ? OFFSET ?"
	add("int64", spec.Limit)
	add("int64", offset)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sqliteRead(ctx, statement, adapter.Query{Statement: statement, Parameters: params, MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}, true, sink)
}
