// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package watchers

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/watch"
)

// MySQL uses source-side triggers and an InnoDB outbox. Its generation record
// makes partially committed DDL resumable without reviving a retired watcher.
type MySQL struct {
	DB    *sql.DB
	Scope watch.Scope
}

const mysqlStateComment = "kelvo-watch-state-v1"
const mysqlSignaturePrefix = "syne-native-v1-" // Keep the installed adoption fingerprint.

func mysqlIdentifier(value string) string { return "`" + strings.ReplaceAll(value, "`", "``") + "`" }

// Hex text is independent of NO_BACKSLASH_ESCAPES and ANSI_QUOTES modes.
func mysqlLiteral(value string) string {
	return "CONVERT(X'" + hex.EncodeToString([]byte(value)) + "' USING utf8mb4)"
}
func (m MySQL) name(suffix string) string {
	return mysqlIdentifier(m.Scope.Schema) + "." + mysqlIdentifier(objectName(m.Scope.ID)+suffix)
}
func (m MySQL) table() string {
	return mysqlIdentifier(m.Scope.Schema) + "." + mysqlIdentifier(m.Scope.Table)
}

type mysqlSession struct {
	*sql.Conn
	definer string
	lock    string
}

func (m MySQL) open(ctx context.Context) (*mysqlSession, error) {
	if ctx == nil || m.DB == nil || m.Scope.Validate() != nil || m.Scope.Schema != m.Scope.Database || len(m.Scope.Schema) > 64 || len(m.Scope.Table) > 64 {
		return nil, watch.ErrInvalid
	}
	conn, err := m.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	var database, definer string
	if err = conn.QueryRowContext(ctx, `SELECT DATABASE(),CURRENT_USER()`).Scan(&database, &definer); err != nil || database != m.Scope.Database || definer == "" {
		_ = conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, watch.ErrConflict
	}
	lock := objectName(m.Scope.ID)
	var acquired sql.NullInt64
	err = conn.QueryRowContext(ctx, `SELECT GET_LOCK(?,5)`, lock).Scan(&acquired)
	if err != nil || !acquired.Valid || acquired.Int64 != 1 {
		// An uncertain acquisition must not return a session lock to the pool.
		if err != nil || !acquired.Valid {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, watch.ErrConflict
	}
	return &mysqlSession{Conn: conn, definer: definer, lock: lock}, nil
}

func (s *mysqlSession) close() {
	releaseMySQLLock(s.Conn, s.lock)
	_ = s.Conn.Close()
}

// Session locks survive a pool return. Retire the physical connection whenever
// release cannot be proved, including a cancelled operation's cleanup.
func releaseMySQLLock(conn *sql.Conn, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var released sql.NullInt64
	err := conn.QueryRowContext(ctx, `SELECT RELEASE_LOCK(?)`, name).Scan(&released)
	if err != nil || !released.Valid || released.Int64 != 1 {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
}

type mysqlColumn struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

func mysqlSignature(columns []mysqlColumn) string {
	var definition strings.Builder
	for _, column := range columns {
		definition.WriteString(column.Name)
		definition.WriteByte(0)
		definition.WriteString(column.Kind)
		definition.WriteByte(0)
	}
	sum := sha256.Sum256([]byte(definition.String()))
	return mysqlSignaturePrefix + hex.EncodeToString(sum[:])
}

func validMySQLColumns(columns []mysqlColumn) bool {
	if len(columns) == 0 || len(columns) > 1024 {
		return false
	}
	seen := map[string]bool{}
	for _, c := range columns {
		if c.Name == "" || len(c.Name) > 256 || strings.ContainsAny(c.Name, "\x00\r\n") || c.Kind == "" || len(c.Kind) > 64 || seen[c.Name] {
			return false
		}
		seen[c.Name] = true
	}
	return true
}

type mysqlState struct {
	status    string
	signature string
	columns   []mysqlColumn
}

type mysqlQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (m MySQL) verifyTable(ctx context.Context, q mysqlQuerier, name, comment string) error {
	var engine, kind, actual string
	err := q.QueryRowContext(ctx, `SELECT ENGINE,TABLE_TYPE,TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, m.Scope.Schema, name).Scan(&engine, &kind, &actual)
	if errors.Is(err, sql.ErrNoRows) {
		return watch.ErrUninitialized
	}
	if err != nil {
		return err
	}
	if engine != "InnoDB" || kind != "BASE TABLE" || actual != comment {
		return watch.ErrConflict
	}
	return nil
}

func (m MySQL) verifyState(ctx context.Context, q mysqlQuerier) error {
	return m.verifyTable(ctx, q, objectName(m.Scope.ID)+"_state", mysqlStateComment)
}

func (m MySQL) state(ctx context.Context, q mysqlQuerier) (mysqlState, error) {
	var key string
	var columns []byte
	var state mysqlState
	err := q.QueryRowContext(ctx, `SELECT scope_key,state,source_signature,source_columns FROM `+m.name("_state")+` WHERE generation=?`, m.Scope.Generation).Scan(&key, &state.status, &state.signature, &columns)
	if errors.Is(err, sql.ErrNoRows) {
		return state, watch.ErrUninitialized
	}
	if err != nil {
		return state, err
	}
	if key != m.Scope.Key() || operations.DecodeStrict(columns, &state.columns, 256<<10) != nil || !validMySQLColumns(state.columns) || state.signature != mysqlSignature(state.columns) {
		return state, watch.ErrConflict
	}
	switch state.status {
	case "installing", "active", "removing", "retired":
		return state, nil
	default:
		return state, watch.ErrConflict
	}
}

func (m MySQL) active(ctx context.Context, q mysqlQuerier) error {
	if err := m.verifyState(ctx, q); err != nil {
		return err
	}
	state, err := m.state(ctx, q)
	if err != nil {
		return err
	}
	if state.status != "active" {
		return watch.ErrConflict
	}
	return m.verifyTable(ctx, q, objectName(m.Scope.ID)+"_events", state.signature)
}

func mysqlColumnsJSON(columns []mysqlColumn) string {
	raw, _ := json.Marshal(columns)
	return string(raw)
}
