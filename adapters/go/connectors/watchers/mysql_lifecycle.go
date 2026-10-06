// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package watchers

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/SYNEHQ/kelvo-go/watch"
)

func (m MySQL) sourceColumns(ctx context.Context, conn *sql.Conn) ([]mysqlColumn, error) {
	var engine, kind string
	err := conn.QueryRowContext(ctx, `SELECT ENGINE,TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, m.Scope.Schema, m.Scope.Table).Scan(&engine, &kind)
	if err != nil {
		return nil, err
	}
	if engine != "InnoDB" || kind != "BASE TABLE" {
		return nil, watch.ErrConflict
	}
	rows, err := conn.QueryContext(ctx, `SELECT COLUMN_NAME,DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, m.Scope.Schema, m.Scope.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []mysqlColumn
	for rows.Next() {
		var column mysqlColumn
		if err := rows.Scan(&column.Name, &column.Kind); err != nil {
			return nil, err
		}
		columns = append(columns, column)
		if len(columns) > 1024 {
			return nil, watch.ErrLimit
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !validMySQLColumns(columns) {
		return nil, watch.ErrConflict
	}
	return columns, nil
}

func mysqlRowJSON(alias string, columns []mysqlColumn) string {
	parts := make([]string, 0, len(columns)*2)
	for _, column := range columns {
		value := alias + "." + mysqlIdentifier(column.Name)
		switch column.Kind {
		case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bit":
			value = "TO_BASE64(" + value + ")"
		case "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection":
			value = "ST_AsText(" + value + ")"
		}
		parts = append(parts, mysqlLiteral(column.Name), value)
	}
	return "JSON_OBJECT(" + strings.Join(parts, ",") + ")"
}

func (m MySQL) triggerBody(operation string, columns []mysqlColumn) string {
	data, old := "NULL", "NULL"
	if operation != "DELETE" {
		data = mysqlRowJSON("NEW", columns)
	}
	if operation != "INSERT" {
		old = mysqlRowJSON("OLD", columns)
	}
	return `INSERT INTO ` + m.name("_events") + ` (operation,row_data,old_row_data,created_at) VALUES (` + mysqlLiteral(operation) + `,` + data + `,` + old + `,UTC_TIMESTAMP(6))`
}

// Check the full existing definition before adopting or dropping a trigger.
// Original columns are retained in state so removal also works after schema DDL.
func (m MySQL) triggerExists(ctx context.Context, s *mysqlSession, operation string, columns []mysqlColumn) (bool, error) {
	var table, event, timing, body, definer string
	err := s.QueryRowContext(ctx, `SELECT EVENT_OBJECT_TABLE,EVENT_MANIPULATION,ACTION_TIMING,ACTION_STATEMENT,DEFINER FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=? AND TRIGGER_NAME=?`, m.Scope.Schema, objectName(m.Scope.ID)+"_"+strings.ToLower(operation)).Scan(&table, &event, &timing, &body, &definer)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if table != m.Scope.Table || event != operation || timing != "AFTER" || strings.TrimSpace(body) != m.triggerBody(operation, columns) || definer != s.definer {
		return false, watch.ErrConflict
	}
	return true, nil
}

func (m MySQL) ensureState(ctx context.Context, s *mysqlSession) error {
	err := m.verifyState(ctx, s.Conn)
	if !errors.Is(err, watch.ErrUninitialized) {
		return err
	}
	_, err = s.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+m.name("_state")+` (generation VARBINARY(128) PRIMARY KEY,scope_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,state VARCHAR(16) CHARACTER SET ascii NOT NULL,source_signature VARCHAR(96) CHARACTER SET ascii NOT NULL,source_columns JSON NOT NULL) ENGINE=InnoDB COMMENT='`+mysqlStateComment+`'`)
	if err != nil {
		return errors.Join(watch.ErrOutcomeUnknown, err)
	}
	return m.verifyState(ctx, s.Conn)
}

func (m MySQL) Install(ctx context.Context) (err error) {
	s, err := m.open(ctx)
	if err != nil {
		return err
	}
	defer s.close()
	if err = m.ensureState(ctx, s); err != nil {
		return err
	}
	state, err := m.state(ctx, s.Conn)
	fresh := errors.Is(err, watch.ErrUninitialized)
	if err != nil && !fresh {
		return err
	}
	if !fresh && (state.status == "retired" || state.status == "removing") {
		return watch.ErrConflict
	}
	columns, err := m.sourceColumns(ctx, s.Conn)
	if err != nil {
		return err
	}
	signature := mysqlSignature(columns)
	if !fresh && signature != state.signature {
		return watch.ErrConflict
	}
	if state.status == "active" {
		if err = m.verifyTable(ctx, s.Conn, objectName(m.Scope.ID)+"_events", signature); err != nil {
			return err
		}
		for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			exists, err := m.triggerExists(ctx, s, operation, columns)
			if err != nil {
				return err
			}
			if !exists {
				return watch.ErrConflict
			}
		}
		return nil
	}
	if fresh {
		var unfinished bool
		if err = s.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+m.name("_state")+` WHERE state<>'retired')`).Scan(&unfinished); err != nil {
			return err
		}
		if unfinished {
			return watch.ErrConflict
		}
	}
	// MySQL autocommits DDL. Any subsequent failure must retain this generation
	// for explicit reconciliation; it must never be described as rolled back.
	defer func() {
		if err != nil {
			err = errors.Join(watch.ErrOutcomeUnknown, err)
		}
	}()
	if fresh {
		if _, err = s.ExecContext(ctx, `INSERT INTO `+m.name("_state")+` (generation,scope_key,state,source_signature,source_columns) VALUES(?,?,'installing',?,?)`, m.Scope.Generation, m.Scope.Key(), signature, mysqlColumnsJSON(columns)); err != nil {
			return err
		}
	}
	if _, err = s.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+m.name("_events")+` (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,operation VARCHAR(6) NOT NULL,row_data JSON NULL,old_row_data JSON NULL,created_at DATETIME(6) NOT NULL) ENGINE=InnoDB COMMENT='`+signature+`'`); err != nil {
		return err
	}
	if err = m.verifyTable(ctx, s.Conn, objectName(m.Scope.ID)+"_events", signature); err != nil {
		return err
	}
	for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
		var exists bool
		if exists, err = m.triggerExists(ctx, s, operation, columns); err != nil {
			return err
		}
		if !exists {
			if _, err = s.ExecContext(ctx, `CREATE TRIGGER `+m.name("_"+strings.ToLower(operation))+` AFTER `+operation+` ON `+m.table()+` FOR EACH ROW `+m.triggerBody(operation, columns)); err != nil {
				return err
			}
		}
	}
	return m.transition(ctx, s, `SET state='active' WHERE generation=? AND state='installing'`)
}

func (m MySQL) Remove(ctx context.Context) (err error) {
	s, err := m.open(ctx)
	if err != nil {
		return err
	}
	defer s.close()
	if err = m.verifyState(ctx, s.Conn); err != nil {
		return err
	}
	state, err := m.state(ctx, s.Conn)
	if err != nil || state.status == "retired" {
		return err
	}
	// Verify every object before marking removal or touching a partial install.
	if err = m.verifyTable(ctx, s.Conn, objectName(m.Scope.ID)+"_events", state.signature); err != nil && !errors.Is(err, watch.ErrUninitialized) {
		return err
	}
	for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
		if _, err = m.triggerExists(ctx, s, operation, state.columns); err != nil {
			return err
		}
	}
	defer func() {
		if err != nil {
			err = errors.Join(watch.ErrOutcomeUnknown, err)
		}
	}()
	if state.status != "removing" {
		if err = m.transition(ctx, s, `SET state='removing' WHERE generation=? AND state IN ('active','installing')`); err != nil {
			return err
		}
	}
	for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
		if _, err = s.ExecContext(ctx, `DROP TRIGGER IF EXISTS `+m.name("_"+strings.ToLower(operation))); err != nil {
			return err
		}
	}
	if _, err = s.ExecContext(ctx, `DROP TABLE IF EXISTS `+m.name("_events")); err != nil {
		return err
	}
	return m.transition(ctx, s, `SET state='retired' WHERE generation=? AND state='removing'`)
}

func (m MySQL) transition(ctx context.Context, s *mysqlSession, clause string) error {
	result, err := s.ExecContext(ctx, `UPDATE `+m.name("_state")+` `+clause, m.Scope.Generation)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return watch.ErrConflict
	}
	return nil
}
