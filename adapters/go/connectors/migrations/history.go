// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package migrations

import (
	"context"
	"database/sql"
	"strings"

	"github.com/SYNEHQ/kelvo-go/migration"
)

const mysqlHistorySQL = `SELECT t.TABLE_TYPE, t.ENGINE, c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.EXTRA, c.GENERATION_EXPRESSION FROM information_schema.TABLES t JOIN information_schema.COLUMNS c ON c.TABLE_SCHEMA=t.TABLE_SCHEMA AND c.TABLE_NAME=t.TABLE_NAME WHERE t.TABLE_SCHEMA=? AND t.TABLE_NAME=? ORDER BY c.ORDINAL_POSITION LIMIT 3`
const mysqlTriggersSQL = `SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?`
const sqlServerHistorySQL = `SELECT TOP (3) o.type, c.name, c.system_type_id, c.user_type_id, c.is_nullable, c.is_computed, c.generated_always_type FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id LEFT JOIN sys.columns c ON c.object_id=o.object_id WHERE s.name=@p1 AND o.name=@p2 ORDER BY c.column_id`
const sqlServerTriggersSQL = `SELECT COUNT(*) FROM sys.triggers t JOIN sys.objects o ON o.object_id=t.parent_id JOIN sys.schemas s ON s.schema_id=o.schema_id WHERE s.name=@p1 AND o.name=@p2 AND t.is_disabled=0`

// History must be a transactional base table. Do not read a view, run computed
// expressions, or update a history table with enabled side-effecting triggers.
func (d *relationalDriver) historyTable(ctx context.Context, source versionQueryer) (bool, error) {
	query := mysqlHistorySQL
	if d.engine == "sqlserver" {
		query = sqlServerHistorySQL
	}
	rows, err := source.QueryContext(ctx, query, d.schema, migration.Table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var name string
		if d.engine == "sqlserver" {
			var kind string
			var column sql.NullString
			var system, user, generated sql.NullInt64
			var nullable, computed sql.NullBool
			if err := rows.Scan(&kind, &column, &system, &user, &nullable, &computed, &generated); err != nil {
				return false, err
			}
			name = column.String
			if strings.TrimSpace(kind) != "U" || !column.Valid || !system.Valid || !user.Valid || !nullable.Valid || !computed.Valid || !generated.Valid || nullable.Bool || computed.Bool || generated.Int64 != 0 || system.Int64 != user.Int64 || name == "version" && system.Int64 != 127 || name == "dirty" && system.Int64 != 104 {
				return false, migration.ErrInvalid
			}
		} else {
			var kind, engine, datatype, nullable, extra string
			var generated sql.NullString
			if err := rows.Scan(&kind, &engine, &name, &datatype, &nullable, &extra, &generated); err != nil {
				return false, err
			}
			if kind != "BASE TABLE" || !strings.EqualFold(engine, "InnoDB") || nullable != "NO" || extra != "" || generated.Valid && generated.String != "" || name == "version" && datatype != "bigint" || name == "dirty" && datatype != "tinyint" {
				return false, migration.ErrInvalid
			}
		}
		if (name != "version" && name != "dirty") || seen[name] {
			return false, migration.ErrInvalid
		}
		seen[name] = true
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if len(seen) == 0 {
		return false, nil
	}
	if len(seen) != 2 {
		return false, migration.ErrInvalid
	}
	query = mysqlTriggersSQL
	if d.engine == "sqlserver" {
		query = sqlServerTriggersSQL
	}
	rows, err = source.QueryContext(ctx, query, d.schema, migration.Table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var triggers int
	if !rows.Next() || rows.Scan(&triggers) != nil || triggers != 0 || rows.Next() {
		return false, migration.ErrInvalid
	}
	return true, rows.Err()
}
