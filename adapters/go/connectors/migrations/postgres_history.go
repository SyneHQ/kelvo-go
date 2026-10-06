// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package migrations

import (
	"context"
	"database/sql"

	"github.com/SYNEHQ/kelvo-go/migration"
)

const postgresHistorySQL = `SELECT c.relkind, c.relrowsecurity, c.relhasrules, a.attname, a.atttypid, a.attnotnull, a.attgenerated, a.attidentity, EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid AND t.tgenabled<>'D'), EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped WHERE c.oid=pg_catalog.to_regclass($1) ORDER BY a.attnum LIMIT 3`

// Read the catalog before evaluating any user relation. History is an ordinary
// table with primitive columns, never a view, domain, generated value or policy.
func postgresHistory(ctx context.Context, source versionQueryer, table string) (bool, error) {
	rows, err := source.QueryContext(ctx, postgresHistorySQL, table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var kind string
		var rls, rules, triggers, inheritance bool
		var name, generated, identity sql.NullString
		var datatype sql.NullInt64
		var notnull sql.NullBool
		if err := rows.Scan(&kind, &rls, &rules, &name, &datatype, &notnull, &generated, &identity, &triggers, &inheritance); err != nil {
			return false, err
		}
		if kind != "r" || rls || rules || triggers || inheritance || !name.Valid || !datatype.Valid || !notnull.Valid || !notnull.Bool || !generated.Valid || generated.String != "" || !identity.Valid || identity.String != "" || seen[name.String] || name.String != "version" && name.String != "dirty" || name.String == "version" && datatype.Int64 != 20 || name.String == "dirty" && datatype.Int64 != 16 {
			return false, migration.ErrInvalid
		}
		seen[name.String] = true
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(seen) == 0 {
		return false, nil
	}
	if len(seen) != 2 {
		return false, migration.ErrInvalid
	}
	return true, nil
}
