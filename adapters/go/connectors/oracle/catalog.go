package oracle

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/SYNEHQ/kelvo-go/adapter"
	"strconv"
	"strings"
)

// Oracle 19c+ catalogs describe objects visible in the connection's current
// container and edition. Qualify SYS views to avoid owner-created shadows.
const oracleObjects = `SELECT TO_CHAR(o.OBJECT_ID) AS "id", o.OWNER AS "schema", o.OBJECT_NAME AS "name",
 o.OWNER AS "owner", o.STATUS AS "status", o.EDITION_NAME AS "edition",
 TO_CHAR(o.LAST_DDL_TIME,'YYYY-MM-DD"T"HH24:MI:SS') AS "last_ddl_time",
 CASE WHEN o.OBJECT_TYPE IN ('FUNCTION','PROCEDURE') THEN '…' END AS "signature",
 CASE WHEN p.AUTHID='CURRENT_USER' THEN 'invoker' WHEN p.AUTHID='DEFINER' THEN 'definer' END AS "security",
 p.DETERMINISTIC AS "deterministic", p.PIPELINED AS "pipelined", p.PARALLEL AS "parallel",
 LOWER(t.STATUS) AS "enabled", t.TRIGGER_TYPE AS "trigger_type", t.TRIGGERING_EVENT AS "events",
 t.BASE_OBJECT_TYPE AS "trigger_scope", t.TABLE_OWNER AS "table_schema", t.TABLE_NAME AS "table_name",
 m.REFRESH_MODE AS "refresh_mode", m.REFRESH_METHOD AS "refresh_method", m.STALENESS AS "staleness", m.COMPILE_STATE AS "compile_state"
 FROM SYS.ALL_OBJECTS o
 LEFT JOIN SYS.ALL_PROCEDURES p ON p.OBJECT_ID=o.OBJECT_ID AND p.PROCEDURE_NAME IS NULL
 LEFT JOIN SYS.ALL_TRIGGERS t ON o.OBJECT_TYPE='TRIGGER' AND t.OWNER=o.OWNER AND t.TRIGGER_NAME=o.OBJECT_NAME
 LEFT JOIN SYS.ALL_MVIEWS m ON o.OBJECT_TYPE='MATERIALIZED VIEW' AND m.OWNER=o.OWNER AND m.MVIEW_NAME=o.OBJECT_NAME
 WHERE o.OBJECT_TYPE=:object_type AND o.ORACLE_MAINTAINED='N'
 AND (:object_id IS NULL OR TO_CHAR(o.OBJECT_ID)=:object_id)
 ORDER BY o.OWNER,o.OBJECT_NAME,o.OBJECT_ID FETCH FIRST 501 ROWS ONLY`

const oracleSource = `SELECT TEXT FROM SYS.ALL_SOURCE WHERE OWNER=:owner AND NAME=:name AND TYPE=:object_type ORDER BY LINE`
const oracleViewSource = `SELECT TEXT FROM SYS.ALL_VIEWS WHERE OWNER=:owner AND VIEW_NAME=:name`
const oracleMViewSource = `SELECT QUERY FROM SYS.ALL_MVIEWS WHERE OWNER=:owner AND MVIEW_NAME=:name`
const oracleErrors = `SELECT TO_CHAR(LINE) AS "line", TO_CHAR(POSITION) AS "position", TEXT AS "text", ATTRIBUTE AS "severity"
 FROM SYS.ALL_ERRORS WHERE OWNER=:owner AND NAME=:name AND TYPE=:object_type ORDER BY SEQUENCE FETCH FIRST 501 ROWS ONLY`
const oracleMembers = `SELECT TO_CHAR(p.SUBPROGRAM_ID) AS "id", COALESCE(p.PROCEDURE_NAME,p.OBJECT_NAME) AS "name",
 p.OVERLOAD AS "overload", p.AUTHID AS "authid", p.DETERMINISTIC AS "deterministic", p.PIPELINED AS "pipelined", p.PARALLEL AS "parallel"
 FROM SYS.ALL_PROCEDURES p WHERE p.OWNER=:owner AND p.OBJECT_NAME=:name
 AND ((:object_type='PACKAGE' AND p.PROCEDURE_NAME IS NOT NULL AND p.OBJECT_TYPE='PACKAGE') OR :object_type<>'PACKAGE' AND p.OBJECT_TYPE=:object_type AND p.PROCEDURE_NAME IS NULL)
 ORDER BY p.SUBPROGRAM_ID FETCH FIRST 501 ROWS ONLY`
const oracleArguments = `SELECT TO_CHAR(a.SUBPROGRAM_ID) AS "member_id", TO_CHAR(a.POSITION) AS "position",
 a.ARGUMENT_NAME AS "name", a.IN_OUT AS "mode", a.DATA_TYPE AS "data_type", a.DEFAULTED AS "defaulted",
 a.TYPE_OWNER AS "type_owner", a.TYPE_NAME AS "type_name", a.TYPE_SUBNAME AS "type_subname", a.TYPE_LINK AS "type_link"
 FROM SYS.ALL_ARGUMENTS a JOIN SYS.ALL_OBJECTS o ON o.OBJECT_ID=a.OBJECT_ID
 WHERE o.OWNER=:owner AND o.OBJECT_NAME=:name AND o.OBJECT_TYPE=:object_type AND a.DATA_LEVEL=0
 ORDER BY a.SUBPROGRAM_ID,a.SEQUENCE FETCH FIRST 10001 ROWS ONLY`
const oracleDependencies = `WITH target AS (
 SELECT OWNER,OBJECT_NAME,OBJECT_TYPE FROM SYS.ALL_OBJECTS WHERE TO_CHAR(OBJECT_ID)=:object_id AND OBJECT_TYPE=:object_type
 ), edges AS (
 SELECT d.REFERENCED_OWNER AS owner,d.REFERENCED_NAME AS name,d.REFERENCED_TYPE AS object_type,d.REFERENCED_LINK_NAME AS db_link,'uses' AS role
 FROM SYS.ALL_DEPENDENCIES d JOIN target t ON d.OWNER=t.OWNER AND d.NAME=t.OBJECT_NAME AND d.TYPE=t.OBJECT_TYPE
 UNION
 SELECT d.OWNER,d.NAME,d.TYPE,CAST(NULL AS VARCHAR2(128)),'used_by'
 FROM SYS.ALL_DEPENDENCIES d JOIN target t ON d.REFERENCED_OWNER=t.OWNER AND d.REFERENCED_NAME=t.OBJECT_NAME AND d.REFERENCED_TYPE=t.OBJECT_TYPE
 WHERE d.REFERENCED_LINK_NAME IS NULL
 )
 SELECT TO_CHAR(o.OBJECT_ID) AS "id",e.owner AS "schema",e.name AS "name",e.object_type AS "object_type",e.db_link AS "database_link",e.role AS "role"
 FROM edges e LEFT JOIN SYS.ALL_OBJECTS o ON e.db_link IS NULL AND o.OWNER=e.owner AND o.OBJECT_NAME=e.name AND o.OBJECT_TYPE=e.object_type AND o.SUBOBJECT_NAME IS NULL
 ORDER BY e.role,e.owner,e.name,e.object_type FETCH FIRST 501 ROWS ONLY`
const oracleLinkedObject = `SELECT TO_CHAR(OBJECT_ID) AS "id" FROM SYS.ALL_OBJECTS
 WHERE OWNER=:owner AND OBJECT_NAME=:name AND OBJECT_TYPE=:object_type AND SUBOBJECT_NAME IS NULL FETCH FIRST 1 ROW ONLY`

var oracleTypes = map[string]string{
	"function": "FUNCTION", "procedure": "PROCEDURE", "package": "PACKAGE", "package_body": "PACKAGE BODY",
	"trigger": "TRIGGER", "view": "VIEW", "materialized_view": "MATERIALIZED VIEW",
}

type Diagnostic struct {
	Line     int    `json:"line"`
	Position int    `json:"position"`
	Text     string `json:"text"`
	Severity string `json:"severity"`
}
type RoutineMember struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Overload      string `json:"overload"`
	Signature     string `json:"signature"`
	ReturnType    string `json:"return_type"`
	Security      string `json:"security"`
	Deterministic string `json:"deterministic"`
	Pipelined     string `json:"pipelined"`
	Parallel      string `json:"parallel"`
}

func value(row map[string]any, key string) string { s, _ := row[key].(string); return s }

func inspectOracle(ctx context.Context, db *sql.DB, database, schema, kind, id string, limit int) (Result, error) {
	result := Result{Objects: []map[string]any{}, Relations: []Relation{}, Kinds: Kinds("oracle")}
	objectType, ok := oracleTypes[kind]
	if !ok || limit < 1 || limit > Limit {
		return result, ErrUnsupported
	}
	// go-ora v2 rejects driver.TxOptions.ReadOnly. Begin pins one connection;
	// make the transaction read-only before any catalog statement and fail closed.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
		return result, fmt.Errorf("establish Oracle read-only transaction: %w", err)
	}
	query := strings.Replace(oracleObjects, "FETCH FIRST 501", fmt.Sprintf("FETCH FIRST %d", limit+1), 1)
	args := []any{sql.Named("object_type", objectType), sql.Named("object_id", id)}
	if schema != "" {
		query = strings.Replace(query, "ORDER BY o.OWNER", "AND o.OWNER=:selected_schema ORDER BY o.OWNER", 1)
		args = append(args, sql.Named("selected_schema", schema))
	}
	rows, err := read(ctx, tx, query, args...)
	if err != nil {
		return result, err
	}
	result.Truncated = len(rows) > limit
	if result.Truncated {
		rows = rows[:limit]
	}
	for _, row := range rows {
		if schema != "" && value(row, "schema") != schema {
			return Result{}, adapter.ErrInvalid
		}
		row["kind"] = kind
		row["database"] = database
	}
	result.Objects = rows
	if id == "" || len(rows) == 0 {
		return result, nil
	}
	if len(rows) != 1 {
		return result, fmt.Errorf("ambiguous Oracle object identity")
	}
	row := rows[0]
	owner, name := value(row, "schema"), value(row, "name")
	notices := []string{}
	sourceQuery := oracleSource
	sourceArgs := []any{sql.Named("owner", owner), sql.Named("name", name), sql.Named("object_type", objectType)}
	row["definition_format"] = "source"
	if kind == "view" || kind == "materialized_view" {
		sourceQuery = oracleViewSource
		if kind == "materialized_view" {
			sourceQuery = oracleMViewSource
		}
		sourceArgs = sourceArgs[:2]
		row["definition_format"] = "query"
	}
	definition, tooLarge, err := readOracleSource(ctx, tx, sourceQuery, sourceArgs...)
	if err != nil {
		return result, err
	}
	if tooLarge {
		notices = append(notices, "Source exceeds 1 MiB and was not loaded.")
	} else if definition != "" {
		row["definition"] = definition
	} else {
		notices = append(notices, "Source is not visible to this database role.")
	}
	errors, err := read(ctx, tx, oracleErrors, sql.Named("owner", owner), sql.Named("name", name), sql.Named("object_type", objectType))
	if err != nil {
		return result, err
	}
	if len(errors) > Limit {
		errors = errors[:Limit]
		notices = append(notices, "Only the first 500 compilation diagnostics are shown.")
	}
	diagnostics := []Diagnostic{}
	for _, item := range errors {
		line, _ := strconv.Atoi(value(item, "line"))
		position, _ := strconv.Atoi(value(item, "position"))
		diagnostics = append(diagnostics, Diagnostic{line, position, value(item, "text"), value(item, "severity")})
	}
	row["diagnostics"] = diagnostics
	if kind == "function" || kind == "procedure" || kind == "package" {
		members, truncated, err := readOracleMembers(ctx, tx, owner, name, objectType)
		if err != nil {
			return result, err
		}
		if truncated {
			notices = append(notices, "Routine metadata exceeds the limit. Some signatures may be incomplete.")
		}
		if kind == "package" {
			row["members"] = members
		} else if len(members) == 1 {
			row["signature"] = members[0].Signature
			row["return_type"] = members[0].ReturnType
		}
	}
	related, err := read(ctx, tx, oracleDependencies, sql.Named("object_id", id), sql.Named("object_type", objectType))
	if err != nil {
		return result, err
	}
	result.RelationsTruncated = len(related) > Limit
	if result.RelationsTruncated {
		related = related[:Limit]
	}
	for _, item := range related {
		relationKind := oracleKind(value(item, "object_type"))
		unavailable := value(item, "id") == "" || relationKind == "other" || value(item, "database_link") != ""
		result.Relations = append(result.Relations, Relation{ID: value(item, "id"), Kind: relationKind, Schema: value(item, "schema"), Name: value(item, "name"), Role: value(item, "role"), ObjectType: value(item, "object_type"), DatabaseLink: value(item, "database_link"), Unavailable: unavailable})
	}
	if kind == "trigger" && value(row, "table_name") != "" && (value(row, "trigger_scope") == "TABLE" || value(row, "trigger_scope") == "VIEW") {
		relation, err := oracleLink(ctx, tx, value(row, "table_schema"), value(row, "table_name"), value(row, "trigger_scope"), "attached_to")
		if err != nil {
			return result, err
		}
		duplicate := false
		for _, existing := range result.Relations {
			if existing.ID == relation.ID && existing.Kind == relation.Kind && existing.Role == relation.Role && existing.Schema == relation.Schema && existing.Name == relation.Name {
				duplicate = true
				break
			}
		}
		if !duplicate {
			result.Relations = append(result.Relations, relation)
		}
	}
	if kind == "package" || kind == "package_body" {
		targetType, role := "PACKAGE BODY", "used_by"
		if kind == "package_body" {
			targetType, role = "PACKAGE", "uses"
		}
		relation, err := oracleLink(ctx, tx, owner, name, targetType, role)
		if err != nil {
			return result, err
		}
		if !relation.Unavailable {
			duplicate := false
			for _, existing := range result.Relations {
				if existing.ID == relation.ID && existing.Kind == relation.Kind && existing.Role == relation.Role && existing.Schema == relation.Schema && existing.Name == relation.Name {
					duplicate = true
					break
				}
			}
			if !duplicate {
				result.Relations = append(result.Relations, relation)
			}
		}
	}
	row["notices"] = notices
	if schema != "" {
		for _, relation := range result.Relations {
			if relation.Schema != schema || relation.DatabaseLink != "" {
				return Result{}, adapter.ErrUnsupported
			}
		}
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return result, err
	}
	if len(encoded) > 4<<20 {
		return Result{}, fmt.Errorf("Oracle catalog detail exceeds response size limit")
	}
	return result, nil
}

func oracleKind(objectType string) string {
	if objectType == "TABLE" {
		return "table"
	}
	for kind, name := range oracleTypes {
		if name == objectType {
			return kind
		}
	}
	return "other"
}
func oracleLink(ctx context.Context, tx *sql.Tx, owner, name, objectType, role string) (Relation, error) {
	relation := Relation{Kind: oracleKind(objectType), Schema: owner, Name: name, Role: role, ObjectType: objectType, Unavailable: true}
	rows, err := read(ctx, tx, oracleLinkedObject, sql.Named("owner", owner), sql.Named("name", name), sql.Named("object_type", objectType))
	if err != nil {
		return relation, err
	}
	if len(rows) == 1 {
		relation.ID = value(rows[0], "id")
		relation.Unavailable = false
	}
	return relation, nil
}

// Concatenate source lines in Go, avoiding Oracle LISTAGG's 4K/32K limits and
// preserving source text and compiler line numbers. Never export partial SQL.
func readOracleSource(ctx context.Context, tx *sql.Tx, query string, args ...any) (string, bool, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	var source strings.Builder
	for rows.Next() {
		var line sql.NullString
		if err := rows.Scan(&line); err != nil {
			return "", false, err
		}
		if source.Len()+len(line.String) > 1<<20 {
			return "", true, nil
		}
		source.WriteString(line.String)
	}
	return source.String(), false, rows.Err()
}
func readOracleMembers(ctx context.Context, tx *sql.Tx, owner, name, objectType string) ([]RoutineMember, bool, error) {
	args := []any{sql.Named("owner", owner), sql.Named("name", name), sql.Named("object_type", objectType)}
	members, err := read(ctx, tx, oracleMembers, args...)
	if err != nil {
		return nil, false, err
	}
	truncated := len(members) > Limit
	if truncated {
		members = members[:Limit]
	}
	arguments, err := readLimit(ctx, tx, oracleArguments, 10001, args...)
	if err != nil {
		return nil, false, err
	}
	if len(arguments) > 10000 {
		arguments = arguments[:10000]
		truncated = true
	}
	signatures := map[string][]string{}
	returns := map[string]string{}
	for _, arg := range arguments {
		id := value(arg, "member_id")
		datatype := value(arg, "data_type")
		if value(arg, "type_name") != "" {
			datatype = value(arg, "type_name")
			if value(arg, "type_owner") != "" {
				datatype = value(arg, "type_owner") + "." + datatype
			}
			if value(arg, "type_subname") != "" {
				datatype += "." + value(arg, "type_subname")
			}
			if value(arg, "type_link") != "" {
				datatype += "@" + value(arg, "type_link")
			}
		}
		if value(arg, "position") == "0" {
			returns[id] = datatype
			continue
		}
		argument := strings.TrimSpace(value(arg, "name") + " " + value(arg, "mode") + " " + datatype)
		if value(arg, "defaulted") == "Y" {
			argument += " DEFAULT …"
		}
		signatures[id] = append(signatures[id], argument)
	}
	result := []RoutineMember{}
	for _, member := range members {
		id := value(member, "id")
		security := "definer"
		if value(member, "authid") == "CURRENT_USER" {
			security = "invoker"
		}
		signature := strings.Join(signatures[id], ", ")
		if truncated {
			if signature != "" {
				signature += ", "
			}
			signature += "…"
		}
		result = append(result, RoutineMember{ID: id, Name: value(member, "name"), Overload: value(member, "overload"), Signature: signature, ReturnType: returns[id], Security: security, Deterministic: value(member, "deterministic"), Pipelined: value(member, "pipelined"), Parallel: value(member, "parallel")})
	}
	return result, truncated, nil
}
