package relational

import "github.com/SYNEHQ/kelvo-go/adapter"

func sqlserverMetadata(object, schema, name string) (string, []any, error) {
	switch object {
	case "catalogs", "databases":
		if name != "" {
			return "", nil, adapter.ErrInvalid
		}
		return `SELECT CAST(DB_NAME() AS nvarchar(128)) AS catalog ORDER BY catalog`, nil, nil
	case "schemas":
		if name != "" {
			return "", nil, adapter.ErrInvalid
		}
		return `SELECT CAST(DB_NAME() AS nvarchar(128)) AS catalog,CAST(SCHEMA_NAME AS nvarchar(128)) AS schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE (?='' OR SCHEMA_NAME=?) ORDER BY SCHEMA_NAME`, []any{schema, schema}, nil
	case "tables":
		return `SELECT CAST(TABLE_CATALOG AS nvarchar(128)) AS catalog,CAST(TABLE_SCHEMA AS nvarchar(128)) AS schema_name,CAST(TABLE_NAME AS nvarchar(128)) AS name,CAST(TABLE_TYPE AS nvarchar(128)) AS type FROM INFORMATION_SCHEMA.TABLES WHERE (?='' OR TABLE_SCHEMA=?) AND (?='' OR TABLE_NAME=?) ORDER BY TABLE_SCHEMA,TABLE_NAME`, []any{schema, schema, name, name}, nil
	case "columns":
		return `SELECT CAST(TABLE_SCHEMA AS nvarchar(128)) AS schema_name,CAST(TABLE_NAME AS nvarchar(128)) AS table_name,CAST(COLUMN_NAME AS nvarchar(128)) AS name,CAST(DATA_TYPE AS nvarchar(128)) AS type,CAST(ORDINAL_POSITION AS bigint) AS position,CAST(IS_NULLABLE AS nvarchar(3)) AS nullable,CAST(COLUMN_DEFAULT AS nvarchar(max)) AS default_value,CAST(NUMERIC_PRECISION AS bigint) AS numeric_precision,CAST(NUMERIC_SCALE AS bigint) AS numeric_scale FROM INFORMATION_SCHEMA.COLUMNS WHERE (?='' OR TABLE_SCHEMA=?) AND (?='' OR TABLE_NAME=?) ORDER BY TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION`, []any{schema, schema, name, name}, nil
	case "primary_keys":
		return `SELECT CAST(k.TABLE_SCHEMA AS nvarchar(128)) AS schema_name,CAST(k.TABLE_NAME AS nvarchar(128)) AS table_name,CAST(k.CONSTRAINT_NAME AS nvarchar(128)) AS name,CAST(k.COLUMN_NAME AS nvarchar(128)) AS column_name,CAST(k.ORDINAL_POSITION AS bigint) AS position FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c ON c.CONSTRAINT_CATALOG=k.CONSTRAINT_CATALOG AND c.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA AND c.CONSTRAINT_NAME=k.CONSTRAINT_NAME WHERE c.CONSTRAINT_TYPE='PRIMARY KEY' AND (?='' OR k.TABLE_SCHEMA=?) AND (?='' OR k.TABLE_NAME=?) ORDER BY k.TABLE_SCHEMA,k.TABLE_NAME,k.ORDINAL_POSITION`, []any{schema, schema, name, name}, nil
	case "foreign_keys", "relationships":
		return `SELECT CAST(s.name AS nvarchar(128)) AS schema_name,CAST(t.name AS nvarchar(128)) AS table_name,CAST(f.name AS nvarchar(128)) AS name,CAST(c.name AS nvarchar(128)) AS column_name,CAST(k.constraint_column_id AS bigint) AS position,CAST(rs.name AS nvarchar(128)) AS referenced_schema,CAST(rt.name AS nvarchar(128)) AS referenced_table,CAST(rc.name AS nvarchar(128)) AS referenced_column FROM sys.foreign_keys f JOIN sys.foreign_key_columns k ON k.constraint_object_id=f.object_id JOIN sys.tables t ON t.object_id=f.parent_object_id JOIN sys.schemas s ON s.schema_id=t.schema_id JOIN sys.columns c ON c.object_id=t.object_id AND c.column_id=k.parent_column_id JOIN sys.tables rt ON rt.object_id=f.referenced_object_id JOIN sys.schemas rs ON rs.schema_id=rt.schema_id JOIN sys.columns rc ON rc.object_id=rt.object_id AND rc.column_id=k.referenced_column_id WHERE (?='' OR s.name=?) AND (?='' OR t.name=?) ORDER BY s.name,t.name,f.name,k.constraint_column_id`, []any{schema, schema, name, name}, nil
	case "indexes":
		return `SELECT CAST(s.name AS nvarchar(128)) AS schema_name,CAST(t.name AS nvarchar(128)) AS table_name,CAST(i.name AS nvarchar(128)) AS name,CAST(c.name AS nvarchar(128)) AS column_name,CAST(k.index_column_id AS bigint) AS position,CAST(CASE WHEN i.is_unique=1 THEN 0 ELSE 1 END AS bigint) AS non_unique FROM sys.indexes i JOIN sys.tables t ON t.object_id=i.object_id JOIN sys.schemas s ON s.schema_id=t.schema_id JOIN sys.index_columns k ON k.object_id=i.object_id AND k.index_id=i.index_id JOIN sys.columns c ON c.object_id=k.object_id AND c.column_id=k.column_id WHERE i.name IS NOT NULL AND (?='' OR s.name=?) AND (?='' OR t.name=?) ORDER BY s.name,t.name,i.name,k.index_column_id`, []any{schema, schema, name, name}, nil
	case "functions", "procedures":
		kind := "FUNCTION"
		if object == "procedures" {
			kind = "PROCEDURE"
		}
		return `SELECT CAST(r.ROUTINE_CATALOG AS nvarchar(128)) AS catalog,CAST(r.ROUTINE_SCHEMA AS nvarchar(128)) AS schema_name,CAST(r.ROUTINE_NAME AS nvarchar(128)) AS name,CAST(r.ROUTINE_TYPE AS nvarchar(128)) AS type,CAST(r.DATA_TYPE AS nvarchar(128)) AS return_type,CAST(r.EXTERNAL_LANGUAGE AS nvarchar(128)) AS language,CAST(m.definition AS nvarchar(max)) AS definition,CAST(r.SPECIFIC_NAME AS nvarchar(128)) AS specific_name FROM INFORMATION_SCHEMA.ROUTINES r LEFT JOIN sys.sql_modules m ON m.object_id=OBJECT_ID(QUOTENAME(r.ROUTINE_SCHEMA)+N'.'+QUOTENAME(r.ROUTINE_NAME)) WHERE r.ROUTINE_TYPE=? AND (?='' OR r.ROUTINE_SCHEMA=?) AND (?='' OR r.ROUTINE_NAME=?) ORDER BY r.ROUTINE_SCHEMA,r.ROUTINE_NAME,r.SPECIFIC_NAME`, []any{kind, schema, schema, name, name}, nil
	default:
		return "", nil, adapter.ErrUnsupported
	}
}
