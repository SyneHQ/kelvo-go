package clickhouse

import (
	"context"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func (s *Session) Inspect(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	statement, err := s.metadataQuery(spec)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	if int64(spec.Limit) > limits.MaxRows {
		return adapter.QueryStats{}, adapter.ErrLimit
	}
	return s.Query(ctx, adapter.Query{Statement: statement, MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}, sink)
}
func (s *Session) metadataQuery(spec operations.MetadataSpec) (string, error) {
	if s == nil || spec.Limit < 1 || spec.Limit > 10000 || spec.Target.Catalog != "" && spec.Target.Catalog != s.database || spec.Target.Schema != "" && spec.Target.Schema != s.database || strings.ContainsAny(spec.Target.Name, "\x00\r\n\\") {
		return "", adapter.ErrInvalid
	}
	offset := int64(0)
	if spec.Cursor != "" {
		var err error
		offset, err = strconv.ParseInt(spec.Cursor, 10, 32)
		if err != nil || offset < 0 || offset > 1000000 || strconv.FormatInt(offset, 10) != spec.Cursor {
			return "", adapter.ErrInvalid
		}
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	database := quote(s.database)
	filter := ""
	if spec.Target.Name != "" {
		filter = " AND name=" + quote(spec.Target.Name)
	}
	var statement string
	switch spec.Object {
	case "catalogs", "databases":
		if spec.Target.Name != "" {
			return "", adapter.ErrInvalid
		}
		statement = "SELECT name AS catalog FROM system.databases WHERE name=" + database + " ORDER BY name"
	case "schemas":
		if spec.Target.Name != "" {
			return "", adapter.ErrInvalid
		}
		statement = "SELECT name AS catalog,name AS schema_name FROM system.databases WHERE name=" + database + " ORDER BY name"
	case "tables":
		statement = "SELECT database AS catalog,database AS schema_name,name,if(position(engine,'View')>0,'VIEW','BASE TABLE') AS type FROM system.tables WHERE database=" + database + filter + " ORDER BY name"
	case "columns":
		filter = ""
		if spec.Target.Name != "" {
			filter = " AND table=" + quote(spec.Target.Name)
		}
		statement = "SELECT database AS schema_name,table AS table_name,name,type,toInt64(position) AS position,if(startsWith(type,'Nullable(') OR startsWith(type,'LowCardinality(Nullable('),'YES','NO') AS nullable,default_expression AS default_value,CAST(NULL,'Nullable(Int64)') AS numeric_precision,CAST(NULL,'Nullable(Int64)') AS numeric_scale FROM system.columns WHERE database=" + database + filter + " ORDER BY table,position"
	default:
		return "", adapter.ErrUnsupported
	}
	return statement + " LIMIT " + strconv.Itoa(spec.Limit) + " OFFSET " + strconv.FormatInt(offset, 10), nil
}
