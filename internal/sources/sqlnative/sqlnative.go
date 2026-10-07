// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package sqlnative provides the bounded database/sql execution path shared by
// native relational sources.
package sqlnative

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
)

type Dialect struct {
	SourceType      string
	DriverName      string
	Backend         string
	ReadOnlySession string
	// ReadOnlyOption is set only when the driver honors database/sql TxOptions.
	ReadOnlyOption bool
	ValidateDSN    func(string) error
	OpenDB         func(string) (*sql.DB, error)
	// Source-aware hooks must be supplied together. Options remain rejected by
	// default; a dialect must validate every accepted option before opening.
	ValidateSource    func(catalog.Source) error
	OpenSourceDB      func(catalog.Source, string) (*sql.DB, error)
	AllowDollarParams bool
	// ErrorCode maps driver errors to a bounded public classification, never text.
	ErrorCode func(error) string
}

type Engine struct {
	sources map[string]catalog.Source
	limits  query.Limits
	dialect Dialect
}

func New(config catalog.Config, limits query.Limits, dialect Dialect) (*Engine, error) {
	if err := limits.Validate(); err != nil || dialect.SourceType == "" || dialect.DriverName == "" || (dialect.ValidateSource == nil) != (dialect.OpenSourceDB == nil) {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid native SQL source configuration")
	}
	sources := make(map[string]catalog.Source)
	for _, source := range config.Sources {
		if source.Type != dialect.SourceType {
			continue
		}
		if !catalog.ValidID(source.ID) || source.DSNEnv == "" || catalog.ValidateEnvironment(source.DSNEnv) != nil || (len(source.Options) != 0 && dialect.ValidateSource == nil) {
			return nil, query.NewError("INVALID_ARGUMENT", "Native SQL source requires an ID and dsn_env")
		}
		if _, exists := sources[source.ID]; exists {
			return nil, query.NewError("INVALID_ARGUMENT", "Duplicate native SQL source")
		}
		source.Options = maps.Clone(source.Options)
		if dialect.ValidateSource != nil {
			checked := source
			checked.Options = maps.Clone(source.Options)
			if err := dialect.ValidateSource(checked); err != nil {
				return nil, query.NewError("INVALID_ARGUMENT", "Native SQL source options are invalid")
			}
		}
		sources[source.ID] = source
	}
	return &Engine{sources: sources, limits: limits, dialect: dialect}, nil
}
func (e *Engine) Close() error { return nil }

func (e *Engine) Execute(parent context.Context, req query.Request, sink query.Sink) (stats query.Stats, err error) {
	stats.Backend, stats.EngineStreaming = e.dialect.SourceType, true
	if e.dialect.Backend != "" {
		stats.Backend = e.dialect.Backend
	}
	started := time.Now()
	defer func() { stats.DurationNS = time.Since(started).Nanoseconds() }()
	if sink == nil || req.Mode != "native" || req.ConnectionID == "" || len(req.Sources) != 0 {
		return stats, query.NewError("INVALID_ARGUMENT", "Native SQL query, connection_id and result sink are required")
	}
	if err := query.ValidateRequest(req); err != nil {
		return stats, err
	}
	normalizedSQL, err := sqlguard.ReadOnlyWithOptions(req.SQL, e.dialect.AllowDollarParams)
	if err != nil {
		return stats, err
	}
	source, ok := e.sources[req.ConnectionID]
	if !ok {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	dsn, ok := os.LookupEnv(source.DSNEnv)
	if !ok || strings.TrimSpace(dsn) == "" {
		return stats, query.NewError("CONFIGURATION_ERROR", "Source credentials are unavailable")
	}
	if len(dsn) > 32<<10 {
		return stats, query.NewError("CONFIGURATION_ERROR", "Source connection details exceed their size limit")
	}
	if e.dialect.ValidateDSN != nil {
		if err := e.dialect.ValidateDSN(dsn); err != nil {
			return stats, e.callbackError(parent, err, "Source connection configuration is invalid")
		}
	}
	values, err := req.Values()
	if err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	var db *sql.DB
	if e.dialect.OpenSourceDB != nil {
		source.Options = maps.Clone(source.Options)
		db, err = e.dialect.OpenSourceDB(source, dsn)
	} else if e.dialect.OpenDB != nil {
		db, err = e.dialect.OpenDB(dsn)
	} else {
		db, err = sql.Open(e.dialect.DriverName, dsn)
	}
	if err != nil {
		if e.dialect.OpenDB != nil || e.dialect.OpenSourceDB != nil {
			return stats, e.callbackError(ctx, err, "Could not initialize source connection")
		}
		return stats, e.sourceError(ctx, err, "Could not initialize source connection")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	db.SetConnMaxLifetime(e.limits.Timeout)
	conn, err := db.Conn(ctx)
	if err != nil {
		return stats, e.sourceError(ctx, err, "Could not connect to source")
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: e.dialect.ReadOnlyOption})
	if err != nil {
		return stats, e.sourceError(ctx, err, "Could not begin read-only source transaction")
	}
	// This is deliberately a rollback-only transaction. A native connector must
	// never make a successful query commit session or transactional changes.
	defer tx.Rollback()
	if e.dialect.ReadOnlySession != "" {
		if _, err = tx.ExecContext(ctx, e.dialect.ReadOnlySession); err != nil {
			return stats, e.sourceError(ctx, err, "Could not enforce read-only source transaction")
		}
	}
	rows, err := tx.QueryContext(ctx, normalizedSQL, values...)
	if err != nil {
		return stats, e.sourceError(ctx, err, "Source rejected query")
	}
	defer rows.Close()
	prepareNS := time.Since(started).Nanoseconds()
	delivered, err := StreamRows(ctx, rows, e.dialect, e.limits, sink)
	stats.PrepareNS = prepareNS + delivered.PrepareNS
	stats.Rows, stats.Bytes, stats.Batches = delivered.Rows, delivered.Bytes, delivered.Batches
	return stats, err
}

// StreamRows converts an already authorized relational result into bounded,
// synchronous Arrow batches. The caller owns rows and its read-only transaction
// and must create them with the same cancellable context. No connection is opened.
func StreamRows(ctx context.Context, rows *sql.Rows, dialect Dialect, limits query.Limits, sink query.Sink) (stats query.Stats, err error) {
	if ctx == nil || rows == nil || sink == nil || limits.Validate() != nil || dialect.SourceType == "" {
		return stats, query.NewError("INVALID_ARGUMENT", "Invalid relational result stream")
	}
	if err := ctx.Err(); err != nil {
		return stats, query.PublicError(err)
	}
	started := time.Now()
	e := &Engine{dialect: dialect, limits: limits}
	columns, err := rows.ColumnTypes()
	if err != nil {
		return stats, e.sourceError(ctx, err, "Source returned an invalid result")
	}
	schema, err := schemaFor(columns, e.dialect)
	if err != nil {
		return stats, query.NewError("UNSUPPORTED", "Source result type is unsupported")
	}
	writer, err := rowarrow.NewWriter(schema, e.limits, sink)
	if err != nil {
		return stats, localResultError(ctx, err, "Could not prepare result stream")
	}
	defer writer.Close()
	stats.PrepareNS = time.Since(started).Nanoseconds()
	valuesOut, scan := make([]any, len(columns)), make([]any, len(columns))
	for i := range valuesOut {
		scan[i] = &valuesOut[i]
	}
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return stats, query.PublicError(err)
		}
		if err := rows.Scan(scan...); err != nil {
			return stats, e.sourceError(ctx, err, "Source returned an invalid row")
		}
		row, err := normalizeRow(schema, valuesOut)
		if err != nil {
			return stats, query.NewError("UNSUPPORTED", "Source result value is unsupported")
		}
		if err = writer.Write(row); err != nil {
			return stats, localResultError(ctx, err, "Could not write result stream")
		}
	}
	if rows.NextResultSet() {
		return stats, query.NewError("UNSUPPORTED", "Source returned multiple result sets")
	}
	if err = rows.Err(); err != nil {
		return stats, e.sourceError(ctx, err, "Source query did not complete")
	}
	written, err := writer.Finish()
	stats.Rows, stats.Bytes, stats.Batches = written.Rows, written.Bytes, written.Batches
	if err != nil {
		return stats, localResultError(ctx, err, "Could not finish result stream")
	}
	return stats, nil
}

func schemaFor(columns []*sql.ColumnType, dialect Dialect) (*arrow.Schema, error) {
	if len(columns) < 1 || len(columns) > 4096 {
		return nil, errors.New("unsupported result column count")
	}
	fields := make([]arrow.Field, len(columns))
	for i, c := range columns {
		name := c.Name()
		if len(name) > 4096 {
			return nil, errors.New("column name too large")
		}
		if name == "" {
			name = fmt.Sprintf("column_%d", i+1)
		}
		typ, err := arrowType(c, dialect)
		if err != nil {
			return nil, err
		}
		fields[i] = arrow.Field{Name: name, Type: typ, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{"native_type": c.DatabaseTypeName(), "source_type": dialect.SourceType})}
	}
	return arrow.NewSchema(fields, nil), nil
}
func arrowType(c *sql.ColumnType, dialect Dialect) (arrow.DataType, error) {
	t := strings.ToUpper(strings.TrimSpace(c.DatabaseTypeName()))
	if postgresFamily(dialect.SourceType) {
		return postgresType(c, t)
	}
	if dialect.SourceType == "mysql" || dialect.SourceType == "mariadb" {
		return mysqlType(c, t)
	}
	switch {
	case dialect.SourceType == "oracle" && t == "LONG":
		return arrow.BinaryTypes.String, nil
	case dialect.SourceType == "oracle" && (t == "TIMESTAMPTZ" || t == "TIMESTAMPTZ_DTY"):
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, nil
	case dialect.SourceType == "oracle" && (t == "TIMESTAMPLTZ_DTY" || t == "TIMESTAMPELTZ"):
		return nil, errors.New("Oracle local timezone decoding requires verified session semantics")
	case dialect.SourceType == "oracle" && t == "IBFLOAT":
		return arrow.PrimitiveTypes.Float32, nil
	case strings.Contains(t, "BOOL") || t == "BIT":
		return arrow.FixedWidthTypes.Boolean, nil
	case t == "BYTEA":
		return arrow.BinaryTypes.Binary, nil
	case t == "UUID" || t == "JSON" || t == "JSONB":
		return arrow.BinaryTypes.String, nil
	case strings.Contains(t, "BINARY") || strings.Contains(t, "BLOB") || t == "RAW":
		return arrow.BinaryTypes.Binary, nil
	case t == "DATE":
		if dialect.SourceType == "sqlserver" {
			return arrow.FixedWidthTypes.Date32, nil
		}
		return &arrow.TimestampType{Unit: arrow.Nanosecond}, nil
	case strings.Contains(t, "TIME") || strings.Contains(t, "TIMESTAMP") || strings.Contains(t, "DATETIME"):
		if strings.Contains(t, "ZONE") || t == "DATETIMEOFFSET" {
			return nil, errors.New("per-value timezone offset unsupported")
		}
		if strings.Contains(t, "TIME") && !strings.Contains(t, "DATE") && !strings.Contains(t, "STAMP") {
			return nil, errors.New("standalone time unsupported")
		}
		return &arrow.TimestampType{Unit: arrow.Nanosecond}, nil
	case strings.Contains(t, "DECIMAL") || strings.Contains(t, "NUMERIC") || strings.Contains(t, "NUMBER") || strings.Contains(t, "MONEY"):
		precision, scale, ok := c.DecimalSize()
		if !ok || precision < 1 || scale > precision || scale < -128 {
			return nil, errors.New("unverified decimal precision or scale")
		}
		if precision <= 38 {
			return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
		}
		if precision <= 76 {
			return &arrow.Decimal256Type{Precision: int32(precision), Scale: int32(scale)}, nil
		}
		return nil, errors.New("decimal precision unsupported")
	case t == "INT2" || t == "SMALLINT":
		return arrow.PrimitiveTypes.Int16, nil
	case t == "INT4" || t == "INTEGER" || t == "INT":
		return arrow.PrimitiveTypes.Int32, nil
	case t == "INT8" || t == "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case t == "FLOAT4" || t == "REAL":
		return arrow.PrimitiveTypes.Float32, nil
	case t == "FLOAT8" || t == "DOUBLE PRECISION" || t == "DOUBLE":
		return arrow.PrimitiveTypes.Float64, nil
	case strings.Contains(t, "UNSIGNED") && strings.Contains(t, "BIGINT"):
		return arrow.PrimitiveTypes.Uint64, nil
	case strings.Contains(t, "UNSIGNED") && strings.Contains(t, "INT"):
		return arrow.PrimitiveTypes.Uint32, nil
	case t == "TINYINT":
		if dialect.SourceType == "sqlserver" {
			return arrow.PrimitiveTypes.Uint8, nil
		}
		return arrow.PrimitiveTypes.Int8, nil
	case strings.Contains(t, "SMALLINT"):
		return arrow.PrimitiveTypes.Int16, nil
	case t == "INT" || t == "INTEGER":
		return arrow.PrimitiveTypes.Int32, nil
	case strings.Contains(t, "BIGINT"):
		return arrow.PrimitiveTypes.Int64, nil
	case strings.Contains(t, "REAL"):
		return arrow.PrimitiveTypes.Float32, nil
	case strings.Contains(t, "FLOAT") || strings.Contains(t, "DOUBLE"):
		return arrow.PrimitiveTypes.Float64, nil
	case strings.Contains(t, "CHAR") || strings.Contains(t, "TEXT") || strings.Contains(t, "CLOB") || strings.Contains(t, "VARCHAR"):
		return arrow.BinaryTypes.String, nil
	default:
		return nil, errors.New("database type unsupported")
	}
}
func normalizeRow(schema *arrow.Schema, values []any) ([]any, error) {
	if len(values) != schema.NumFields() {
		return nil, errors.New("source row width differs from schema")
	}
	out := make([]any, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}
		sourceType, _ := schema.Field(i).Metadata.GetValue("source_type")
		if sourceType == "mysql" || sourceType == "mariadb" {
			if tv, ok := value.(time.Time); ok && tv.IsZero() {
				return nil, errors.New("MySQL zero dates cannot be represented as valid dates")
			}
		}
		var err error
		out[i], err = normalizeValue(schema.Field(i).Type, value)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
func normalizeValue(typ arrow.DataType, value any) (any, error) {
	text, err := scalarText(value)
	if err != nil {
		return nil, err
	}
	switch t := typ.(type) {
	case *arrow.StringType:
		switch value.(type) {
		case string, []byte:
		default:
			return nil, errors.New("source text is not a string")
		}
		return text, nil
	case *arrow.BinaryType:
		if v, ok := value.([]byte); ok {
			// The synchronous row writer copies bytes before rows.Next advances.
			return v, nil
		}
		if _, ok := value.(string); !ok {
			return nil, errors.New("source binary is not bytes")
		}
		return []byte(text), nil
	case *arrow.Int8Type:
		n, err := strconv.ParseInt(text, 10, 8)
		return int8(n), err
	case *arrow.Int16Type:
		n, err := strconv.ParseInt(text, 10, 16)
		return int16(n), err
	case *arrow.Int32Type:
		n, err := strconv.ParseInt(text, 10, 32)
		return int32(n), err
	case *arrow.Int64Type:
		n, err := strconv.ParseInt(text, 10, 64)
		return n, err
	case *arrow.Uint8Type:
		n, err := strconv.ParseUint(text, 10, 8)
		return uint8(n), err
	case *arrow.Uint16Type:
		n, err := strconv.ParseUint(text, 10, 16)
		return uint16(n), err
	case *arrow.Uint32Type:
		n, err := strconv.ParseUint(text, 10, 32)
		return uint32(n), err
	case *arrow.Uint64Type:
		n, err := strconv.ParseUint(text, 10, 64)
		return n, err
	case *arrow.Float32Type:
		n, err := strconv.ParseFloat(text, 32)
		return float32(n), err
	case *arrow.Float64Type:
		n, err := strconv.ParseFloat(text, 64)
		return n, err
	case *arrow.BooleanType:
		n, err := strconv.ParseBool(text)
		return n, err
	case *arrow.Date32Type:
		tv, err := asTime(value)
		if err != nil {
			return nil, errors.New("invalid source date")
		}
		return time.Date(tv.Year(), tv.Month(), tv.Day(), 0, 0, 0, 0, time.UTC), nil
	case *arrow.TimestampType:
		tv, err := asTime(value)
		if err != nil {
			return nil, errors.New("invalid source timestamp")
		}
		if t.TimeZone == "" {
			tv = time.Date(tv.Year(), tv.Month(), tv.Day(), tv.Hour(), tv.Minute(), tv.Second(), tv.Nanosecond(), time.UTC)
		}
		return tv.UTC(), err
	case *arrow.Decimal128Type:
		if _, ok := value.(float64); ok {
			return nil, errors.New("floating source cannot preserve decimal precision")
		}
		if _, ok := value.(float32); ok {
			return nil, errors.New("floating source cannot preserve decimal precision")
		}
		if !exactDecimal(text, t.Scale) {
			return nil, errors.New("decimal scale would lose precision")
		}
		return decimal128.FromString(text, t.Precision, t.Scale)
	case *arrow.Decimal256Type:
		if _, ok := value.(float64); ok {
			return nil, errors.New("floating source cannot preserve decimal precision")
		}
		if _, ok := value.(float32); ok {
			return nil, errors.New("floating source cannot preserve decimal precision")
		}
		if !exactDecimal(text, t.Scale) {
			return nil, errors.New("decimal scale would lose precision")
		}
		return decimal256.FromString(text, t.Precision, t.Scale)
	default:
		return nil, errors.New("unsupported result type")
	}
}
func scalarText(value any) (string, error) {
	switch v := value.(type) {
	case time.Time:
		return "", nil
	case []byte:
		return string(v), nil
	case string:
		return v, nil
	case int64, int32, int16, int8, uint64, uint32, uint16, uint8, float64, float32, bool:
		return fmt.Sprint(v), nil
	default:
		return "", errors.New("unsupported scalar value")
	}
}

var decimalSyntax = regexp.MustCompile(`^[+-]?[0-9]+(?:\.[0-9]+)?$`)

func exactDecimal(value string, scale int32) bool {
	if len(value) > 256 || !decimalSyntax.MatchString(value) || scale < -128 || scale > 76 {
		return false
	}
	r, ok := new(big.Rat).SetString(value)
	if !ok {
		return false
	}
	if scale < 0 {
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-scale)), nil)
		return new(big.Int).Mod(r.Num(), factor).Sign() == 0 && r.Denom().Cmp(big.NewInt(1)) == 0
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	n := new(big.Int).Mul(r.Num(), factor)
	return new(big.Int).Mod(n, r.Denom()).Sign() == 0
}

func asTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case []byte:
		return time.Parse(time.RFC3339Nano, string(v))
	case string:
		return time.Parse(time.RFC3339Nano, v)
	}
	return time.Time{}, errors.New("unsupported time")
}
