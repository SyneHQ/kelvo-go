// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package clickhouse

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
	"github.com/shopspring/decimal"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

const nativeBatchRows = 4096

// executeNative runs one read over ClickHouse's native TCP+TLS protocol
// (clickhouses://host:port?database=db) with the same settings as HTTP.
func (e *Engine) executeNative(ctx context.Context, endpoint *url.URL, config *tls.Config, username, password, queryID, sql string, sink query.Sink, stats *query.Stats, started time.Time) (err error) {
	host := endpoint.Hostname()
	conn, err := ch.Open(&ch.Options{
		Protocol:        ch.Native,
		Addr:            []string{endpoint.Host},
		Auth:            ch.Auth{Database: endpoint.Query().Get("database"), Username: username, Password: password},
		TLS:             config.Clone(),
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    1,
		MaxIdleConns:    1,
		ConnMaxLifetime: e.limits.Timeout + time.Minute,
	})
	if err != nil {
		return failed(ctx, sourceError(ctx, "Could not execute source query"), "other", host, nil, err)
	}
	defer conn.Close()
	rows, err := e.queryNative(ctx, conn, queryID, sql)
	if err != nil {
		return nativeFailed(ctx, host, err)
	}
	defer rows.Close()
	columns := rows.ColumnTypes()
	fields := make([]arrow.Field, len(columns))
	appenders := make([]func(array.Builder, reflect.Value), len(columns))
	dest := make([]any, len(columns))
	for i, column := range columns {
		dataType, nullable, appendFn := nativeType(column.DatabaseTypeName())
		fields[i] = arrow.Field{Name: column.Name(), Type: dataType, Nullable: nullable}
		appenders[i] = appendFn
		dest[i] = reflect.New(column.ScanType()).Interface()
	}
	schema := arrow.NewSchema(fields, nil)
	stats.PrepareNS = time.Since(started).Nanoseconds()
	if err := sink.Schema(schema); err != nil {
		return query.PublicError(err)
	}
	allocator := &boundedAllocator{base: memory.NewGoAllocator(), limit: int64(e.limits.MemoryMB) * (1 << 20)}
	defer func() {
		if recover() != nil { // only boundedAllocator panics inside builders
			err = query.NewError("RESOURCE_EXHAUSTED", "Source result exceeds memory or byte limit")
		}
	}()
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()
	pending := int64(0)
	flush := func() error {
		if pending == 0 {
			return nil
		}
		record := builder.NewRecordBatch()
		defer record.Release()
		pending = 0
		if record.NumRows() > e.limits.MaxRows-stats.Rows {
			return query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds row limit")
		}
		batchBytes := arrowutil.TotalRecordSize(record)
		if batchBytes > e.limits.MaxBytes-stats.Bytes {
			return query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds byte limit")
		}
		if err := sink.Write(record); err != nil {
			return query.PublicError(err)
		}
		stats.Rows += record.NumRows()
		stats.Bytes += batchBytes
		stats.Batches++
		return nil
	}
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return query.PublicError(err)
		}
		if err := rows.Scan(dest...); err != nil {
			return nativeFailed(ctx, host, err)
		}
		for i, value := range dest {
			appenders[i](builder.Field(i), reflect.ValueOf(value).Elem())
		}
		pending++
		if pending >= nativeBatchRows || stats.Rows+pending > e.limits.MaxRows {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nativeFailed(ctx, host, err)
	}
	if err := flush(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return query.PublicError(err)
	}
	return nil
}

// queryNative sends the query with Kelvo's limits as settings. A managed
// profile may constrain some of them (code 452, SETTING_CONSTRAINT_VIOLATION);
// then retry without the resource settings, whose server-side limits the
// profile already enforces, and finally with readonly alone. readonly=1 is
// never dropped; Kelvo still enforces rows, bytes and its own timeout.
func (e *Engine) queryNative(ctx context.Context, conn driver.Conn, queryID, sql string) (driver.Rows, error) {
	tiers := []ch.Settings{
		{
			"readonly":             1,
			"max_execution_time":   int64(math.Ceil(e.limits.Timeout.Seconds())),
			"max_result_rows":      e.limits.MaxRows,
			"max_result_bytes":     e.limits.MaxBytes,
			"result_overflow_mode": "throw",
			"max_memory_usage":     int64(e.limits.MemoryMB) * (1 << 20),
			"max_threads":          e.limits.Threads,
		},
		{"readonly": 1, "max_result_rows": e.limits.MaxRows, "max_result_bytes": e.limits.MaxBytes, "result_overflow_mode": "throw"},
		{"readonly": 1},
	}
	var err error
	for i, settings := range tiers {
		id := queryID
		if i > 0 {
			id = fmt.Sprintf("%s-%d", queryID, i)
		}
		var rows driver.Rows
		// ctx cancellation makes the driver send Cancel.
		rows, err = conn.Query(ch.Context(ctx, ch.WithQueryID(id), ch.WithSettings(settings)), sql)
		var exception *ch.Exception
		if err == nil || !errors.As(err, &exception) || exception.Code != settingConstraintViolation {
			return rows, err
		}
	}
	return nil, err
}

const settingConstraintViolation = 452

// nativeFailed classifies a driver error into failure.go's vocabulary. A
// server exception keeps the public HTTP wording and records its code.
func nativeFailed(ctx context.Context, host string, err error) error {
	var exception *ch.Exception
	if errors.As(err, &exception) {
		public := sourceError(ctx, "Source rejected query")
		if exception.Code == 241 && ctx.Err() == nil {
			public = query.NewError("RESOURCE_EXHAUSTED", "ClickHouse source query exceeded its memory limit (code 241)")
		}
		f := failed(ctx, public, "http_status", host, nil, nil).(*Failure)
		f.ClickHouseCode = strconv.Itoa(int(exception.Code))
		return f
	}
	return failed(ctx, sourceError(ctx, "Could not execute source query"), "other", host, nil, err)
}

var (
	decimalType    = regexp.MustCompile(`^Decimal\((\d+),\s*(\d+)\)$`)
	dateTime64Type = regexp.MustCompile(`^DateTime64\((\d)(?:,\s*'([^']*)')?\)$`)
	dateTimeType   = regexp.MustCompile(`^DateTime(?:\('([^']*)'\))?$`)
)

// nativeType maps a ClickHouse result type to Arrow and a value appender. The
// reflect.Value is the scanned destination (a pointer for Nullable columns).
func nativeType(name string) (arrow.DataType, bool, func(array.Builder, reflect.Value)) {
	inner := name
	if s, ok := strings.CutPrefix(inner, "LowCardinality("); ok {
		inner = strings.TrimSuffix(s, ")")
	}
	nullable := false
	if s, ok := strings.CutPrefix(inner, "Nullable("); ok {
		inner, nullable = strings.TrimSuffix(s, ")"), true
	}
	dataType, appendValue := scalarType(inner)
	return dataType, nullable, func(b array.Builder, v reflect.Value) {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				b.AppendNull()
				return
			}
			v = v.Elem()
		}
		appendValue(b, v.Interface())
	}
}

func scalarType(name string) (arrow.DataType, func(array.Builder, any)) {
	switch name {
	case "Int8":
		return arrow.PrimitiveTypes.Int8, func(b array.Builder, v any) { b.(*array.Int8Builder).Append(v.(int8)) }
	case "Int16":
		return arrow.PrimitiveTypes.Int16, func(b array.Builder, v any) { b.(*array.Int16Builder).Append(v.(int16)) }
	case "Int32":
		return arrow.PrimitiveTypes.Int32, func(b array.Builder, v any) { b.(*array.Int32Builder).Append(v.(int32)) }
	case "Int64":
		return arrow.PrimitiveTypes.Int64, func(b array.Builder, v any) { b.(*array.Int64Builder).Append(v.(int64)) }
	case "UInt8":
		return arrow.PrimitiveTypes.Uint8, func(b array.Builder, v any) { b.(*array.Uint8Builder).Append(v.(uint8)) }
	case "UInt16":
		return arrow.PrimitiveTypes.Uint16, func(b array.Builder, v any) { b.(*array.Uint16Builder).Append(v.(uint16)) }
	case "UInt32":
		return arrow.PrimitiveTypes.Uint32, func(b array.Builder, v any) { b.(*array.Uint32Builder).Append(v.(uint32)) }
	case "UInt64":
		return arrow.PrimitiveTypes.Uint64, func(b array.Builder, v any) { b.(*array.Uint64Builder).Append(v.(uint64)) }
	case "Float32":
		return arrow.PrimitiveTypes.Float32, func(b array.Builder, v any) { b.(*array.Float32Builder).Append(v.(float32)) }
	case "Float64":
		return arrow.PrimitiveTypes.Float64, func(b array.Builder, v any) { b.(*array.Float64Builder).Append(v.(float64)) }
	case "Bool":
		return arrow.FixedWidthTypes.Boolean, func(b array.Builder, v any) { b.(*array.BooleanBuilder).Append(v.(bool)) }
	case "String":
		return arrow.BinaryTypes.String, func(b array.Builder, v any) { b.(*array.StringBuilder).Append(v.(string)) }
	case "Date", "Date32":
		return arrow.FixedWidthTypes.Date32, func(b array.Builder, v any) {
			t := v.(time.Time)
			b.(*array.Date32Builder).Append(arrow.Date32FromTime(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)))
		}
	}
	if strings.HasPrefix(name, "FixedString(") {
		return arrow.BinaryTypes.Binary, func(b array.Builder, v any) { b.(*array.BinaryBuilder).Append([]byte(v.(string))) }
	}
	if m := dateTimeType.FindStringSubmatch(name); m != nil {
		return timestampType(arrow.Second, m[1])
	}
	if m := dateTime64Type.FindStringSubmatch(name); m != nil {
		unit := arrow.Nanosecond
		switch p, _ := strconv.Atoi(m[1]); {
		case p == 0:
			unit = arrow.Second
		case p <= 3:
			unit = arrow.Millisecond
		case p <= 6:
			unit = arrow.Microsecond
		}
		return timestampType(unit, m[2])
	}
	if m := decimalType.FindStringSubmatch(name); m != nil {
		precision, _ := strconv.Atoi(m[1])
		scale, _ := strconv.Atoi(m[2])
		if precision <= 38 {
			return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, func(b array.Builder, v any) {
				unscaled := v.(decimal.Decimal).Shift(int32(scale)).BigInt()
				b.(*array.Decimal128Builder).Append(decimal128.FromBigInt(unscaled))
			}
		}
	}
	// ponytail: Decimal(>38), UUID, Enum, IPv4/6, Array, Map, Tuple, JSON,
	// Variant and other types are delivered as their text form; add nested
	// Arrow types when a consumer needs structure rather than display text.
	return arrow.BinaryTypes.String, func(b array.Builder, v any) {
		if d, ok := v.(decimal.Decimal); ok {
			b.(*array.StringBuilder).Append(d.String())
			return
		}
		if n, ok := v.(big.Int); ok {
			b.(*array.StringBuilder).Append(n.String())
			return
		}
		b.(*array.StringBuilder).Append(fmt.Sprint(v))
	}
}

func timestampType(unit arrow.TimeUnit, zone string) (arrow.DataType, func(array.Builder, any)) {
	if zone == "" {
		zone = "UTC"
	}
	return &arrow.TimestampType{Unit: unit, TimeZone: zone}, func(b array.Builder, v any) {
		t := v.(time.Time)
		var n int64
		switch unit {
		case arrow.Second:
			n = t.Unix()
		case arrow.Millisecond:
			n = t.UnixMilli()
		case arrow.Microsecond:
			n = t.UnixMicro()
		default:
			n = t.UnixNano()
		}
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(n))
	}
}
