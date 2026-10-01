// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative

import (
	"database/sql"
	"errors"
	"github.com/apache/arrow-go/v18/arrow"
)

func postgresFamily(kind string) bool {
	switch kind {
	case "postgres", "postgresql", "cockroachdb", "alloydb", "redshift":
		return true
	}
	return false
}
func postgresType(c *sql.ColumnType, t string) (arrow.DataType, error) {
	switch t {
	case "BOOL":
		return arrow.FixedWidthTypes.Boolean, nil
	case "INT2":
		return arrow.PrimitiveTypes.Int16, nil
	case "INT4":
		return arrow.PrimitiveTypes.Int32, nil
	case "INT8":
		return arrow.PrimitiveTypes.Int64, nil
	case "OID":
		return arrow.PrimitiveTypes.Uint32, nil
	case "FLOAT4":
		return arrow.PrimitiveTypes.Float32, nil
	case "FLOAT8":
		return arrow.PrimitiveTypes.Float64, nil
	case "BYTEA":
		return arrow.BinaryTypes.Binary, nil
	case "NAME", "TEXT", "VARCHAR", "BPCHAR", "UUID", "JSON", "JSONB":
		return arrow.BinaryTypes.String, nil
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nil
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case "TIMESTAMPTZ":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case "NUMERIC":
		return relationalDecimal(c, 76)
	default:
		return nil, errors.New("unsupported PostgreSQL type")
	}
}
func mysqlType(c *sql.ColumnType, t string) (arrow.DataType, error) {
	switch t {
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8, nil
	case "SMALLINT", "YEAR":
		return arrow.PrimitiveTypes.Int16, nil
	case "MEDIUMINT", "INT":
		return arrow.PrimitiveTypes.Int32, nil
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "UNSIGNED TINYINT":
		return arrow.PrimitiveTypes.Uint8, nil
	case "UNSIGNED SMALLINT":
		return arrow.PrimitiveTypes.Uint16, nil
	case "UNSIGNED MEDIUMINT", "UNSIGNED INT":
		return arrow.PrimitiveTypes.Uint32, nil
	case "UNSIGNED BIGINT":
		return arrow.PrimitiveTypes.Uint64, nil
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32, nil
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, nil
	case "DECIMAL":
		return relationalDecimal(c, 65)
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nil
	case "DATETIME":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case "BIT", "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		return arrow.BinaryTypes.Binary, nil
	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET", "JSON":
		return arrow.BinaryTypes.String, nil
	case "NULL":
		return arrow.Null, nil
	default:
		return nil, errors.New("unsupported MySQL type")
	}
}
func relationalDecimal(c *sql.ColumnType, maxPrecision int64) (arrow.DataType, error) {
	p, s, ok := c.DecimalSize()
	// pgx v5 derives NUMERIC metadata from typmod. Unconstrained NUMERIC returns
	// sentinel values 65535/65531, not a usable precision/scale. Negative scales
	// are encoded in typmod differently across server versions; reject ambiguity.
	if !ok || p < 1 || p > maxPrecision || s < 0 || s > p {
		return nil, errors.New("unverified decimal precision or scale")
	}
	if p <= 38 {
		return &arrow.Decimal128Type{Precision: int32(p), Scale: int32(s)}, nil
	}
	return &arrow.Decimal256Type{Precision: int32(p), Scale: int32(s)}, nil
}
