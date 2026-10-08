// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative

import (
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

type scalarConversion func(any) (any, error)

type columnConversion struct {
	value          scalarConversion
	rejectZeroDate bool
}

type rowConversion struct {
	columns []columnConversion
	row     []any
}

func newRowConversion(schema *arrow.Schema) *rowConversion {
	conversion := &rowConversion{columns: make([]columnConversion, schema.NumFields()), row: make([]any, schema.NumFields())}
	for i := range conversion.columns {
		field := schema.Field(i)
		source, _ := field.Metadata.GetValue("source_type")
		conversion.columns[i] = columnConversion{value: scalarConverter(field.Type), rejectZeroDate: source == "mysql" || source == "mariadb"}
	}
	return conversion
}

// convert borrows its row until the next call. The Arrow writer copies each
// value synchronously before the caller advances the source rows.
func (c *rowConversion) convert(values []any) ([]any, error) {
	if len(values) != len(c.columns) {
		return nil, errors.New("source row width differs from schema")
	}
	clear(c.row)
	for i, value := range values {
		if value == nil {
			continue
		}
		column := c.columns[i]
		if column.rejectZeroDate {
			if date, ok := value.(time.Time); ok && date.IsZero() {
				return nil, errors.New("MySQL zero dates cannot be represented as valid dates")
			}
		}
		converted, err := column.value(value)
		if err != nil {
			return nil, err
		}
		c.row[i] = converted
	}
	return c.row, nil
}

func normalizeRow(schema *arrow.Schema, values []any) ([]any, error) {
	return newRowConversion(schema).convert(values)
}

func normalizeValue(typ arrow.DataType, value any) (any, error) {
	return scalarConverter(typ)(value)
}

func scalarConverter(typ arrow.DataType) scalarConversion {
	switch t := typ.(type) {
	case *arrow.Int8Type:
		return signedConverter[int8](math.MinInt8, math.MaxInt8, 8)
	case *arrow.Int16Type:
		return signedConverter[int16](math.MinInt16, math.MaxInt16, 16)
	case *arrow.Int32Type:
		return signedConverter[int32](math.MinInt32, math.MaxInt32, 32)
	case *arrow.Int64Type:
		return signedConverter[int64](math.MinInt64, math.MaxInt64, 64)
	case *arrow.Uint8Type:
		return unsignedConverter[uint8](math.MaxUint8, 8)
	case *arrow.Uint16Type:
		return unsignedConverter[uint16](math.MaxUint16, 16)
	case *arrow.Uint32Type:
		return unsignedConverter[uint32](math.MaxUint32, 32)
	case *arrow.Uint64Type:
		return unsignedConverter[uint64](math.MaxUint64, 64)
	case *arrow.StringType:
		return func(value any) (any, error) {
			if _, ok := value.(string); ok {
				return value, nil
			}
			return normalizeTextValue(typ, value)
		}
	case *arrow.BinaryType:
		return func(value any) (any, error) {
			if _, ok := value.([]byte); ok {
				return value, nil
			}
			return normalizeTextValue(typ, value)
		}
	case *arrow.BooleanType:
		return func(value any) (any, error) {
			if _, ok := value.(bool); ok {
				return value, nil
			}
			return normalizeTextValue(typ, value)
		}
	case *arrow.Float32Type:
		return func(value any) (any, error) {
			if _, ok := value.(float32); ok {
				return value, nil
			}
			if number, ok := value.(float64); ok && float64(float32(number)) == number {
				return float32(number), nil
			}
			return normalizeTextValue(typ, value)
		}
	case *arrow.Float64Type:
		return func(value any) (any, error) {
			if _, ok := value.(float64); ok {
				return value, nil
			}
			// Keep the existing decimal-text semantics when widening float32.
			return normalizeTextValue(typ, value)
		}
	case *arrow.Date32Type:
		return func(value any) (any, error) {
			date, err := asTime(value)
			if err != nil {
				return nil, errors.New("invalid source date")
			}
			return time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC), nil
		}
	case *arrow.TimestampType:
		return func(value any) (any, error) {
			date, err := asTime(value)
			if err != nil {
				return nil, errors.New("invalid source timestamp")
			}
			if t.TimeZone == "" {
				date = time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), date.Nanosecond(), time.UTC)
			}
			return date.UTC(), nil
		}
	default:
		return func(value any) (any, error) { return normalizeTextValue(typ, value) }
	}
}

type signedValue interface{ int8 | int16 | int32 | int64 }
type unsignedValue interface {
	uint8 | uint16 | uint32 | uint64
}

type integerScalar struct {
	signed     int64
	unsigned   uint64
	isUnsigned bool
}

func integerFrom(value any) (integerScalar, bool) {
	switch v := value.(type) {
	case int8:
		return integerScalar{signed: int64(v)}, true
	case int16:
		return integerScalar{signed: int64(v)}, true
	case int32:
		return integerScalar{signed: int64(v)}, true
	case int64:
		return integerScalar{signed: v}, true
	case uint8:
		return integerScalar{unsigned: uint64(v), isUnsigned: true}, true
	case uint16:
		return integerScalar{unsigned: uint64(v), isUnsigned: true}, true
	case uint32:
		return integerScalar{unsigned: uint64(v), isUnsigned: true}, true
	case uint64:
		return integerScalar{unsigned: v, isUnsigned: true}, true
	default:
		return integerScalar{}, false
	}
}

func signedConverter[T signedValue](minimum, maximum int64, bits int) scalarConversion {
	return func(value any) (any, error) {
		if _, ok := value.(T); ok {
			return value, nil
		}
		if number, ok := integerFrom(value); ok {
			if number.isUnsigned {
				if number.unsigned > uint64(maximum) {
					return nil, strconv.ErrRange
				}
				return T(number.unsigned), nil
			}
			if number.signed < minimum || number.signed > maximum {
				return nil, strconv.ErrRange
			}
			return T(number.signed), nil
		}
		text, err := scalarText(value)
		if err != nil {
			return nil, err
		}
		number, err := strconv.ParseInt(text, 10, bits)
		return T(number), err
	}
}

func unsignedConverter[T unsignedValue](maximum uint64, bits int) scalarConversion {
	return func(value any) (any, error) {
		if _, ok := value.(T); ok {
			return value, nil
		}
		if number, ok := integerFrom(value); ok {
			if number.isUnsigned {
				if number.unsigned > maximum {
					return nil, strconv.ErrRange
				}
				return T(number.unsigned), nil
			}
			if number.signed < 0 || uint64(number.signed) > maximum {
				return nil, strconv.ErrRange
			}
			return T(number.signed), nil
		}
		text, err := scalarText(value)
		if err != nil {
			return nil, err
		}
		number, err := strconv.ParseUint(text, 10, bits)
		return T(number), err
	}
}
