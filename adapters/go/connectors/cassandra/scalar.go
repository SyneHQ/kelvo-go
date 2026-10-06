package cassandra

import (
	"math"
	"math/big"
	"net"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

func parameterValues(parameters []operations.Parameter) ([]any, error) {
	for _, parameter := range parameters {
		switch parameter.Type {
		case "null", "bool", "string", "int64", "float64", "binary", "timestamp":
		default:
			return nil, adapter.ErrUnsupported
		}
	}
	return adapter.SQLParameters(parameters)
}

func resultSchema(columns []column) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(columns))
	seen := map[string]bool{}
	for index, column := range columns {
		if column.Name == "" || seen[column.Name] || !utf8.ValidString(column.Name) {
			return nil, adapter.ErrUnsupported
		}
		seen[column.Name] = true
		typeName := strings.ToLower(column.SourceType)
		var typ arrow.DataType
		switch typeName {
		case "tinyint":
			typ = arrow.PrimitiveTypes.Int8
		case "smallint":
			typ = arrow.PrimitiveTypes.Int16
		case "int":
			typ = arrow.PrimitiveTypes.Int32
		case "bigint", "counter":
			typ = arrow.PrimitiveTypes.Int64
		case "float":
			typ = arrow.PrimitiveTypes.Float32
		case "double":
			typ = arrow.PrimitiveTypes.Float64
		case "boolean":
			typ = arrow.FixedWidthTypes.Boolean
		case "ascii", "text", "varchar", "inet", "decimal", "varint":
			typ = arrow.BinaryTypes.String
		case "blob":
			typ = arrow.BinaryTypes.Binary
		case "uuid", "timeuuid":
			typ = &arrow.FixedSizeBinaryType{ByteWidth: 16}
		case "timestamp":
			typ = &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}
		case "date":
			typ = arrow.FixedWidthTypes.Date32
		case "time":
			typ = &arrow.Time64Type{Unit: arrow.Nanosecond}
		default:
			return nil, adapter.ErrUnsupported
		}
		metadata := map[string]string{"kelvo.source_type": typeName}
		if typeName == "decimal" || typeName == "varint" {
			// CQL permits unbounded precision and per-value scale, so a fixed
			// Arrow decimal type would silently narrow some valid source values.
			metadata["kelvo.logical_type"] = "exact_" + typeName
		}
		fields[index] = arrow.Field{Name: column.Name, Type: typ, Nullable: true, Metadata: arrow.MetadataFrom(metadata)}
	}
	return arrow.NewSchema(fields, nil), nil
}

// normalizeValue validates before appending and counts conservative Arrow value
// bytes (validity, offsets and values). IPC/schema overhead is not included.
func normalizeValue(sourceType string, value any) (any, int64, error) {
	if value == nil {
		return nil, 17, nil
	}
	var normalized any
	size := int64(17)
	switch strings.ToLower(sourceType) {
	case "tinyint", "smallint", "int", "bigint", "counter":
		v := reflect.ValueOf(value)
		if v.Kind() < reflect.Int || v.Kind() > reflect.Int64 {
			return nil, 0, adapter.ErrUnsupported
		}
		n := v.Int()
		switch strings.ToLower(sourceType) {
		case "tinyint":
			if n < math.MinInt8 || n > math.MaxInt8 {
				return nil, 0, adapter.ErrUnsupported
			}
			normalized = int8(n)
		case "smallint":
			if n < math.MinInt16 || n > math.MaxInt16 {
				return nil, 0, adapter.ErrUnsupported
			}
			normalized = int16(n)
		case "int":
			if n < math.MinInt32 || n > math.MaxInt32 {
				return nil, 0, adapter.ErrUnsupported
			}
			normalized = int32(n)
		default:
			normalized = n
		}
	case "float":
		v, ok := value.(float32)
		if !ok || math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, 0, adapter.ErrUnsupported
		}
		normalized = v
	case "double":
		v, ok := value.(float64)
		if !ok || math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, 0, adapter.ErrUnsupported
		}
		normalized = v
	case "boolean":
		v, ok := value.(bool)
		if !ok {
			return nil, 0, adapter.ErrUnsupported
		}
		normalized = v
	case "ascii", "text", "varchar":
		v, ok := value.(string)
		if !ok || !utf8.ValidString(v) {
			return nil, 0, adapter.ErrUnsupported
		}
		normalized = v
		size += int64(len(v))
	case "blob":
		v, ok := value.([]byte)
		if !ok {
			return nil, 0, adapter.ErrUnsupported
		}
		normalized = v
		size += int64(len(v))
	case "uuid", "timeuuid":
		v, ok := value.(gocql.UUID)
		if !ok {
			return nil, 0, adapter.ErrUnsupported
		}
		normalized = v[:]
	case "inet":
		v, ok := value.(net.IP)
		if !ok || v.To16() == nil {
			return nil, 0, adapter.ErrUnsupported
		}
		normalized = v.String()
		size += int64(len(normalized.(string)))
	case "timestamp":
		v, ok := value.(time.Time)
		if !ok {
			return nil, 0, adapter.ErrUnsupported
		}
		stamp, err := arrow.TimestampFromTime(v, arrow.Millisecond)
		if err != nil || !stamp.ToTime(arrow.Millisecond).Equal(v) {
			return nil, 0, adapter.ErrUnsupported
		}
		normalized = stamp
	case "date":
		v, ok := value.(string)
		if !ok {
			return nil, 0, adapter.ErrUnsupported
		}
		parsed, err := time.Parse("2006-01-02", v)
		if err != nil {
			return nil, 0, adapter.ErrUnsupported
		}
		days := parsed.Unix() / 86400
		if days < math.MinInt32 || days > math.MaxInt32 {
			return nil, 0, adapter.ErrUnsupported
		}
		normalized = arrow.Date32(days)
	case "time":
		v, ok := value.(time.Duration)
		if !ok || v < 0 || v >= 24*time.Hour {
			return nil, 0, adapter.ErrUnsupported
		}
		normalized = arrow.Time64(v)
	case "varint":
		v, ok := value.(*big.Int)
		if !ok {
			return nil, 0, adapter.ErrUnsupported
		}
		if v == nil {
			return nil, size, nil
		}
		if v.BitLen() > 1<<18 {
			return nil, 0, adapter.ErrLimit
		}
		normalized = v.String()
		size += int64(len(normalized.(string)))
	case "decimal":
		v, ok := value.(*inf.Dec)
		if !ok {
			return nil, 0, adapter.ErrUnsupported
		}
		if v == nil {
			return nil, size, nil
		}
		if v.Scale() > 10000 || v.Scale() < -10000 || v.UnscaledBig().BitLen() > 1<<18 {
			return nil, 0, adapter.ErrLimit
		}
		normalized = v.String()
		size += int64(len(normalized.(string)))
	default:
		return nil, 0, adapter.ErrUnsupported
	}
	if size > 1<<20 {
		return nil, 0, adapter.ErrLimit
	}
	return normalized, size, nil
}

func appendValue(builder array.Builder, value any) {
	if value == nil {
		builder.AppendNull()
		return
	}
	switch b := builder.(type) {
	case *array.Int8Builder:
		b.Append(value.(int8))
	case *array.Int16Builder:
		b.Append(value.(int16))
	case *array.Int32Builder:
		b.Append(value.(int32))
	case *array.Int64Builder:
		b.Append(value.(int64))
	case *array.Float32Builder:
		b.Append(value.(float32))
	case *array.Float64Builder:
		b.Append(value.(float64))
	case *array.BooleanBuilder:
		b.Append(value.(bool))
	case *array.StringBuilder:
		b.Append(value.(string))
	case *array.BinaryBuilder:
		b.Append(value.([]byte))
	case *array.FixedSizeBinaryBuilder:
		b.Append(value.([]byte))
	case *array.TimestampBuilder:
		b.Append(value.(arrow.Timestamp))
	case *array.Date32Builder:
		b.Append(value.(arrow.Date32))
	case *array.Time64Builder:
		b.Append(value.(arrow.Time64))
	}
}
