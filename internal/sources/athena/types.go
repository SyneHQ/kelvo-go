// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package athena

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/apache/arrow-go/v18/arrow"
)

var decimalType = regexp.MustCompile(`^decimal(?:\(([0-9]+)(?:,\s*([0-9]+))?\))?$`)
var stringType = regexp.MustCompile(`^(?:varchar|char)(?:\([0-9]+\))?$`)
var timestampType = regexp.MustCompile(`^timestamp(?:\(([0-9]+)\))?( without time zone| with time zone)?$`)
var timestampFraction = regexp.MustCompile(`:[0-9]{2}\.([0-9]+)`)

func makeSchema(columns []athenaColumn) (*arrow.Schema, error) {
	if len(columns) < 1 || len(columns) > 4096 {
		return nil, query.NewError("UNSUPPORTED", "Unsupported Athena result schema")
	}
	fields := make([]arrow.Field, len(columns))
	for i, c := range columns {
		if len(c.Name) > 4096 {
			return nil, query.NewError("UNSUPPORTED", "Athena column name exceeds limit")
		}
		native := strings.ToLower(strings.TrimSpace(c.Type))
		var dt arrow.DataType
		switch {
		case decimalType.MatchString(native):
			parts := decimalType.FindStringSubmatch(native)
			precision, scale := c.Precision, c.Scale
			if parts[1] != "" {
				p, err := strconv.Atoi(parts[1])
				if err != nil {
					return nil, unsupportedType()
				}
				s := 0
				if parts[2] != "" {
					s, err = strconv.Atoi(parts[2])
					if err != nil {
						return nil, unsupportedType()
					}
				}
				if (c.Precision != 0 && c.Precision != p) || (c.Scale != 0 && c.Scale != s) {
					return nil, unsupportedType()
				}
				precision, scale = p, s
			}
			if precision < 1 || precision > 38 || scale < 0 || scale > precision {
				return nil, unsupportedType()
			}
			dt = &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
		case timestampType.MatchString(native):
			parts := timestampType.FindStringSubmatch(native)
			precision := 3
			if parts[1] != "" {
				p, err := strconv.Atoi(parts[1])
				if err != nil {
					return nil, unsupportedType()
				}
				precision = p
			}
			if precision > 9 {
				return nil, unsupportedType()
			}
			unit := arrow.Millisecond
			if precision > 6 {
				unit = arrow.Nanosecond
			} else if precision > 3 {
				unit = arrow.Microsecond
			}
			tz := ""
			if parts[2] == " with time zone" {
				tz = "UTC"
			}
			dt = &arrow.TimestampType{Unit: unit, TimeZone: tz}
		case stringType.MatchString(native) || native == "string":
			dt = arrow.BinaryTypes.String
		case native == "varbinary" || native == "binary":
			dt = arrow.BinaryTypes.Binary
		case native == "real" || native == "float":
			dt = arrow.PrimitiveTypes.Float32
		case native == "double":
			dt = arrow.PrimitiveTypes.Float64
		case native == "boolean":
			dt = arrow.FixedWidthTypes.Boolean
		case native == "tinyint":
			dt = arrow.PrimitiveTypes.Int8
		case native == "smallint":
			dt = arrow.PrimitiveTypes.Int16
		case native == "int" || native == "integer":
			dt = arrow.PrimitiveTypes.Int32
		case native == "bigint":
			dt = arrow.PrimitiveTypes.Int64
		case native == "date":
			dt = arrow.FixedWidthTypes.Date32
		default:
			return nil, unsupportedType()
		}
		fields[i] = arrow.Field{Name: c.Name, Type: dt, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{"native_type": native})}
	}
	return arrow.NewSchema(fields, nil), nil
}
func unsupportedType() error {
	return query.NewError("UNSUPPORTED", "Athena result contains an unsupported type")
}
func convert(dt arrow.DataType, s string) (any, error) {
	fail := func() (any, error) {
		return nil, query.NewError("UNSUPPORTED", "Athena result value cannot be represented without loss")
	}
	switch t := dt.(type) {
	case *arrow.BinaryType:
		// Athena renders varbinary as hexadecimal byte pairs, optionally separated
		// by spaces. Never interpret this text as base64 or silently retain it.
		if strings.Contains(s, " ") {
			parts := strings.Split(s, " ")
			for _, part := range parts {
				if len(part) != 2 {
					return fail()
				}
			}
			s = strings.Join(parts, "")
		}
		value, err := hex.DecodeString(s)
		if err != nil {
			return fail()
		}
		return value, nil
	case *arrow.BooleanType:
		if s != "true" && s != "false" {
			return fail()
		}
	case *arrow.TimestampType:
		if fraction := timestampFraction.FindStringSubmatch(s); len(fraction) > 0 && len(fraction[1]) > 9 {
			return fail()
		}
		layouts := []string{"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999"}
		if t.TimeZone != "" {
			layouts = []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999 Z07:00"}
			for _, zone := range []string{" UTC", " UT", " GMT", " Z"} {
				if strings.HasSuffix(s, zone) {
					s = strings.TrimSuffix(s, zone) + " +00:00"
					break
				}
			}
		}
		for _, layout := range layouts {
			value, err := time.Parse(layout, s)
			if err != nil {
				continue
			}
			stamp, err := arrow.TimestampFromTime(value, t.Unit)
			if err == nil && stamp.ToTime(t.Unit).Equal(value) {
				return value, nil
			}
		}
		return fail()
	}
	row, err := cloudapi.Row(arrow.NewSchema([]arrow.Field{{Name: "v", Type: dt, Nullable: true}}, nil), []any{s}, "athena")
	if err != nil {
		return fail()
	}
	if len(row) != 1 {
		return nil, fmt.Errorf("invalid scalar conversion")
	}
	return row[0], nil
}
