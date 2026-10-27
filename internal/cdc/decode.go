package cdc

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PostgreSQL type OIDs captured exactly. Anything else fails at validation.
var supportedTypes = map[uint32]string{
	16: "bool", 17: "bytea", 20: "int8", 21: "int2", 23: "int4", 25: "text",
	114: "json", 700: "float4", 701: "float8", 1042: "bpchar", 1043: "varchar",
	1082: "date", 1114: "timestamp", 1184: "timestamptz", 1700: "numeric",
	2950: "uuid", 3802: "jsonb",
}

// Text output settings both connections pin so decoding is deterministic.
var sessionParams = map[string]string{
	"DateStyle": "ISO, MDY", "TimeZone": "UTC", "extra_float_digits": "3", "bytea_output": "hex",
}

// decodeText converts one PostgreSQL text-format value. Types map to:
// bool, []byte, int64/int16/int32, string (text, varchar, bpchar, uuid, json,
// jsonb), float32/float64, Numeric, and time.Time in UTC (date, timestamp,
// timestamptz). Infinite or BC dates/timestamps fail.
func decodeText(oid uint32, s string) (any, error) {
	var (
		v   any
		err error
	)
	switch oid {
	case 16:
		switch s {
		case "t":
			v = true
		case "f":
			v = false
		default:
			err = fmt.Errorf("bad bool %q", s)
		}
	case 17:
		if !strings.HasPrefix(s, `\x`) {
			return nil, fmt.Errorf("%w: bytea not in hex output", ErrUnsupported)
		}
		v, err = hex.DecodeString(s[2:])
	case 20:
		v, err = strconv.ParseInt(s, 10, 64)
	case 21:
		var n int64
		n, err = strconv.ParseInt(s, 10, 16)
		v = int16(n)
	case 23:
		var n int64
		n, err = strconv.ParseInt(s, 10, 32)
		v = int32(n)
	case 25, 114, 1042, 1043, 2950, 3802:
		v = s
	case 700:
		var f float64
		f, err = strconv.ParseFloat(s, 32)
		v = float32(f)
	case 701:
		v, err = strconv.ParseFloat(s, 64)
	case 1700:
		v = Numeric(s)
	case 1082:
		v, err = time.Parse("2006-01-02", s)
	case 1114:
		v, err = time.Parse("2006-01-02 15:04:05.999999", s)
	case 1184:
		v, err = time.Parse("2006-01-02 15:04:05.999999-07", s)
		if err == nil {
			v = v.(time.Time).UTC()
		}
	default:
		return nil, fmt.Errorf("%w: type oid %d", ErrUnsupported, oid)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %s value %q: %v", ErrUnsupported, supportedTypes[oid], s, err)
	}
	return v, nil
}
