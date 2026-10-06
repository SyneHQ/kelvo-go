package cloudapi

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// OperationParameters keeps the native analytical request's typed values intact.
func OperationParameters(parameters []query.Parameter) []operations.Parameter {
	out := make([]operations.Parameter, len(parameters))
	for i, p := range parameters {
		value := p.Value
		if p.Type == "null" && len(value) == 0 {
			value = json.RawMessage("null")
		}
		out[i] = operations.Parameter{Type: p.Type, Value: value}
	}
	return out
}

func parameterValue(p operations.Parameter) (any, string, error) {
	if p.Validate() != nil {
		return nil, "", operations.ErrInvalid
	}
	var value any
	if operations.DecodeStrict(p.Value, &value, 16<<10) != nil {
		return nil, "", operations.ErrInvalid
	}
	var lexical string
	switch v := value.(type) {
	case string:
		lexical = v
	case json.Number:
		lexical = string(v)
	case bool:
		lexical = strconv.FormatBool(v)
	}
	return value, lexical, nil
}

// D1Parameters follows D1's JSON binding protocol. Integers outside JavaScript's
// exact range fail before HTTP rather than rounding during provider decoding.
func D1Parameters(parameters []operations.Parameter) ([]any, error) {
	if len(parameters) > 1000 {
		return nil, operations.ErrInvalid
	}
	out := make([]any, len(parameters))
	for i, p := range parameters {
		value, lexical, err := parameterValue(p)
		if err != nil {
			return nil, err
		}
		switch {
		case p.Type == "null", p.Type == "string", p.Type == "date", p.Type == "timestamp":
			out[i] = value
		case p.Type == "bool":
			out[i] = 0
			if value.(bool) {
				out[i] = 1
			}
		case strings.HasPrefix(p.Type, "int"):
			n, _ := strconv.ParseInt(lexical, 10, 64)
			if n < -9007199254740991 || n > 9007199254740991 {
				return nil, unsupportedParameters()
			}
			out[i] = n
		case strings.HasPrefix(p.Type, "uint"):
			n, _ := strconv.ParseUint(lexical, 10, 64)
			if n > 9007199254740991 {
				return nil, unsupportedParameters()
			}
			out[i] = n
		case p.Type == "float32", p.Type == "float64":
			bits := 64
			if p.Type == "float32" {
				bits = 32
			}
			out[i], _ = strconv.ParseFloat(lexical, bits)
		case p.Type == "binary":
			data, _ := base64.StdEncoding.Strict().DecodeString(lexical)
			bytes := make([]int, len(data))
			for j, b := range data {
				bytes[j] = int(b)
			}
			out[i] = bytes
		default:
			return nil, unsupportedParameters()
		}
	}
	return out, nil
}

type StatementParameter struct {
	Name  string  `json:"name"`
	Type  string  `json:"type"`
	Value *string `json:"value,omitempty"`
}

// DatabricksParameters uses the documented named, typed string representation;
// the absent value is NULL and an explicit empty string stays an empty string.
func DatabricksParameters(parameters []operations.Parameter) ([]StatementParameter, error) {
	if len(parameters) > 1000 {
		return nil, operations.ErrInvalid
	}
	out := make([]StatementParameter, len(parameters))
	for i, p := range parameters {
		_, lexical, err := parameterValue(p)
		if err != nil {
			return nil, err
		}
		typ := ""
		switch p.Type {
		case "null", "string":
			typ = "STRING"
		case "bool":
			typ = "BOOLEAN"
		case "int8":
			typ = "TINYINT"
		case "int16", "uint8":
			typ = "SMALLINT"
		case "int32", "uint16":
			typ = "INT"
		case "int64", "uint32":
			typ = "BIGINT"
		case "uint64":
			typ = "DECIMAL(20,0)"
		case "float32":
			typ = "FLOAT"
		case "float64":
			typ = "DOUBLE"
		case "date":
			typ = "DATE"
		case "timestamp":
			typ = "TIMESTAMP"
			v, _ := time.Parse(time.RFC3339Nano, lexical)
			if v.Nanosecond()%1000 != 0 {
				return nil, unsupportedParameters()
			}
			lexical = v.UTC().Format(time.RFC3339Nano)
		case "decimal128":
			digits := strings.TrimPrefix(lexical, "-")
			scale := 0
			if at := strings.IndexByte(digits, '.'); at >= 0 {
				scale = len(digits) - at - 1
			}
			precision := len(strings.ReplaceAll(digits, ".", ""))
			typ = "DECIMAL(" + strconv.Itoa(precision) + "," + strconv.Itoa(scale) + ")"
		default:
			return nil, unsupportedParameters()
		}
		out[i] = StatementParameter{Name: "p" + strconv.Itoa(i+1), Type: typ}
		if p.Type != "null" {
			out[i].Value = &lexical
		}
	}
	return out, nil
}

func unsupportedParameters() error {
	return query.NewError("UNSUPPORTED", "Parameter type or precision is unavailable in the source protocol")
}
