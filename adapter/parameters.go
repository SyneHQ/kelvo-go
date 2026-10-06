package adapter

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// SQLParameters preserves exact large values. Decimal and unsigned values above
// MaxInt64 use text so database/sql cannot round them or reject uint64 before
// the source applies the statement's parameter type. Driver capabilities still
// decide which declared parameter types are accepted.
func SQLParameters(parameters []operations.Parameter) ([]any, error) {
	values := make([]any, len(parameters))
	for i, parameter := range parameters {
		if err := parameter.Validate(); err != nil {
			return nil, err
		}
		var value any
		if err := operations.DecodeStrict(parameter.Value, &value, 16<<10); err != nil {
			return nil, err
		}
		lexical := func() string {
			switch v := value.(type) {
			case string:
				return v
			case json.Number:
				return string(v)
			}
			return ""
		}
		var err error
		switch {
		case strings.HasPrefix(parameter.Type, "int"):
			value, err = strconv.ParseInt(lexical(), 10, 64)
		case strings.HasPrefix(parameter.Type, "uint"):
			var unsigned uint64
			unsigned, err = strconv.ParseUint(lexical(), 10, 64)
			if unsigned <= math.MaxInt64 {
				value = int64(unsigned)
			} else {
				value = strconv.FormatUint(unsigned, 10)
			}
		case strings.HasPrefix(parameter.Type, "float"):
			bits := 64
			if parameter.Type == "float32" {
				bits = 32
			}
			value, err = strconv.ParseFloat(lexical(), bits)
		case parameter.Type == "binary":
			value, err = base64.StdEncoding.Strict().DecodeString(lexical())
		case parameter.Type == "date":
			value, err = time.Parse("2006-01-02", lexical())
		case parameter.Type == "timestamp":
			value, err = time.Parse(time.RFC3339Nano, lexical())
		case parameter.Type == "json":
			value = string(parameter.Value)
		}
		if err != nil {
			return nil, ErrInvalid
		}
		values[i] = value
	}
	return values, nil
}
