package clickhouselambda

import (
	"context"
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/apache/arrow-go/v18/arrow"
)

// The Lambda contract supplies bare TSV without names or types. Keep text
// exact, preserve empty strings, and recognize only the documented NULL escape.
func writeTSV(ctx context.Context, body string, limits query.Limits, sink query.Sink) (query.Stats, error) {
	var stats query.Stats
	if body == "" {
		return stats, sink.Schema(arrow.NewSchema(nil, nil))
	}
	body = strings.TrimSuffix(body, "\n")
	first, _, _ := strings.Cut(body, "\n")
	width := strings.Count(first, "\t") + 1
	if width > 1024 {
		return stats, query.NewError("RESOURCE_EXHAUSTED", "Lambda result has too many columns")
	}
	fields := make([]arrow.Field, width)
	for i := range fields {
		fields[i] = arrow.Field{Name: "col" + strconv.Itoa(i+1), Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{"kelvo.source_type": "untyped-clickhouse-tsv", "kelvo.logical_type": "exact_text"})}
	}
	writer, err := rowarrow.NewWriter(arrow.NewSchema(fields, nil), limits, sink)
	if err != nil {
		return stats, err
	}
	defer writer.Close()
	for line := range strings.SplitSeq(body, "\n") {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		cells := strings.Split(line, "\t")
		if len(cells) != width {
			return stats, query.NewError("QUERY_FAILED", "Lambda returned inconsistent row widths")
		}
		values := make([]any, width)
		for i, cell := range cells {
			if cell == `\N` {
				continue
			}
			value, err := unescapeTSV(cell)
			if err != nil {
				return stats, err
			}
			values[i] = value
		}
		if err := writer.Write(values); err != nil {
			return stats, err
		}
	}
	return writer.Finish()
}

func unescapeTSV(value string) (string, error) {
	bad := query.NewError("QUERY_FAILED", "Lambda returned unsupported TSV data")
	if len(value) > 1<<20 {
		return "", query.NewError("RESOURCE_EXHAUSTED", "Lambda value exceeds size limit")
	}
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] != '\\' {
			out.WriteByte(value[i])
			continue
		}
		i++
		if i == len(value) {
			return "", bad
		}
		switch value[i] {
		case '0':
			out.WriteByte(0)
		case 'a':
			out.WriteByte('\a')
		case 'b':
			out.WriteByte('\b')
		case 'f':
			out.WriteByte('\f')
		case 'n':
			out.WriteByte('\n')
		case 'r':
			out.WriteByte('\r')
		case 't':
			out.WriteByte('\t')
		case 'v':
			out.WriteByte('\v')
		case '\\', '\'':
			out.WriteByte(value[i])
		case 'x':
			if i+2 >= len(value) {
				return "", bad
			}
			b, err := hex.DecodeString(value[i+1 : i+3])
			if err != nil {
				return "", bad
			}
			out.WriteByte(b[0])
			i += 2
		default:
			return "", bad
		}
	}
	if !utf8.ValidString(out.String()) {
		return "", bad
	}
	return out.String(), nil
}
