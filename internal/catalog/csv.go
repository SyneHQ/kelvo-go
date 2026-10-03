// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"strconv"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

const (
	MinCSVBufferBytes = 256 << 10
	MaxCSVBufferBytes = 256 << 20
	MaxCSVLineBytes   = 64 << 20
)

// CSVReadOptions controls only trusted native file-source setup. Both values
// are explicit because DuckDB otherwise couples buffer and line-size defaults.
// LineBytes is a parser setting, not a strict byte-level security boundary.
type CSVReadOptions struct {
	BufferBytes int64
	LineBytes   int64
}

// CSVOptions never forwards arbitrary SQL options, silently skips malformed
// rows, or changes omitted defaults. External adapters own their own options.
func (s Source) CSVOptions() (*CSVReadOptions, error) {
	if CanonicalType(s.Type) != "csv" || s.Adapter != "" || s.Federation != nil {
		return nil, nil
	}
	if len(s.Options) == 0 {
		return nil, nil
	}
	fail := func() (*CSVReadOptions, error) {
		return nil, query.NewError("CONFIGURATION_ERROR", "CSV options require paired bounded decimal buffer_size and maximum_line_size values")
	}
	if len(s.Options) != 2 {
		return fail()
	}
	bufferText, bufferOK := s.Options["buffer_size"]
	lineText, lineOK := s.Options["maximum_line_size"]
	if !bufferOK || !lineOK {
		return fail()
	}
	buffer, bufferErr := strconv.ParseInt(bufferText, 10, 64)
	line, lineErr := strconv.ParseInt(lineText, 10, 64)
	if bufferErr != nil || lineErr != nil || strconv.FormatInt(buffer, 10) != bufferText || strconv.FormatInt(line, 10) != lineText ||
		buffer < MinCSVBufferBytes || buffer > MaxCSVBufferBytes || line < 1 || line > MaxCSVLineBytes || buffer/4 < line {
		return fail()
	}
	return &CSVReadOptions{BufferBytes: buffer, LineBytes: line}, nil
}
