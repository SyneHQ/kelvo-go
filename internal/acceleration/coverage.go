// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// CoverageWriter is implemented by every refresh writer. The value is stored in
// the snapshot metadata as covered_until; empty stores nothing, so datasets
// without a route keep byte-identical manifests.
type CoverageWriter interface{ SetCoveredUntil(string) }

func (tx *Transaction) SetCoveredUntil(v string) {
	tx.mu.Lock()
	tx.coveredUntil = v
	tx.mu.Unlock()
}
func (t *multipartTransaction) SetCoveredUntil(v string) { t.tx.SetCoveredUntil(v) }

// coverage tracks MAX(column) over the rows a refresh writes. Value renders as a
// decimal integer or an RFC3339Nano UTC time, "" when nothing exact is known
// (no rows, only NULLs, an unsupported type or a missing column).
type coverage struct {
	column  string
	isTime  bool
	seen    bool
	bad     bool
	maxInt  int64
	maxTime time.Time
}

func (c *coverage) observe(record arrow.RecordBatch) {
	if c == nil || c.bad {
		return
	}
	index := -1
	for i, field := range record.Schema().Fields() {
		if strings.EqualFold(field.Name, c.column) {
			index = i
		}
	}
	if index < 0 {
		c.bad = true
		return
	}
	column := record.Column(index)
	for row := 0; row < column.Len() && !c.bad; row++ {
		if column.IsNull(row) {
			continue
		}
		switch a := column.(type) {
		case *array.Int8:
			c.int(int64(a.Value(row)))
		case *array.Int16:
			c.int(int64(a.Value(row)))
		case *array.Int32:
			c.int(int64(a.Value(row)))
		case *array.Int64:
			c.int(a.Value(row))
		case *array.Uint8:
			c.int(int64(a.Value(row)))
		case *array.Uint16:
			c.int(int64(a.Value(row)))
		case *array.Uint32:
			c.int(int64(a.Value(row)))
		case *array.Uint64:
			if a.Value(row) > math.MaxInt64 {
				c.bad = true
			} else {
				c.int(int64(a.Value(row)))
			}
		case *array.Timestamp:
			c.time(a.Value(row).ToTime(a.DataType().(*arrow.TimestampType).Unit))
		case *array.Date32:
			c.time(a.Value(row).ToTime())
		case *array.Date64:
			c.time(a.Value(row).ToTime())
		default:
			c.bad = true
		}
	}
}

func (c *coverage) int(v int64) {
	c.isTime = false
	if !c.seen || v > c.maxInt {
		c.maxInt = v
	}
	c.seen = true
}

func (c *coverage) time(v time.Time) {
	c.isTime = true
	if !c.seen || v.After(c.maxTime) {
		c.maxTime = v.UTC()
	}
	c.seen = true
}

func (c *coverage) Value() string {
	switch {
	case c == nil || c.bad || !c.seen:
		return ""
	case c.isTime:
		return c.maxTime.Format(time.RFC3339Nano)
	}
	return strconv.FormatInt(c.maxInt, 10)
}
