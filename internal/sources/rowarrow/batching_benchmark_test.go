// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rowarrow

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// BenchmarkWriterBatching isolates row conversion and optional uncompressed IPC
// encoding. Each operation handles 65,536 already-decoded, repeated input rows;
// the byte rate is nominal input payload, not network or database throughput.
func BenchmarkWriterBatching(b *testing.B) {
	const rows = 65536
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "payload", Type: arrow.BinaryTypes.String},
	}, nil)
	for _, width := range []int{32, 256, 1024} {
		for _, batch := range []struct {
			name   string
			target int64
		}{{"legacy1024", 0}, {"target1MiB", 1 << 20}, {"target4MiB", 4 << 20}} {
			for _, encode := range []bool{false, true} {
				mode := "convert"
				if encode {
					mode = "convertIPC"
				}
				b.Run(fmt.Sprintf("row%d/%s/%s", width, batch.name, mode), func(b *testing.B) {
					l := limits()
					l.MaxRows, l.MaxBytes, l.MemoryMB, l.RowBatchTargetBytes = rows, 128<<20, 64, batch.target
					row := []any{int64(9007199254740993), strings.Repeat("x", width-8)}
					b.SetBytes(int64(rows * width))
					b.ReportAllocs()
					for range b.N {
						sink := &benchmarkBatchSink{encode: encode}
						w, err := NewWriter(schema, l, sink)
						if err != nil {
							b.Fatal(err)
						}
						for range rows {
							if err := w.Write(row); err != nil {
								w.Close()
								b.Fatal(err)
							}
						}
						stats, err := w.Finish()
						w.Close()
						if err != nil {
							b.Fatal(err)
						}
						if sink.writer != nil {
							if err := sink.writer.Close(); err != nil {
								b.Fatal(err)
							}
						}
						b.ReportMetric(float64(stats.Batches), "batches/op")
						if encode {
							b.ReportMetric(float64(sink.output.bytes), "ipc-bytes/op")
						}
					}
				})
			}
		}
	}
}

type benchmarkBatchSink struct {
	encode bool
	writer *ipc.Writer
	output benchmarkByteCounter
}

func (s *benchmarkBatchSink) Schema(schema *arrow.Schema) error {
	if s.encode {
		s.writer = ipc.NewWriter(&s.output, ipc.WithSchema(schema))
	}
	return nil
}

func (s *benchmarkBatchSink) Write(record arrow.RecordBatch) error {
	if s.writer != nil {
		return s.writer.Write(record)
	}
	return nil
}

type benchmarkByteCounter struct{ bytes int64 }

func (w *benchmarkByteCounter) Write(p []byte) (int, error) {
	w.bytes += int64(len(p))
	return len(p), nil
}
