//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package audit

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// Use -benchtime=1000x so retention remains intentionally bounded. These are
// per-operation journal costs, not SQL throughput or native-memory measurements.
func BenchmarkAuditDurableReceipt(b *testing.B) {
	if b.N > 4096 {
		b.Skip("use -benchtime=1000x for bounded durable retention")
	}
	cfg := testConfig(b.TempDir())
	cfg.MaxEntries = 4096
	j, err := Open(cfg, testScope())
	if err != nil {
		b.Fatal(err)
	}
	defer j.Close(context.Background())
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
		if err != nil {
			b.Fatal(err)
		}
		if err = r.Finish(context.Background(), Succeeded, None); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// Use -benchtime=1x. Heap and RSS deltas are local journal costs only; the
// surrounding runtime, query engine, and filesystem cache need separate budgets.
func BenchmarkAuditOpenPreallocated(b *testing.B) {
	for _, entries := range []int{4096, 65536} {
		b.Run(fmt.Sprint(entries), func(b *testing.B) {
			if b.N != 1 {
				b.Skip("use -benchtime=1x for preallocated storage measurement")
			}
			cfg := testConfig(b.TempDir())
			cfg.MaxEntries = entries
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			rssBefore := auditRSS()
			b.ResetTimer()
			j, err := Open(cfg, testScope())
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
			defer j.Close(context.Background())
			runtime.GC()
			runtime.ReadMemStats(&after)
			rssAfter := auditRSS()
			var stat unix.Stat_t
			if err := unix.Fstat(int(j.file.Fd()), &stat); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(stat.Blocks*512), "disk-bytes")
			b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), "heap-bytes")
			if rssBefore >= 0 && rssAfter >= 0 {
				b.ReportMetric(float64(rssAfter-rssBefore), "rss-bytes")
			}
			runtime.KeepAlive(j)
		})
	}
}

func auditRSS() int64 {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) == 3 {
				value, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					return value * 1024
				}
			}
		}
	}
	return -1
}
