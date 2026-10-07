// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import (
	"bytes"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIPCTransferSeparatesDisjointParentWorkAndBoundaries(t *testing.T) {
	r := New()
	complete := IPCTransfer{Observed: true, Duration: 5 * time.Second, SinkDuration: 2 * time.Second, HasSink: true, InputBytes: 160, DecodedBytes: 128, Batches: 2, Complete: true}
	r.ObserveIPCTransfer(KindQuery, &complete)
	incomplete := complete
	incomplete.Complete = false
	r.ObserveIPCTransfer(KindQuery, &incomplete)
	s := r.Snapshot().IPCTransfer
	if s.ReadValidate[KindQuery].Count != 2 || s.ReadValidate[KindQuery].SumSeconds != 6 || s.Sink[KindQuery].Count != 2 || s.Sink[KindQuery].SumSeconds != 4 {
		t.Fatalf("parent read/wait and sink durations overlap: %+v", s)
	}
	for _, status := range []IPCTransferStatus{IPCTransferComplete, IPCTransferIncomplete} {
		if s.Calls[KindQuery][status] != 1 || s.InputBytes[KindQuery][status] != 160 || s.DecodedBytes[KindQuery][status] != 128 || s.Batches[KindQuery][status] != 2 {
			t.Fatalf("byte boundaries or completion status conflated: %+v", s)
		}
	}
	if r.Snapshot().Outcomes != ([2][3]uint64{}) {
		t.Fatal("IPC diagnostics fabricated terminal query outcomes")
	}
	var output bytes.Buffer
	if err := writeMetrics(&output, r.Snapshot()); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`kelvo_worker_ipc_reads_total{kind="query",status="complete"} 1`,
		`kelvo_worker_ipc_read_validate_wait_seconds_sum{kind="query"} 6`,
		`kelvo_worker_ipc_sink_seconds_sum{kind="query"} 4`,
		`kelvo_worker_ipc_input_bytes_total{kind="query",status="incomplete"} 160`,
		`kelvo_worker_ipc_decoded_bytes_total{kind="query",status="complete"} 128`,
	} {
		if !strings.Contains(output.String(), want+"\n") {
			t.Errorf("missing metric %q", want)
		}
	}
}

func TestIPCTransferUnknownIsNotZero(t *testing.T) {
	r := New()
	r.ObserveIPCTransfer(KindQuery, nil)
	r.ObserveIPCTransfer(KindQuery, &IPCTransfer{})
	if r.Snapshot() != (Snapshot{}) {
		t.Fatal("absent read became a zero observation")
	}
	r.ObserveIPCTransfer(KindRefresh, &IPCTransfer{Observed: true, InputBytes: 2, Duration: time.Second})
	s := r.Snapshot().IPCTransfer
	if s.Calls[KindRefresh][IPCTransferIncomplete] != 1 || s.ReadValidate[KindRefresh].Count != 1 || s.Sink[KindRefresh].Count != 0 || s.SinkUnknown[KindRefresh] != 1 {
		t.Fatalf("unreached sink became a zero duration: %+v", s)
	}
	r.ObserveIPCTransfer(KindRefresh, &IPCTransfer{Observed: true, HasSink: true, Complete: true})
	s = r.Snapshot().IPCTransfer
	if s.Sink[KindRefresh].Count != 1 || s.Sink[KindRefresh].SumSeconds != 0 || s.SinkUnknown[KindRefresh] != 1 {
		t.Fatal("observed zero sink was treated as unknown")
	}
}

func TestIPCTransferRejectsMalformedDiagnostics(t *testing.T) {
	valid := IPCTransfer{Observed: true, Duration: time.Second, SinkDuration: time.Millisecond, HasSink: true, InputBytes: 160, DecodedBytes: 128, Batches: 2, Complete: true}
	for name, mutate := range map[string]func(*IPCTransfer){
		"negative duration":    func(p *IPCTransfer) { p.Duration = -1 },
		"unbounded duration":   func(p *IPCTransfer) { p.Duration = 25 * time.Hour },
		"negative sink":        func(p *IPCTransfer) { p.SinkDuration = -1 },
		"sink exceeds total":   func(p *IPCTransfer) { p.SinkDuration = math.MaxInt64 },
		"negative input":       func(p *IPCTransfer) { p.InputBytes = -1 },
		"negative decoded":     func(p *IPCTransfer) { p.DecodedBytes = -1 },
		"negative batches":     func(p *IPCTransfer) { p.Batches = -1 },
		"no observed callback": func(p *IPCTransfer) { p.HasSink = false },
	} {
		t.Run(name, func(t *testing.T) {
			observation := valid
			mutate(&observation)
			r := New()
			r.ObserveIPCTransfer(KindQuery, &observation)
			s := r.Snapshot().IPCTransfer
			if observation.Valid() || s.Calls[KindQuery][IPCTransferMalformed] != 1 || s.ReadValidate[KindQuery].Count != 0 || s.Sink[KindQuery].Count != 0 || s.InputBytes[KindQuery] != ([IPCTransferMalformed]uint64{}) || s.DecodedBytes[KindQuery] != ([IPCTransferMalformed]uint64{}) {
				t.Fatalf("malformed instrumentation entered totals: %+v", s)
			}
		})
	}
	var disabled *Registry
	disabled.ObserveIPCTransfer(KindQuery, &valid)
	r := New()
	r.ObserveIPCTransfer(Kind(255), &valid)
	if r.Snapshot() != (Snapshot{}) {
		t.Fatal("unknown kind was converted into a metric label")
	}
}

func TestConcurrentIPCTransferObservationAndSnapshot(t *testing.T) {
	r := New()
	observation := IPCTransfer{Observed: true, Duration: time.Second, SinkDuration: time.Millisecond, HasSink: true, InputBytes: 160, DecodedBytes: 128, Batches: 2, Complete: true}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Go(func() {
			for j := 0; j < 100; j++ {
				r.ObserveIPCTransfer(KindQuery, &observation)
				_ = r.Snapshot()
			}
		})
	}
	group.Wait()
	s := r.Snapshot().IPCTransfer
	if s.Calls[KindQuery][IPCTransferComplete] != 800 || s.InputBytes[KindQuery][IPCTransferComplete] != 128000 || s.DecodedBytes[KindQuery][IPCTransferComplete] != 102400 || s.Batches[KindQuery][IPCTransferComplete] != 1600 {
		t.Fatalf("concurrent recording lost counters: %+v", s)
	}
}

func BenchmarkObserveIPCTransfer(b *testing.B) {
	observation := IPCTransfer{Observed: true, Duration: time.Second, SinkDuration: time.Millisecond, HasSink: true, InputBytes: 1 << 20, DecodedBytes: 1 << 20, Batches: 32, Complete: true}
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		var registry *Registry
		if enabled {
			name, registry = "enabled", New()
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					registry.ObserveIPCTransfer(KindQuery, &observation)
				}
			})
		})
	}
}
