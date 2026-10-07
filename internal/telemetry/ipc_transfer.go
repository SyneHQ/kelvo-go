// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package telemetry

import "time"

// IPCTransfer describes one parent's read of untrusted child Arrow output.
// Duration minus SinkDuration includes pipe wait, framing validation, decoding,
// accounting and release; it is not pure CPU time. SinkDuration includes
// synchronous Schema/Write work, which may encode, compress or block downstream.
// Both overlap child timing and the existing execution_delivery/sink metrics.
//
// InputBytes counts actual child pipe bytes read, including framing and a failed
// trailing-byte probe. DecodedBytes and Batches count validated batches offered
// to the sink, including a rejected last callback. None measures network traffic
// or client receipt. Complete requires valid EOS/EOF and successful callbacks;
// the caller must still verify process outcome and commit result success.
//
// The fixed-size observation stores no samples, schema, labels or request data.
// Unreached sink work remains unknown instead of becoming a zero duration.
type IPCTransfer struct {
	Observed     bool
	Duration     time.Duration
	SinkDuration time.Duration
	HasSink      bool
	InputBytes   int64
	DecodedBytes int64
	Batches      int64
	Complete     bool
}

func (p IPCTransfer) Valid() bool {
	if !p.Observed || p.Duration < 0 || p.Duration > 24*time.Hour || p.SinkDuration < 0 || p.SinkDuration > p.Duration {
		return false
	}
	if p.InputBytes < 0 || p.DecodedBytes < 0 || p.Batches < 0 {
		return false
	}
	return p.HasSink || (p.SinkDuration == 0 && p.DecodedBytes == 0 && p.Batches == 0 && !p.Complete)
}

type IPCTransferStatus uint8

const (
	IPCTransferComplete IPCTransferStatus = iota
	IPCTransferIncomplete
	IPCTransferMalformed
	ipcTransferStatusCount
)

var ipcTransferStatusNames = [...]string{"complete", "incomplete", "malformed"}

type IPCTransferSnapshot struct {
	Calls        [2][ipcTransferStatusCount]uint64
	ReadValidate [2]Histogram
	Sink         [2]Histogram
	SinkUnknown  [2]uint64
	InputBytes   [2][IPCTransferMalformed]uint64
	DecodedBytes [2][IPCTransferMalformed]uint64
	Batches      [2][IPCTransferMalformed]uint64
}

// ObserveIPCTransfer records at most one aggregate per attempted IPC read. An
// absent observation adds no zero sample; invalid diagnostics cannot affect the
// query outcome or contribute byte/duration totals. Nil disables collection.
func (r *Registry) ObserveIPCTransfer(kind Kind, transfer *IPCTransfer) {
	if r == nil || kind >= kindCount || transfer == nil || !transfer.Observed {
		return
	}
	status := IPCTransferIncomplete
	if !transfer.Valid() {
		status = IPCTransferMalformed
	} else if transfer.Complete {
		status = IPCTransferComplete
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.state.IPCTransfer
	s.Calls[kind][status]++
	if status == IPCTransferMalformed {
		return
	}
	observe(&s.ReadValidate[kind], transfer.Duration-transfer.SinkDuration)
	if transfer.HasSink {
		observe(&s.Sink[kind], transfer.SinkDuration)
	} else {
		s.SinkUnknown[kind]++
	}
	s.InputBytes[kind][status] += uint64(transfer.InputBytes)
	s.DecodedBytes[kind][status] += uint64(transfer.DecodedBytes)
	s.Batches[kind][status] += uint64(transfer.Batches)
}

func writeIPCTransferMetrics(out *metricBuffer, snapshot Snapshot) {
	s := snapshot.IPCTransfer
	const calls = "kelvo_worker_ipc_reads_total"
	out.printf("# HELP %s Parent IPC read attempts; complete validates input EOS/EOF and sink callbacks, not process outcome or client completion.\n# TYPE %s counter\n", calls, calls)
	for kind, kindName := range kindNames {
		for status, statusName := range ipcTransferStatusNames {
			out.printf("%s{kind=%q,status=%q} %d\n", calls, kindName, statusName, s.Calls[kind][status])
		}
	}
	writeHistogram(out, "kelvo_worker_ipc_read_validate_wait_seconds", "Parent IPC duration excluding synchronous sinks; includes pipe wait, validation and decode, overlaps child execution and worker execution_delivery.", s.ReadValidate)
	writeHistogram(out, "kelvo_worker_ipc_sink_seconds", "Synchronous IPC schema/batch callbacks including encoding and downstream waits; overlaps worker sink metrics, excludes result finalization.", s.Sink)
	const unknown = "kelvo_worker_ipc_sink_unknown_total"
	out.printf("# HELP %s IPC reads that never reached a sink callback; no zero duration sample is recorded.\n# TYPE %s counter\n", unknown, unknown)
	for kind, kindName := range kindNames {
		out.printf("%s{kind=%q} %d\n", unknown, kindName, s.SinkUnknown[kind])
	}
	writeIPCTransferCounters(out, "kelvo_worker_ipc_input_bytes_total", "Child pipe bytes read including Arrow framing and rejected trailing probes; not source, network or client bytes.", s.InputBytes)
	writeIPCTransferCounters(out, "kelvo_worker_ipc_decoded_bytes_total", "Validated Arrow buffer bytes offered to sinks, including failed callbacks; not successfully delivered bytes.", s.DecodedBytes)
	writeIPCTransferCounters(out, "kelvo_worker_ipc_batches_total", "Validated Arrow batches offered to sinks, including failed callbacks; complete refers only to the IPC read.", s.Batches)
}

func writeIPCTransferCounters(out *metricBuffer, name, help string, counts [2][IPCTransferMalformed]uint64) {
	out.printf("# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	for kind, kindName := range kindNames {
		for status, count := range counts[kind] {
			out.printf("%s{kind=%q,status=%q} %d\n", name, kindName, ipcTransferStatusNames[status], count)
		}
	}
}
