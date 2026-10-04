// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

const maxChildTimingBytes = 2 << 10

// UnmarshalJSON isolates optional diagnostic failures from the core outcome.
// Malformed timing must never turn a valid query result into an error.
func (o *Outcome) UnmarshalJSON(data []byte) error {
	var wire struct {
		Stats  query.Stats         `json:"stats"`
		Error  *query.Error        `json:"error,omitempty"`
		Timing optionalChildTiming `json:"timing"`
	}
	err := json.Unmarshal(data, &wire)
	*o = Outcome{Stats: wire.Stats, Error: wire.Error, Timing: wire.Timing.report, timingMalformed: wire.Timing.malformed}
	return err
}

type optionalChildTiming struct {
	report    *telemetry.ChildTiming
	seen      bool
	malformed bool
}

func (o *optionalChildTiming) UnmarshalJSON(data []byte) error {
	if o.seen {
		o.report, o.malformed = nil, true
		return nil
	}
	o.seen = true
	o.report = decodeChildTiming(data)
	o.malformed = o.report == nil
	return nil
}

// EncodeOutcome preserves the core outcome under the stderr cap. Timing is
// optional and is dropped first if the otherwise-valid outcome would not fit.
func EncodeOutcome(w io.Writer, outcome Outcome) error {
	data, err := json.Marshal(outcome)
	if err != nil {
		return err
	}
	if len(data)+1 > maxOutcomeBytes && outcome.Timing != nil {
		outcome.Timing = nil
		data, err = json.Marshal(outcome)
		if err != nil {
			return err
		}
	}
	if len(data)+1 > maxOutcomeBytes {
		return errors.New("worker outcome exceeds bounded diagnostic channel")
	}
	data = append(data, '\n')
	n, err := w.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

// The fixed decoder rejects duplicates, missing keys, coercions and extra keys.
// It does not accept arbitrary diagnostic maps or retain data proportional to
// query size. Invalid reports return nil, independently of core JSON decoding.
func decodeChildTiming(data []byte) *telemetry.ChildTiming {
	if len(data) > maxChildTimingBytes {
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if !childDelimiter(d, '{') {
		return nil
	}
	var report telemetry.ChildTiming
	var seen uint8
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil
		}
		switch key {
		case "version":
			if seen&1 != 0 {
				return nil
			}
			seen |= 1
			version, ok := childInteger(d)
			if !ok || version != int64(telemetry.ChildTimingVersion) {
				return nil
			}
			report.Version = uint8(version)
		case "total_ns":
			if seen&2 != 0 {
				return nil
			}
			seen |= 2
			var ok bool
			report.TotalNS, ok = childInteger(d)
			if !ok {
				return nil
			}
		case "stages":
			if seen&4 != 0 || !childDelimiter(d, '[') {
				return nil
			}
			seen |= 4
			for i := range report.Stages {
				interval, ok := decodeChildInterval(d)
				if !ok {
					return nil
				}
				report.Stages[i] = interval
			}
			if !childDelimiter(d, ']') {
				return nil
			}
		default:
			return nil
		}
	}
	if seen != 7 || !childDelimiter(d, '}') || !report.Valid(time.Duration(report.TotalNS)) {
		return nil
	}
	if _, err := d.Token(); err != io.EOF {
		return nil
	}
	return &report
}

func decodeChildInterval(d *json.Decoder) (telemetry.ChildInterval, bool) {
	var interval telemetry.ChildInterval
	if !childDelimiter(d, '{') {
		return interval, false
	}
	var seen uint8
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return interval, false
		}
		switch key {
		case "start_ns":
			if seen&1 != 0 {
				return interval, false
			}
			seen |= 1
			var ok bool
			interval.StartNS, ok = childInteger(d)
			if !ok {
				return interval, false
			}
		case "end_ns":
			if seen&2 != 0 {
				return interval, false
			}
			seen |= 2
			var ok bool
			interval.EndNS, ok = childInteger(d)
			if !ok {
				return interval, false
			}
		case "observed":
			if seen&4 != 0 {
				return interval, false
			}
			seen |= 4
			value, err := d.Token()
			var ok bool
			interval.Observed, ok = value.(bool)
			if err != nil || !ok {
				return interval, false
			}
		default:
			return interval, false
		}
	}
	return interval, seen == 7 && childDelimiter(d, '}')
}

func childDelimiter(d *json.Decoder, want json.Delim) bool {
	token, err := d.Token()
	return err == nil && token == want
}

func childInteger(d *json.Decoder) (int64, bool) {
	token, err := d.Token()
	if err != nil {
		return 0, false
	}
	number, ok := token.(json.Number)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(string(number), 10, 64)
	return n, err == nil
}

func recordChildTiming(metrics *telemetry.Registry, ctx context.Context, outcome Outcome, decodeErr error, terminated bool, parentBound time.Duration) {
	if metrics == nil {
		return
	}
	status := telemetry.ChildTimingObserved
	switch {
	case decodeErr != nil || outcome.timingMalformed:
		// This counter covers an unusable diagnostic channel as well as a bad
		// optional report. Only decodeErr affects the core query error path.
		status = telemetry.ChildTimingMalformed
	case outcome.Timing == nil:
		status = telemetry.ChildTimingMissing
	case !outcome.Timing.Valid(parentBound):
		status = telemetry.ChildTimingMalformed
	}
	if status != telemetry.ChildTimingObserved && terminated {
		status = telemetry.ChildTimingTerminated
	}
	kind := telemetry.KindQuery
	if refresh, _ := ctx.Value(refreshTelemetryKey{}).(bool); refresh {
		kind = telemetry.KindRefresh
	}
	metrics.ObserveChildTiming(kind, outcome.Timing, status, parentBound)
}
