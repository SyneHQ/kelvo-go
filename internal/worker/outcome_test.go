// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"encoding/json"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"strings"
	"testing"
	"time"
)

func TestBoundedOutcomeFitsSnapshotAndFederationStatistics(t *testing.T) {
	outcome := Outcome{Timing: childTimingTestFixture()}
	for i := 0; i < 64; i++ {
		outcome.Stats.Accelerations = append(outcome.Stats.Accelerations, query.AccelerationVersion{Dataset: strings.Repeat("a", 63), Generation: strings.Repeat("f", 64), RefreshedAt: time.Now()})
	}
	for i := 0; i < 32; i++ {
		outcome.Stats.Federation = append(outcome.Stats.Federation, query.FederationScan{Source: strings.Repeat("s", 63), Table: strings.Repeat("t", 63), Rows: 100000000, Bytes: 1 << 40, Scans: 1024, Batches: 100000000})
	}
	encoded, err := json.Marshal(outcome)
	if err != nil {
		t.Fatal(err)
	}
	var buffer boundedBuffer
	if _, err = buffer.Write(encoded); err != nil {
		t.Fatal(err)
	}
	var decoded Outcome
	if json.Unmarshal(buffer.Bytes(), &decoded) != nil || len(decoded.Stats.Accelerations) != 64 || len(decoded.Stats.Federation) != 32 {
		t.Fatal("valid outcome was truncated")
	}
	_, _ = buffer.Write(make([]byte, maxOutcomeBytes))
	if buffer.Len() != maxOutcomeBytes {
		t.Fatal("outcome buffer is not bounded")
	}
}
