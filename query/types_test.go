// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package query_test

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/query"
	"go.yaml.in/yaml/v3"
)

func TestTypedParameterFidelity(t *testing.T) {
	r := query.Request{Parameters: []query.Parameter{
		{Type: "int64", Value: json.RawMessage(`"9223372036854775807"`)},
		{Type: "uint64", Value: json.RawMessage(`"18446744073709551615"`)},
		{Type: "null", Value: json.RawMessage("null")},
		{Type: "bool", Value: json.RawMessage("true")},
	}}
	v, e := r.Values()
	if e != nil {
		t.Fatal(e)
	}
	if v[0] != int64(math.MaxInt64) || v[1] != uint64(math.MaxUint64) || v[2] != nil || v[3] != true {
		t.Fatalf("lost parameter fidelity: %#v", v)
	}
}
func TestInvalidTypedValuesFail(t *testing.T) {
	for _, p := range []query.Parameter{
		{Type: "string", Value: json.RawMessage("null")},
		{Type: "bool", Value: json.RawMessage("null")},
		{Type: "float64", Value: json.RawMessage("null")},
		{Type: "int64", Value: json.RawMessage("9223372036854775808")},
		{Type: "uint64", Value: json.RawMessage("-1")},
		{Type: "null", Value: json.RawMessage("1")},
	} {
		if _, e := (query.Request{Parameters: []query.Parameter{p}}).Values(); e == nil {
			t.Fatalf("accepted invalid parameter %#v", p)
		}
	}
}

func TestResultCompressionLimitsValidateExactCodecs(t *testing.T) {
	for _, compression := range []string{"", "none", "lz4_frame"} {
		limits := query.DefaultLimits()
		limits.ResultCompression = compression
		if err := limits.Validate(); err != nil {
			t.Fatalf("compression %q: %v", compression, err)
		}
		if limits.ResultCompression != compression {
			t.Fatal("validation changed the configured codec")
		}
	}
	for _, compression := range []string{"lz4", "zstd", "gzip", "LZ4_FRAME", " lz4_frame", "lz4_frame ", "none\x00"} {
		limits := query.DefaultLimits()
		limits.ResultCompression = compression
		if err := limits.Validate(); err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" {
			t.Fatalf("accepted compression %q: %v", compression, err)
		}
		if _, err := query.ResultIPCOptions(compression); err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" {
			t.Fatalf("writer accepted compression %q: %v", compression, err)
		}
	}
	limits := query.DefaultLimits()
	limits.ResultCompression = "lz4_frame"
	limits.MaxBytes = 1023
	if err := limits.Validate(); err == nil {
		t.Fatal("compression relaxed the resource limits")
	}
}

func TestResultCompressionConfigurationRoundTripAndOmittedDefault(t *testing.T) {
	for name, encoding := range map[string]struct {
		marshal   func(any) ([]byte, error)
		unmarshal func([]byte, any) error
	}{
		"json": {json.Marshal, json.Unmarshal},
		"yaml": {yaml.Marshal, yaml.Unmarshal},
	} {
		t.Run(name, func(t *testing.T) {
			for _, compression := range []string{"", "none", "lz4_frame"} {
				limits := query.DefaultLimits()
				limits.ResultCompression = compression
				encoded, err := encoding.marshal(limits)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(encoded), "result_compression") != (compression != "") {
					t.Fatalf("default field omission changed: %s", encoded)
				}
				var decoded query.Limits
				if err := encoding.unmarshal(encoded, &decoded); err != nil || decoded != limits {
					t.Fatalf("limits did not round trip: %+v, %v", decoded, err)
				}
			}
		})
	}
}

func TestRowBatchTargetLimits(t *testing.T) {
	for _, target := range []int64{0, 1024, 1 << 20, 64 << 20} {
		limits := query.DefaultLimits()
		limits.RowBatchTargetBytes = target
		if err := limits.Validate(); err != nil {
			t.Fatalf("target %d: %v", target, err)
		}
	}
	for _, target := range []int64{-1, 1, 1023, (64 << 20) + 1, math.MaxInt64} {
		limits := query.DefaultLimits()
		limits.RowBatchTargetBytes = target
		if err := limits.Validate(); err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" {
			t.Fatalf("accepted invalid target %d: %v", target, err)
		}
	}
	limits := query.DefaultLimits()
	limits.RowBatchTargetBytes = 1 << 20
	limits.MaxBytes = 1023
	if err := limits.Validate(); err == nil {
		t.Fatal("batch target relaxed result limits")
	}
}

func TestRowBatchTargetConfigurationRoundTrip(t *testing.T) {
	for name, encoding := range map[string]struct {
		marshal   func(any) ([]byte, error)
		unmarshal func([]byte, any) error
	}{
		"json": {json.Marshal, json.Unmarshal},
		"yaml": {yaml.Marshal, yaml.Unmarshal},
	} {
		t.Run(name, func(t *testing.T) {
			for _, target := range []int64{0, 1024, 64 << 20} {
				limits := query.DefaultLimits()
				limits.RowBatchTargetBytes = target
				encoded, err := encoding.marshal(limits)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(encoded), "row_batch_target_bytes") != (target != 0) {
					t.Fatalf("default field omission changed: %s", encoded)
				}
				var decoded query.Limits
				if err := encoding.unmarshal(encoded, &decoded); err != nil || decoded != limits {
					t.Fatalf("limits did not round trip: %+v, %v", decoded, err)
				}
			}
		})
	}
}
