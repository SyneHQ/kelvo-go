// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package query

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

type Request struct {
	SQL          string        `json:"sql" yaml:"sql"`
	Mode         string        `json:"mode,omitempty" yaml:"mode,omitempty"`
	Sources      []string      `json:"sources,omitempty" yaml:"sources,omitempty"`
	ConnectionID string        `json:"connection_id,omitempty" yaml:"connection_id,omitempty"`
	Parameters   []Parameter   `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	Mongo        *MongoRequest `json:"mongo,omitempty" yaml:"mongo,omitempty"`
}

// MongoRequest carries native aggregation syntax. SQL and Mongo are mutually
// exclusive; collection access is bounded by the configured database grants.
type MongoRequest struct {
	Collection string            `json:"collection" yaml:"collection"`
	Pipeline   []json.RawMessage `json:"pipeline" yaml:"pipeline"`
}
type Parameter struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

func (r Request) Values() ([]any, error) {
	out := make([]any, len(r.Parameters))
	for i, p := range r.Parameters {
		var err error
		if p.Type != "null" && bytes.Equal(bytes.TrimSpace(p.Value), []byte("null")) {
			return nil, NewError("INVALID_ARGUMENT", fmt.Sprintf("Parameter %d requires an explicit null type", i+1))
		}
		switch p.Type {
		case "null":
			if len(p.Value) > 0 && string(p.Value) != "null" {
				err = errors.New("expected null")
			}
		case "string":
			var v string
			err = json.Unmarshal(p.Value, &v)
			out[i] = v
		case "bool":
			var v bool
			err = json.Unmarshal(p.Value, &v)
			out[i] = v
		case "int64", "uint64":
			var v string
			if json.Unmarshal(p.Value, &v) != nil {
				v = string(p.Value)
			}
			if p.Type == "int64" {
				out[i], err = strconv.ParseInt(v, 10, 64)
			} else {
				out[i], err = strconv.ParseUint(v, 10, 64)
			}
		case "float64":
			var v float64
			err = json.Unmarshal(p.Value, &v)
			if math.IsInf(v, 0) || math.IsNaN(v) {
				err = errors.New("nonfinite number")
			}
			out[i] = v
		default:
			err = errors.New("unsupported parameter type")
		}
		if err != nil {
			return nil, NewError("INVALID_ARGUMENT", fmt.Sprintf("Invalid parameter %d (%s)", i+1, p.Type))
		}
	}
	return out, nil
}

type Limits struct {
	MaxRows           int64         `json:"max_rows" yaml:"max_rows"`
	MaxBytes          int64         `json:"max_bytes" yaml:"max_bytes"`
	Timeout           time.Duration `json:"timeout" yaml:"timeout"`
	MemoryMB          int           `json:"memory_mb" yaml:"memory_mb"`
	Threads           int           `json:"threads" yaml:"threads"`
	MaxTempMB         int           `json:"max_temp_mb" yaml:"max_temp_mb"`
	ResultCompression string        `json:"result_compression,omitempty" yaml:"result_compression,omitempty"`
}

func DefaultLimits() Limits {
	return Limits{MaxRows: 1000000, MaxBytes: 256 << 20, Timeout: 30 * time.Second, MemoryMB: 256, Threads: 2, MaxTempMB: 1024}
}
func (l Limits) Validate() error {
	if l.MaxRows < 1 || l.MaxRows > 100000000 || l.MaxBytes < 1024 || l.MaxBytes > 1<<40 || l.Timeout <= 0 || l.Timeout > time.Hour || l.MemoryMB < 16 || l.MemoryMB > 1048576 || l.Threads < 1 || l.Threads > 1024 || l.MaxTempMB < 1 || l.MaxTempMB > 1048576 {
		return NewError("INVALID_ARGUMENT", "Invalid query resource limits")
	}
	return validateResultCompression(l.ResultCompression)
}

// FederationScan reports source batches actually fetched, not rows scanned by
// the remote database. Bytes measures logical Arrow buffers, excluding framing.
// SourceWireBytes counts supported adapters' encoded response body bytes read,
// including IPC framing but excluding schema discovery and HTTP/TLS headers.
type FederationScan struct {
	Source          string `json:"source"`
	Table           string `json:"table"`
	Scans           int64  `json:"scans"`
	Rows            int64  `json:"rows_fetched"`
	Bytes           int64  `json:"arrow_bytes_fetched"`
	Batches         int64  `json:"batches_fetched"`
	SourceWireBytes int64  `json:"source_wire_bytes,omitempty"`
}

// Stats keeps encoded source response bytes separate from worker/client output.
// Only adapters with source body accounting contribute to SourceWireBytes.
type Stats struct {
	Federation      []FederationScan      `json:"federation,omitempty"`
	Rows            int64                 `json:"rows"`
	Batches         int64                 `json:"batches"`
	Bytes           int64                 `json:"arrow_bytes"`
	WireBytes       int64                 `json:"wire_bytes"`
	SourceWireBytes int64                 `json:"source_wire_bytes,omitempty"`
	Backend         string                `json:"backend"`
	PrepareNS       int64                 `json:"prepare_ns"`
	DurationNS      int64                 `json:"duration_ns"`
	EngineStreaming bool                  `json:"engine_streaming"`
	Accelerations   []AccelerationVersion `json:"accelerations,omitempty"`
}

type AccelerationVersion struct {
	Dataset     string    `json:"dataset" yaml:"dataset"`
	Generation  string    `json:"generation" yaml:"generation"`
	RefreshedAt time.Time `json:"refreshed_at" yaml:"refreshed_at"`
}

// Sink methods are synchronous. The record is borrowed only for Write's duration.
type Sink interface {
	Schema(*arrow.Schema) error
	Write(arrow.RecordBatch) error
}
type Executor interface {
	Execute(context.Context, Request, Sink) (Stats, error)
}
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string            { return e.Message }
func NewError(code, message string) error { return &Error{Code: code, Message: message} }
func PublicError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Code: "DEADLINE_EXCEEDED", Message: "Query deadline exceeded"}
	}
	if errors.Is(err, context.Canceled) {
		return &Error{Code: "CANCELLED", Message: "Query cancelled"}
	}
	return &Error{Code: "QUERY_FAILED", Message: "Query failed"}
}
