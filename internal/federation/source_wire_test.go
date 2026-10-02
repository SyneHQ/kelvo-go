// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"errors"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type sourceWireFixture struct {
	stats  query.Stats
	err    error
	seen   query.Sink
	closed bool
}

func (e *sourceWireFixture) Execute(_ context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	e.seen = sink
	return e.stats, e.err
}
func (e *sourceWireFixture) Close() error { e.closed = true; return nil }

func TestSourceWireAdapterPreservesSinkAndFailureCounts(t *testing.T) {
	failure := errors.New("source read failed")
	for _, test := range []struct {
		source, legacy, want int64
		err                  error
	}{
		{0, 123, 123, nil}, {0, 123, 123, failure}, {456, 123, 456, failure}, {0, -1, 0, nil},
	} {
		inner := &sourceWireFixture{stats: query.Stats{Rows: 2, Batches: 1, Bytes: 16, WireBytes: test.legacy, SourceWireBytes: test.source}, err: test.err}
		engine, err := withSourceWire(inner, nil)
		if err != nil {
			t.Fatal(err)
		}
		sink := &describeSink{}
		stats, gotErr := engine.Execute(context.Background(), query.Request{}, sink)
		if gotErr != test.err || inner.seen != sink || stats.SourceWireBytes != test.want || stats.WireBytes != test.legacy || stats.Rows != 2 || stats.Bytes != 16 || stats.Batches != 1 {
			t.Fatalf("wrapper changed native delivery or statistics: %+v %v", stats, gotErr)
		}
		_ = engine.Close()
		if !inner.closed {
			t.Fatal("wrapper did not close its executor")
		}
	}
}

func TestSourceWireAdapterPreservesConstructorError(t *testing.T) {
	failure := errors.New("constructor failed")
	inner := &sourceWireFixture{}
	if engine, err := withSourceWire(inner, failure); engine != nil || err != failure || inner.closed {
		t.Fatal("constructor error was lost or unsuccessful ownership was assumed")
	}
	if engine, err := withSourceWire(nil, nil); engine != nil || err == nil {
		t.Fatal("nil executor was accepted")
	}
}
