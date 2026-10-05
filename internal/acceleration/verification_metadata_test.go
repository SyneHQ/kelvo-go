// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

// Embed only Client even when the underlying fixture has extra capabilities.
type verificationMetadataOnly struct{ objectstore.Client }

func TestVerificationMetadataSharesDataBudgetWithoutRangeOrWriteCapability(t *testing.T) {
	data := &verificationTestClient{rangeGet: func(context.Context, string, string, int64, int64) (io.ReadCloser, objectstore.Info, error) {
		return io.NopCloser(strings.NewReader("abc")), objectstore.Info{}, nil
	}}
	metadata := verificationBodyClient(io.NopCloser(strings.NewReader("root")), nil)
	meter := newVerificationReader(data, 9)
	view := meter.metadataClient(verificationMetadataOnly{metadata})
	if _, ok := view.(objectstore.RangeClient); ok {
		t.Fatal("metadata-only identity acquired range capability")
	}
	body, _, err := view.Get(context.Background(), "root", "version")
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := io.ReadAll(body); err != nil || string(raw) != "root" {
		t.Fatal("metadata read failed", err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	body, _, err = meter.GetRange(context.Background(), "data", "version", 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := io.ReadAll(body); err != nil || string(raw) != "abc" {
		t.Fatal("data read failed", err)
	}
	if err := body.Close(); err != nil || meter.Remaining() != 1 {
		t.Fatal("root and range bytes did not share accounting", err, meter.Remaining())
	}
	if _, _, err := meter.GetRange(context.Background(), "data", "version", 0, 1); !errors.Is(err, errVerificationLimit) || data.ranges.Load() != 1 {
		t.Fatal("data range ignored metadata consumption", err)
	}
	view.Close()
	meter.Close()
	if metadata.closes.Load() != 0 || data.closes.Load() != 0 {
		t.Fatal("borrowed view closed a provider")
	}
	writeMeter := newVerificationReader(data, 8)
	if _, err := writeMeter.metadataClient(metadata).Put(context.Background(), "root", strings.NewReader("x"), 1, "digest", objectstore.Condition{}); !errors.Is(err, errReaderInvalid) || metadata.puts.Load() != 0 {
		t.Fatal("metadata wrapper forwarded a write", err)
	}
}

func TestVerificationMetadataPreservesPartialBodyAndCloseFailureLedger(t *testing.T) {
	providerErr := errors.New("fixture partial metadata failure")
	body := &verificationTestBody{close: func() error { return errors.New("fixture close failure") }}
	client := verificationBodyClient(body, providerErr)
	meter := newVerificationReader(&verificationTestClient{}, 32)
	view := meter.metadataClient(verificationMetadataOnly{client})
	partial, _, err := view.Get(context.Background(), "root", "version")
	if partial == nil || !errors.Is(err, providerErr) {
		t.Fatal("partial metadata body lost ownership", err)
	}
	if err := partial.Close(); !errors.Is(err, errReaderCleanupUnknown) || body.closes.Load() != 1 {
		t.Fatal("partial metadata close failure disappeared", err)
	}
	if !errors.Is(meter.Err(), providerErr) || !errors.Is(meter.Err(), errReaderCleanupUnknown) {
		t.Fatal("metadata failures did not reach the data ledger", meter.Err())
	}
	if _, _, err := meter.GetRange(context.Background(), "data", "version", 0, 1); err == nil {
		t.Fatal("data access survived sticky metadata failure")
	}
}
