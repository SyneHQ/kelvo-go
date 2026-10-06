// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/watch"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type watchSession struct {
	read, install, ack, remove, close int
	err                               error
	scope                             watch.Scope
}

func (s *watchSession) Close() error { s.close++; return nil }
func (s *watchSession) InstallWatch(_ context.Context, scope watch.Scope) error {
	s.scope = scope
	s.install++
	return s.err
}
func (s *watchSession) RemoveWatch(_ context.Context, scope watch.Scope) error {
	s.scope = scope
	s.remove++
	return s.err
}
func (s *watchSession) AckWatch(_ context.Context, scope watch.Scope, _ watch.Checkpoint, _ string) error {
	s.scope = scope
	s.ack++
	return s.err
}
func (s *watchSession) ReadWatch(_ context.Context, scope watch.Scope, _, _ int, _ int64) (watch.Batch, error) {
	s.scope = scope
	s.read++
	if s.err != nil {
		return watch.Batch{}, s.err
	}
	return watch.NewBatch(scope, nil)
}

func watchRequest(t *testing.T, kind operations.Kind) adapter.ProcessRequest {
	t.Helper()
	r := runtimeRequest(t, operations.ConnectionTest)
	r.Request.Kind = kind
	r.AppTeam = "team"
	r.Source.Schema = "public"
	r.Request.Connection.Schema = "public"
	r.Request.Spec.Watch = &operations.WatchSpec{ID: "watcher", Generation: "generation", Mode: "native", Target: operations.ObjectRef{Name: "orders", Schema: "public"}}
	if kind.Mutating() {
		r.Request.IdempotencyKey = "watch-operation"
	} else {
		r.Request.Spec.Watch.MaxEvents = 100
		r.Request.Spec.Watch.MaxWaitMS = 1
	}
	var err error
	r.RequestSHA256, err = operations.Digest(r.Request)
	if err != nil || r.Validate() != nil {
		t.Fatal("invalid request", err)
	}
	return r
}
func runWatch(t *testing.T, r adapter.ProcessRequest, s *watchSession, out io.Writer) (operations.Receipt, error) {
	t.Helper()
	raw, _ := json.Marshal(r)
	var received bytes.Buffer
	runner := Runner{Open: func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error) {
		return s, nil
	}}
	err := runner.Run(context.Background(), bytes.NewReader(raw), out, &received)
	var receipt operations.Receipt
	if operations.DecodeStrict(received.Bytes(), &receipt, operations.MaxReceiptBytes) != nil || receipt.Validate() != nil {
		t.Fatal("bad receipt", err)
	}
	if s.close != 1 {
		t.Fatal("session leaked")
	}
	return receipt, err
}

func TestWatchReadReturnsBoundBatchWithoutInstallingOrAcknowledging(t *testing.T) {
	r := watchRequest(t, operations.WatchRead)
	s := &watchSession{}
	var output bytes.Buffer
	receipt, err := runWatch(t, r, s, &output)
	if err != nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectNone || s.read != 1 || s.install != 0 || s.ack != 0 || s.remove != 0 {
		t.Fatal("read had side effects", err)
	}
	reader, err := ipc.NewReader(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() || reader.Schema().Field(0).Name != "watch" {
		t.Fatal("missing watch batch")
	}
	if _, err := watch.ParseBatch(reader.RecordBatch().Column(0).(*array.Binary).Value(0), s.scope); err != nil {
		t.Fatal(err)
	}
	if reader.Next() || reader.Err() != nil {
		t.Fatal("trailing batch")
	}
}

func TestWatchMutationUnknownOutcomeAndNoReplay(t *testing.T) {
	for _, kind := range []operations.Kind{operations.WatchInstall, operations.WatchRemove} {
		s := &watchSession{err: watch.ErrOutcomeUnknown}
		var output bytes.Buffer
		receipt, err := runWatch(t, watchRequest(t, kind), s, &output)
		if err == nil || receipt.Outcome != operations.OutcomeUnknown || receipt.Effect != operations.EffectUnknown || s.install+s.remove != 1 || output.Len() != 0 {
			t.Fatal("unknown operation replayed or reported complete", err)
		}
	}
}
