// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
)

func fixtureTrace() tracing.Carrier {
	return tracing.Carrier{Version: 1, TraceID: strings.Repeat("a", 32), SpanID: strings.Repeat("b", 16), Sampled: true}
}

func TestClusterTraceDurableAdmissionAndImmutableCAS(t *testing.T) {
	for _, mutation := range []string{"replace", "remove", "aliased pointer"} {
		t.Run(mutation, func(t *testing.T) {
			policy := principalTestPolicy()
			kv := &principalKV{}
			store := &NATSStore{policy: policy, kv: kv}
			request := query.Request{Mode: "native", ConnectionID: "sales_native", SQL: "SELECT 1"}
			want := fixtureTrace()
			ctx := tracing.WithCarrier(context.Background(), want)
			if _, err := store.Submit(ctx, request); err == nil || len(kv.raw) != 0 {
				t.Fatal("trace bypassed principal authorization")
			}
			authority, _ := authorityForPrincipal(policy, "analyst")
			ctx = context.WithValue(ctx, jobAuthorityKey{}, authority)
			previous, err := store.Submit(ctx, request)
			if err != nil || jobTrace(previous.Job.Trace) != want {
				t.Fatal("authorized carrier not persisted", err)
			}
			next := previous.Job
			next.State = Failed
			switch mutation {
			case "replace":
				other := want
				other.TraceID = strings.Repeat("c", 32)
				next.Trace = &other
			case "remove":
				next.Trace = nil
			case "aliased pointer":
				next.Trace.TraceID = strings.Repeat("c", 32)
			}
			updated, err := store.CompareAndSwap(ctx, previous, next)
			if err != nil || jobTrace(updated.Job.Trace) != want {
				t.Fatal("CAS replaced immutable durable trace", err)
			}
			updated.Job.Trace.TraceID = strings.Repeat("d", 32)
			durable, err := store.Get(ctx, previous.Job.ID)
			if err != nil || jobTrace(durable.Job.Trace) != want {
				t.Fatal("returned trace aliases durable context", err)
			}
		})
	}
}

func TestClusterTraceLegacyAndMalformedState(t *testing.T) {
	for name, value := range map[string]string{
		"legacy": "", "null": "null", "string": `"private-data"`, "array": `[]`,
		"empty": `{}`, "unknown version": `{"version":2,"trace_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","span_id":"bbbbbbbbbbbbbbbb","sampled":true}`,
		"extra data": `{"version":1,"trace_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","span_id":"bbbbbbbbbbbbbbbb","sampled":true,"sql":"secret"}`,
		"oversized":  `{"private":"` + strings.Repeat("x", 1024) + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := &NATSStore{policy: testPolicy(), kv: &principalKV{}}
			previous, err := store.Submit(context.Background(), query.Request{Mode: "federated", SQL: "SELECT 1"})
			if err != nil {
				t.Fatal(err)
			}
			kv := store.kv.(*principalKV)
			if value != "" {
				kv.raw = []byte(`{"trace":` + value + `,` + string(kv.raw[1:]))
			}
			previous, err = store.Get(context.Background(), previous.Job.ID)
			if err != nil || previous.Job.Trace != nil {
				t.Fatal("optional telemetry broke legacy job", err)
			}
			next := previous.Job
			next.State = Failed
			next.Trace = copyJobTraceValue(fixtureTrace())
			updated, err := store.CompareAndSwap(context.Background(), previous, next)
			if err != nil || updated.Job.Trace != nil || strings.Contains(string(kv.raw), `"trace"`) {
				t.Fatal("CAS added context to legacy job", err)
			}
		})
	}
}

func TestClusterTraceSizeBudgetPreservesTerminalState(t *testing.T) {
	store := &NATSStore{policy: testPolicy(), kv: &principalKV{}}
	previous, err := store.Submit(tracing.WithCarrier(context.Background(), fixtureTrace()), query.Request{Mode: "federated", SQL: "SELECT 1"})
	if err != nil {
		t.Fatal(err)
	}
	next := previous.Job
	next.State = Failed
	next.Error = &query.Error{Code: "QUERY_FAILED", Message: ""}
	withoutTrace := next
	withoutTrace.Trace = nil
	raw, err := json.Marshal(withoutTrace)
	if err != nil {
		t.Fatal(err)
	}
	// Leave room for wall-clock fractional precision changes in HeartbeatAt.
	next.Error.Message = strings.Repeat("x", jobValueLimit-len(raw)-32)
	updated, err := store.CompareAndSwap(context.Background(), previous, next)
	if err != nil || updated.Job.Trace != nil || updated.Job.State != Failed {
		t.Fatal("optional trace prevented terminal update", err)
	}
	durable, err := store.Get(context.Background(), previous.Job.ID)
	if err != nil || durable.Job.Trace != nil || durable.Job.Error.Message != next.Error.Message {
		t.Fatal("returned state does not match durable fallback", err)
	}
	next.Error.Message = strings.Repeat("x", jobValueLimit)
	if _, err := encodeJob(next); err == nil {
		t.Fatal("core oversized job accepted")
	}
}

func TestClusterTraceSizeBudgetPreservesSubmission(t *testing.T) {
	// HTML escaping expands this valid parameter enough to approach the KV bound.
	request := query.Request{Mode: "federated", SQL: "SELECT ?", Parameters: []query.Parameter{{Type: "string", Value: json.RawMessage(`""`)}}}
	probe := &NATSStore{policy: testPolicy(), kv: &principalKV{}}
	base, err := probe.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(base.Job)
	if err != nil {
		t.Fatal(err)
	}
	count := (jobValueLimit - len(raw) - 64) / 6
	request.Parameters[0].Value = json.RawMessage(fmt.Sprintf(`"%s"`, strings.Repeat("&", count)))
	store := &NATSStore{policy: testPolicy(), kv: &principalKV{}}
	snapshot, err := store.Submit(tracing.WithCarrier(context.Background(), fixtureTrace()), request)
	if err != nil || snapshot.Job.Trace != nil {
		t.Fatal("optional trace broke valid near-limit submission", err)
	}
	durable, err := store.Get(context.Background(), snapshot.Job.ID)
	if err != nil || durable.Job.Trace != nil {
		t.Fatal("submission returned different diagnostic state", err)
	}
	request.Parameters[0].Value = json.RawMessage(fmt.Sprintf(`"%s"`, strings.Repeat("&", count+100)))
	if _, err := store.Submit(context.Background(), request); err == nil || errors.Is(err, ErrCapacity) {
		t.Fatal("core overflow did not retain its size error")
	}
}
