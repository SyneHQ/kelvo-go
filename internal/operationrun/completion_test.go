// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operationrun

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	api "github.com/SYNEHQ/kelvo-go/operations"
)

func TestOperationCompletionAndCleanupPreserveConfirmedEffects(t *testing.T) {
	for _, mode := range []string{"success", "lost acknowledgement", "timed out acknowledgement", "persistence failure", "delivery failure", "close failure", "close panic", "source panic"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, 5*time.Second)
			if mode == "persistence failure" {
				f.backend.failState = string(api.Completed)
			}
			if mode == "lost acknowledgement" || mode == "timed out acknowledgement" {
				f.backend.loseState = string(api.Completed)
				f.backend.loseUntilDeadline = mode == "timed out acknowledgement"
			}
			submitted := f.submit()
			pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 1})
			if err != nil {
				t.Fatal(err)
			}
			var completion containment.Completion
			closed := 0
			hooks := f.hooks()
			hooks.Prepare = func(_ context.Context, record ledger.Record, _ api.Request, _ ledger.Binding) (Prepared, error) {
				return &prepared{
					execute: func(ctx context.Context) (api.Receipt, error) {
						f.calls.Add(1)
						reservation, err := pool.Acquire(ctx, admission.Request{MemoryBytes: 1})
						if err != nil {
							return api.Receipt{}, err
						}
						custody, _ := containment.NewCustody(reservation.Release)
						defer custody.Complete()
						if err := containment.RetainUntilCompletion(containment.WithCompletion(ctx, &completion), custody); err != nil {
							return api.Receipt{}, err
						}
						if mode == "source panic" {
							panic("private fixture panic")
						}
						receipt := api.Receipt{Outcome: api.Completed, Effect: api.EffectCommitted}
						if mode == "delivery failure" {
							return receipt, errors.New("private fixture delivery error")
						}
						return receipt, nil
					},
					close: func() error {
						closed++
						if pool.Snapshot().Active != 1 {
							t.Error("receipt completion lost its reservation")
						}
						current, err := f.store.Get(context.Background(), record.Scope, record.ID)
						if err != nil {
							t.Error(err)
						}
						if mode != "persistence failure" && !current.Record.Terminal() {
							t.Error("Close ran before durable receipt completion")
						}
						completion.Complete()
						if mode == "close panic" {
							panic("private close panic")
						}
						if mode == "close failure" {
							return errors.New("private cleanup error")
						}
						return nil
					},
				}, nil
			}
			r, err := New(f.store, Config{WorkerID: "worker-a", Owner: strings.Repeat("a", 32), Concurrency: 1, PollInterval: 10 * time.Millisecond}, hooks)
			if err != nil {
				t.Fatal(err)
			}
			r.run(context.Background(), submitted.Record)
			var want error
			switch mode {
			case "persistence failure":
				want = ErrCompletion
			case "delivery failure":
				want = ErrDelivery
			case "close failure", "close panic":
				want = ErrCleanup
			}
			if err := r.Close(context.Background()); !errors.Is(err, want) {
				t.Fatal("lifecycle failure was not surfaced", err, want)
			}
			if err := r.Err(); err != nil && strings.Contains(err.Error(), "private") {
				t.Fatal("private failure details escaped")
			}
			current, err := f.store.Get(context.Background(), f.scope, submitted.Record.ID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "persistence failure" {
				if current.Record.State != ledger.Running {
					t.Fatal("failed persistence rewrote source outcome")
				}
			} else if mode == "source panic" {
				if current.Record.Receipt.Outcome != api.OutcomeUnknown {
					t.Fatal("source panic assumed rollback")
				}
			} else if current.Record.Receipt.Outcome != api.Completed || current.Record.Receipt.Effect != api.EffectCommitted {
				t.Fatal("lifecycle failure replaced confirmed write")
			}
			// Even an explicit rescan of the original envelope cannot rerun it.
			r.run(context.Background(), submitted.Record)
			if f.calls.Load() != 1 || closed != 1 || pool.Snapshot().Active != 0 {
				t.Fatal("completion replayed or leaked work", f.calls.Load(), closed)
			}
		})
	}
}
