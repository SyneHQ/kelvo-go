// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// Measures what one single-call transient read costs the job store: broker
// messages sent by the gateway and by the node, allocations and CPU. It replays
// the store calls of a read (see replayRead) against a real JetStream and is
// skipped without one. It is a store-call replay, not an end-to-end measurement:
// it leaves out the node's lease renewals, the resolver's lease reads, the
// gateway's result-wait polls and re-reads, audit, and the result stream. It
// guards the store's cost against regression; the end-to-end figures need the
// profile harness. Run it twice, before and after a change:
//
//	KELVO_MEASURE_READS=400 go test ./internal/cluster -run TestMeasureTransientReadStoreCost -v
func TestMeasureTransientReadStoreCost(t *testing.T) {
	reads := 0
	if raw := os.Getenv("KELVO_MEASURE_READS"); raw != "" {
		for _, c := range raw {
			reads = reads*10 + int(c-'0')
		}
	}
	if reads == 0 {
		t.Skip("set KELVO_MEASURE_READS to measure")
	}
	cfg, policy := fixtureConfig(t)
	policy.TenantID = "diet"
	policy.MaxQueries = 64
	policy.Workers = map[string]int{"w1": 32}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	gateway, err := OpenStore(ctx, cfg, policy, true)
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	node, err := OpenStore(ctx, cfg, policy, false)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	const concurrent = 16
	run := func(n int) {
		var wg sync.WaitGroup
		per := n / concurrent
		for range concurrent {
			wg.Go(func() {
				for range per {
					if err := replayRead(ctx, gateway, node); err != nil {
						t.Error(err)
						return
					}
				}
			})
		}
		wg.Wait()
	}
	run(2 * 64) // fill every slot with a recent terminal job: the steady state
	g0, n0 := gateway.nc.Stats(), node.nc.Stats()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	cpu0 := cpuTime()
	run(reads)
	cpu1 := cpuTime()
	runtime.ReadMemStats(&m1)
	g1, n1 := gateway.nc.Stats(), node.nc.Stats()
	done := float64(reads / concurrent * concurrent)
	gatewayMsgs, nodeMsgs := float64(g1.OutMsgs-g0.OutMsgs)/done, float64(n1.OutMsgs-n0.OutMsgs)/done
	if gatewayMsgs > 8 || nodeMsgs > 9 {
		t.Errorf("store cost regressed: %.1f gateway and %.1f node broker messages per read (was 6 and 7)", gatewayMsgs, nodeMsgs)
	}
	if kb := float64(m1.TotalAlloc-m0.TotalAlloc) / done / 1024; kb > 150 {
		t.Errorf("store allocations regressed: %.0f KB per read (was 45)", kb)
	}
	t.Logf("per read: gateway broker msgs %.1f, node broker msgs %.1f, total %.1f; allocs %.0f KB (%.0f objects); CPU %.2f ms (test process, includes the NATS client)",
		float64(g1.OutMsgs-g0.OutMsgs)/done, float64(n1.OutMsgs-n0.OutMsgs)/done,
		float64(g1.OutMsgs-g0.OutMsgs+n1.OutMsgs-n0.OutMsgs)/done,
		float64(m1.TotalAlloc-m0.TotalAlloc)/done/1024, float64(m1.Mallocs-m0.Mallocs)/done,
		float64((cpu1-cpu0).Microseconds())/done/1000)
}

func cpuTime() time.Duration {
	var u syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
	return time.Duration(u.Utime.Nano() + u.Stime.Nano())
}

// replayRead performs the store operations of one single-call read in the order
// the gateway and the node perform them: gateway submit and enqueue; node
// dispatch (fetch, read, assign, ack); gateway sees the assignment and claims;
// node starts the claimed job and publishes the result; gateway commits it.
func replayRead(ctx context.Context, gateway, node Store) error {
	snap, err := submitAndEnqueue(ctx, gateway, query.Request{Mode: "federated", SQL: "select 1"}, true)
	if err != nil {
		return err
	}
	var delivered Delivery
	for delivered == nil {
		d, err := node.Next(ctx)
		if err != nil {
			if err == ErrNoJob {
				continue
			}
			return err
		}
		// Another reader's message may arrive first: serve whichever job comes.
		delivered = d
	}
	nodeSnap, err := node.Get(ctx, delivered.ID())
	if err != nil {
		return err
	}
	assigned := nodeSnap.Job
	assigned.State, assigned.WorkerID, assigned.Owner = Assigned, "w1", randomHex(16)
	nodeSnap, err = node.CompareAndSwap(ctx, nodeSnap, assigned)
	if err != nil {
		return err
	}
	if err := delivered.Ack(ctx); err != nil {
		return err
	}
	// The gateway's wait observes the assignment and claims it.
	id := delivered.ID()
	claimSnap, err := gateway.Get(ctx, id)
	if err != nil {
		return err
	}
	claimed := claimSnap.Job
	claimed.State, claimed.Claim = Claimed, randomHex(16)
	if claimSnap, err = gateway.CompareAndSwap(ctx, claimSnap, claimed); err != nil {
		return err
	}
	// The node starts it, then publishes the result (Node.mutate: get, then swap).
	// Node.mutate: the first transition reads the job (the gateway wrote it
	// since the node's own write); the second starts from the node's own latest
	// write, which nobody has replaced.
	s, err := node.Get(ctx, id)
	if err != nil {
		return err
	}
	j := s.Job
	j.State = Running
	if s, err = node.CompareAndSwap(ctx, s, j); err != nil {
		return err
	}
	j = s.Job
	j.State, j.Stats = ResultReady, query.Stats{Rows: 50, Batches: 1, WireBytes: 3128}
	if _, err = node.CompareAndSwap(ctx, s, j); err != nil {
		return err
	}
	// The gateway commits (commitResult: read, then swap).
	final, err := gateway.Get(ctx, id)
	if err != nil {
		return err
	}
	done := final.Job
	done.State = Succeeded
	_, err = gateway.CompareAndSwap(ctx, final, done)
	_ = snap
	_ = nodeSnap
	return err
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
