//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/jackc/pgx/v5"
)

// A load run that stops making progress used to leave only its symptoms: a
// count of deadline errors and a flat CPU line. stallWatch notices when gateway
// queries are in flight but none has completed for stallAfter, and captures the
// state that says where they are stuck: every goroutine of the node and gateway
// (one process), each contained worker's kernel stack, the source's backend
// states, the admission pool and the metrics. It captures twice, stallRecheck
// apart, so a stuck goroutine can be told from a slow one.
const (
	stallAfter   = 20 * time.Second
	stallRecheck = 30 * time.Second
)

type stallWatch struct {
	dir          string
	inflight     atomic.Int64
	started      atomic.Int64
	completed    atomic.Int64
	lastProgress atomic.Int64 // unix nanoseconds
	captures     int
	firstAt      time.Time
}

func newStallWatch(out string) *stallWatch {
	w := &stallWatch{dir: filepath.Join(out, "stall")}
	w.lastProgress.Store(time.Now().UnixNano())
	return w
}

// begin counts one gateway query request and returns the function that ends it.
func (w *stallWatch) begin() func(status int) {
	w.started.Add(1)
	if w.inflight.Add(1) == 1 {
		w.lastProgress.Store(time.Now().UnixNano()) // idle time is not a stall
	}
	return func(status int) {
		w.inflight.Add(-1)
		if status < 400 {
			w.completed.Add(1)
			w.lastProgress.Store(time.Now().UnixNano())
		}
	}
}

// stallSources is what a capture reads besides the process itself.
type stallSources struct {
	cgroupRoot, unitCgroup string
	pool                   *admission.Pool
	metrics                func() string // the Prometheus text
	poolReads              func() (hits, misses, spawns int64)
	adminDSN               string
}

// poll runs on the sampling tick. It returns whether a stall has been captured.
func (w *stallWatch) poll(now time.Time, src stallSources, logf func(string, ...any)) bool {
	switch {
	case w.captures == 0:
		if w.inflight.Load() == 0 || now.Sub(time.Unix(0, w.lastProgress.Load())) < stallAfter {
			return false
		}
		w.firstAt = now
	case w.captures == 1 && now.Sub(w.firstAt) >= stallRecheck:
	default:
		return w.captures > 0
	}
	w.captures++
	dir := filepath.Join(w.dir, fmt.Sprintf("capture-%d", w.captures))
	logf("STALL: %d gateway queries in flight, none completed for %s; capturing to %s", w.inflight.Load(), now.Sub(time.Unix(0, w.lastProgress.Load())).Round(time.Second), dir)
	w.capture(dir, now, src)
	return true
}

func (w *stallWatch) capture(dir string, now time.Time, src stallSources) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return
	}
	write := func(name, text string) { _ = os.WriteFile(filepath.Join(dir, name), []byte(text), 0600) }
	hits, misses, spawns := src.poolReads()
	summary, _ := json.Marshal(map[string]any{"at": now.UTC().Format(time.RFC3339), "gateway_inflight": w.inflight.Load(), "gateway_started": w.started.Load(),
		"gateway_completed": w.completed.Load(), "warm_pool_hits": hits, "warm_pool_misses": misses, "warm_pool_spawns": spawns})
	write("summary.json", string(summary))
	// Every goroutine of the node and the gateway, with the reason each waits:
	// a lost wakeup or a deadlock is a goroutine parked on a channel nobody closes.
	if f, err := os.Create(filepath.Join(dir, "goroutines.txt")); err == nil {
		_ = pprof.Lookup("goroutine").WriteTo(f, 2)
		_ = f.Close()
	}
	write("procs.txt", processStates(src.cgroupRoot, src.unitCgroup))
	if snapshot, err := json.MarshalIndent(src.pool.Snapshot(), "", " "); err == nil {
		write("admission.json", string(snapshot))
	}
	write("metrics.prom", src.metrics())
	write("source-backends.txt", sourceBackends(src.adminDSN))
}

// processStates lists each contained process with its state, what it sleeps in
// and its kernel stack (best effort: the stack needs privileges).
func processStates(cgroupRoot, unitCgroup string) string {
	var out strings.Builder
	var visit func(dir string)
	visit = func(dir string) {
		if raw, err := os.ReadFile(filepath.Join(dir, "cgroup.procs")); err == nil {
			for _, pid := range strings.Fields(string(raw)) {
				fmt.Fprintf(&out, "== pid %s (%s)\n", pid, strings.TrimPrefix(dir, unitCgroup))
				cmdline, _ := os.ReadFile("/proc/" + pid + "/cmdline")
				fmt.Fprintf(&out, "cmdline: %s\n", strings.ReplaceAll(string(cmdline), "\x00", " "))
				if status, err := os.ReadFile("/proc/" + pid + "/status"); err == nil {
					for _, line := range strings.Split(string(status), "\n") {
						if strings.HasPrefix(line, "State:") || strings.HasPrefix(line, "VmRSS:") || strings.HasPrefix(line, "Threads:") {
							fmt.Fprintln(&out, line)
						}
					}
				}
				wchans := map[string]int{}
				if tasks, err := os.ReadDir("/proc/" + pid + "/task"); err == nil {
					for _, task := range tasks {
						if wchan, err := os.ReadFile("/proc/" + pid + "/task/" + task.Name() + "/wchan"); err == nil {
							wchans[string(wchan)]++
						}
					}
				}
				fmt.Fprintf(&out, "thread wchans: %v\n", wchans)
				if stack, err := os.ReadFile("/proc/" + pid + "/stack"); err == nil {
					fmt.Fprintf(&out, "kernel stack:\n%s", stack)
				}
			}
		}
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if e.IsDir() {
				visit(filepath.Join(dir, e.Name()))
			}
		}
	}
	visit(cgroupRoot)
	visit(filepath.Join(unitCgroup, "supervisor"))
	return out.String()
}

// sourceBackends groups the source's customer sessions by state and wait event:
// held connections show as "idle in transaction" or "active", an unused pool as "idle".
func sourceBackends(adminDSN string) string {
	if adminDSN == "" {
		return "no admin DSN (KELVO_LOAD_PG_ADMIN_DSN)\n"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return "connect failed\n"
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx, `SELECT usename, coalesce(state,'?'), coalesce(wait_event_type,''), coalesce(wait_event,''), count(*), coalesce(max(round(extract(epoch FROM now()-state_change))),0)::bigint, coalesce(max(left(query,70)),'')
		FROM pg_stat_activity WHERE datname LIKE 'cust%' GROUP BY 1,2,3,4 ORDER BY 1,2`)
	if err != nil {
		return "query failed: " + err.Error() + "\n"
	}
	defer rows.Close()
	var out strings.Builder
	fmt.Fprintln(&out, "usename state wait_type wait_event count max_state_age_s query")
	for rows.Next() {
		var user, state, waitType, waitEvent, query string
		var count, age int64
		if rows.Scan(&user, &state, &waitType, &waitEvent, &count, &age, &query) == nil {
			fmt.Fprintf(&out, "%s | %s | %s | %s | %d | %d | %s\n", user, state, waitType, waitEvent, count, age, query)
		}
	}
	return out.String()
}
