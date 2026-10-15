//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// durationLog keeps every observed duration, in seconds, so a profiling window
// can report only what happened inside it.
type durationLog struct {
	mu sync.Mutex
	v  []float64
}

func (d *durationLog) add(seconds float64) {
	d.mu.Lock()
	d.v = append(d.v, seconds)
	d.mu.Unlock()
}
func (d *durationLog) len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.v)
}
func (d *durationLog) since(i int) []float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]float64(nil), d.v[min(i, len(d.v)):]...)
}

func summarizeSeconds(v []float64) map[string]any {
	if len(v) == 0 {
		return map[string]any{"n": 0}
	}
	sorted := append([]float64(nil), v...)
	sort.Float64s(sorted)
	var sum float64
	for _, x := range sorted {
		sum += x
	}
	at := func(p float64) float64 { return sorted[max(int(math.Ceil(p/100*float64(len(sorted))))-1, 0)] * 1000 }
	return map[string]any{"n": len(v), "mean_ms": sum / float64(len(v)) * 1000, "p50_ms": at(50), "p95_ms": at(95), "p99_ms": at(99), "max_ms": sorted[len(sorted)-1] * 1000, "total_s": sum}
}

type windowState struct {
	name      string
	began     time.Time
	cpu       *os.File
	index     map[string]int
	unitCPU   uint64
	workerCPU map[string]uint64
}

// profileWindows starts and stops profiling windows on request: a file
// window-NAME.start begins one, window-NAME.stop ends it and writes
// window-NAME.json, the CPU, allocation and heap profiles, then window-NAME.done.
// The driver brackets exactly the load it wants profiled.
type profileWindows struct {
	out, jobs, unit string
	logs            map[string]*durationLog
	active          map[string]*windowState
	finished        map[string]bool
}

func newProfileWindows(out, jobs, unit string, logs map[string]*durationLog) *profileWindows {
	return &profileWindows{out: out, jobs: jobs, unit: unit, logs: logs, active: map[string]*windowState{}, finished: map[string]bool{}}
}

func (p *profileWindows) workerCPU() map[string]uint64 {
	m := map[string]uint64{}
	entries, _ := os.ReadDir(p.jobs)
	for _, e := range entries {
		if e.IsDir() {
			m[e.Name()] = readUint(filepath.Join(p.jobs, e.Name(), "cpu.stat"), "usage_usec")
		}
	}
	return m
}

func (p *profileWindows) writeProfile(name, kind string) {
	if f, err := os.Create(filepath.Join(p.out, "window-"+name+"."+kind+".pprof")); err == nil {
		_ = pprof.Lookup(kind).WriteTo(f, 0)
		_ = f.Close()
	}
}

// poll is called about once a second from the node's sampling loop.
func (p *profileWindows) poll(t *testing.T) {
	starts, _ := filepath.Glob(filepath.Join(p.out, "window-*.start"))
	for _, path := range starts {
		name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "window-"), ".start")
		if p.active[name] != nil || p.finished[name] {
			continue
		}
		runtime.GC()
		p.writeProfile(name+".base", "allocs")
		w := &windowState{name: name, began: time.Now(), index: map[string]int{}, unitCPU: readUint(filepath.Join(p.unit, "cpu.stat"), "usage_usec"), workerCPU: p.workerCPU()}
		for k, l := range p.logs {
			w.index[k] = l.len()
		}
		if f, err := os.Create(filepath.Join(p.out, "window-"+name+".cpu.pprof")); err == nil {
			if pprof.StartCPUProfile(f) == nil {
				w.cpu = f
			} else {
				_ = f.Close()
			}
		}
		p.active[name] = w
		_ = os.WriteFile(filepath.Join(p.out, "window-"+name+".started"), nil, 0600)
	}
	for name, w := range p.active {
		if _, err := os.Stat(filepath.Join(p.out, "window-"+name+".stop")); err == nil {
			p.stop(t, w)
		}
	}
}

func (p *profileWindows) stop(t *testing.T, w *windowState) {
	if w.cpu != nil {
		pprof.StopCPUProfile()
		_ = w.cpu.Close()
	}
	elapsed := time.Since(w.began).Seconds()
	p.writeProfile(w.name, "allocs")
	runtime.GC()
	p.writeProfile(w.name, "heap")
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	workers := p.workerCPU()
	var workerDelta uint64
	for k, v := range workers {
		workerDelta += v - min(v, w.workerCPU[k])
	}
	stats := map[string]any{}
	for k, l := range p.logs {
		stats[k] = summarizeSeconds(l.since(w.index[k]))
	}
	report := map[string]any{"name": w.name, "elapsed_s": elapsed, "stages": stats,
		"unit_cpu_s": float64(readUint(filepath.Join(p.unit, "cpu.stat"), "usage_usec")-w.unitCPU) / 1e6, "worker_cgroups_cpu_s": float64(workerDelta) / 1e6,
		"worker_cgroups_live": len(workers), "node_heap_in_use_bytes": mem.HeapInuse, "node_heap_alloc_bytes": mem.HeapAlloc, "node_sys_bytes": mem.Sys, "node_total_alloc_bytes": mem.TotalAlloc,
		"node_mallocs": mem.Mallocs, "node_gc_cycles": mem.NumGC, "node_gc_pause_total_ms": float64(mem.PauseTotalNs) / 1e6, "worker_memory": p.workerMemory()}
	raw, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile(filepath.Join(p.out, "window-"+w.name+".json"), raw, 0600)
	delete(p.active, w.name)
	p.finished[w.name] = true
	_ = os.WriteFile(filepath.Join(p.out, "window-"+w.name+".done"), nil, 0600)
}

// workerMemory breaks the resident memory of every contained process into the
// binary's own mappings (text, read-only data, data), anonymous memory (Go heap,
// stacks, runtime) and other files.
func (p *profileWindows) workerMemory() []map[string]any {
	var out []map[string]any
	entries, _ := os.ReadDir(p.jobs)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(p.jobs, e.Name())
		raw, err := os.ReadFile(filepath.Join(dir, "cgroup.procs"))
		if err != nil {
			continue
		}
		for _, pid := range strings.Fields(string(raw)) {
			item := map[string]any{"cgroup": e.Name(), "pid": pid, "cgroup_memory_current_bytes": readUint(filepath.Join(dir, "memory.current"), ""), "cgroup_memory_peak_bytes": readUint(filepath.Join(dir, "memory.peak"), "")}
			exe, _ := os.Readlink("/proc/" + pid + "/exe")
			item["exe"] = exe
			if info, err := os.Stat("/proc/" + pid + "/exe"); err == nil {
				item["exe_bytes"] = info.Size()
			}
			status, _ := os.ReadFile("/proc/" + pid + "/status")
			for _, line := range strings.Split(string(status), "\n") {
				if f := strings.Fields(line); len(f) >= 2 {
					switch f[0] {
					case "VmRSS:", "RssAnon:", "RssFile:", "RssShmem:", "Threads:":
						kb, _ := strconv.ParseUint(f[1], 10, 64)
						item[strings.ToLower(strings.TrimSuffix(f[0], ":"))+"_kb"] = kb
					}
				}
			}
			cats := map[string]uint64{}
			smaps, _ := os.ReadFile("/proc/" + pid + "/smaps")
			category := "anon"
			for _, line := range strings.Split(string(smaps), "\n") {
				f := strings.Fields(line)
				if len(f) >= 5 && strings.Contains(f[0], "-") && len(f[1]) == 4 {
					path := ""
					if len(f) >= 6 {
						path = strings.Join(f[5:], " ")
					}
					switch {
					case path == "[heap]" || path == "[stack]" || strings.HasPrefix(path, "[stack"):
						category = path
					case path == "" || strings.HasPrefix(path, "[anon"):
						category = "anon (Go heap, stacks, runtime)"
					case path == exe:
						category = "binary " + map[bool]string{true: "text (r-x)", false: map[bool]string{true: "read-only data (r--)", false: "data (rw-)"}[f[1][1] == '-']}[f[1][2] == 'x']
					case strings.HasPrefix(path, "["):
						category = path
					default:
						category = "other file " + filepath.Base(path)
					}
				} else if len(f) == 3 && f[0] == "Rss:" {
					kb, _ := strconv.ParseUint(f[1], 10, 64)
					cats[category] += kb
				}
			}
			item["rss_by_mapping_kb"] = cats
			out = append(out, item)
		}
	}
	return out
}
