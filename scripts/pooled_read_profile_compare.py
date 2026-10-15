#!/usr/bin/env python3
"""Compare artifacts of the `Pooled read profile` workflow (before / after) in one Markdown table.

    pooled_read_profile_compare.py before=/path/to/artifact after=/path/to/artifact

Needs `go` on PATH for `go tool pprof`. Prints, per run and load (low, 50 concurrent): warm latency
percentiles, failures, reads per second, source round trips per read, node and worker CPU per read
(net of the idle window), node allocations per read, ed25519 verifications per read, adapter
allocations, worker memory.
"""
import json
import re
import subprocess
import sys
from pathlib import Path


def pprof(*args):
    return subprocess.run(["go", "tool", "pprof", *args], capture_output=True, text=True).stdout


def seconds(text):
    return float(text[:-2]) / 1000 if text.endswith("ms") else float(text[:-1])


def profile_total(path):
    m = re.search(r"Total samples = ([\d.]+m?s)", pprof("-top", "-nodecount=1", str(path)))
    return seconds(m[1]) if m else 0.0


def cum(path, name):
    for line in pprof("-top", "-cum", "-nodecount=800", str(path)).splitlines():
        m = re.match(r"\s*\S+\s+[\d.]+%\s+[\d.]+%\s+([\d.]+m?s)\s+[\d.]+%\s+(\S.*)$", line)
        if m and m[2] == name:
            return seconds(m[1])
    return 0.0


def alloc_bytes(base, path):
    out = pprof("-sample_index=alloc_space", "-base", str(base), "-top", "-nodecount=1", str(path))
    m = re.search(r"of ([\d.]+)(kB|MB|GB)\s+total", out)
    return float(m[1]) * {"kB": 1e3, "MB": 1e6, "GB": 1e9}[m[2]] if m else 0.0


def collect(root):
    root = Path(root)
    report = json.loads((root / "report.json").read_text())
    pp = report["profile_plain"]
    idle = pp["idle"]["window"]
    idle_rate = idle["unit_cpu_s"] / idle["elapsed_s"]
    verify_us = (report.get("node_summary") or {}).get("ed25519_verify_micros") or 37.0
    rows = {}
    for n in ("low", "c50"):
        x, w = pp[n], pp[n]["window"]
        warm = x["loadgen"]["classes"]["plain_warm"]
        reads = max(x["reads"], 1)
        net = w["unit_cpu_s"] - idle_rate * w["elapsed_s"]
        verify = cum(root / f"window-{n}.cpu.pprof", "crypto/ed25519.Verify (inline)")
        a = json.loads((root / f"adapter-{n}.json").read_text()) if (root / f"adapter-{n}.json").exists() else {}
        mem = w["worker_memory"]
        rows[n] = {
            "client p50 ms": warm["p50_ms"], "client p95 ms": warm["p95_ms"], "client p99 ms": warm["p99_ms"],
            "reads/s": x["loadgen"]["warm_reads_per_second"], "failed": "%d / %d" % (warm["errors"], warm["n"]),
            "source turns per read": x["source_round_trips_per_read"],
            "node CPU ms per read (net of idle)": 1000 * net / reads, "worker CPU ms per read": x["worker_cpu_ms_per_read"],
            "node alloc MB per read": alloc_bytes(root / f"window-{n}.base.allocs.pprof", root / f"window-{n}.allocs.pprof") / reads / 1e6,
            "ed25519 verifications per read": verify / (verify_us / 1e6) / reads,
            "adapter alloc KB per read": a.get("allocated_bytes_per_read", 0) / 1e3, "adapter mallocs per read": a.get("mallocs_per_read", 0),
            "adapter p50 ms (in process)": a.get("p50_ms", 0),
            "worker anon MiB (mean)": sum(m.get("rssanon_kb", 0) for m in mem) / max(len(mem), 1) / 1024,
        }
    rows["idle cores"] = {"idle": idle_rate}
    return rows


runs = [(a.split("=", 1)[0], collect(a.split("=", 1)[1])) for a in sys.argv[1:]]
metrics = list(next(iter(runs))[1]["low"].keys())
print("| | " + " | ".join("%s %s" % (n, load) for n, _ in runs for load in ("low", "50 conc.")) + " |")
print("|---|" + "---|" * (2 * len(runs)))
for metric in metrics:
    cells = []
    for _, r in runs:
        for load in ("low", "c50"):
            v = r[load].get(metric, "-")
            cells.append(v if isinstance(v, str) else "%.2f" % v if abs(v) < 100 else "%.0f" % v)
    print("| " + metric + " | " + " | ".join(cells) + " |")
print("\nidle node: " + ", ".join("%s %.3f cores" % (n, r["idle cores"]["idle"]) for n, r in runs))
