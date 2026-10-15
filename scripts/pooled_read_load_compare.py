#!/usr/bin/env python3
"""Compare several report.json files from scripts/pooled_read_load.py in one Markdown table.

    pooled_read_load_compare.py name=path/report.json name2=other/report.json
"""
import json
import sys

runs = [(a.split("=", 1)[0], json.load(open(a.split("=", 1)[1]))) for a in sys.argv[1:]]


def cls(r, name, key):
    s = r.get("loadgen", {}).get("classes", {}).get(name)
    return "-" if not s else ("%d" % s[key] if key in ("n", "errors") else "%.1f s" % (s[key] / 1000))


def ratio(r, name):
    s = r.get("loadgen", {}).get("classes", {}).get(name)
    return "-" if not s else "%d/%d" % (s["errors"], s["n"])


rows = [
    ("requests failed / sent: explorer read", lambda r: ratio(r, "explorer_read")),
    ("explorer inspect", lambda r: ratio(r, "explorer_inspect")),
    ("dashboard panel", lambda r: ratio(r, "dashboard_panel")),
    ("console", lambda r: ratio(r, "console")),
    ("analysis step", lambda r: ratio(r, "analysis_step")),
    ("explorer page open (42 requests) p50", lambda r: cls(r, "explorer_page", "p50_ms")),
    ("explorer page open p95", lambda r: cls(r, "explorer_page", "p95_ms")),
    ("explorer page open p99", lambda r: cls(r, "explorer_page", "p99_ms")),
    ("explorer read p50 / p95 / p99", lambda r: " / ".join(cls(r, "explorer_read", k) for k in ("p50_ms", "p95_ms", "p99_ms"))),
    ("explorer inspect p50 / p95 / p99", lambda r: " / ".join(cls(r, "explorer_inspect", k) for k in ("p50_ms", "p95_ms", "p99_ms"))),
    ("dashboard panel p50 / p95 / p99", lambda r: " / ".join(cls(r, "dashboard_panel", k) for k in ("p50_ms", "p95_ms", "p99_ms"))),
    ("dashboard refresh (10 panels) p95", lambda r: cls(r, "dashboard_refresh", "p95_ms")),
    ("console p50 / p95 / p99", lambda r: " / ".join(cls(r, "console", k) for k in ("p50_ms", "p95_ms", "p99_ms"))),
    ("analysis step p50", lambda r: cls(r, "analysis_step", "p50_ms")),
    ("errors by code", lambda r: json.dumps(r.get("loadgen", {}).get("errors_by_code", {})).replace("|", "/")),
    ("server capacity errors (RESOURCE_EXHAUSTED)", lambda r: str(r.get("loadgen", {}).get("capacity_errors"))),
    ("client-admission refusals (generator cap 256)", lambda r: str(r.get("loadgen", {}).get("client_admission_refusals"))),
    ("in flight: mean / peak", lambda r: "%.0f / %s" % (r.get("loadgen", {}).get("mean_inflight_requests", 0), r.get("loadgen", {}).get("peak_inflight_requests"))),
    ("node CPU cores (mean) / % of limit", lambda r: "%s / %s%%" % (r["node"].get("cpu_cores_mean"), r["node"].get("cpu_pct_of_limit"))),
    ("CPU throttled periods", lambda r: str(r["node"].get("nr_throttled"))),
    ("cgroup memory peak MiB", lambda r: str(r["node"].get("memory_peak_mib"))),
    ("RSS gateway+node peak / workers peak MiB", lambda r: "%s / %s" % (r["node"].get("rss_node_peak_mib"), r["node"].get("rss_workers_peak_mib"))),
    ("workers: spawns / peak job cgroups", lambda r: "%s / %s" % (r["node"]["warm_pool"].get("spawns"), r["node"].get("job_cgroups_peak"))),
    ("pool hits / misses / fallbacks", lambda r: "%s / %s / %s" % tuple(r["node"]["warm_pool"].get(k) for k in ("hits", "misses", "fallbacks"))),
    ("evictions: ttl / revision / auth", lambda r: "%s / %s / %s" % tuple(r["node"]["warm_pool"].get(k) for k in ("evictions_ttl", "evictions_revision", "evictions_auth"))),
    ("resolver calls (300 ms each)", lambda r: str(r["node"].get("resolver_calls"))),
    ("dashboard source queries per viewer refresh (10 = no cache)", lambda r: "%.2f" % r.get("loadgen", {}).get("dashboard", {}).get("source_queries_per_viewer_refresh", 0)),
    ("dashboard cache hit rate", lambda r: "%.2f" % r.get("loadgen", {}).get("dashboard", {}).get("cache_hit_rate", 0)),
    ("source backends (max)", lambda r: str(r.get("source_backends", {}).get("max"))),
    ("runner CPU busy mean / max %", lambda r: "%s / %s" % (r.get("runner_cpu_busy_pct", {}).get("mean"), r.get("runner_cpu_busy_pct", {}).get("max"))),
    ("CPU outside the node: loadgen / proxy / PostgreSQL (s)", lambda r: "%s / %s / %s" % tuple(r.get("outside_node_cpu", {}).get(k) for k in ("loadgen_cpu_s", "latency_proxy_cpu_s", "postgres_container_cpu_s"))),
]
print("| | " + " | ".join(n for n, _ in runs) + " |")
print("|---|" + "---|" * len(runs))
for label, f in rows:
    cells = []
    for _, r in runs:
        try:
            cells.append(f(r))
        except Exception:
            cells.append("-")
    print("| " + label + " | " + " | ".join(cells) + " |")
