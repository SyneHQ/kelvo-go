#!/usr/bin/env python3
"""Print a report.json from scripts/pooled_read_load.py as Markdown tables."""
import json
import sys

report = json.load(open(sys.argv[1]))
load, node = report.get("loadgen", {}), report.get("node", {})
print(f"### {report.get('label') or report['profile']}: {'PASS' if report['passed'] else 'FAIL'}")
print("\nGate: " + ", ".join(f"{k}={'ok' if v else 'FAILED'}" for k, v in report.get("gate", {}).items()))
print("\n| class | n | errors | p50 ms | p95 ms | p99 ms | max ms |\n|---|---|---|---|---|---|---|")
for name, s in sorted(load.get("classes", {}).items()):
    print(f"| {name} | {s['n']} | {s['errors']} | {s['p50_ms']:.0f} | {s['p95_ms']:.0f} | {s['p99_ms']:.0f} | {s['max_ms']:.0f} |")
print(f"\nOutside the node (same runner): `{json.dumps(report.get('outside_node_cpu'))}`; unavailable errors {load.get('unavailable_errors')}; client-admission refusals {load.get('client_admission_refusals')}")
print(f"\nErrors by code: `{json.dumps(load.get('errors_by_code', {}))}`; capacity errors: {load.get('capacity_errors')}; peak in flight: {load.get('peak_inflight_requests')}; mean in flight: {load.get('mean_inflight_requests', 0):.1f}")
pool = node.get("warm_pool", {})
print(f"\nNode: CPU {node.get('cpu_cores_mean')} cores ({node.get('cpu_pct_of_limit')}% of limit), throttled {node.get('nr_throttled')} periods; memory peak {node.get('memory_peak_mib')} MiB; RSS node {node.get('rss_node_peak_mib')} MiB, workers peak {node.get('rss_workers_peak_mib')} MiB; job cgroups peak {node.get('job_cgroups_peak')}")
print(f"\nPool: `{json.dumps(pool)}`; resolver calls {node.get('resolver_calls')}; node-logged failed reads `{json.dumps(node.get('failed_reads_logged', {}))}`")
dash = load.get("dashboard", {})
print(f"\nDashboard: {dash.get('panel_requests')} panel requests -> {dash.get('source_queries')} source queries (cache hit rate {dash.get('cache_hit_rate', 0):.2f}); {dash.get('source_queries_per_viewer_refresh', 0):.2f} source queries per viewer refresh (10 without a cache)")
print(f"\nExplicit refresh probe: `{json.dumps({k: v for k, v in load.get('probe_explicit_refresh', {}).items() if k != 'samples'})}`")
print(f"\nRunner CPU busy: {report.get('runner_cpu_busy_pct')}; source backends max {report.get('source_backends', {}).get('max')}")
