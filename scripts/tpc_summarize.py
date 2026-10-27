#!/usr/bin/env python3
"""Markdown tables from tpc_benchmark.py reports (reads JSON only)."""
import argparse
import json
import math
import sys

sys.dont_write_bytecode = True
NAMES = {"kelvo": "Kelvo (Parquet)", "kelvo_accel": "Kelvo (snapshot)", "duckdb": "DuckDB Python", "trino": "Trino"}
PASS = ("exact_order", "matched_unordered")


def cell(record):
    if record is None:
        return "not run"
    if record["state"] != "passed":
        fails = [t for t in record["trials"] if t["verdict"] not in PASS]
        kind = "mismatch" if any(t["verdict"] == "mismatch" for t in fails) else "failed"
        return f"{kind} {len(fails)}/{len(record['trials'])}"
    s = record["seconds"]
    return f"{s['median']:.2f} ({s['min']:.2f}–{s['max']:.2f})"


def per_query(report):
    systems = report["systems"]
    out = ["| Query | " + " | ".join(NAMES[s] for s in systems) + " | Kelvo peak cgroup MiB |",
           "| --- |" + " ---: |" * (len(systems) + 1)]
    for q, entry in sorted(report["queries"].items()):
        k = entry["systems"].get("kelvo")
        mem = f"{k['peak_cgroup_memory_bytes'] / 2**20:.0f}" if k else "–"
        out.append(f"| {q} | " + " | ".join(cell(entry["systems"].get(s)) for s in systems) + f" | {mem} |")
    return "\n".join(out)


def totals(report):
    """Each system is compared with DuckDB only on queries where both passed every trial."""
    systems, queries = report["systems"], report["queries"]
    out = ["| System | Queries passed (all trials) | Mismatch trials | Failed trials | Queries shared with DuckDB | "
           "Sum of medians, s (system / DuckDB) | Geomean ratio vs DuckDB |",
           "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for s in systems:
        recs = [e["systems"][s] for e in queries.values() if s in e["systems"]]
        trials = [t for r in recs for t in r["trials"]]
        both = [e["systems"] for e in queries.values()
                if e["systems"].get(s, {}).get("state") == "passed" and e["systems"].get("duckdb", {}).get("state") == "passed"]
        mine = [b[s]["seconds"]["median"] for b in both]
        base = [b["duckdb"]["seconds"]["median"] for b in both]
        geo = math.exp(sum(math.log(x / y) for x, y in zip(mine, base)) / len(both)) if both else float("nan")
        out.append(f"| {NAMES[s]} | {sum(r['state'] == 'passed' for r in recs)}/{len(recs)} | "
                   f"{sum(t['verdict'] == 'mismatch' for t in trials)} | {sum(t['verdict'] == 'failed' for t in trials)} | "
                   f"{len(both)} | {sum(mine):.2f} / {sum(base):.2f} | {geo:.2f} |")
    return "\n".join(out)


def concurrency(report):
    out = ["| Streams | Queries | Succeeded | Failed | 429 refusals (retried) | Wall s | Throughput q/h | p50 s | p95 s | p99 s | Server peak cgroup MiB |",
           "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for _, v in sorted(report["levels"].items(), key=lambda kv: int(kv[0])):
        mem = (v.get("server") or {}).get("peak_cgroup_memory_bytes", 0) / 2**20
        out.append(f"| {v['streams']} | {v['queries']} | {v['succeeded']} | {v['failed']} | {v['refusals']} | "
                   f"{v['wall_seconds']:.1f} | {v['throughput_qph']:.0f} | {v['p50_s']:.2f} | {v['p95_s']:.2f} | "
                   f"{v['p99_s']:.2f} | {mem:.0f} |")
    return "\n".join(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+")
    parser.add_argument("--per-query", action="store_true")
    args = parser.parse_args()
    for path in args.reports:
        report = json.load(open(path))
        if "levels" in report:
            print(f"### {path}: TPC-H SF{report['sf']:g} concurrency, {report['host'].get('cpu_model')}\n")
            print(concurrency(report) + "\n")
            continue
        print(f"### {path}: {report['suite'].upper()} SF{report['sf']:g}, {report['host'].get('cpu_model')}\n")
        print(totals(report) + "\n")
        if args.per_query:
            print(per_query(report) + "\n")


if __name__ == "__main__":
    main()
