#!/usr/bin/env python3
"""Consistency checks on recorded TPC evidence JSON (no engines, no re-runs).

For every report: each query/system has the planned trial count, every failed
or mismatched trial keeps its error/detail, recorded medians/min/max and
pass state recompute from the raw trials, and peaks are maxima over ALL trials
(failures included). Then a tampered copy of the first report must be rejected.
"""
import argparse
import copy
import json
from pathlib import Path
import statistics
import sys

sys.dont_write_bytecode = True
PASS = ("exact_order", "matched_unordered")


def problems(report):
    found = []
    if "levels" in report:
        for key, level in report["levels"].items():
            ok = [r["latency_s"] for r in level["results"] if r["error"] is None]
            if level["succeeded"] != len(ok) or level["queries"] != len(level["results"]):
                found.append(f"streams {key}: counts")
            if level["refusals"] != sum(r["refusals"] for r in level["results"]):
                found.append(f"streams {key}: refusals")
            if level["queries"] != 22 * level["streams"]:
                found.append(f"streams {key}: expected 22 queries per stream")
        return found
    expected = {"tpch": 22, "tpcds": 99}[report["suite"]]
    if len(report["queries"]) != expected:
        found.append(f"query count {len(report['queries'])} != {expected}")
    for q, entry in report["queries"].items():
        for system in report["systems"]:
            record = entry["systems"].get(system)
            if record is None:
                found.append(f"{q}/{system}: missing")
                continue
            trials = record["trials"]
            if len(trials) != report["trials"]:
                found.append(f"{q}/{system}: {len(trials)} trials != {report['trials']}")
            good = [t["process_seconds"] for t in trials if t["verdict"] in PASS]
            for t in trials:
                if t["verdict"] == "failed" and not t.get("error"):
                    found.append(f"{q}/{system}: failure without error text")
                if t["verdict"] == "mismatch" and not t.get("verdict_detail"):
                    found.append(f"{q}/{system}: mismatch without detail")
            stats = record["seconds"]
            if good:
                if stats is None or abs(stats["median"] - statistics.median(good)) > 1e-9 or stats["min"] != min(good) \
                        or stats["max"] != max(good) or stats["n"] != len(good):
                    found.append(f"{q}/{system}: seconds do not recompute")
            elif stats is not None:
                found.append(f"{q}/{system}: seconds without passing trials")
            if record["state"] != ("passed" if len(good) == len(trials) else "failed"):
                found.append(f"{q}/{system}: state")
            for key in ("peak_cgroup_memory_bytes", "peak_sampled_tree_rss_bytes", "peak_spill_bytes"):
                if record[key] != max((t.get(key) or 0) for t in trials):
                    found.append(f"{q}/{system}: {key} is not the all-trial maximum")
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", nargs="*", type=Path)
    args = parser.parse_args()
    paths = args.evidence or sorted(Path(__file__).resolve().parent.parent.glob("docs/evidence/tpc-*.json"))
    reports, failed = {}, False
    for path in paths:
        reports[path.name] = json.loads(path.read_text())
        found = problems(reports[path.name])
        failed |= bool(found)
        print(json.dumps({"file": path.name, "state": "failed" if found else "passed", "problems": found[:20]}))
    run = next((r for r in reports.values() if "queries" in r), None)
    if run is None:
        print(json.dumps({"state": "failed", "check": "no run report to tamper"}))
        raise SystemExit(1)
    q, entry = next(iter(run["queries"].items()))
    system = run["systems"][0]
    tampers = {}
    t = copy.deepcopy(run)
    t["queries"][q]["systems"][system]["trials"].pop()
    tampers["dropped_trial"] = t
    t = copy.deepcopy(run)
    rec = t["queries"][q]["systems"][system]
    rec["trials"][0].update(verdict="failed", error="injected", peak_cgroup_memory_bytes=rec["peak_cgroup_memory_bytes"] + 1)
    tampers["failure_hidden_behind_old_summary"] = t
    t = copy.deepcopy(run)
    t["queries"][q]["systems"][system]["trials"][0]["verdict"] = "mismatch"
    t["queries"][q]["systems"][system]["trials"][0].pop("verdict_detail", None)
    tampers["mismatch_without_detail"] = t
    for name, value in tampers.items():
        caught = bool(problems(value))
        failed |= not caught
        print(json.dumps({"tamper": name, "rejected": caught}))
    print(json.dumps({"state": "failed" if failed else "passed", "reports": len(reports),
                      "scope": "JSON consistency only; no benchmarks or answers re-executed"}))
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
