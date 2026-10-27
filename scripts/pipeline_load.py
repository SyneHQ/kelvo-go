#!/usr/bin/env python3
"""Pipeline-scale workflows, dashboard fan-out and soak through the cluster.

  references  DuckDB answers over the generator's Parquet (outside timing)
  validate    every workflow native / federated / snapshot, exact-checked
  fanout      50-tile dashboard x 1/5/20/50 concurrent viewers
  soak        mixed load for --seconds with scheduled fault injection

Runs on the Linux VM against pipeline_cluster.py. Reuses Failure, Sampler and
percentile from federation_cluster_load. Every answer is compared with the
DuckDB reference; a mismatch is a failure, never a retry.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import decimal
import hashlib
import io
import json
import os
from pathlib import Path
import random
import signal
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.request

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / "benchmarks/pipelines/workflows.yml"
PRIVATE = ROOT / "artifacts/pipeline-private"
TABLES = ("orders", "customers", "shipments", "sellers", "warehouses", "order_items", "events_hot", "events_cold")
FEDERATED = {"orders": "pg.orders", "customers": "pg.customers", "shipments": "my.shipments", "sellers": "my.sellers",
             "warehouses": "my.warehouses", "order_items": "ch.order_items", "events_hot": "ch.events_hot", "events_cold": "cold"}
SNAPSHOT = {t: t + "_snap" for t in TABLES if t != "events_cold"} | {"events_cold": "cold"}
EOS = b"\xff\xff\xff\xff\0\0\0\0"


def bind(sql, mapping):
    return sql.format(**mapping)


def sources_of(sql, mapping):
    used = [mapping[t] for t in TABLES if "{" + t + "}" in sql]
    return sorted({name.split(".")[0] for name in used})


def normalize(value):
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, decimal.Decimal):
        if value != value.to_integral_value():
            raise ValueError("non-integral decimal in exact answer")
        return int(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    raise ValueError("unsupported answer type " + type(value).__name__)


def digest(rows):
    """Order-insensitive exact digest of a list of row tuples."""
    canonical = sorted(json.dumps([normalize(v) for v in row]) for row in rows)
    return {"rows": len(canonical), "sha256": hashlib.sha256("\n".join(canonical).encode()).hexdigest()}


def tiles():
    """Fifty dashboard tiles over snapshots: (name, DuckDB SQL with placeholders)."""
    result = []
    for t in range(1, 9):
        result.append((f"tenant{t}_daily_gmv", "SELECT CAST(created_at AS DATE) d, CAST(COUNT(*) AS BIGINT) n, CAST(SUM(amount_cents) AS BIGINT) gmv "
                       f"FROM {{orders}} WHERE tenant_id = {t} AND created_at >= TIMESTAMP '2025-12-01' GROUP BY 1"))
    for c in range(1, 13):
        result.append((f"carrier{c}_weekly_breach", "SELECT CAST(date_trunc('week', promised_at) AS DATE) w, CAST(COUNT(*) AS BIGINT) n, "
                       "CAST(COUNT(*) FILTER (WHERE delivered_at IS NULL OR delivered_at > promised_at) AS BIGINT) late "
                       f"FROM {{shipments}} WHERE carrier_id = {c} AND promised_at >= TIMESTAMP '2025-10-01' GROUP BY 1"))
    for e in ("created", "assigned", "picked", "in_transit", "out_for_delivery", "attempted", "delivered"):
        result.append((f"hot_{e}_hourly", "SELECT CAST(EXTRACT(HOUR FROM event_time) AS BIGINT) h, CAST(COUNT(*) AS BIGINT) n "
                       f"FROM {{events_hot}} WHERE event_type = '{e}' AND event_time >= TIMESTAMP '2025-12-30' GROUP BY 1"))
    for p in ("cod", "upi", "card"):
        for s in ("completed", "returned", "cancelled"):
            result.append((f"{p}_{s}_nov", "SELECT CAST(COUNT(*) AS BIGINT) n, CAST(SUM(amount_cents) AS BIGINT) cents FROM {orders} "
                           f"WHERE payment_method = '{p}' AND status = '{s}' AND created_at >= TIMESTAMP '2025-11-01' AND created_at < TIMESTAMP '2025-12-01'"))
    for k in ("fashion", "grocery", "electronics", "home", "beauty", "books"):
        result.append((f"{k}_top_skus", "SELECT sku_id, CAST(SUM(qty) AS BIGINT) units FROM {order_items} "
                       f"WHERE category = '{k}' AND order_id > 19178083 GROUP BY 1 ORDER BY units DESC, sku_id LIMIT 5"))
    for z in ("north", "south", "east", "west"):
        result.append((f"zone_{z}_orders_dec", "SELECT w.city_id, CAST(COUNT(*) AS BIGINT) n FROM {orders} o JOIN {warehouses} w ON w.warehouse_id = o.warehouse_id "
                       f"WHERE w.zone = '{z}' AND o.created_at >= TIMESTAMP '2025-12-01' GROUP BY 1"))
    for g in ("retail", "prime", "b2b"):
        result.append((f"segment_{g}_customers", f"SELECT city_id, CAST(COUNT(*) AS BIGINT) n FROM {{customers}} WHERE segment = '{g}' GROUP BY 1"))
    result.append(("kpi_sellers_by_category", "SELECT category, CAST(COUNT(*) AS BIGINT) n FROM {sellers} GROUP BY 1"))
    assert len(result) == 50 and len({name for name, _ in result}) == 50
    return result


def spec():
    return yaml.safe_load(WORKFLOWS.read_text())


# ---------------------------------------------------------------- references

def references(data):
    import duckdb
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='8GB'")
    files = {t: f"read_parquet('{data / (t + '.parquet')}')" for t in TABLES}
    out, public = {}, {}
    doc = spec()
    jobs = [(name, w["sql"], w.get("reference_filter", {})) for name, w in doc["workflows"].items()]
    jobs += [("tile:" + name, sql, {}) for name, sql in tiles()]
    jobs.append(("export:shipments_week", EXPORT_SQL, {}))
    jobs.append(("probe:tenant_b", PROBE_B, {}))
    for name, sql, filters in jobs:
        mapping = dict(files)
        for table, predicate in filters.items():
            mapping[table] = f"(SELECT * FROM {files[table]} WHERE {predicate})"
        started = time.perf_counter()
        rows = con.execute(bind(sql, mapping)).fetchall()
        out[name] = digest(rows)
        public[name] = dict(out[name], reference_seconds=round(time.perf_counter() - started, 3))
        print(json.dumps({"reference": name, **public[name]}), flush=True)
    PRIVATE.mkdir(mode=0o700, exist_ok=True)
    (PRIVATE / "references.json").write_text(json.dumps(out, indent=2))
    return {"engine": "duckdb " + duckdb.__version__, "scope": "Direct DuckDB over the generator Parquet, the same rows loaded into each source; outside all timings.",
            "answers": public}


EXPORT_SQL = ("SELECT shipment_id, carrier_id, rider_id, promised_at, delivered_at, status FROM {shipments} "
              "WHERE promised_at >= TIMESTAMP '2025-12-20' AND promised_at < TIMESTAMP '2025-12-27'")
PROBE_B = "SELECT carrier_id, CAST(COUNT(*) AS BIGINT) n FROM {shipments} WHERE promised_at >= TIMESTAMP '2025-12-28' GROUP BY 1"


# ---------------------------------------------------------------- client

class Client:
    def __init__(self):
        from federation_capacity_cluster import DIR
        manifest = json.loads((DIR / "capacity-manifest.json").read_text())
        self.gateways = manifest["gateway_urls"]
        self.keys = json.loads((DIR / "pipeline-client-keys.json").read_text())
        context = ssl.create_default_context(cafile=manifest["ca_file"])
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context))
        self.manifest = manifest

    def call(self, tenant, principal, gateway, path, body=None, timeout=900):
        request = urllib.request.Request(self.gateways[gateway] + path, data=None if body is None else json.dumps(body).encode(),
                                         headers={"Authorization": "Bearer " + self.keys[tenant][principal], "Content-Type": "application/json"})
        from federation_cluster_load import Failure, safe_code
        try:
            return self.opener.open(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            try:
                code = safe_code(json.loads(error.read(65536)).get("error", {}).get("code"), "HTTP_ERROR")
            except (ValueError, AttributeError):
                code = "HTTP_ERROR"
            raise Failure(code, error.code) from None

    def query(self, tenant, principal, body, gateway=0, slow_reader=None, retries=0):
        """Submit on one gateway, read on the other. Returns a result record."""
        import pyarrow as pa
        from federation_cluster_load import Failure
        record = {"tenant": tenant, "principal": principal, "refusals": 0, "ok": False}
        started = time.perf_counter()
        identifier = None
        try:
            while True:
                try:
                    with self.call(tenant, principal, gateway, "/v1/queries", body) as response:
                        identifier = json.load(response)["id"]
                    break
                except Failure as failure:
                    if failure.status == 429 and record["refusals"] < retries:
                        record["refusals"] += 1
                        time.sleep(0.25 * 2 ** min(record["refusals"], 4) * random.random())
                        continue
                    raise
            data = bytearray()
            with self.call(tenant, principal, 1 - gateway, f"/v1/queries/{identifier}/results") as response:
                while block := response.read(1024 if slow_reader else 1 << 16):
                    data += block
                    if slow_reader:
                        time.sleep(slow_reader)
                    if len(data) > 256 << 20:
                        raise Failure("RESULT_TOO_LARGE")
            if not bytes(data).endswith(EOS):
                raise Failure("MISSING_ARROW_EOS")
            table = pa.ipc.open_stream(io.BytesIO(bytes(data))).read_all()
            record["latency_seconds"] = time.perf_counter() - started
            record["result_bytes"] = len(data)
            record["answer"] = digest([tuple(r.values()) for r in table.to_pylist()])
            try:
                with self.call(tenant, principal, gateway, f"/v1/queries/{identifier}") as response:
                    status = json.load(response)
                record["state"], record["stats"] = status.get("state"), status.get("stats", {})
            except Failure as failure:
                record["status_unavailable"] = failure.status
            record["ok"] = True
        except Failure as failure:
            record.update(error=failure.code, http_status=failure.status)
        except (OSError, ValueError, KeyError) as error:
            record.update(error="CLIENT_" + type(error).__name__)
        except Exception as error:  # pyarrow decode errors
            record.update(error="DECODE_" + type(error).__name__)
        finally:
            record.setdefault("latency_seconds", time.perf_counter() - started)
            if identifier and not record["ok"]:
                record.update(self.status_error(tenant, principal, gateway, identifier))
                try:
                    self.call(tenant, principal, gateway, f"/v1/queries/{identifier}/cancel", {}, timeout=5).close()
                except Exception:
                    pass
        return record

    def status_error(self, tenant, principal, gateway, identifier):
        try:
            with self.call(tenant, principal, gateway, f"/v1/queries/{identifier}", timeout=10) as response:
                status = json.load(response)
            return {"state": status.get("state"), "upstream_error": (status.get("error") or {}).get("code"),
                    "upstream_message": ((status.get("error") or {}).get("message") or "")[:200]}
        except Exception:
            return {}


def check(record, expected):
    if record["ok"] and record["answer"] != expected:
        record.update(ok=False, error="EXACT_REFERENCE_MISMATCH")
    return record


def scan_totals(stats):
    rows = sum(item.get("rows_fetched", 0) for item in stats.get("federation", []) or [])
    nbytes = sum(item.get("arrow_bytes_fetched", 0) for item in stats.get("federation", []) or [])
    return rows, nbytes


def summary_stats(stats):
    """Keep scalar stats and federation scan totals; drop bulky lists."""
    keep = {k: v for k, v in stats.items() if not isinstance(v, (list, dict))}
    for key in ("accelerations", "resources", "spill", "temp", "phases"):
        if isinstance(stats.get(key), (dict, list)):
            keep[key] = stats[key]
    keep["rows_fetched"], keep["arrow_bytes_fetched"] = scan_totals(stats)
    return keep


# ---------------------------------------------------------------- validate

def validate(client, fixture, runs, only=None):
    from federation_cluster_load import Sampler
    doc, refs = spec(), json.loads((PRIVATE / "references.json").read_text())
    database = json.loads(Path(fixture).read_text())["database"]
    results = []
    for name, w in doc["workflows"].items():
        principal = w.get("principal", "ops")
        modes = []
        if "native" in w:
            modes.append(("native", {"mode": "native", "connection_id": w["native"]["source"], "sql": w["native"]["sql"].replace("{db}", "`" + database + "`")}))
        modes.append(("federated", {"mode": "federated", "sources": sources_of(w["sql"], FEDERATED), "sql": bind(w["sql"], FEDERATED)}))
        if w.get("snapshot", True):
            modes.append(("snapshot", {"mode": "federated", "sources": sources_of(w["sql"], SNAPSHOT), "sql": bind(w["sql"], SNAPSHOT)}))
        for mode, body in modes:
            if only and mode not in only:
                continue
            for run in range(runs):
                sampler = Sampler(fixture, interval=0.2)
                sampler.start()
                record = check(client.query("a", principal, body, gateway=run % 2), refs[name])
                resources = sampler.stop()
                stats = record.pop("stats", {}) or {}
                record.update(workflow=name, mode=mode, run=run, stats=summary_stats(stats),
                              largest_query_worker_rss_bytes=resources["largest_query_worker_rss_bytes"],
                              source_cpu_seconds={k: round(v, 2) for k, v in resources["sampled_cpu_core_seconds_by_role"].items() if k.startswith("source_")},
                              peak_rss_bytes_by_role=resources["simultaneous_peak_rss_bytes_by_role"])
                record["expected"] = refs[name]
                print(json.dumps({k: record.get(k) for k in ("workflow", "mode", "run", "ok", "error", "upstream_message", "latency_seconds")}), flush=True)
                results.append(record)
    denials = []
    for name, d in doc["denials"].items():
        record = client.query(d.get("tenant", "a"), d["principal"], {"mode": "federated", "sources": d["sources"], "sql": d["sql"]})
        denials.append({"check": name, "refused": not record["ok"], "error": record.get("error"), "http_status": record.get("http_status"),
                        "upstream_error": record.get("upstream_error")})
        print(json.dumps(denials[-1]), flush=True)
    return {"runs_per_mode": runs, "results": results, "denials": denials}


# ---------------------------------------------------------------- fan-out

def dashboard_body(sql):
    return {"mode": "federated", "sources": sources_of(sql, SNAPSHOT), "sql": bind(sql, SNAPSHOT)}


def fanout(client, levels, per_viewer, retries):
    refs = json.loads((PRIVATE / "references.json").read_text())
    board = tiles()
    report = []
    for viewers in levels:
        records, loads = [], []
        lock = threading.Lock()
        started = time.perf_counter()

        def viewer(index):
            load_started = time.perf_counter()
            with ThreadPoolExecutor(per_viewer) as pool:
                out = list(pool.map(lambda tile: dict(check(client.query("a", "ops", dashboard_body(tile[1]), gateway=index % 2, retries=retries),
                                                              refs["tile:" + tile[0]]), tile=tile[0]), board))
            with lock:
                records.extend(out)
                loads.append(time.perf_counter() - load_started)

        with ThreadPoolExecutor(viewers) as pool:
            list(pool.map(viewer, range(viewers)))
        elapsed = time.perf_counter() - started
        report.append(level_summary(viewers, per_viewer, retries, records, loads, elapsed))
        print(json.dumps(report[-1]), flush=True)
    return report


def level_summary(viewers, per_viewer, retries, records, loads, elapsed):
    from federation_cluster_load import percentile
    ok = [r["latency_seconds"] for r in records if r["ok"]]
    return {"viewers": viewers, "tiles_per_dashboard": 50, "per_viewer_parallel_tiles": per_viewer, "retry_budget_on_429": retries,
            "queries": len(records), "succeeded": len(ok), "failed": len(records) - len(ok),
            "error_classes": dict(Counter(r.get("error") for r in records if not r["ok"])),
            "admission_refusals_retried": sum(r["refusals"] for r in records),
            "elapsed_seconds": round(elapsed, 3), "queries_per_second": round(len(ok) / elapsed, 3),
            "tile_p50_seconds": percentile(ok, .5), "tile_p95_seconds": percentile(ok, .95), "tile_p99_seconds": percentile(ok, .99),
            "dashboard_load_p50_seconds": percentile(loads, .5), "dashboard_load_max_seconds": max(loads) if loads else None}


# ---------------------------------------------------------------- soak

class Faults:
    """Inject faults against owned fixture processes only (recorded identities)."""

    def __init__(self):
        import federation_capacity_cluster as fcc
        self.fcc = fcc

    def record(self, name):
        return next(item for item in self.fcc.process_records() if item["name"] == name and self.fcc.matches(item))

    def kill_query_worker(self):
        nodes = [item["pid"] for item in self.fcc.process_records() if item["name"] in ("a1", "a2") and self.fcc.matches(item)]
        for _ in range(600):
            children = [c["pid"] for pid in nodes for c in self.fcc.descendants(pid)]
            if children:
                os.kill(random.choice(children), signal.SIGKILL)
                return {"killed": "query_worker"}
            time.sleep(0.1)
        return {"killed": None}

    def restart_broker(self, name="nats-1"):
        item = self.record(name)
        os.kill(item["pid"], signal.SIGTERM)
        while self.fcc.matches(item):
            time.sleep(0.1)
        directory = self.fcc.DIR
        with open(directory / (name + ".log"), "ab") as log:
            process = subprocess.Popen([str(directory / "nats-server"), "-c", str(directory / (name + ".conf"))], stdout=log, stderr=log, start_new_session=True)
        time.sleep(0.5)
        self.fcc.save_process(name, process.pid)
        return {"restarted": name}

    def kill_node(self, name="a2"):
        item = self.record(name)
        os.kill(item["pid"], signal.SIGKILL)
        return {"killed": name}

    def start_node(self, name="a2"):
        directory = self.fcc.DIR
        environment = json.loads((directory / (name + "-environment.json")).read_text())
        wrapper = json.loads((PRIVATE / "tenant-sources.json").read_text())["trust_wrapper"]
        command = [self.fcc.manifest()["binary"], "node", "--config", str(directory / (name + ".yml"))]
        for attempt in range(20):  # the old worker lease must expire first
            try:
                self.fcc.launch(name, [wrapper, "--env-file", str(directory / (name + "-environment.json"))] + command, environment, command)
                return {"restarted": name, "attempts": attempt + 1}
            except RuntimeError:
                time.sleep(3)
        return {"restarted": None}


def soak(client, seconds, fixture):
    from federation_cluster_load import Sampler, percentile
    doc, refs = spec(), json.loads((PRIVATE / "references.json").read_text())
    board = tiles()
    workflows = [(n, w) for n, w in doc["workflows"].items() if n not in ("cohort_retention", "cod_rto_by_segment")]
    records, events = [], []
    lock, stop = threading.Lock(), threading.Event()
    origin = time.perf_counter()

    def add(kind, record):
        record.update(kind=kind, at=round(time.perf_counter() - origin, 3))
        record.pop("stats", None)
        with lock:
            records.append(record)

    def dashboard(index):
        while not stop.is_set():
            name, sql = random.choice(board)
            add("tile", check(client.query("a", "ops", dashboard_body(sql), gateway=index % 2, retries=3), refs["tile:" + name]))

    def workflow():
        while not stop.is_set():
            name, w = random.choice(workflows)
            mode = random.choice(["federated", "snapshot"] if w.get("snapshot", True) else ["federated"])
            mapping = FEDERATED if mode == "federated" else SNAPSHOT
            body = {"mode": "federated", "sources": sources_of(w["sql"], mapping), "sql": bind(w["sql"], mapping)}
            add("workflow:" + mode, check(client.query("a", w.get("principal", "ops"), body, retries=3), refs[name]))

    def export():
        while not stop.is_set():
            body = {"mode": "federated", "sources": ["shipments_snap"], "sql": bind(EXPORT_SQL, SNAPSHOT)}
            add("export", check(client.query("a", "ops", body, gateway=1, retries=3), refs["export:shipments_week"]))
            stop.wait(5)

    def tenant_b():
        while not stop.is_set():
            body = {"mode": "federated", "sources": ["my"], "sql": bind(PROBE_B, FEDERATED)}
            add("tenant_b", check(client.query("b", "ops", body, retries=3), refs["probe:tenant_b"]))
            stop.wait(2)

    def refresher():
        # Scheduled refreshes (5m/10m datasets) run on the nodes; observe the store.
        while not stop.wait(300):
            subprocess_refresh(events, origin)

    faults = Faults()
    third = seconds / 8
    schedule = [(1 * third, "kill_query_worker"), (2 * third, "restart_broker"), (3 * third, "kill_node"),
                (3 * third + 60, "start_node"), (4 * third, "slow_client"), (5 * third, "kill_query_worker"),
                (6 * third, "restart_broker")]

    def injector():
        for at, action in schedule:
            if stop.wait(max(0, at - (time.perf_counter() - origin))):
                return
            started = time.perf_counter() - origin
            if action == "slow_client":
                body = {"mode": "federated", "sources": ["shipments_snap"], "sql": bind(EXPORT_SQL, SNAPSHOT)}
                outcome = check(client.query("a", "ops", body, slow_reader=0.02, retries=3), refs["export:shipments_week"])
                result = {"ok": outcome["ok"], "error": outcome.get("error"), "seconds": round(outcome["latency_seconds"], 1)}
            else:
                try:
                    result = getattr(faults, action)()
                except Exception as error:
                    result = {"injection_error": type(error).__name__}
            events.append({"fault": action, "at": round(started, 3), **result})
            print(json.dumps(events[-1]), flush=True)

    sampler = Sampler(fixture, interval=1.0)
    sampler.start()
    threads = [threading.Thread(target=dashboard, args=(i,)) for i in range(4)]
    threads += [threading.Thread(target=f) for f in (workflow, export, tenant_b, injector, refresher)]
    for thread in threads:
        thread.start()
    while not stop.wait(60):
        elapsed = time.perf_counter() - origin
        with lock:
            done = len(records)
            failed = sum(not r["ok"] for r in records)
        print(json.dumps({"elapsed": round(elapsed), "queries": done, "failed": failed}), flush=True)
        if elapsed >= seconds:
            stop.set()
    for thread in threads:
        thread.join()
    resources = sampler.stop()
    return soak_summary(records, events, seconds, resources, percentile)


def subprocess_refresh(events, origin):
    """Record the snapshot store tree; scheduled refreshes add generations."""
    directory = PRIVATE / "acceleration" / "a"
    generations = sorted(str(p.relative_to(directory)) for p in directory.rglob("*") if p.is_dir())
    events.append({"observation": "acceleration_tree", "at": round(time.perf_counter() - origin, 3), "directories": len(generations)})


def soak_summary(records, events, seconds, resources, percentile):
    kinds = sorted({r["kind"] for r in records})
    faults = [e for e in events if "fault" in e]
    for fault in faults:
        after = [r for r in records if r["at"] >= fault["at"]]
        failed_after = [r for r in after if not r["ok"] and r["at"] <= fault["at"] + 120]
        first_ok = next((r["at"] for r in sorted(after, key=lambda r: r["at"]) if r["ok"]), None)
        fault["failures_within_120s"] = len(failed_after)
        fault["failure_classes_within_120s"] = dict(Counter(r.get("error") for r in failed_after))
        last_fail = max((r["at"] for r in failed_after), default=None)
        fault["last_failure_completion_after_fault_seconds"] = None if last_fail is None else round(last_fail - fault["at"], 1)
        fault["first_success_after_fault_seconds"] = None if first_ok is None else round(first_ok - fault["at"], 1)
        tail = [r for r in records if r["at"] > fault["at"] + 180]
        fault["exact_answers_after_recovery"] = {"checked": len(tail), "mismatches": sum(r.get("error") == "EXACT_REFERENCE_MISMATCH" for r in tail)}
    by_kind = {}
    for kind in kinds:
        group = [r for r in records if r["kind"] == kind]
        ok = [r["latency_seconds"] for r in group if r["ok"]]
        by_kind[kind] = {"queries": len(group), "succeeded": len(ok), "failure_rate": round(1 - len(ok) / len(group), 5),
                         "p50_seconds": percentile(ok, .5), "p95_seconds": percentile(ok, .95), "p99_seconds": percentile(ok, .99),
                         "error_classes": dict(Counter(r.get("error") for r in group if not r["ok"]))}
    return {"duration_seconds": seconds, "queries": len(records), "succeeded": sum(r["ok"] for r in records),
            "failure_rate": round(1 - sum(r["ok"] for r in records) / max(len(records), 1), 5),
            "mismatches": sum(r.get("error") == "EXACT_REFERENCE_MISMATCH" for r in records),
            "refusals_retried": sum(r["refusals"] for r in records), "by_kind": by_kind, "faults": faults,
            "observations": [e for e in events if "observation" in e], "resources": resources}


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str) + "\n")
    print(str(path))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=("references", "validate", "fanout", "soak"))
    parser.add_argument("--data", type=Path)
    parser.add_argument("--fixture", type=Path, help="relational fixture manifest.json (source container sampling)")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--runs", type=int, default=2)
    parser.add_argument("--modes", help="comma list of native,federated,snapshot (default all)")
    parser.add_argument("--levels", default="1,5,20,50")
    parser.add_argument("--per-viewer", type=int, default=6)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=7200)
    args = parser.parse_args()
    random.seed(20261027)
    if args.action == "references":
        write(args.report, references(args.data.resolve()))
        return
    client = Client()
    meta = {"binary_sha256": client.manifest.get("binary_sha256"), "started_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
    if args.action == "validate":
        result = validate(client, args.fixture, args.runs, args.modes and args.modes.split(","))
    elif args.action == "fanout":
        result = {"levels": fanout(client, [int(x) for x in args.levels.split(",")], args.per_viewer, args.retries)}
    else:
        result = soak(client, args.seconds, args.fixture)
    write(args.report, {**meta, "finished_utc": dt.datetime.now(dt.timezone.utc).isoformat(), **result})


if __name__ == "__main__":
    main()
