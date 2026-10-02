#!/usr/bin/env python3
"""Bounded real NYC TLC dataset preparation and federation capacity trials.

Run on the designated Linux VM only. This does not build software or modify an
existing service. Dataset files, owned tables and private credentials are kept
for the coordinating tenant/relational harnesses until explicit cleanup.
"""
from __future__ import annotations

import argparse
import base64
import copy
import csv
from datetime import datetime, timezone
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import secrets
import signal
import shutil
import socket
import struct
import subprocess
import sys
import time
import threading
import urllib.error
import urllib.request
from urllib.parse import urlsplit
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from federation_acceptance import private_directory, private_write, require, wait_until
from benchmark_clickhouse import sha256_file
from benchmark_clickhouse import children_of, read_rss_kib


ROOT = Path(__file__).resolve().parents[1]
PRIVATE = ROOT / "artifacts/capacity-private"
STATE = PRIVATE / "state.json"
PROVENANCE = ROOT / "docs/evidence/federation-capacity-provenance.json"
SOURCE_ROOT = "https://d37ci6vzurychx.cloudfront.net"
FILES = [
    (201901, "yellow_tripdata_2019-01.parquet", 7696617, 110439634),
    (201902, "yellow_tripdata_2019-02.parquet", 7049370, 103356025),
    (201903, "yellow_tripdata_2019-03.parquet", 7866620, 116017372),
]
TOTAL_ROWS = sum(row[2] for row in FILES)
BATCH_ROWS = 65536
SAMPLE_ROWS = 1_000_000
SCHEMA = pa.schema([
    pa.field("trip_id", pa.int64(), nullable=False),
    pa.field("source_month", pa.int32(), nullable=False),
    pa.field("source_row", pa.int32(), nullable=False),
    pa.field("pickup_unix_us", pa.int64()),
    pa.field("pickup_zone_id", pa.int32()),
    pa.field("dropoff_zone_id", pa.int32()),
    pa.field("passenger_count", pa.float64()),
    pa.field("trip_distance", pa.float64()),
    pa.field("total_amount", pa.float64()),
    pa.field("fare_cents", pa.int64()),
])
SAMPLE_SCHEMA = pa.schema([SCHEMA.field(name) for name in
                          ("trip_id", "pickup_zone_id", "fare_cents")])
RAW_COLUMNS = ["tpep_pickup_datetime", "PULocationID", "DOLocationID",
               "passenger_count", "trip_distance", "total_amount"]

QUERIES = {
    "zone_join": {
        "sql": "SELECT z.borough, CAST(COUNT(*) AS BIGINT) AS trips, CAST(SUM(t.fare_cents) AS BIGINT) AS fare_cents FROM taxi.trips t INNER JOIN taxi.zones z ON t.pickup_zone_id=z.zone_id GROUP BY z.borough ORDER BY z.borough",
        "native": "SELECT z.borough, toInt64(count()) AS trips, toInt64(sum(t.fare_cents)) AS fare_cents FROM {db}.trips t INNER JOIN {db}.zones z ON t.pickup_zone_id=z.zone_id GROUP BY z.borough ORDER BY z.borough",
        "scope": "22.6M real fact rows joined to a 265-row real taxi-zone dimension; physical hash-build choice was not captured",
    },
    "large_hash_join": {
        "sql": "SELECT s.pickup_zone_id AS pickup_zone_id, CAST(COUNT(*) AS BIGINT) AS trips, CAST(SUM(t.fare_cents) AS BIGINT) AS fare_cents FROM taxi.trips t INNER JOIN taxi.taxi_sample s ON t.trip_id=s.trip_id GROUP BY s.pickup_zone_id ORDER BY s.pickup_zone_id NULLS FIRST",
        "native": "SELECT s.pickup_zone_id AS pickup_zone_id, toInt64(count()) AS trips, toInt64(sum(t.fare_cents)) AS fare_cents FROM {db}.trips t INNER JOIN {db}.taxi_sample s ON t.trip_id=s.trip_id GROUP BY s.pickup_zone_id ORDER BY s.pickup_zone_id NULLS FIRST",
        "scope": "22.6M real fact relation with a deterministic 1M-row joined relation projected from original records; physical hash-build choice was not captured",
    },
    "ordered_million": {
        "sql": "SELECT trip_id,pickup_zone_id,fare_cents FROM taxi.taxi_sample ORDER BY trip_id",
        "native": "SELECT trip_id,pickup_zone_id,fare_cents FROM {db}.taxi_sample ORDER BY trip_id",
        "scope": "Ordered export of the 1M-row real January projection",
    },
    "three_adapter_join": {
        "sql": "SELECT z.borough, CAST(COUNT(*) AS BIGINT) AS trips, CAST(SUM(t.fare_cents) AS BIGINT) AS fare_cents FROM taxi.trips t INNER JOIN pg.taxi_sample s ON t.trip_id=s.trip_id INNER JOIN my.zones z ON s.pickup_zone_id=z.zone_id GROUP BY z.borough ORDER BY z.borough",
        "native": "SELECT z.borough, toInt64(count()) AS trips, toInt64(sum(t.fare_cents)) AS fare_cents FROM {db}.trips t INNER JOIN {db}.taxi_sample s ON t.trip_id=s.trip_id INNER JOIN {db}.zones z ON s.pickup_zone_id=z.zone_id GROUP BY z.borough ORDER BY z.borough",
        "scope": "ClickHouse22.6M real fact, PostgreSQL1M real projection and MySQL265 real taxi zones; same copied relations in native reference",
    },
    "full_external_sort": {
        "sql": "SELECT trip_id,pickup_zone_id,fare_cents FROM taxi.trips ORDER BY pickup_zone_id NULLS FIRST,fare_cents NULLS FIRST,trip_id",
        "native": "SELECT trip_id,pickup_zone_id,fare_cents FROM {db}.trips ORDER BY pickup_zone_id NULLS FIRST,fare_cents NULLS FIRST,trip_id",
        "scope": "Full 22.6M-row ORDER BY and Arrow export; no top-N shortcut",
    },
}


def canonical_arrow(path):
    """Value digest independent of Arrow chunking and signed integer widths."""
    with path.open("rb") as complete:
        complete.seek(-8, os.SEEK_END)
        require(complete.read() == b"\xff\xff\xff\xff\0\0\0\0", "Arrow result lacks a successful EOS marker")
    hashes, rows, sums, nulls, physical = {}, 0, {}, {}, None
    with path.open("rb") as stream, pa.ipc.open_stream(stream) as reader:
        physical = [(field.name, str(field.type)) for field in reader.schema]
        for field in reader.schema:
            hashes[field.name] = {"values": hashlib.sha256(), "validity": hashlib.sha256(),
                                  "lengths": hashlib.sha256()}
            sums[field.name], nulls[field.name] = 0, 0
        for batch in reader:
            rows += batch.num_rows
            for field, column in zip(batch.schema, batch.columns):
                target = hashes[field.name]
                nulls[field.name] += column.null_count
                valid = pc.cast(pc.is_valid(column), pa.uint8())
                target["validity"].update(memoryview(valid.buffers()[1])[valid.offset:valid.offset + len(valid)])
                if pa.types.is_integer(column.type) or pa.types.is_decimal(column.type):
                    integer = pc.fill_null(pc.cast(column, pa.int64(), safe=True), 0)
                    target["values"].update(memoryview(integer.buffers()[1])[
                        integer.offset * 8:(integer.offset + len(integer)) * 8])
                    sums[field.name] += int(pc.sum(integer).as_py() or 0)
                elif pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
                    sums.pop(field.name, None)
                    for value in column.to_pylist():
                        data = (value or "").encode()
                        target["lengths"].update(struct.pack("<Q", len(data)))
                        target["values"].update(data)
                else:
                    raise AssertionError("capacity reference contains an unsupported canonical type")
    fields = [{"name": name, **{key: value.hexdigest() for key, value in digest.items()}}
              for name, digest in hashes.items()]
    checksum = hashlib.sha256(json.dumps({"rows": rows, "fields": fields},
        separators=(",", ":")).encode()).hexdigest()
    return {"rows": rows, "canonical_value_sha256": checksum, "integer_sums": sums,
            "null_counts": nulls, "physical_arrow_schema": physical,
            "arrow_ipc_bytes": path.stat().st_size, "arrow_ipc_sha256": sha256_file(path)}


def native_references():
    state = json.loads(STATE.read_text())
    directory = private_directory(PRIVATE / "references")
    results = {}
    for name, spec in QUERIES.items():
        sql = spec["native"].format(db="`" + state["database"] + "`")
        key = hashlib.sha256((sql + sha256_file(PROVENANCE)).encode()).hexdigest()[:12]
        path = directory / (name + "-" + key + ".arrow")
        if not path.exists():
            temporary = path.with_suffix(".partial")
            with temporary.open("wb") as output:
                proc = subprocess.run(["sudo", "-n", "docker", "exec", "kelvo-clickhouse",
                    "clickhouse-client", "--max_threads", "2", "--max_memory_usage", str(2 << 30),
                    "--max_bytes_before_external_sort", str(256 << 20),
                    "--query", sql + " FORMAT ArrowStream"], stdout=output, stderr=subprocess.PIPE, timeout=240)
            if proc.returncode:
                private_write(directory / (name + ".log"), proc.stderr)
                raise AssertionError("native capacity reference query failed")
            temporary.replace(path)
        results[name] = {"scope": spec["scope"], "native_sql": spec["native"].format(db="capacity_fixture"),
                         "duckdb_sql": spec["sql"], **canonical_arrow(path)}
        print(json.dumps({"stage": "native_reference_verified", "query": name,
                          "rows": results[name]["rows"], "sha256": results[name]["canonical_value_sha256"]}), flush=True)
    report = {"dataset_provenance_sha256": sha256_file(PROVENANCE), "queries": results,
              "canonical_digest": "Per column, concatenate little-endian Int64 values (nulls zero-filled), per-row UInt8 validity, and UTF-8 strings with UInt64 byte lengths; hash each stream, then hash ordered column descriptors and row count. Integers of differing signed physical Arrow widths compare by exact value. No Float64 aggregate is compared as an exact integer.",
              "native_reference_scope": "Direct ClickHouse query over the same imported real records and identical copied dimensions. Reference computation/validation is outside all Kelvo timings."}
    write_json(ROOT / "docs/evidence/federation-capacity-references.json", report, private=False)
    write_json(PRIVATE / "reference-results.json", results)


class ProcessMetrics:
    """50ms /proc samples; distinct coordinator/worker RSS, CPU and temp files."""

    def __init__(self, parent_pid, scratch):
        self.pid, self.scratch = parent_pid, scratch
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.peaks, self.cpu_first, self.cpu_last, self.roles = {}, {}, {}, {}
        self.temporary_peak, self.allocated_peak, self.samples = 0, 0, 0
        self.observed = {parent_pid}

    def run(self):
        while not self.stop_event.is_set():
            pending, inspected = list(self.observed), set()
            while pending:
                pid = pending.pop()
                if pid in inspected:
                    continue
                inspected.add(pid)
                try:
                    children = children_of(pid)
                except OSError:
                    children = set()
                self.observed.update(children)
                pending.extend(children - inspected)
            for pid in list(self.observed):
                rss = read_rss_kib(pid)
                if rss is not None:
                    self.peaks[pid] = max(self.peaks.get(pid, 0), rss)
                try:
                    arguments = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
                    if len(arguments) > 1 and arguments[1] in (b"query", b"serve", b"worker"):
                        self.roles[pid] = "worker" if arguments[1] == b"worker" else "coordinator"
                    fields = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()
                    ticks = int(fields[11]) + int(fields[12])
                    self.cpu_first.setdefault(pid, ticks if pid == self.pid else 0)
                    self.cpu_last[pid] = ticks
                except (OSError, ValueError, IndexError):
                    pass
            logical = allocated = 0
            for root, _, files in os.walk(self.scratch):
                for name in files:
                    try:
                        info = (Path(root) / name).stat()
                        logical += info.st_size
                        allocated += info.st_blocks * 512
                    except FileNotFoundError:
                        pass
            self.temporary_peak = max(self.temporary_peak, logical)
            self.allocated_peak = max(self.allocated_peak, allocated)
            self.samples += 1
            self.stop_event.wait(0.05)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join()
        workers = {pid: rss for pid, rss in self.peaks.items() if self.roles.get(pid) == "worker"}
        coordinators = [rss for pid, rss in self.peaks.items() if self.roles.get(pid) == "coordinator"]
        return {"coordinator_peak_rss_kib": max(coordinators, default=None),
                "worker_peak_rss_kib_max": max(workers.values(), default=None),
                "observed_worker_processes": len(workers),
                "sampled_cpu_core_seconds": sum(self.cpu_last[p] - first for p, first in self.cpu_first.items()) / os.sysconf("SC_CLK_TCK"),
                "temporary_logical_bytes_peak": self.temporary_peak,
                "temporary_allocated_bytes_peak": self.allocated_peak,
                "sample_interval_seconds": 0.05, "samples": self.samples,
                "sampling_limit": "Sampled process RSS and scratch file peaks are lower bounds; memory_mb is not a hard whole-process RSS cap. Scratch excludes final Arrow exports and original dataset files."}


class SourceProxy(ThreadingHTTPServer):
    """Test-only fixed-destination HTTP relay; optional application WAN emulator."""
    daemon_threads = True

    def __init__(self, endpoint, rtt_ms=0, bytes_per_second=0):
        upstream = urlsplit(endpoint)
        require(upstream.scheme == "http" and upstream.hostname and not upstream.username,
                "capacity relay requires its dedicated fixed HTTP endpoint")
        self.upstream_host, self.upstream_port = upstream.hostname, upstream.port or 8123
        self.rtt_ms, self.rate = rtt_ms, bytes_per_second
        self.lock = threading.Lock()
        self.events, self.active, self.next_transfer = [], 0, time.monotonic()
        super().__init__(("127.0.0.1", 0), SourceHandler)
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def pace(self, count):
        if not self.rate:
            return
        with self.lock:
            scheduled = max(time.monotonic(), self.next_transfer)
            self.next_transfer = scheduled + count / self.rate
        delay = scheduled - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    def mark(self):
        with self.lock:
            return len(self.events)

    def snapshot(self, mark, database):
        with self.lock:
            events = copy.deepcopy(self.events[mark:])
        for event in events:
            event["sql"] = event["sql"].replace(database, "capacity_fixture")
        return events

    def close(self):
        self.shutdown()
        self.server_close()
        self.thread.join()


class SourceHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_POST(self):
        proxy = self.server
        length = self.headers.get("Content-Length", "")
        if not length.isdigit() or not 0 < int(length) <= 1 << 20:
            self.send_error(400)
            return
        sql = self.rfile.read(int(length)).decode()
        event = {"sql": sql, "describe": sql.endswith(" LIMIT 0"), "wire_bytes": 0,
                 "arrow_rows": 0, "arrow_buffer_bytes": 0, "complete": False}
        with proxy.lock:
            proxy.events.append(event)
            proxy.active += 1
        upstream = http.client.HTTPConnection(proxy.upstream_host, proxy.upstream_port, timeout=300)
        try:
            if proxy.rtt_ms:
                time.sleep(proxy.rtt_ms / 2000)
            upstream.request("POST", self.path, body=sql.encode(), headers={
                "Authorization": self.headers.get("Authorization", ""),
                "Content-Type": "text/plain; charset=utf-8"})
            response = upstream.getresponse()
            event["status"] = response.status
            if proxy.rtt_ms:
                time.sleep(proxy.rtt_ms / 2000)
            self.send_response(response.status)
            self.send_header("Connection", "close")
            self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
            self.end_headers()
            self.close_connection = True

            class Relay(io.RawIOBase):
                def readable(inner):
                    return True

                def read(inner, requested=-1):
                    require(0 <= requested <= 16 << 20, "relay IPC request exceeds its bounded read")
                    block = response.read(requested)
                    event["wire_bytes"] += len(block)
                    for offset in range(0, len(block), 65536):
                        part = block[offset:offset + 65536]
                        proxy.pace(len(part))
                        self.wfile.write(part)
                        self.wfile.flush()
                    return block

                def readinto(inner, output):
                    block = inner.read(len(output))
                    output[:len(block)] = block
                    return len(block)

            relay = Relay()
            if response.status == 200:
                with pa.ipc.open_stream(relay) as reader:
                    event["arrow_columns"] = reader.schema.names
                    for batch in reader:
                        event["arrow_rows"] += batch.num_rows
                        event["arrow_buffer_bytes"] += batch.get_total_buffer_size()
                require(not relay.read(1), "source appended bytes after Arrow EOS")
                event["complete"] = True
            else:
                while relay.read(4096):
                    pass
                event["complete"] = True
        except Exception as error:
            event["error_type"] = type(error).__name__
        finally:
            upstream.close()
            with proxy.lock:
                proxy.active -= 1


class Capacity:
    def __init__(self, binary, report):
        require(sys.platform.startswith("linux"), "capacity measurements require the designated Linux VM")
        self.binary = binary.resolve()
        self.binary_hash = sha256_file(self.binary)
        self.report = report
        self.state = json.loads(STATE.read_text())
        self.manifest = json.loads((PRIVATE / "tenant-sources.json").read_text())
        self.references = json.loads((PRIVATE / "reference-results.json").read_text())
        self.directory = private_directory(PRIVATE / ("measure-" + uuid.uuid4().hex))
        self.results = []
        self.index = 0
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.suite_complete = False
        self.fixture_integrity = {}
        cpu_models = [line.split(":", 1)[1].strip() for line in
                      Path("/proc/cpuinfo").read_text().splitlines()
                      if line.startswith("model name")]
        memory = next(line for line in Path("/proc/meminfo").read_text().splitlines()
                      if line.startswith("MemTotal:"))
        self.host = {"logical_cpus": os.cpu_count(), "cpu_model": cpu_models[0] if cpu_models else "unknown",
                     "physical_memory_kib": int(memory.split()[1]),
                     "kernel_release": os.uname().release}

    def verify_fixture(self, phase):
        database = "`" + self.state["database"] + "`"
        counts = {table: int(admin(f"SELECT count() FROM {database}.`{table}`"))
                  for table in ("trips", "taxi_sample", "zones")}
        counts["existing_fact_events"] = int(admin("SELECT coalesce(sum(rows),0) FROM system.parts "
            "WHERE active AND database='kelvo_bench' AND table='fact_events'"))
        require(counts == {"trips": TOTAL_ROWS, "taxi_sample": SAMPLE_ROWS,
                           "zones": 265, "existing_fact_events": 100_000_000},
                "capacity fixture or existing benchmark row counts changed")
        self.fixture_integrity[phase] = counts

    def setup_query(self, sources, proxy=None):
        tenant = self.manifest["tenants"]["a"]
        catalog = {"sources": [source for source in tenant["catalog"]["sources"] if source["id"] in sources]}
        require(len(catalog["sources"]) == len(sources), "capacity source registration is missing")
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "GOMAXPROCS": "2"}
        for source in catalog["sources"]:
            for key in ("dsn_env", "url_env", "username_env", "password_env", "token_env"):
                if source.get(key):
                    env[source[key]] = tenant["environment"][source[key]]
        if proxy:
            env["KELVO_SOURCE_TAXI_URL"] = f"http://127.0.0.1:{proxy.server_port}/"
        return catalog, env

    def cli(self, name, memory_mb=256, proxy=None, sources=None, sql=None, reference=None, threads=2):
        sources = sources or ["taxi"]
        reference = reference or name
        sql = sql or QUERIES[name]["sql"]
        self.index += 1
        directory = private_directory(self.directory / str(self.index))
        scratch = private_directory(directory / "scratch")
        output = directory / "result.arrow"
        config, env = self.setup_query(sources, proxy)
        env["TMPDIR"] = str(scratch)
        config_path, env_path = directory / "catalog.json", directory / "environment.json"
        write_json(config_path, config)
        write_json(env_path, env)
        command = [str(self.binary), "query", "--config", str(config_path),
            "--mode", "federated", "--sources", ",".join(sources), "--sql", sql,
            "--out", str(output), "--max-rows", "25000000", "--max-bytes", str(2 << 30),
            "--memory-mb", str(memory_mb), "--threads", str(threads), "--temp-mb", "4096", "--timeout", "240s"]
        if any(source in ("pg", "my") for source in sources):
            command = [self.manifest["trust_wrapper"], "--env-file", str(env_path), *command]
        mark = proxy.mark() if proxy else 0
        started = time.perf_counter()
        proc = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        sampler = ProcessMetrics(proc.pid, scratch)
        sampler.start()
        timed_out = False
        try:
            stdout, stderr = proc.communicate(timeout=270)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(proc.pid, signal.SIGKILL)
            stdout, stderr = proc.communicate()
        elapsed = time.perf_counter() - started
        metrics = sampler.stop()
        private_write(directory / "cli.log", stdout + b"\n" + stderr)
        result = {"query": name, "sql": sql, "source_ids": sources,
                  "memory_mb": memory_mb, "threads": threads,
                  "scope": QUERIES[reference]["scope"],
                  "completed_at": datetime.now(timezone.utc).isoformat(),
                  "elapsed_seconds": elapsed, "success": proc.returncode == 0, **metrics}
        if proxy:
            deadline = time.monotonic() + 5
            while proxy.active and time.monotonic() < deadline:
                time.sleep(0.01)
            result["source_observations"] = proxy.snapshot(mark, self.state["database"])
            result["emulator"] = {"round_trip_delay_ms": proxy.rtt_ms,
                                  "shared_response_body_bytes_per_second": proxy.rate,
                                  "applies_to_source_ids": ["taxi"],
                                  "unpaced_source_ids": [source for source in sources if source != "taxi"],
                                  "kind": "Isolated loopback application relay; not a geographic WAN or packet-level emulator"}
        if proc.returncode == 0:
            stats = json.loads(stderr)
            result["federation"] = stats.get("federation", [])
            if proxy:
                require(all(entry["complete"] and entry.get("status") == 200
                            for entry in result["source_observations"]),
                        "successful query has an incomplete observed ClickHouse transfer")
                reported_rows = sum(entry["rows_fetched"] for entry in result["federation"]
                                    if entry["source"] == "taxi")
                observed_rows = sum(entry["arrow_rows"] for entry in result["source_observations"]
                                    if not entry["describe"])
                require(reported_rows == observed_rows,
                        "reported ClickHouse rows differ from observed Arrow transfer")
            verified = canonical_arrow(output)
            expected = self.references[reference]
            require(verified["canonical_value_sha256"] == expected["canonical_value_sha256"] and
                    verified["rows"] == expected["rows"], "capacity query changed native reference values")
            result.update(verified)
            result["reference_match"] = True
            result["output_rows_per_second"] = verified["rows"] / elapsed
        else:
            require(not output.exists(), "failed capacity query published an Arrow result")
            # Classify only known evidence. A generic execution failure is not
            # evidence that the configured memory budget was exhausted.
            diagnostic = (stdout + b"\n" + stderr).decode(errors="replace").lower()
            if timed_out:
                failure_class = "measurement_harness_timeout"
            elif any(value in diagnostic for value in ("out of memory", "memory limit exceeded", "failed to pin block")):
                failure_class = "explicit_engine_memory_exhaustion"
            elif "context deadline exceeded" in diagnostic or "query timed out" in diagnostic:
                failure_class = "query_deadline_exceeded"
            elif any(value in diagnostic for value in ("row limit exceeded", "byte limit exceeded", "scan limit exceeded")):
                failure_class = "explicit_result_or_source_budget_exhaustion"
            else:
                failure_class = "execution_failure_requires_diagnosis"
            result.update(failure_class=failure_class, process_exit_code=proc.returncode,
                          private_diagnostic_sha256=sha256_file(directory / "cli.log"),
                          result_export_published=False)
        require(not list(scratch.iterdir()), "capacity query left a worker scratch directory")
        self.results.append(result)
        self.save()
        print(json.dumps({"stage": "capacity_query_finished", "query": name,
                          "memory_mb": memory_mb, "success": result["success"],
                          "elapsed_seconds": elapsed,
                          "spill_allocated_peak": result["temporary_allocated_bytes_peak"]}), flush=True)
        # Large exports are validated and hashed before removal; avoid retaining
        # multiple copies of the 22.6M-row result across capacity settings.
        output.unlink(missing_ok=True)
        return result

    def save(self):
        require(sha256_file(self.binary) == self.binary_hash, "capacity binary changed during measurements")
        write_json(self.report, {"binary_sha256": self.binary_hash,
            "measurement_script_sha256": sha256_file(Path(__file__)),
            "started_at": self.started_at, "suite_complete": self.suite_complete,
            "host": self.host, "fixture_integrity": self.fixture_integrity,
            "dataset_rows": TOTAL_ROWS, "projection_rows": SAMPLE_ROWS,
            "dataset_provenance_sha256": sha256_file(PROVENANCE),
            "runs": self.results,
            "scope": "Complete CLI coordinator, disposable worker, source scans, DuckDB query, durable Arrow export. Original source/reference loading and canonical value validation are outside timed intervals. Source observer decoding and configured application emulation are inside the path when enabled.",
            "limitations": ["Warm/uncontrolled caches; no production throughput claim.",
                "Sampled RSS differs from configured engine memory and excludes remote database/proxy memory.",
                "Application delay/bandwidth emulation does not establish geographic WAN performance, TCP loss or jitter behavior."]}, private=False)

    def run(self):
        self.verify_fixture("before")
        plain = SourceProxy(self.state["source_endpoint"])
        try:
            for memory in (128, 256):
                self.cli("zone_join", memory, plain)
                self.cli("large_hash_join", memory, plain)
                self.cli("full_external_sort", memory, plain)
            for source in ("pg", "my"):
                self.cli("large_hash_join_" + source, 256, plain, ["taxi", source],
                    QUERIES["large_hash_join"]["sql"].replace("taxi.taxi_sample", source + ".taxi_sample"),
                    reference="large_hash_join")
            self.cli("three_adapter_join", 256, plain, ["taxi", "pg", "my"], threads=4)
        finally:
            plain.close()
        emulated = SourceProxy(self.state["source_endpoint"], rtt_ms=50, bytes_per_second=10 << 20)
        try:
            self.cli("zone_join", 256, emulated)
            self.cli("large_hash_join_pg", 256, emulated, ["taxi", "pg"],
                QUERIES["large_hash_join"]["sql"].replace("taxi.taxi_sample", "pg.taxi_sample"),
                reference="large_hash_join")
        finally:
            emulated.close()
        self.slow_results()
        self.verify_fixture("after")
        self.suite_complete = True
        self.save()

    def slow_results(self, include_downloads=True):
        directory = private_directory(self.directory / "slow-http")
        scratch = private_directory(directory / "scratch")
        catalog, env = self.setup_query(["taxi"])
        catalog_path = directory / "catalog.json"
        write_json(catalog_path, catalog)
        token = secrets.token_hex(32)
        env.update(TMPDIR=str(scratch), KELVO_CAPACITY_TOKEN=token)
        with socket.socket() as address:
            address.bind(("127.0.0.1", 0))
            port = address.getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        log = (directory / "server.log").open("wb")
        proc = subprocess.Popen([str(self.binary), "serve", "--config", str(catalog_path),
            "--listen", f"127.0.0.1:{port}", "--token-env", "KELVO_CAPACITY_TOKEN",
            "--concurrency", "1", "--max-queries", "16", "--max-rows", "2000000",
            "--max-bytes", str(64 << 20), "--memory-mb", "256", "--threads", "2",
            "--temp-mb", "256", "--timeout", "90s"], env=env,
            stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)

        def request(path, body=None):
            data = json.dumps(body).encode() if body is not None else None
            return urllib.request.urlopen(urllib.request.Request(base + path, data=data,
                headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"}), timeout=100)

        def ready():
            if proc.poll() is not None:
                raise AssertionError("capacity HTTP service exited before readiness")
            try:
                with request("/health") as response:
                    return response.status == 200
            except OSError:
                return False

        def submit(sql):
            with request("/v1/queries", {"mode": "federated", "sources": ["taxi"], "sql": sql}) as response:
                return json.load(response)["id"]

        def state(identifier):
            with request("/v1/queries/" + identifier) as response:
                return json.load(response)["state"]

        try:
            require(wait_until(ready, 10), "capacity HTTP service did not become ready")
            for rate in ((0, 1 << 20) if include_downloads else ()):
                output = directory / ("download-" + str(rate) + ".arrow")
                sampler = ProcessMetrics(proc.pid, scratch)
                sampler.start()
                began = time.perf_counter()
                identifier = submit(QUERIES["ordered_million"]["sql"])
                transferred = 0
                with request("/v1/queries/" + identifier + "/results") as response, output.open("wb") as target:
                    while chunk := response.read(65536):
                        target.write(chunk)
                        transferred += len(chunk)
                        if rate:
                            desired = transferred / rate - (time.perf_counter() - began)
                            if desired > 0:
                                time.sleep(desired)
                    target.flush()
                    os.fsync(target.fileno())
                elapsed = time.perf_counter() - began
                metrics = sampler.stop()
                require(wait_until(lambda: state(identifier) == "succeeded", 5),
                        "capacity HTTP result did not succeed")
                values = canonical_arrow(output)
                require(values["canonical_value_sha256"] == self.references["ordered_million"]["canonical_value_sha256"],
                        "slow HTTP result changed source values")
                self.results.append({"query": "ordered_million_http", "success": True,
                    "elapsed_seconds": elapsed, "download_rate_bytes_per_second": rate,
                    "scope": "Real authenticated HTTP result consumption with optional client-side read pacing; includes submit, query, transfer and file fsync. This is not geographic WAN latency.",
                    **metrics, **values})
                self.save()
                output.unlink()
                print(json.dumps({"stage": "http_download_verified", "read_rate": rate,
                                  "elapsed_seconds": elapsed}), flush=True)

            identifier = submit(QUERIES["ordered_million"]["sql"])
            with request("/v1/queries/" + identifier + "/results") as response:
                require(response.read(65536), "slow cancellation produced no real result bytes")
                before_cancel = state(identifier)
                require(before_cancel in ("running", "streaming"), "cancellation query finished before the controlled slow read")
                began = time.perf_counter()
                with request("/v1/queries/" + identifier + "/cancel", {}) as cancelled:
                    require(cancelled.status == 200, "slow HTTP cancellation failed")
            require(wait_until(lambda: state(identifier) == "cancelled", 8),
                    "cancelled slow HTTP query stayed active")
            replacement = submit("SELECT COUNT(*) AS n FROM taxi.zones")
            table, admission_rejections = None, 0

            def replacement_ready():
                nonlocal table, admission_rejections
                try:
                    with request("/v1/queries/" + replacement + "/results") as response:
                        table = pa.ipc.open_stream(response).read_all()
                    return True
                except urllib.error.HTTPError as error:
                    if error.code != 429:
                        raise
                    error.close()
                    admission_rejections += 1
                    return False

            require(wait_until(replacement_ready, 8), "cancelled query did not release admission capacity within eight seconds")
            require(table.to_pylist() == [{"n": 265}], "cancelled query did not release the single admission slot")
            self.results.append({"query": "slow_http_cancel_slot_recovery", "success": True,
                "elapsed_seconds": time.perf_counter() - began, "real_bytes_observed_before_cancel": True,
                "state_before_cancel": before_cancel, "transient_admission_rejections_during_teardown": admission_rejections,
                "single_slot_recovered_with_real_zone_query": True})
            self.save()
        finally:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
            log.close()


def write_json(path, value, private=True):
    text = json.dumps(value, indent=2) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".new")
    private_write(temporary, text)
    temporary.replace(path)
    if not private:
        path.chmod(0o644)


def admin(sql, payload=None, timeout=90):
    command = ["sudo", "-n", "docker", "exec", "-i", "kelvo-clickhouse",
               "clickhouse-client", "--max_threads", "2", "--max_memory_usage", str(2 << 30),
               "--max_insert_threads", "1", "--max_insert_block_size", str(BATCH_ROWS),
               "--min_insert_block_size_rows", str(BATCH_ROWS),
               "--min_insert_block_size_bytes", str(16 << 20)]
    if payload is None:
        command.append("--multiquery")
        payload = sql.encode()
    else:
        command += ["--query", sql]
    proc = subprocess.run(command, input=payload, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout)
    if proc.returncode:
        private_write(PRIVATE / "admin-error.log", proc.stderr)
        raise RuntimeError("capacity fixture administration failed; private diagnostic retained")
    return proc.stdout.decode().strip()


def download(url, path, expected_bytes=None):
    if not path.is_file():
        temporary = path.with_suffix(path.suffix + ".part")
        req = urllib.request.Request(url, headers={"User-Agent": "Kelvo capacity dataset verification"})
        with urllib.request.urlopen(req, timeout=60) as response, temporary.open("wb") as output:
            length = response.headers.get("Content-Length")
            actual = 0
            while chunk := response.read(1 << 20):
                output.write(chunk)
                actual += len(chunk)
            require(length is None or actual == int(length), "dataset download was incomplete")
        require(expected_bytes is None or temporary.stat().st_size == expected_bytes,
                "dataset byte count changed from verified sizing")
        temporary.replace(path)
    require(expected_bytes is None or path.stat().st_size == expected_bytes,
            "cached dataset differs from verified byte count")
    return {"url": url, "bytes": path.stat().st_size, "sha256": sha256_file(path)}


def convert(batch, month, start):
    columns = {name: batch.column(batch.schema.get_field_index(name)) for name in RAW_COLUMNS}
    amount = columns["total_amount"]
    # TLC publishes binary Float64 amounts. Preserve them verbatim, and persist
    # this explicitly derived integer metric once for cross-engine exact sums.
    rounded = pc.round(pc.multiply(amount, 100.0), ndigits=0, round_mode="half_to_even")
    cents = pc.cast(pc.if_else(pc.is_finite(amount), rounded, None), pa.int64(), safe=True)
    ordinal = pa.array(range(start, start + batch.num_rows), type=pa.int32())
    trip_id = pc.add(pc.cast(ordinal, pa.int64()), (month - 201900) * 100_000_000)
    return pa.RecordBatch.from_arrays([
        trip_id, pa.array([month] * batch.num_rows, type=pa.int32()), ordinal,
        pc.cast(columns["tpep_pickup_datetime"], pa.int64()),
        pc.cast(columns["PULocationID"], pa.int32(), safe=True),
        pc.cast(columns["DOLocationID"], pa.int32(), safe=True),
        columns["passenger_count"], columns["trip_distance"], amount, cents,
    ], schema=SCHEMA)


def serialize(batch):
    buffer = pa.BufferOutputStream()
    with pa.ipc.new_stream(buffer, batch.schema) as writer:
        writer.write_batch(batch)
    return buffer.getvalue().to_pybytes()


def prepare():
    require(sys.platform.startswith("linux"), "capacity dataset preparation requires the VM")
    os.umask(0o077)
    private_directory(PRIVATE)
    datasets = private_directory(PRIVATE / "datasets")
    require(shutil.disk_usage(PRIVATE).free >= 20 << 30, "capacity preparation needs 20GiB free scratch")
    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    if STATE.exists():
        state = json.loads(STATE.read_text())
    else:
        suffix = uuid.uuid4().hex[:12]
        state = {"version": 1, "database": "kelvo_capacity_" + suffix,
                 "created_at": datetime.now(timezone.utc).isoformat(),
                 "source_endpoint": "http://172.18.0.2:8123/", "loaded_months": [],
                 "users": {tenant: {"username": "kelvo_cap_" + suffix + "_" + tenant,
                                     "password": secrets.token_hex(32)} for tenant in ("a", "b")}}
        write_json(STATE, state)
    database = state["database"]
    require(database.startswith("kelvo_capacity_") and database.replace("_", "").isalnum(),
            "invalid owned capacity database")
    original_rows = int(admin("SELECT coalesce(sum(rows),0) FROM system.parts "
                              "WHERE active AND database='kelvo_bench' AND table='fact_events'"))
    require(original_rows == 100_000_000, "existing benchmark row count differs before preparation")
    admin(f"""CREATE DATABASE IF NOT EXISTS `{database}`;
CREATE TABLE IF NOT EXISTS `{database}`.trips (
trip_id Int64, source_month Int32, source_row Int32,
pickup_unix_us Nullable(Int64), pickup_zone_id Nullable(Int32), dropoff_zone_id Nullable(Int32),
passenger_count Nullable(Float64), trip_distance Nullable(Float64),
total_amount Nullable(Float64), fare_cents Nullable(Int64))
ENGINE=MergeTree PARTITION BY source_month ORDER BY (source_month,source_row);
CREATE TABLE IF NOT EXISTS `{database}`.zones (
zone_id Int32, borough String, zone String, service_zone String)
ENGINE=MergeTree ORDER BY zone_id;
CREATE TABLE IF NOT EXISTS `{database}`.taxi_sample (
trip_id Int64, pickup_zone_id Nullable(Int32), fare_cents Nullable(Int64))
ENGINE=MergeTree ORDER BY trip_id;""")
    zone_path = datasets / "taxi_zone_lookup.csv"
    zone_meta = download(SOURCE_ROOT + "/misc/taxi_zone_lookup.csv", zone_path, 12331)
    with zone_path.open(newline="") as source:
        zone_rows = list(csv.DictReader(source))
    require(len({int(row["LocationID"]) for row in zone_rows}) == len(zone_rows),
            "zone lookup contains duplicate identifiers")
    zone_schema = pa.schema([pa.field("zone_id", pa.int32(), False),
                            *[pa.field(name, pa.string(), False) for name in ("borough", "zone", "service_zone")]])
    zones = pa.RecordBatch.from_arrays([
        pa.array([int(row["LocationID"]) for row in zone_rows], type=pa.int32()),
        *[pa.array([row[key] for row in zone_rows]) for key in ("Borough", "Zone", "service_zone")],
    ], schema=zone_schema)
    if int(admin(f"SELECT count() FROM `{database}`.zones")) == 0:
        admin(f"INSERT INTO `{database}`.zones FORMAT ArrowStream", serialize(zones))
    require(int(admin(f"SELECT count() FROM `{database}`.zones")) == len(zone_rows),
            "zone fixture row count changed")
    zone_meta["rows"] = len(zone_rows)
    print(json.dumps({"stage": "real_zone_dimension_ready", "rows": len(zone_rows),
                      "sha256": zone_meta["sha256"]}), flush=True)
    files = []
    sample_path = datasets / "taxi_sample_1m.csv"
    sample_parquet = datasets / "taxi_sample_1m.parquet"
    for month, filename, expected_rows, expected_bytes in FILES:
        source_path = datasets / filename
        meta = download(SOURCE_ROOT + "/trip-data/" + filename, source_path, expected_bytes)
        parquet = pq.ParquetFile(source_path)
        require(parquet.metadata.num_rows == expected_rows, "TLC row count changed from source metadata")
        meta.update(source_month=month, rows=expected_rows)
        files.append(meta)
        count = int(admin(f"SELECT count() FROM `{database}`.trips WHERE source_month={month}"))
        sample_ready = month != 201901 or (sample_path.is_file() and sample_parquet.is_file())
        if count == expected_rows and sample_ready:
            print(json.dumps({"stage": "month_already_imported", "month": month, "rows": count}), flush=True)
            continue
        if count:
            admin(f"ALTER TABLE `{database}`.trips DROP PARTITION {month}")
        offset = 0
        sample_file = sample_path.open("w", newline="") if month == 201901 else None
        sample_writer = csv.writer(sample_file, lineterminator="\n") if sample_file else None
        if sample_writer:
            sample_writer.writerow(SAMPLE_SCHEMA.names)
        sample_pq = pq.ParquetWriter(sample_parquet, SAMPLE_SCHEMA) if sample_file else None
        began = time.monotonic()
        last_report = began
        try:
            for raw in parquet.iter_batches(batch_size=BATCH_ROWS, columns=RAW_COLUMNS, use_threads=False):
                batch = convert(raw, month, offset)
                admin(f"INSERT INTO `{database}`.trips FORMAT ArrowStream", serialize(batch))
                if sample_writer and offset < SAMPLE_ROWS:
                    chosen = batch.slice(0, min(batch.num_rows, SAMPLE_ROWS - offset))
                    sample = pa.RecordBatch.from_arrays([
                        chosen.column(chosen.schema.get_field_index(name)) for name in SAMPLE_SCHEMA.names],
                        schema=SAMPLE_SCHEMA)
                    sample_pq.write_batch(sample)
                    sample_writer.writerows(zip(*[column.to_pylist() for column in sample.columns]))
                offset += batch.num_rows
                if time.monotonic() - last_report >= 15:
                    print(json.dumps({"stage": "importing_real_records", "month": month,
                                      "rows": offset, "expected_rows": expected_rows}), flush=True)
                    last_report = time.monotonic()
        finally:
            if sample_pq:
                sample_pq.close()
            if sample_file:
                sample_file.close()
        require(offset == expected_rows and int(admin(
            f"SELECT count() FROM `{database}`.trips WHERE source_month={month}")) == expected_rows,
            "import did not preserve every source row")
        if month not in state["loaded_months"]:
            state["loaded_months"].append(month)
        write_json(STATE, state)
        print(json.dumps({"stage": "month_import_complete", "month": month,
                          "rows": offset, "elapsed_seconds": time.monotonic() - began}), flush=True)
    require(int(admin(f"SELECT count() FROM `{database}`.trips")) == TOTAL_ROWS,
            "capacity fact relation has an unexpected row count")
    sample_count = int(admin(f"SELECT count() FROM `{database}`.taxi_sample"))
    if sample_count == 0:
        admin(f"INSERT INTO `{database}`.taxi_sample SELECT trip_id,pickup_zone_id,fare_cents "
              "FROM `" + database + "`.trips WHERE source_month=201901 AND source_row<1000000")
    require(int(admin(f"SELECT count() FROM `{database}`.taxi_sample")) == SAMPLE_ROWS,
            "large join projection count changed")
    sample_truth = json.loads(admin(f"SELECT count() AS rows, sum(trip_id) AS trip_id_sum, "
        f"sum(pickup_zone_id) AS pickup_zone_sum, sum(fare_cents) AS fare_cents_sum, "
        f"countIf(isNull(fare_cents)) AS fare_cents_nulls FROM `{database}`.taxi_sample FORMAT JSONEachRow"))
    for identity in state["users"].values():
        admin(f"CREATE USER IF NOT EXISTS `{identity['username']}` IDENTIFIED WITH sha256_password BY '{identity['password']}'; "
              f"GRANT SELECT ON `{database}`.* TO `{identity['username']}`")
    source = {"id": "taxi", "type": "clickhouse", "url_env": "KELVO_SOURCE_TAXI_URL",
              "username_env": "KELVO_SOURCE_TAXI_USER", "password_env": "KELVO_SOURCE_TAXI_PASSWORD",
              "federation": {"tables": [{"name": name, "database": database, "table": name}
                  for name in ("trips", "zones", "taxi_sample")],
                  "max_scan_rows": 30_000_000, "max_scan_bytes": 2 << 30}}
    tenants = {key: {"catalog": {"sources": [source]}, "environment": {
        "KELVO_SOURCE_TAXI_URL": state["source_endpoint"],
        "KELVO_SOURCE_TAXI_USER": value["username"],
        "KELVO_SOURCE_TAXI_PASSWORD": value["password"]}} for key, value in state["users"].items()}
    write_json(PRIVATE / "tenant-sources.json", {"version": 1,
               "provenance_path": str(PROVENANCE), "tenants": tenants})
    final_original = int(admin("SELECT coalesce(sum(rows),0) FROM system.parts "
        "WHERE active AND database='kelvo_bench' AND table='fact_events'"))
    require(final_original == original_rows, "existing ClickHouse benchmark dataset changed")
    provenance = {"dataset": "NYC TLC yellow taxi trip records, January through March 2019",
        "prepared_at": datetime.now(timezone.utc).isoformat(), "rows": TOTAL_ROWS,
        "source_files": files, "zone_lookup": zone_meta,
        "official_documentation": ["https://clickhouse.com/docs/getting-started/example-datasets/nyc-taxi",
            "https://clickhouse.com/docs/integrations/data-formats/parquet",
            "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"],
        "selection": "All physical rows in three original TLC Parquet files. Retained columns are listed in imported_schema; no synthetic fact rows were generated and no anomalous rows were dropped.",
        "imported_schema": str(SCHEMA),
        "derived_fields": {"trip_id": "(source_month - 201900) * 100000000 + zero-based physical Parquet row ordinal; unique only within this explicitly selected three-month fixture",
            "source_month": "Original source file month as YYYYMM, irrespective of anomalous timestamps in its records",
            "source_row": "Zero-based physical Parquet row ordinal within the original monthly file",
            "pickup_unix_us": "Original microsecond timestamp represented as signed Int64, preserving nulls",
            "fare_cents": "Round original Float64 total_amount * 100 to nearest integer with half-to-even rounding, then safe cast Int64; null/non-finite original amounts become null. The original Float64 total_amount is retained unchanged. This is a derived metric, not a claim of exact original monetary decimal precision."},
        "large_join_projection": {"selection": "First 1000000 physical rows of the January file, projected to trip_id,pickup_zone_id,fare_cents",
            "rows": SAMPLE_ROWS, "csv_sha256": sha256_file(sample_path),
            "parquet_sha256": sha256_file(sample_parquet), "reference_aggregates": sample_truth},
        "import_limits": {"arrow_batch_rows": BATCH_ROWS, "clickhouse_threads": 2,
                          "clickhouse_query_memory_bytes": 2 << 30},
        "existing_100m_benchmark_rows_preserved": final_original}
    write_json(PROVENANCE, provenance, private=False)
    print(json.dumps({"stage": "capacity_dataset_ready", "rows": TOTAL_ROWS,
                      "zone_rows": len(zone_rows), "projection_rows": SAMPLE_ROWS}), flush=True)
    if state.get("relational_fixture"):
        configure_relational(Path(state["relational_fixture"]))


def configure_relational(fixture):
    manifest_path = PRIVATE / "tenant-sources.json"
    manifest = json.loads(manifest_path.read_text())
    additional = json.loads((fixture / "capacity-catalog.json").read_text())["sources"]
    secrets_map = json.loads((fixture / "environment.json").read_text())
    for tenant in manifest["tenants"].values():
        replaced_ids = {source["id"] for source in additional}
        tenant["catalog"]["sources"] = [s for s in tenant["catalog"]["sources"]
            if s["id"] not in replaced_ids] + additional
        available = {**tenant["environment"], **secrets_map}
        references = {source[key] for source in tenant["catalog"]["sources"]
            for key in ("dsn_env", "url_env", "username_env", "password_env", "token_env")
            if source.get(key)}
        require(all(name.startswith("KELVO_SOURCE_") and name in available for name in references),
                "capacity relational fixture has unavailable environment references")
        tenant["environment"] = {name: available[name] for name in references}
    manifest["trust_wrapper"] = str(fixture / "with-fixture-trust.sh")
    manifest["relational_fixture"] = str(fixture)
    write_json(manifest_path, manifest)
    state = json.loads(STATE.read_text())
    state["relational_fixture"] = str(fixture)
    write_json(STATE, state)
    print(json.dumps({"stage": "tenant_sources_configured", "tenants": 2,
                      "source_ids": [source["id"] for source in additional]}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("prepare", "configure", "references", "measure"))
    parser.add_argument("--relational-fixture", type=Path)
    parser.add_argument("--binary", type=Path, default=ROOT / "artifacts/image-bin-capacity/kelvo")
    parser.add_argument("--report", type=Path, default=ROOT / "docs/evidence/federation-capacity.json")
    args = parser.parse_args()
    if args.stage == "prepare":
        prepare()
    elif args.stage == "configure":
        require(args.relational_fixture is not None, "configure requires the private relational fixture path")
        configure_relational(args.relational_fixture)
    elif args.stage == "references":
        native_references()
    elif args.stage == "measure":
        Capacity(args.binary, args.report).run()


if __name__ == "__main__":
    main()
