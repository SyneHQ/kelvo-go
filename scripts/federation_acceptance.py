#!/usr/bin/env python3
"""Live ClickHouse -> Go Arrow -> DuckDB bridge acceptance, on the Linux test VM.

Requires the explicitly tagged bridge binary, pyarrow, and an existing dedicated
ClickHouse container with local CLI administration. Installs and builds nothing.
The existing service and kelvo_bench dataset are never changed. Only a uniquely
named acceptance database/user and this script's child processes are removed.
The loopback observing proxy forwards to one fixed endpoint; its bounded captures
and CLI diagnostics stay private. Published SQL refers only to synthetic data.
"""

from __future__ import annotations

import argparse
import base64
import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import signal
import stat
import struct
import subprocess
import sys
import threading
import time
from urllib.parse import parse_qs, urlsplit
import uuid

import pyarrow as pa

from benchmark_clickhouse import RSSSampler, sha256_file


ROOT = Path(__file__).resolve().parents[1]
EOS = b"\xff\xff\xff\xff\0\0\0\0"
ROWS = 1_000_000
CAPTURE_LIMIT = 32 << 20


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def private_directory(path):
    require(not path.is_symlink(), "private directory cannot be a symlink")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(stat.S_IMODE(path.stat().st_mode) == 0o700,
            "private directory must have mode 0700")
    return path


def private_write(path, data):
    require(not path.is_symlink(), "private file cannot be a symlink")
    path.write_bytes(data.encode() if isinstance(data, str) else data)
    path.chmod(0o600)


def wait_until(predicate, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.04)
    return predicate()


class Observer(ThreadingHTTPServer):
    """Trusted test-only relay; never forwards to a request-chosen destination."""

    daemon_threads = False
    block_on_close = True

    def __init__(self, owner, endpoint):
        self.owner = owner
        parsed = urlsplit(endpoint)
        require(parsed.scheme == "http" and parsed.hostname and
                not parsed.username and not parsed.query and not parsed.fragment,
                "acceptance endpoint must be plain dedicated HTTP")
        self.host, self.port = parsed.hostname, parsed.port or 8123
        self.upstream_path = parsed.path or "/"
        self.lock = threading.Lock()
        self.events = []
        self.active = 0
        self.max_active = 0
        self.barrier = None
        self.sockets = set()
        super().__init__(("127.0.0.1", 0), ObserveHandler)
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def mark(self):
        with self.lock:
            return len(self.events)

    def since(self, mark, scans_only=True):
        with self.lock:
            events = list(self.events[mark:])
        return [event for event in events if not scans_only or not event["describe"]]

    def idle(self):
        with self.lock:
            return self.active == 0

    def stop(self):
        self.shutdown()
        with self.lock:
            sockets = list(self.sockets)
        for connection in sockets:
            connection.close()
        self.server_close()
        self.thread.join()

    def public(self, events):
        result = []
        for event in events:
            row = {key: event.get(key) for key in (
                "sql", "describe", "status", "wire_bytes", "complete",
                "arrow_rows", "arrow_buffer_bytes", "arrow_columns",
                "readonly", "overflow_mode", "max_result_rows", "max_result_bytes")}
            row["sql"] = row["sql"].replace(self.owner.database, "acceptance_fixture")
            result.append(row)
        return result


class ObserveHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_POST(self):
        observer = self.server
        length = self.headers.get("Content-Length", "")
        if not length.isdigit() or not 0 < int(length) <= 1 << 20:
            self.send_error(400)
            return
        sql = self.rfile.read(int(length)).decode("utf-8", "strict")
        parsed = urlsplit(self.path)
        if parsed.path != "/":
            self.send_error(404)
            return
        settings = parse_qs(parsed.query)
        if not (settings.get("readonly") == ["1"] and
                settings.get("result_overflow_mode") == ["throw"] and
                settings.get("cancel_http_readonly_queries_on_client_close") == ["1"]):
            self.send_error(403)
            return
        with observer.lock:
            index = len(observer.events)
            event = {"sql": sql, "describe": sql.endswith(" LIMIT 0"),
                     "wire_bytes": 0, "complete": False, "status": None,
                     "finished": False, "readonly": True, "overflow_mode": "throw",
                     "max_result_rows": int(settings["max_result_rows"][0]),
                     "max_result_bytes": int(settings["max_result_bytes"][0])}
            observer.events.append(event)
            observer.active += 1
            observer.max_active = max(observer.active, observer.max_active)
            gate = observer.barrier if not event["describe"] else None
        capture = observer.owner.directory / f"upstream-{index}.arrow"
        connection = http.client.HTTPConnection(observer.host, observer.port, timeout=35)
        with observer.lock:
            observer.sockets.add(connection)
        try:
            if gate:
                gate.wait(timeout=8)
                # Keep both independent worker processes observable by /proc.
                time.sleep(0.15)
            connection.request("POST", observer.upstream_path + "?" + parsed.query,
                               body=sql.encode(), headers={
                                   "Authorization": self.headers.get("Authorization", ""),
                                   "Content-Type": "text/plain; charset=utf-8",
                                   "Accept": "application/vnd.apache.arrow.stream",
                               })
            response = connection.getresponse()
            event["status"] = response.status
            self.send_response(response.status)
            self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            with capture.open("wb") as output:
                while True:
                    chunk = response.read1(32768)
                    if not chunk:
                        event["complete"] = True
                        break
                    # wire_bytes is the dechunked HTTP body from ClickHouse,
                    # not TCP bytes and not decoded Arrow buffer size.
                    event["wire_bytes"] += len(chunk)
                    require(event["wire_bytes"] <= CAPTURE_LIMIT,
                            "observing proxy capture limit exceeded")
                    output.write(chunk)
                    self.wfile.write(chunk)
                    self.wfile.flush()
            if event["complete"] and response.status == 200:
                try:
                    with capture.open("rb") as source, pa.ipc.open_stream(source) as reader:
                        event["arrow_columns"] = reader.schema.names
                        event["arrow_rows"] = 0
                        event["arrow_buffer_bytes"] = 0
                        for batch in reader:
                            event["arrow_rows"] += batch.num_rows
                            event["arrow_buffer_bytes"] += batch.get_total_buffer_size()
                except Exception:
                    event["arrow_decode_failed"] = True
        except (OSError, http.client.HTTPException, threading.BrokenBarrierError,
                AssertionError):
            # Cancellation, bounded overflow and source exceptions are expected
            # in specific negative tests. Never publish arbitrary error text.
            pass
        finally:
            connection.close()
            event["finished"] = True
            with observer.lock:
                observer.sockets.discard(connection)
                observer.active -= 1


class Acceptance:
    def __init__(self, args):
        os.umask(0o077)
        self.args = args
        self.directory = private_directory(private_directory(
            ROOT / "artifacts/federation-private") / ("run-" + uuid.uuid4().hex))
        self.database = "kelvo_fed_" + uuid.uuid4().hex[:16]
        self.username = self.database + "_reader"
        self.password = secrets.token_hex(32)
        self.created_database = False
        self.created_user = False
        self.observer = None
        self.children = []
        self.checks = []
        self.stage = "prerequisites"
        self.command_number = 0
        self.command_lock = threading.Lock()
        self.env = dict(os.environ, GOMAXPROCS="2")
        self.version = None
        self.binary_sha256 = None
        self.preserved_rows = None

    def admin(self, sql):
        # SQL, including the disposable credential, is passed via stdin rather
        # than process arguments; neither command output nor SQL is printed.
        proc = subprocess.run(["sudo", "-n", "docker", "exec", "-i", self.args.container,
                               "clickhouse-client", "--multiquery"],
                              input=sql.encode(), stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, timeout=30)
        if proc.returncode != 0:
            private_write(self.directory / "admin-error.txt", proc.stderr)
        require(proc.returncode == 0, "acceptance ClickHouse administration failed")
        return proc.stdout.decode().strip()

    def record(self, name, **data):
        self.checks.append({"test": name, "passed": True, **data})
        print(name + ": passed", flush=True)

    def configuration(self, *, table="fact", max_rows=ROWS * 2, max_bytes=64 << 20):
        source = {"id": "warehouse", "type": "clickhouse",
                  "url_env": "KELVO_SOURCE_FED_ACCEPTANCE_URL",
                  "username_env": "KELVO_SOURCE_FED_ACCEPTANCE_USER",
                  "password_env": "KELVO_SOURCE_FED_ACCEPTANCE_PASSWORD",
                  "federation": {"tables": [{"name": table,
                      "database": self.database, "table": table}],
                      "max_scan_rows": max_rows, "max_scan_bytes": max_bytes}}
        hidden = copy.deepcopy(source)
        hidden["id"] = "unselected"
        for key, suffix in (("url_env", "URL"), ("username_env", "USER"), ("password_env", "PASSWORD")):
            hidden[key] = "KELVO_SOURCE_FED_UNSELECTED_" + suffix
            # An unselected source must never need its unavailable credentials.
            self.env.pop(hidden[key], None)
        hidden["federation"]["tables"] = [
            {"name": "hidden", "database": self.database, "table": "hidden"}]
        config = {"sources": [source, hidden, {"id": "local_factors", "type": "csv",
                                              "path": str(self.directory / "factors.csv")} ]}
        path = self.directory / ("catalog-" + uuid.uuid4().hex + ".json")
        private_write(path, json.dumps(config))
        return path

    def setup(self):
        require(sys.platform.startswith("linux"), "acceptance requires Linux VM")
        require(self.args.binary.is_file(), "tagged bridge binary is unavailable")
        self.binary_sha256 = sha256_file(self.args.binary)
        self.version = self.admin("SELECT version()")
        self.preserved_rows = int(self.admin(
            "SELECT coalesce(sum(rows),0) FROM system.parts "
            "WHERE active AND database='kelvo_bench' AND table='fact_events'"))
        self.admin(f"CREATE DATABASE `{self.database}`")
        self.created_database = True
        self.admin(f"""CREATE TABLE `{self.database}`.fact (
row_id UInt64, i64 Nullable(Int64), u64 Nullable(UInt64),
amount Nullable(Decimal(38,12)), label Nullable(String), payload String)
ENGINE=MergeTree ORDER BY row_id;
INSERT INTO `{self.database}`.fact SELECT number,
multiIf(number=0, CAST('-9223372036854775808' AS Int64),
number=1, CAST('9223372036854775807' AS Int64), number=2, NULL, toInt64(number)),
multiIf(number=0, toUInt64(0), number=1, CAST('18446744073709551615' AS UInt64),
number=2, NULL, toUInt64(number)),
multiIf(number=0, CAST('-99999999999999999999999999.123456789012' AS Decimal(38,12)),
number=1, CAST('99999999999999999999999999.123456789012' AS Decimal(38,12)),
number=2, NULL, CAST(number AS Decimal(38,12))),
multiIf(number=0,'first',number=1,'second',number=2,NULL,'ordinary'),
repeat('x',512) FROM numbers({ROWS})
SETTINGS max_threads=2,max_block_size=65536,max_insert_block_size=65536,
min_insert_block_size_rows=65536,min_insert_block_size_bytes=33554432,
max_memory_usage=536870912;
CREATE TABLE `{self.database}`.hidden (row_id UInt64) ENGINE=TinyLog;
INSERT INTO `{self.database}`.hidden VALUES (42);
CREATE VIEW `{self.database}`.slow AS
SELECT number AS row_id, sleepEachRow(0.00001) AS delay FROM numbers(2000000);
""")
        self.admin(f"CREATE USER `{self.username}` IDENTIFIED WITH sha256_password BY '{self.password}'")
        self.created_user = True
        self.admin(f"GRANT SELECT ON `{self.database}`.* TO `{self.username}`")
        # SELECT-only grants deny writes without a readonly=1 user profile,
        # which would also forbid Kelvo's safe per-request limit settings.
        endpoint = urlsplit(self.args.endpoint)
        connection = http.client.HTTPConnection(endpoint.hostname, endpoint.port or 8123, timeout=5)
        try:
            credentials = base64.b64encode((self.username + ":" + self.password).encode()).decode()
            connection.request("POST", endpoint.path or "/",
                               body=f"INSERT INTO `{self.database}`.hidden VALUES (43)",
                               headers={"Authorization": "Basic " + credentials})
            response = connection.getresponse()
            denied = response.read(4096)
            require(response.status != 200 and b"ACCESS_DENIED" in denied,
                    "reader principal unexpectedly permits writes")
        finally:
            connection.close()
        self.observer = Observer(self, self.args.endpoint)
        self.env.update(KELVO_SOURCE_FED_ACCEPTANCE_URL=f"http://127.0.0.1:{self.observer.server_port}/",
                        KELVO_SOURCE_FED_ACCEPTANCE_USER=self.username,
                        KELVO_SOURCE_FED_ACCEPTANCE_PASSWORD=self.password)
        private_write(self.directory / "factors.csv", "factor\n2\n3\n")
        self.config = self.configuration()
        self.record("owned_fixture_created", fixture_rows=ROWS,
                    existing_benchmark_rows=self.preserved_rows,
                    clickhouse_version=self.version, reader_write_denied=True)

    def start(self, sql, config=None, sources="warehouse", timeout="15s", threads=2):
        with self.command_lock:
            self.command_number += 1
            index = self.command_number
        output = self.directory / f"query-{index}.arrow"
        command = [str(self.args.binary), "query", "--config", str(config or self.config),
                   "--mode", "federated", "--sources", sources, "--sql", sql,
                   "--out", str(output), "--threads", str(threads), "--memory-mb", "256",
                   "--temp-mb", "64", "--max-rows", str(ROWS * 2),
                   "--max-bytes", str(64 << 20), "--timeout", timeout]
        started = time.perf_counter()
        proc = subprocess.Popen(command, env=self.env, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                start_new_session=True)
        self.children.append(proc)
        sampler = RSSSampler(proc.pid)
        sampler.start()
        return {"process": proc, "sampler": sampler, "output": output,
                "index": index, "started": started}

    def finish(self, running, success=True, timeout=40):
        proc = running["process"]
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            stdout, stderr = proc.communicate()
            raise AssertionError("CLI acceptance deadline exceeded") from None
        finally:
            running["rss"] = running["sampler"].stop()
        elapsed = time.perf_counter() - running["started"]
        private_write(self.directory / f"cli-{running['index']}.log", stdout + b"\n" + stderr)
        require((proc.returncode == 0) == success, "CLI returned unexpected exit status")
        sampled = running["rss"]
        require(wait_until(lambda: all(not Path(f"/proc/{pid}").exists()
                    for pid in sampled["worker_peak_rss_kib_by_pid"])),
                "query worker remained alive after coordinator exit")
        metrics = {"elapsed_seconds": elapsed,
                   "coordinator_peak_rss_kib": sampled["coordinator_peak_rss_kib"],
                   "worker_peak_rss_kib_max": sampled["worker_peak_rss_kib_max"],
                   "observed_worker_count": len(sampled["worker_peak_rss_kib_by_pid"]),
                   "rss_samples": sampled["samples"]}
        if not success:
            require(not running["output"].exists(), "failed query published an Arrow export")
            error = None
            for line in stderr.decode(errors="replace").splitlines():
                try:
                    parsed = json.loads(line)
                    candidate = parsed.get("error", parsed)
                    if isinstance(candidate, dict):
                        error = candidate.get("code", error)
                except (ValueError, AttributeError):
                    pass
            if error is None:
                match = re.search(r"\b(PERMISSION_DENIED|RESOURCE_EXHAUSTED|UNSUPPORTED|QUERY_FAILED|CANCELLED|DEADLINE_EXCEEDED)\b", stderr.decode(errors="replace"))
                error = match.group(1) if match else "nonzero_exit"
            public_message = stderr.decode(errors="replace").strip()
            known_messages = {
                "Native source pushed filter is unsupported": "UNSUPPORTED",
                "Required federation predicate is unsupported": "UNSUPPORTED",
                "Federation predicates require exact matching integer or boolean types": "UNSUPPORTED",
                "Query uses a capability unavailable to this execution": "PERMISSION_DENIED",
                "Query failed": "QUERY_FAILED", "Source rejected query": "QUERY_FAILED",
                "Query cancelled": "CANCELLED",
            }
            if error == "nonzero_exit" and public_message in known_messages:
                error = known_messages[public_message]
                metrics["error_code_basis"] = "known sanitized CLI message; CLI does not print structured codes"
            metrics["error_code"] = error
            return None, metrics
        wire = running["output"].read_bytes()
        require(wire.endswith(EOS), "successful export omitted Arrow EOS")
        table = pa.ipc.open_stream(wire).read_all()
        stats = json.loads(stderr)
        require(stats.get("backend") == "duckdb", "federated query did not use DuckDB")
        require(isinstance(stats.get("federation"), list) and stats["federation"],
                "CLI omitted federation source statistics")
        metrics["federation"] = [{key: entry[key] for key in (
            "source", "table", "scans", "rows_fetched", "arrow_bytes_fetched", "batches_fetched")}
            for entry in stats["federation"]]
        metrics["output_rows"] = table.num_rows
        metrics["output_wire_bytes"] = len(wire)
        return table, metrics

    def query(self, sql, **kwargs):
        success = kwargs.pop("success", True)
        mark = self.observer.mark()
        table, metrics = self.finish(self.start(sql, **kwargs), success)
        require(not list(self.directory.glob(".kelvo-result-*")),
                "query left an unpublished local output file")
        require(wait_until(self.observer.idle), "upstream stream did not close")
        events = self.observer.since(mark)
        if success and all(event.get("arrow_rows") is not None for event in events):
            require(sum(entry["rows_fetched"] for entry in metrics["federation"]) ==
                    sum(event["arrow_rows"] for event in events),
                    "reported federation rows differ from observed upstream rows")
        metrics["upstream_scans"] = self.observer.public(events)
        return table, metrics, events

    def checked_scan(self, events, rows, columns=None):
        require(len(events) == 1, "expected exactly one upstream scan")
        event = events[0]
        require(event["complete"] and event["status"] == 200 and
                event.get("arrow_rows") == rows, "upstream scan row count differs")
        if columns is not None:
            require(event.get("arrow_columns") == columns,
                    "upstream projection differs from required columns")
        require(" LIMIT " not in event["sql"], "source scan was truncated by SQL LIMIT")
        return event

    def correctness(self):
        self.stage = "typed_fidelity"
        sql = "SELECT row_id,i64,u64,amount,label FROM warehouse.fact WHERE row_id<3 ORDER BY row_id"
        table, metrics, events = self.query(sql)
        require([table.schema.field(name).type for name in table.column_names] ==
                [pa.uint64(), pa.int64(), pa.uint64(), pa.decimal128(38, 12), pa.string()],
                "typed Arrow schema changed")
        require(table.to_pylist() == [
            {"row_id": 0, "i64": -(1 << 63), "u64": 0,
             "amount": Decimal("-99999999999999999999999999.123456789012"), "label": "first"},
            {"row_id": 1, "i64": (1 << 63) - 1, "u64": (1 << 64) - 1,
             "amount": Decimal("99999999999999999999999999.123456789012"), "label": "second"},
            {"row_id": 2, "i64": None, "u64": None, "amount": None, "label": None}],
            "exact integer/decimal/NULL values changed")
        self.checked_scan(events, 3)
        self.typed_reference = table
        self.record("typed_fidelity", **metrics)

        self.stage = "projection_and_filter_pushdown"
        table, narrow, events = self.query(
            f"SELECT row_id FROM warehouse.fact WHERE row_id>={ROWS - 10} ORDER BY row_id")
        require(table.column(0).to_pylist() == list(range(ROWS - 10, ROWS)), "pushed filter result changed")
        event = self.checked_scan(events, 10, ["row_id"])
        require(" WHERE " in event["sql"] and f"CAST('{ROWS - 10}' AS UInt64)" in event["sql"],
                "upstream filter did not preserve its exact constant")
        _, wide, wide_events = self.query(
            f"SELECT row_id,payload FROM warehouse.fact WHERE row_id>={ROWS - 10} ORDER BY row_id")
        wide_event = self.checked_scan(wide_events, 10)
        require(event["wire_bytes"] < wide_event["wire_bytes"], "projection did not reduce transferred bytes")
        self.record("projection_and_filter_pushdown", narrow=narrow, wide=wide,
                    source_fixture_rows=ROWS, selected_source_rows=10)

        for name, predicate, expected in [
            ("uint64_max_predicate", "u64=18446744073709551615::UBIGINT", [1]),
            ("int64_min_predicate", "i64=(-9223372036854775807::BIGINT-1)", [0]),
            ("null_predicate", "i64 IS NULL", [2]),
            ("range_conjunction", "row_id>=5 AND row_id<8", [5, 6, 7])]:
            self.stage = name
            table, metrics, events = self.query(
                "SELECT row_id FROM warehouse.fact WHERE " + predicate + " ORDER BY row_id")
            require(table.column(0).to_pylist() == expected, "exact predicate result changed")
            if name == "null_predicate":
                # Pinned DuckDB may retain this filter above its Arrow scan.
                # Correct results do not imply source-side NULL pushdown.
                require(len(events) == 1 and events[0].get("arrow_rows") in (1, ROWS),
                        "NULL predicate scan was incomplete")
                metrics["filter_executed_upstream"] = events[0]["arrow_rows"] == 1
            else:
                self.checked_scan(events, len(expected))
            self.record(name, **metrics)

        self.stage = "count_star_projection"
        table, metrics, events = self.query("SELECT COUNT(*) AS n FROM warehouse.fact")
        require(table.to_pylist() == [{"n": ROWS}], "empty-projection count changed")
        event = self.checked_scan(events, ROWS)
        require(event["arrow_columns"] in (["__kelvo_count"], ["row_id"]),
                "count scan fetched an unexpected projection")
        self.record("count_star_projection", observed_count_projection=event["arrow_columns"],
                    projection_note="Pinned DuckDB may retain the first physical column for COUNT(*); an empty producer projection uses the private count column.", **metrics)

        self.stage = "self_join_rescan"
        table, metrics, events = self.query(
            "SELECT a.row_id,a.i64,b.u64 FROM warehouse.fact a JOIN warehouse.fact b "
            "ON a.row_id=b.row_id WHERE a.row_id<3 ORDER BY a.row_id")
        require(table.num_rows == 3 and table.column("u64").to_pylist() ==
                [0, (1 << 64) - 1, None], "self-join changed values")
        require(len(events) == 2 and all(event.get("arrow_rows") == 3 for event in events),
                "self-join did not open independent filtered scans")
        self.record("self_join_rescan", **metrics)

        self.stage = "csv_cross_join"
        table, metrics, events = self.query(
            "SELECT row_id,factor FROM warehouse.fact CROSS JOIN local_factors "
            "WHERE row_id<3 ORDER BY row_id,factor", sources="warehouse,local_factors")
        require(table.to_pylist() == [{"row_id": row, "factor": factor}
                                     for row in range(3) for factor in (2, 3)],
                "CSV cross join changed values")
        self.checked_scan(events, 3)
        self.record("csv_cross_join", **metrics)

    def million_row_exports(self):
        self.stage = "million_row_projected_exports"
        expected_digest = hashlib.sha256()
        for row_id in range(ROWS):
            value = (1 << 64) - 1 if row_id == 1 else (0 if row_id == 2 else row_id)
            expected_digest.update(struct.pack("<QQ?", row_id, value, row_id != 2))
        expected_checksum = expected_digest.hexdigest()
        trials = []
        for trial in range(1, 4):
            table, metrics, events = self.query(
                "SELECT row_id,u64 FROM warehouse.fact ORDER BY row_id", timeout="30s")
            event = self.checked_scan(events, ROWS)
            require(len(event["arrow_columns"]) == 2 and
                    set(event["arrow_columns"]) == {"row_id", "u64"},
                    "million-row export fetched unrelated source columns")
            require(table.num_rows == ROWS and table.column("u64").null_count == 1,
                    "million-row export count or NULL count changed")
            checksum = hashlib.sha256()
            for batch in table.to_batches():
                for row_id, value in zip(batch.column(0).to_pylist(), batch.column(1).to_pylist()):
                    checksum.update(struct.pack("<QQ?", row_id, value or 0, value is not None))
            require(checksum.hexdigest() == expected_checksum,
                    "million-row export value checksum changed")
            metrics.update(trial=trial, canonical_value_sha256=checksum.hexdigest(),
                           rows_per_complete_cli_second=ROWS / metrics["elapsed_seconds"])
            trials.append(metrics)
        self.record("million_row_projected_exports", fixture_rows=ROWS, trials=trials,
                    checksum_encoding="Rows sorted by row_id; each is little-endian UInt64 row_id, UInt64 u64-or-zero, boolean u64-validity (<QQ?).",
                    memory_limit_mib=256, threads=2, scan_row_limit=ROWS * 2,
                    scan_byte_limit=64 << 20, final_byte_limit=64 << 20,
                    measurement_note="Complete CLI includes coordinator, worker, source scan, DuckDB ORDER BY and file-synced Arrow export; an active test proxy captures/decodes source batches. Validation/checksum and fixture loading are outside the interval. Caches are warm/uncontrolled; this is not a production throughput claim.")

    def boundaries(self):
        self.stage = "query_wide_table_cap"
        source = json.loads(self.config.read_text())["sources"][0]
        source["federation"]["tables"] = [
            {"name": "fact_" + str(index), "database": self.database, "table": "fact"}
            for index in range(32)]
        extra = copy.deepcopy(source)
        extra["id"] = "extra"
        extra["federation"]["tables"] = [
            {"name": "fact", "database": self.database, "table": "fact"}]
        config = self.directory / "table-cap.json"
        private_write(config, json.dumps({"sources": [source, extra]}))
        mark = self.observer.mark()
        _, metrics, _ = self.query("SELECT 1", config=config,
                                   sources="warehouse,extra", success=False)
        require(not self.observer.since(mark, False),
                "query-wide table cap allowed source metadata requests")
        self.record("query_wide_table_cap", configured_tables=33,
                    no_upstream_describe_or_scan=True, **metrics)

        self.stage = "unsupported_required_predicate"
        _, metrics, events = self.query(
            "SELECT row_id FROM warehouse.fact WHERE label='first'", success=False)
        require(metrics["error_code"] == "UNSUPPORTED", "unsupported required filter did not fail explicitly")
        require(not events, "unsupported required filter opened an upstream scan")
        self.record("unsupported_required_predicate", **metrics)

        for name, config, sql in [
            ("row_budget_no_truncation", self.configuration(max_rows=10),
             "SELECT COUNT(*) AS n FROM warehouse.fact"),
            ("byte_budget_no_truncation", self.configuration(max_bytes=1024),
             "SELECT SUM(length(payload)) AS n FROM warehouse.fact")]:
            self.stage = name
            _, metrics, events = self.query(sql, config=config, success=False)
            require(events and all(" LIMIT " not in event["sql"] for event in events),
                    "budget failure truncated source relation")
            self.record(name, **metrics)

        for name, sql, sources, early in [
            ("unselected_source_denied", "SELECT * FROM unselected.hidden", "warehouse", False),
            ("unregistered_table_denied", "SELECT * FROM warehouse.hidden", "warehouse", False),
            ("source_not_selected_denied", "SELECT * FROM warehouse.fact", "local_factors", True),
            ("direct_arrow_scan_denied", "SELECT * FROM arrow_scan(1,2,3)", "warehouse", True),
            ("quoted_arrow_scan_denied", 'SELECT * FROM "ARROW_SCAN_DUMB"(1,2,3)', "warehouse", True),
            ("dynamic_query_denied", "SELECT * FROM query('SELECT * FROM warehouse.fact')", "warehouse", True),
            ("direct_file_access_denied", "SELECT * FROM read_blob('/etc/passwd')", "warehouse", True)]:
            self.stage = name
            mark = self.observer.mark()
            _, metrics, events = self.query(sql, sources=sources, success=False)
            require(not events, "denied query scanned an upstream table")
            if early:
                require(not self.observer.since(mark, False), "denied query made an upstream request")
            require(all(".hidden" not in event["sql"] and "`hidden`" not in event["sql"]
                        for event in self.observer.since(mark, False)),
                    "unselected table was described")
            self.record(name, **metrics)

    def concurrency(self):
        self.stage = "concurrent_query_workers"
        mark = self.observer.mark()
        self.observer.barrier = threading.Barrier(2)
        jobs = [self.start("SELECT SUM(row_id) AS total FROM warehouse.fact WHERE row_id<10"),
                self.start(f"SELECT SUM(row_id) AS total FROM warehouse.fact WHERE row_id>={ROWS - 10}")]
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(self.finish, jobs))
        finally:
            self.observer.barrier = None
        require([table.column(0).to_pylist()[0] for table, _ in results] ==
                [45, sum(range(ROWS - 10, ROWS))], "independent concurrent query results changed")
        worker_sets = [set(job["rss"]["worker_peak_rss_kib_by_pid"]) for job in jobs]
        require(all(worker_sets) and not worker_sets[0].intersection(worker_sets[1]),
                "concurrent queries did not use distinct observed workers")
        require(wait_until(self.observer.idle), "concurrent upstream stream remained open")
        require(not list(self.directory.glob(".kelvo-result-*")),
                "concurrent query left an unpublished local output file")
        self.record("concurrent_query_workers", runs=[metrics for _, metrics in results],
                    distinct_worker_processes=True,
                    upstream_scans=self.observer.public(self.observer.since(mark)))

    def cancellation(self):
        self.stage = "cancel_live_upstream_scan"
        config = self.configuration(table="slow", max_rows=3000000, max_bytes=32 << 20)
        mark = self.observer.mark()
        running = self.start("SELECT SUM(delay) FROM warehouse.slow", config=config, timeout="30s")
        active_sql = f"SELECT count() FROM system.processes WHERE user='{self.username}'"
        require(wait_until(lambda: bool(self.observer.since(mark)) and
                          int(self.admin(active_sql)) > 0, timeout=10),
                "cancellation test did not observe an active upstream scan")
        started = time.perf_counter()
        running["process"].send_signal(signal.SIGINT)
        _, metrics = self.finish(running, success=False, timeout=10)
        require(not list(self.directory.glob(".kelvo-result-*")),
                "cancellation left an unpublished local output file")
        require(wait_until(self.observer.idle), "cancellation left proxy/upstream response open")
        require(wait_until(lambda: int(self.admin(active_sql)) == 0),
                "cancellation left ClickHouse query running")
        metrics["cancellation_cleanup_seconds"] = time.perf_counter() - started
        metrics["upstream_scans"] = self.observer.public(self.observer.since(mark))
        self.record("cancel_live_upstream_scan", no_worker_remaining=True,
                    no_proxy_stream_remaining=True, no_upstream_query_remaining=True, **metrics)

    def sandbox(self):
        self.stage = "landlock_custom_federation"
        launcher = self.args.binary.parent / "kelvo-landlock"
        require(launcher.is_file(), "Landlock launcher is unavailable")
        workspace = private_directory(self.directory / "sandbox-work")
        forbidden = self.directory / "unselected-synthetic.csv"
        private_write(forbidden, "marker\nsynthetic-unselected-data\n")
        source = json.loads(self.config.read_text())["sources"][0]
        limits = {"max_rows": ROWS * 2, "max_bytes": 64 << 20,
                  "timeout": 15000000000, "memory_mb": 256,
                  "threads": 2, "max_temp_mb": 64}

        def execute(label, sources, sql, confined):
            env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                   "HOME": str(workspace), "TMPDIR": str(workspace), "GOMAXPROCS": "2"}
            for selected in sources:
                for key in ("url_env", "username_env", "password_env"):
                    if selected.get(key):
                        env[selected[key]] = self.env[selected[key]]
            payload = {"config": {"sources": sources}, "limits": limits,
                       "request": {"mode": "federated", "sources": [s["id"] for s in sources], "sql": sql}}
            command = [str(self.args.binary), "worker"]
            if confined:
                command = [str(launcher), "--write", str(workspace), "--", *command]
            proc = subprocess.Popen(command, env=env, cwd=workspace, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            self.children.append(proc)
            try:
                stdout, stderr = proc.communicate(json.dumps(payload).encode(), timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.communicate()
                raise AssertionError("sandbox worker exceeded acceptance deadline") from None
            private_write(self.directory / (label + ".log"), stderr)
            require(proc.returncode == 0, "sandbox worker failed to exit normally")
            outcome = json.loads(stderr)
            return stdout, outcome, sorted(env)

        mark = self.observer.mark()
        data, outcome, names = execute("sandbox-native", [source],
            "SELECT row_id,i64,u64,amount,label FROM warehouse.fact WHERE row_id<3 ORDER BY row_id", True)
        require(not outcome.get("error") and data.endswith(EOS), "sandboxed federation query failed")
        table = pa.ipc.open_stream(data).read_all()
        require(table.equals(self.typed_reference), "sandboxed federation changed exact Arrow values")
        require(wait_until(self.observer.idle), "sandboxed federation left an upstream stream")
        self.checked_scan(self.observer.since(mark), 3)
        self.record("landlock_custom_federation", rows=3, exact_values_preserved=True,
                    launcher_sha256=sha256_file(launcher),
                    worker_environment_names=names, selected_source_credentials_only=True,
                    upstream_scans=self.observer.public(self.observer.since(mark)),
                    scope="Direct worker through real Landlock launcher; this is separate from the CLI coordinator measurements. Landlock filesystem policy does not isolate network destinations.")

        self.stage = "landlock_unselected_file_denial"
        denied_source = [{"id": "synthetic_file", "type": "csv", "path": str(forbidden)}]
        sql = "SELECT marker FROM synthetic_file"
        data, outcome, _ = execute("file-control", denied_source, sql, False)
        require(not outcome.get("error") and pa.ipc.open_stream(data).read_all().to_pylist() ==
                [{"marker": "synthetic-unselected-data"}], "synthetic file control failed")
        data, outcome, _ = execute("file-denied", denied_source, sql, True)
        require(outcome.get("error") and not data, "Landlock permitted the unselected synthetic file")
        self.record("landlock_unselected_file_denial", same_file_control_succeeded=True,
                    denied_without_arrow_output=True)

    def cleanup(self):
        for proc in self.children:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=10)
        if self.observer:
            self.observer.stop()
            private_write(self.directory / "observed-queries.json",
                          json.dumps(self.observer.public(self.observer.since(0, False)), indent=2))
        if self.created_user:
            # Scope cleanup to this acceptance identity only.
            self.admin(f"KILL QUERY WHERE user='{self.username}' SYNC")
            self.admin(f"DROP USER IF EXISTS `{self.username}`")
        if self.created_database:
            self.admin(f"DROP DATABASE IF EXISTS `{self.database}` SYNC")
        require(int(self.admin(f"SELECT count() FROM system.databases WHERE name='{self.database}'")) == 0,
                "owned database was not removed")
        require(int(self.admin(f"SELECT count() FROM system.users WHERE name='{self.username}'")) == 0,
                "owned reader was not removed")
        final_rows = int(self.admin("SELECT coalesce(sum(rows),0) FROM system.parts "
                                   "WHERE active AND database='kelvo_bench' AND table='fact_events'"))
        require(self.preserved_rows is None or self.preserved_rows == final_rows,
                "existing ClickHouse dataset row count changed")
        self.record("fixture_cleanup_and_service_preservation", existing_benchmark_rows=final_rows,
                    owned_database_removed=True, owned_reader_removed=True,
                    existing_service_responds=self.admin("SELECT 1") == "1")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "bin/kelvo")
    parser.add_argument("--endpoint", required=True, help="Existing dedicated ClickHouse HTTP endpoint")
    parser.add_argument("--container", default="kelvo-clickhouse")
    parser.add_argument("--suite", choices=("all", "correctness", "exports"), default="all",
                        help="Separate throughput trials from correctness while VM builds finish")
    parser.add_argument("--report", type=Path, default=ROOT / "docs/evidence/federation-clickhouse.json")
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    require(re.fullmatch(r"[A-Za-z0-9_.-]+", args.container), "invalid container identifier")
    run = Acceptance(args)
    failure = None
    started = time.perf_counter()
    try:
        run.setup()
        if args.suite != "exports":
            run.correctness()
        if args.suite != "correctness":
            run.million_row_exports()
        if args.suite != "exports":
            run.boundaries()
            run.concurrency()
            run.cancellation()
            run.sandbox()
        require(sha256_file(args.binary) == run.binary_sha256,
                "binary changed during the acceptance run")
    except Exception as error:
        # Publish only controlled stage and exception type; private diagnostics
        # are available to the operator without entering the evidence artifact.
        failure = {"stage": run.stage, "exception_type": type(error).__name__}
        private_write(run.directory / "failure.txt", str(error))
    finally:
        try:
            run.cleanup()
        except Exception as error:
            failure = failure or {"stage": "cleanup", "exception_type": type(error).__name__}
            private_write(run.directory / "cleanup-failure.txt", str(error))
    report = {
        "acceptance": "kelvo-clickhouse-go-arrow-duckdb-federation-v1",
        "suite": args.suite,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "passed": failure is None, "failure": failure,
        "binary_sha256": run.binary_sha256,
        "clickhouse_version": run.version, "checks": run.checks,
        "elapsed_seconds": time.perf_counter() - started,
        "measurement_scope": "Synthetic acceptance fixture, tagged CLI coordinator and isolated worker; warm/uncontrolled caches. Proxy records generated SQL, dechunked source HTTP body bytes and decoded Arrow rows/buffers. Timings and 20ms /proc VmRSS samples are diagnostic observations, not production throughput claims.",
        "limitations": ["SQL capability guards are defense in depth; native DuckDB workers require OS isolation in shared deployments.",
                         "External process cleanup and upstream query cleanup do not measure internal Go/C handle counts; bridge lifetime tests cover those separately."]}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "checks": len(run.checks), "failure": failure}))
    return 0 if failure is None else 1


if __name__ == "__main__":
    sys.exit(main())
