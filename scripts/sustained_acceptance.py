#!/usr/bin/env python3
"""Isolated, bounded, failure-inclusive two-tenant Linux sustained acceptance.

Uses real Kelvo workers, Parquet and an owned three-broker JetStream fixture.
The outer process owns exactly one delegated service; no existing service or
source database is adopted. A smoke report never certifies the multi-hour gate.
"""
import argparse
import concurrent.futures
import errno
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import pwd
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import urllib.request
import uuid

import containment_acceptance as identity
import operational_acceptance as ops
import process_loss_acceptance as loss

ROOT = Path(__file__).resolve().parents[1]
TENANTS = ("a", "b")
REQUIRED = ("startup", "tenant_isolation", "saturation", "gateway_loss", "worker_loss", "broker_loss", "mixed_load", "slow_readers", "cancellations", "cleanup")
ROW_COUNT = 1_000_000
SLOW_ROWS = 16_384
SLOW_BODY_BOUND = 8 << 20
SLOW_QUERY = f"SELECT id, metric, tenant, repeat('x', 256) AS payload FROM ops_snapshot WHERE id < {SLOW_ROWS} ORDER BY id"
HISTOGRAM_BOUNDS = (.005, .01, .025, .05, .1, .25, .5, 1., 2., 5., 10., 20., 40., 80.)


def finite_number(value, minimum=0):
    try:
        return type(value) in (int, float) and math.isfinite(value) and value >= minimum
    except OverflowError:
        return False


def integer_count(value, minimum=0):
    return type(value) is int and value >= minimum


def histogram():
    return {"count": 0, "sum_seconds": 0., "maximum_seconds": 0., "buckets": [0] * (len(HISTOGRAM_BOUNDS) + 1)}


def observe(hist, seconds):
    ops.require(finite_number(seconds) and seconds <= 300, "INVALID_LATENCY")
    hist["count"] += 1
    hist["sum_seconds"] += seconds
    hist["maximum_seconds"] = max(hist["maximum_seconds"], seconds)
    for index, bound in enumerate((*HISTOGRAM_BOUNDS, float("inf"))):
        if seconds <= bound:
            hist["buckets"][index] += 1


def histogram_evidence(hist):
    result = dict(hist)
    result["bucket_upper_seconds"] = [*HISTOGRAM_BOUNDS, None]
    result["quantile_upper_bounds_seconds"] = {}
    for quantile in (.5, .95, .99):
        target = hist["count"] * quantile
        result["quantile_upper_bounds_seconds"][str(quantile)] = next((bound for bound, count in zip((*HISTOGRAM_BOUNDS, None), hist["buckets"]) if count >= target), None) if target else None
    return result


def safe_category(error):
    value = str(error)
    return value if isinstance(error, ops.AcceptanceError) and re.fullmatch(r"[A-Z_]+", value) else type(error).__name__


def write_json(path, value):
    raw = json.dumps(value, indent=2) + "\n"
    temporary = path.with_name(path.name + ".pending")
    with temporary.open("w") as target:
        target.write(raw)
        target.flush()
        os.fsync(target.fileno())
    os.replace(temporary, path)


def valid_histogram(value, expected):
    try:
        count = value["count"]
        buckets = value["buckets"]
        return (integer_count(expected, 1) and integer_count(count, 1) and count == expected
                and isinstance(buckets, list) and len(buckets) == len(HISTOGRAM_BOUNDS) + 1
                and all(type(number) is int and 0 <= number <= count for number in buckets)
                and buckets == sorted(buckets) and buckets[-1] == count
                and all(finite_number(value[key]) for key in ("sum_seconds", "maximum_seconds"))
                and value["maximum_seconds"] <= 300 and value["maximum_seconds"] <= value["sum_seconds"] <= count * 300
                and all(bound is None or finite_number(bound) for bound in value["bucket_upper_seconds"])
                and value["bucket_upper_seconds"] == [*HISTOGRAM_BOUNDS, None]
                and all(bound is None or finite_number(bound) for bound in value["quantile_upper_bounds_seconds"].values())
                and value["quantile_upper_bounds_seconds"] == histogram_evidence(value)["quantile_upper_bounds_seconds"])
    except (AttributeError, KeyError, OverflowError, TypeError, ValueError):
        return False


def reconcile(report, expected_revision):
    try:
        checks = report["checks"]
        if (not isinstance(expected_revision, str) or not re.fullmatch(r"[0-9a-f]{40}", expected_revision)
                or report.get("revision") != expected_revision
                or report.get("interrupted") is not False or len(checks) != len(REQUIRED)
                or {check["test"] for check in checks} != set(REQUIRED)
                or not all(check.get("passed") is True for check in checks)
                or report.get("failure") or report.get("source_unchanged") is not True
                or report.get("owned_service_removed") is not True or report.get("owned_cgroup_removed") is not True
                or type(report.get("service_exit_code")) is not int or report["service_exit_code"] != 0
                or set(report.get("binary_sha256", {})) != {"kelvo", "kelvo-landlock"}
                or not all(isinstance(value, str) and identity.HEX_SHA256.fullmatch(value) for value in report["binary_sha256"].values())
                or not identity.valid_source_identity(report["source"])
                or not identity.valid_source_identity(report["build_source"])
                or report["build_source"]["base_revision"] != expected_revision
                or report["source"]["base_revision"] != expected_revision
                or report["build_source"] != report["source"]):
            return False
        controls = report.get("control_response_counts")
        if (not isinstance(controls, dict) or not integer_count(controls.get("queued_status_http_200"), 1)
                or not all(isinstance(key, str) and re.fullmatch(r"(?:queued_status|status|cancel)_(?:http_[1-5][0-9]{2}|transport_error)", key)
                           and integer_count(value) for key, value in controls.items())):
            return False
        for check in checks:
            if check["test"] in ("gateway_loss", "worker_loss", "broker_loss"):
                if (not all(type(check.get(key)) is int and check[key] == 2 for key in ("running_before_fault", "queued_before_fault"))
                        or check.get("surviving_tenant_exact_result") is not True
                        or check.get("failed_attempt_result_rejected") is not True
                        or check.get("queued_handles_preserved") is not True):
                    return False
            if check['test'] == 'broker_loss' and not loss.lease_supervision.valid_evidence(check.get('lease_supervision')):
                return False
        windows = report["progress_windows"]
        if not finite_number(report["requested_seconds"], 60) or report["requested_seconds"] > 14400:
            return False
        if not windows or not finite_number(windows[-1]["end_seconds"]) or windows[-1]["end_seconds"] < report["requested_seconds"]:
            return False
        previous = 0.
        for window in windows:
            if (not finite_number(window["start_seconds"]) or not finite_number(window["end_seconds"])
                    or window["start_seconds"] != previous or window["end_seconds"] < previous
                    or not all(integer_count(window["deltas"][kind][tenant]) for kind in ("queries", "refreshes") for tenant in TENANTS)):
                return False
            if window["end_seconds"] - previous >= 60 and not all(window["deltas"][kind][tenant] > 0 for kind in ("queries", "refreshes") for tenant in TENANTS):
                return False
            previous = window["end_seconds"]
        mixed = next(check for check in checks if check["test"] == "mixed_load")
        if not (finite_number(mixed["observed_seconds"]) and mixed["observed_seconds"] >= report["requested_seconds"]
                and windows[-1]["end_seconds"] == mixed["observed_seconds"]):
            return False
        if report["mode"] not in ("smoke", "sustained") or (report["mode"] == "sustained" and (report["requested_seconds"] < 7200 or not identity.HEX_SHA256.fullmatch(report.get("prerequisite_smoke_sha256", "")))):
            return False
        workload = report["workload"]
        for tenant in TENANTS:
            if not all(type(workload[kind][tenant]) is int and workload[kind][tenant] > 0 for kind in ("queries", "slow_readers", "cancellations", "refreshes")):
                return False
        for kind, count_kind in (("query", "queries"), ("dispatch_observation", "queries"), ("first_byte", "slow_readers"), ("slow_delivery", "slow_readers"), ("cancel", "cancellations")):
            if not all(valid_histogram(report["latencies"][kind][tenant], workload[count_kind][tenant]) for tenant in TENANTS):
                return False
        process = report["process_sampling"]
        if (process["process_tree_rss_available"] is not True or type(process["samples"]) is not int or process["samples"] < 1
                or type(process["sampled_process_tree_rss_peak_bytes"]) is not int or process["sampled_process_tree_rss_peak_bytes"] <= 0
                or process["read_errors"]):
            return False
        samples = report["resources"]
        if (samples.get("final_cgroup_sample_after_shutdown") is not True or not integer_count(samples["samples"], 1) or samples["counter_errors"] or samples["resource_violations"]
                or any(type(samples["cgroup_memory_events_delta"].get(key)) is not int or samples["cgroup_memory_events_delta"][key] != 0 for key in ("oom", "oom_kill"))
                or not integer_count(samples.get("query_refresh_overlap_samples"), 1)
                or not integer_count(samples["peak_scratch_bytes"]) or samples["peak_scratch_bytes"] > 1024 << 20):
            return False
        cleanup = next(check for check in checks if check["test"] == "cleanup")
        return (all(type(cleanup[key]) is int and cleanup[key] == 0 for key in ("forced_application_kills", "observed_live_descendants", "worker_scratch_directories", "remaining_containment_records"))
                and cleanup["all_owned_brokers_stopped"] is True and cleanup["original_configurations_verified"] is True)
    except (AttributeError, KeyError, OverflowError, TypeError, ValueError, StopIteration):
        return False


class SlowConnection(ops.StrictHTTPSConnection):
    def connect(self):
        super().connect()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 << 10)


class SlowHandler(urllib.request.HTTPSHandler):
    def https_open(self, request):
        return self.do_open(SlowConnection, request, context=self._context)


PROC_DISAPPEARED_ERRNOS = frozenset((errno.ENOENT, errno.ESRCH))
PROCESS_DIAGNOSTIC_LIMIT = 32


def sampled_identity(pid, proc_root=Path("/proc"), diagnostic=None):
    """Classify known exit errno directly; never hide errors with an exists check."""
    def record(outcome, error_number=None):
        if diagnostic is not None:
            diagnostic("stat", outcome, error_number)
    try:
        raw = (proc_root / str(pid) / "stat").read_text()
    except OSError as error:
        if error.errno in PROC_DISAPPEARED_ERRNOS:
            record("disappeared", error.errno)
            return None, "disappeared"
        record("oserror", error.errno)
        return None, "read_error"
    except UnicodeError:
        record("malformed")
        return None, "read_error"
    try:
        prefix, suffix = raw.rsplit(") ", 1)
        fields = suffix.split()
        if int(prefix.split(" (", 1)[0]) != pid or len(fields[0]) != 1 or not fields[19].isascii() or not fields[19].isdigit() or len(fields[19]) > 20 or int(fields[19]) > (1 << 64) - 1:
            raise ValueError("invalid process stat")
        if fields[0] in ("Z", "X", "x"):
            record("exited")
            return None, "exited"
        return (pid, fields[19]), "live"
    except (IndexError, ValueError):
        record("malformed")
        return None, "read_error"


class LockedSamples(ops.Samples):
    def __init__(self, processes):
        super().__init__(processes)
        self.lock = threading.RLock()
        self.disappeared = 0
        self.diagnostics = {}
        self.diagnostic_overflow = 0
        self.sampling_started = time.monotonic()

    def diagnostic(self, operation, outcome, error_number=None):
        # Fixed labels and numeric errno only. No PID, path, command line,
        # source data, exception message, or unbounded per-event journal.
        if outcome in ("oserror", "malformed"):
            self.errors.add("PROCESS_IDENTITY_READ_FAILED" if operation == "stat" else "PROCESS_READ_FAILED")
        elif outcome in ("disappeared", "exited"):
            self.disappeared += 1
        error_number = error_number if type(error_number) is int and 0 < error_number < 4096 else None
        key = (operation, outcome, error_number)
        if key not in self.diagnostics:
            if len(self.diagnostics) >= PROCESS_DIAGNOSTIC_LIMIT:
                self.diagnostic_overflow += 1
                return
            self.diagnostics[key] = {"operation": operation, "outcome": outcome, "errno": error_number,
                                     "count": 0, "first_elapsed_seconds": round(time.monotonic() - self.sampling_started, 6)}
        item = self.diagnostics[key]
        item["count"] += 1
        item["last_elapsed_seconds"] = round(time.monotonic() - self.sampling_started, 6)

    def read_process_text(self, path, operation):
        try:
            return path.read_text()
        except OSError as error:
            self.diagnostic(operation, "disappeared" if error.errno in PROC_DISAPPEARED_ERRNOS else "oserror", error.errno)
        except UnicodeError:
            self.diagnostic(operation, "malformed")
        return None

    def sample(self):
        with self.lock:
            pending = [proc.pid for proc in list(self.processes.values()) if proc.poll() is None]
            seen, total = set(), 0
            while pending:
                pid = pending.pop()
                if pid in seen:
                    continue
                seen.add(pid)
                base = self.proc_root / str(pid)
                owned, _ = sampled_identity(pid, self.proc_root, self.diagnostic)
                if owned is None:
                    continue
                self.owned[pid] = owned
                if len(self.owned) > 200000:
                    self.errors.add("PROCESS_IDENTITY_BOUND_EXCEEDED")
                    break
                status = self.read_process_text(base / "status", "status")
                if status is None:
                    continue
                if re.search(r"^State:\s+[ZXx](?:\s|$)", status, re.MULTILINE):
                    self.diagnostic("status", "exited")
                    continue
                try:
                    rss = ops.rss_bytes(status)
                    if rss is None:
                        raise ValueError("missing process RSS")
                except (ValueError, ops.AcceptanceError):
                    self.diagnostic("status", "malformed")
                    continue
                self.rss_available = True
                total += rss
                try:
                    tasks = list((base / "task").iterdir())
                except OSError as error:
                    self.diagnostic("tasks", "disappeared" if error.errno in PROC_DISAPPEARED_ERRNOS else "oserror", error.errno)
                    continue
                for task in tasks:
                    if not task.name.isdigit():
                        continue
                    children = self.read_process_text(task / "children", "children")
                    if children is None:
                        continue
                    try:
                        child_ids = []
                        for field in children.split():
                            if not field.isascii() or not field.isdigit() or len(field) > 10:
                                raise ValueError("invalid child PID")
                            child = int(field)
                            if not 0 < child <= (1 << 31) - 1:
                                raise ValueError("invalid child PID")
                            child_ids.append(child)
                    except (ValueError, OverflowError):
                        self.diagnostic("children", "malformed")
                        continue
                    pending.extend(child_ids)
            self.samples += 1
            self.peak_rss = max(self.peak_rss, total)

    def evidence(self):
        with self.lock:
            return {"interval_ms": 50, "samples": self.samples, "process_tree_rss_available": self.rss_available,
                    "sampled_process_tree_rss_peak_bytes": self.peak_rss if self.rss_available else None,
                    "disappeared_or_exited_process_or_thread_samples": self.disappeared, "read_errors": sorted(self.errors),
                    "read_diagnostics": [dict(value) for value in self.diagnostics.values()],
                    "read_diagnostic_limit": PROCESS_DIAGNOSTIC_LIMIT, "read_diagnostic_overflow_events": self.diagnostic_overflow,
                    "scope": "Owned application parents and observed descendants; excludes brokers/Python. RSS sampling can miss short peaks and double-count shared mappings. Dedicated service cgroup counters include all fixture processes."}


class Campaign(loss.LossAcceptance):
    def __init__(self, args, artifact, group):
        ops.ROWS = ROW_COUNT
        super().__init__(args)
        self.samples = LockedSamples(self.processes)
        self.artifact, self.group = artifact, group
        self.stop_clients = threading.Event()
        self.pause_clients = threading.Event()
        self.client_lock = threading.Lock()
        self.active_clients = 0
        self.client_failures = []
        self.phase = "startup"
        self.data_lock = threading.RLock()
        self.workload = {kind: {tenant: 0 for tenant in TENANTS} for kind in ("queries", "slow_readers", "cancellations", "refreshes")}
        self.latencies = {kind: {tenant: histogram() for tenant in TENANTS} for kind in ("query", "dispatch_observation", "first_byte", "slow_delivery", "cancel")}
        self.monitor_stop = threading.Event()
        self.monitor_thread = None
        self.resource_samples = {"samples": 0, "counter_errors": [], "resource_violations": [], "peak_scratch_bytes": 0,
                                 "peak_cgroup_current_bytes": 0, "peak_cgroup_bytes": 0, "cgroup_memory_events_delta": {}, "node_metric_epochs": [], "query_refresh_overlap_samples": 0, "peak_artifact_bytes": 0}
        self.initial_events = ops.numeric_counters((group / "memory.events").read_text())
        self.last_metrics = {}
        self.window_counts = []
        self.progress_started = None
        self.containment_roots = {}

    def start_process(self, name, command):
        if name in ("a1", "b1"):
            config = self.directory / (name + ".yml")
            text = config.read_text()
            resources = {"max_concurrent": 2, "memory_mb": 1024, "baseline_mb": 128, "overhead_mb": 224,
                         "scratch_mb": 512, "query_reserve_slots": 1, "query_reserve_memory_mb": 352, "query_reserve_scratch_mb": 256}
            text = ops.replace_field(text, "resources", resources)
            state = self.directory / (name + "-containment")
            state.mkdir(mode=0o700, exist_ok=True)
            root = self.group / (name + "-jobs")
            root.mkdir(mode=0o755, exist_ok=True)
            self.containment_roots[name] = root
            containment = {"root": str(root), "state_directory": str(state), "native_overhead_mb": 64,
                           "parent_overhead_mb": 32, "max_processes": 64, "max_groups": 64, "cleanup_timeout": "5s"}
            if "\ncontainment:" in "\n" + text:
                text = ops.replace_field(text, "containment", containment)
            else:
                text += ops.yaml_document({"containment": containment})
            ops.cf.write(config, text)
            import yaml
            catalog = self.directory / (name + "-catalog.yml")
            data = yaml.safe_load(catalog.read_text())
            data["acceleration"]["datasets"][0]["limits"]["max_bytes"] = 64 << 20
            data["acceleration"]["datasets"][0]["refresh_interval"] = "10s"
            ops.cf.write(catalog, ops.yaml_document(data))
        return super().start_process(name, command)

    def startup(self):
        detail = super().startup()
        self.slow_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), SlowHandler(context=self.client))
        self.start_monitor()
        return {**detail, "query_memory_mb": 128, "native_tree_memory_mb": 192, "parent_arrow_allowance_mb": 160,
                "service_cpu_percent": 200, "service_memory_bytes": 6 << 30, "per_node_scratch_bytes": 512 << 20,
                "containment_enabled": True, "ports_exclusive_to_campaign": True, "fixture_config_sha256": dict(self.config_hashes)}

    def metric_sample(self, name):
        proc = self.processes.get(name)
        if proc is None or proc.poll() is not None:
            return
        code, raw = self.call("/metrics", node=name, timeout=2)
        ops.require(code == 200, "METRIC_SAMPLE_FAILED")
        match = re.search(rb'(?m)^kelvo_jobs_completed_total\{kind="refresh",outcome="success"\} ([0-9]+)$', raw)
        ops.require(match is not None, "REFRESH_METRIC_MISSING")
        value = int(match[1])
        epoch = (name, proc.pid)
        previous = self.last_metrics.get(epoch)
        if previous is None:
            self.resource_samples["node_metric_epochs"].append({"node": name, "ordinal": sum(item["node"] == name for item in self.resource_samples["node_metric_epochs"]),
                "initial_refreshes": value, "local_queue_histogram": []})
        else:
            ops.require(value >= previous, "REFRESH_COUNTER_REGRESSED")
            self.workload["refreshes"][name[0]] += value - previous
        self.last_metrics[epoch] = value
        latest = next(item for item in reversed(self.resource_samples["node_metric_epochs"]) if item["node"] == name)
        latest["final_refreshes"] = value
        latest["local_queue_histogram"] = [line for line in raw.decode().splitlines() if line.startswith("kelvo_job_queue_wait_seconds_")]

    def scratch_size(self):
        total = 0
        for name in ("a1", "b1"):
            root = self.directory / (name + "-runtime") / "managed"
            for current, dirs, files in os.walk(root, followlinks=False):
                dirs[:] = [name for name in dirs if not (Path(current) / name).is_symlink()]
                for name in files:
                    try:
                        stat = (Path(current) / name).lstat()
                        total += stat.st_size
                    except FileNotFoundError:
                        pass
        return total

    def sample_resources(self):
        with self.data_lock:
            state = self.resource_samples
            state["samples"] += 1
            try:
                current = int((self.group / "memory.current").read_text())
                peak = int((self.group / "memory.peak").read_text())
                events = ops.numeric_counters((self.group / "memory.events").read_text())
                state["peak_cgroup_current_bytes"] = max(current, state["peak_cgroup_current_bytes"])
                state["peak_cgroup_bytes"] = max(peak, state["peak_cgroup_bytes"])
                state["cgroup_memory_events_delta"] = {name: value - self.initial_events.get(name, 0) for name, value in events.items()}
                state["final_cpu_stat"] = ops.numeric_counters((self.group / "cpu.stat").read_text())
                state["peak_scratch_bytes"] = max(state["peak_scratch_bytes"], self.scratch_size())
                ops.require(state["peak_scratch_bytes"] <= 1024 << 20, "SCRATCH_BOUND_EXCEEDED")
            except Exception as error:
                state["counter_errors"].append(safe_category(error))
                self.stop_clients.set()
            for name in ("a1", "b1"):
                try:
                    resources = self.resources(name)
                    if resources["BackgroundActive"] > 0 and resources["Active"] > resources["BackgroundActive"]:
                        state["query_refresh_overlap_samples"] += 1
                    self.metric_sample(name)
                except Exception as error:
                    if not self.phase.startswith("fault_") and self.phase not in ("startup", "cleanup"):
                        state["resource_violations"].append(safe_category(error))
                        self.stop_clients.set()

    def start_monitor(self):
        def monitor():
            while not self.monitor_stop.is_set():
                self.sample_resources()
                self.monitor_stop.wait(1.)
        self.monitor_thread = threading.Thread(target=monitor, daemon=True)
        self.monitor_thread.start()

    def timed_query(self, tenant):
        start = time.monotonic()
        query_id = self.submit(tenant)
        submitted = time.monotonic()
        self.wait(lambda: self.state(query_id, tenant) == "assigned", 15)
        assigned = time.monotonic()
        code, raw, headers = self.call("/v1/queries/" + query_id + "/results", tenant, timeout=25, with_headers=True)
        ops.require(code == 200, "INTERACTIVE_RESULT_FAILED")
        ops.verify_completed_arrow(raw, headers, self.expected[tenant])
        finished = time.monotonic()
        with self.data_lock:
            self.workload["queries"][tenant] += 1
            observe(self.latencies["query"][tenant], finished - start)
            observe(self.latencies["dispatch_observation"][tenant], assigned - submitted)

    def slow_reader(self, tenant):
        import pyarrow as pa
        query_id = self.submit(tenant, SLOW_QUERY)
        request = urllib.request.Request(f"https://127.0.0.1:{14439+self.default_gateway}/v1/queries/{query_id}/results",
                                         headers={"Authorization": "Bearer " + self.env["KELVO_TOKEN_" + tenant.upper()]})
        started = time.monotonic()
        body = bytearray()
        with self.slow_opener.open(request, timeout=25) as response:
            ops.require(response.status == 200, "SLOW_READER_HTTP_FAILURE")
            ops.require(response.headers.get_all("Kelvo-Result-Completion") == ["durable-eos-v1"], "SLOW_READER_COMPLETION_REQUIRED")
            ops.require(response.headers.get_all("Transfer-Encoding") == ["chunked"] and response.headers.get("Content-Length") is None, "SLOW_READER_FRAMING_REQUIRED")
            first = response.read(1)
            body.extend(first)
            first_byte = time.monotonic() - started
            time.sleep(.25)
            ops.require(self.state(query_id, tenant) in ("running", "result_ready"), "DELIVERY_BACKPRESSURE_NOT_OBSERVED")
            while True:
                block = response.read(32 << 10)
                if not block:
                    break
                body.extend(block)
                ops.require(len(body) <= SLOW_BODY_BOUND, "SLOW_BODY_BOUND")
                time.sleep(.04)
        ops.require(body.endswith(ops.EOS), "SLOW_READER_INCOMPLETE_ARROW")
        source = pa.BufferReader(body)
        table = pa.ipc.open_stream(source).read_all()
        ops.require(source.tell() == len(body) and table.num_rows == SLOW_ROWS, "SLOW_READER_ROW_COUNT")
        ops.require(table.schema.names == ["id", "metric", "tenant", "payload"], "SLOW_READER_SCHEMA")
        ops.require([field.type for field in table.schema] == [pa.int64(), pa.int64(), pa.string(), pa.string()], "SLOW_READER_TYPES")
        ops.require(table.column("id").to_pylist() == list(range(SLOW_ROWS)), "SLOW_READER_IDS")
        ops.require(table.column("metric").to_pylist() == [i % 101 if i % 17 else None for i in range(SLOW_ROWS)], "SLOW_READER_NULLS")
        ops.require(table.column("tenant").to_pylist() == [tenant] * SLOW_ROWS and table.column("payload").to_pylist() == ["x" * 256] * SLOW_ROWS, "SLOW_READER_VALUES")
        with self.data_lock:
            self.workload["slow_readers"][tenant] += 1
            observe(self.latencies["first_byte"][tenant], first_byte)
            observe(self.latencies["slow_delivery"][tenant], time.monotonic() - started)

    def cancel_query(self, tenant):
        started = time.monotonic()
        query_id = self.submit(tenant, loss.SLOW_SQL)
        future = self.pool.submit(self.hold, tenant, query_id, self.default_gateway)
        self.pending.append(future)
        self.wait(lambda: self.state(query_id, tenant) == "running", 8)
        self.failed_attempt({"running": query_id, "future": future}, tenant, cancel=True)
        with self.data_lock:
            self.workload["cancellations"][tenant] += 1
            observe(self.latencies["cancel"][tenant], time.monotonic() - started)

    def inherited_lease_children(self, name):
        # Durable running state precedes native process startup. Observe the
        # actual inherited lease before injecting worker loss; never infer it
        # from the running job state or waive the positive descriptor proof.
        deadline = time.monotonic() + 3.
        while True:
            self.samples.sample()
            observed = super().inherited_lease_children(name)
            if observed or time.monotonic() >= deadline:
                return observed
            time.sleep(.02)

    def queued_result(self, item, tenant):
        # Observe recovery before claiming the existing result exactly once.
        # The query policy is20s, so the inherited12s read deadline is shorter
        # than permitted execution; retain a bounded30s delivery envelope.
        deadline = time.monotonic() + 25
        def assigned():
            remaining = deadline - time.monotonic()
            ops.require(remaining > 0, "CONDITION_DEADLINE")
            try:
                code, raw = self.call("/v1/queries/" + item["queued"], tenant, timeout=min(2, remaining))
            except (OSError, urllib.error.URLError, http.client.HTTPException):
                self.count_control("queued_status_transport_error")
                raise
            self.count_control("queued_status_http_" + str(code))
            ops.require(time.monotonic() <= deadline, "CONDITION_DEADLINE")
            # A broker outage can make a read temporarily unavailable even
            # after /ready succeeds. Count every observation and require the
            # same admitted handle to recover within the existing deadline.
            if code in (429, 503):
                return False
            ops.require(code == 200, "ADMITTED_QUEUED_STATUS_LOST")
            state = json.loads(raw)["state"]
            ops.require(state in ("queued", "assigned"), "ADMITTED_QUEUED_STATE_LOST")
            return state == "assigned"
        self.wait(assigned, 25)
        code, raw, headers = self.call("/v1/queries/" + item["queued"] + "/results", tenant, timeout=30, with_headers=True)
        ops.require(code == 200, "ADMITTED_QUEUED_QUERY_LOST")
        ops.verify_completed_arrow(raw, headers, self.expected[tenant])
        self.wait(lambda: self.resources(tenant + "1")["Active"] == self.resources(tenant + "1")["BackgroundActive"])

    def isolation(self):
        query_id = self.submit("a")
        for suffix, body in (("", None), ("/results", None), ("/cancel", {})):
            code, _ = self.call("/v1/queries/" + query_id + suffix, "b", body=body)
            ops.require(code == 404, "CROSS_TENANT_HANDLE_VISIBLE")
        code, raw, headers = self.retry("/v1/queries/" + query_id + "/results", "a", with_headers=True)
        ops.require(code == 200, "ISOLATION_OWNER_LOST_RESULT")
        ops.verify_completed_arrow(raw, headers, self.expected["a"])
        return {"foreign_status_results_cancel_rejected": True, "owner_exact_result": True}

    def saturation(self):
        ids = []
        try:
            for _ in range(8):
                ids.append(self.submit("a"))
            code, _ = self.call("/v1/queries", "a", {"mode": "federated", "sources": ["ops_snapshot"], "sql": ops.SQL})
            ops.require(code == 429, "TENANT_QUEUE_NOT_BOUNDED")
            ops.require(self.call("/health")[0] == 200, "SATURATED_LIVENESS_FAILED")
            self.query("b")
        finally:
            for query_id in ids:
                self.cancel_running(query_id, "a")
        self.wait(lambda: self.resources("a1")["Active"] == self.resources("a1")["BackgroundActive"], 15)
        for tenant in TENANTS:
            self.query(tenant)
        return {"admitted_handles": 8, "rejected_http": 429, "surviving_tenant_exact_result": True, "cancel_recovered": True}

    def checkpoint(self, elapsed):
        with self.data_lock:
            total = 0
            for root in (self.artifact, self.directory):
                for current, dirs, files in os.walk(root, followlinks=False):
                    dirs[:] = [name for name in dirs if not (Path(current) / name).is_symlink()]
                    for name in files:
                        try:
                            total += (Path(current) / name).lstat().st_size
                        except FileNotFoundError:
                            pass
            self.resource_samples["peak_artifact_bytes"] = max(total, self.resource_samples["peak_artifact_bytes"])
            ops.require(total <= 8 << 30, "OWNED_ARTIFACT_BOUND_EXCEEDED")
            previous = self.window_counts[-1] if self.window_counts else {"end_seconds": 0., "counts": {kind: {tenant: 0 for tenant in TENANTS} for kind in self.workload}}
            interval = elapsed - previous["end_seconds"]
            deltas = {kind: {tenant: self.workload[kind][tenant] - previous["counts"][kind][tenant] for tenant in TENANTS} for kind in self.workload}
            if interval >= 60:
                ops.require(all(deltas["queries"][tenant] > 0 and deltas["refreshes"][tenant] > 0 for tenant in TENANTS), "TENANT_PROGRESS_WINDOW_EMPTY")
            self.window_counts.append({"start_seconds": previous["end_seconds"], "end_seconds": round(elapsed, 3), "deltas": deltas, "counts": json.loads(json.dumps(self.workload))})
            point = {"elapsed_seconds": round(elapsed, 3), "phase": self.phase, "counts": self.workload,
                     "cgroup_peak_bytes": self.resource_samples["peak_cgroup_bytes"], "scratch_peak_bytes": self.resource_samples["peak_scratch_bytes"]}
            with (self.artifact / "progress.jsonl").open("a") as target:
                target.write(json.dumps(point) + "\n")
                target.flush()
                os.fsync(target.fileno())
            write_json(self.artifact / "checkpoint.json", self.evidence())
            print(json.dumps(point), flush=True)

    def quiesce(self):
        with self.client_lock:
            self.pause_clients.set()
        self.wait(lambda: self.active_clients == 0, 30)
        self.wait(lambda: all(self.resources(name)["Active"] == self.resources(name)["BackgroundActive"] for name in ("a1", "b1")), 20)

    def mixed(self):
        started = time.monotonic()
        self.progress_started = started
        deadline = started + self.args.duration
        self.phase = "mixed"
        def client(tenant):
            while not self.stop_clients.is_set() and time.monotonic() < deadline:
                with self.client_lock:
                    admitted = not self.pause_clients.is_set()
                    if admitted:
                        self.active_clients += 1
                if not admitted:
                    self.stop_clients.wait(.05)
                    continue
                try:
                    self.timed_query(tenant)
                except Exception as error:
                    with self.data_lock:
                        self.client_failures.append(safe_category(error))
                    ops.cf.write(self.artifact / ("client-" + tenant + "-failure.log"), traceback.format_exc())
                    self.stop_clients.set()
                finally:
                    with self.client_lock:
                        self.active_clients -= 1
                self.stop_clients.wait(.5)
        tasks = [threading.Thread(target=client, args=(tenant,), daemon=True) for tenant in TENANTS]
        for task in tasks:
            task.start()
        events = [(self.args.duration * ratio, name, callback) for ratio, name, callback in (
            (.1, "saturation", self.saturation), (.25, "gateway_loss", self.gateway_loss), (.5, "worker_loss", self.worker_loss), (.75, "broker_loss", self.broker_loss))]
        slow_due, cancel_due, checkpoint_due = 0., 8., 0.
        slow_index = cancel_index = 0
        try:
            while time.monotonic() < deadline and not self.stop_clients.is_set():
                elapsed = time.monotonic() - started
                if events and elapsed >= events[0][0]:
                    _, name, callback = events.pop(0)
                    self.quiesce()
                    self.phase = "fault_" + name
                    passed = self.record(name, callback)
                    self.phase = "mixed"
                    self.pause_clients.clear()
                    ops.require(passed, "FAULT_GATE_FAILED")
                elif elapsed >= slow_due:
                    self.slow_reader(TENANTS[slow_index % 2])
                    slow_index += 1
                    slow_due = elapsed + 30.
                elif elapsed >= cancel_due:
                    # Terminal handles share bounded storage with later jobs.
                    # Stop new admission until the no-replay assertion finishes
                    # so terminal-slot reuse cannot turn its expected409 into404.
                    self.quiesce()
                    try:
                        self.cancel_query(TENANTS[cancel_index % 2])
                    finally:
                        self.pause_clients.clear()
                    cancel_index += 1
                    cancel_due = elapsed + 30.
                if elapsed >= checkpoint_due:
                    self.checkpoint(time.monotonic() - started)
                    checkpoint_due = time.monotonic() - started + 60.
                self.stop_clients.wait(.2)
        finally:
            self.stop_clients.set()
            for task in tasks:
                task.join(timeout=30)
            ops.require(all(not task.is_alive() for task in tasks), "CLIENT_THREAD_DID_NOT_STOP")
        elapsed = time.monotonic() - started
        self.checkpoint(elapsed)
        ops.require(not self.client_failures, "INTERACTIVE_CLIENT_FAILED")
        ops.require(elapsed >= self.args.duration and not events, "SUSTAINED_INTERVAL_INCOMPLETE")
        return {"observed_seconds": round(elapsed, 3), "client_threads": 2, "pace_seconds_after_query": .5,
                "scheduled_fault_gates": 4, "fault_phase_workload": "Normal clients briefly quiesce for control gates and explicit cancellation checks; each fault gate establishes its own two running and two queued tenant queries before disruption.", "query_refresh_overlap_samples": self.resource_samples["query_refresh_overlap_samples"]}

    def evidence(self):
        with self.data_lock:
            return {"workload": json.loads(json.dumps(self.workload)), "resources": json.loads(json.dumps(self.resource_samples)),
                    "control_response_counts": dict(self.control_response_counts), "broker_supervision": self.broker_supervision,
                    "latencies": {kind: {tenant: histogram_evidence(value) for tenant, value in values.items()} for kind, values in self.latencies.items()},
                    "latency_scope": "Client elapsed times and dispatch observation upper bounds (50ms polling), not exact distributed queue attribution; node admission histograms exclude dispatch.",
                    "client_failures": list(self.client_failures), "progress_windows": json.loads(json.dumps(self.window_counts)), "process_sampling": self.samples.evidence(),
                    "checks": list(self.checks), "phase": self.phase}

    def cleanup(self):
        self.phase = "cleanup"
        self.stop_clients.set()
        self.monitor_stop.set()
        if self.monitor_thread:
            self.monitor_thread.join(timeout=8)
            ops.require(not self.monitor_thread.is_alive(), "RESOURCE_MONITOR_DID_NOT_STOP")
        detail = super().cleanup()
        remaining = sum(path.name != ".kelvo-containment.lock" for name in ("a1", "b1") for path in (self.directory / (name + "-containment")).iterdir())
        ops.require(remaining == 0 and all(not list(root.iterdir()) or not any(item.is_dir() for item in root.iterdir()) for root in self.containment_roots.values()), "CONTAINMENT_CUSTODY_REMAINS")
        for broker in self.brokers:
            ops.require(self.broker_alive(broker), "BROKER_CLEANUP_IDENTITY_MISMATCH")
            owned = ops.proc_identity(broker["pid"])
            ops.require(owned is not None, "BROKER_CLEANUP_IDENTITY_MISMATCH")
            ops.signal_owned(owned, signal.SIGTERM)
            def gone():
                try:
                    os.waitpid(broker["pid"], os.WNOHANG)
                except ChildProcessError:
                    pass
                return ops.proc_identity(broker["pid"]) != owned
            self.wait(gone, 10)
        events = ops.numeric_counters((self.group / "memory.events").read_text())
        self.resource_samples["cgroup_memory_events_delta"] = {name: value - self.initial_events.get(name, 0) for name, value in events.items()}
        self.resource_samples["peak_cgroup_bytes"] = max(self.resource_samples["peak_cgroup_bytes"], int((self.group / "memory.peak").read_text()))
        self.resource_samples["final_cpu_stat"] = ops.numeric_counters((self.group / "cpu.stat").read_text())
        self.resource_samples["final_cgroup_sample_after_shutdown"] = True
        return {**detail, "remaining_containment_records": remaining, "all_owned_brokers_stopped": True}


def provision_fixture(fixture, nats_archive=None):
    ops.cf.DIR = fixture
    ops.cf.provision(nats_archive)
    import yaml
    for path in [*fixture.glob("gateway*.yml"), fixture / "init.yml", *(fixture / (name + ".yml") for name in ("a1", "a2", "b1"))]:
        data = yaml.safe_load(path.read_text())
        policies = [tenant["policy"] for tenant in data["tenants"]] if "tenants" in data else [data["policy"]]
        for policy in policies:
            policy["job_ttl"] = "60s"
            policy["limits"].update(memory_mb=128, max_bytes=64 << 20, timeout="20s", max_temp_mb=128)
        # The base fixture has caller-defined identifier map keys (for example
        # token environment names). Its full document is broader than the
        # deliberately restricted emitter used for generated policy blocks.
        ops.cf.write(path, yaml.safe_dump(data, sort_keys=False))


def inside(args):
    artifact = Path(args.artifact)
    group = Path("/sys/fs/cgroup/system.slice") / (args.unit + ".service")
    report = {"checks": [], "interrupted": False, "failure": "SETUP_INCOMPLETE"}
    campaign = None
    try:
        ops.require(Path("/proc/self/cgroup").read_text().strip() == "0::/system.slice/" + args.unit + ".service", "SERVICE_OWNERSHIP_MISMATCH")
        ops.require(identity.zero_capabilities(Path("/proc/self/status").read_text()), "CAPABILITIES_NOT_ZERO")
        supervisor = group / "supervisor"
        supervisor.mkdir()
        (supervisor / "cgroup.procs").write_text(str(os.getpid()))
        (group / "cgroup.subtree_control").write_text("+cpu +memory +pids")
        fixture = artifact / "fixture"
        args.fixture = str(fixture)
        provision_fixture(fixture, args.nats_archive)
        campaign = Campaign(args, artifact, group)
        report.pop("failure")
        if campaign.record("startup", campaign.startup) and campaign.record("tenant_isolation", campaign.isolation):
            campaign.record("mixed_load", campaign.mixed)
        for name, kind in (("slow_readers", "slow_readers"), ("cancellations", "cancellations")):
            campaign.record(name, lambda kind=kind: (ops.require(all(campaign.workload[kind][tenant] > 0 for tenant in TENANTS), "WORKLOAD_CLASS_DID_NOT_PROGRESS") or {"counts": dict(campaign.workload[kind])}))
    except BaseException as error:
        report["failure"] = safe_category(error)
        report["interrupted"] = isinstance(error, (KeyboardInterrupt, InterruptedError))
        ops.cf.write(artifact / "setup-failure.log", traceback.format_exc())
    finally:
        if campaign is not None:
            campaign.record("cleanup", campaign.cleanup)
            report.update(campaign.evidence())
        for name in REQUIRED:
            if not any(check["test"] == name for check in report["checks"]):
                report["checks"].append({"test": name, "passed": False, "category": "PREREQUISITE_FAILED"})
        write_json(artifact / "inside.json", report)
    return 0 if not report.get("failure") and all(item.get("passed") is True for item in report["checks"]) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default=str(ROOT / "bin/kelvo"))
    parser.add_argument("--sandbox", default=str(ROOT / "bin/kelvo-landlock"))
    parser.add_argument("--go", default="go")
    parser.add_argument("--expected-revision", required=True, help="Exact committed source revision independently selected before build")
    parser.add_argument("--nats-archive", help="Optional cached archive, verified against the fixture's pinned digest")
    parser.add_argument("--duration", type=float, default=7200.)
    parser.add_argument("--build-source", type=Path, default=ROOT / "artifacts/sustained-build-source.json")
    parser.add_argument("--mode", choices=("smoke", "sustained"), default="sustained")
    parser.add_argument("--smoke-report", type=Path, help="Required for sustained mode: a fully passing smoke with identical source and binaries")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--artifact", help=argparse.SUPPRESS)
    parser.add_argument("--unit", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_revision):
        parser.error("--expected-revision requires a full lowercase40-character Git commit")
    args.managed_scratch = True
    if not (sys.platform == "linux" and os.geteuid() != 0 and 60 <= args.duration <= 14400 and (args.mode == "smoke" or args.duration >= 7200)):
        parser.error("non-root Linux and bounded duration required; sustained mode needs at least7200seconds")
    if args.inside:
        return inside(args)
    args.output = args.output.resolve()
    ops.require(not args.output.exists(), "OUTPUT_ALREADY_EXISTS")
    artifact = ROOT / "artifacts/sustained-private" / ("run-" + uuid.uuid4().hex[:12])
    artifact.mkdir(mode=0o700, parents=True)
    unit = "kelvo-sustained-" + uuid.uuid4().hex[:12]
    report = {"schema": 1, "requested_seconds": args.duration, "mode": args.mode, "passed": False,
              "checked_at": datetime.now(timezone.utc).isoformat(), "revision": args.expected_revision, "checks": [],
              "scope": "Dedicated service on a shared test VM; synthetic Parquet correctness/lifecycle, not source-provider, WAN or multi-host capacity",
              "resource_budget": {"cpu_percent": 200, "memory_bytes": 6 << 30, "swap_bytes": 0, "tasks": 512},
              "interrupted": False}
    process = None
    started = time.monotonic()
    try:
        report["build_source"] = json.loads(args.build_source.read_text())
        ops.require(identity.valid_source_identity(report["build_source"]), "BUILD_SOURCE_MANIFEST_INVALID")
        report["source"] = identity.source_manifest(ROOT)
        ops.require(report["source"]["base_revision"] == args.expected_revision, "FROZEN_REVISION_REQUIRED")
        ops.require(report["build_source"] == report["source"], "EXACT_BUILD_SOURCE_REQUIRED")
        report["binary_sha256"] = {"kelvo": identity.digest(args.binary), "kelvo-landlock": identity.digest(args.sandbox)}
        if args.mode == "sustained":
            ops.require(args.smoke_report is not None, "MATCHED_PASSING_SMOKE_REQUIRED")
            smoke = json.loads(args.smoke_report.read_text())
            ops.require(smoke.get("mode") == "smoke" and smoke.get("passed") is True and reconcile(smoke, args.expected_revision)
                        and smoke["source"] == report["source"] and smoke["binary_sha256"] == report["binary_sha256"], "MATCHED_PASSING_SMOKE_REQUIRED")
            report["prerequisite_smoke_sha256"] = identity.digest(args.smoke_report)
        command = ["sudo", "-n", "systemd-run", "--unit=" + unit, "--uid=" + pwd.getpwuid(os.geteuid()).pw_name,
                   "--property=Delegate=yes", "--property=PrivateNetwork=yes", "--property=CPUQuota=200%", "--property=MemoryMax=6G", "--property=MemorySwapMax=0",
                   "--property=TasksMax=512", "--property=NoNewPrivileges=yes", "--property=CapabilityBoundingSet=",
                   "--property=AmbientCapabilities=", "--collect", "--wait", "--pipe", sys.executable, str(Path(__file__).resolve()),
                   "--inside", "--unit", unit, "--artifact", str(artifact), "--mode", args.mode, "--duration", str(args.duration),
                   "--expected-revision", args.expected_revision,
                   "--binary", str(Path(args.binary).resolve()), "--sandbox", str(Path(args.sandbox).resolve()), "--go", args.go,
                   "--output", str(args.output)]
        if args.nats_archive:
            command.extend(["--nats-archive", str(Path(args.nats_archive).resolve())])
        with (artifact / "service.log").open("w") as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            print(json.dumps({"unit": unit, "control_pid": process.pid, "private_artifact": str(artifact), "started_monotonic": started, "requested_seconds": args.duration}), flush=True)
            report["service_exit_code"] = process.wait(timeout=args.duration + 300)
        if (artifact / "inside.json").exists():
            report.update(json.loads((artifact / "inside.json").read_text()))
        else:
            report["failure"] = "INNER_REPORT_MISSING"
    except BaseException as error:
        report["failure"] = safe_category(error)
        report["interrupted"] = isinstance(error, (KeyboardInterrupt, InterruptedError))
    finally:
        report.update(identity.cleanup_owned(unit))
        if process is not None:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.terminate()
                report["failure"] = "SERVICE_CONTROL_UNREAPED"
        try:
            report["source_unchanged"] = report.get("source") == identity.source_manifest(ROOT)
        except Exception:
            report["source_unchanged"] = False
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        report["passed"] = reconcile(report, args.expected_revision)
        report["multi_hour_gate_passed"] = report["passed"] and report["mode"] == "sustained"
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as output:
            json.dump(report, output, indent=2)
            output.write("\n")
    print(json.dumps({key: report.get(key) for key in ("passed", "multi_hour_gate_passed", "mode", "failure", "service_exit_code", "elapsed_seconds")}), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
