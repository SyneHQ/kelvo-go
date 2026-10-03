#!/usr/bin/env python3
"""VM-only operational acceptance against an idle, provisioned cluster fixture.

Installs nothing. Copies application YAML; never rewrites original configuration
or stops brokers. Public evidence has no credentials, SQL, logs or private paths.
"""
import argparse
import concurrent.futures
from datetime import datetime, timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.request

import cluster_fixture as cf

ROOT = Path(__file__).resolve().parents[1]
EOS = b"\xff\xff\xff\xff\0\0\0\0"
MAX_BODY = 1 << 20
ROWS = 100_000
REQUIRED = ("startup", "oversized_reservation_rejected", "mixed_query_refresh",
            "saturated_liveness_and_recovery", "accepted_query_during_node_drain",
            "sandboxed_resource_and_source_lease_failures", "cleanup")
SQL = """WITH grouped AS (
SELECT tenant, CAST(id % 10 AS BIGINT) AS bucket, COUNT(*)::BIGINT AS n,
COUNT(metric)::BIGINT AS present, SUM(metric)::BIGINT AS total
FROM ops_snapshot GROUP BY tenant, id % 10)
SELECT tenant,bucket,n,present,total FROM grouped ORDER BY bucket"""


class AcceptanceError(RuntimeError):
    pass


def require(condition, code):
    if not condition:
        raise AcceptanceError(code)



class StrictHTTPResponse(http.client.HTTPResponse):
    """Require complete chunk framing for our fixture's trailer-free responses.

    The stdlib accepts EOF instead of the final trailer CRLF and discards
    chunk separators without checking their value. Neither can certify a
    completed durable result. Kelvo's fixture emits no extensions or trailers.
    """
    def _read_next_chunk_size(self):
        line = self.fp.readline(8193)
        require(re.fullmatch(rb"[0-9a-fA-F]+\r\n", line) is not None, "INVALID_HTTP_CHUNK_SIZE")
        return int(line[:-2], 16)

    def _read_and_discard_trailer(self):
        require(self.fp.readline(8193) == b"\r\n", "INCOMPLETE_HTTP_CHUNK_TRAILER")

    def _get_chunk_left(self):
        remaining = self.chunk_left
        if not remaining:
            if remaining is not None:
                require(self._safe_read(2) == b"\r\n", "INVALID_HTTP_CHUNK_SEPARATOR")
            remaining = self._read_next_chunk_size()
            if remaining == 0:
                self._read_and_discard_trailer()
                self._close_conn()
                remaining = None
            self.chunk_left = remaining
        return remaining


class StrictHTTPSConnection(http.client.HTTPSConnection):
    response_class = StrictHTTPResponse


class StrictHTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self, request):
        return self.do_open(StrictHTTPSConnection, request, context=self._context)


def read_http_response(response):
    """Bound the body while rejecting ambiguous headers and incomplete framing."""
    headers = {}
    for name in response.headers.keys():
        name = name.lower()
        if name in headers:
            continue
        values = response.headers.get_all(name)
        if name in ("content-length", "transfer-encoding", "kelvo-result-completion"):
            require(len(values) == 1, "DUPLICATE_HTTP_PROTOCOL_HEADER")
        headers[name] = ", ".join(values)
    length = headers.get("content-length")
    transfer = headers.get("transfer-encoding")
    require(not (length is not None and transfer is not None), "AMBIGUOUS_HTTP_FRAMING")
    if length is not None:
        require(re.fullmatch(r"[0-9]+", length) is not None, "INVALID_HTTP_CONTENT_LENGTH")
        length = int(length)
        require(length <= MAX_BODY, "HTTP_RESPONSE_BOUND")
    if transfer is not None:
        require(transfer.lower() == "chunked", "UNSUPPORTED_HTTP_TRANSFER_ENCODING")
    if "kelvo-result-completion" in headers:
        require(length is not None or transfer is not None, "HTTP_COMPLETION_FRAMING_REQUIRED")
    raw = response.read(MAX_BODY+1)
    require(len(raw) <= MAX_BODY, "HTTP_RESPONSE_BOUND")
    # A bounded HTTPResponse.read(amt) can return short without IncompleteRead.
    if length is not None:
        require(len(raw) == length, "INCOMPLETE_HTTP_CONTENT_LENGTH")
    return raw, headers


def duration_value(raw):
    value = float(raw)
    if not 30 <= value <= 14_400:
        raise argparse.ArgumentTypeError("duration must be between 30 and 14400 seconds")
    return value


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def yaml_document(value, level=0):
    """Emit our generated YAML with scalar or mapping sequence members."""
    pad = "  " * level
    if isinstance(value, dict):
        lines = []
        for key, item in value.items():
            require(isinstance(key, str) and re.fullmatch(r"[a-z_]+", key), "INVALID_FIXTURE_KEY")
            if isinstance(item, (dict, list)) and item:
                lines.append(pad+key+":\n"+yaml_document(item, level+1))
            else:
                lines.append(pad+key+": "+json.dumps(item)+"\n")
        return "".join(lines)
    if isinstance(value, list):
        return "".join(pad+"-\n"+yaml_document(item, level+1) if isinstance(item, (dict, list)) and item
                       else pad+"- "+json.dumps(item)+"\n" for item in value)
    raise AcceptanceError("INVALID_FIXTURE_YAML_SHAPE")


def replace_field(text, field, value):
    """Only replace one top-level block in the fixture emitter's known YAML."""
    pattern = re.compile(r"(?m)^" + re.escape(field) + r":.*(?:\n[ \t]+[^\n]*)*\n?")
    matches = list(pattern.finditer(text))
    require(len(matches) == 1, "AMBIGUOUS_FIXTURE_FIELD")
    return pattern.sub(lambda _: yaml_document({field: value}), text, count=1)


def proc_identity(pid, proc_root=Path("/proc")):
    try:
        suffix = (proc_root / str(pid) / "stat").read_text().rsplit(") ", 1)[1].split()
        if suffix[0] in ("Z", "X"):
            return None
        return (int(pid), suffix[19])
    except (OSError, IndexError, ValueError):
        return None


def signal_owned(identity, sig):
    """Pin the process before signalling, preventing PID-reuse cleanup races."""
    pid = identity[0]
    if proc_identity(pid) != identity:
        return
    try:
        descriptor = os.pidfd_open(pid)
    except ProcessLookupError:
        return
    try:
        if proc_identity(pid) == identity:
            signal.pidfd_send_signal(descriptor, sig)
    except ProcessLookupError:
        pass
    finally:
        os.close(descriptor)


def rss_bytes(raw):
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[0] == "VmRSS:" and parts[2] == "kB":
            value = int(parts[1])
            require(value >= 0, "INVALID_RSS")
            return value * 1024
    return None


def numeric_counters(raw):
    result = {}
    for line in raw.splitlines():
        parts = line.split()
        require(len(parts) == 2 and parts[1].isdigit(), "INVALID_CGROUP_COUNTER")
        result[parts[0]] = int(parts[1])
    return result


def expected_rows(tenant):
    groups = [{"tenant": tenant, "bucket": k, "n": 0, "present": 0, "total": 0} for k in range(10)]
    for i in range(ROWS):
        row = groups[i % 10]
        row["n"] += 1
        if i % 17:
            row["present"] += 1
            row["total"] += i % 101
    return groups


def verify_arrow(body, expected):
    import pyarrow as pa
    require(len(body) <= MAX_BODY and body.endswith(EOS), "INCOMPLETE_ARROW")
    buffer = pa.BufferReader(body)
    reader = pa.ipc.open_stream(buffer)
    table = reader.read_all()
    require(buffer.tell() == len(body), "TRAILING_ARROW_DATA")
    require(table.to_pylist() == expected, "INCORRECT_ARROW_VALUES")
    expected_types = {"tenant": pa.string(), "bucket": pa.int64(), "n": pa.int64(),
                      "present": pa.int64(), "total": pa.int64()}
    if expected and set(expected[0]) == set(expected_types):
        require(all(table.schema.field(key).type == kind for key, kind in expected_types.items()),
                "INCORRECT_ARROW_TYPES")
    return table.num_rows


def verify_completed_arrow(body, headers, expected):
    require(headers.get("kelvo-result-completion") == "durable-eos-v1", "DURABLE_COMPLETION_CAPABILITY_REQUIRED")
    # Caller obtains body only after a successful complete HTTP read. The
    # advertised protocol alone is not success; exact Arrow/EOS checks follow.
    return verify_arrow(body, expected)


def reconcile(report):
    if report.get("interrupted") is not False:
        return False
    checks = report.get("checks", [])
    names = [item.get("test") for item in checks]
    if not (len(names) == len(REQUIRED) and set(names) == set(REQUIRED)
            and all(item.get("passed") is True for item in checks)):
        return False
    mixed = next(item for item in checks if item["test"] == "mixed_query_refresh")
    cleanup = next(item for item in checks if item["test"] == "cleanup")
    try:
        counts = mixed["client_query_counts"]
        return (30 <= report["requested_duration_seconds"] <= 14_400
                and mixed["observed_seconds"] >= report["requested_duration_seconds"]
                and len(counts) == 4 and all(type(n) is int and n > 0 for n in counts)
                and sum(counts) == mixed["completed_queries"]
                and mixed["completion_protocol"] == "durable-eos-v1"
                and type(mixed["reclaimed_status_after_certified_delivery"]) is int
                and 0 <= mixed["reclaimed_status_after_certified_delivery"] <= mixed["completed_queries"]
                and mixed["query_refresh_overlap_samples"] > 0
                and set(mixed["successful_refresh_deltas"]) == {"a1", "b1"}
                and all(n > 0 for n in mixed["successful_refresh_deltas"].values())
                and cleanup["forced_application_kills"] == 0
                and cleanup["observed_live_descendants"] == 0
                and cleanup["worker_scratch_directories"] == 0
                and cleanup["broker_processes_untouched"] is True
                and cleanup["original_configurations_untouched"] is True)
    except (KeyError, TypeError, ValueError):
        return False


class Samples:
    def __init__(self, processes, proc_root=Path("/proc"), cgroup_root=Path("/sys/fs/cgroup")):
        self.processes = processes
        self.proc_root = proc_root
        self.cgroup_root = cgroup_root
        self.owned = {}
        self.samples = 0
        self.peak_rss = 0
        self.rss_available = False
        self.errors = set()
        self.cgroups = {}
        self.stop = threading.Event()
        self.thread = None

    def sample(self):
        pending = [proc.pid for proc in list(self.processes.values()) if proc.poll() is None]
        seen, total = set(), 0
        sampled_cgroups = set()
        while pending:
            pid = pending.pop()
            if pid in seen:
                continue
            seen.add(pid)
            identity = proc_identity(pid, self.proc_root)
            if identity is None:
                continue
            self.owned[pid] = identity
            base = self.proc_root / str(pid)
            try:
                rss = rss_bytes((base / "status").read_text())
                if rss is not None:
                    self.rss_available = True
                    total += rss
                # A Go process may launch children from any OS thread. The
                # thread-group leader's children file alone misses them.
                for task in (base / "task").iterdir():
                    if not task.name.isdigit():
                        continue
                    try:
                        pending.extend(int(value) for value in (task / "children").read_text().split())
                    except FileNotFoundError:
                        pass  # The thread can exit between enumeration and read.
                    except (OSError, ValueError):
                        self.errors.add("PROCESS_CHILDREN_SAMPLE_UNAVAILABLE")
                for line in (base / "cgroup").read_text().splitlines():
                    if line.startswith("0::"):
                        relative = Path(line[3:].lstrip("/"))
                        if ".." in relative.parts:
                            self.errors.add("CGROUP_PATH_OUTSIDE_NAMESPACE")
                            continue
                        path = self.cgroup_root / relative
                        if path not in sampled_cgroups:
                            sampled_cgroups.add(path)
                            self.sample_cgroup(path)
            except (OSError, ValueError, AcceptanceError):
                self.errors.add("PROCESS_SAMPLE_UNAVAILABLE")
        self.samples += 1
        self.peak_rss = max(self.peak_rss, total)

    def sample_cgroup(self, path):
        key = str(path)
        state = self.cgroups.setdefault(key, {"samples": 0, "peak_current_bytes": 0})
        try:
            current = int((path / "memory.current").read_text())
            events = numeric_counters((path / "memory.events").read_text())
            state.setdefault("initial_events", events)
            state["final_events"] = events
            state["peak_current_bytes"] = max(state["peak_current_bytes"], current)
            state["samples"] += 1
        except (OSError, ValueError, AcceptanceError):
            self.errors.add("CGROUP_MEMORY_COUNTERS_UNAVAILABLE")

    def start(self):
        def loop():
            while not self.stop.is_set():
                self.sample()
                self.stop.wait(0.05)
        self.thread = threading.Thread(target=loop, daemon=True)
        self.thread.start()

    def finish(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=3)
            require(not self.thread.is_alive(), "SAMPLER_DID_NOT_STOP")

    def evidence(self):
        groups = []
        for state in self.cgroups.values():
            if "initial_events" not in state:
                continue
            groups.append({"samples": state["samples"], "peak_current_bytes": state["peak_current_bytes"],
                           "memory_event_deltas": {key: value-state["initial_events"].get(key, value)
                                                   for key, value in state["final_events"].items()}})
        missing = set(self.errors)
        if not groups:
            missing.add("CGROUP_V2_MEMORY_NOT_OBSERVED")
        if not self.rss_available:
            missing.add("PROCESS_RSS_NOT_OBSERVED")
        return {"interval_ms": 50, "samples": self.samples, "process_tree_rss_available": self.rss_available,
                "sampled_process_tree_rss_peak_bytes": self.peak_rss if self.rss_available else None,
                "rss_scope": "Owned application parents, separate fault-test process and observed descendants after startup; excludes brokers and Python client. Sampling can miss brief peaks.",
                "cgroup_v2_memory_available": bool(groups), "cgroups": groups,
                "cgroup_scope": "Observed cgroups may be shared with unowned workloads; counters are not Kelvo-only usage or enforced reservations.",
                "missed_counter_categories": sorted(missing)}


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.fixture = Path(args.fixture).resolve()
        self.binary = Path(args.binary).resolve()
        self.sandbox = Path(args.sandbox).resolve()
        base = ROOT / "artifacts/operational-private"
        base.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(not base.is_symlink() and base.stat().st_mode & 0o077 == 0, "PRIVATE_DIRECTORY_REQUIRED")
        self.directory = Path(tempfile.mkdtemp(prefix="run-", dir=base))
        self.processes = {}
        self.samples = Samples(self.processes)
        self.expected = {tenant: expected_rows(tenant) for tenant in ("a", "b")}
        self.checks = []
        self.observation_lock = threading.Lock()
        self.reclaimed_statuses = 0
        self.env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(self.directory),
                    "TMPDIR": str(self.directory), "GOMAXPROCS": "2"}
        self.client = None
        self.worker = None
        self.started = time.monotonic()

    def record(self, name, callback):
        started = time.monotonic()
        try:
            detail = callback() or {}
            self.checks.append({"test": name, "passed": True, "seconds": round(time.monotonic()-started, 3), **detail})
            print(name + ": passed", flush=True)
            return True
        except BaseException as error:
            cf.write(self.directory / (name + "-failure.log"), traceback.format_exc())
            category = str(error) if isinstance(error, AcceptanceError) and re.fullmatch(r"[A-Z_]+", str(error)) else type(error).__name__
            self.checks.append({"test": name, "passed": False, "category": category,
                                "seconds": round(time.monotonic()-started, 3)})
            print(name + ": failed (private diagnostics retained)", file=sys.stderr, flush=True)
            if isinstance(error, (KeyboardInterrupt, InterruptedError)):
                raise
            return False

    def wait(self, predicate, timeout=15):
        deadline = time.monotonic()+timeout
        while time.monotonic() < deadline:
            try:
                value = predicate()
                if value:
                    return value
            except (OSError, urllib.error.URLError, http.client.HTTPException):
                pass
            time.sleep(0.05)
        raise AcceptanceError("CONDITION_DEADLINE")

    def call(self, path, tenant="a", body=None, node=None, timeout=12, with_headers=False):
        opener = self.worker_opener if node else self.client_opener
        port = {"a1": 14443, "b1": 14445}.get(node, 14440)
        headers = {} if node else {"Authorization": "Bearer " + self.env["KELVO_TOKEN_" + tenant.upper()]}
        data = None if body is None else json.dumps(body).encode()
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(f"https://127.0.0.1:{port}"+path, data=data, headers=headers)
        try:
            with opener.open(request, timeout=timeout) as response:
                raw, response_headers = read_http_response(response)
                if with_headers:
                    return response.status, raw, response_headers
                return response.status, raw
        except urllib.error.HTTPError as error:
            with error:
                raw, response_headers = read_http_response(error)
                if with_headers:
                    return error.code, raw, response_headers
                return error.code, raw

    def retry(self, path, tenant="a", body=None, with_headers=False):
        deadline = time.monotonic()+20
        while True:
            response = self.call(path, tenant, body, with_headers=with_headers)
            if response[0] != 429:
                return response
            require(time.monotonic() < deadline, "HTTP_ADMISSION_DEADLINE")
            time.sleep(0.02)

    def submit(self, tenant="a", sql=SQL, sources=None):
        code, raw = self.retry("/v1/queries", tenant, {"mode": "federated", "sources": ["ops_snapshot"] if sources is None else sources, "sql": sql})
        require(code == 201, "QUERY_SUBMISSION_FAILED")
        return json.loads(raw)["id"]

    def state(self, query_id, tenant="a"):
        code, raw = self.retry("/v1/queries/"+query_id, tenant)
        if code != 200:
            self.debug_response("query_status", code, raw)
        require(code == 200, "QUERY_STATUS_FAILED")
        return json.loads(raw)["state"]

    def debug_response(self, stage, code, raw):
        # Bounded, private debugging only; never copied into public evidence.
        path = self.directory/(stage+"-"+str(threading.get_ident())+".log")
        cf.write(path, "HTTP "+str(code)+"\n"+raw[:MAX_BODY].decode(errors="replace"))

    def query(self, tenant):
        query_id = self.submit(tenant)
        code, raw, headers = self.retry("/v1/queries/"+query_id+"/results", tenant, with_headers=True)
        if code != 200:
            self.debug_response("query_result", code, raw)
        require(code == 200, "QUERY_RESULT_FAILED")
        count = verify_completed_arrow(raw, headers, self.expected[tenant])
        # EOS has already certified durable success. Slot reuse can make a
        # later diagnostic GET return 404; never infer success from 404 alone.
        code, status = self.retry("/v1/queries/"+query_id, tenant)
        if code == 404:
            with self.observation_lock:
                self.reclaimed_statuses += 1
        else:
            if code != 200:
                self.debug_response("query_status", code, status)
            require(code == 200 and json.loads(status).get("state") == "succeeded", "QUERY_NOT_COMMITTED")
        return count

    def start_process(self, name, command):
        runtime = self.directory / (name+"-runtime")
        runtime.mkdir(mode=0o700, exist_ok=True)
        env = dict(self.env, TMPDIR=str(runtime), HOME=str(runtime))
        with (self.directory / (name+".log")).open("ab") as log:
            proc = subprocess.Popen([str(part) for part in command], env=env, cwd=ROOT,
                                    stdout=log, stderr=log, start_new_session=True)
        self.processes[name] = proc
        return proc

    def startup(self):
        require(sys.platform == "linux", "LINUX_REQUIRED")
        require(os.geteuid() != 0, "NON_ROOT_WORKER_REQUIRED")
        for executable in (self.binary, self.sandbox):
            require(executable.is_file() and os.access(executable, os.X_OK)
                    and executable.stat().st_mode & 0o022 == 0, "BUILT_PRIVATE_EXECUTABLE_REQUIRED")
        # Never attach to or modify another run's application processes.
        for port in (14440, 14441, 14443, 14444, 14445):
            with socket.socket() as sock:
                sock.settimeout(0.2)
                require(sock.connect_ex(("127.0.0.1", port)) != 0, "APPLICATION_PORT_OCCUPIED")
        private_env = self.fixture / "environment.json"
        require(private_env.is_file() and not private_env.is_symlink() and private_env.stat().st_mode & 0o077 == 0,
                "PRIVATE_FIXTURE_ENVIRONMENT_REQUIRED")
        self.env.update(json.loads(private_env.read_text()))
        previous = cf.DIR
        cf.DIR = self.fixture
        try:
            cf.wait_for_brokers(json.loads((self.fixture/"pids.json").read_text()))
        finally:
            cf.DIR = previous
        self.client = ssl.create_default_context(cafile=str(self.fixture/"ca.pem"))
        self.client.minimum_version = ssl.TLSVersion.TLSv1_3
        self.worker = ssl.create_default_context(cafile=str(self.fixture/"ca.pem"))
        self.worker.minimum_version = ssl.TLSVersion.TLSv1_3
        self.worker.load_cert_chain(str(self.fixture/"gateway.pem"), str(self.fixture/"gateway.key"))
        # Fixture credentials must never traverse an ambient HTTP proxy.
        self.client_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), StrictHTTPSHandler(context=self.client))
        self.worker_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), StrictHTTPSHandler(context=self.worker))
        import pyarrow as pa
        import pyarrow.parquet as pq
        for tenant, name in (("a", "a1"), ("b", "b1")):
            source = self.directory/(tenant+".parquet")
            table = pa.table({"id": pa.array(range(ROWS), type=pa.int64()),
                              "metric": pa.array([i % 101 if i % 17 else None for i in range(ROWS)], type=pa.int64()),
                              "tenant": pa.array([tenant]*ROWS, type=pa.string())})
            pq.write_table(table, source)
            dataset = {"id": "ops_snapshot", "query": {"mode": "federated", "sources": ["ops_raw"], "sql": "SELECT * FROM ops_raw"},
                       "refresh_interval": "5s", "max_age": "1m", "authorization_version": "operations-v1",
                       "limits": {"max_rows": ROWS, "max_bytes": 16 << 20, "timeout": "8s", "memory_mb": 128, "threads": 1, "max_temp_mb": 64}}
            catalog = {"sources": [{"id": "ops_raw", "type": "parquet", "path": str(source)}],
                       "acceleration": {"directory": str(self.directory/"snapshots"), "tenant_id": tenant, "datasets": [dataset]}}
            catalog_path = self.directory/(name+"-catalog.yml")
            cf.write(catalog_path, yaml_document(catalog))
            text = (self.fixture/(name+".yml")).read_text()
            text = replace_field(text, "catalog_file", str(catalog_path))
            text = replace_field(text, "sandbox_path", str(self.sandbox))
            resources = {"max_concurrent": 2, "memory_mb": 1024, "baseline_mb": 128, "overhead_mb": 128,
                         "scratch_mb": 512, "query_reserve_slots": 1, "query_reserve_memory_mb": 384, "query_reserve_scratch_mb": 256}
            text = replace_field(text, "resources", resources)
            cf.write(self.directory/(name+".yml"), text)
        gateway = replace_field((self.fixture/"gateway1.yml").read_text(), "max_http_requests", 2)
        cf.write(self.directory/"gateway.yml", gateway)
        with (self.directory/"init.log").open("wb") as log:
            subprocess.run([str(self.binary), "cluster-init", "--config", str(self.fixture/"init.yml")], env=self.env,
                           stdout=log, stderr=log, timeout=45, check=True)
        for name in ("a1", "b1"):
            command = [self.binary, "node", "--config", self.directory/(name+".yml"), "--drain-timeout", "8s"]
            self.start_process(name, command)
            # A preceding fixture's expired owner can still occupy its lease.
            # Retry only the fixed identity-conflict startup failure, never a
            # running process or an arbitrary startup error.
            def ready():
                proc = self.processes[name]
                if proc.poll() is not None:
                    log = (self.directory/(name+".log")).read_bytes()
                    require(b"worker identity is already active" in log[-4096:], "NODE_STARTUP_FAILED")
                    self.start_process(name, command)
                    return False
                return self.call("/ready", node=name, timeout=2)[0] == 200
            self.wait(ready, 20)
        self.start_process("gateway", [self.binary, "gateway", "--config", self.directory/"gateway.yml", "--drain-timeout", "2s"])
        self.wait(lambda: self.call("/ready")[0] == 200)
        self.wait(lambda: all(self.dataset_ready(name) for name in ("a1", "b1")), 20)
        for tenant in ("a", "b"):
            self.query(tenant)
        self.samples.start()
        return {"workers": 2, "tenants": 2, "broker_replicas": 3, "source_rows_per_tenant": ROWS,
                "dataset": "Deterministic synthetic Parquet with nullable integers and tenant markers"}

    def dataset_ready(self, node):
        code, raw = self.call("/datasets", node=node)
        require(code == 200, "DATASET_DIAGNOSTICS_FAILED")
        entries = json.loads(raw)
        return any(entry.get("id") == "ops_snapshot" and entry.get("state") == "ready" for entry in entries)

    def oversized(self):
        text = (self.directory/"a1.yml").read_text()
        text = replace_field(text, "resources", {"max_concurrent": 1, "memory_mb": 256, "baseline_mb": 128,
                                                 "overhead_mb": 64, "scratch_mb": 512})
        path = self.directory/"oversized.yml"
        cf.write(path, text)
        proc = subprocess.run([str(self.binary), "node", "--config", str(path)], env=self.env,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
        cf.write(self.directory/"oversized.log", (proc.stdout+proc.stderr).decode(errors="replace"))
        require(proc.returncode != 0 and b"query reservation exceeds node resources" in proc.stderr,
                "OVERSIZED_RESERVATION_NOT_REJECTED")
        return {"boundary": "node startup rejects query reservation larger than configured resources"}

    def resources(self, name):
        code, raw = self.call("/resources", node=name)
        require(code == 200, "RESOURCE_DIAGNOSTICS_FAILED")
        state = json.loads(raw)
        require(0 <= state["BackgroundActive"] <= state["Active"] <= state["Limits"]["MaxConcurrent"], "SLOT_BUDGET_EXCEEDED")
        for key in ("MemoryBytes", "ScratchBytes"):
            require(0 <= state["Used"][key] <= state["Limits"][key], "RESERVATION_BUDGET_EXCEEDED")
        return state

    def refresh_count(self, name):
        code, raw = self.call("/metrics", node=name)
        require(code == 200, "METRICS_FAILED")
        match = re.search(rb'(?m)^kelvo_jobs_completed_total\{kind="refresh",outcome="success"\} ([0-9]+)$', raw)
        require(match is not None, "REFRESH_METRIC_MISSING")
        return int(match[1])

    def mixed(self):
        before = {name: self.refresh_count(name) for name in ("a1", "b1")}
        before_reclaimed = self.reclaimed_statuses
        stop = threading.Event()
        counts = []
        failures = []
        lock = threading.Lock()
        deadline = time.monotonic()+self.args.duration
        started = time.monotonic()
        overlap_samples = 0
        max_active = 0
        def client(tenant):
            count = 0
            try:
                while not stop.is_set() and time.monotonic() < deadline:
                    self.query(tenant)
                    count += 1
            except BaseException as error:
                cf.write(self.directory/("mixed-client-"+str(threading.get_ident())+"-failure.log"), traceback.format_exc())
                with lock:
                    failures.append(error)
                stop.set()
            finally:
                with lock:
                    counts.append(count)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                tasks = [pool.submit(client, tenant) for tenant in ("a", "b", "a", "b")]
                try:
                    while time.monotonic() < deadline and not stop.is_set():
                        for name in ("a1", "b1"):
                            state = self.resources(name)
                            max_active = max(max_active, state["Active"])
                            if state["BackgroundActive"] > 0 and state["Active"] > state["BackgroundActive"]:
                                overlap_samples += 1
                        stop.wait(0.05)
                finally:
                    # Stop clients before the executor context waits for them,
                    # including a failed observer during an hours-long soak.
                    stop.set()
                for task in tasks:
                    task.result(timeout=25)
        finally:
            stop.set()
        if failures:
            category = str(failures[0]) if isinstance(failures[0], AcceptanceError) and re.fullmatch(r"[A-Z_]+", str(failures[0])) else type(failures[0]).__name__.upper()
            raise AcceptanceError("MIXED_"+category)
        require(sum(counts) >= 4 and all(count > 0 for count in counts), "MIXED_CLIENT_DID_NOT_PROGRESS")
        after = {name: self.refresh_count(name) for name in before}
        require(all(after[name] > before[name] for name in before), "SCHEDULED_REFRESH_DID_NOT_PROGRESS")
        require(overlap_samples > 0, "QUERY_REFRESH_OVERLAP_NOT_OBSERVED")
        return {"requested_seconds": self.args.duration, "observed_seconds": round(time.monotonic()-started, 3),
                "completed_queries": sum(counts), "concurrent_clients": 4, "client_query_counts": counts,
                "reclaimed_status_after_certified_delivery": self.reclaimed_statuses-before_reclaimed,
                "completion_protocol": "durable-eos-v1",
                "successful_refresh_deltas": {name: after[name]-before[name] for name in before},
                "query_refresh_overlap_samples": overlap_samples, "maximum_observed_reservations": max_active,
                "correctness": "All returned groups, counts, nullable sums, tenant labels and Arrow types checked"}

    def queued_waiter_saturation(self, ids):
        """Establish parked waiters from durable states, never submission order."""
        snapshot = {}
        def observe():
            snapshot.update({query_id: self.state(query_id) for query_id in ids})
            require(len(snapshot) == 8 and set(snapshot.values()) <= {"queued", "assigned"},
                    "SATURATION_QUEUE_STATE_CHANGED")
            assigned = [query_id for query_id, state in snapshot.items() if state == "assigned"]
            queued = [query_id for query_id, state in snapshot.items() if state == "queued"]
            require(len(assigned) <= 1, "SATURATION_UNEXPECTED_WORKER_CAPACITY")
            return (assigned[0], queued[:3]) if len(assigned) == 1 and len(queued) >= 3 else None
        assigned, queued = self.wait(observe, 5)
        starts = [threading.Event(), threading.Event()]
        attempts = [0, 0]
        def hold(index):
            deadline = time.monotonic()+6
            while time.monotonic() < deadline:
                attempts[index] += 1
                starts[index].set()
                try:
                    response = self.call("/v1/queries/"+queued[index]+"/results", timeout=6)
                except (OSError, urllib.error.URLError, http.client.HTTPException):
                    return None
                if response[0] != 429:
                    return response
                # A third simultaneous handler can briefly occupy an active
                # permit before parking. Never lose a holder on that 429.
                time.sleep(0.02)
            raise AcceptanceError("QUEUED_WAITER_ADMISSION_DEADLINE")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            tasks = [pool.submit(hold, index) for index in range(2)]
            try:
                require(all(event.wait(2) for event in starts), "QUEUED_WAITER_DID_NOT_START")
                def probe():
                    require(not any(task.done() for task in tasks), "QUEUED_WAITER_RETURNED_EARLY")
                    code, raw = self.call("/v1/queries/"+queued[2]+"/results", timeout=0.2)
                    if code != 429:
                        return False
                    error = json.loads(raw).get("error", {})
                    if error.get("message") != "Queued result wait capacity unavailable":
                        return False
                    require(error.get("code") == "RESOURCE_EXHAUSTED", "INVALID_WAITER_REJECTION")
                    require(not any(task.done() for task in tasks), "QUEUED_WAITER_RETURNED_EARLY")
                    require(all(self.state(query_id) == "queued" for query_id in queued),
                            "PARKED_QUERY_WAS_ASSIGNED")
                    require(self.state(assigned) == "assigned", "SATURATION_BLOCKER_CHANGED")
                    return True
                self.wait(probe, 3)
                require(self.call("/health")[0] == 200, "WAITERS_BROKE_LIVENESS")
                for query_id in queued[:2]:
                    code, _ = self.retry("/v1/queries/"+query_id+"/cancel", body={})
                    require(code == 200, "WAITER_CANCELLATION_FAILED")
                for task in tasks:
                    response = task.result(timeout=2)
                    require(response is None or response[0] != 200, "QUEUED_WAITER_COMPLETED_UNEXPECTEDLY")
            finally:
                # Retain bounded scheduling evidence for an inconclusive gate.
                cf.write(self.directory/"saturation-waiters.json", json.dumps({
                    "observed_states": list(snapshot.values()), "request_attempts": attempts,
                    "started": [event.is_set() for event in starts],
                    "returned": [task.done() for task in tasks]}, indent=2))
        return {"queued_handles_confirmed": 3, "waiter_requests_started": 2,
                "waiter_rejection_source": "parked_waiter_pool"}

    def saturated(self):
        ids = []
        def held(tenant, query_id):
            try:
                return self.call("/v1/queries/"+query_id+"/results", tenant, timeout=3)
            except (OSError, urllib.error.URLError, http.client.HTTPException):
                return None
        def cancel_all(handles):
            failed = False
            for tenant, query_id in handles:
                try:
                    code, _ = self.retry("/v1/queries/"+query_id+"/cancel", tenant, body={})
                    failed = failed or code != 200
                except Exception:
                    failed = True
            require(not failed, "SATURATION_CANCELLATION_FAILED")
        try:
            for _ in range(8):
                ids.append(self.submit("a", "SELECT 7::BIGINT AS value", []))
            code, _ = self.call("/v1/queries", body={"sql": "SELECT 1", "mode": "federated"})
            require(code == 429, "TENANT_QUEUE_NOT_BOUNDED")
            waiter_evidence = self.queued_waiter_saturation(ids)
        finally:
            cancel_all([("a", query_id) for query_id in ids])
        self.wait(lambda: self.resources("a1")["Active"] == 0 and self.resources("a1")["Waiting"] == 0)
        active = []
        try:
            for tenant in ("a", "b"):
                active.append((tenant, self.submit(tenant, "SELECT sum(i) FROM range(1000000000000) t(i)", [])))
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                tasks = [pool.submit(held, tenant, query_id) for tenant, query_id in active]
                self.wait(lambda: all(self.resources(name)["Active"] > self.resources(name)["BackgroundActive"] for name in ("a1", "b1")), 2)
                require(self.call("/v1/queries/"+active[0][1])[0] == 429, "ACTIVE_HTTP_PERMITS_NOT_BOUNDED")
                require(self.call("/health")[0] == 200, "SATURATION_BROKE_LIVENESS")
                require(self.call("/ready")[0] == 200, "SATURATION_BROKE_READINESS")
                for task in tasks:
                    response = task.result(timeout=5)
                    require(response is None or response[0] != 200 or not response[1].endswith(EOS), "CANCELLED_QUERY_COMPLETED_UNEXPECTEDLY")
        finally:
            cancel_all(active)
        self.wait(lambda: all(self.resources(name)["Active"] == 0 and self.resources(name)["Waiting"] == 0 for name in ("a1", "b1")))
        self.query("a")
        self.query("b")
        return {"tenant_handles": 8, "bounded_queued_result_waiters": 2, "waiters_preserve_status_progress": True,
                "active_http_results": 2, "health_during_http_saturation": 200,
                "queue_rejection": 429, "waiter_rejection": 429, "http_rejection": 429,
                "post_cancel_queries_correct": True, **waiter_evidence}

    def drain(self):
        query_id = self.submit("a", "SELECT 42::BIGINT AS value", [])
        self.wait(lambda: self.state(query_id) == "assigned")
        proc = self.processes["a1"]
        require(proc.poll() is None, "DRAIN_TARGET_EXITED")
        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        self.wait(lambda: self.call("/ready", node="a1")[0] == 503, 3)
        require(self.call("/health", node="a1")[0] == 200, "DRAIN_BROKE_LIVENESS")
        code, raw, headers = self.retry("/v1/queries/"+query_id+"/results", with_headers=True)
        require(code == 200, "ACCEPTED_DRAIN_QUERY_FAILED")
        verify_completed_arrow(raw, headers, [{"value": 42}])
        require(self.state(query_id) == "succeeded", "DRAIN_RESULT_NOT_COMMITTED")
        require(proc.wait(timeout=8) == 0 and time.monotonic()-started < 8, "NODE_DRAIN_DEADLINE")
        self.wait(lambda: not list((self.directory/"a1-runtime").glob("kelvo-worker-*")), 2)
        return {"ready_during_drain": 503, "health_during_drain": 200,
                "accepted_result_correct": True, "exit_before_grace_deadline": True,
                "drain_seconds": round(time.monotonic()-started, 3)}

    def failure_gate(self):
        name = "TestSandboxedWorkerFailurePreservesSnapshotAndAdmission"
        env = dict(os.environ, KELVO_TEST_WORKER_BINARY=str(self.binary), KELVO_TEST_SANDBOX_BINARY=str(self.sandbox), GOMAXPROCS="2")
        path = self.directory/"worker-failure-tests.jsonl"
        with path.open("wb") as log:
            proc = subprocess.Popen([self.args.go, "test", "-json", "-p", "2", "-tags", "duckdb_arrow", "./internal/cluster",
                                     "-run", "^"+name+"$", "-count=1", "-timeout=3m"],
                                    cwd=ROOT, env=env, stdout=log, stderr=log, start_new_session=True)
        self.processes["failure-gate"] = proc
        require(proc.wait(timeout=240) == 0, "WORKER_FAILURE_GATE_FAILED")
        expected = {name+"/"+layout+"/"+failure for layout in ("single", "multipart") for failure in ("native_memory", "source_ownership")}
        passed = set()
        with path.open() as log:
            for line in log:
                require(len(line) <= 1 << 20, "GO_TEST_EVENT_BOUND")
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                require(event.get("Action") not in ("skip", "fail"), "WORKER_FAILURE_GATE_SKIPPED_OR_FAILED")
                if event.get("Action") == "pass" and event.get("Test") in expected:
                    passed.add(event["Test"])
        require(passed == expected, "WORKER_FAILURE_GATE_INCOMPLETE")
        return {"subtests": 4, "native_worker_memory_failure": "real sandboxed worker",
                "source_quota_loss": "in-memory coordination failure injection, real worker IPC",
                "verified": "single/multipart prior-generation retention, permit/scratch cleanup and exact recovered Arrow values"}

    def cleanup(self):
        errors = []
        try:
            self.samples.finish()
        except Exception:
            errors.append("SAMPLER_STOP_FAILED")
        try:
            self.samples.sample()
        except Exception:
            errors.append("FINAL_SAMPLE_FAILED")
        for proc in list(self.processes.values()):
            try:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
        forced = 0
        for proc in list(self.processes.values()):
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                forced += 1
                proc.kill()
                proc.wait(timeout=3)
        remaining = [pid for pid, identity in self.samples.owned.items() if proc_identity(pid) == identity]
        for pid in remaining:
            signal_owned(self.samples.owned[pid], signal.SIGKILL)
        self.wait(lambda: all(proc_identity(pid) != identity for pid, identity in self.samples.owned.items()), 3)
        leftovers = list(self.directory.glob("*-runtime/kelvo-worker-*"))
        require(not errors and forced == 0 and not remaining and not leftovers, "OWNED_PROCESS_OR_SCRATCH_LEAK")
        return {"forced_application_kills": forced, "observed_live_descendants": len(remaining),
                "worker_scratch_directories": len(leftovers), "broker_processes_untouched": True,
                "original_configurations_untouched": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default=str(ROOT/"bin/kelvo"))
    parser.add_argument("--sandbox", default=str(ROOT/"bin/kelvo-landlock"))
    parser.add_argument("--fixture", default=str(ROOT/"artifacts/cluster-private"))
    parser.add_argument("--duration", type=duration_value, default=30.0)
    parser.add_argument("--go", default="go")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    require(not output.exists() and not output.is_symlink(), "OUTPUT_ALREADY_EXISTS")
    os.umask(0o077)
    acceptance = Acceptance(args)
    report = {"schema_version": 1, "passed": False, "checked_at": datetime.now(timezone.utc).isoformat(),
              "checks": acceptance.checks, "requested_duration_seconds": args.duration,
              "scope": "Local synthetic lifecycle acceptance, not source-provider conformance, WAN capacity or production certification",
              "script_sha256": sha256(Path(__file__)), "fixture_script_sha256": sha256(Path(cf.__file__)),
              "fault_test_sha256": sha256(ROOT/"internal/cluster/worker_failure_integration_test.go")}
    for key, path in (("binary_sha256", acceptance.binary), ("sandbox_sha256", acceptance.sandbox)):
        if path.is_file():
            report[key] = sha256(path)
    def interrupted(signum, frame):
        raise InterruptedError()
    signal.signal(signal.SIGTERM, interrupted)
    was_interrupted = False
    try:
        active = acceptance.record("startup", acceptance.startup)
        for name, callback in (("oversized_reservation_rejected", acceptance.oversized), ("mixed_query_refresh", acceptance.mixed),
                               ("saturated_liveness_and_recovery", acceptance.saturated), ("accepted_query_during_node_drain", acceptance.drain)):
            if active:
                active = acceptance.record(name, callback)
            else:
                acceptance.checks.append({"test": name, "passed": False, "category": "DEPENDENCY_FAILED"})
        if acceptance.binary.is_file() and acceptance.sandbox.is_file():
            acceptance.record("sandboxed_resource_and_source_lease_failures", acceptance.failure_gate)
    except (KeyboardInterrupt, InterruptedError):
        was_interrupted = True
    finally:
        # Once teardown begins, a second signal records interruption without
        # abandoning owned processes half-way through cleanup.
        def cleaning_signal(signum, frame):
            nonlocal was_interrupted
            was_interrupted = True
        signal.signal(signal.SIGTERM, cleaning_signal)
        signal.signal(signal.SIGINT, cleaning_signal)
        acceptance.record("cleanup", acceptance.cleanup)
    for name in REQUIRED:
        if not any(item["test"] == name for item in acceptance.checks):
            acceptance.checks.append({"test": name, "passed": False,
                                      "category": "INTERRUPTED" if was_interrupted else "PREREQUISITES_FAILED"})
    report["interrupted"] = was_interrupted
    report["sampling"] = acceptance.samples.evidence()
    report["elapsed_seconds"] = round(time.monotonic()-acceptance.started, 3)
    report["passed"] = reconcile(report)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as destination:
        json.dump(report, destination, indent=2)
        destination.write("\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
