#!/usr/bin/env python3
"""Real-process acceleration correctness acceptance; run on the test VM.

Requires an already built bin/kelvo and pyarrow. --cluster additionally requires
the already provisioned, running scripts/cluster_fixture.py broker and Linux
bin/kelvo-landlock. This script installs nothing and never stops broker processes.
Only its own application processes are stopped; fixture catalogs are restored.
Private files and diagnostics stay in ignored artifacts/acceleration-private.
The JSON evidence contains check results, never credentials or filesystem paths.
"""

import argparse
import copy
import csv
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import io
import json
import os
from pathlib import Path
import re
import signal
import socket
import ssl
import stat
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
import uuid

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "bin/kelvo"
EVIDENCE = ROOT / "docs/evidence/acceleration-acceptance.json"
EOS = b"\xff\xff\xff\xff\0\0\0\0"
LIMITS = {
    "max_rows": 1000, "max_bytes": 8 << 20, "timeout": "15s",
    "memory_mb": 128, "threads": 1, "max_temp_mb": 64,
}
TYPED_SQL = """SELECT CAST(row_id AS BIGINT) AS row_id,
CAST(NULLIF(substr(i64_text, 3), 'NULL') AS BIGINT) AS i64,
CAST(NULLIF(substr(u64_text, 3), 'NULL') AS UBIGINT) AS u64,
CAST(NULLIF(substr(amount_text, 3), 'NULL') AS DECIMAL(38,12)) AS amount,
CAST(NULLIF(substr(date_text, 3), 'NULL') AS DATE) AS day,
CAST(NULLIF(substr(ts_text, 3), 'NULL') AS TIMESTAMP) AS observed_at,
NULLIF(label, 'NULL') AS label FROM raw"""
TYPED_SCHEMA = {
    "row_id": pa.int64(), "i64": pa.int64(), "u64": pa.uint64(),
    "amount": pa.decimal128(38, 12), "day": pa.date32(),
    "observed_at": pa.timestamp("us"), "label": pa.string(),
}


def require(condition, message):
    # Explicit checks continue to work when Python is invoked with -O.
    if not condition:
        raise AssertionError(message)


def private_directory(path):
    require(not path.is_symlink(), "private directory cannot be a symlink")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(stat.S_IMODE(path.stat().st_mode) == 0o700,
            "private directory must already have mode 0700")
    return path


def write_private(path, data):
    require(not path.is_symlink(), "private output cannot be a symlink")
    path.write_bytes(data if isinstance(data, bytes) else data.encode())
    path.chmod(0o600)


def write_config(path, config):
    # JSON is a YAML subset, so fixture generation needs no YAML dependency.
    write_private(path, json.dumps(config, indent=2) + "\n")


def dataset(name, sql, max_age="1h", interval="0s"):
    return {
        "id": name, "query": {"mode": "federated", "sources": ["raw"], "sql": sql},
        "refresh_interval": interval, "max_age": max_age,
        "authorization_version": "acceptance-v1", "limits": dict(LIMITS),
    }


def typed_csv(label="original", invalid=False):
    rows = [
        [1, "v:-9223372036854775808", "v:0", "v:-99999999999999999999999999.123456789012",
         "v:1960-01-02", "v:1960-01-02 03:04:05.123456", label],
        [2, "v:9223372036854775807", "v:18446744073709551615", "v:99999999999999999999999999.123456789012",
         "v:2026-10-01", "v:2026-10-01 12:34:56.654321", "second"],
        [3, "v:NULL", "v:NULL", "v:NULL", "v:NULL", "v:NULL", "NULL"],
    ]
    if invalid:
        rows[0][1] = "v:not-an-integer"
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["row_id", "i64_text", "u64_text", "amount_text", "date_text", "ts_text", "label"])
    writer.writerows(rows)
    return output.getvalue()


def expected_rows(label="original"):
    return [
        {"row_id": 1, "i64": -(1 << 63), "u64": 0,
         "amount": Decimal("-99999999999999999999999999.123456789012"),
         "day": date(1960, 1, 2), "observed_at": datetime(1960, 1, 2, 3, 4, 5, 123456), "label": label},
        {"row_id": 2, "i64": (1 << 63) - 1, "u64": (1 << 64) - 1,
         "amount": Decimal("99999999999999999999999999.123456789012"),
         "day": date(2026, 10, 1), "observed_at": datetime(2026, 10, 1, 12, 34, 56, 654321), "label": "second"},
        {"row_id": 3, "i64": None, "u64": None, "amount": None,
         "day": None, "observed_at": None, "label": None},
    ]


def check_typed(table, label="original", empty=False):
    require(table.column_names == list(TYPED_SCHEMA), "typed column names changed")
    for name, expected_type in TYPED_SCHEMA.items():
        require(table.schema.field(name).type == expected_type, "typed field changed: " + name)
    require(table.to_pylist() == ([] if empty else expected_rows(label)), "typed values or NULLs changed")


def parse_status(data):
    """Read only the fixed scalar fields emitted by `accelerate status`.

    This deliberately is not a general YAML parser. Snapshot paths remain in
    private CLI logs; all paths used by this script are derived from its fixture.
    """
    text = data.decode()
    result = {}
    for key in ("ready", "generation", "fingerprint", "sha256", "rows", "bytes", "refreshed_at"):
        match = re.search(r"^\s*" + key + r":\s*([^\n]+)$", text, re.MULTILINE)
        require(match is not None, "CLI status is missing " + key)
        raw = match.group(1).strip()
        if raw.startswith('"'):
            raw = json.loads(raw)
        elif raw.startswith("'") and raw.endswith("'"):
            raw = raw[1:-1].replace("''", "'")
        result[key] = raw
    require(result["ready"] in ("true", "false"), "invalid CLI ready field")
    result["ready"] = result["ready"] == "true"
    for key in ("rows", "bytes"):
        result[key] = int(result[key])
    require(re.fullmatch("[0-9a-f]{32}", result["generation"]) is not None, "invalid generation")
    for key in ("fingerprint", "sha256"):
        require(re.fullmatch("[0-9a-f]{64}", result[key]) is not None, "invalid snapshot digest")
    return result


class Acceptance:
    def __init__(self, cluster):
        os.umask(0o077)
        base = private_directory(ROOT / "artifacts/acceleration-private")
        self.directory = private_directory(base / ("run-" + uuid.uuid4().hex))
        self.env = dict(os.environ)
        self.processes = {}
        self.catalog_backups = {}
        self.checks = []
        self.stage = "prerequisites"
        self.cluster = cluster
        self.command_number = 0

    def record(self, name, **data):
        self.checks.append({"test": name, "passed": True, **data})
        print(name + ": passed", flush=True)

    def cli(self, args, success=True, timeout=45):
        self.command_number += 1
        proc = subprocess.Popen([str(BIN), *map(str, args)], env=self.env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except BaseException:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
            stdout, stderr = proc.communicate()
            write_private(self.directory / f"cli-{self.command_number}.log", stdout + b"\n" + stderr)
            raise
        write_private(self.directory / f"cli-{self.command_number}.log", stdout + b"\n" + stderr)
        require((proc.returncode == 0) == success, "CLI returned an unexpected exit status")
        return stdout, stderr

    def status(self, config, name, command="status", success=True):
        stdout, _ = self.cli(["accelerate", command, "--config", config, "--dataset", name], success)
        return parse_status(stdout) if success else None

    def query(self, config, name="typed_snapshot", sql=None, success=True):
        output = self.directory / ("query-" + uuid.uuid4().hex + ".arrow")
        _, stderr = self.cli([
            "query", "--config", config, "--mode", "federated", "--sources", name,
            "--sql", sql or ("SELECT * FROM " + name + " ORDER BY row_id"),
            "--out", output, "--threads", "1", "--memory-mb", "128", "--temp-mb", "64",
        ], success)
        if not success:
            require(not output.exists(), "failed query published an export")
            return None, None
        wire = output.read_bytes()
        require(wire.endswith(EOS), "Arrow export has no successful EOS")
        table = pa.ipc.open_stream(wire).read_all()
        stats = json.loads(stderr)
        require(stats.get("backend") == "duckdb", "selected snapshot query did not use DuckDB")
        versions = stats.get("accelerations", [])
        if name != "raw":
            require(len(versions) == 1 and versions[0]["dataset"] == name,
                    "query statistics omitted its selected snapshot")
        return table, stats

    def standalone(self):
        self.stage = "standalone_setup"
        raw = self.directory / "input.csv"
        write_private(raw, typed_csv())
        store = self.directory / "snapshots"
        config = {
            "sources": [{"id": "raw", "type": "csv", "path": str(raw)}],
            "acceleration": {
                "directory": str(store), "tenant_id": "acceptance",
                "datasets": [dataset("typed_snapshot", TYPED_SQL),
                             dataset("empty_snapshot", TYPED_SQL + " WHERE false"),
                             dataset("short_lived", TYPED_SQL, max_age="5s"),
                             dataset("schema_guard", "SELECT * FROM raw")],
            },
        }
        catalog = self.directory / "catalog.yml"
        write_config(catalog, config)

        self.stage = "cold_snapshot"
        self.query(catalog, success=False)
        self.status(catalog, "typed_snapshot", success=False)
        first = self.status(catalog, "typed_snapshot", command="refresh")
        require(first["ready"] and first["rows"] == 3, "cold refresh did not become ready")
        table, stats = self.query(catalog)
        check_typed(table)
        require(stats["accelerations"][0]["generation"] == first["generation"], "query used an unexpected generation")
        self.record("cold_snapshot_requires_refresh")

        self.stage = "typed_parquet_and_verify"
        payload = store / "acceptance/typed_snapshot" / (first["generation"] + ".parquet")
        check_typed(pq.read_table(payload).sort_by([("row_id", "ascending")]))
        require(hashlib.sha256(payload.read_bytes()).hexdigest() == first["sha256"], "snapshot hash mismatch")
        require(payload.stat().st_size == first["bytes"], "snapshot byte count mismatch")
        require(stat.S_IMODE(store.stat().st_mode) == 0o700, "snapshot root is not private")
        require(stat.S_IMODE(payload.stat().st_mode) == 0o400, "published snapshot is not immutable by mode")
        verified = self.status(catalog, "typed_snapshot", command="verify")
        require(verified["generation"] == first["generation"], "verify changed the generation")
        self.record("typed_arrow_parquet_roundtrip", rows=3,
                    types=["int64_extremes", "uint64_max", "decimal128_38_12", "date32", "timestamp_us", "utf8", "null"])
        self.record("cli_status_and_integrity_verify")

        self.stage = "empty_snapshot"
        empty = self.status(catalog, "empty_snapshot", command="refresh")
        empty_table, _ = self.query(catalog, "empty_snapshot")
        check_typed(empty_table, empty=True)
        require(empty["ready"] and empty["rows"] == 0, "empty snapshot was not ready")
        self.record("empty_snapshot_preserves_schema")

        self.stage = "source_change_snapshot_isolation"
        write_private(raw, typed_csv("updated"))
        for _ in range(2):
            current, _ = self.query(catalog)
            check_typed(current)
        unchanged = self.status(catalog, "typed_snapshot")
        require(unchanged["generation"] == first["generation"], "source change silently replaced snapshot")
        updated = self.status(catalog, "typed_snapshot", command="refresh")
        require(updated["generation"] != first["generation"], "successful refresh reused a generation")
        check_typed(self.query(catalog)[0], "updated")
        self.record("source_changes_visible_only_after_refresh", repeated_cached_queries=2)

        self.stage = "verified_generation_restore"
        inventory, _ = self.cli(["accelerate", "inventory", "--config", catalog, "--dataset", "typed_snapshot"])
        require(first["generation"].encode() in inventory and updated["generation"].encode() in inventory,
                "inventory omitted retained generations")
        restored, _ = self.cli(["accelerate", "restore", "--config", catalog, "--dataset", "typed_snapshot",
                               "--generation", first["generation"], "--expected-generation", updated["generation"]])
        restored = parse_status(restored)
        require(restored["generation"] == first["generation"] and restored["refreshed_at"] == first["refreshed_at"],
                "restore changed generation identity or freshness")
        check_typed(self.query(catalog)[0])
        self.cli(["accelerate", "restore", "--config", catalog, "--dataset", "typed_snapshot",
                  "--generation", updated["generation"], "--expected-generation", updated["generation"]], success=False)
        require(self.status(catalog, "typed_snapshot")["generation"] == first["generation"],
                "stale restore precondition changed current generation")
        self.cli(["accelerate", "restore", "--config", catalog, "--dataset", "typed_snapshot",
                  "--generation", updated["generation"], "--expected-generation", first["generation"]])
        check_typed(self.query(catalog)[0], "updated")
        self.record("verified_restore_preserves_timestamp_and_fences_stale_requests")

        self.stage = "cross_generation_schema_contract"
        guard = self.status(catalog, "schema_guard", command="refresh")
        write_private(raw, typed_csv("updated").replace("\n1,", "\nchanged,", 1))
        try:
            self.status(catalog, "schema_guard", command="refresh", success=False)
            require(self.status(catalog, "schema_guard")["generation"] == guard["generation"],
                    "incompatible schema replaced valid snapshot")
        finally:
            write_private(raw, typed_csv("updated"))
        self.record("source_schema_change_preserves_previous_generation")

        self.stage = "failed_refresh_preserves_snapshot"
        write_private(raw, typed_csv("updated", invalid=True))
        try:
            self.status(catalog, "typed_snapshot", command="refresh", success=False)
            retained = self.status(catalog, "typed_snapshot")
            require(retained["generation"] == updated["generation"], "failed refresh replaced the manifest")
            check_typed(self.query(catalog)[0], "updated")
        finally:
            write_private(raw, typed_csv("updated"))
        self.record("source_cast_failure_preserves_previous_generation")

        self.stage = "source_file_outage"
        unavailable = raw.with_suffix(".offline")
        raw.rename(unavailable)
        try:
            check_typed(self.query(catalog)[0], "updated")
            self.status(catalog, "typed_snapshot", command="refresh", success=False)
            require(self.status(catalog, "typed_snapshot")["generation"] == updated["generation"],
                    "source outage replaced the manifest")
        finally:
            unavailable.rename(raw)
        self.record("source_file_outage_keeps_healthy_snapshot_readable")

        self.stage = "authorization_version_invalidation"
        changed = copy.deepcopy(config)
        changed["acceleration"]["datasets"][0]["authorization_version"] = "acceptance-v2"
        changed_catalog = self.directory / "authorization-v2.yml"
        write_config(changed_catalog, changed)
        require(not self.status(changed_catalog, "typed_snapshot")["ready"], "authorization change remained ready")
        self.query(changed_catalog, success=False)
        check_typed(self.query(catalog)[0], "updated")
        self.record("authorization_version_rejects_old_snapshot")

        self.stage = "freshness_expiry"
        require(self.status(catalog, "short_lived", command="refresh")["ready"], "short-lived refresh failed")
        time.sleep(6)
        require(not self.status(catalog, "short_lived")["ready"], "expired snapshot remained ready")
        self.query(catalog, "short_lived", success=False)
        self.record("max_age_expiry_fails_closed", configured_max_age_seconds=5)

    def start(self, name, command):
        with open(self.directory / (name + ".log"), "ab") as log:
            proc = subprocess.Popen(list(map(str, command)), env=self.env, stdout=log, stderr=log,
                                    start_new_session=True)
        self.processes[name] = proc

    def stop(self, name):
        proc = self.processes[name]
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        try:
            proc.wait(timeout=12)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=3)

    def wait(self, fn, timeout=45):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for proc in self.processes.values():
                require(proc.poll() in (None, 0), "acceptance application process exited unexpectedly")
            try:
                result = fn()
                if result:
                    return result
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.1)
        raise AssertionError("acceptance condition did not become ready")

    def cluster_call(self, path, tenant="a", body=None, headers=None):
        auth = {"Authorization": "Bearer " + self.env["KELVO_TOKEN_" + tenant.upper()]}
        auth.update(headers or {})
        data = None if body is None else json.dumps(body).encode()
        if data is not None:
            auth["Content-Type"] = "application/json"
        request = urllib.request.Request("https://127.0.0.1:14440" + path, data=data, headers=auth)
        try:
            with urllib.request.urlopen(request, context=self.tls, timeout=15) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as err:
            return err.code, err.read()

    def cluster_query(self, tenant, total, headers=None):
        code, body = self.cluster_call("/v1/queries", tenant, {
            "mode": "federated", "sources": ["orders_fast"],
            "sql": "SELECT label, CAST(sum(id) AS BIGINT) AS total FROM orders_fast GROUP BY label",
        }, headers)
        require(code == 201, "cluster query admission failed")
        query_id = json.loads(body)["id"]
        code, body = self.cluster_call("/v1/queries/" + query_id + "/results", tenant)
        require(code == 200 and body.endswith(EOS), "cluster Arrow result was incomplete")
        table = pa.ipc.open_stream(body).read_all()
        require(table.to_pylist() == [{"label": tenant, "total": total}], "cluster query read another tenant or stale data")
        code, body = self.cluster_call("/v1/queries/" + query_id, tenant)
        require(code == 200, "cluster query status unavailable")
        state = json.loads(body)
        require(state["state"] == "succeeded", "cluster Arrow result did not commit success")
        versions = state["stats"].get("accelerations", [])
        require(len(versions) == 1 and versions[0]["dataset"] == "orders_fast", "cluster status omitted accelerated alias")
        return query_id

    def cluster_checks(self):
        self.stage = "cluster_prerequisites"
        fixture = ROOT / "artifacts/cluster-private"
        require((fixture / "environment.json").is_file(), "cluster fixture is not provisioned")
        require((ROOT / "bin/kelvo-landlock").is_file(), "cluster sandbox launcher is unavailable")
        self.env.update(json.loads((fixture / "environment.json").read_text()))
        self.tls = ssl.create_default_context(cafile=str(fixture / "ca.pem"))
        self.tls.minimum_version = ssl.TLSVersion.TLSv1_3
        worker_tls = ssl.create_default_context(cafile=str(fixture / "ca.pem"))
        worker_tls.minimum_version = ssl.TLSVersion.TLSv1_3
        worker_tls.load_cert_chain(str(fixture / "gateway.pem"), str(fixture / "gateway.key"))
        for port in (14440, 14443, 14444, 14445):
            with socket.socket() as sock:
                sock.settimeout(0.2)
                require(sock.connect_ex(("127.0.0.1", port)) != 0,
                        "cluster application port is occupied; existing processes were left untouched")
        for port in (18222, 18223, 18224):
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=2) as response:
                require(json.load(response).get("status") == "ok", "fixture broker is not healthy")

        self.stage = "cluster_catalogs"
        shared = self.directory / "cluster-snapshots"
        catalogs, sources = {}, {}
        for tenant in ("a", "b"):
            source = self.directory / ("tenant-" + tenant + ".csv")
            sources[tenant] = source
            rows = (1, 2) if tenant == "a" else (10, 20)
            write_private(source, f"id,label\n{rows[0]},{tenant}\n{rows[1]},{tenant}\n")
            refresh = dataset("orders_fast", "SELECT CAST(id AS BIGINT) AS id, CAST(label AS VARCHAR) AS label FROM sample",
                              max_age="1m", interval="5s")
            refresh["query"]["sources"] = ["sample"]
            diagnostic_datasets = []
            if tenant == "a":
                for probe_id in ("required_probe", "optional_probe"):
                    probe_dataset = dataset(probe_id, refresh["query"]["sql"])
                    probe_dataset["query"]["sources"] = ["sample"]
                    diagnostic_datasets.append(probe_dataset)
            config = {"sources": [{"id": "sample", "type": "csv", "path": str(source)}],
                      "acceleration": {"directory": str(shared), "tenant_id": tenant,
                                       "datasets": [refresh] + diagnostic_datasets}}
            for name in (("a1", "a2") if tenant == "a" else ("b1",)):
                path = fixture / (name + "-catalog.yml")
                require(path.is_file() and not path.is_symlink(), "fixture catalog is unavailable")
                self.catalog_backups[path] = (path.read_bytes(), stat.S_IMODE(path.stat().st_mode))
                write_config(path, config)
                catalogs[name] = path
        # Fixture node files are YAML. Add one top-level field without requiring
        # a YAML dependency, and restore the exact original bytes during cleanup.
        required_node = fixture / "a1.yml"
        require(required_node.is_file() and not required_node.is_symlink(), "fixture node config unavailable")
        original_node = required_node.read_bytes()
        require(re.search(rb"^required_datasets\s*:", original_node, re.MULTILINE) is None,
                "fixture already defines required datasets; existing configuration left untouched")
        self.catalog_backups[required_node] = (original_node, stat.S_IMODE(required_node.stat().st_mode))
        write_private(required_node, original_node.rstrip() + b'\nrequired_datasets: ["required_probe"]\n')
        self.cli(["cluster-init", "--config", fixture / "init.yml"])
        # A prior acceptance run may have ended before its five-second worker
        # lease expired. Waiting does not alter or stop any fixture process.
        time.sleep(6)

        def node_ready(name, port):
            def probe():
                require(self.processes[name].poll() is None, "cluster node exited during startup")
                with urllib.request.urlopen(f"https://127.0.0.1:{port}/health", context=worker_tls, timeout=2) as response:
                    return response.status == 200
            self.wait(probe)

        self.stage = "cluster_cold_refresh"
        for name, port in (("a1", 14443), ("b1", 14445)):
            self.start(name, [BIN, "node", "--config", fixture / (name + ".yml")])
            node_ready(name, port)
        def worker_call(path):
            try:
                with urllib.request.urlopen("https://127.0.0.1:14443" + path,
                                            context=worker_tls, timeout=8) as response:
                    return response.status, response.read()
            except urllib.error.HTTPError as error:
                return error.code, error.read()

        self.stage = "cluster_required_dataset_readiness"
        require(worker_call("/health")[0] == 200, "missing dataset affected liveness")
        require(worker_call("/ready")[0] == 503, "missing required dataset did not gate readiness")
        code, payload = worker_call("/datasets")
        require(code == 200, "authenticated dataset diagnostics unavailable")
        missing = {entry["id"]: entry for entry in json.loads(payload)}
        require(missing["required_probe"]["state"] == "missing", "required missing state not reported")
        require(missing["optional_probe"]["state"] == "missing", "optional missing state not reported")
        self.record("cluster_required_missing_snapshot_gates_readiness_not_liveness")
        self.status(catalogs["a1"], "required_probe", command="refresh")
        self.wait(lambda: worker_call("/ready")[0] == 200)
        code, payload = worker_call("/datasets")
        require(code == 200, "dataset diagnostics unavailable after refresh")
        entries = json.loads(payload)
        by_id = {entry["id"]: entry for entry in entries}
        require(by_id["required_probe"]["state"] == "ready", "required refresh did not become ready")
        require(by_id["optional_probe"]["state"] == "missing", "optional probe unexpectedly refreshed")
        allowed = {"id", "state", "generation", "refreshed_at", "age_ns", "schema_hash"}
        require(all(set(entry).issubset(allowed) for entry in entries), "diagnostics exposed unapproved fields")
        require(str(shared).encode() not in payload and b"fingerprint" not in payload
                and b"SELECT" not in payload and b"tenant-a.csv" not in payload,
                "diagnostics exposed private storage or source configuration")
        require(worker_call("/health")[0] == 200, "dataset probes affected liveness")
        self.record("cluster_required_refresh_restores_readiness_optional_missing_ignored")
        self.record("cluster_dataset_diagnostics_sanitized", allowed_fields=len(allowed))

        self.start("gateway", [BIN, "gateway", "--config", fixture / "gateway1.yml"])
        self.wait(lambda: self.cluster_call("/ready")[0] == 200)

        def ready_snapshot(name, previous=None):
            # During a cold start, a nonzero status exit is expected. Keep its
            # diagnostics private and retry without declaring a failed check.
            try:
                snapshot = self.status(catalogs[name], "orders_fast")
            except AssertionError:
                return False
            return snapshot if snapshot["ready"] and snapshot["generation"] != previous else False

        first_a = self.wait(lambda: ready_snapshot("a1"))
        first_b = self.wait(lambda: ready_snapshot("b1"))
        self.cluster_query("a", 3)  # a1 is the only available A worker.
        self.cluster_query("b", 30)
        self.record("cluster_cold_refresh_through_nats", tenants=2, broker_replicas=3)

        self.stage = "cluster_second_worker_alias"
        self.start("a2", [BIN, "node", "--config", fixture / "a2.yml"])
        node_ready("a2", 14444)
        require(self.status(catalogs["a2"], "orders_fast")["ready"], "second worker cannot see the shared snapshot")
        self.stop("a1")
        self.cluster_query("a", 3)  # a2 is now the only available A worker.
        self.record("cluster_alias_works_on_both_tenant_a_nodes", nodes_verified=2)

        self.stage = "cluster_scheduled_refresh"
        write_private(sources["a"], "id,label\n7,a\n8,a\n")
        write_private(sources["b"], "id,label\n70,b\n80,b\n")
        self.wait(lambda: ready_snapshot("a2", first_a["generation"]))
        self.wait(lambda: ready_snapshot("b1", first_b["generation"]))
        # A generation may have refreshed just before the source edit. Wait on
        # its actual contents rather than assuming every new generation saw it.
        def refreshed_values():
            for tenant, name, total in (("a", "a2", 15), ("b", "b1", 150)):
                snap = self.status(catalogs[name], "orders_fast")
                file = shared / tenant / "orders_fast" / (snap["generation"] + ".parquet")
                rows = pq.read_table(file).to_pylist()
                if sum(row["id"] for row in rows) != total or any(row["label"] != tenant for row in rows):
                    return False
            return True
        self.wait(refreshed_values)
        query_a = self.cluster_query("a", 15, headers={"X-Kelvo-Tenant": "b"})
        query_b = self.cluster_query("b", 150)
        self.record("cluster_scheduled_refresh_replaces_both_tenant_snapshots", configured_interval_seconds=5)

        self.stage = "cluster_tenant_isolation"
        for query_id, other in ((query_a, "b"), (query_b, "a")):
            for suffix in ("", "/results"):
                code, _ = self.cluster_call("/v1/queries/" + query_id + suffix, other,
                                            headers={"X-Kelvo-Tenant": "a" if other == "b" else "b"})
                require(code == 404, "cross-tenant query handle or result was visible")
        for tenant in ("a", "b"):
            require(stat.S_IMODE((shared / tenant).stat().st_mode) == 0o700, "tenant namespace is not private")
        self.record("cluster_tenant_snapshots_and_handles_are_isolated", spoofed_tenant_header_ignored=True)

        self.stage = "cluster_refresh_queue_evidence"
        def queue_deliveries():
            observed = set()
            for port in (18222, 18223, 18224):
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/jsz?accounts=true&streams=true&consumers=true", timeout=2) as response:
                    monitor = json.load(response)
                for account in monitor.get("account_details", []):
                    for stream in account.get("stream_detail", []):
                        if account.get("name") in ("a", "b") and stream.get("name") == "KELVO_ACCEL_QUEUE":
                            for consumer in stream.get("consumer_detail", []):
                                if consumer.get("name") == "refresh" and consumer.get("delivered", {}).get("consumer_seq", 0) > 0:
                                    observed.add(account["name"])
            return observed == {"a", "b"}
        self.wait(queue_deliveries)
        self.record("private_refresh_queues_delivered_jobs", tenant_queues=2)

    def cleanup(self):
        failed = False
        for name in reversed(self.processes):
            try:
                self.stop(name)
            except BaseException:
                failed = True
        for path, (contents, mode) in self.catalog_backups.items():
            try:
                write_private(path, contents)
                path.chmod(mode)
            except BaseException:
                failed = True
        require(not failed, "one or more acceptance cleanup operations failed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster", action="store_true", help="Also exercise the existing loopback cluster fixture")
    args = parser.parse_args()
    acceptance = Acceptance(args.cluster)
    def interrupted(signum, frame):
        raise InterruptedError("acceptance interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    outcome = {"schema_version": 1, "passed": False, "mode": "standalone_and_cluster" if args.cluster else "standalone",
               "checked_at": datetime.now(timezone.utc).isoformat(), "checks": acceptance.checks,
               "scope": "Real-process correctness acceptance; no throughput or production-readiness claim"}
    result = 1
    try:
        require(BIN.is_file() and os.access(BIN, os.X_OK), "built Kelvo binary is unavailable")
        acceptance.standalone()
        if args.cluster:
            acceptance.cluster_checks()
        outcome["passed"] = True
        result = 0
    except BaseException as error:
        outcome["failure"] = {"stage": acceptance.stage, "type": type(error).__name__}
        write_private(acceptance.directory / "failure.log", traceback.format_exc())
        print("Acceleration acceptance failed at " + acceptance.stage + "; diagnostics retained privately", file=sys.stderr)
    finally:
        try:
            acceptance.cleanup()
        except BaseException as error:
            outcome["passed"] = False
            outcome["cleanup_failure"] = {"type": type(error).__name__}
            write_private(acceptance.directory / "cleanup-failure.log", traceback.format_exc())
            result = 1
        EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
        EVIDENCE.write_text(json.dumps(outcome, indent=2) + "\n")
    return result


if __name__ == "__main__":
    sys.exit(main())
