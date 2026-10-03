#!/usr/bin/env python3
"""Exact-pair Linux rolling upgrade acceptance in an owned network namespace.

Uses prebuilt binaries and a cached checksum-pinned broker archive. No installs,
source submission retries, shared fixtures, or arbitrary-version compatibility
claims. Private logs, keys and source SQL remain under ignored artifacts.
"""
import argparse
from datetime import datetime, timezone
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import operational_acceptance as ops
import process_loss_acceptance as loss
from gateway_key_rotation_acceptance import atomic_private

OLD_REVISION = "c7ecbf1a8f8391b13cd1bc96a4ae847d5c8b052c"
NEW_REVISION = "0d6b489fb072c823a8cd6d20e6beba98a3aa3158"
APP_ROLES = ("gateway", "a1", "gateway2", "b1")
WAVES = tuple("upgrade_"+role for role in APP_ROLES) + tuple("broker_restart_"+str(i) for i in range(3)) + tuple("rollback_"+role for role in APP_ROLES)
REQUIRED = ("startup", "key_rotation_and_floor", *WAVES, "stale_startup_rejected", "ambiguous_gateway_loss", "cleanup")


def key_document(revision, analysts, others):
    return ops.yaml_document({"version": 2, "revision": revision, "principals": {
        tenant: {"analyst": analysts[tenant], "other": [others[tenant]]} for tenant in ("a", "b")}}).encode()


def verify_gate_result(body, headers, tenant, marker):
    import pyarrow as pa
    ops.verify_completed_arrow(body, headers, [{"tenant": tenant, "marker": marker}])
    schema = pa.ipc.open_stream(body).schema
    ops.require(schema.equals(pa.schema([("tenant", pa.string()), ("marker", pa.int64())]), check_metadata=True),
                "GATE_RESULT_SCHEMA_CHANGED")


def validate_matrix(matrix):
    ops.require(matrix.get("old_revision") == OLD_REVISION and matrix.get("new_revision") == NEW_REVISION,
                "UNDECLARED_BINARY_PAIR")
    ops.require(matrix.get("nats_server_version") == ops.cf.VERSION and matrix.get("nats_client_unchanged") is True
                and matrix["old"]["nats_client"] == matrix["new"]["nats_client"], "UNDECLARED_BROKER_CLIENT_PAIR")
    for version in ("old", "new"):
        for field in ("source_archive_sha256", "binary_sha256", "launcher_sha256"):
            ops.require(re.fullmatch(r"[a-f0-9]{64}", matrix[version].get(field, "")) is not None, "ARTIFACT_HASH_REQUIRED")
    ops.require(matrix["old"]["launcher_sha256"] == matrix["new"]["launcher_sha256"], "UNDECLARED_SANDBOX_CHANGE")


def reconcile(report):
    checks = report.get("checks", [])
    if (report.get("interrupted") is not False or len(checks) != len(REQUIRED)
            or {item.get("test") for item in checks} != set(REQUIRED)
            or not all(item.get("passed") is True for item in checks)):
        return False
    by_name = {item["test"]: item for item in checks}
    for name in WAVES:
        item = by_name[name]
        if (item.get("running_before_transition") != 2 or item.get("queued_before_transition") != 2
                or item.get("exact_running_results") != 2 or item.get("exact_queued_results") != 2
                or item.get("original_handles_preserved") is not True or item.get("consumed_results_rejected") is not True
                or item.get("one_observed_source_request_per_marker") is not True
                or item.get("revoked_keys_denied") is not True or item.get("foreign_handles_hidden") is not True):
            return False
    security = by_name["stale_startup_rejected"]
    fault = by_name["ambiguous_gateway_loss"]
    clean = by_name["cleanup"]
    return (by_name["key_rotation_and_floor"].get("both_replica_floors") == 3
            and security.get("rejected_probes") == 8 and security.get("current_metadata_revalidated") is True
            and security.get("current_configuration_hashes_preserved") is True
            and fault.get("failed_result_rejected") is True and fault.get("failed_attempt_not_replayed") is True
            and fault.get("surviving_tenant_exact_result") is True and fault.get("queued_handles_preserved") is True
            and fault.get("one_observed_source_request_per_marker") is True
            and clean.get("forced_application_kills") == 0 and clean.get("observed_live_descendants") == 0
            and clean.get("worker_scratch_directories") == 0 and clean.get("original_configurations_verified") is True
            and clean.get("all_owned_brokers_stopped") is True and clean.get("fixture_gate_errors") == 0
            and clean.get("input_artifact_hashes_preserved") is True)


class ArrowGate:
    """One registered marker produces one tenant-labelled Arrow batch on release."""
    def __init__(self):
        import pyarrow  # Fail clearly before opening a fixture socket if absent.
        self.lock = threading.Lock()
        self.entries = {}
        self.errors = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_POST(self):
                try:
                    parsed = urllib.parse.urlsplit(self.path)
                    settings = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
                    ops.require(parsed.path in ("/a", "/b") and len(self.path) <= 8192, "GATE_PATH_INVALID")
                    ops.require(self.headers.get("Transfer-Encoding") is None, "GATE_FRAMING_INVALID")
                    lengths = self.headers.get_all("Content-Length", [])
                    ops.require(len(lengths) == 1 and lengths[0].isdigit() and 0 < int(lengths[0]) <= 4096,
                                "GATE_BODY_BOUND")
                    for key, value in (("readonly", "1"), ("default_format", "ArrowStream"),
                                       ("cancel_http_readonly_queries_on_client_close", "1"), ("result_overflow_mode", "throw")):
                        ops.require(settings.get(key) == [value], "GATE_SETTINGS_INVALID")
                    self.connection.settimeout(3)
                    raw = self.rfile.read(int(lengths[0]))
                    match = re.fullmatch(rb"SELECT ([0-9]{1,12}) AS marker", raw)
                    ops.require(match is not None, "GATE_MARKER_INVALID")
                    identity = (parsed.path[1:], int(match[1]))
                    with owner.lock:
                        entry = owner.entries.get(identity)
                        ops.require(entry is not None, "GATE_MARKER_UNREGISTERED")
                        entry["requests"] += 1
                        ops.require(entry["requests"] == 1, "GATE_SOURCE_REPLAY")
                        entry["started"].set()
                    ops.require(entry["release"].wait(50), "GATE_RELEASE_DEADLINE")
                    import pyarrow as pa
                    table = pa.table({"tenant": pa.array([identity[0]], type=pa.string()),
                                      "marker": pa.array([identity[1]], type=pa.int64())})
                    sink = pa.BufferOutputStream()
                    with pa.ipc.new_stream(sink, table.schema) as writer:
                        writer.write_table(table)
                    body = sink.getvalue().to_pybytes()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    # An intentionally killed gateway cancels its upstream.
                    # Such a disconnect is observed, not a source retry.
                    pass
                except Exception as error:
                    with owner.lock:
                        owner.errors.append(type(error).__name__)
                    self.close_connection = True

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()

    def register(self, tenant, marker):
        with self.lock:
            ops.require((tenant, marker) not in self.entries, "GATE_DUPLICATE_MARKER")
            entry = {"started": threading.Event(), "release": threading.Event(), "requests": 0}
            self.entries[tenant, marker] = entry
            return entry

    def close(self):
        with self.lock:
            for entry in self.entries.values():
                entry["release"].set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        ops.require(not self.thread.is_alive(), "GATE_THREAD_REMAINS")


class UpgradeAcceptance(loss.LossAcceptance):
    def __init__(self, args):
        super().__init__(args)
        self.env["GOMAXPROCS"] = "1"
        self.matrix = json.loads(Path(args.matrix).read_text())
        validate_matrix(self.matrix)
        self.binaries = {"old": self.binary, "new": Path(args.new_binary).resolve()}
        self.roles = {role: "old" for role in APP_ROLES}
        self.gate = ArrowGate()
        self.gate.start()
        self.old = {tenant: secrets.token_urlsafe(32) for tenant in ("a", "b")}
        self.new = {tenant: secrets.token_urlsafe(32) for tenant in ("a", "b")}
        self.other = {tenant: secrets.token_urlsafe(32) for tenant in ("a", "b")}
        self.current = dict(self.old)
        self.keys = {gateway: self.directory/f"keys-{gateway}.yml" for gateway in (1, 2)}
        self.marker = 0
        self.artifacts = {self.binaries[v]: self.matrix[v]["binary_sha256"] for v in ("old", "new")}
        self.artifacts.update({Path(args.old_archive).resolve(): self.matrix["old"]["source_archive_sha256"],
                               Path(args.new_archive).resolve(): self.matrix["new"]["source_archive_sha256"],
                               self.sandbox: self.matrix["old"]["launcher_sha256"],
                               Path(args.nats_archive).resolve(): ops.cf.DIGEST})
        self.fixture_provisioned = False

    def call(self, path, tenant="a", body=None, node=None, timeout=12, with_headers=False, gateway=None, token=None):
        if node:
            return ops.Acceptance.call(self, path, tenant, body, node, timeout, with_headers)
        gateway = self.default_gateway if gateway is None else gateway
        headers = {"Authorization": "Bearer "+(token if token is not None else self.current[tenant])}
        data = None if body is None else json.dumps(body).encode()
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(f"https://127.0.0.1:{14439+gateway}"+path, data=data, headers=headers)
        try:
            with self.client_opener.open(request, timeout=timeout) as response:
                raw, returned = ops.read_http_response(response)
                return (response.status, raw, returned) if with_headers else (response.status, raw)
        except urllib.error.HTTPError as error:
            with error:
                raw, returned = ops.read_http_response(error)
                return (error.code, raw, returned) if with_headers else (error.code, raw)

    def start_process(self, name, command):
        import yaml
        command = list(command)
        if name in self.roles:
            command[0] = self.binaries[self.roles[name]]
        if name in ("a1", "b1"):
            path = self.directory/(name+"-catalog.yml")
            catalog = yaml.safe_load(path.read_text())
            if not any(source["id"] == "roll_source" for source in catalog["sources"]):
                catalog["sources"].append({"id": "roll_source", "type": "clickhouse",
                                           "url_env": "KELVO_SOURCE_ROLL_"+name[0].upper()+"_URL"})
                ops.cf.write(path, yaml.safe_dump(catalog, sort_keys=False))
        return super().start_process(name, command)

    def start_gateway(self, gateway):
        name = "gateway" if gateway == 1 else "gateway2"
        self.start_process(name, [self.binary, "gateway", "--config", self.directory/f"loss-gateway-{gateway}.yml",
                                  "--drain-timeout", "8s"])
        self.wait(lambda: self.call("/ready", gateway=gateway, timeout=1)[0] == 200)

    def startup(self):
        import yaml
        validate_matrix(self.matrix)
        ops.require(all(path.is_file() and ops.sha256(path) == digest for path, digest in self.artifacts.items()),
                    "INPUT_ARTIFACT_MISMATCH")
        ops.require(not self.fixture.exists(), "FRESH_EXCLUSIVE_FIXTURE_REQUIRED")
        previous = ops.cf.DIR
        ops.cf.DIR = self.fixture
        try:
            ops.cf.provision(self.args.nats_archive)
        finally:
            ops.cf.DIR = previous
            self.fixture_provisioned = (self.fixture/"pids.json").is_file()
        for gateway in (1, 2):
            atomic_private(self.keys[gateway], key_document(1, {t: [v] for t, v in self.old.items()}, self.other))
        for path in self.fixture.glob("*.yml"):
            if path.name.endswith("-catalog.yml"):
                continue
            config = yaml.safe_load(path.read_text())
            policies = [item["policy"] for item in config.get("tenants", [])]
            if "policy" in config:
                policies.append(config["policy"])
            for policy in policies:
                policy["job_ttl"] = "2m"
                policy["limits"]["timeout"] = "60s"
                policy["access"] = {"revision": 2, "principals": {
                    "analyst": {"kind": "user", "native_sources": ["roll_source"], "federated_sources": ["ops_snapshot"]},
                    "other": {"kind": "user", "federated_sources": ["ops_snapshot"]}}}
            if "tenants" in config:
                for tenant in config["tenants"]:
                    tenant.pop("token_env", None)
                key = 2 if path.name == "gateway2.yml" else 1
                config["authentication"] = {"keys_file": str(self.keys[key]), "reload_interval": "1s", "min_revision": 1}
            ops.cf.write(path, yaml.safe_dump(config, sort_keys=False))
        for tenant in ("a", "b"):
            self.env["KELVO_SOURCE_ROLL_"+tenant.upper()+"_URL"] = f"http://127.0.0.1:{self.gate.server.server_port}/{tenant}"
        detail = super().startup()
        # The base creates both gateway configs from gateway1; give replica two
        # its own key-document path before this campaign rotates either one.
        self.processes["gateway2"].send_signal(signal.SIGTERM)
        ops.require(self.processes["gateway2"].wait(timeout=12) == 0, "INITIAL_GATEWAY_DRAIN_FAILED")
        path = self.directory/"loss-gateway-2.yml"
        config = yaml.safe_load(path.read_text())
        config["authentication"]["keys_file"] = str(self.keys[2])
        ops.cf.write(path, yaml.safe_dump(config, sort_keys=False))
        self.start_gateway(2)
        return {**detail, "explicit_principals": 4, "policy_revision": 2,
                "declared_old_revision": OLD_REVISION, "declared_new_revision": NEW_REVISION,
                "source_fixture": "Gated ClickHouse HTTP protocol returning exact Arrow; not a ClickHouse server benchmark"}

    def key_rotation(self):
        import yaml
        overlap = key_document(2, {t: [self.old[t], self.new[t]] for t in ("a", "b")}, self.other)
        for path in self.keys.values():
            atomic_private(path, overlap)
        for gateway in (1, 2):
            for tenant in ("a", "b"):
                self.wait(lambda: self.call("/v1/queries/key-probe", gateway=gateway, token=self.new[tenant])[0] == 404, 5)
                ops.require(self.call("/v1/queries/key-probe", gateway=gateway, token=self.old[tenant])[0] == 404,
                            "KEY_OVERLAP_NOT_OBSERVED")
        current = key_document(3, {t: [self.new[t]] for t in ("a", "b")}, self.other)
        for path in self.keys.values():
            atomic_private(path, current)
        self.current = dict(self.new)
        for gateway in (1, 2):
            for tenant in ("a", "b"):
                self.wait(lambda: self.call("/v1/queries/key-probe", gateway=gateway, token=self.old[tenant])[0] == 401, 5)
            path = self.directory/f"loss-gateway-{gateway}.yml"
            config = yaml.safe_load(path.read_text())
            config["authentication"]["min_revision"] = 3
            ops.cf.write(path, yaml.safe_dump(config, sort_keys=False))
        self.security()
        return {"overlap_and_revocation_on_both_replicas": True, "both_replica_floors": 3,
                "rollback_requires_current_keys_and_policy": True}

    def security(self):
        before = sum(entry["requests"] for entry in self.gate.entries.values())
        for gateway in (1, 2):
            for tenant in ("a", "b"):
                ops.require(self.call("/v1/queries/key-probe", gateway=gateway, token=self.old[tenant])[0] == 401,
                            "REVOKED_KEY_RESTORED")
                code, _ = self.call("/v1/queries", tenant, gateway=gateway, token=self.other[tenant],
                                    body={"mode": "native", "connection_id": "roll_source", "sql": "SELECT 0 AS marker"})
                ops.require(code == 403, "UNGRANTED_SOURCE_ACCEPTED")
        ops.require(sum(entry["requests"] for entry in self.gate.entries.values()) == before, "DENIED_SOURCE_EXECUTED")

    def ownership(self, handles):
        for tenant, item in handles.items():
            for query_id in (item["running"], item["queued"]):
                for token in (self.other[tenant], self.current["b" if tenant == "a" else "a"]):
                    for suffix, body in (("", None), ("/results", None), ("/cancel", {})):
                        ops.require(self.call("/v1/queries/"+query_id+suffix, token=token, body=body)[0] == 404,
                                    "FOREIGN_HANDLE_DISCLOSED_OR_MUTATED")

    def hold(self, tenant, query_id, gateway):
        try:
            return self.call("/v1/queries/"+query_id+"/results", tenant, gateway=gateway, timeout=45, with_headers=True)
        except (OSError, urllib.error.URLError, http.client.HTTPException, ops.AcceptanceError):
            return None

    def prepare_load(self):
        handles = {}
        for tenant, gateway in (("a", 1), ("b", 2)):
            self.marker += 1
            gate = self.gate.register(tenant, self.marker)
            code, raw = self.call("/v1/queries", tenant, gateway=gateway,
                                  body={"mode": "native", "connection_id": "roll_source", "sql": f"SELECT {self.marker} AS marker"})
            ops.require(code == 201, "GATED_QUERY_NOT_ADMITTED")
            query_id = json.loads(raw)["id"]
            future = self.pool.submit(self.hold, tenant, query_id, gateway)
            self.pending.append(future)
            handles[tenant] = {"running": query_id, "future": future, "gate": gate, "marker": self.marker}
        self.wait(lambda: all(item["gate"]["started"].is_set() and self.state(item["running"], tenant) == "running"
                              for tenant, item in handles.items()), 10)
        for tenant, item in handles.items():
            item["queued"] = self.submit(tenant)
        ops.require(all(self.state(item["running"], tenant) == "running" and self.state(item["queued"], tenant) == "queued"
                        and not item["future"].done() for tenant, item in handles.items()), "ADMITTED_MIXED_STATES_NOT_OBSERVED")
        self.ownership(handles)
        return handles

    def finish_load(self, handles):
        for item in handles.values():
            item["gate"]["release"].set()
        for tenant, item in handles.items():
            response = item["future"].result(timeout=20)
            ops.require(response is not None and response[0] == 200, "RUNNING_RESULT_LOST_DURING_ROLL")
            verify_gate_result(response[1], response[2], tenant, item["marker"])
            self.queued_result(item, tenant)
        # No new submission may replace a terminal slot before these checks.
        for tenant, item in handles.items():
            for name in ("running", "queued"):
                ops.require(self.state(item[name], tenant) == "succeeded", "ORIGINAL_HANDLE_STATE_LOST")
                ops.require(self.call("/v1/queries/"+item[name]+"/results", tenant)[0] == 409, "CONSUMED_RESULT_REPLAYED")
            ops.require(item["gate"]["requests"] == 1, "SOURCE_REQUEST_REPLAYED")
        self.ownership(handles)
        return {"running_before_transition": 2, "queued_before_transition": 2, "exact_running_results": 2,
                "exact_queued_results": 2, "original_handles_preserved": True, "consumed_results_rejected": True,
                "one_observed_source_request_per_marker": True, "foreign_handles_hidden": True}

    def restart_role(self, role, version):
        self.roles[role] = version
        if role.startswith("gateway"):
            self.start_gateway(1 if role == "gateway" else 2)
        else:
            command = [self.binaries[version], "node", "--config", self.directory/(role+".yml"), "--drain-timeout", "8s"]
            self.start_process(role, command)
            def ready():
                process = self.processes[role]
                if process.poll() is not None:
                    tail = (self.directory/(role+".log")).read_bytes()[-4096:]
                    ops.require(b"worker identity is already active" in tail, "ROLLED_WORKER_START_FAILED")
                    self.start_process(role, command)
                    return False
                return self.call("/ready", node=role, timeout=1)[0] == 200
            self.wait(ready, 20)

    def app_wave(self, role, version):
        handles = self.prepare_load()
        process = self.processes[role]
        process.send_signal(signal.SIGTERM)
        node = None if role.startswith("gateway") else role
        gateway = 1 if role == "gateway" else 2
        self.wait(lambda: self.call("/ready", node=node, gateway=gateway, timeout=1)[0] == 503, 3)
        ops.require(process.poll() is None and all(not item["future"].done() for item in handles.values()),
                    "DRAIN_DID_NOT_RETAIN_RUNNING_WORK")
        for item in handles.values():
            item["gate"]["release"].set()
        if node:
            # The target's queued handle intentionally stays unassigned until
            # its replacement can reserve work. The other tenant keeps running.
            self.default_gateway = 2 if gateway == 1 else 1
            ops.require(process.wait(timeout=12) == 0, "WORKER_GRACEFUL_EXIT_FAILED")
            self.restart_role(role, version)
            detail = self.finish_load(handles)
        else:
            detail = self.finish_load(handles)
            ops.require(process.wait(timeout=12) == 0, "GATEWAY_GRACEFUL_EXIT_FAILED")
            self.restart_role(role, version)
        self.default_gateway = 1
        self.security()
        return {**detail, "role": role, "destination": version, "roles_after_transition": dict(self.roles),
                "readiness_rejected_during_drain": True, "graceful_exit": True, "revoked_keys_denied": True}

    def broker_wave(self, index):
        handles = self.prepare_load()
        broker = next(item for item in self.brokers if item["name"] == "nats-"+str(index))
        ops.require(self.broker_alive(broker), "BROKER_IDENTITY_CHANGED")
        identity = ops.proc_identity(broker["pid"])
        ops.require(identity is not None, "BROKER_NOT_RUNNING")
        ops.signal_owned(identity, signal.SIGTERM)
        self.wait(lambda: ops.proc_identity(identity[0]) != identity and self.broker_ports_closed(broker), 5)
        ops.require(all(not item["future"].done() for item in handles.values()), "BROKER_TRANSITION_LOAD_ENDED_EARLY")
        # Preserve two replicas, restart the same pinned broker, and require all
        # metadata peers current before checking the original query handles.
        self.restart_broker(broker)
        detail = self.finish_load(handles)
        self.security()
        return {**detail, "broker_version_before": ops.cf.VERSION, "broker_version_after": ops.cf.VERSION,
                "replicas_after_rejoin": 3, "one_broker_at_a_time": True, "revoked_keys_denied": True}

    def stale_startups(self):
        import yaml
        protected = list(self.keys.values()) + [self.directory/f"loss-gateway-{g}.yml" for g in (1, 2)]
        protected += [self.directory/(role+".yml") for role in ("a1", "b1")] + [self.fixture/"init.yml"]
        before = {path: ops.sha256(path) for path in protected}
        stale_key = self.directory/"stale-keys.yml"
        atomic_private(stale_key, key_document(2, {t: [self.old[t]] for t in ("a", "b")}, self.other))
        rejected = 0
        for version in ("old", "new"):
            for kind in ("keys", "gateway_policy", "node_policy", "initializer_policy"):
                role = "node" if kind == "node_policy" else "cluster-init" if kind == "initializer_policy" else "gateway"
                template = self.directory/"a1.yml" if role == "node" else self.fixture/"init.yml" if role == "cluster-init" else self.directory/"loss-gateway-1.yml"
                config = yaml.safe_load(template.read_text())
                if kind == "keys":
                    config["authentication"]["keys_file"] = str(stale_key)
                    config["authentication"]["min_revision"] = 3
                elif role == "node":
                    config["policy"]["access"]["revision"] = 1
                else:
                    for tenant in config["tenants"]:
                        tenant["policy"]["access"]["revision"] = 1
                if role != "cluster-init":
                    config["listen"] = "127.0.0.1:14450"
                path = self.directory/f"probe-{version}-{kind}.yml"
                ops.cf.write(path, yaml.safe_dump(config, sort_keys=False))
                process = subprocess.run([str(self.binaries[version]), role, "--config", str(path)], env=self.env,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
                ops.cf.write(path.with_suffix(".log"), (process.stdout+process.stderr).decode(errors="replace"))
                expected = b"key" if kind == "keys" else b"cluster metadata mismatch"
                ops.require(process.returncode != 0 and expected in process.stderr, "STALE_STARTUP_NOT_REJECTED")
                rejected += 1
                ops.require(all(ops.sha256(p) == digest for p, digest in before.items()), "GOOD_CONFIGURATION_REWRITTEN")
            # A current create-only initialization must still validate the
            # exact durable metadata after each stale initializer attempt.
            with (self.directory/(version+"-current-metadata.log")).open("wb") as log:
                process = subprocess.run([str(self.binaries[version]), "cluster-init", "--config", str(self.fixture/"init.yml")],
                                         env=self.env, stdout=log, stderr=log, timeout=15)
            ops.require(process.returncode == 0, "CURRENT_DURABLE_METADATA_CHANGED")
        for tenant in ("a", "b"):
            self.query(tenant)
        self.security()
        return {"rejected_probes": rejected, "roles": ["gateway", "worker", "initializer"],
                "current_metadata_revalidated": True, "current_configuration_hashes_preserved": True,
                "revoked_keys_remain_denied": True}

    def ambiguous_loss(self):
        handles = self.prepare_load()
        self.kill_application("gateway")
        self.default_gateway = 2
        try:
            for item in handles.values():
                item["gate"]["release"].set()
            self.failed_attempt(handles["a"], "a")
            response = handles["b"]["future"].result(timeout=12)
            ops.require(response is not None and response[0] == 200, "SURVIVING_TENANT_FAILED")
            verify_gate_result(response[1], response[2], "b", handles["b"]["marker"])
            for tenant, item in handles.items():
                self.queued_result(item, tenant)
            ops.require(self.state(handles["a"]["running"], "a") in loss.TERMINAL_FAILURE, "FAILED_HANDLE_REGRESSED")
            ops.require(self.call("/v1/queries/"+handles["a"]["running"]+"/results")[0] == 409, "FAILED_HANDLE_REPLAYED")
            ops.require(all(item["gate"]["requests"] == 1 for item in handles.values()), "AMBIGUOUS_SOURCE_REPLAY")
        finally:
            self.restart_role("gateway", "old")
            self.default_gateway = 1
        self.security()
        return {"running_before_fault": 2, "queued_before_fault": 2, "failed_result_rejected": True,
                "failed_attempt_not_replayed": True, "surviving_tenant_exact_result": True,
                "queued_handles_preserved": True, "one_observed_source_request_per_marker": True,
                "scope": "A killed result-owning gateway cancels its observed attempt; no general exactly-once source execution claim"}

    def cleanup(self):
        for entry in self.gate.entries.values():
            entry["release"].set()
        detail = None
        try:
            detail = super().cleanup()
        finally:
            self.gate.close()
            if self.fixture_provisioned:
                # Cleanup only identities checked against this fresh fixture.
                if not self.brokers:
                    self.brokers = json.loads((self.fixture/"pids.json").read_text())
                for broker in self.brokers:
                    if self.broker_alive(broker):
                        identity = ops.proc_identity(broker["pid"])
                        if identity is not None:
                            ops.signal_owned(identity, signal.SIGTERM)
                self.wait(lambda: all(not self.broker_alive(b) and self.broker_ports_closed(b) for b in self.brokers), 5)
        ops.require(not self.gate.errors, "SOURCE_FIXTURE_FAILED")
        ops.require(all(ops.sha256(path) == digest for path, digest in self.artifacts.items()), "INPUT_ARTIFACT_CHANGED")
        return {**detail, "all_owned_brokers_stopped": True, "fixture_gate_errors": 0, "input_artifact_hashes_preserved": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "new-binary", "sandbox", "matrix", "old-archive", "new-archive", "nats-archive", "fixture", "output"):
        parser.add_argument("--"+name, required=True)
    args = parser.parse_args()
    args.managed_scratch = True
    output = Path(args.output)
    ops.require(not output.exists() and not output.is_symlink(), "OUTPUT_ALREADY_EXISTS")
    os.umask(0o077)
    acceptance = UpgradeAcceptance(args)
    report = {"schema_version": 1, "passed": False, "checks": acceptance.checks, "matrix": acceptance.matrix,
              "checked_at": datetime.now(timezone.utc).isoformat(),
              "scope": "Exact two-binary single-host rolling acceptance with gated native Arrow and synthetic CTE queries. Same-version NATS restarts only; not arbitrary-version, multi-host or capacity certification.",
              "script_sha256": ops.sha256(Path(__file__)), "operational_script_sha256": ops.sha256(Path(ops.__file__)),
              "process_loss_script_sha256": ops.sha256(Path(loss.__file__)), "fixture_script_sha256": ops.sha256(Path(ops.cf.__file__))}
    interrupted = False
    def interrupt(signum, frame):
        raise InterruptedError()
    signal.signal(signal.SIGTERM, interrupt)
    actions = [("startup", acceptance.startup), ("key_rotation_and_floor", acceptance.key_rotation)]
    actions.extend(("upgrade_"+role, lambda role=role: acceptance.app_wave(role, "new")) for role in APP_ROLES)
    actions.extend(("broker_restart_"+str(i), lambda i=i: acceptance.broker_wave(i)) for i in range(3))
    actions.extend(("rollback_"+role, lambda role=role: acceptance.app_wave(role, "old")) for role in APP_ROLES)
    actions.extend((("stale_startup_rejected", acceptance.stale_startups), ("ambiguous_gateway_loss", acceptance.ambiguous_loss)))
    try:
        active = True
        for name, callback in actions:
            if active:
                active = acceptance.record(name, callback)
            else:
                acceptance.checks.append({"test": name, "passed": False, "category": "DEPENDENCY_FAILED"})
    except (KeyboardInterrupt, InterruptedError):
        interrupted = True
    finally:
        def cleaning_signal(signum, frame):
            nonlocal interrupted
            interrupted = True
        signal.signal(signal.SIGTERM, cleaning_signal)
        signal.signal(signal.SIGINT, cleaning_signal)
        acceptance.record("cleanup", acceptance.cleanup)
    for name in REQUIRED:
        if not any(check["test"] == name for check in acceptance.checks):
            acceptance.checks.append({"test": name, "passed": False, "category": "INTERRUPTED"})
    report["interrupted"] = interrupted
    report["elapsed_seconds"] = round(time.monotonic()-acceptance.started, 3)
    report["resource_observations"] = acceptance.samples.evidence()
    report["passed"] = reconcile(report)
    ops.cf.write(output, json.dumps(report, indent=2)+"\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
