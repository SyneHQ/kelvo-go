#!/usr/bin/env python3
"""Offline audited-cluster smoke in a fresh, non-root private-network service.

Requires prebuilt candidate/launcher, cached pinned NATS, PyArrow and PyYAML.
All generated credentials, broker state and logs remain in the private fixture.
Public output contains fixed checks, counts, hashes and scope limitations only.
"""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import signal
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import pyarrow as pa
import yaml

import cluster_fixture as cf


class Failure(RuntimeError):
    pass


def require(condition, code):
    if not condition:
        raise Failure(code)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class SourceFixture:
    """Strict two-query ClickHouse HTTP/Arrow fixture, never a real DB claim."""
    def __init__(self, password):
        self.errors, self.describes, self.scans = [], 0, 0
        self.authorization = "Basic " + base64.b64encode(("audit:" + password).encode()).decode()
        table = pa.table({"id": pa.array([1, 2], type=pa.int64()),
                          "label": pa.array(["a", "a"], type=pa.string())})
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                try:
                    self.connection.settimeout(3)
                    require(len(self.path) <= 8192, "SOURCE_PATH_BOUND")
                    parsed = urllib.parse.urlsplit(self.path)
                    settings = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
                    require(parsed.path == "/", "SOURCE_PATH")
                    require(hmac.compare_digest(self.headers.get("Authorization", ""), owner.authorization), "SOURCE_AUTH")
                    require(self.headers.get("Transfer-Encoding") is None, "SOURCE_FRAMING")
                    lengths = self.headers.get_all("Content-Length", [])
                    require(len(lengths) == 1 and lengths[0].isdigit() and 0 < int(lengths[0]) <= 4096, "SOURCE_BODY_BOUND")
                    for key, value in (("readonly", "1"), ("default_format", "ArrowStream"),
                                       ("result_overflow_mode", "throw"), ("max_threads", "1")):
                        require(settings.get(key) == [value], "SOURCE_SETTINGS")
                    sql = self.rfile.read(int(lengths[0]))
                    if sql == b"SELECT * FROM `audit`.`sample` LIMIT 0":
                        result = table.slice(0, 0)
                        owner.describes += 1
                    elif sql == b"SELECT `id` FROM `audit`.`sample`":
                        # Return both rows. Only Kelvo's local guard may apply
                        # the principal filter; the fixture does not enforce it.
                        result = table.select(["id"])
                        owner.scans += 1
                    else:
                        raise Failure("SOURCE_QUERY_UNEXPECTED")
                    sink = pa.BufferOutputStream()
                    with pa.ipc.new_stream(sink, result.schema) as writer:
                        writer.write_table(result)
                    body = sink.getvalue().to_pybytes()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except Exception as error:
                    owner.errors.append(failure_code(error))
                    self.close_connection = True

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        require(not self.thread.is_alive(), "SOURCE_THREAD_REMAINS")
        require(not self.errors, "SOURCE_FIXTURE_ERROR")


class Smoke:
    def __init__(self, args):
        self.args = args
        self.binary = Path(args.binary).resolve()
        self.sandbox = Path(args.sandbox).resolve()
        self.processes = {}
        self.expected_failures = set()
        self.worker_restart_after = 0
        self.checks = []
        self.env = dict(os.environ, GOMAXPROCS="1")
        self.gateway_journal = cf.DIR / "audit-gateway"
        self.node_journal = cf.DIR / "audit-worker"
        self.context = None
        self.source = None
        self.claimed_fixture = False
        self.inputs = {"binary": self.binary, "sandbox": self.sandbox,
                       "harness": Path(__file__), "fixture_harness": Path(cf.__file__),
                       "nats": Path(args.nats_archive)}
        self.input_hashes = {name: digest(path) for name, path in self.inputs.items()}
        require(self.input_hashes["binary"] == args.binary_sha256, "CANDIDATE_HASH_MISMATCH")
        require(self.input_hashes["sandbox"] == args.sandbox_sha256, "SANDBOX_HASH_MISMATCH")
        require(self.input_hashes["nats"] == cf.DIGEST, "NATS_HASH_MISMATCH")

    def check(self, name, **data):
        self.checks.append(dict(test=name, passed=True, **data))
        print(name + ": passed", flush=True)

    def config(self, name):
        return yaml.safe_load((cf.DIR / (name + ".yml")).read_text())

    def write_config(self, name, config):
        cf.write(cf.DIR / (name + ".yml"), yaml.safe_dump(config, sort_keys=False))

    def audit_config(self, name, directory):
        return dict(service_id=name, directory=str(directory), max_entries=128,
                    max_pending=8, retention="1h", write_timeout="2s")

    def provision(self):
        require(os.geteuid() != 0, "ROOT_EXECUTION_REFUSED")
        require(not cf.DIR.exists() and not cf.DIR.is_symlink(), "FRESH_FIXTURE_REQUIRED")
        self.claimed_fixture = True
        cf.provision(nats_archive=Path(self.args.nats_archive))
        self.env.update(cf.fixture_env())
        self.env.update(KELVO_SOURCE_AUDIT_USER="audit", KELVO_SOURCE_AUDIT_PASSWORD=secrets.token_hex(24))
        self.source = SourceFixture(self.env["KELVO_SOURCE_AUDIT_PASSWORD"])
        self.env["KELVO_SOURCE_AUDIT_URL"] = "http://127.0.0.1:" + str(self.source.server.server_port) + "/"
        self.context = ssl.create_default_context(cafile=str(cf.DIR / "ca.pem"))
        self.context.minimum_version = ssl.TLSVersion.TLSv1_3
        self.gateway_context = ssl.create_default_context(cafile=str(cf.DIR / "ca.pem"))
        self.gateway_context.minimum_version = ssl.TLSVersion.TLSv1_3
        self.gateway_context.load_cert_chain(cf.DIR / "gateway.pem", cf.DIR / "gateway.key")
        self.client = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=self.context))
        self.worker = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=self.gateway_context))
        gateway = self.config("gateway1")
        tenant = next(t for t in gateway["tenants"] if t["policy"]["tenant_id"] == "a")
        tenant.pop("token_env")
        policy = tenant["policy"]
        policy["workers"] = {"a1": 1}
        policy["access"] = {
            "revision": 1,
            "principals": {"analyst": {
                "kind": "user", "federated_sources": ["sample"],
                "allow_literal_queries": True,
                "row_column_policy": {"sources": {"sample": {"tables": {
                    "sample": {"columns": ["id"], "rows": {
                        "kind": "comparison", "column": "id", "op": "eq",
                        "type": "int64", "value": "1"}}}}}}}}}
        tenant["workers"] = [e for e in tenant["workers"] if e["id"] == "a1"]
        gateway["tenants"] = [tenant]
        gateway["max_concurrent"] = 1
        gateway["audit"] = self.audit_config("gateway-one", self.gateway_journal)
        keys = cf.DIR / "principal-keys.yml"
        cf.write(keys, yaml.safe_dump({"version": 2, "revision": 1,
                    "principals": {"a": {"analyst": [self.env["KELVO_TOKEN_A"]]}}}))
        gateway["authentication"] = {"keys_file": str(keys), "reload_interval": "1s", "min_revision": 1}
        self.write_config("gateway1", gateway)
        init = json.loads(json.dumps(gateway))
        init["tenants"][0]["nats"].update(username="aadmin", password_env="KELVO_NATS_AADMIN")
        self.write_config("init", init)
        node = self.config("a1")
        node["policy"] = policy
        node["sandbox_path"] = str(self.sandbox)
        node["audit"] = self.audit_config("worker-a1", self.node_journal)
        self.write_config("a1", node)
        catalog = yaml.safe_load(Path(node["catalog_file"]).read_text())
        catalog["sources"][0]["id"] = "raw"
        catalog["sources"].append({"id": "sample", "type": "clickhouse",
            "url_env": "KELVO_SOURCE_AUDIT_URL", "username_env": "KELVO_SOURCE_AUDIT_USER",
            "password_env": "KELVO_SOURCE_AUDIT_PASSWORD", "federation": {
                "max_scan_rows": 10, "max_scan_bytes": 1 << 20,
                "tables": [{"name": "sample", "database": "audit", "table": "sample"}]}})
        catalog["acceleration"] = {
            "directory": str(cf.DIR / "snapshots"), "tenant_id": "a",
            "datasets": [{"id": "audit_snapshot", "query": {
                "mode": "federated", "sources": ["raw"], "sql": "SELECT * FROM raw"},
                "refresh_interval": "5s", "max_age": "1m", "authorization_version": "audit-v1",
                "limits": {"max_rows": 10, "max_bytes": 1 << 20, "timeout": "8s",
                           "memory_mb": 128, "threads": 1, "max_temp_mb": 64}}]}
        cf.write(Path(node["catalog_file"]), yaml.safe_dump(catalog, sort_keys=False))
        self.command(["cluster-init", "--config", str(cf.DIR / "init.yml")])
        require(not self.gateway_journal.exists() and not self.node_journal.exists(), "INIT_OPENED_AUDIT")
        self.check("offline_private_fixture_and_principal_policy")

    def command(self, args, expected=0):
        result = subprocess.run([str(self.binary), *args], env=self.env, capture_output=True, timeout=30)
        if result.returncode != expected:
            cf.write(cf.DIR / "failed-cli-private.log", result.stdout.decode(errors="replace") + result.stderr.decode(errors="replace"))
        require(result.returncode == expected, "CLI_COMMAND_FAILED")
        return result.stdout

    def start(self, name, expected_failure=False):
        require(name not in self.processes or self.processes[name].poll() is not None, "PROCESS_ALREADY_RUNNING")
        if name == "a1":
            # The fixture's durable worker identity lease is five seconds.
            # A clean process exit does not release that broker TTL early.
            time.sleep(max(0, self.worker_restart_after - time.monotonic()))
        command = [str(self.binary), "gateway" if name == "gateway1" else "node",
                   "--config", str(cf.DIR / (name + ".yml")), "--drain-timeout", "0s"]
        with open(cf.DIR / (name + "-audit-smoke.log"), "ab") as log:
            proc = subprocess.Popen(command, env=self.env, stdout=log, stderr=log, start_new_session=True)
        self.processes[name] = proc
        self.expected_failures.discard(name)
        if expected_failure:
            require(proc.wait(timeout=15) != 0, "CORRUPT_JOURNAL_STARTED")
            self.expected_failures.add(name)
            return
        path = "/ready"
        def ready():
            require(proc.poll() is None, "SERVICE_EXITED_" + name.upper())
            return self.call(path, worker=name == "a1")[0] == 200
        self.wait(ready, "SERVICE_NOT_READY_" + name.upper())

    def stop(self, name, expected=0):
        proc = self.processes.get(name)
        if proc is not None and proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
                raise Failure("GRACEFUL_STOP_TIMED_OUT") from None
            if name == "a1":
                self.worker_restart_after = time.monotonic() + 6
        if proc is not None and name not in self.expected_failures:
            require(proc.returncode == expected, "GRACEFUL_STOP_FAILED_" + name.upper())
            if expected != 0:
                self.expected_failures.add(name)

    def wait(self, fn, code):
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            try:
                if fn():
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.05)
        raise Failure(code)

    def call(self, path, body=None, worker=False, authenticated=True):
        headers = {"Authorization": "Bearer " + self.env["KELVO_TOKEN_A"]} if authenticated else {}
        headers["X-Kelvo-Principal"] = "administrator"
        data = json.dumps(body).encode() if body is not None else None
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request("https://127.0.0.1:" + ("14443" if worker else "14440") + path,
                                         data=data, headers=headers)
        try:
            with (self.worker if worker else self.client).open(request, timeout=15) as response:
                raw = response.read((1 << 20) + 1)
                require(len(raw) <= 1 << 20, "RESPONSE_BOUND")
                return response.status, raw
        except urllib.error.HTTPError as error:
            return error.code, error.read(4096)

    def submit(self, sql="SELECT * FROM sample.sample ORDER BY id", sources=None):
        status, body = self.call("/v1/queries", {"mode": "federated", "sql": sql,
                                                "sources": ["sample"] if sources is None else sources})
        require(status == 201, "SUBMISSION_FAILED")
        return json.loads(body)["id"]

    def result(self, identifier):
        status, body = self.call("/v1/queries/" + identifier + "/results")
        if status != 200 or not body.endswith(b"\xff\xff\xff\xff\0\0\0\0"):
            (cf.DIR / "failed-result-private.bin").write_bytes(body)
            state_status, state_body = self.call("/v1/queries/" + identifier)
            cf.write(cf.DIR / "failed-result-status-private.json", json.dumps({
                "http_status": status, "state_http_status": state_status,
                "state_body": state_body.decode(errors="replace")}))
            raise Failure("INCOMPLETE_RESULT_HTTP_" + str(status))
        table = pa.ipc.open_stream(body).read_all()
        require(table.column_names == ["id"] and table.to_pydict() == {"id": [1]}, "ROW_COLUMN_POLICY_MISMATCH")
        return len(body)

    def state(self, identifier):
        status, raw = self.call("/v1/queries/" + identifier)
        require(status == 200, "STATUS_UNAVAILABLE")
        return json.loads(raw)["state"]

    def events(self, directory):
        page = json.loads(self.command(["audit", "read", "--directory", str(directory), "--cursor", "0", "--limit", "256"]))
        require(page["done"] and len(page["events"]) < 128, "AUDIT_PAGE_BOUND")
        return page["events"]

    def refreshed(self):
        status, body = self.call("/metrics", worker=True)
        require(status == 200, "REFRESH_METRICS_UNAVAILABLE")
        match = re.search(rb'(?m)^kelvo_jobs_completed_total\{kind="refresh",outcome="success"\} ([0-9]+)$', body)
        return match is not None and int(match[1]) > 0

    def flow(self):
        self.start("a1")
        self.start("gateway1")
        active_read = subprocess.run([str(self.binary), "audit", "read", "--directory", str(self.gateway_journal)],
                                     env=self.env, capture_output=True, timeout=15)
        require(active_read.returncode != 0 and not active_read.stdout, "LIVE_OPERATOR_READ_ALLOWED")
        require(self.call("/v1/queries", {}, authenticated=False)[0] == 401, "AUTHENTICATION_BYPASS")
        identifier = self.submit()
        wire_bytes = self.result(identifier)
        require(self.state(identifier) == "succeeded", "DURABLE_RESULT_STATE")
        canceled = self.submit("SELECT 1", [])
        require(self.call("/v1/queries/" + canceled + "/cancel", {})[0] == 200, "GATEWAY_CANCEL_FAILED")
        require(self.state(canceled) == "cancelled", "GATEWAY_CANCEL_NOT_DURABLE")
        direct = self.submit("SELECT 1", [])
        self.wait(lambda: self.state(direct) == "assigned", "DIRECT_CANCEL_NOT_ASSIGNED")
        require(self.call("/internal/queries/" + direct + "/cancel", {}, worker=True)[0] == 204, "DIRECT_CANCEL_FAILED")
        require(self.state(direct) == "cancelled", "DIRECT_CANCEL_NOT_DURABLE")
        self.wait(self.refreshed, "SCHEDULED_REFRESH_DID_NOT_COMPLETE")
        self.check("audited_principal_query_and_both_cancellation_paths", rows=1, wire_bytes=wire_bytes)
        self.stop("gateway1")
        self.stop("a1")
        gateway_events, worker_events = self.events(self.gateway_journal), self.events(self.node_journal)
        snapshot = yaml.safe_load(self.command(["accelerate", "verify", "--config", str(cf.DIR / "a1-catalog.yml"), "--dataset", "audit_snapshot"]))
        require(snapshot["ready"] and snapshot["snapshot"]["rows"] == 2, "REFRESH_SNAPSHOT_ROWS_MISMATCH")
        kinds = {event["kind"] for event in gateway_events}
        require({"query_submit", "query_results", "query_cancel", "authentication"} <= kinds, "GATEWAY_AUDIT_MISSING")
        for event in gateway_events:
            binding = event["binding"]
            if event["kind"] == "authentication":
                require(binding["tenant_id"] == "" and binding["principal_kind"] == "unknown", "DENIAL_ATTRIBUTION")
            else:
                require(binding["tenant_id"] == "a" and binding["principal_id"] == "analyst"
                        and binding["principal_kind"] == "user" and len(binding["policy_version"]) == 64, "PRINCIPAL_ATTRIBUTION")
        require(any(e["kind"] == "query_cancel" and e["binding"]["principal_id"] == "gateway"
                    and e["binding"]["principal_kind"] == "service" for e in worker_events), "WORKER_CANCEL_ATTRIBUTION")
        for events in (gateway_events, worker_events):
            cancellations = [e for e in events if e["kind"] == "query_cancel"]
            require(len(cancellations) == 1 and cancellations[0]["outcome"] == "succeeded"
                    and cancellations[0]["category"] == "none", "CANCELLATION_AUDIT_OUTCOME")
        refreshes = [e for e in worker_events if e["kind"] == "refresh_execution"]
        require(refreshes and any(e["outcome"] == "succeeded" and e["category"] == "none" for e in refreshes), "REFRESH_AUDIT_OUTCOME")
        require(all(e["binding"]["principal_id"] == "worker-a1" and e["binding"]["principal_kind"] == "service"
                    and e["binding"]["tenant_id"] == "a" and len(e["binding"]["policy_version"]) == 64 for e in refreshes), "REFRESH_SERVICE_ATTRIBUTION")
        self.check("scheduled_refresh_publishes_verified_snapshot_with_service_receipt", snapshot_rows=2, refresh_receipts=len(refreshes))
        for directory in (self.gateway_journal, self.node_journal):
            raw = (directory / "journal.bin").read_bytes()
            for secret in (self.env["KELVO_TOKEN_A"], self.env["KELVO_SOURCE_AUDIT_PASSWORD"], "SELECT *", "administrator", "a1-data.csv"):
                require(secret.encode() not in raw, "AUDIT_PAYLOAD_LEAK")
        before_ids = {e["id"] for e in gateway_events}
        self.check("offline_receipts_have_trusted_identity_and_no_payload", gateway_receipts=len(gateway_events), worker_receipts=len(worker_events))
        self.start("a1")
        self.start("gateway1")
        self.result(self.submit())
        self.stop("gateway1")
        require(before_ids <= {e["id"] for e in self.events(self.gateway_journal)}, "RESTART_LOST_RECEIPTS")
        self.check("restart_preserves_durable_local_receipts")
        self.stop("a1")
        before_executions = sum(e["kind"] == "query_execution" and e["binding"]["principal_kind"] == "user"
                                for e in self.events(self.node_journal))
        self.start("a1")
        self.start("gateway1")
        # Corrupt only this fixture's private journal, after established idle
        # service operation. No host or shared fixture storage is touched.
        with open(self.gateway_journal / "journal.bin", "r+b") as journal:
            journal.truncate(1)
            journal.flush()
            os.fsync(journal.fileno())
        require(self.call("/v1/queries", {"mode": "federated", "sql": "SELECT 1"})[0] == 503, "DAMAGED_AUDIT_ADMITTED_QUERY")
        require(self.call("/ready")[0] == 503, "DAMAGED_AUDIT_REMAINED_READY")
        self.stop("gateway1", expected=1)
        self.stop("a1")
        after_executions = sum(e["kind"] == "query_execution" and e["binding"]["principal_kind"] == "user"
                               for e in self.events(self.node_journal))
        require(after_executions == before_executions, "SOURCE_EXECUTED_WITH_FAILED_AUDIT")
        self.start("gateway1", expected_failure=True)
        require((self.gateway_journal / "journal.bin").stat().st_size == 1, "CORRUPT_JOURNAL_REWRITTEN")
        self.check("runtime_and_restart_fail_closed_after_journal_damage")
        replacement = cf.DIR / "audit-gateway-replacement"
        config = self.config("gateway1")
        config["audit"] = self.audit_config("gateway-one", replacement)
        self.write_config("gateway1", config)
        self.start("a1")
        self.start("gateway1")
        self.result(self.submit())
        self.stop("gateway1")
        self.stop("a1")
        require(self.events(replacement), "REPLACEMENT_JOURNAL_EMPTY")
        require((self.gateway_journal / "journal.bin").stat().st_size == 1, "REPLACEMENT_DESTROYED_OLD_JOURNAL")
        require(self.source.scans == 3 and self.source.describes >= 3 and not self.source.errors, "SOURCE_REQUEST_COUNTS")
        self.check("explicit_replacement_recovers_service_and_preserves_old_journal")

    def cleanup(self):
        errors = []
        for name in list(self.processes):
            try:
                self.stop(name)
            except Exception as error:
                errors.append(failure_code(error))
        if self.source is not None:
            try:
                self.source.close()
            except Exception as error:
                errors.append(failure_code(error))
        if not self.claimed_fixture:
            return errors
        cf.stop()
        records = json.loads((cf.DIR / "pids.json").read_text()) if (cf.DIR / "pids.json").exists() else []
        deadline = time.monotonic() + 5
        while any(cf.broker_alive(item) for item in records) and time.monotonic() < deadline:
            time.sleep(0.05)
        require(not any(cf.broker_alive(item) for item in records), "BROKER_CLEANUP_INCOMPLETE")
        require(all(p.poll() is not None for p in self.processes.values()), "SERVICE_CLEANUP_INCOMPLETE")
        self.check("owned_service_and_broker_processes_stopped")
        return errors


def failure_code(error):
    return str(error) if isinstance(error, Failure) else type(error).__name__


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--sandbox", required=True)
    parser.add_argument("--nats-archive", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--sandbox-sha256", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    require(re.fullmatch(r"[0-9a-f]{40}", args.candidate_revision), "FULL_CANDIDATE_REVISION_REQUIRED")
    output = Path(args.output)
    require(not output.exists() and not output.is_symlink(), "FRESH_OUTPUT_REQUIRED")
    smoke = Smoke(args)
    outcome, failure, cleanup_errors, integrity_errors = "passed", None, [], []
    started = time.monotonic()
    try:
        smoke.provision()
        smoke.flow()
    except (Exception, SystemExit) as error:
        outcome, failure = "failed", failure_code(error)
    finally:
        try:
            cleanup_errors = smoke.cleanup()
        except Exception as error:
            cleanup_errors.append(failure_code(error))
        for name, path in smoke.inputs.items():
            try:
                require(digest(path) == smoke.input_hashes[name], "INPUT_CHANGED_" + name.upper())
            except Exception as error:
                integrity_errors.append(failure_code(error))
        if cleanup_errors or integrity_errors:
            outcome = "failed"
    evidence = {
        "date": datetime.now(timezone.utc).isoformat(), "status": outcome, "failure": failure,
        "cleanup_errors": cleanup_errors, "integrity_errors": integrity_errors,
        "candidate_revision": args.candidate_revision,
        "input_sha256_before_and_after": smoke.input_hashes,
        "duration_seconds": round(time.monotonic() - started, 3), "checks": smoke.checks,
        "scope": "Fresh offline loopback NATS, ClickHouse HTTP/Arrow protocol fixture, real audited gateway and sandboxed worker, local row/column guard, CSV scheduled refresh",
        "limits": ["local receipt IDs only", "no power-loss or host-loss guarantee", "no external database/provider acceptance", "no sustained load claim"],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as stream:
        stream.write(json.dumps(evidence, indent=2) + "\n")
    if outcome != "passed":
        raise SystemExit("Audit service acceptance failed: " + (failure or "CLEANUP_OR_INTEGRITY_FAILURE"))


if __name__ == "__main__":
    main()
