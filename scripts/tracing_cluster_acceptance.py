#!/usr/bin/env python3
"""VM-only actual-CLI OTLP acceptance; no installation or benchmark."""
import argparse
import concurrent.futures
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import ssl
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True


REQUIRED = {
    "private_mounted_trust_and_three_peer_fixture",
    "client_context_and_cross_tenant_rejection",
    "durable_trace_across_gateway_restart",
    "exact_arrow_values_and_durable_eos",
    "collector_outages_preserve_queries_and_bound_shutdown",
    "collector_privacy_and_caps",
    "owned_service_and_broker_processes_stopped",
}
FORGED_TRACE = "1" * 32
FORGED_SPAN = "2" * 16
CANARY = "kelvo-otlp-private-canary"
SOURCE = "otlp_private_source"
SQL = "SELECT id, amount, at, note FROM otlp_private_source ORDER BY id"
VIOLATIONS = ("privacy_violations", "plan_source_privacy_violations", "cap_violations",
              "protocol_violations", "auth_violations", "write_violations", "connections_rejected")


def validate_durable_parents(events, trace_id, expected_parent_id, require):
    """Check an explicit parent graph against the unmodified captured corpus."""
    children = []
    for name, role in (("kelvo.cluster.dispatch", "worker"), ("kelvo.query", "worker"),
                       ("kelvo.cluster.result_wait", "gateway2"), ("kelvo.cluster.relay", "gateway2")):
        matching = [event for event in events if event["name"] == name and event["role"] == role]
        if name == "kelvo.cluster.result_wait" and not matching:
            require(False, "RESULT_WAIT_OBSERVATION_INCONCLUSIVE")
        require(len(matching) == 1, "EXPECTED_SPAN_COUNT")
        child = matching[0]
        require(child["trace_id"] == trace_id and child["parent_id"] == expected_parent_id, "DURABLE_PARENT_RELATIONSHIP")
        children.append(child)
    return children


def main(args):
    # Import reviewed helpers from the pinned candidate; never rewrite them.
    repo = Path(args.repo).resolve()
    sys.path.insert(0, str(repo / "scripts"))
    import audit_cluster_acceptance as audit
    import cluster_fixture as cf
    import operational_acceptance as ops
    import pyarrow as pa
    import pyarrow.parquet as pq
    import yaml

    require = audit.require
    private = Path(args.private).resolve()
    require(not private.exists(), "FRESH_PRIVATE_DIRECTORY_REQUIRED")
    private.mkdir(mode=0o700)
    os.umask(0o077)
    cf.DIR = private / "cluster-private"

    class Acceptance(audit.Smoke):
        def __init__(self):
            super().__init__(args)
            self.collector = Path(args.collector).resolve()
            self.ca = Path(args.ca_directory).resolve()
            self.mode_file = private / "collector-mode"
            self.event_file = private / "events-private.jsonl"
            self.summary_file = private / "collector-summary-private.json"
            self.public = Path(args.output).resolve()
            self.outage_durations = {}
            self.manifest = json.loads((self.ca / "manifest.json").read_text())
            self.inputs.update(collector=self.collector, acceptance_harness=Path(__file__),
                               operational_harness=Path(ops.__file__))
            self.input_hashes.update({name: audit.digest(path) for name, path in self.inputs.items()})
            require(self.input_hashes["collector"] == args.collector_sha256, "COLLECTOR_HASH_MISMATCH")
            self.env.update(KELVO_OTLP_FIXTURE_TOKEN=secrets.token_hex(32),
                            OTEL_SERVICE_NAME=CANARY,
                            OTEL_RESOURCE_ATTRIBUTES="fixture.private=" + CANARY)
            for name in ("SSL_CERT_FILE", "SSL_CERT_DIR"):
                self.env.pop(name, None)
            self.system_context = ssl.create_default_context()
            self.system_context.minimum_version = ssl.TLSVersion.TLSv1_3
            self.expected = pa.table({
                "id": pa.array([9007199254740993, 9223372036854775807], type=pa.int64()),
                "amount": pa.array([Decimal("-12345678901234.5678"), None], type=pa.decimal128(20, 4)),
                "at": pa.array([1700000000123456789, None], type=pa.timestamp("ns")),
                "note": pa.array([CANARY, None], type=pa.string()),
            })

        def provision(self):
            require(sys.platform == "linux" and os.geteuid() != 0, "NON_ROOT_LINUX_REQUIRED")
            require(not any(name in os.environ for name in ("SSL_CERT_FILE", "SSL_CERT_DIR")), "AMBIENT_CA_OVERRIDE_REFUSED")
            bundle = Path("/etc/ssl/certs/ca-certificates.crt")
            require(audit.digest(bundle) == self.manifest["prepared_bundle_sha256"], "PREPARED_TRUST_NOT_MOUNTED")
            mounted = [line.split() for line in Path("/proc/self/mountinfo").read_text().splitlines()]
            require(any(fields[4] == str(bundle) and "ro" in fields[5].split(",") for fields in mounted), "READ_ONLY_CA_BIND_REQUIRED")
            for path in (self.binary, self.sandbox, self.collector):
                require(path.is_file() and not path.is_symlink() and os.access(path, os.X_OK)
                        and path.stat().st_mode & 0o022 == 0, "PRIVATE_PREBUILT_EXECUTABLE_REQUIRED")
            self.claimed_fixture = True
            cf.provision(nats_archive=Path(args.nats_archive))
            self.env.update(cf.fixture_env())
            self.context = ssl.create_default_context(cafile=str(cf.DIR / "ca.pem"))
            self.context.minimum_version = ssl.TLSVersion.TLSv1_3
            self.gateway_context = ssl.create_default_context(cafile=str(cf.DIR / "ca.pem"))
            self.gateway_context.minimum_version = ssl.TLSVersion.TLSv1_3
            self.gateway_context.load_cert_chain(cf.DIR / "gateway.pem", cf.DIR / "gateway.key")
            source_path = cf.DIR / "source-private-canary.parquet"
            pq.write_table(self.expected, source_path)
            gateway = self.config("gateway1")
            tenant = next(t for t in gateway["tenants"] if t["policy"]["tenant_id"] == "a")
            policy = tenant["policy"]
            policy.update(job_ttl="2m", workers={"a1": 1})
            policy["access"] = {"revision": 1, "principals": {"analyst": {
                "kind": "user", "federated_sources": [SOURCE], "allow_literal_queries": False}}}
            tenant.pop("token_env")
            tenant["workers"] = [entry for entry in tenant["workers"] if entry["id"] == "a1"]
            # Both tenants use principal files. B is used only for denial checks.
            other = next(t for t in gateway["tenants"] if t["policy"]["tenant_id"] == "b")
            other.pop("token_env")
            other["policy"]["access"] = {"revision": 1, "principals": {"outsider": {
                "kind": "user", "federated_sources": [], "allow_literal_queries": False}}}
            keys = cf.DIR / "principal-keys.yml"
            cf.write(keys, yaml.safe_dump({"version": 2, "revision": 1, "principals": {
                "a": {"analyst": [self.env["KELVO_TOKEN_A"]]},
                "b": {"outsider": [self.env["KELVO_TOKEN_B"]]}}}))
            gateway["authentication"] = {"keys_file": str(keys), "reload_interval": "1s", "min_revision": 1}
            for role, port in (("gateway1", 14440), ("gateway2", 14441)):
                config = json.loads(json.dumps(gateway))
                config["listen"] = "127.0.0.1:" + str(port)
                config["tracing"] = self.tracing(role)
                self.write_config(role, config)
            init = json.loads(json.dumps(gateway))
            for entry in init["tenants"]:
                name = entry["policy"]["tenant_id"] + "admin"
                entry["nats"].update(username=name, password_env="KELVO_NATS_" + name.upper())
            self.write_config("init", init)
            node = self.config("a1")
            node.update(policy=policy, sandbox_path=str(self.sandbox), tracing=self.tracing("worker"), metrics={"enabled": False})
            cf.write(Path(node["catalog_file"]), yaml.safe_dump({"sources": [
                {"id": SOURCE, "type": "parquet", "path": str(source_path)}]}, sort_keys=False))
            self.write_config("a1", node)
            denied = [SQL, SOURCE, CANARY, str(source_path), "fixture.private", "administrator"]
            denied.extend(value for key, value in self.env.items() if key.startswith("KELVO_TOKEN_") or key.startswith("KELVO_NATS_") and len(value) >= 8)
            denied.append(self.env["KELVO_OTLP_FIXTURE_TOKEN"])
            cf.write(private / "denied-private.json", json.dumps(sorted(set(denied))))
            cf.write(private / "plan-source-denied-private.json", json.dumps([SQL, SOURCE, str(source_path)]))
            self.set_mode("accept")
            self.command(["cluster-init", "--config", str(cf.DIR / "init.yml")])
            self.check("private_mounted_trust_and_three_peer_fixture", nats_peers=3, tls_minimum="1.3")

        def tracing(self, role):
            return {"endpoint": "https://127.0.0.1:14318/v1/traces/" + role,
                    "token_env": "KELVO_OTLP_FIXTURE_TOKEN", "sample_ratio": 1,
                    "queue_size": 64, "export_timeout": "3s"}

        def set_mode(self, mode):
            require(mode in ("accept", "stall", "unavailable"), "INVALID_COLLECTOR_MODE")
            temporary = self.mode_file.with_suffix(".next")
            cf.write(temporary, mode + "\n")
            temporary.replace(self.mode_file)

        def launch(self, name, command):
            require(name not in self.processes or self.processes[name].poll() is not None, "PROCESS_ALREADY_RUNNING")
            with (private / (name + "-private.log")).open("ab") as log:
                self.processes[name] = subprocess.Popen(command, env=self.env, cwd=repo,
                    stdout=log, stderr=log, start_new_session=True)

        def start_collector(self):
            self.launch("collector", [str(self.collector), "--cert", str(self.ca / "collector.pem"),
                "--key", str(self.ca / "collector.key"), "--mode-file", str(self.mode_file),
                "--deny-file", str(private / "denied-private.json"), "--events", str(self.event_file),
                "--plan-source-deny-file", str(private / "plan-source-denied-private.json"),
                "--summary", str(self.summary_file)])
            self.wait(lambda: self.collector_status()["requests"] == 0, "COLLECTOR_NOT_READY")

        def start(self, name):
            require(name in ("gateway1", "gateway2", "a1"), "INVALID_SERVICE_NAME")
            if name == "a1":
                time.sleep(max(0, self.worker_restart_after - time.monotonic()))
            self.launch(name, [str(self.binary), "node" if name == "a1" else "gateway",
                "--config", str(cf.DIR / (name + ".yml")), "--drain-timeout", "0s"])
            def ready():
                require(self.processes[name].poll() is None, "SERVICE_EXITED")
                return self.call("/ready", role=name)[0] == 200
            self.wait(ready, "SERVICE_NOT_READY")

        def call(self, path, body=None, role="gateway2", authenticated=True, tenant="a", sent=None):
            require(role in ("gateway1", "gateway2", "a1", "collector"), "INVALID_HTTP_ROLE")
            port = {"gateway1": 14440, "gateway2": 14441, "a1": 14443, "collector": 14318}[role]
            context = self.system_context if role == "collector" else self.gateway_context if role == "a1" else self.context
            connection = ops.StrictHTTPSConnection("127.0.0.1", port, context=context, timeout=12 if sent else 3)
            headers = {}
            if authenticated:
                token = self.env["KELVO_OTLP_FIXTURE_TOKEN"] if role == "collector" else self.env["KELVO_TOKEN_" + tenant.upper()]
                headers["Authorization"] = "Bearer " + token
            if role != "collector":
                headers.update(traceparent="00-" + FORGED_TRACE + "-" + FORGED_SPAN + "-01",
                               tracestate="private=" + CANARY, baggage="private=" + CANARY,
                               **{"X-Kelvo-Principal": "administrator"})
            data = json.dumps(body).encode() if body is not None else None
            if data is not None:
                headers["Content-Type"] = "application/json"
            try:
                connection.request("POST" if data is not None else "GET", path, body=data, headers=headers)
                if sent is not None:
                    sent.set()
                response = connection.getresponse()
                raw, response_headers = ops.read_http_response(response)
                require(not any(key in response_headers for key in ("traceparent", "tracestate", "baggage")), "PUBLIC_TRACE_HEADERS")
                return response.status, raw, response_headers
            finally:
                connection.close()

        def collector_status(self):
            require(self.processes["collector"].poll() is None, "COLLECTOR_EXITED")
            code, body, _ = self.call("/fixture/status", role="collector")
            require(code == 200, "COLLECTOR_STATUS_FAILED")
            return json.loads(body)

        def state(self, identifier, role="gateway2"):
            code, body, _ = self.call("/v1/queries/" + identifier, role=role)
            require(code == 200, "DURABLE_STATUS_UNAVAILABLE")
            decoded = json.loads(body)
            require(not any("trace" in key.lower() for key in decoded), "PUBLIC_TRACE_FIELD")
            return decoded["state"]

        def submit(self, role="gateway2"):
            code, body, _ = self.call("/v1/queries", {"mode": "federated", "sources": [SOURCE], "sql": SQL}, role=role)
            require(code == 201, "QUERY_SUBMISSION_FAILED")
            decoded = json.loads(body)
            require(set(decoded) == {"id", "state"}, "PUBLIC_SUBMISSION_FIELDS")
            return decoded["id"]

        def result(self, identifier, sent=None):
            code, body, headers = self.call("/v1/queries/" + identifier + "/results", sent=sent)
            require(code == 200 and headers.get("kelvo-result-completion") == "durable-eos-v1"
                    and body.endswith(ops.EOS), "INCOMPLETE_DURABLE_ARROW")
            source = pa.BufferReader(body)
            with pa.ipc.open_stream(source) as reader:
                actual = reader.read_all()
                require(source.tell() == len(body), "TRAILING_ARROW_BYTES")
            require(actual.schema.equals(self.expected.schema) and actual.equals(self.expected), "ARROW_PRECISION_OR_NULL_LOSS")
            require(actual.column("at").cast(pa.int64()).to_pylist() == [1700000000123456789, None], "TIMESTAMP_PRECISION_LOSS")
            require(self.state(identifier) == "succeeded", "EOS_WITHOUT_DURABLE_SUCCESS")
            return len(body)

        def graph(self):
            require(self.event_file.stat().st_size < 4 << 20, "EVENT_FILE_BOUND")
            events = [json.loads(line) for line in self.event_file.read_text().splitlines()]
            require(len(events) <= 2048 and all(e["mode"] == "accept" for e in events), "CORRECTNESS_EVENT_BOUND")
            require(all(e["trace_id"] != FORGED_TRACE and e["span_id"] != FORGED_SPAN and e["parent_id"] != FORGED_SPAN for e in events), "CLIENT_TRACE_CONTEXT_ACCEPTED")
            require(len({e["span_id"] for e in events}) == len(events), "DUPLICATE_SPAN")
            def one(group, name, role):
                matching = [event for event in group if event["name"] == name and event["role"] == role]
                require(len(matching) == 1, "EXPECTED_SPAN_COUNT")
                return matching[0]
            root = one(events, "kelvo.cluster.submit", "gateway1")
            require(root["parent_id"] == "", "SUBMISSION_NOT_ROOT")
            durable = [event for event in events if event["trace_id"] == root["trace_id"]]
            startup = [event for event in events if event["trace_id"] != root["trace_id"]]
            # Runtime starts each operation from the immutable stored carrier.
            # Dispatch, result_wait, relay and query are siblings, not a chain.
            children = validate_durable_parents(durable, root["trace_id"], root["span_id"], require)
            query_span = next(e for e in children if e["name"] == "kelvo.query")
            phase_names = {"kelvo.phase." + name for name in ("validation", "node_admission", "source_admission", "prepare", "execution_delivery", "cleanup")}
            def check_phases(group, parent):
                phases = [e for e in group if e["name"].startswith("kelvo.phase.")]
                require(len(phases) == 6 and {e["name"] for e in phases} == phase_names and all(e["role"] == "worker"
                    and e["trace_id"] == parent["trace_id"] and e["parent_id"] == parent["span_id"] and e["attributes"] == {} for e in phases), "QUERY_PHASE_PARENT_RELATIONSHIP")
            check_phases(durable, query_span)
            # NewNode executes an independent SELECT 1 probe after tracing,
            # resource admission and source quota hooks are installed. Account
            # for that exact tree; never discard arbitrary unrelated spans.
            probe = one(startup, "kelvo.query", "worker")
            require(probe["parent_id"] == "", "STARTUP_PROBE_NOT_ROOT")
            check_phases(startup, probe)
            require(len(durable) == 11 and len(startup) == 7 and len(events) == 18
                and all(e["attributes"] == {"kelvo.kind": "query", "kelvo.outcome": "success"} for e in [root, probe, *children]), "UNEXPECTED_TRACE_EVENT")
            # Independently evaluate an exact wrong-parent expectation against
            # these same events; do not alter the corpus or start another run.
            negative_rejected = False
            try:
                validate_durable_parents(durable, root["trace_id"], children[0]["span_id"], require)
            except audit.Failure as error:
                require(str(error) == "DURABLE_PARENT_RELATIONSHIP", "CONTROLLED_PARENT_NEGATIVE_WRONG_FAILURE")
                negative_rejected = True
            require(negative_rejected, "CONTROLLED_PARENT_NEGATIVE_ACCEPTED")
            # Do not compare cross-process timestamps or sum nested spans.
            self.check("durable_trace_across_gateway_restart", durable_spans=len(durable), startup_probe_spans=len(startup),
                       durable_phases=6, startup_probe_phases=6, submit_gateway_stopped_before_dispatch=True,
                       controlled_wrong_parent_rejected=True)

        def flow(self):
            self.start_collector()
            self.start("gateway1")
            request = {"mode": "federated", "sources": [SOURCE], "sql": SQL}
            require(self.call("/v1/queries", request, role="gateway1", authenticated=False)[0] == 401, "UNAUTHENTICATED_QUERY_ACCEPTED")
            forged = dict(request, trace={"version": 1, "trace_id": FORGED_TRACE, "span_id": FORGED_SPAN, "sampled": True})
            require(self.call("/v1/queries", forged, role="gateway1")[0] == 400, "CLIENT_TRACE_BODY_ACCEPTED")
            identifier = self.submit("gateway1")
            require(self.state(identifier, "gateway1") == "queued", "QUERY_NOT_DURABLY_QUEUED")
            require(self.call("/v1/queries/" + identifier, role="gateway1", tenant="b")[0] == 404, "CROSS_TENANT_HANDLE_VISIBLE")
            self.check("client_context_and_cross_tenant_rejection", unauthenticated_status=401, forged_body_status=400, cross_tenant_status=404)
            self.stop("gateway1")
            self.start("gateway2")
            sent = threading.Event()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(self.result, identifier, sent)
                require(sent.wait(3), "RESULT_REQUEST_NOT_SENT")
                require(self.state(identifier) == "queued", "DURABLE_QUEUE_LOST_ON_RESTART")
                time.sleep(0.25)  # Exact wait-span observation is required below.
                self.start("a1")
                wire = pending.result(timeout=15)
            self.stop("gateway2")
            self.stop("a1")
            self.graph()
            self.check("exact_arrow_values_and_durable_eos", rows=2, wire_bytes=wire,
                       checks=["int64", "decimal128", "timestamp_ns", "null", "durable_eos"])
            for mode in ("stall", "unavailable"):
                self.set_mode(mode)
                before = self.collector_status()
                self.start("a1")
                self.start("gateway2")
                self.result(self.submit())
                field = "stalled" if mode == "stall" else "unavailable"
                def observed_outage():
                    status = self.collector_status()
                    return all(status["roles"][i][field] > before["roles"][i][field]
                        and (mode != "stall" or status["roles"][i]["active_stalls"] > 0) for i in (1, 2))
                self.wait(observed_outage, "EXPORTER_OUTAGE_NOT_EXERCISED")
                def stop_timed(name):
                    started = time.monotonic()
                    self.stop(name)
                    elapsed = time.monotonic() - started
                    require(elapsed <= 8, "EXPORTER_SHUTDOWN_BOUND_EXCEEDED")
                    return elapsed
                # Stop both exporters while their stalled requests are active.
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                    pending = {name: executor.submit(stop_timed, name) for name in ("gateway2", "a1")}
                    durations = {name: future.result(timeout=20) for name, future in pending.items()}
                self.outage_durations[mode] = durations
            self.check("collector_outages_preserve_queries_and_bound_shutdown", successful_queries=2,
                       shutdown_seconds=self.outage_durations, per_process_bound_seconds=8)
            status = self.collector_status()
            require(all(status[field] == 0 for field in VIOLATIONS), "COLLECTOR_VIOLATION")
            self.check("collector_privacy_and_caps", requests=status["requests"], spans=status["spans"],
                       violations={field: status[field] for field in VIOLATIONS})

        def cleanup(self):
            errors = []
            # Exporters finish before collector. The inherited cleanup then
            # verifies/stops only this fixture's recorded services and brokers.
            for name in ("gateway1", "gateway2", "a1", "collector"):
                try:
                    self.stop(name)
                except Exception as error:
                    errors.append(audit.failure_code(error))
            try:
                errors.extend(super().cleanup())
            except Exception as error:
                errors.append(audit.failure_code(error))
            try:
                if self.summary_file.exists():
                    require(self.summary_file.stat().st_size <= 16 << 10, "FINAL_COLLECTOR_SUMMARY_BOUND")
                    summary = json.loads(self.summary_file.read_text())
                    roles = summary["roles"]
                    require(all(type(summary[field]) is int and summary[field] == 0 for field in VIOLATIONS)
                        and type(roles) is list and len(roles) == 3
                        and all(type(role["active_stalls"]) is int and role["active_stalls"] == 0 for role in roles), "FINAL_COLLECTOR_VIOLATION")
                elif "collector" in self.processes:
                    errors.append("FINAL_COLLECTOR_SUMMARY_MISSING")
            except Exception as error:
                errors.append(audit.failure_code(error))
            return errors

    run = None
    failure = None
    cleanup_errors = []
    report = {"schema_version": 1, "started_at": datetime.now(timezone.utc).isoformat(),
              "scope": "actual CLI TLS OTLP correctness; no throughput, overhead or production capacity claim"}
    def interrupted(signum, frame):
        raise audit.Failure("INTERRUPTED")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        run = Acceptance()
        run.provision()
        run.flow()
    except Exception as error:
        failure = audit.failure_code(error)
    finally:
        if run is not None:
            try:
                cleanup_errors = run.cleanup()
            except Exception as error:
                cleanup_errors = [audit.failure_code(error)]
            report.update(checks=run.checks, input_sha256=run.input_hashes)
        checks = report.get("checks", [])
        report.update(failure=failure, cleanup_errors=cleanup_errors,
            inconclusive=failure == "RESULT_WAIT_OBSERVATION_INCONCLUSIVE",
            passed=failure is None and not cleanup_errors and len(checks) == len(REQUIRED)
                and {check["test"] for check in checks} == REQUIRED and all(check["passed"] for check in checks))
        output = Path(args.output)
        require(not output.exists() and not output.is_symlink(), "FRESH_REPORT_PATH_REQUIRED")
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "binary", "binary-sha256", "sandbox", "sandbox-sha256", "collector", "collector-sha256",
                 "nats-archive", "ca-directory", "private", "output"):
        parser.add_argument("--" + name, required=True)
    raise SystemExit(main(parser.parse_args()))
