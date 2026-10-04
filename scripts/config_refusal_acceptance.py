#!/usr/bin/env python3
"""VM-only immutable old-binary controls and explicit security-config refusal.

Uses private, disposable three-broker clusters and a strict Arrow source fixture.
It never retries query submission. All generated config, logs and credentials
remain private; public evidence includes only fixed checks, counts and hashes.
"""
import argparse
import copy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import signal
import socket
import ssl
import subprocess
import time
import traceback
import urllib.request

import pyarrow as pa
import yaml

import audit_cluster_acceptance as audit
import cluster_fixture as cf
import operational_acceptance as ops

OLD_BUILDS = {
    "old_c7ecbf1": ("c7ecbf1a8f8391b13cd1bc96a4ae847d5c8b052c",
                    "4e818e2d9e274c4fe25f8d877a1d618da5ee59f89e53dc40bfab5e63dc09a08f"),
    "old_0d6b489": ("0d6b489fb072c823a8cd6d20e6beba98a3aa3158",
                    "d5460cae30c210a6536299cfbcaae4d6adb57ed166487ac091d442304865a4fe"),
}
FEATURES = ("row_column_policy", "audit")
ROLES = ("gateway", "node", "cluster-init")


def isolation_evidence():
    ops.require(os.geteuid() != 0, "NON_ROOT_REQUIRED")
    host_network = os.environ.get("KELVO_TEST_HOST_NETWORK_NAMESPACE", "")
    ops.require(re.fullmatch(r"net:\[[0-9]+\]", host_network) is not None, "HOST_NETWORK_IDENTITY_REQUIRED")
    # Reading /proc/1/ns/net requires ptrace permission on hardened hosts. The
    # launcher provides its own nonsecret namespace identity before isolation.
    ops.require(os.readlink("/proc/self/ns/net") != host_network and socket.if_nameindex() == [(1, "lo")],
                "PRIVATE_NETWORK_REQUIRED")
    relative = next(line[3:] for line in Path("/proc/self/cgroup").read_text().splitlines() if line.startswith("0::"))
    group = Path("/sys/fs/cgroup") / relative.lstrip("/")
    memory = int((group / "memory.max").read_text())
    swap = int((group / "memory.swap.max").read_text())
    quota, period = map(int, (group / "cpu.max").read_text().split())
    tasks = int((group / "pids.max").read_text())
    ops.require(memory == 3 << 30 and swap == 0 and quota == period and tasks == 256,
                "REQUIRED_SERVICE_LIMITS_MISSING")
    return dict(non_root=True, private_network=True, memory_max_bytes=memory, swap_max_bytes=swap,
                cpu_quota=quota, cpu_period=period, tasks_max=tasks)


def add_feature(config, feature, journal):
    config = copy.deepcopy(config)
    if feature == "audit":
        config["audit"] = dict(service_id="compatibility", directory=str(journal), max_entries=128,
                               max_pending=8, retention="1h", write_timeout="2s")
    elif feature == "row_column_policy":
        policies = [t["policy"] for t in config.get("tenants", [])]
        if "policy" in config:
            policies.append(config["policy"])
        for policy in policies:
            policy["access"]["principals"]["analyst"][feature] = {"sources": {"sample": {"tables": {
                "sample": {"columns": ["id"], "rows": {"kind": "comparison", "column": "id",
                             "op": "eq", "type": "int64", "value": "1"}}}}}}
    else:
        raise ops.AcceptanceError("UNKNOWN_FEATURE")
    return config


class RefusalCase(audit.Smoke):
    def __init__(self, args, feature=None):
        super().__init__(args)
        self.feature = feature
        self.inputs["harness"] = Path(__file__)
        self.inputs["audit_helper"] = Path(audit.__file__)
        self.inputs["protocol_helper"] = Path(ops.__file__)
        self.input_hashes = {name: ops.sha256(path) for name, path in self.inputs.items()}

    def provision(self):
        ops.require(os.geteuid() != 0, "ROOT_EXECUTION_REFUSED")
        ops.require(not cf.DIR.exists() and not cf.DIR.is_symlink(), "FRESH_FIXTURE_REQUIRED")
        self.claimed_fixture = True
        cf.provision(self.args.nats_archive)
        self.env = {"PATH": "/usr/bin:/bin", "HOME": str(cf.DIR), "TMPDIR": str(cf.DIR), "GOMAXPROCS": "1"}
        self.env.update(cf.fixture_env())
        self.env.update(KELVO_SOURCE_AUDIT_USER="audit", KELVO_SOURCE_AUDIT_PASSWORD=self.env["KELVO_TOKEN_A"])
        self.source = audit.SourceFixture(self.env["KELVO_SOURCE_AUDIT_PASSWORD"])
        self.env["KELVO_SOURCE_AUDIT_URL"] = "http://127.0.0.1:" + str(self.source.server.server_port) + "/"
        self.context = ssl.create_default_context(cafile=str(cf.DIR / "ca.pem"))
        self.context.minimum_version = ssl.TLSVersion.TLSv1_3
        self.gateway_context = ssl.create_default_context(cafile=str(cf.DIR / "ca.pem"))
        self.gateway_context.minimum_version = ssl.TLSVersion.TLSv1_3
        self.gateway_context.load_cert_chain(cf.DIR / "gateway.pem", cf.DIR / "gateway.key")
        self.client = urllib.request.build_opener(urllib.request.ProxyHandler({}), ops.StrictHTTPSHandler(context=self.context))
        self.worker = urllib.request.build_opener(urllib.request.ProxyHandler({}), ops.StrictHTTPSHandler(context=self.gateway_context))
        gateway = self.config("gateway1")
        tenant = gateway["tenants"][0]
        tenant.pop("token_env")
        tenant["workers"] = [w for w in tenant["workers"] if w["id"] == "a1"]
        policy = tenant["policy"]
        policy["workers"] = {"a1": 1}
        policy["access"] = {"revision": 1, "principals": {"analyst": {
            "kind": "user", "federated_sources": ["sample"], "allow_literal_queries": True}}}
        gateway["tenants"] = [tenant]
        gateway["max_concurrent"] = 1
        keys = cf.DIR / "principal-keys.yml"
        cf.write(keys, yaml.safe_dump({"version": 2, "revision": 1,
                 "principals": {"a": {"analyst": [self.env["KELVO_TOKEN_A"]]}}}))
        gateway["authentication"] = {"keys_file": str(keys), "reload_interval": "1s", "min_revision": 1}
        node = self.config("a1")
        node["policy"] = copy.deepcopy(policy)
        node["sandbox_path"] = str(self.sandbox)
        catalog = {"sources": [{"id": "sample", "type": "clickhouse",
                   "url_env": "KELVO_SOURCE_AUDIT_URL", "username_env": "KELVO_SOURCE_AUDIT_USER",
                   "password_env": "KELVO_SOURCE_AUDIT_PASSWORD", "federation": {
                       "max_scan_rows": 10, "max_scan_bytes": 1 << 20,
                       "tables": [{"name": "sample", "database": "audit", "table": "sample"}]}}]}
        cf.write(Path(node["catalog_file"]), yaml.safe_dump(catalog, sort_keys=False))
        if self.feature:
            gateway = add_feature(gateway, self.feature, self.gateway_journal)
            node = add_feature(node, self.feature, self.node_journal)
        self.write_config("gateway1", gateway)
        self.write_config("a1", node)
        initializer = copy.deepcopy(gateway)
        initializer["tenants"][0]["nats"].update(username="aadmin", password_env="KELVO_NATS_AADMIN")
        self.write_config("init", initializer)
        self.command(["cluster-init", "--config", str(cf.DIR / "init.yml")])
        ops.require(not self.gateway_journal.exists() and not self.node_journal.exists(), "INIT_OPENED_AUDIT")
        self.check("initialized_exact_baseline_policy")

    def result(self, identifier):
        request = urllib.request.Request("https://127.0.0.1:14440/v1/queries/" + identifier + "/results",
                     headers={"Authorization": "Bearer " + self.env["KELVO_TOKEN_A"]})
        with self.client.open(request, timeout=15) as response:
            ops.require(response.status == 200, "RESULT_HTTP_FAILED")
            body, headers = ops.read_http_response(response)
        rows = [{"id": 1}] if self.feature == "row_column_policy" else [{"id": 1}, {"id": 2}]
        ops.verify_completed_arrow(body, headers, rows)
        ops.require(pa.ipc.open_stream(body).schema.equals(pa.schema([("id", pa.int64())]), check_metadata=True),
                    "RESULT_SCHEMA_CHANGED")
        ops.require(self.state(identifier) == "succeeded", "DURABLE_RESULT_NOT_SUCCEEDED")
        ops.require(self.call("/v1/queries/" + identifier + "/results")[0] == 409, "CONSUMED_RESULT_REPLAYABLE")
        return len(rows)

    def positive(self):
        self.start("a1")
        self.start("gateway1")
        count = self.result(self.submit("SELECT id FROM sample.sample ORDER BY id"))
        self.stop("gateway1")
        self.stop("a1")
        ops.require(not self.source.errors and self.source.scans == 1 and self.source.describes >= 1,
                    "POSITIVE_SOURCE_COUNTS")
        if self.feature == "audit":
            for directory in (self.gateway_journal, self.node_journal):
                events = self.events(directory)
                ops.require(bool(events) and any(e.get("binding", {}).get("principal_id") == "analyst" for e in events),
                            "AUDIT_POSITIVE_RECEIPT_MISSING")
        self.check("supported_configuration_executes_exact_query", rows=count, source_scans=1,
                   durable_success=True, consumed_result_refused=True,
                   row_policy_applied=self.feature == "row_column_policy", audit_receipts=self.feature == "audit")

    def connections(self):
        values = []
        for i in range(3):
            report = cf.broker_monitor(18222+i, "/varz", 1)
            count = report.get("total_connections")
            ops.require(type(count) is int and count >= 0, "BROKER_CONNECTION_COUNTER_REQUIRED")
            values.append(count)
        return values

    def refusal(self, feature, role):
        template = "a1" if role == "node" else "init" if role == "cluster-init" else "gateway1"
        journal = cf.DIR / ("refused-" + feature + "-" + role)
        config = add_feature(self.config(template), feature, journal)
        config["listen"] = "127.0.0.1:14450"
        path = cf.DIR / ("refusal-" + feature + "-" + role + ".yml")
        cf.write(path, yaml.safe_dump(config, sort_keys=False))
        protected = {p: ops.sha256(p) for p in cf.DIR.glob("*.yml")}
        connections = self.connections()
        sources = self.source.scans, self.source.describes
        with (path.with_suffix(".log")).open("wb") as log:
            process = subprocess.Popen([str(self.binary), role, "--config", str(path)], env=self.env,
                                        stdout=log, stderr=log, start_new_session=True)
        self.processes["refusal"] = process
        self.expected_failures.add("refusal")
        opened = False
        deadline = time.monotonic() + 10
        while process.poll() is None and time.monotonic() < deadline:
            with socket.socket() as probe:
                probe.settimeout(0.01)
                opened |= probe.connect_ex(("127.0.0.1", 14450)) == 0
            time.sleep(0.001)
        ops.require(process.poll() is not None and process.returncode != 0, "UNSUPPORTED_CONFIG_STARTED")
        diagnostic = path.with_suffix(".log").read_bytes()
        ops.require(b"invalid cluster YAML configuration" in diagnostic, "EXPLICIT_CONFIG_REFUSAL_MISSING")
        with socket.socket() as probe:
            probe.settimeout(0.05)
            opened |= probe.connect_ex(("127.0.0.1", 14450)) == 0
        ops.require(not opened and not journal.exists(), "REFUSED_SERVICE_SIDE_EFFECT")
        ops.require(connections == self.connections(), "REFUSAL_CONNECTED_TO_BROKER")
        ops.require(sources == (self.source.scans, self.source.describes) and not self.source.errors,
                    "REFUSAL_INVOKED_SOURCE")
        ops.require(all(ops.sha256(p) == digest for p, digest in protected.items()), "REFUSAL_REWROTE_CONFIGURATION")
        self.command(["cluster-init", "--config", str(cf.DIR / "init.yml")])
        self.check("reject_"+feature+"_"+role, nonzero_exit=True, strict_yaml_refusal=True,
                   observed_service_listener=False, broker_connections_added=0, source_requests_added=0,
                   audit_journal_created=False, original_configuration_unchanged=True,
                   original_durable_metadata_revalidated=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("matrix", "sandbox", "sandbox-sha256", "nats-archive", "fixture-root", "output"):
        parser.add_argument("--"+name, required=True)
    parser.add_argument("--stage", choices=("old", "candidate"), required=True)
    args = parser.parse_args()
    os.umask(0o077)
    isolation = isolation_evidence()
    root, output = Path(args.fixture_root), Path(args.output)
    ops.require(not root.exists() and not output.exists(), "FRESH_CAMPAIGN_REQUIRED")
    root.mkdir(mode=0o700)
    matrix = json.loads(Path(args.matrix).read_text())
    reports = []
    builds = list(OLD_BUILDS) if args.stage == "old" else ["candidate"]
    original_dir = cf.DIR
    for label in builds:
        build = matrix[label]
        if label in OLD_BUILDS:
            ops.require((build["revision"], build["sha256"]) == OLD_BUILDS[label], "UNDECLARED_OLD_BINARY")
        for feature in ((None,) if label != "candidate" else (None, *FEATURES)):
            case_args = argparse.Namespace(binary=build["path"], binary_sha256=build["sha256"],
                  sandbox=args.sandbox, sandbox_sha256=args.sandbox_sha256, nats_archive=args.nats_archive)
            case_name = label + "_" + (feature or "baseline")
            cf.DIR = root / case_name
            case = RefusalCase(case_args, feature)
            expected = ["initialized_exact_baseline_policy", "supported_configuration_executes_exact_query"]
            if label != "candidate":
                expected += ["reject_"+f+"_"+r for f in FEATURES for r in ROLES]
            expected += ["owned_service_and_broker_processes_stopped"]
            failure, cleanup_errors = None, []
            started = time.monotonic()
            try:
                case.provision()
                case.positive()
                if label != "candidate":
                    for refused_feature in FEATURES:
                        for role in ROLES:
                            case.refusal(refused_feature, role)
            except BaseException as error:
                failure = str(error) if isinstance(error, (ops.AcceptanceError, audit.Failure)) else type(error).__name__
                cf.write(root / (case_name + "-private-failure.log"), traceback.format_exc())
            finally:
                try:
                    cleanup_errors = case.cleanup()
                    ops.require(all(ops.sha256(path) == case.input_hashes[name] for name, path in case.inputs.items()),
                                "INPUT_HASH_CHANGED")
                except Exception as error:
                    cleanup_errors.append(type(error).__name__)
            for name in expected:
                if not any(item["test"] == name for item in case.checks):
                    case.checks.append(dict(test=name, passed=False, category="DEPENDENCY_FAILED"))
            reports.append(dict(build=label, revision=build["revision"], feature=feature or "baseline",
                           passed=failure is None and not cleanup_errors and all(c["passed"] for c in case.checks),
                           failure=failure, cleanup_errors=cleanup_errors, checks=case.checks,
                           input_sha256_before_and_after=case.input_hashes,
                           elapsed_seconds=round(time.monotonic()-started, 3)))
    cf.DIR = original_dir
    report = dict(schema_version=1, stage=args.stage, checked_at=datetime.now(timezone.utc).isoformat(),
                  passed=all(r["passed"] for r in reports), cases=reports,
                  enforced_isolation=isolation,
                  unrun_stages=["candidate"] if args.stage == "old" else ["old"],
                  scope="Fresh single-host old-binary positive controls and one-feature strict YAML refusal; protocol fixture, not a live database or throughput test")
    with output.open("x") as stream:
        stream.write(json.dumps(report, indent=2)+"\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
