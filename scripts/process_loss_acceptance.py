#!/usr/bin/env python3
"""Bounded, isolated Linux process-loss acceptance with failure-inclusive evidence.

Requires an exclusively owned provisioned cluster_fixture directory. Installs
nothing. Only exact fixture brokers and application children may be signalled.
"""
import argparse
import concurrent.futures
from datetime import datetime, timezone
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

import operational_acceptance as ops
import lease_supervision

REQUIRED = ("startup", "gateway_loss_under_admitted_load", "worker_loss_under_admitted_load",
            "broker_loss_under_admitted_load", "cleanup")
TERMINAL_FAILURE = {"failed", "cancelled"}
SLOW_SQL = "SELECT sum(i + metric) AS total FROM ops_snapshot, range(1000000000000) t(i)"


def rejected_result(response):
    """A transport error or non-success is failure; successful Arrow is never one."""
    return response is None or response[0] != 200 or not response[1].endswith(ops.EOS)


def reconcile(report):
    checks = report.get("checks", [])
    if (report.get("interrupted") is not False or len(checks) != len(REQUIRED)
            or {item.get("test") for item in checks} != set(REQUIRED)
            or not all(item.get("passed") is True for item in checks)):
        return False
    for check in checks:
        if check["test"] in REQUIRED[1:-1]:
            if (check.get("running_before_fault") != 2 or check.get("queued_before_fault") != 2
                    or check.get("surviving_tenant_exact_result") is not True
                    or check.get("failed_attempt_result_rejected") is not True
                    or check.get("queued_handles_preserved") is not True):
                return False
        if check['test'] == REQUIRED[3] and not lease_supervision.valid_evidence(check.get('lease_supervision')):
            return False
    if report.get("managed_scratch_requested", False) is True:
        worker = next(item for item in checks if item["test"] == "worker_loss_under_admitted_load")
        startup = next(item for item in checks if item["test"] == "startup")
        if (startup.get("managed_scratch") is not True
                or type(worker.get("inherited_sandbox_lease_children")) is not int
                or worker["inherited_sandbox_lease_children"] < 1):
            return False
    cleanup = next(item for item in checks if item["test"] == "cleanup")
    return (cleanup.get("forced_application_kills") == 0
            and cleanup.get("observed_live_descendants") == 0
            and cleanup.get("worker_scratch_directories") == 0
            and cleanup.get("original_configurations_verified") is True
            and cleanup.get("all_owned_brokers_healthy") is True)


class LossAcceptance(ops.Acceptance):
    def __init__(self, args):
        super().__init__(args)
        self.default_gateway = 1
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        self.pending = []
        self.brokers = []
        self.config_hashes = {}
        self.crash_scratch = []
        self.intentional_kills = 0
        self.cleanup_observations = {}
        self.managed_roots = {}
        self.control_response_counts = {}
        self.broker_supervision = None

    def call(self, path, tenant="a", body=None, node=None, timeout=12, with_headers=False, gateway=None):
        if node:
            return super().call(path, tenant, body, node, timeout, with_headers)
        gateway = self.default_gateway if gateway is None else gateway
        data = None if body is None else json.dumps(body).encode()
        headers = {"Authorization": "Bearer " + self.env["KELVO_TOKEN_"+tenant.upper()]}
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
        if name in ("a1", "b1") and getattr(self.args, "managed_scratch", False):
            runtime = self.directory/(name+"-runtime")
            runtime.mkdir(mode=0o700, exist_ok=True)
            root = runtime/"managed"
            root.mkdir(mode=0o700, exist_ok=True)
            config = self.directory/(name+".yml")
            text = config.read_text()
            if "\nscratch_directory:" not in "\n"+text:
                ops.cf.write(config, text+ops.yaml_document({"scratch_directory": str(root)}))
            self.managed_roots[name] = root
        return super().start_process(name, command)

    def scratch_leftovers(self, name=None):
        names = (name,) if name else ("a1", "b1")
        return [path for worker in names for pattern in ("kelvo-worker-*", "managed/kelvo-worker-*")
                for path in (self.directory/(worker+"-runtime")).glob(pattern)]

    def inherited_lease_children(self, name):
        root = self.managed_roots.get(name)
        if root is None:
            return None
        observed = 0
        for pid, identity in list(self.samples.owned.items()):
            if ops.proc_identity(pid) != identity:
                continue
            try:
                base = Path(f"/proc/{pid}")
                if not (base/"cwd").resolve().is_relative_to(root):
                    continue
                for descriptor in (base/"fd").iterdir():
                    target = descriptor.resolve()
                    if target.parent == root and target.name.startswith(".kelvo-lease-"):
                        current, held = target.stat(), descriptor.stat()
                        if (current.st_ino, current.st_dev) == (held.st_ino, held.st_dev):
                            observed += 1
                            break
            except (OSError, RuntimeError):
                continue
        return observed

    def startup(self):
        self.config_hashes = {path.name: ops.sha256(path) for pattern in ("*.yml", "*.conf")
                              for path in self.fixture.glob(pattern)}
        self.brokers = json.loads((self.fixture/"pids.json").read_text())
        detail = super().startup()
        # Long result deliveries and status/cancel observation need independent
        # HTTP permits; this campaign does not measure saturation capacity.
        self.processes["gateway"].send_signal(signal.SIGTERM)
        ops.require(self.processes["gateway"].wait(timeout=8) == 0, "INITIAL_GATEWAY_DRAIN_FAILED")
        text = ops.replace_field((self.directory/"gateway.yml").read_text(), "max_http_requests", 8)
        for gateway in (1, 2):
            path = self.directory/f"loss-gateway-{gateway}.yml"
            ops.cf.write(path, ops.replace_field(text, "listen", f"127.0.0.1:{14439+gateway}"))
            self.start_gateway(gateway)
        return {**detail, "gateways": 2, "max_http_requests_per_gateway": 8,
                "fault_signals": "SIGKILL of exact owned identities", "original_configuration_count": len(self.config_hashes),
                "managed_scratch": bool(self.managed_roots)}

    def start_gateway(self, gateway):
        name = "gateway" if gateway == 1 else "gateway2"
        self.start_process(name, [self.binary, "gateway", "--config", self.directory/f"loss-gateway-{gateway}.yml",
                                  "--drain-timeout", "2s"])
        self.wait(lambda: self.call("/ready", gateway=gateway, timeout=1)[0] == 200)

    def hold(self, tenant, query_id, gateway):
        try:
            return self.call("/v1/queries/"+query_id+"/results", tenant, gateway=gateway, timeout=12)
        except (OSError, urllib.error.URLError, http.client.HTTPException):
            return None

    def prepare_load(self):
        """Observe actual execution plus durable queued states, not submit order."""
        handles = {}
        for tenant, gateway in (("a", 1), ("b", 2)):
            query_id = self.submit(tenant, SLOW_SQL)
            future = self.pool.submit(self.hold, tenant, query_id, gateway)
            self.pending.append(future)
            handles[tenant] = {"running": query_id, "future": future}
        self.wait(lambda: all(self.state(item["running"], tenant) == "running"
                              for tenant, item in handles.items()), 4)
        for tenant, item in handles.items():
            item["queued"] = self.submit(tenant)
        ops.require(all(self.state(item["running"], tenant) == "running"
                        and self.state(item["queued"], tenant) == "queued"
                        and not item["future"].done() for tenant, item in handles.items()), "PREFAULT_MIXED_STATES_NOT_OBSERVED")
        self.samples.sample()
        return handles

    def kill_application(self, name):
        proc = self.processes[name]
        identity = ops.proc_identity(proc.pid)
        ops.require(proc.poll() is None and identity is not None, "FAULT_TARGET_NOT_RUNNING")
        ops.signal_owned(identity, signal.SIGKILL)
        ops.require(proc.wait(timeout=3) == -signal.SIGKILL, "FAULT_SIGNAL_NOT_CONFIRMED")
        self.intentional_kills += 1

    def count_control(self, key):
        self.control_response_counts[key] = self.control_response_counts.get(key, 0)+1

    def cancel_running(self, query_id, tenant):
        # Cancellation targets an existing handle and is idempotent. A broker
        # election can temporarily fail its read/CAS even while /ready is200.
        # Never apply this retry policy to SQL submission or result claiming.
        deadline = time.monotonic()+10
        while True:
            try:
                code, raw = self.call("/v1/queries/"+query_id+"/cancel", tenant, body={}, timeout=2)
            except (OSError, urllib.error.URLError, http.client.HTTPException):
                self.count_control("cancel_transport_error")
            else:
                self.count_control("cancel_http_"+str(code))
                if code == 200:
                    return
                if code not in (409, 429, 503):
                    self.debug_response("fault_cancel", code, raw)
                    raise ops.AcceptanceError("SURVIVING_CANCELLATION_FAILED")
            ops.require(time.monotonic() < deadline, "SURVIVING_CANCELLATION_DEADLINE")
            time.sleep(0.05)

    def failed_attempt(self, item, tenant, cancel=False):
        if cancel:
            self.cancel_running(item["running"], tenant)
        def terminal():
            code, raw = self.call("/v1/queries/"+item["running"], tenant, timeout=2)
            self.count_control("status_http_"+str(code))
            if code in (429, 503):
                return False
            ops.require(code == 200, "FAILED_ATTEMPT_STATUS_LOST")
            return json.loads(raw)["state"] in TERMINAL_FAILURE
        self.wait(terminal, 12)
        ops.require(rejected_result(item["future"].result(timeout=3)), "FAILED_QUERY_RETURNED_COMPLETE_RESULT")
        code, _ = self.retry("/v1/queries/"+item["running"]+"/results", tenant)
        ops.require(code == 409, "FAILED_ATTEMPT_WAS_REPLAYABLE")

    def queued_result(self, item, tenant):
        code, raw, headers = self.retry("/v1/queries/"+item["queued"]+"/results", tenant, with_headers=True)
        ops.require(code == 200, "ADMITTED_QUEUED_QUERY_LOST")
        ops.verify_completed_arrow(raw, headers, self.expected[tenant])
        self.wait(lambda: self.resources(tenant+"1")["Active"] == self.resources(tenant+"1")["BackgroundActive"])

    def common_detail(self, handles):
        # Verify the original failed handles again after queued delivery. No new
        # submissions are made first, so bounded terminal-slot reuse cannot hide
        # a state regression behind an unrelated replacement handle.
        for tenant, item in handles.items():
            ops.require(self.state(item["running"], tenant) in TERMINAL_FAILURE, "TERMINAL_ATTEMPT_REGRESSED")
            ops.require(self.retry("/v1/queries/"+item["running"]+"/results", tenant)[0] == 409,
                        "TERMINAL_ATTEMPT_REPLAYED_AFTER_RECOVERY")
        return {"running_before_fault": 2, "queued_before_fault": 2, "surviving_tenant_exact_result": True,
                "failed_attempt_result_rejected": True, "queued_handles_preserved": True,
                "replay_scope": "Original failed handles remain terminal and reject result requests before and after recovery; no exactly-once source execution claim"}

    def gateway_loss(self):
        handles = self.prepare_load()
        self.kill_application("gateway")
        self.default_gateway = 2
        self.failed_attempt(handles["a"], "a")
        self.failed_attempt(handles["b"], "b", cancel=True)
        for tenant, item in handles.items():
            self.queued_result(item, tenant)
        self.start_gateway(1)
        detail = self.common_detail(handles)
        self.default_gateway = 1
        return {**detail, "replacement_gateway_ready": True, "surviving_gateway": 2}

    def restart_worker(self):
        command = [self.binary, "node", "--config", self.directory/"a1.yml", "--drain-timeout", "8s"]
        self.start_process("a1", command)
        def ready():
            proc = self.processes["a1"]
            if proc.poll() is not None:
                tail = (self.directory/"a1.log").read_bytes()[-4096:]
                ops.require(b"worker identity is already active" in tail, "REPLACEMENT_WORKER_START_FAILED")
                self.start_process("a1", command)
                return False
            return self.call("/ready", node="a1", timeout=1)[0] == 200
        self.wait(ready, 20)

    def worker_loss(self):
        handles = self.prepare_load()
        self.samples.sample()
        before = set(self.samples.owned.values())
        inherited = self.inherited_lease_children("a1")
        if self.managed_roots:
            ops.require(inherited >= 1, "SANDBOX_WORKER_DID_NOT_INHERIT_SCRATCH_LEASE")
        self.kill_application("a1")
        self.failed_attempt(handles["a"], "a")
        ops.require(self.state(handles["a"]["queued"], "a") == "queued", "UNASSIGNED_QUERY_LOST_DURING_WORKER_OUTAGE")
        self.failed_attempt(handles["b"], "b", cancel=True)
        self.queued_result(handles["b"], "b")
        # Record leftovers before replacement. Never delete them to make the
        # acceptance pass; an unclean node death cannot run Go deferred cleanup.
        self.crash_scratch = self.scratch_leftovers("a1")
        self.restart_worker()
        self.queued_result(handles["a"], "a")
        detail = self.common_detail(handles)
        # Surviving parents can remain; observe only old a1 descendants, which
        # are identified by this run's private working directory.
        live_crash_children = 0
        for identity in before:
            if ops.proc_identity(identity[0]) != identity:
                continue
            try:
                cwd = Path(f"/proc/{identity[0]}/cwd").resolve()
                if cwd.is_relative_to(self.directory/"a1-runtime"):
                    live_crash_children += 1
            except OSError:
                pass
        ops.require(live_crash_children == 0, "WORKER_CRASH_LEFT_RUNNING_CHILD")
        return {**detail, "same_worker_identity_recovered": True, "running_crash_children": live_crash_children,
                "scratch_directories_after_crash": len(self.crash_scratch),
                "inherited_sandbox_lease_children": inherited,
                "scratch_reclamation_checked_at_cleanup": True}

    def broker_alive(self, broker):
        previous = ops.cf.DIR
        ops.cf.DIR = self.fixture
        try:
            return ops.cf.broker_alive(broker)
        finally:
            ops.cf.DIR = previous

    def broker_ports_closed(self, broker):
        config = json.loads((self.fixture/(broker["name"]+".conf")).read_text())
        for endpoint in (config["listen"], config["http"], config["cluster"]["listen"]):
            host, port = endpoint.rsplit(":", 1)
            ops.require(host == "127.0.0.1", "BROKER_NOT_LOOPBACK")
            with socket.socket() as sock:
                sock.settimeout(0.1)
                if sock.connect_ex((host, int(port))) == 0:
                    return False
        return True

    def restart_broker(self, broker):
        ops.require(not self.broker_alive(broker) and self.broker_ports_closed(broker), "BROKER_STILL_RUNNING")
        with (self.fixture/(broker["name"]+".log")).open("ab") as log:
            proc = subprocess.Popen([str(self.fixture/"nats-server"), "-c", str(self.fixture/(broker["name"]+".conf"))],
                                    stdout=log, stderr=log, start_new_session=True)
        broker["pid"] = proc.pid
        # Manifest transfer returns this exact child to the fixture lifecycle.
        # If the write fails keep it in application cleanup ownership.
        try:
            ops.cf.write(self.fixture/"pids.json", json.dumps(self.brokers))
        except BaseException:
            self.processes["replacement-broker"] = proc
            raise
        self.wait_brokers()

    def wait_brokers(self):
        previous = ops.cf.DIR
        ops.cf.DIR = self.fixture
        try:
            ops.cf.wait_for_brokers(self.brokers)
        finally:
            ops.cf.DIR = previous

    def broker_loss(self):
        handles = self.prepare_load()
        supervisor = lease_supervision.LeaseSupervisor(self)
        broker = next(item for item in self.brokers if item["name"] == "nats-2")
        ops.require(self.broker_alive(broker), "BROKER_IDENTITY_CHANGED")
        identity = ops.proc_identity(broker["pid"])
        ops.require(identity is not None and self.broker_alive(broker), "BROKER_IDENTITY_CHANGED")
        faulted_at = time.monotonic()
        ops.signal_owned(identity, signal.SIGKILL)
        self.intentional_kills += 1
        try:
            supervisor.start(faulted_at)
            self.wait(lambda: ops.proc_identity(identity[0]) != identity and self.broker_ports_closed(broker), 3)
            self.wait(lambda: self.call("/ready", timeout=1)[0] == 200, 10)
            for tenant, item in handles.items():
                self.failed_attempt(item, tenant, cancel=True)
                self.queued_result(item, tenant)
            detail = self.common_detail(handles)
        finally:
            try:
                supervisor.stop()
            finally:
                try:
                    supervision = supervisor.evidence()
                    self.broker_supervision = supervision
                finally:
                    self.restart_broker(broker)
        ops.require(lease_supervision.valid_evidence(supervision), 'BROKER_NODE_SUPERVISION_FAILED')
        for tenant in ("a", "b"):
            self.query(tenant)
        return {**detail, "unavailable_brokers_during_delivery": 1, "current_replicas_after_rejoin": 3,
                "post_rejoin_queries_correct": True, "lease_supervision": supervision}

    def cleanup(self):
        # Future requests have bounded socket and server deadlines. Let base
        # cleanup stop owned applications even after the campaign fails.
        failure = None
        try:
            detail = super().cleanup()
        except Exception as error:
            failure = error
            detail = {"forced_application_kills": None, "observed_live_descendants": None,
                      "worker_scratch_directories": len(self.scratch_leftovers())}
        self.pool.shutdown(wait=True, cancel_futures=True)
        unchanged = all((self.fixture/name).is_file() and ops.sha256(self.fixture/name) == digest
                        for name, digest in self.config_hashes.items())
        healthy = False
        try:
            if self.brokers:
                self.wait_brokers()
                healthy = True
        except Exception as error:
            if failure is None:
                failure = error
        # Preserve safe measured counters even when cleanup's assertion fails.
        self.cleanup_observations = {
            "scratch_directories_after_crash": len(self.crash_scratch),
            "worker_scratch_directories": len(self.scratch_leftovers()),
            "original_configurations_verified": unchanged, "all_owned_brokers_healthy": healthy}
        if failure:
            raise failure
        ops.require(not self.scratch_leftovers(), "MANAGED_SCRATCH_REMAINS")
        ops.require(unchanged and healthy, "FIXTURE_NOT_RESTORED")
        detail.pop("broker_processes_untouched", None)
        return {**detail, "original_configurations_verified": unchanged, "all_owned_brokers_healthy": healthy,
                "intentional_fault_kills": self.intentional_kills}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default=str(ops.ROOT/"bin/kelvo"))
    parser.add_argument("--sandbox", default=str(ops.ROOT/"bin/kelvo-landlock"))
    parser.add_argument("--fixture", required=True, help="Exclusively owned provisioned fixture directory")
    parser.add_argument("--output", required=True)
    parser.add_argument("--managed-scratch", action="store_true", help="Exercise opt-in owned worker scratch roots")
    args = parser.parse_args()
    output = Path(args.output)
    ops.require(not output.exists() and not output.is_symlink(), "OUTPUT_ALREADY_EXISTS")
    os.umask(0o077)
    acceptance = LossAcceptance(args)
    report = {"schema_version": 1, "passed": False, "checks": acceptance.checks,
              "managed_scratch_requested": args.managed_scratch,
              "checked_at": datetime.now(timezone.utc).isoformat(),
              "scope": "Bounded single-host process termination under admitted mixed jobs; not multi-host HA, snapshot publication loss, provider faults or sustained capacity",
              "script_sha256": ops.sha256(Path(__file__)), "operational_script_sha256": ops.sha256(Path(ops.__file__)),
              "fixture_script_sha256": ops.sha256(Path(ops.cf.__file__))}
    for key, path in (("binary_sha256", acceptance.binary), ("sandbox_sha256", acceptance.sandbox)):
        if path.is_file():
            report[key] = ops.sha256(path)
    interrupted = False
    def interrupt(signum, frame):
        raise InterruptedError()
    signal.signal(signal.SIGTERM, interrupt)
    try:
        active = acceptance.record("startup", acceptance.startup)
        for name, callback in ((REQUIRED[1], acceptance.gateway_loss), (REQUIRED[2], acceptance.worker_loss),
                               (REQUIRED[3], acceptance.broker_loss)):
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
    report["passed"] = reconcile(report)
    report["crash_scratch_directories_observed"] = len(acceptance.crash_scratch)
    report["worker_scratch_directories_after_cleanup"] = len(acceptance.scratch_leftovers())
    report["cleanup_observations"] = acceptance.cleanup_observations
    report["control_response_counts"] = acceptance.control_response_counts
    report["broker_supervision"] = acceptance.broker_supervision
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as target:
        json.dump(report, target, indent=2)
        target.write("\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
