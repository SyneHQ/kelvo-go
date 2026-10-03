#!/usr/bin/env python3
"""Private two-gateway API-key rotation acceptance on the idle Linux fixture.

Requires prebuilt Kelvo/Landlock binaries and the existing three-broker fixture.
Never installs dependencies, changes original configuration, or stops brokers.
"""
import argparse
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import secrets
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import operational_acceptance as ops

REQUIRED = ("legacy_and_file_startup", "overlap_on_both_gateways", "per_replica_revocation",
            "fail_closed_and_recovery", "restart_revision_floor", "parked_cancellation",
            "active_cancellation", "cleanup")


def key_document(revision, keys):
    return ops.yaml_document({"version": 1, "revision": revision, "tenants": keys}).encode()


def atomic_private(path, raw):
    """Replace only this run's owned file with an fsynced private complete file."""
    temporary = path.with_name(path.name+".next")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def reconcile(report):
    checks = report.get("checks", [])
    return (report.get("interrupted") is False and len(checks) == len(REQUIRED)
            and {item.get("test") for item in checks} == set(REQUIRED)
            and all(item.get("passed") is True for item in checks)
            and next(item for item in checks if item["test"] == "cleanup").get("original_configurations_verified") is True)


class RotationAcceptance(ops.Acceptance):
    def __init__(self, args):
        super().__init__(args)
        self.current = {}
        self.key_paths = {gateway: self.directory/f"gateway-{gateway}-keys.yml" for gateway in (1, 2)}
        self.config_paths = {gateway: self.directory/f"rotation-gateway-{gateway}.yml" for gateway in (1, 2)}
        self.config_hashes = {}
        self.revision = 1

    def call(self, path, tenant="a", body=None, node=None, timeout=12, with_headers=False,
             gateway=1, token=None, extra_headers=None):
        if node:
            return super().call(path, tenant, body, node, timeout, with_headers)
        value = token if token is not None else self.current.get(tenant, self.env["KELVO_TOKEN_"+tenant.upper()])
        headers = {"Authorization": "Bearer "+value}
        headers.update(extra_headers or {})
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

    def accepted(self, gateway, token, wanted=404):
        return self.call("/v1/queries/rotation-probe", gateway=gateway, token=token)[0] == wanted

    def wait_key(self, gateway, token, wanted=404):
        self.wait(lambda: self.accepted(gateway, token, wanted), 5)

    def publish(self, gateways, revision, keys):
        raw = key_document(revision, keys)
        for gateway in gateways:
            atomic_private(self.key_paths[gateway], raw)

    def stop_gateway(self, gateway):
        name = "gateway" if gateway == 1 else "gateway2"
        process = self.processes[name]
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            ops.require(process.wait(timeout=8) == 0, "GATEWAY_DRAIN_FAILED")

    def start_gateway(self, gateway):
        name = "gateway" if gateway == 1 else "gateway2"
        return self.start_process(name, [self.binary, "gateway", "--config", self.config_paths[gateway], "--drain-timeout", "2s"])

    def query_on(self, gateway, tenant="a", token=None):
        code, raw = self.call("/v1/queries", tenant=tenant, gateway=gateway, token=token,
                              body={"mode": "federated", "sources": ["ops_snapshot"], "sql": ops.SQL})
        ops.require(code == 201, "ROTATION_QUERY_SUBMISSION_FAILED")
        query_id = json.loads(raw)["id"]
        code, raw, headers = self.call("/v1/queries/"+query_id+"/results", tenant=tenant, gateway=gateway,
                                       token=token, with_headers=True)
        ops.require(code == 200, "ROTATION_QUERY_RESULT_FAILED")
        ops.verify_completed_arrow(raw, headers, self.expected[tenant])
        return query_id

    def startup(self):
        self.config_hashes = {path.name: ops.sha256(path) for path in self.fixture.glob("*.yml")}
        detail = super().startup()  # real legacy token_env requests still succeed
        self.old = {tenant: self.env["KELVO_TOKEN_"+tenant.upper()] for tenant in ("a", "b")}
        self.new = {tenant: secrets.token_urlsafe(32) for tenant in ("a", "b")}
        self.current = dict(self.old)
        keys = {tenant: [value] for tenant, value in self.old.items()}
        self.publish((1, 2), 1, keys)
        text = (self.directory/"gateway.yml").read_text()
        import re
        text, count = re.subn(r"(?m)^[ \t]+token_env:[^\n]*\n", "", text)
        ops.require(count == 2, "FIXTURE_TOKEN_REFERENCE_SHAPE")
        for gateway in (1, 2):
            config = ops.replace_field(text, "listen", f"127.0.0.1:{14439+gateway}")
            config += ops.yaml_document({"authentication": {"keys_file": str(self.key_paths[gateway]),
                                                           "reload_interval": "1s", "min_revision": 1}})
            ops.cf.write(self.config_paths[gateway], config)
        self.stop_gateway(1)
        for gateway in (1, 2):
            self.start_gateway(gateway)
            self.wait(lambda: self.call("/ready", gateway=gateway)[0] == 200)
            self.wait_key(gateway, self.old["a"])
            self.query_on(gateway)
        return {**detail, "gateways": 2, "legacy_then_live_file_mode": True,
                "unchanged_environment_token_is_not_a_fallback": True}

    def overlap(self):
        keys = {tenant: [self.old[tenant], self.new[tenant]] for tenant in ("a", "b")}
        self.publish((1, 2), 2, keys)
        for gateway in (1, 2):
            for tenant in ("a", "b"):
                self.wait_key(gateway, self.new[tenant])
                self.wait_key(gateway, self.old[tenant])
                self.query_on(gateway, tenant, self.new[tenant])
        self.current = dict(self.new)
        query_id = self.submit("a", "SELECT 7::BIGINT AS value", [])
        try:
            code, _ = self.call("/v1/queries/"+query_id, token=self.new["b"], extra_headers={"X-Kelvo-Tenant": "a"})
            ops.require(code == 404, "ROTATION_CROSS_TENANT_HANDLE_DISCLOSURE")
            code, _ = self.call("/v1/queries/"+query_id, token=self.new["a"], extra_headers={"X-Kelvo-Tenant": "b"})
            ops.require(code == 200, "TENANT_HEADER_OVERRIDES_KEY")
        finally:
            self.call("/v1/queries/"+query_id+"/cancel", body={})
        return {"old_and_new_keys_on_each_gateway": True, "tenant_header_cannot_override_key": True,
                "typed_queries_on_each_tenant_and_gateway": 4}

    def revoke(self):
        self.keys = {tenant: [self.new[tenant]] for tenant in ("a", "b")}
        started = time.monotonic()
        self.publish((1,), 3, self.keys)
        self.wait_key(1, self.old["a"], 401)
        ops.require(self.accepted(2, self.old["a"]), "PROPAGATION_WINDOW_NOT_EXERCISED")
        self.publish((2,), 3, self.keys)
        for gateway in (1, 2):
            for tenant in ("a", "b"):
                self.wait_key(gateway, self.old[tenant], 401)
                self.wait_key(gateway, self.new[tenant])
            self.query_on(gateway)
        self.revision = 3
        return {"old_keys_rejected_by_both_replicas": True,
                "deliberate_propagation_window_observed": True,
                "rollout_seconds": round(time.monotonic()-started, 3)}

    def fail_closed(self):
        path = self.key_paths[1]
        valid = key_document(self.revision, self.keys)
        modes = ("missing", "permissions", "symlink", "malformed", "duplicate", "rollback", "equivocation")
        for mode in modes:
            sibling = path.with_name(path.name+".held")
            if mode == "missing":
                path.unlink()
            elif mode == "permissions":
                path.chmod(0o644)
            elif mode == "symlink":
                atomic_private(sibling, valid)
                path.unlink()
                path.symlink_to(sibling)
            else:
                raw = {"malformed": b"version: [invalid\n",
                       "duplicate": key_document(self.revision+1, {"a": [self.new["a"]], "b": [self.new["a"]]}),
                       "rollback": key_document(2, {tenant: [self.old[tenant], self.new[tenant]] for tenant in ("a", "b")}),
                       "equivocation": key_document(self.revision, {tenant: [self.old[tenant]] for tenant in ("a", "b")})}[mode]
                atomic_private(path, raw)
            try:
                self.wait_key(1, self.new["a"], 401)
                ops.require(self.call("/ready", gateway=1)[0] == 503, "AUTH_FAILURE_DID_NOT_CHANGE_READINESS")
                ops.require(self.call("/health", gateway=1)[0] == 200, "AUTH_FAILURE_BROKE_LIVENESS")
                ops.require(self.accepted(1, self.old["a"], 401), "AUTH_FAILURE_USED_ENVIRONMENT_FALLBACK")
                self.query_on(2)
            finally:
                if path.is_symlink():
                    path.unlink()
                atomic_private(path, valid)
                sibling.unlink(missing_ok=True)
            self.wait_key(1, self.new["a"])
            self.wait(lambda: self.call("/ready", gateway=1)[0] == 200)
        return {"failure_modes": list(modes), "unaffected_replica_remains_usable": True,
                "no_environment_fallback": True, "same_valid_revision_recovers_transient_failure": True}

    def restart_floor(self):
        self.stop_gateway(1)
        text = self.config_paths[1].read_text().replace("min_revision: 1", "min_revision: 4")
        ops.cf.write(self.config_paths[1], text)
        process = self.start_gateway(1)
        ops.require(process.wait(timeout=8) != 0, "RESTART_ACCEPTED_OLD_KEY_REVISION")
        self.publish((1, 2), 4, self.keys)
        self.start_gateway(1)
        self.wait_key(1, self.new["a"])
        self.stop_gateway(2)
        text = self.config_paths[2].read_text().replace("min_revision: 1", "min_revision: 4")
        ops.cf.write(self.config_paths[2], text)
        self.start_gateway(2)
        for gateway in (1, 2):
            self.wait_key(gateway, self.new["a"])
            ops.require(self.accepted(gateway, self.old["a"], 401), "RESTART_RESTORED_REVOKED_KEY")
            self.query_on(gateway)
        self.revision = 4
        return {"below_floor_startup_rejected": True, "both_replicas_restarted_at_new_floor": True}

    def interrupted_result(self, query_id, token):
        deadline = time.monotonic()+6
        while time.monotonic() < deadline:
            try:
                response = self.call("/v1/queries/"+query_id+"/results", token=token, timeout=10, with_headers=True)
            except (OSError, urllib.error.URLError, http.client.HTTPException, ops.AcceptanceError):
                return None
            if response[0] != 429:
                return response
            # Admission pressure before the request parks never consumes its
            # claim. Keep the intended holder alive through a transient 429.
            time.sleep(0.02)
        raise ops.AcceptanceError("ROTATION_RESULT_ADMISSION_DEADLINE")

    def add_temporary_key(self):
        token = secrets.token_urlsafe(32)
        self.revision += 1
        self.publish((1, 2), self.revision, {"a": [self.new["a"], token], "b": self.keys["b"]})
        self.wait_key(1, token)
        return token

    def remove_temporary_key(self, token):
        self.revision += 1
        self.publish((1, 2), self.revision, self.keys)
        for gateway in (1, 2):
            self.wait_key(gateway, token, 401)

    def cancel_handles(self, handles):
        for query_id in handles:
            code, _ = self.retry("/v1/queries/"+query_id+"/cancel", body={})
            ops.require(code == 200, "ROTATION_HANDLE_CLEANUP_FAILED")
        self.wait(lambda: self.resources("a1")["Active"] == 0 and self.resources("a1")["Waiting"] == 0)

    def queued_revocation(self, handles, token):
        snapshot = {}
        def observe():
            snapshot.update({query_id: self.state(query_id) for query_id in handles})
            ops.require(len(snapshot) == 8 and set(snapshot.values()) <= {"queued", "assigned"}, "ROTATION_QUEUE_STATE_CHANGED")
            assigned = [query_id for query_id, state in snapshot.items() if state == "assigned"]
            queued = [query_id for query_id, state in snapshot.items() if state == "queued"]
            ops.require(len(assigned) <= 1, "ROTATION_UNEXPECTED_WORKER_CAPACITY")
            return (assigned[0], queued[:3]) if len(assigned) == 1 and len(queued) >= 3 else None
        assigned, queued = self.wait(observe, 5)
        starts = [threading.Event(), threading.Event()]
        def hold(index):
            starts[index].set()
            return self.interrupted_result(queued[index], token)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            tasks = [pool.submit(hold, index) for index in range(2)]
            try:
                ops.require(all(event.wait(2) for event in starts), "ROTATION_WAITER_DID_NOT_START")
                def parked_full():
                    ops.require(not any(task.done() for task in tasks), "PARKED_REQUEST_EXITED_EARLY")
                    code, raw = self.call("/v1/queries/"+queued[2]+"/results", timeout=0.2)
                    if code != 429:
                        return False
                    error = json.loads(raw).get("error", {})
                    if error.get("message") != "Queued result wait capacity unavailable":
                        return False
                    ops.require(error.get("code") == "RESOURCE_EXHAUSTED", "INVALID_WAITER_REJECTION")
                    ops.require(not any(task.done() for task in tasks), "PARKED_REQUEST_EXITED_EARLY")
                    ops.require(all(self.state(query_id) == "queued" for query_id in queued), "ROTATION_PARKED_QUERY_ASSIGNED")
                    ops.require(self.state(assigned) == "assigned", "ROTATION_BLOCKER_CHANGED")
                    return True
                self.wait(parked_full, 3)
                self.remove_temporary_key(token)
                for task in tasks:
                    response = task.result(timeout=5)
                    ops.require(response is None or not response[1].endswith(ops.EOS), "REVOKED_WAITER_DELIVERED_RESULT")
                ops.require(all(self.state(query_id) == "queued" for query_id in queued[:2]), "REVOKED_WAITER_CONSUMED_HANDLE")
            finally:
                ops.cf.write(self.directory/"rotation-waiters.json", json.dumps({
                    "observed_states": list(snapshot.values()), "started": [event.is_set() for event in starts],
                    "returned": [task.done() for task in tasks]}, indent=2))
        return {"parked_requests_canceled": 2, "waiter_rejection_source": "parked_waiter_pool",
                "unclaimed_handles_remain_tenant_owned": True}

    def parked(self):
        token = self.add_temporary_key()
        handles = []
        try:
            for _ in range(8):
                handles.append(self.submit("a", "SELECT 7::BIGINT AS value", []))
            evidence = self.queued_revocation(handles, token)
        finally:
            self.cancel_handles(handles)
        self.query_on(1)
        return {**evidence, "replacement_key_recovers_capacity": True}

    def active(self):
        token = self.add_temporary_key()
        query_id = self.submit("a", "SELECT sum(i) FROM range(1000000000000) t(i)", [])
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(self.interrupted_result, query_id, token)
                self.wait(lambda: self.state(query_id) == "running", 5)
                self.remove_temporary_key(token)
                response = result.result(timeout=5)
                ops.require(response is None or not response[1].endswith(ops.EOS), "REVOKED_ACTIVE_QUERY_COMPLETED")
            self.wait(lambda: self.state(query_id) in ("failed", "cancelled"), 5)
        finally:
            self.cancel_handles([query_id])
        self.query_on(1)
        self.query_on(2)
        return {"running_request_canceled": True, "no_completed_arrow_result": True,
                "subsequent_typed_queries_correct_on_both_replicas": True}

    def cleanup(self):
        result = super().cleanup()
        unchanged = all((self.fixture/name).is_file() and ops.sha256(self.fixture/name) == digest
                        for name, digest in self.config_hashes.items())
        ops.require(unchanged, "ORIGINAL_FIXTURE_CONFIGURATION_CHANGED")
        return {**result, "original_configurations_verified": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default=str(ops.ROOT/"bin/kelvo"))
    parser.add_argument("--sandbox", default=str(ops.ROOT/"bin/kelvo-landlock"))
    parser.add_argument("--fixture", default=str(ops.ROOT/"artifacts/cluster-private"))
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    args.duration = 30
    output = Path(args.output)
    ops.require(not output.exists() and not output.is_symlink(), "OUTPUT_ALREADY_EXISTS")
    os.umask(0o077)
    acceptance = RotationAcceptance(args)
    report = {"schema_version": 1, "passed": False, "checks": acceptance.checks,
              "checked_at": datetime.now(timezone.utc).isoformat(),
              "scope": "Local two-gateway credential rollout; not global atomic revocation, tenant policy changes or TLS rotation",
              "script_sha256": ops.sha256(Path(__file__)), "shared_runner_sha256": ops.sha256(Path(ops.__file__)),
              "binary_sha256": ops.sha256(acceptance.binary), "sandbox_sha256": ops.sha256(acceptance.sandbox)}
    interrupted = False
    def stop(signum, frame):
        raise InterruptedError()
    signal.signal(signal.SIGTERM, stop)
    try:
        active = True
        for name, callback in zip(REQUIRED[:-1], (acceptance.startup, acceptance.overlap, acceptance.revoke,
                                                  acceptance.fail_closed, acceptance.restart_floor,
                                                  acceptance.parked, acceptance.active)):
            if active:
                active = acceptance.record(name, callback)
            else:
                acceptance.checks.append({"test": name, "passed": False, "category": "DEPENDENCY_FAILED"})
    except (KeyboardInterrupt, InterruptedError):
        interrupted = True
    finally:
        def cleaning(signum, frame):
            nonlocal interrupted
            interrupted = True
        signal.signal(signal.SIGTERM, cleaning)
        signal.signal(signal.SIGINT, cleaning)
        acceptance.record("cleanup", acceptance.cleanup)
    for name in REQUIRED:
        if not any(item["test"] == name for item in acceptance.checks):
            acceptance.checks.append({"test": name, "passed": False, "category": "INTERRUPTED"})
    report["interrupted"] = interrupted
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
