#!/usr/bin/env python3
"""Exact, pinned NATS broker/client compatibility in a private Linux cluster.

Broker mode: 2.14.7 -> 2.15.0 -> 2.14.7, one broker at a time.
Client mode: same-source v1.53.1 -> v1.54.0 -> v1.53.1, one role at a time,
on one explicitly pinned server release. Run each server as a separate case.
No stream moves, replica changes, scaling, source replay or dependency updates.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tarfile
import time

import operational_acceptance as ops
import process_loss_acceptance as loss
import rolling_upgrade_acceptance as roll
from config_refusal_acceptance import isolation_evidence

CLIENTS = ("v1.53.1", "v1.54.0")
SERVERS = ("2.14.7", "2.15.0")


def validate_matrix(matrix):
    ops.require(matrix.get("mode") in ("broker", "client"), "UNDECLARED_COMPATIBILITY_MODE")
    revision = matrix.get("old_revision", "")
    ops.require(re.fullmatch(r"[0-9a-f]{40}", revision) is not None and revision == matrix.get("new_revision"),
                "IDENTICAL_FULL_SOURCE_REVISION_REQUIRED")
    for version in ("old", "new"):
        for field in ("source_archive_sha256", "binary_sha256", "launcher_sha256", "module_manifest_sha256",
                      "effective_modfile_sha256", "effective_sumfile_sha256"):
            ops.require(re.fullmatch(r"[a-f0-9]{64}", matrix[version].get(field, "")) is not None,
                        "BUILD_IDENTITY_HASH_REQUIRED")
    ops.require(matrix["old"]["source_archive_sha256"] == matrix["new"]["source_archive_sha256"]
                and matrix["old"]["launcher_sha256"] == matrix["new"]["launcher_sha256"],
                "IDENTICAL_SOURCE_AND_LAUNCHER_REQUIRED")
    expected = CLIENTS if matrix["mode"] == "client" else (CLIENTS[1], CLIENTS[1])
    ops.require(tuple(matrix[v].get("nats_client") for v in ("old", "new")) == expected,
                "UNDECLARED_CLIENT_PAIR")
    if matrix["mode"] == "broker":
        ops.require(matrix["old"] == matrix["new"], "BROKER_MODE_MUST_HOLD_APPLICATION_CONSTANT")
    ops.require(matrix.get("default_dependencies_unchanged") is True, "DEFAULT_DEPENDENCIES_CHANGED")


def expected_steps(mode):
    prefix = ("startup", "key_rotation_and_floor", "initial_pair")
    if mode == "broker":
        transitions = tuple("broker_upgrade_"+str(i) for i in range(3)) + tuple("broker_rollback_"+str(i) for i in range(3))
    else:
        transitions = tuple("client_upgrade_"+role for role in roll.APP_ROLES) + ("upgraded_pair",)
        transitions += tuple("client_rollback_"+role for role in roll.APP_ROLES) + ("rolled_back_pair",)
    return (*prefix, *transitions, "cleanup")


def reconcile(report):
    mode = report.get("mode")
    if mode not in ("broker", "client") or report.get("interrupted") is not False:
        return False
    checks = report.get("checks", [])
    if (len(checks) != len(expected_steps(mode)) or {c.get("test") for c in checks} != set(expected_steps(mode))
            or not all(c.get("passed") is True for c in checks)):
        return False
    for item in checks:
        name = item["test"]
        if name.startswith(("broker_", "client_")) or name.endswith("_pair"):
            if (not all(type(item.get(k)) is int and item[k] == 2 for k in
                        ("running_before_transition", "queued_before_transition", "exact_running_results", "exact_queued_results"))
                    or not all(item.get(k) is True for k in ("original_handles_preserved", "consumed_results_rejected",
                                "one_observed_source_request_per_marker", "foreign_handles_hidden"))):
                return False
        if name.startswith("broker_"):
            if (item.get("one_broker_at_a_time") is not True or item.get("surviving_broker_identities_preserved") is not True
                    or type(item.get("post_rejoin_new_queries")) is not int or item["post_rejoin_new_queries"] != 2
                    or item.get("revoked_keys_denied") is not True
                    or (item.get("broker_version_before"), item.get("broker_version_after")) !=
                       (SERVERS if name.startswith("broker_upgrade_") else tuple(reversed(SERVERS)))
                    or set(item.get("expected_peer_versions", {})) != set(ops.cf.BROKER_NAMES.values())
                    or any(v not in SERVERS for v in item["expected_peer_versions"].values())):
                return False
        if name.endswith("_pair"):
            destination = "new" if name == "upgraded_pair" else "old"
            expected_client = report.get("matrix", {}).get(destination, {}).get("nats_client")
            if (item.get("roles") != dict.fromkeys(roll.APP_ROLES, destination)
                    or expected_client not in CLIENTS
                    or item.get("client_versions") != dict.fromkeys(roll.APP_ROLES, expected_client)
                    or item.get("expected_peer_versions") != dict.fromkeys(ops.cf.BROKER_NAMES.values(), report.get("start_server_version"))
                    or report.get("start_server_version") not in SERVERS
                    or item.get("revoked_keys_denied") is not True):
                return False
        if name.startswith("client_"):
            if not all(item.get(k) is True for k in ("readiness_rejected_during_drain", "graceful_exit", "revoked_keys_denied")):
                return False
    clean = next(c for c in checks if c["test"] == "cleanup")
    rotation = next(c for c in checks if c["test"] == "key_rotation_and_floor")
    return (rotation.get("both_replica_floors") == 3
            and rotation.get("overlap_and_revocation_on_both_replicas") is True
            and rotation.get("rollback_requires_current_keys_and_policy") is True
            and all(type(clean.get(k)) is int and clean[k] == 0 for k in
                ("forced_application_kills", "observed_live_descendants", "worker_scratch_directories", "fixture_gate_errors"))
            and all(clean.get(k) is True for k in ("original_configurations_verified", "all_owned_brokers_stopped",
                                                    "input_artifact_hashes_preserved")))


class CompatibilityAcceptance(roll.UpgradeAcceptance):
    validate_matrix = staticmethod(validate_matrix)

    def __init__(self, args):
        self.server_version = args.start_server_version
        self.broker_versions = dict.fromkeys(ops.cf.BROKER_NAMES.values(), self.server_version)
        self.archives = {"2.14.7": Path(args.predecessor_archive).resolve(), "2.15.0": Path(args.current_archive).resolve()}
        args.nats_archive = str(self.archives[self.server_version])
        super().__init__(args)
        self.artifacts.update({p: ops.cf.RELEASES[v] for v, p in self.archives.items()})
        self.artifacts.update({Path(args.old_modules).resolve(): self.matrix["old"]["module_manifest_sha256"],
                               Path(args.new_modules).resolve(): self.matrix["new"]["module_manifest_sha256"]})

    def archive_digest(self):
        return ops.cf.RELEASES[self.server_version]

    def provision_fixture(self):
        ops.cf.provision(self.args.nats_archive, server_version=self.server_version)

    def wait_brokers(self):
        previous = ops.cf.DIR
        ops.cf.DIR = self.fixture
        try:
            ops.cf.wait_for_brokers(self.brokers, expected_versions=self.broker_versions)
        finally:
            ops.cf.DIR = previous

    def install_broker(self, version):
        archive = self.archives[version]
        ops.require(ops.sha256(archive) == ops.cf.RELEASES[version], "BROKER_ARCHIVE_CHANGED")
        target = self.fixture / ("nats-server-stage-"+version)
        ops.require(not target.exists() and not target.is_symlink(), "BROKER_STAGE_EXISTS")
        with tarfile.open(archive) as bundle:
            member = bundle.getmember(f"nats-server-v{version}-linux-amd64/nats-server")
            ops.require(member.isfile() and 0 < member.size <= 64 << 20, "BROKER_MEMBER_INVALID")
            with target.open("xb") as output:
                source = bundle.extractfile(member)
                while block := source.read(1 << 20):
                    output.write(block)
                output.flush()
                os.fsync(output.fileno())
        target.chmod(0o700)
        # Running peers retain the old executable inode; only the stopped peer
        # executes this new path. Exact /varz versions are checked after restart.
        os.replace(target, self.fixture / "nats-server")

    def pair(self):
        self.wait_brokers()
        detail = self.finish_load(self.prepare_load())
        self.security()
        return {**detail, "expected_peer_versions": dict(self.broker_versions), "roles": dict(self.roles),
                "client_versions": {r: self.matrix[v]["nats_client"] for r, v in self.roles.items()},
                "revoked_keys_denied": True}

    def broker_wave(self, index, destination):
        handles = self.prepare_load()
        name = "nats-"+str(index)
        peer = ops.cf.BROKER_NAMES[name]
        before_version = self.broker_versions[peer]
        ops.require(destination in SERVERS and before_version != destination, "BROKER_VERSION_MUST_CHANGE")
        broker = next(b for b in self.brokers if b["name"] == name)
        survivors = {b["name"]: ops.proc_identity(b["pid"]) for b in self.brokers if b["name"] != name}
        ops.require(all(value is not None for value in survivors.values()), "SURVIVING_BROKER_ABSENT")
        identity = ops.proc_identity(broker["pid"])
        ops.require(identity is not None and self.broker_alive(broker), "BROKER_IDENTITY_CHANGED")
        ops.signal_owned(identity, signal.SIGTERM)
        self.wait(lambda: ops.proc_identity(identity[0]) != identity and self.broker_ports_closed(broker), 5)
        ops.require(all(not item["future"].done() for item in handles.values()), "LOAD_ENDED_BEFORE_BROKER_REPLACEMENT")
        self.install_broker(destination)
        self.broker_versions[peer] = destination
        self.restart_broker(broker)
        ops.require(all(ops.proc_identity(pid) == (pid, start) for pid, start in survivors.values()),
                    "SURVIVING_BROKER_IDENTITY_CHANGED")
        detail = self.finish_load(handles)
        # Monitor readiness alone is insufficient. These new submissions write
        # durable jobs, dispatch, claim, consume and commit after every rejoin.
        for tenant in ("a", "b"):
            self.query(tenant)
        self.security()
        return {**detail, "broker_version_before": before_version, "broker_version_after": destination,
                "expected_peer_versions": dict(self.broker_versions), "one_broker_at_a_time": True,
                "surviving_broker_identities_preserved": True, "post_rejoin_new_queries": 2,
                "revoked_keys_denied": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "new-binary", "sandbox", "matrix", "old-archive", "new-archive", "predecessor-archive",
                 "current-archive", "old-modules", "new-modules", "fixture", "output"):
        parser.add_argument("--"+name, required=True)
    parser.add_argument("--mode", choices=("broker", "client"), required=True)
    parser.add_argument("--start-server-version", choices=SERVERS, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    isolation = isolation_evidence()
    output = Path(args.output)
    ops.require(not output.exists() and not output.is_symlink(), "OUTPUT_ALREADY_EXISTS")
    args.managed_scratch = True
    acceptance = CompatibilityAcceptance(args)
    ops.require(acceptance.matrix["mode"] == args.mode, "MATRIX_MODE_MISMATCH")
    ops.require(args.mode != "broker" or args.start_server_version == SERVERS[0], "BROKER_START_VERSION_MISMATCH")
    actions = [("startup", acceptance.startup), ("key_rotation_and_floor", acceptance.key_rotation),
               ("initial_pair", acceptance.pair)]
    if args.mode == "broker":
        for verb, version in (("upgrade", SERVERS[1]), ("rollback", SERVERS[0])):
            actions.extend(("broker_"+verb+"_"+str(i), lambda i=i, v=version: acceptance.broker_wave(i, v)) for i in range(3))
    else:
        for verb, version, final in (("upgrade", "new", "upgraded_pair"), ("rollback", "old", "rolled_back_pair")):
            actions.extend(("client_"+verb+"_"+role, lambda r=role, v=version: acceptance.app_wave(r, v)) for role in roll.APP_ROLES)
            actions.append((final, acceptance.pair))
    report = dict(schema_version=1, mode=args.mode, start_server_version=args.start_server_version, passed=False,
                  checked_at=datetime.now(timezone.utc).isoformat(), checks=acceptance.checks,
                  matrix=acceptance.matrix, enforced_isolation=isolation,
                  script_sha256=ops.sha256(Path(__file__)), fixture_script_sha256=ops.sha256(Path(ops.cf.__file__)),
                  rolling_script_sha256=ops.sha256(Path(roll.__file__)), operational_script_sha256=ops.sha256(Path(ops.__file__)),
                  process_loss_script_sha256=ops.sha256(Path(loss.__file__)),
                  scope="Exact same-source full engine binaries and pinned NATS releases, single-host private network, gated Arrow protocol fixture and synthetic CTEs; not a WAN, capacity, arbitrary-version or exactly-once execution guarantee")
    interrupted = False
    def interrupt(signum, frame):
        raise InterruptedError()
    signal.signal(signal.SIGTERM, interrupt)
    try:
        active = True
        for name, callback in actions:
            if active:
                active = acceptance.record(name, callback)
            else:
                acceptance.checks.append(dict(test=name, passed=False, category="DEPENDENCY_FAILED"))
    except (KeyboardInterrupt, InterruptedError):
        interrupted = True
    finally:
        def cleaning_signal(signum, frame):
            nonlocal interrupted
            interrupted = True
        signal.signal(signal.SIGTERM, cleaning_signal)
        signal.signal(signal.SIGINT, cleaning_signal)
        acceptance.record("cleanup", acceptance.cleanup)
    for name in expected_steps(args.mode):
        if not any(c["test"] == name for c in acceptance.checks):
            acceptance.checks.append(dict(test=name, passed=False, category="INTERRUPTED"))
    report.update(interrupted=interrupted, elapsed_seconds=round(time.monotonic()-acceptance.started, 3),
                  resource_observations=acceptance.samples.evidence())
    report["passed"] = reconcile(report)
    with output.open("x") as stream:
        stream.write(json.dumps(report, indent=2)+"\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
