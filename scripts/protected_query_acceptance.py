#!/usr/bin/env python3
"""Owned, offline broker support for the contained protected-query gate."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess

from export_broker_acceptance import ExportBrokerFixture

ROOT = "TestContainedProtectedObjectQueries"
LEAVES = [ROOT + "/" + name for name in (
    "publish-real-single-and-two-part", "tenant-principal-cte-join-aggregate",
    "hidden-schema-and-foreign-handles", "forged-stale-and-unsupported-before-io",
    "revocation-none-chunked", "revocation-none-length",
    "revocation-lz4-chunked", "revocation-lz4-length",
    "cancelled-range-releases-custody", "stalled-registry-retains-custody")]
VERSION = "2.15.0"
MAX_BINARY = 64 << 20
SOURCE_REQUIRED = {"scripts/protected_query_acceptance.py", "scripts/export_broker_acceptance.py",
                   "scripts/nats_export_permissions.py", "internal/cluster/protected_object_query_linux_test.go",
                   "internal/cluster/protected_object_query_fixture_linux_test.go",
                   "internal/testutil/protectedobject/fixture.go", "internal/testutil/protectedobject/http.go"}
ENVIRONMENT = {"KELVO_TEST_QUERY_NATS_URL", "KELVO_TEST_QUERY_NATS_CA_FILE"} | {
    "KELVO_TEST_QUERY_NATS_" + tenant + "_" + role + "_" + field
    for tenant in ("A", "B") for role in ("INITIALIZER", "GATEWAY", "WORKER") for field in ("USER", "PASSWORD")}


def seed_binary(source, artifact, expected):
    """Copy a pinned regular file; never run the unverified input or adopt a path."""
    if not source.is_absolute() or not isinstance(expected, str) or not re.fullmatch(r"[a-f0-9]{64}", expected):
        raise RuntimeError("absolute cached broker and SHA-256 required")
    target = artifact / "nats-server"
    descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    created = False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= MAX_BINARY:
            raise RuntimeError("cached broker must be a bounded regular file")
        checksum, size = hashlib.sha256(), 0
        with target.open("xb") as output, os.fdopen(descriptor, "rb", closefd=False) as incoming:
            created = True
            while block := incoming.read(1 << 20):
                size += len(block)
                if size > MAX_BINARY:
                    raise RuntimeError("cached broker exceeds byte limit")
                checksum.update(block)
                output.write(block)
        if size != info.st_size or checksum.hexdigest() != expected:
            raise RuntimeError("cached broker checksum mismatch")
        target.chmod(0o700)
        version = subprocess.run([str(target), "--version"], capture_output=True, text=True, timeout=5)
        if version.returncode or version.stdout.strip() != "nats-server: v" + VERSION:
            raise RuntimeError("cached broker version mismatch")
        return {"version": VERSION, "sha256": expected, "bytes": size}
    except BaseException:
        if created:
            target.unlink(missing_ok=True)
        raise
    finally:
        os.close(descriptor)


def process_identity(pid, binary, config, group):
    if type(pid) is not int or pid <= 0:
        raise RuntimeError("positive owned broker PID required")
    base = Path("/proc") / str(pid)
    command = base.joinpath("cmdline").read_bytes()
    expected = b"\0".join(os.fsencode(value) for value in (binary, "-c", config)) + b"\0"
    fields = base.joinpath("stat").read_text().rsplit(") ", 1)[1].split()
    membership = base.joinpath("cgroup").read_text().splitlines()
    if command != expected or membership != ["0::" + group] or not fields[19].isdigit():
        raise RuntimeError("broker process identity mismatch")
    return {"pid": pid, "start_ticks": int(fields[19]), "cgroup": group,
            "argv_sha256": hashlib.sha256(command).hexdigest()}


class QueryBroker(ExportBrokerFixture):
    """Broker shares the delegated supervisor cap and records custody before readiness."""
    def __init__(self, directory, binary, binary_sha256, group, *, mode="query", environment=ENVIRONMENT):
        super().__init__(directory, binary, binary_sha256, VERSION, mode)
        self.expected_environment = frozenset(environment)
        self.group = group
        self.identity = None
        self.closed_verified = False
        self.receipt = {"started": False, "reaped": False, "pid_absent": False}

    def record_start(self):
        # Popen still owns the child even if /proc validation fails. Retain that
        # creation evidence before reading any optional OS identity fields.
        self.receipt["created_pid"] = self.process.pid
        self._save()
        self.identity = process_identity(self.process.pid, self.binary, self.directory / "broker.conf", self.group)
        self.receipt.update(started=True, identity=self.identity)
        self._save()

    def start(self):
        result = super().start()
        if set(self.environment) != self.expected_environment:
            raise RuntimeError("exact contained broker environment required")
        if process_identity(self.process.pid, self.binary, self.directory / "broker.conf", self.group) != self.identity:
            raise RuntimeError("broker changed during readiness")
        return result

    def _save(self):
        if self.created:
            target = self.directory / "ownership.json"
            temporary = self.directory / ".ownership.tmp"
            temporary.write_text(json.dumps(self.receipt, indent=2) + "\n")
            os.replace(temporary, target)

    def close(self):
        if self.closed_verified:
            return True
        try:
            alive = self.process is not None and self.process.poll() is None
            self.receipt["alive_before_stop"] = alive
            if alive and self.identity is not None:
                if process_identity(self.process.pid, self.binary, self.directory / "broker.conf", self.group) != self.identity:
                    raise RuntimeError("refusing to signal a changed broker process")
            # A failed identity read must not orphan the unreaped Popen child.
            # This cleanup cannot pass without the full start receipt below.
            reaped = super().close()
            absent = self.identity is not None and not Path("/proc", str(self.identity["pid"])).exists()
            self.receipt.update(reaped=bool(reaped and self.identity), pid_absent=absent, forced_kill=self.forced_kill)
            if self.identity is not None:
                self.receipt["returncode"] = self.process.returncode
            if reaped and absent:
                for name in ("server.key", "broker.conf", "environment.json"):
                    path = self.directory / name
                    if path.is_file() and not path.is_symlink():
                        path.unlink()
            self.closed_verified = bool(alive and reaped and absent and not self.forced_kill and self.process.returncode == 0)
            return self.closed_verified
        finally:
            self._save()


def valid_broker(receipt, group):
    if (not isinstance(receipt, dict) or receipt.get("started") is not True or receipt.get("reaped") is not True
            or receipt.get("pid_absent") is not True or receipt.get("alive_before_stop") is not True
            or receipt.get("forced_kill") is not False
            or type(receipt.get("returncode")) is not int or receipt["returncode"] != 0):
        return False
    identity = receipt.get("identity")
    return (isinstance(identity, dict) and set(identity) == {"pid", "start_ticks", "cgroup", "argv_sha256"}
            and type(identity["pid"]) is int and identity["pid"] > 0
            and type(receipt.get("created_pid")) is int and receipt["created_pid"] == identity["pid"]
            and type(identity["start_ticks"]) is int and identity["start_ticks"] > 0
            and identity["cgroup"] == group and type(receipt.get("returncode")) is int
            and isinstance(identity["argv_sha256"], str) and re.fullmatch(r"[a-f0-9]{64}", identity["argv_sha256"]) is not None)
