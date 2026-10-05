#!/usr/bin/env python3
"""Offline negative controls for the contained query broker's owned lifecycle."""
import copy
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import export_broker_acceptance as broker
import protected_query_acceptance as fixture


GROUP = "/system.slice/kelvo-contained-fixture.service/supervisor"
PID = 424242
IDENTITY = {"pid": PID, "start_ticks": 111, "cgroup": GROUP, "argv_sha256": "a" * 64}


class Process:
    """Only the Popen-owned child can be signalled in these controls."""
    def __init__(self, outcomes=(), returncode=None):
        self.pid, self.returncode = PID, returncode
        self.outcomes = list(outcomes)
        self.terminated, self.killed, self.waits = 0, 0, []

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated += 1

    def kill(self):
        self.killed += 1

    def wait(self, timeout):
        self.waits.append(timeout)
        outcome = self.outcomes.pop(0) if self.outcomes else 0
        if isinstance(outcome, BaseException):
            raise outcome
        self.returncode = outcome
        return outcome


class BrokerControlFixture:
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.binary = self.root / "cached-server"
        self.binary.write_bytes(b"pinned fixture bytes, never executed")
        self.digest = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        self.artifact = self.root / "artifact"
        self.artifact.mkdir()

    def seed(self, **kwargs):
        return fixture.seed_binary(kwargs.get("source", self.binary), self.artifact,
                                   kwargs.get("digest", self.digest))

    def version(self, version=fixture.VERSION):
        return subprocess.CompletedProcess([], 0, "nats-server: v" + version + "\n", "")

    def owned(self, process=None):
        owned = fixture.QueryBroker(self.root / "owned", self.binary, self.digest, GROUP)
        owned.directory.mkdir()
        owned.created = True
        owned.process = process or Process()
        owned.identity = dict(IDENTITY)
        owned.receipt.update(created_pid=PID, started=True, identity=dict(IDENTITY))
        for name in ("server.key", "broker.conf", "environment.json"):
            (owned.directory / name).write_text("private fixture\n")
        return owned

    def close_context(self, identity=IDENTITY, present=False):
        stack = ExitStack()
        stack.enter_context(mock.patch.object(fixture, "process_identity", return_value=dict(identity)))
        original_exists = Path.exists

        def exists(path):
            if path == Path("/proc", str(PID)):
                return present
            return original_exists(path)

        stack.enter_context(mock.patch.object(Path, "exists", exists))
        return stack

    def start_context(self, process=None, ready=True):
        """Mock external programs/network; keep actual bounded fixture files."""
        stack = ExitStack()
        stack.enter_context(mock.patch.object(broker.platform, "system", return_value="Linux"))
        stack.enter_context(mock.patch.object(broker.os, "umask"))
        stack.enter_context(mock.patch.object(broker.subprocess, "run", return_value=self.version()))
        launch = stack.enter_context(mock.patch.object(broker.subprocess, "Popen", return_value=process or Process()))
        socket = stack.enter_context(mock.patch.object(broker.socket, "socket"))
        socket.return_value.__enter__.return_value.getsockname.return_value = ("127.0.0.1", 43123)
        connection = stack.enter_context(mock.patch.object(broker.socket, "create_connection"))
        if not ready:
            connection.side_effect = OSError("fixture connection refused")
        return stack, launch, connection


class ProtectedQueryControls(BrokerControlFixture, unittest.TestCase):
    def test_seed_copies_only_verified_bounded_bytes_and_runs_the_copy(self):
        with mock.patch.object(fixture.subprocess, "run", return_value=self.version()) as run:
            receipt = self.seed()
        target = self.artifact / "nats-server"
        self.assertEqual(target.read_bytes(), self.binary.read_bytes())
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)
        self.assertEqual(receipt, {"version": fixture.VERSION, "sha256": self.digest,
                                   "bytes": self.binary.stat().st_size})
        self.assertEqual(run.call_args.args[0], [str(target), "--version"])
        self.assertEqual(run.call_args.kwargs["timeout"], 5)

    def test_seed_requires_absolute_source_and_exact_lowercase_digest(self):
        for source, digest in ((Path("relative"), self.digest), (self.binary, "bad"),
                               (self.binary, self.digest.upper()), (self.binary, None)):
            with self.subTest(source=source, digest=digest), mock.patch.object(fixture.subprocess, "run") as run:
                with self.assertRaises(RuntimeError):
                    self.seed(source=source, digest=digest)
                run.assert_not_called()
        self.assertFalse((self.artifact / "nats-server").exists())

    def test_seed_refuses_symlink_and_nonregular_source_without_execution(self):
        link = self.root / "link"
        link.symlink_to(self.binary)
        directory = self.root / "directory"
        directory.mkdir()
        fifo = self.root / "fifo"
        os.mkfifo(fifo, 0o600)
        for source in (link, directory, fifo):
            with self.subTest(source=source), mock.patch.object(fixture.subprocess, "run") as run:
                with self.assertRaises((OSError, RuntimeError)):
                    self.seed(source=source)
                run.assert_not_called()
                self.assertFalse((self.artifact / "nats-server").exists())

    def test_seed_refuses_empty_oversized_and_changed_size_inputs(self):
        empty = self.root / "empty"
        empty.touch()
        with mock.patch.object(fixture.subprocess, "run") as run:
            with self.assertRaises(RuntimeError):
                self.seed(source=empty)
            with mock.patch.object(fixture, "MAX_BINARY", 1), self.assertRaises(RuntimeError):
                self.seed()
            actual = self.binary.stat()
            with mock.patch.object(fixture.os, "fstat", return_value=SimpleNamespace(st_mode=actual.st_mode, st_size=actual.st_size - 1)), \
                    self.assertRaises(RuntimeError):
                self.seed()
            run.assert_not_called()
        self.assertFalse((self.artifact / "nats-server").exists())

    def test_seed_never_adopts_or_removes_existing_target(self):
        target = self.artifact / "nats-server"
        for symlink in (False, True):
            with self.subTest(symlink=symlink):
                if symlink:
                    target.symlink_to(self.binary)
                else:
                    target.write_bytes(b"already owned elsewhere")
                with mock.patch.object(fixture.subprocess, "run") as run, self.assertRaises(FileExistsError):
                    self.seed()
                run.assert_not_called()
                self.assertEqual(target.is_symlink(), symlink)
                self.assertEqual(target.read_bytes(), self.binary.read_bytes() if symlink else b"already owned elsewhere")
                target.unlink()

    def test_seed_digest_and_version_failure_remove_only_the_new_copy(self):
        with mock.patch.object(fixture.subprocess, "run") as run, self.assertRaises(RuntimeError):
            self.seed(digest="0" * 64)
        run.assert_not_called()
        for outcome in (self.version("2.14.7"), subprocess.CompletedProcess([], 1, "", ""),
                        subprocess.TimeoutExpired("fixture", 5)):
            with self.subTest(outcome=type(outcome).__name__):
                kwargs = {"side_effect": outcome} if isinstance(outcome, BaseException) else {"return_value": outcome}
                with mock.patch.object(fixture.subprocess, "run", **kwargs), self.assertRaises((RuntimeError, subprocess.TimeoutExpired)):
                    self.seed()
                self.assertFalse((self.artifact / "nats-server").exists())
                self.assertTrue(self.binary.exists())

    def test_query_mode_has_two_accounts_exact_roles_no_exports_and_bounded_storage(self):
        owned = broker.ExportBrokerFixture(self.root / "query", self.binary, self.digest, fixture.VERSION, "query")
        stack, launch, _ = self.start_context()
        with stack:
            owned.start()
            config = json.loads((owned.directory / "broker.conf").read_text())
            self.assertEqual(set(owned.environment), fixture.ENVIRONMENT)
            self.assertEqual(set(config["accounts"]), {"a", "b"})
            self.assertEqual(config["host"], "127.0.0.1")
            self.assertNotIn("authorization", config)
            self.assertEqual(config["max_payload"], 1 << 20)
            self.assertEqual(config["max_connections"], 32)
            self.assertEqual(config["max_subscriptions"], 256)
            self.assertEqual(config["max_pending"], 4 << 20)
            self.assertFalse(config["debug"])
            self.assertFalse(config["trace"])
            self.assertEqual(config["jetstream"], {"store_dir": str(owned.directory / "state"),
                                                   "max_mem_store": 32 << 20, "max_file_store": 128 << 20})
            passwords = set()
            for tenant, account in config["accounts"].items():
                self.assertEqual(set(account), {"jetstream", "users"})
                self.assertEqual(account["jetstream"], {"max_memory": 16 << 20, "max_file": 64 << 20,
                                                        "max_streams": 5, "max_consumers": 2})
                self.assertEqual(len(account["users"]), 3)
                for role, user in zip(("initializer", "gateway", "worker"), account["users"]):
                    self.assertEqual(user["user"], tenant + "-" + role)
                    prefix = "KELVO_TEST_QUERY_NATS_" + tenant.upper() + "_" + role.upper()
                    self.assertEqual(owned.environment[prefix + "_USER"], user["user"])
                    self.assertEqual(owned.environment[prefix + "_PASSWORD"], user["password"])
                    self.assertRegex(user["password"], r"^[0-9a-f]{64}$")
                    passwords.add(user["password"])
                    permissions = user["permissions"]
                    self.assertEqual(permissions, broker.tenant_permissions(role, False))
                    subjects = permissions["publish"]["allow"]
                    self.assertFalse(any("EXPORT" in value or "export.ready" in value for value in subjects))
                    self.assertNotIn(">", subjects)
                    self.assertNotIn("$JS.API.>", subjects)
                    self.assertEqual(permissions["subscribe"]["allow"], ["_INBOX.>"])
                    if role == "initializer":
                        self.assertNotIn("$KV.KELVO_JOBS.slot.*", subjects)
                        self.assertNotIn("job.ready", subjects)
                    else:
                        self.assertIn("$KV.KELVO_JOBS.slot.*", subjects)
                        self.assertFalse(any(".STREAM.CREATE." in value or ".STREAM.DELETE." in value for value in subjects))
            self.assertEqual(len(passwords), 6)
            self.assertEqual(launch.call_args.args[0], [str(self.binary), "-c", str(owned.directory / "broker.conf")])
            self.assertTrue(owned.close())

    def test_start_identity_failure_retains_creation_receipt_and_cleans_popen(self):
        process = Process()
        owned = fixture.QueryBroker(self.root / "start-failure", self.binary, self.digest, GROUP)
        stack, _, _ = self.start_context(process)
        with stack, mock.patch.object(fixture, "process_identity", side_effect=RuntimeError("fixture identity failed")):
            with self.assertRaisesRegex(RuntimeError, "identity failed"):
                with owned:
                    self.fail("invalid identity became ready")
        receipt = json.loads((owned.directory / "ownership.json").read_text())
        self.assertEqual(receipt["created_pid"], PID)
        self.assertFalse(receipt["started"])
        self.assertFalse(receipt["reaped"], "OS identity failure cannot become verified ownership")
        self.assertEqual(process.terminated, 1)
        self.assertEqual(process.returncode, 0)
        self.assertTrue(owned.log.closed)
        self.assertFalse(fixture.valid_broker(receipt, GROUP))

    def test_broker_exit_before_readiness_is_rejected(self):
        owned = fixture.QueryBroker(self.root / "crashed-start", self.binary, self.digest, GROUP)
        process = Process(returncode=2)
        stack, _, connection = self.start_context(process)
        with stack, self.close_context(), self.assertRaisesRegex(RuntimeError, "did not start"):
            with owned:
                self.fail("crashed broker became ready")
        connection.assert_not_called()
        self.assertFalse(fixture.valid_broker(owned.receipt, GROUP))
        self.assertEqual(process.terminated, 0)

    def test_readiness_deadline_rejects_unavailable_broker_and_reaps_it(self):
        process = Process()
        owned = fixture.QueryBroker(self.root / "readiness-timeout", self.binary, self.digest, GROUP)
        stack, _, _ = self.start_context(process, ready=False)
        with stack, self.close_context(), mock.patch.object(broker.time, "monotonic", side_effect=[0, 11]), \
                self.assertRaisesRegex(RuntimeError, "did not start"):
            with owned:
                self.fail("unready broker became ready")
        self.assertEqual(process.terminated, 1)
        self.assertEqual(process.returncode, 0)

    def test_query_start_requires_exact_environment_and_same_readiness_identity(self):
        for bad in ("environment", "identity"):
            with self.subTest(bad=bad):
                owned = fixture.QueryBroker(self.root / ("mismatch-" + bad), self.binary, self.digest, GROUP)
                owned.process = Process()
                owned.identity = dict(IDENTITY)
                owned.environment = {key: "fixture" for key in fixture.ENVIRONMENT}
                if bad == "environment":
                    owned.environment["UNRELATED_CREDENTIAL"] = "fixture"
                current = dict(IDENTITY, start_ticks=112) if bad == "identity" else IDENTITY
                with mock.patch.object(broker.ExportBrokerFixture, "start", return_value=owned), \
                        mock.patch.object(fixture, "process_identity", return_value=current), self.assertRaises(RuntimeError):
                    owned.start()

    def test_changed_or_reused_pid_refuses_every_signal_and_retains_receipt(self):
        for change in ({"start_ticks": 112}, {"pid": PID + 1}, {"argv_sha256": "b" * 64}, {"cgroup": GROUP + "-foreign"}):
            with self.subTest(change=change):
                owned = self.owned()
                with self.close_context(dict(IDENTITY, **change)), self.assertRaisesRegex(RuntimeError, "refusing to signal"):
                    owned.close()
                self.assertEqual(owned.process.terminated, 0)
                self.assertEqual(owned.process.killed, 0)
                self.assertEqual(owned.process.waits, [])
                self.assertTrue((owned.directory / "ownership.json").exists())
                self.assertFalse(fixture.valid_broker(owned.receipt, GROUP))
                for path in owned.directory.iterdir():
                    path.unlink()
                owned.directory.rmdir()

    def test_wait_timeout_and_forced_kill_never_count_as_graceful_acceptance(self):
        for exit_code in (-9, 0):
            with self.subTest(exit_code=exit_code):
                process = Process([subprocess.TimeoutExpired("fixture", 5), exit_code])
                owned = self.owned(process)
                with self.close_context():
                    self.assertFalse(owned.close(), "a timed-out stop must not pass even if exit races with kill")
                self.assertEqual(process.terminated, 1)
                self.assertEqual(process.killed, 1)
                self.assertFalse(fixture.valid_broker(owned.receipt, GROUP))
                for path in owned.directory.iterdir():
                    path.unlink()
                owned.directory.rmdir()

    def test_held_kill_wait_preserves_unreaped_failure(self):
        process = Process([subprocess.TimeoutExpired("fixture", 5), subprocess.TimeoutExpired("fixture", 5)])
        owned = self.owned(process)
        with self.close_context(), self.assertRaises(subprocess.TimeoutExpired):
            owned.close()
        self.assertIsNone(process.returncode)
        self.assertEqual(process.killed, 1)
        self.assertFalse(fixture.valid_broker(json.loads((owned.directory / "ownership.json").read_text()), GROUP))

    def test_already_exited_or_unexpected_exit_cannot_pass_cleanup(self):
        for code in (0, 2, -9):
            with self.subTest(code=code):
                owned = self.owned(Process(returncode=code))
                with self.close_context():
                    self.assertFalse(owned.close())
                self.assertEqual(owned.process.terminated, 0)
                self.assertFalse(fixture.valid_broker(owned.receipt, GROUP))
                for path in owned.directory.iterdir():
                    path.unlink()
                owned.directory.rmdir()
        owned = self.owned(Process([2]))
        with self.close_context():
            self.assertFalse(owned.close())
        self.assertFalse(fixture.valid_broker(owned.receipt, GROUP))

    def test_graceful_stop_is_idempotent_and_removes_only_owned_secret_files(self):
        owned = self.owned()
        for name in ("broker.log", "server.crt", "keep-me.txt"):
            (owned.directory / name).write_text("retained evidence\n")
        with self.close_context():
            self.assertTrue(owned.close())
            first = copy.deepcopy(owned.receipt)
            self.assertTrue(owned.close())
        self.assertEqual(owned.receipt, first)
        self.assertEqual(owned.process.terminated, 1)
        self.assertEqual(owned.process.killed, 0)
        self.assertEqual(owned.process.waits, [5])
        self.assertTrue(fixture.valid_broker(first, GROUP))
        for name in ("server.key", "broker.conf", "environment.json"):
            self.assertFalse((owned.directory / name).exists())
        for name in ("broker.log", "server.crt", "keep-me.txt", "ownership.json"):
            self.assertTrue((owned.directory / name).is_file())

    def test_pid_still_present_is_not_cleanup_even_after_wait(self):
        owned = self.owned()
        with self.close_context(present=True):
            self.assertFalse(owned.close())
        self.assertFalse(fixture.valid_broker(owned.receipt, GROUP))
        self.assertTrue((owned.directory / "broker.conf").exists())

    def test_missing_or_malformed_receipts_never_pass(self):
        owned = self.owned()
        with self.close_context():
            self.assertTrue(owned.close())
        receipt = owned.receipt
        for bad in (None, False, [], {}, {"started": True}):
            self.assertFalse(fixture.valid_broker(bad, GROUP))
        for key in ("created_pid", "started", "reaped", "pid_absent", "alive_before_stop", "returncode", "forced_kill", "identity"):
            bad = copy.deepcopy(receipt)
            del bad[key]
            self.assertFalse(fixture.valid_broker(bad, GROUP), key)
        for key in ("started", "reaped", "pid_absent", "alive_before_stop"):
            for value in (False, 1, "true", None):
                bad = copy.deepcopy(receipt)
                bad[key] = value
                self.assertFalse(fixture.valid_broker(bad, GROUP), (key, value))
        for value in (True, 0, "false", None):
            bad = copy.deepcopy(receipt)
            bad["forced_kill"] = value
            self.assertFalse(fixture.valid_broker(bad, GROUP), ("forced_kill", value))
        for key, value in (("returncode", True), ("returncode", -9), ("returncode", 2),
                           ("created_pid", True), ("created_pid", PID + 1)):
            bad = copy.deepcopy(receipt)
            bad[key] = value
            self.assertFalse(fixture.valid_broker(bad, GROUP), (key, value))
        for key, value in (("pid", True), ("pid", -1), ("start_ticks", True), ("start_ticks", 0),
                           ("cgroup", GROUP + "-other"), ("argv_sha256", "invalid"), ("argv_sha256", "A" * 64)):
            bad = copy.deepcopy(receipt)
            bad["identity"][key] = value
            self.assertFalse(fixture.valid_broker(bad, GROUP), (key, value))
        bad = copy.deepcopy(receipt)
        bad["identity"]["unrecognized"] = True
        self.assertFalse(fixture.valid_broker(bad, GROUP))

    def test_process_identity_binds_exact_argv_start_ticks_and_cgroup(self):
        config = self.root / "broker.conf"
        command = b"\0".join(os.fsencode(value) for value in (self.binary, "-c", config)) + b"\0"
        status = "424242 (fixture process) " + " ".join(["S"] + ["0"] * 18 + ["111"])
        def read_text(path):
            return status if path.name == "stat" else "0::" + GROUP + "\n"
        with mock.patch.object(Path, "read_bytes", return_value=command), mock.patch.object(Path, "read_text", read_text):
            identity = fixture.process_identity(PID, self.binary, config, GROUP)
        self.assertEqual(identity, dict(IDENTITY, argv_sha256=hashlib.sha256(command).hexdigest()))
        for bad in (0, -1, True, "424242"):
            with self.subTest(pid=bad), self.assertRaises(RuntimeError):
                fixture.process_identity(bad, self.binary, config, GROUP)
        for bad_command, bad_stat, bad_group in ((command + b"extra\0", status, GROUP),
                                               (command, status[:-3] + "bad", GROUP),
                                               (command, status, GROUP + "-other")):
            def changed_text(path):
                return bad_stat if path.name == "stat" else "0::" + bad_group + "\n"
            with mock.patch.object(Path, "read_bytes", return_value=bad_command), \
                    mock.patch.object(Path, "read_text", changed_text), self.assertRaises(RuntimeError):
                fixture.process_identity(PID, self.binary, config, GROUP)


if __name__ == "__main__":
    unittest.main()
