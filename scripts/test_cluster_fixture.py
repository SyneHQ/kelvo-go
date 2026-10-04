#!/usr/bin/env python3
"""Fixture regressions; mocked brokers and bounded Python children, no downloads or Go."""
import builtins
import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest import mock

import cluster_fixture as fixture

REAL_POPEN = subprocess.Popen


class OwnedChild:
    """Direct-child model for failures before any real broker can be started."""
    def __init__(self, pid, stubborn=False, clock=None):
        self.pid = pid
        self.returncode = None
        self.stubborn = stubborn
        self.clock = clock
        self.reaped = False
        self.signals = []

    def terminate(self):
        self.signals.append(signal.SIGTERM)
        if not self.stubborn:
            self.returncode = -signal.SIGTERM

    def kill(self):
        self.signals.append(signal.SIGKILL)
        self.returncode = -signal.SIGKILL

    def wait(self, timeout):
        if self.returncode is None:
            if self.clock is not None:
                self.clock[0] += timeout
            raise subprocess.TimeoutExpired("owned-test-child", timeout)
        self.reaped = True
        return self.returncode


class ProvisionOwnershipTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="kelvo-provision-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name) / "fixture"
        self.archive = Path(temporary.name) / "cached.tar"
        with tarfile.open(self.archive, "w") as archive:
            content = b"test-only broker placeholder; never executed"
            member = tarfile.TarInfo(f"nats-server-v{fixture.VERSION}-linux-amd64/nats-server")
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
        self.children, self.logs = [], []
        stack = contextlib.ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(mock.patch.object(fixture, "DIR", self.directory))
        stack.enter_context(mock.patch.dict(fixture.RELEASES, {
            fixture.VERSION: hashlib.sha256(self.archive.read_bytes()).hexdigest()}))
        stack.enter_context(mock.patch.object(fixture, "cert", return_value={"cert_file": "fixture-only"}))
        stack.enter_context(mock.patch.object(fixture.subprocess, "run"))
        self.popen = stack.enter_context(mock.patch.object(fixture.subprocess, "Popen", side_effect=self.spawn))
        self.ready = stack.enter_context(mock.patch.object(fixture, "wait_for_brokers"))
        self.open_log = stack.enter_context(mock.patch.object(fixture, "open", side_effect=self.log, create=True))
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))

    def spawn(self, *args, **kwargs):
        child = OwnedChild(90000 + len(self.children))
        self.children.append(child)
        return child

    def log(self, *args, **kwargs):
        stream = builtins.open(*args, **kwargs)
        self.logs.append(stream)
        self.addCleanup(stream.close)
        return stream

    def provision(self):
        fixture.provision(self.archive)

    def assert_reaped(self, count):
        self.assertEqual(len(self.children), count)
        for child in self.children:
            self.assertTrue(child.reaped, "owned broker was not reaped after provisioning failed")
            self.assertEqual(child.signals, [signal.SIGTERM])
        self.assertTrue(all(stream.closed for stream in self.logs), "spawn log descriptor left open")

    def fail_write(self, name):
        original = fixture.write
        def write(path, data):
            if path.name == name:
                raise OSError("injected publication failure")
            return original(path, data)
        return mock.patch.object(fixture, "write", side_effect=write)

    def test_later_config_failure_reaps_previously_started_child(self):
        with self.fail_write("nats-1.conf"), self.assertRaisesRegex(OSError, "injected"):
            self.provision()
        self.assert_reaped(1)

    def test_later_log_open_failure_reaps_previously_started_child(self):
        def opening(path, *args, **kwargs):
            if path.name == "nats-1.log":
                raise OSError("injected log failure")
            return self.log(path, *args, **kwargs)
        self.open_log.side_effect = opening
        with self.assertRaisesRegex(OSError, "injected log failure"):
            self.provision()
        self.assert_reaped(1)

    def test_later_spawn_failure_reaps_previously_started_child_and_closes_logs(self):
        def spawning(*args, **kwargs):
            if self.children:
                raise OSError("injected spawn failure")
            return self.spawn(*args, **kwargs)
        self.popen.side_effect = spawning
        with self.assertRaisesRegex(OSError, "injected spawn failure"):
            self.provision()
        self.assert_reaped(1)
        self.assertEqual(len(self.logs), 2)

    def test_log_close_failure_reaps_child_before_pid_publication(self):
        owner = self
        class FailingClose:
            def __init__(self, *args, **kwargs):
                self.stream = owner.log(*args, **kwargs)

            def __enter__(self):
                return self.stream

            def __exit__(self, *args):
                self.stream.close()
                raise OSError("injected log close failure")
        self.open_log.side_effect = FailingClose
        with mock.patch.object(fixture, "publish_pid_records") as publish:
            with self.assertRaisesRegex(OSError, "injected log close failure"):
                self.provision()
        self.assert_reaped(1)
        publish.assert_not_called()

    def test_first_pid_publication_failure_still_reaps_child(self):
        with mock.patch.object(fixture, "publish_pid_records", side_effect=OSError("injected PID failure")):
            with self.assertRaisesRegex(OSError, "injected PID failure"):
                self.provision()
        self.assert_reaped(1)

    def test_later_pid_publication_failure_reaps_all_and_keeps_previous_snapshot(self):
        original = fixture.publish_pid_records
        def publish(items):
            if len(items) == 2:
                raise OSError("injected PID failure")
            original(items)
        with mock.patch.object(fixture, "publish_pid_records", side_effect=publish):
            with self.assertRaisesRegex(OSError, "injected PID failure"):
                self.provision()
        self.assert_reaped(2)
        self.assertEqual(json.loads((self.directory / "pids.json").read_text()),
                         [{"name": "nats-0", "pid": self.children[0].pid}])

    def test_environment_write_failure_reaps_all_started_children(self):
        with self.fail_write("environment.json"), self.assertRaisesRegex(OSError, "injected"):
            self.provision()
        self.assert_reaped(3)

    def test_manifest_write_failure_reaps_all_started_children(self):
        with self.fail_write("manifest.json"), self.assertRaisesRegex(OSError, "injected"):
            self.provision()
        self.assert_reaped(3)

    def test_readiness_failure_reaps_children_and_retains_private_pid_evidence(self):
        self.ready.side_effect = fixture.FixtureReadinessError("injected readiness failure")
        with self.assertRaisesRegex(SystemExit, "injected readiness failure"):
            self.provision()
        self.assert_reaped(3)
        records = self.directory / "pids.json"
        self.assertEqual(json.loads(records.read_text()),
                         [{"name": f"nats-{i}", "pid": child.pid} for i, child in enumerate(self.children)])
        self.assertEqual(records.stat().st_mode & 0o777, 0o600)
        self.assertTrue((self.directory / "manifest.json").is_file())

    def test_interruption_reaps_children_before_propagating(self):
        self.ready.side_effect = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.provision()
        self.assert_reaped(3)

    def test_cleanup_failure_retains_original_exception_and_checks_other_children(self):
        def spawning(*args, **kwargs):
            child = self.spawn(*args, **kwargs)
            if len(self.children) == 1:
                child.stubborn = True
                child.kill = mock.Mock(side_effect=OSError("injected kill failure"))
            return child
        self.popen.side_effect = spawning
        with self.fail_write("environment.json"):
            with self.assertRaises(fixture.FixtureCleanupError) as raised:
                self.provision()
        self.assertIsInstance(raised.exception.__context__, OSError)
        self.assertEqual(str(raised.exception.__context__), "injected publication failure")
        self.assertFalse(self.children[0].reaped)
        self.children[0].kill.assert_called_once()
        self.assertTrue(all(child.reaped for child in self.children[1:]))
        self.assertTrue(all(stream.closed for stream in self.logs))

    def test_success_transfers_live_children_with_all_pid_records_and_closed_logs(self):
        self.provision()
        records = [{"name": f"nats-{i}", "pid": child.pid} for i, child in enumerate(self.children)]
        self.assertEqual(json.loads((self.directory / "pids.json").read_text()), records)
        self.ready.assert_called_once_with(records, expected_versions=dict.fromkeys(fixture.BROKER_NAMES.values(), fixture.VERSION))
        self.assertTrue(all(not child.reaped and not child.signals for child in self.children))
        self.assertTrue(all(stream.closed for stream in self.logs))

    def test_existing_pid_inventory_is_not_overwritten_or_used_as_cleanup_targets(self):
        self.directory.mkdir()
        records = self.directory / "pids.json"
        records.write_text('[{"name":"prior-owned-broker","pid":123}]')
        before = records.read_bytes()
        with self.assertRaisesRegex(SystemExit, "Fixture exists"):
            self.provision()
        self.assertEqual(records.read_bytes(), before)
        self.popen.assert_not_called()
        self.assertEqual(list(self.directory.iterdir()), [records])

    @unittest.skipUnless(sys.platform == "linux", "bounded Linux direct-child cleanup regression")
    def test_real_partial_spawn_failure_reaps_owned_child_and_preserves_unrelated(self):
        ready_path = self.directory.parent / "child-ready"
        failure_time = []
        child_code = ("import pathlib,signal,sys,time; "
                      "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                      "pathlib.Path(sys.argv[1]).write_text('ready'); time.sleep(30)")
        unrelated = REAL_POPEN([sys.executable, "-c", "import time; time.sleep(30)"],
                               stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               start_new_session=True)
        def finish(process):
            if process.poll() is None:
                process.kill()
            process.wait(timeout=2)
        self.addCleanup(finish, unrelated)

        def spawning(*args, **kwargs):
            if self.children:
                failure_time.append(time.monotonic())
                raise OSError("injected later spawn failure")
            process = REAL_POPEN([sys.executable, "-c", child_code, str(ready_path)], **kwargs)
            self.children.append(process)
            self.addCleanup(finish, process)
            deadline = time.monotonic() + 2
            while not ready_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(ready_path.exists(), "owned child did not become ready")
            return process

        self.popen.side_effect = spawning
        with mock.patch.object(fixture, "BROKER_TERM_TIMEOUT", 0.1, create=True), \
                mock.patch.object(fixture, "BROKER_KILL_TIMEOUT", 0.5, create=True):
            with self.assertRaisesRegex(OSError, "injected later spawn failure"):
                self.provision()
            elapsed = time.monotonic() - failure_time[0]
        self.assertEqual(self.children[0].returncode, -signal.SIGKILL,
                         "owned child was not reaped after partial provisioning")
        with self.assertRaises(ChildProcessError):
            os.waitpid(self.children[0].pid, os.WNOHANG)
        self.assertIsNone(unrelated.poll(), "unrelated child was signalled")
        self.assertLess(elapsed, 3, "partial provision cleanup exceeded its bounds")
        self.assertTrue(all(stream.closed for stream in self.logs))


class PIDPublicationAndReapingTests(unittest.TestCase):
    def test_failed_atomic_replace_preserves_the_last_complete_inventory(self):
        with tempfile.TemporaryDirectory(prefix="kelvo-pid-record-test-") as directory:
            root = Path(directory)
            before = [{"name": "nats-0", "pid": 10}]
            with mock.patch.object(fixture, "DIR", root):
                fixture.publish_pid_records(before)
                with mock.patch.object(fixture.os, "replace", side_effect=OSError("injected replace failure")):
                    with self.assertRaisesRegex(OSError, "injected replace failure"):
                        fixture.publish_pid_records(before + [{"name": "nats-1", "pid": 11}])
            self.assertEqual(json.loads((root / "pids.json").read_text()), before)
            self.assertEqual((root / "pids.json").stat().st_mode & 0o777, 0o600)
            self.assertEqual(list(root.iterdir()), [root / "pids.json"])

    def test_wait_budgets_are_shared_and_all_pending_children_are_killed_and_reaped(self):
        clock = [0.0]
        children = [OwnedChild(i, stubborn=True, clock=clock) for i in range(3)]
        with mock.patch.object(fixture.time, "monotonic", side_effect=lambda: clock[0]):
            fixture.reap_started_brokers(children)
        self.assertEqual(clock[0], fixture.BROKER_TERM_TIMEOUT)
        self.assertTrue(all(child.signals == [signal.SIGTERM, signal.SIGKILL] for child in children))
        self.assertTrue(all(child.reaped for child in children))


def healthy_reports():
    names = list(fixture.BROKER_NAMES.values())
    return {
        name: {
            "server": {"server_name": name, "server_id": "id-" + name, "version": fixture.VERSION},
            "health": {"status": "ok"},
            "jetstream": {
                "server_id": "id-" + name,
                "meta_cluster": {
                    "name": "kelvo-test", "leader": names[0],
                    "cluster_size": 3, "quorum_needed": 2,
                    "replicas": [{"name": peer, "current": name == names[0]}
                                 for peer in names if peer != name],
                },
            },
        } for name in names
    }


class MetadataReadinessTests(unittest.TestCase):
    def test_versions_must_be_explicit_known_and_match_each_peer(self):
        for value in (None, "", "2.14.7", "2.16.0", ["2.15.0"]):
            with self.subTest(version=value):
                reports = healthy_reports()
                reports["kelvo-test-1"]["server"]["version"] = value
                self.assertEqual(fixture.metadata_readiness(reports), "monitor_version")
        reports = healthy_reports()
        del reports["kelvo-test-1"]["server"]["version"]
        self.assertEqual(fixture.metadata_readiness(reports), "monitor_version")
        for expected in ({}, {"kelvo-test-0": "2.15.0"},
                         dict.fromkeys(fixture.BROKER_NAMES.values(), "2.16.0")):
            self.assertEqual(fixture.metadata_readiness(healthy_reports(), expected), "unsupported_expected_versions")

    def test_predecessor_is_allowed_only_when_declared_per_peer(self):
        reports = healthy_reports()
        expected = dict.fromkeys(fixture.BROKER_NAMES.values(), fixture.VERSION)
        for name in expected:
            expected[name] = "2.14.7"
            reports[name]["server"]["version"] = "2.14.7"
            del reports[name]["jetstream"]["meta_cluster"]["quorum_needed"]
            self.assertEqual(fixture.metadata_readiness(reports, expected), "ready")
            self.assertEqual(fixture.metadata_readiness(reports), "monitor_version")

    def test_predecessor_still_requires_complete_current_consensus(self):
        expected = dict.fromkeys(fixture.BROKER_NAMES.values(), "2.14.7")
        for key in ("name", "cluster_size", "leader", "replicas"):
            with self.subTest(missing=key):
                reports = healthy_reports()
                for report in reports.values():
                    report["server"]["version"] = "2.14.7"
                    del report["jetstream"]["meta_cluster"]["quorum_needed"]
                del reports["kelvo-test-0"]["jetstream"]["meta_cluster"][key]
                self.assertNotEqual(fixture.metadata_readiness(reports, expected), "ready")

    def test_current_version_never_inherits_predecessor_missing_quorum_exception(self):
        reports = healthy_reports()
        del reports["kelvo-test-0"]["jetstream"]["meta_cluster"]["quorum_needed"]
        self.assertEqual(fixture.metadata_readiness(reports), "metadata_membership")

    def test_membership_numbers_require_integers(self):
        for key, value in (("cluster_size", 3.0), ("quorum_needed", 2.0),
                           ("cluster_size", True), ("quorum_needed", True)):
            with self.subTest(key=key, value=value):
                reports = healthy_reports()
                reports["kelvo-test-0"]["jetstream"]["meta_cluster"][key] = value
                self.assertEqual(fixture.metadata_readiness(reports), "metadata_membership")

    def test_only_the_leader_current_view_is_authoritative(self):
        reports = healthy_reports()
        # A follower's view can mark another healthy follower stale/offline.
        reports["kelvo-test-1"]["jetstream"]["meta_cluster"]["replicas"][1]["offline"] = True
        self.assertEqual(fixture.metadata_readiness(reports), "ready")

    def test_any_expected_member_can_be_the_agreed_leader(self):
        reports = healthy_reports()
        for name, report in reports.items():
            meta = report["jetstream"]["meta_cluster"]
            meta["leader"] = "kelvo-test-2"
            for replica in meta["replicas"]:
                replica["current"] = name == "kelvo-test-2"
        self.assertEqual(fixture.metadata_readiness(reports), "ready")

    def test_absent_or_unexpected_leader_is_not_ready(self):
        for leader in (None, "", "other-cluster", ["kelvo-test-0"]):
            with self.subTest(leader=leader):
                reports = healthy_reports()
                reports["kelvo-test-1"]["jetstream"]["meta_cluster"]["leader"] = leader
                self.assertEqual(fixture.metadata_readiness(reports), "metadata_leader")

    def test_healthy_brokers_disagreeing_about_the_leader_are_not_ready(self):
        reports = healthy_reports()
        reports["kelvo-test-1"]["jetstream"]["meta_cluster"]["leader"] = "kelvo-test-1"
        self.assertEqual(fixture.metadata_readiness(reports), "metadata_leader_disagreement")

    def test_any_member_with_wrong_cluster_membership_is_not_ready(self):
        for key, value in (("name", "other"), ("cluster_size", 2),
                           ("quorum_needed", 1), ("rescue", True)):
            with self.subTest(key=key):
                reports = healthy_reports()
                reports["kelvo-test-2"]["jetstream"]["meta_cluster"][key] = value
                self.assertEqual(fixture.metadata_readiness(reports), "metadata_membership")

    def test_leader_requires_exactly_two_expected_current_online_peers(self):
        for peers in (
            [],
            [{"name": "kelvo-test-1", "current": True}] * 2,
            [{"name": "kelvo-test-1", "current": True}, {"name": "foreign", "current": True}],
            [{"name": "kelvo-test-1", "current": True}, {"name": "kelvo-test-2", "current": False}],
            [{"name": "kelvo-test-1", "current": True}, {"name": "kelvo-test-2", "current": True, "offline": True}],
        ):
            with self.subTest(peers=peers):
                reports = healthy_reports()
                reports["kelvo-test-0"]["jetstream"]["meta_cluster"]["replicas"] = peers
                self.assertEqual(fixture.metadata_readiness(reports), "metadata_replicas")

    def test_monitor_identity_and_metadata_health_are_required_on_all_members(self):
        for section, key, value, expected in (
            ("server", "server_name", "foreign", "monitor_identity"),
            ("jetstream", "server_id", "another-server", "monitor_identity"),
            ("health", "status", "unavailable", "metadata_health"),
            ("jetstream", "disabled", True, "metadata_health"),
        ):
            with self.subTest(section=section, key=key):
                reports = healthy_reports()
                reports["kelvo-test-2"][section][key] = value
                self.assertEqual(fixture.metadata_readiness(reports), expected)

    def test_duplicate_server_identity_cannot_substitute_for_a_third_broker(self):
        reports = healthy_reports()
        for key in ("server", "jetstream"):
            reports["kelvo-test-2"][key]["server_id"] = "id-kelvo-test-1"
        self.assertEqual(fixture.metadata_readiness(reports), "monitor_identity")


class BrokerIdentityTests(unittest.TestCase):
    def test_pid_must_have_the_exact_owned_command(self):
        item = {"name": "nats-0", "pid": 123}
        expected = b"\0".join(part.encode() for part in (
            str(fixture.DIR / "nats-server"), "-c", str(fixture.DIR / "nats-0.conf"))) + b"\0"
        for command, alive in ((expected, True), (b"", False),
                               (expected + b"--extra\0", False),
                               (expected.replace(b"nats-0.conf", b"nats-1.conf"), False)):
            with self.subTest(command=command), mock.patch.object(Path, "read_bytes", return_value=command):
                self.assertEqual(fixture.broker_alive(item), alive)
        with mock.patch.object(Path, "read_bytes", side_effect=FileNotFoundError):
            self.assertFalse(fixture.broker_alive(item))


class WaitForBrokersTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="kelvo-readiness-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.items = [{"name": name, "pid": 100 + i} for i, name in enumerate(fixture.BROKER_NAMES)]
        # Capacity fixtures remap ports; readiness must use their owned config.
        for i, item in enumerate(self.items):
            (self.directory / (item["name"] + ".conf")).write_text(json.dumps({
                "server_name": fixture.BROKER_NAMES[item["name"]],
                "http": f"127.0.0.1:{28222 + i}", "cluster": {"name": "kelvo-test"},
            }))
        self.reports = healthy_reports()
        self.now = 0.0
        self.calls = []
        for patcher in (
            mock.patch.object(fixture, "DIR", self.directory),
            mock.patch.object(fixture.time, "monotonic", side_effect=lambda: self.now),
            mock.patch.object(fixture.time, "sleep", side_effect=self.advance),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.alive = mock.patch.object(fixture, "broker_alive", return_value=True).start()
        self.monitor = mock.patch.object(fixture, "broker_monitor", side_effect=self.observe).start()
        self.kill = mock.patch.object(fixture.os, "kill").start()
        self.addCleanup(mock.patch.stopall)

    def advance(self, seconds):
        self.now += seconds

    def observe(self, port, path, timeout):
        self.calls.append((port, path, timeout))
        key = {"/varz": "server", "/healthz?js-meta-only=true": "health", "/jsz": "jetstream"}[path]
        return copy.deepcopy(self.reports[f"kelvo-test-{port - 28222}"][key])

    def test_healthy_metadata_proceeds_without_a_fixed_startup_sleep(self):
        fixture.wait_for_brokers(self.items)
        self.assertEqual(self.now, 0)
        self.assertEqual(len(self.calls), 9)
        self.assertTrue(all(0 < timeout <= 1 for _, _, timeout in self.calls))
        self.kill.assert_not_called()

    def test_missing_leader_is_retried_before_proceeding(self):
        def warming(port, path, timeout):
            result = self.observe(port, path, timeout)
            if path == "/jsz" and self.now == 0:
                result["meta_cluster"].pop("leader")
            return result

        self.monitor.side_effect = warming
        fixture.wait_for_brokers(self.items)
        self.assertEqual(self.now, 0.1)
        self.assertEqual(len(self.calls), 18)

    def test_dead_owned_process_fails_without_monitoring_or_stopping_anything(self):
        self.alive.return_value = False
        with self.assertRaisesRegex(fixture.FixtureReadinessError, "dead or unowned: nats-0"):
            fixture.wait_for_brokers(self.items)
        self.monitor.assert_not_called()
        self.kill.assert_not_called()

    def test_process_death_during_monitoring_cannot_pass_readiness(self):
        self.alive.side_effect = [True, True, True, False]
        with self.assertRaisesRegex(fixture.FixtureReadinessError, "dead or unowned: nats-0"):
            fixture.wait_for_brokers(self.items)
        self.assertEqual(len(self.calls), 9)
        self.kill.assert_not_called()

    def test_requested_timeout_is_capped_and_last_call_uses_remaining_budget(self):
        def unavailable(port, path, timeout):
            self.calls.append((port, path, timeout))
            self.advance(timeout)
            raise fixture.MonitorDeadline()

        self.monitor.side_effect = unavailable
        with self.assertRaisesRegex(fixture.FixtureReadinessError, "readiness timed out"):
            fixture.wait_for_brokers(self.items, timeout=300)
        self.assertAlmostEqual(self.now, 30)
        self.assertTrue(all(0 < timeout <= 1 for _, _, timeout in self.calls))
        self.assertLess(self.calls[-1][2], 1)
        self.kill.assert_not_called()

    def test_success_observed_at_the_deadline_does_not_pass(self):
        def late(port, path, timeout):
            result = self.observe(port, path, timeout)
            if port == 28224 and path == "/jsz":
                self.advance(0.25)
            return result

        self.monitor.side_effect = late
        with self.assertRaisesRegex(fixture.FixtureReadinessError, "readiness timed out: ready"):
            fixture.wait_for_brokers(self.items, timeout=0.25)

    def test_timeout_preserves_cleanup_artifacts_and_redacts_monitor_details(self):
        for name in ("pids.json", "environment.json", "manifest.json"):
            (self.directory / name).write_text("private fixture state")
        before = {path.name: path.read_bytes() for path in self.directory.iterdir()}
        self.reports["kelvo-test-0"]["jetstream"]["meta_cluster"]["leader"] = "secret-monitor-name"
        self.reports["kelvo-test-1"]["server"]["credentials"] = "secret-credentials"
        with self.assertRaises(fixture.FixtureReadinessError) as raised:
            fixture.wait_for_brokers(self.items, timeout=0.2)
        message = str(raised.exception)
        self.assertNotIn("secret", message)
        self.assertIn("missing_or_unexpected", message)
        self.assertEqual(before, {path.name: path.read_bytes() for path in self.directory.iterdir()})
        self.kill.assert_not_called()

    def test_monitor_error_text_is_not_disclosed(self):
        self.monitor.side_effect = OSError("secret-monitor-error")
        with self.assertRaises(fixture.FixtureReadinessError) as raised:
            fixture.wait_for_brokers(self.items, timeout=0.2)
        self.assertNotIn("secret", str(raised.exception))
        self.assertIn("server_unavailable", str(raised.exception))

    def test_invalid_inventory_or_nonloopback_monitor_fails_before_requests(self):
        invalid = copy.deepcopy(self.items)
        invalid[2]["pid"] = invalid[1]["pid"]
        with self.assertRaisesRegex(fixture.FixtureReadinessError, "inventory is invalid"):
            fixture.wait_for_brokers(invalid)
        config_path = self.directory / "nats-0.conf"
        config = json.loads(config_path.read_text())
        config["http"] = "external.invalid:8222"
        config_path.write_text(json.dumps(config))
        with self.assertRaisesRegex(fixture.FixtureReadinessError, "monitor configuration is invalid"):
            fixture.wait_for_brokers(self.items)
        self.monitor.assert_not_called()


class BrokerMonitorTests(unittest.TestCase):
    def setUp(self):
        self.connection = mock.Mock()
        self.response = self.connection.getresponse.return_value
        self.response.status = 200
        self.response.read.return_value = b'{"status":"ok"}'
        self.http = mock.patch.object(fixture.http.client, "HTTPConnection", return_value=self.connection).start()
        self.timer = mock.patch.object(fixture.signal, "setitimer").start()
        self.handler = mock.patch.object(fixture.signal, "signal").start()
        self.gettimer = mock.patch.object(fixture.signal, "getitimer", return_value=(0, 0)).start()
        mock.patch.object(fixture.signal, "getsignal", return_value="prior-handler").start()
        self.addCleanup(mock.patch.stopall)

    def test_whole_request_is_capped_and_alarm_is_restored(self):
        self.assertEqual(fixture.broker_monitor(28222, "/healthz?js-meta-only=true", 3), {"status": "ok"})
        self.http.assert_called_once_with("127.0.0.1", 28222, timeout=1)
        self.timer.assert_has_calls([mock.call(fixture.signal.ITIMER_REAL, 1),
                                    mock.call(fixture.signal.ITIMER_REAL, 0)])
        self.assertEqual(self.handler.call_args, mock.call(fixture.signal.SIGALRM, "prior-handler"))
        self.connection.close.assert_called_once()

    def test_alarm_interrupts_body_read_and_still_closes_connection(self):
        def read(limit):
            alarm_handler = self.handler.call_args_list[0].args[1]
            alarm_handler(fixture.signal.SIGALRM, None)

        self.response.read.side_effect = read
        with self.assertRaises(fixture.MonitorDeadline):
            fixture.broker_monitor(28222, "/jsz", 0.2)
        self.assertEqual(self.timer.call_args, mock.call(fixture.signal.ITIMER_REAL, 0))
        self.assertEqual(self.handler.call_args, mock.call(fixture.signal.SIGALRM, "prior-handler"))
        self.connection.close.assert_called_once()

    def test_existing_alarm_is_never_replaced(self):
        self.gettimer.return_value = (5, 0)
        with self.assertRaisesRegex(fixture.FixtureReadinessError, "active alarm"):
            fixture.broker_monitor(28222, "/jsz", 1)
        self.http.assert_not_called()
        self.timer.assert_not_called()

    def test_redirects_large_bodies_and_nonobject_responses_are_rejected(self):
        for status, body in ((302, b""), (200, b"x" * (fixture.MONITOR_MAX_BYTES + 1)),
                             (200, b"[]"), (200, b"not JSON")):
            with self.subTest(status=status, length=len(body)):
                self.response.status = status
                self.response.read.return_value = body
                with self.assertRaises(ValueError):
                    fixture.broker_monitor(28222, "/jsz", 1)


if __name__ == "__main__":
    unittest.main()
