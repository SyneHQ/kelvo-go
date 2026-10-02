#!/usr/bin/env python3
"""Deterministic fixture readiness regressions; no brokers, downloads or Go."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import cluster_fixture as fixture


def healthy_reports():
    names = list(fixture.BROKER_NAMES.values())
    return {
        name: {
            "server": {"server_name": name, "server_id": "id-" + name},
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
