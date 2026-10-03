#!/usr/bin/env python3
"""Negative controls for process-loss evidence and owned fault injection."""
import copy
import json
from pathlib import Path
import signal
import tempfile
import unittest
from unittest import mock

import process_loss_acceptance as fixture


def valid_report():
    checks = [{"test": name, "passed": True} for name in fixture.REQUIRED]
    for check in checks:
        if check["test"] in fixture.REQUIRED[1:-1]:
            check.update(running_before_fault=2, queued_before_fault=2,
                         surviving_tenant_exact_result=True, failed_attempt_result_rejected=True,
                         queued_handles_preserved=True)
        elif check["test"] == "cleanup":
            check.update(forced_application_kills=0, observed_live_descendants=0,
                         worker_scratch_directories=0, original_configurations_verified=True,
                         all_owned_brokers_healthy=True)
    return {"interrupted": False, "checks": checks}


class EvidenceTests(unittest.TestCase):
    def test_required_gates_cannot_be_failed_omitted_or_duplicated(self):
        self.assertTrue(fixture.reconcile(valid_report()))
        for index in range(len(fixture.REQUIRED)):
            failed = valid_report()
            failed["checks"][index]["passed"] = False
            self.assertFalse(fixture.reconcile(failed))
            omitted = valid_report()
            omitted["checks"].pop(index)
            self.assertFalse(fixture.reconcile(omitted))
        repeated = valid_report()
        repeated["checks"].append(copy.deepcopy(repeated["checks"][0]))
        self.assertFalse(fixture.reconcile(repeated))

    def test_interrupted_campaign_never_passes(self):
        for value in (True, None, 0, "false"):
            report = valid_report()
            report["interrupted"] = value
            self.assertFalse(fixture.reconcile(report))

    def test_real_prefault_states_exact_values_and_non_replay_are_required(self):
        for index in (1, 2, 3):
            for field, invalid in (("running_before_fault", 0), ("queued_before_fault", 0),
                                   ("surviving_tenant_exact_result", False),
                                   ("failed_attempt_result_rejected", False),
                                   ("queued_handles_preserved", False)):
                report = valid_report()
                report["checks"][index][field] = invalid
                self.assertFalse(fixture.reconcile(report), field)

    def test_dirty_cleanup_or_unhealthy_quorum_cannot_pass(self):
        for key, value in (("forced_application_kills", 1), ("observed_live_descendants", 1),
                           ("worker_scratch_directories", 1), ("original_configurations_verified", False),
                           ("all_owned_brokers_healthy", False)):
            report = valid_report()
            report["checks"][-1][key] = value
            self.assertFalse(fixture.reconcile(report), key)

    def test_managed_scratch_requires_actual_sandbox_lease_observation(self):
        report = valid_report()
        report["managed_scratch_requested"] = True
        self.assertFalse(fixture.reconcile(report))
        report["checks"][0]["managed_scratch"] = True
        worker = report["checks"][2]
        for missing in (None, False, 0):
            worker["inherited_sandbox_lease_children"] = missing
            self.assertFalse(fixture.reconcile(report))
        worker["inherited_sandbox_lease_children"] = 1
        self.assertTrue(fixture.reconcile(report))

    def test_complete_successful_result_is_not_a_failed_attempt(self):
        self.assertFalse(fixture.rejected_result((200, b"arrow"+fixture.ops.EOS)))
        self.assertTrue(fixture.rejected_result(None))
        self.assertTrue(fixture.rejected_result((409, b"conflict")))
        self.assertTrue(fixture.rejected_result((200, b"partial")))

    def test_kill_uses_exact_live_identity_and_requires_sigkill_exit(self):
        acceptance = fixture.LossAcceptance.__new__(fixture.LossAcceptance)
        process = mock.Mock(pid=17)
        process.poll.return_value = None
        process.wait.return_value = -signal.SIGKILL
        acceptance.processes = {"a1": process}
        acceptance.intentional_kills = 0
        with mock.patch.object(fixture.ops, "proc_identity", return_value=(17, "start")), \
                mock.patch.object(fixture.ops, "signal_owned") as kill:
            acceptance.kill_application("a1")
            kill.assert_called_once_with((17, "start"), signal.SIGKILL)
        self.assertEqual(acceptance.intentional_kills, 1)
        process.poll.return_value = 0
        with mock.patch.object(fixture.ops, "proc_identity", return_value=None), \
                mock.patch.object(fixture.ops, "signal_owned") as kill:
            with self.assertRaisesRegex(fixture.ops.AcceptanceError, "FAULT_TARGET_NOT_RUNNING"):
                acceptance.kill_application("a1")
            kill.assert_not_called()

    def test_terminal_result_request_is_never_silently_accepted(self):
        acceptance = fixture.LossAcceptance.__new__(fixture.LossAcceptance)
        acceptance.call = mock.Mock(return_value=(200, b'{"state":"failed"}'))
        acceptance.control_response_counts = {}
        acceptance.wait = lambda callback, timeout: callback()
        acceptance.retry = mock.Mock(return_value=(200, b"unexpected"))
        future = mock.Mock()
        future.result.return_value = None
        with self.assertRaisesRegex(fixture.ops.AcceptanceError, "FAILED_ATTEMPT_WAS_REPLAYABLE"):
            acceptance.failed_attempt({"running": "private-handle", "future": future}, "a")

    def test_idempotent_cancel_retries_transient_control_failures_only(self):
        acceptance = fixture.LossAcceptance.__new__(fixture.LossAcceptance)
        acceptance.control_response_counts = {}
        acceptance.call = mock.Mock(side_effect=[(503, b"unavailable"), (409, b"changed"), (200, b"canceled")])
        with mock.patch.object(fixture.time, "sleep"):
            acceptance.cancel_running("owned-handle", "a")
        self.assertEqual(acceptance.control_response_counts, {"cancel_http_503": 1, "cancel_http_409": 1, "cancel_http_200": 1})
        self.assertTrue(all(call.args[0].endswith("/cancel") for call in acceptance.call.call_args_list))
        acceptance.call = mock.Mock(return_value=(403, b"forbidden"))
        acceptance.debug_response = mock.Mock()
        with self.assertRaisesRegex(fixture.ops.AcceptanceError, "SURVIVING_CANCELLATION_FAILED"):
            acceptance.cancel_running("owned-handle", "a")
        self.assertEqual(acceptance.call.call_count, 1)

    def test_transient_cancellation_recovery_has_deadline(self):
        acceptance = fixture.LossAcceptance.__new__(fixture.LossAcceptance)
        acceptance.control_response_counts = {}
        acceptance.call = mock.Mock(return_value=(503, b"unavailable"))
        with mock.patch.object(fixture.time, "monotonic", side_effect=[0, 11]):
            with self.assertRaisesRegex(fixture.ops.AcceptanceError, "SURVIVING_CANCELLATION_DEADLINE"):
                acceptance.cancel_running("owned-handle", "a")
        self.assertEqual(acceptance.call.call_count, 1)

    def test_failed_manifest_write_keeps_replacement_broker_owned_for_cleanup(self):
        acceptance = fixture.LossAcceptance.__new__(fixture.LossAcceptance)
        acceptance.broker_alive = lambda broker: False
        acceptance.broker_ports_closed = lambda broker: True
        acceptance.brokers = [{"name": "nats-2", "pid": 1}]
        acceptance.processes = {}
        process = mock.Mock(pid=2)
        with tempfile.TemporaryDirectory() as temporary:
            acceptance.fixture = Path(temporary)
            with mock.patch.object(fixture.subprocess, "Popen", return_value=process), \
                    mock.patch.object(fixture.ops.cf, "write", side_effect=OSError("failure")):
                with self.assertRaises(OSError):
                    acceptance.restart_broker(acceptance.brokers[0])
            self.assertIs(acceptance.processes["replacement-broker"], process)

    def test_broker_helpers_restore_shared_fixture_directory_after_failure(self):
        acceptance = fixture.LossAcceptance.__new__(fixture.LossAcceptance)
        acceptance.fixture = Path("/private/owned")
        previous = fixture.ops.cf.DIR
        with mock.patch.object(fixture.ops.cf, "broker_alive", side_effect=RuntimeError("failure")):
            with self.assertRaises(RuntimeError):
                acceptance.broker_alive({"name": "nats-2", "pid": 1})
        self.assertEqual(fixture.ops.cf.DIR, previous)


if __name__ == "__main__":
    unittest.main()
