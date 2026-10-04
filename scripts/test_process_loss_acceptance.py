#!/usr/bin/env python3
"""Negative controls for process-loss evidence and owned fault injection."""
import copy
from concurrent.futures import Future
import json
from pathlib import Path
import signal
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import process_loss_acceptance as fixture
from test_lease_supervision import recovered_evidence, unfenced_evidence


def valid_report():
    checks = [{"test": name, "passed": True} for name in fixture.REQUIRED]
    for check in checks:
        if check['test'] == fixture.REQUIRED[3]:
            check['lease_supervision'] = unfenced_evidence()
        if check["test"] in fixture.REQUIRED[1:-1]:
            check.update(running_before_fault=2, queued_before_fault=2,
                         surviving_tenant_exact_result=True, failed_attempt_result_rejected=True,
                         queued_handles_preserved=True)
        elif check["test"] == "cleanup":
            check.update(forced_application_kills=0, observed_live_descendants=0,
                         worker_scratch_directories=0, original_configurations_verified=True,
                         all_owned_brokers_healthy=True)
    return {"interrupted": False, "checks": checks}


def ready_observation():
    return {"passed": True, "timeout_seconds": fixture.WORKER_PREFAULT_TIMEOUT,
            "attempts": 2, "zero_observations": 1, "read_failures": 0,
            "elapsed_seconds": .05, "inherited_sandbox_lease_children": 1}


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

    def test_broker_recovery_requires_complete_supervision_evidence(self):
        for evidence in (None, {}, recovered_evidence()):
            report = valid_report()
            report['checks'][3]['lease_supervision'] = evidence
            self.assertEqual(fixture.reconcile(report), evidence == recovered_evidence())
        evidence = recovered_evidence()
        evidence['nodes'][0]['replacement_ready_seconds'] = None
        report['checks'][3]['lease_supervision'] = evidence
        self.assertFalse(fixture.reconcile(report))

    def test_broker_restoration_survives_supervisor_shutdown_failure(self):
        acceptance = fixture.LossAcceptance.__new__(fixture.LossAcceptance)
        acceptance.prepare_load = mock.Mock(return_value={})
        acceptance.brokers = [{'name': 'nats-2', 'pid': 17}]
        acceptance.broker_alive = mock.Mock(return_value=True)
        acceptance.wait = mock.Mock(side_effect=fixture.ops.AcceptanceError('TEST_GATE_FAILURE'))
        acceptance.restart_broker = mock.Mock()
        acceptance.intentional_kills = 0
        supervisor = mock.Mock()
        supervisor.stop.side_effect = fixture.ops.AcceptanceError('SUPERVISOR_DID_NOT_STOP')
        supervisor.evidence.return_value = {'errors': ['SUPERVISOR_DID_NOT_STOP']}
        with mock.patch.object(fixture.lease_supervision, 'LeaseSupervisor', return_value=supervisor), \
             mock.patch.object(fixture.ops, 'proc_identity', return_value=(17, 'owned')), \
             mock.patch.object(fixture.ops, 'signal_owned'):
            with self.assertRaisesRegex(fixture.ops.AcceptanceError, 'SUPERVISOR_DID_NOT_STOP'):
                acceptance.broker_loss()
        acceptance.restart_broker.assert_called_once_with(acceptance.brokers[0])
        self.assertEqual(acceptance.broker_supervision, supervisor.evidence.return_value)

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
        self.assertFalse(fixture.reconcile(report))
        report["worker_prefault_observation"] = ready_observation()
        self.assertTrue(fixture.reconcile(report))
        for key, invalid in (("passed", False), ("timeout_seconds", 5), ("attempts", 0),
                             ("zero_observations", 2), ("read_failures", 1),
                             ("elapsed_seconds", 4.001), ("elapsed_seconds", float("nan")),
                             ("inherited_sandbox_lease_children", 2)):
            changed = copy.deepcopy(report)
            changed["worker_prefault_observation"][key] = invalid
            self.assertFalse(fixture.reconcile(changed), key)

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

    def test_prefault_identity_and_deadline_are_checked_before_signalling(self):
        acceptance = fixture.LossAcceptance.__new__(fixture.LossAcceptance)
        process = mock.Mock(pid=17)
        process.poll.return_value = None
        acceptance.processes = {"a1": process}
        acceptance.intentional_kills = 0
        for identity, deadline, category in (((17, "changed"), 5, "FAULT_TARGET_IDENTITY_CHANGED"),
                                             ((17, "start"), 1, "SANDBOX_WORKER_LEASE_READINESS_DEADLINE")):
            with self.subTest(category=category), \
                    mock.patch.object(fixture.ops, "proc_identity", return_value=identity), \
                    mock.patch.object(fixture.time, "monotonic", return_value=1), \
                    mock.patch.object(fixture.ops, "signal_owned") as kill:
                with self.assertRaisesRegex(fixture.ops.AcceptanceError, category):
                    acceptance.kill_application("a1", expected_identity=(17, "start"), deadline=deadline)
                kill.assert_not_called()
        self.assertEqual(acceptance.intentional_kills, 0)

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


class WorkerReadinessTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.
        self.node, self.child = (17, "node-start"), (18, "child-start")
        self.acceptance = acceptance = fixture.LossAcceptance.__new__(fixture.LossAcceptance)
        process = mock.Mock(pid=self.node[0])
        process.poll.return_value = None
        acceptance.processes = {"a1": process}
        acceptance.managed_roots = {"a1": Path("/owned/a1/managed")}
        acceptance.samples = mock.Mock(proc_root=Path("/proc"), cgroup_root=Path("/sys/fs/cgroup"))
        acceptance.samples.owned = {self.node[0]: self.node}
        acceptance.samples.errors = set()
        acceptance.samples.lock = None
        acceptance.worker_prefault_observation = None
        self.handles = {tenant: {"running": tenant+"-run", "queued": tenant+"-queued", "future": Future()}
                        for tenant in ("a", "b")}
        acceptance.prepare_load = mock.Mock(return_value=self.handles)
        acceptance.kill_application = mock.Mock()
        acceptance.state = mock.Mock(side_effect=AssertionError("Unbounded status helper called"))
        self.states = {tenant+suffix: state for tenant in ("a", "b")
                       for suffix, state in (("-run", "running"), ("-queued", "queued"))}
        acceptance.call = mock.Mock(side_effect=self.status)
        acceptance._lease_child_observation = mock.Mock(side_effect=self.leases)
        self.snapshot_values = [({self.node[0]: self.node, self.child[0]: self.child}, set())]
        self.snapshots = []
        for patcher in (mock.patch.object(fixture.ops, "Samples", side_effect=self.snapshot),
                        mock.patch.object(fixture.ops, "proc_identity", return_value=self.node),
                        mock.patch.object(fixture.time, "monotonic", side_effect=lambda: self.now),
                        mock.patch.object(fixture.time, "sleep", side_effect=self.advance)):
            patcher.start()
            self.addCleanup(patcher.stop)

    def advance(self, seconds):
        self.now += seconds

    def status(self, path, tenant, timeout):
        self.assertGreater(timeout, 0)
        self.assertLessEqual(timeout, .5)
        self.assertTrue(path.startswith("/v1/queries/"+tenant+"-"))
        return 200, json.dumps({"state": self.states[path.rsplit("/", 1)[1]]}).encode()

    def leases(self, name, identities):
        self.assertEqual(name, "a1")
        return {"identities": [self.child] if self.child in identities else [], "read_failures": 0}

    def snapshot(self, processes, proc_root, cgroup_root):
        self.assertEqual(processes, self.acceptance.processes)
        self.assertEqual(proc_root, Path("/proc"))
        self.assertEqual(cgroup_root, Path("/sys/fs/cgroup"))
        owned, errors = self.snapshot_values[min(len(self.snapshots), len(self.snapshot_values)-1)]
        snapshot = mock.Mock(owned=dict(owned), errors=set(errors))
        self.snapshots.append(snapshot)
        return snapshot

    def assert_no_fault(self, category):
        with self.assertRaisesRegex(fixture.ops.AcceptanceError, category):
            self.acceptance.worker_loss()
        self.acceptance.kill_application.assert_not_called()
        detail = self.acceptance.worker_prefault_observation
        self.assertFalse(detail["passed"])
        self.assertEqual(detail["failure_category"], category)
        self.assertGreaterEqual(detail["elapsed_seconds"], 0)
        return detail

    def test_zero_then_live_child_resamples_and_rechecks_original_handles(self):
        self.snapshot_values.insert(0, ({self.node[0]: self.node}, set()))
        before, count, target, deadline = self.acceptance.wait_worker_child(self.handles)
        self.assertEqual((before, count, target, deadline), ({self.node, self.child}, 1, self.node, 4.))
        self.assertEqual(len(self.snapshots), 2)
        for snapshot in self.snapshots:
            snapshot.sample.assert_called_once_with()
        calls = self.acceptance._lease_child_observation.call_args_list
        self.assertEqual(calls[0], mock.call("a1", []))
        self.assertEqual(calls[-1], mock.call("a1", [self.child]))
        self.assertEqual(self.acceptance.call.call_count, 12)
        self.assertEqual(self.acceptance.worker_prefault_observation, {
            **ready_observation(),
            "read_failure_scope": "Surfaced status, lease and descendant-sampling failures; sampling categories counted once per attempt",
            "scope": "An owned managed sandbox child; not attributed to a particular running query"})
        self.acceptance.state.assert_not_called()

    def test_persistent_zero_expires_without_a_fault(self):
        self.snapshot_values = [({self.node[0]: self.node}, set())]
        detail = self.assert_no_fault("SANDBOX_WORKER_LEASE_READINESS_DEADLINE")
        self.assertEqual(detail["elapsed_seconds"], fixture.WORKER_PREFAULT_TIMEOUT)
        self.assertEqual(detail["zero_observations"], detail["attempts"])
        self.assertLessEqual(detail["attempts"], 81)
        self.assertEqual(len(self.snapshots), detail["attempts"])

    def test_changed_original_running_or_queued_state_prevents_fault(self):
        for handle in self.states:
            with self.subTest(handle=handle):
                previous = self.states[handle]
                self.states[handle] = "completed"
                self.assert_no_fault("PREFAULT_MIXED_STATES_NOT_OBSERVED")
                self.states[handle] = previous

    def test_either_completed_result_future_prevents_fault(self):
        for tenant in self.handles:
            with self.subTest(tenant=tenant):
                self.handles[tenant]["future"].set_result(None)
                self.assert_no_fault("PREFAULT_MIXED_STATES_NOT_OBSERVED")
                self.handles[tenant]["future"] = Future()

    def test_future_finishing_during_final_other_tenant_status_prevents_fault(self):
        def status(path, tenant, timeout):
            if self.acceptance.call.call_count == 8:
                self.handles["a"]["future"].set_result(None)
            return self.status(path, tenant, timeout)
        self.acceptance.call.side_effect = status
        self.assert_no_fault("PREFAULT_MIXED_STATES_NOT_OBSERVED")

    def test_state_change_during_final_recheck_prevents_fault(self):
        def status(path, tenant, timeout):
            if self.acceptance.call.call_count == 8:
                self.states["b-queued"] = "running"
            return self.status(path, tenant, timeout)
        self.acceptance.call.side_effect = status
        self.assert_no_fault("PREFAULT_MIXED_STATES_NOT_OBSERVED")

    def test_child_that_loses_lease_during_final_recheck_never_passes(self):
        def lease(name, identities):
            if self.acceptance._lease_child_observation.call_count % 2 == 0:
                return {"identities": [], "read_failures": 0}
            return self.leases(name, identities)
        self.acceptance._lease_child_observation.side_effect = lease
        detail = self.assert_no_fault("SANDBOX_WORKER_LEASE_READINESS_DEADLINE")
        self.assertEqual(detail["zero_observations"], detail["attempts"])
        self.assertEqual(self.acceptance._lease_child_observation.call_count, 2*detail["attempts"])

    def test_read_failures_are_sanitized_and_prevent_fault(self):
        cases = ((self.acceptance._lease_child_observation, {"identities": [self.child], "read_failures": 1},
                  "SANDBOX_WORKER_LEASE_OBSERVATION_FAILED"),
                 (self.acceptance.call, OSError("private endpoint details"), "PREFAULT_QUERY_STATUS_UNAVAILABLE"))
        for method, result, category in cases:
            with self.subTest(category=category):
                previous = method.side_effect
                method.side_effect = result if isinstance(result, Exception) else lambda *args: result
                detail = self.assert_no_fault(category)
                self.assertEqual(detail["read_failures"], 1)
                self.assertNotIn("private", json.dumps(detail))
                method.side_effect = previous

    def test_sample_exception_is_sanitized_and_prevents_fault(self):
        with mock.patch.object(fixture.ops, "Samples", side_effect=OSError("private process path")):
            detail = self.assert_no_fault("SANDBOX_WORKER_LEASE_OBSERVATION_FAILED")
        self.assertEqual(detail["read_failures"], 1)
        self.assertNotIn("private", json.dumps(detail))

    def test_fresh_repeated_sample_error_is_not_hidden_by_cumulative_history(self):
        category = "PROCESS_CHILDREN_SAMPLE_UNAVAILABLE"
        self.acceptance.samples.errors.add(category)
        self.snapshot_values[0][1].add(category)
        detail = self.assert_no_fault("SANDBOX_WORKER_LEASE_OBSERVATION_FAILED")
        self.assertEqual(detail["read_failures"], 1)
        self.assertIn(category, self.acceptance.samples.errors)

    def test_general_sampler_errors_stay_unchanged_by_scoped_cgroup_counters(self):
        historical = "PROCESS_SAMPLE_UNAVAILABLE"
        telemetry = "CGROUP_MEMORY_COUNTERS_UNAVAILABLE"
        self.acceptance.samples.errors.add(historical)
        self.snapshot_values[0][1].add(telemetry)
        self.acceptance.wait_worker_child(self.handles)
        self.assertEqual(self.acceptance.samples.errors, {historical})
        self.assertEqual(self.acceptance.worker_prefault_observation["read_failures"], 0)

    def test_shared_ownership_updates_and_snapshot_use_existing_sampler_lock(self):
        lock = mock.MagicMock()
        self.acceptance.samples.lock = lock
        before, _, _, _ = self.acceptance.wait_worker_child(self.handles)
        self.assertEqual(before, {self.node, self.child})
        self.assertEqual(lock.__enter__.call_count, 2)
        self.assertEqual(lock.__exit__.call_count, 2)

    def test_result_finishing_during_ownership_snapshot_lock_prevents_fault(self):
        lock = mock.MagicMock()
        def enter():
            if lock.__enter__.call_count == 2:
                self.handles["a"]["future"].set_result(None)
        lock.__enter__.side_effect = enter
        self.acceptance.samples.lock = lock
        self.assert_no_fault("PREFAULT_MIXED_STATES_NOT_OBSERVED")
        self.assertEqual(lock.__enter__.call_count, 2)

    def test_node_replacement_prevents_fault(self):
        with mock.patch.object(fixture.ops, "proc_identity", side_effect=[self.node, (17, "replacement")]):
            self.assert_no_fault("FAULT_TARGET_IDENTITY_CHANGED")

    def test_status_deadline_is_rechecked_after_the_read(self):
        def status(path, tenant, timeout):
            self.advance(fixture.WORKER_PREFAULT_TIMEOUT)
            return self.status(path, tenant, timeout)
        self.acceptance.call.side_effect = status
        self.assert_no_fault("SANDBOX_WORKER_LEASE_READINESS_DEADLINE")

    def test_last_moment_kill_guard_failure_is_retained_in_evidence(self):
        category = "FAULT_TARGET_IDENTITY_CHANGED"
        self.acceptance.kill_application.side_effect = fixture.ops.AcceptanceError(category)
        with self.assertRaisesRegex(fixture.ops.AcceptanceError, category):
            self.acceptance.worker_loss()
        detail = self.acceptance.worker_prefault_observation
        self.assertFalse(detail["passed"])
        self.assertEqual(detail["failure_category"], category)
        self.acceptance.kill_application.assert_called_once_with("a1", expected_identity=self.node, deadline=4.)

    def test_unmanaged_worker_loss_keeps_the_original_fault_path(self):
        self.acceptance.managed_roots = {}
        self.acceptance.kill_application.side_effect = RuntimeError("fault boundary")
        with self.assertRaisesRegex(RuntimeError, "fault boundary"):
            self.acceptance.worker_loss()
        self.acceptance.samples.sample.assert_called_once_with()
        self.acceptance.kill_application.assert_called_once_with("a1")
        self.assertIsNone(self.acceptance.worker_prefault_observation)
        self.assertEqual(self.snapshots, [])

    def test_legacy_one_argument_override_keeps_integer_contract(self):
        class Legacy(fixture.LossAcceptance):
            def inherited_lease_children(self, name):
                return super().inherited_lease_children(name)
        self.acceptance.__class__ = Legacy
        self.acceptance._lease_child_observation.side_effect = None
        for observation, expected in ((None, None), ({"identities": [], "read_failures": 0}, 0),
                                      ({"identities": [self.child], "read_failures": 0}, 1)):
            self.acceptance._lease_child_observation.return_value = observation
            self.assertEqual(self.acceptance.inherited_lease_children("a1"), expected)
        self.acceptance._lease_child_observation.side_effect = self.leases
        self.acceptance.inherited_lease_children = mock.Mock(side_effect=AssertionError("Legacy override called"))
        self.acceptance.wait_worker_child(self.handles)
        self.acceptance.inherited_lease_children.assert_not_called()


class LeaseObservationTests(unittest.TestCase):
    def test_live_owned_child_requires_exact_lease_inode_and_device(self):
        acceptance = fixture.LossAcceptance.__new__(fixture.LossAcceptance)
        identity = (18, "child-start")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            managed = root/"managed"
            managed.mkdir()
            scratch = managed/"kelvo-worker-owned"
            scratch.mkdir()
            lease = managed/".kelvo-lease-owned"
            lease.write_bytes(b"")
            proc = root/"proc"/"18"
            (proc/"fd").mkdir(parents=True)
            (proc/"cwd").symlink_to(scratch, target_is_directory=True)
            descriptor = proc/"fd"/"3"
            descriptor.symlink_to(lease)
            acceptance.samples = SimpleNamespace(proc_root=root/"proc", owned={18: identity})
            acceptance.managed_roots = {"a1": managed}
            current = lease.stat()
            original_stat = Path.stat
            for inode, device, expected in ((current.st_ino, current.st_dev, 1),
                                             (current.st_ino+1, current.st_dev, 0),
                                             (current.st_ino, current.st_dev+1, 0)):
                def stat(path, *args, **kwargs):
                    if path == descriptor and kwargs.get("follow_symlinks", True):
                        return SimpleNamespace(st_ino=inode, st_dev=device)
                    return original_stat(path, *args, **kwargs)
                with self.subTest(inode=inode, device=device), \
                        mock.patch.object(fixture.ops, "proc_identity", return_value=identity), \
                        mock.patch.object(Path, "stat", stat):
                    self.assertEqual(acceptance.inherited_lease_children("a1"), expected)
            with mock.patch.object(fixture.ops, "proc_identity", side_effect=[identity, (18, "reused")]):
                self.assertEqual(acceptance.inherited_lease_children("a1"), 0)
            with mock.patch.object(fixture.ops, "proc_identity", return_value=(18, "unowned")):
                self.assertEqual(acceptance.inherited_lease_children("a1"), 0)
            with mock.patch.object(fixture.ops, "proc_identity", return_value=identity), \
                    mock.patch.object(Path, "iterdir", side_effect=PermissionError("private path")):
                self.assertEqual(acceptance._lease_child_observation("a1"), {"identities": [], "read_failures": 1})
            (proc/"cwd").unlink()
            (proc/"cwd").symlink_to(root, target_is_directory=True)
            with mock.patch.object(fixture.ops, "proc_identity", return_value=identity):
                self.assertEqual(acceptance.inherited_lease_children("a1"), 0)


if __name__ == "__main__":
    unittest.main()
