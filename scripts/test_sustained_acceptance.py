#!/usr/bin/env python3
"""Negative controls: smoke, duration, missing/fault gates, ownership and limits."""
import copy
import errno
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import sustained_acceptance as fixture
from test_lease_supervision import recovered_evidence, unfenced_evidence

REVISION = "a" * 40


def valid_report():
    files = {name: "a" * 64 for name in fixture.identity.SOURCE_REQUIRED}
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    checks = [{"test": name, "passed": True} for name in fixture.REQUIRED]
    for check in checks:
        if check['test'] == 'broker_loss':
            check['lease_supervision'] = unfenced_evidence()
        if check["test"] in ("gateway_loss", "worker_loss", "broker_loss"):
            check.update(running_before_fault=2, queued_before_fault=2, surviving_tenant_exact_result=True,
                         failed_attempt_result_rejected=True, queued_handles_preserved=True)
        if check["test"] == "mixed_load":
            check["observed_seconds"] = 7200.
        if check["test"] in ("file_joins", "durable_exports"):
            check["counts"] = {tenant: 5 for tenant in fixture.TENANTS}
        if check["test"] == "cleanup":
            check.update(forced_application_kills=0, observed_live_descendants=0, worker_scratch_directories=0,
                         remaining_containment_records=0, remaining_export_entries=0, all_owned_brokers_stopped=True, original_configurations_verified=True)
    report = {"schema": 2, "revision": REVISION, "checks": checks, "requested_seconds": 7200., "mode": "sustained", "interrupted": False,
            "prerequisite_smoke_sha256": "d" * 64,
            "service_exit_code": 0, "source_unchanged": True, "owned_service_removed": True, "owned_cgroup_removed": True,
            "source": {"verified": True, "file_count": len(files), "files": files, "sha256": hashlib.sha256(canonical).hexdigest(),
                       "base_revision": REVISION, "excluded_metadata": [".DS_Store", "._*"]},
            "binary_sha256": {"kelvo": "b" * 64, "kelvo-landlock": "c" * 64},
            "workload": {kind: {tenant: 10 if kind == "export_downloads" else 5 for tenant in fixture.TENANTS} for kind in fixture.WORKLOAD_KINDS},
            "client_failures": [],
            "resources": {"samples": 7200, "counter_errors": [], "resource_violations": [], "peak_scratch_bytes": 1,
                          "cgroup_memory_events_delta": {"oom": 0, "oom_kill": 0}, "query_refresh_overlap_samples": 1, "final_cgroup_sample_after_shutdown": True},
            "progress_windows": [{"start_seconds": 0., "end_seconds": 7200., "deltas": {kind: {tenant: 10 if kind == "export_downloads" else 5 for tenant in fixture.TENANTS} for kind in fixture.WORKLOAD_KINDS}}]}
    report["build_source"] = copy.deepcopy(report["source"])
    report["control_response_counts"] = {"queued_status_http_200": 6}
    report["mixed_response_counts"] = {tenant: {stage + "_http_" + str(code): 10 if stage == "export_part" else 5
        for stage, code in (("join_submit", 201), ("join_status", 200), ("join_result", 200), ("export_submit", 201),
            ("export_status", 200), ("export_manifest", 200), ("export_part", 200), ("export_cancel", 200), ("export_withdrawn", 409),
            ("export_foreign_status", 404), ("export_foreign_manifest", 404), ("export_foreign_part", 404), ("export_foreign_cancel", 404))}
        for tenant in fixture.TENANTS}
    report["process_sampling"] = {"process_tree_rss_available": True, "samples": 100,
        "sampled_process_tree_rss_peak_bytes": 1024, "read_errors": [],
        "rss_source": fixture.PROCESS_RSS_SOURCE, "page_size_bytes": 4096}
    report["latencies"] = {}
    for kind, count_kind in fixture.LATENCY_COUNTS.items():
        report["latencies"][kind] = {}
        for tenant in fixture.TENANTS:
            observed = fixture.histogram()
            for _ in range(report["workload"][count_kind][tenant]):
                fixture.observe(observed, .01)
            report["latencies"][kind][tenant] = fixture.histogram_evidence(observed)
    return report


class SustainedControls(unittest.TestCase):
    def test_mixed_evidence_rejects_missing_or_extra_requests_and_lost_acknowledgements(self):
        for tenant in fixture.TENANTS:
            for stage in valid_report()["mixed_response_counts"][tenant]:
                for value in (0, -1, True, 1.5, "5", None):
                    report = valid_report()
                    report["mixed_response_counts"][tenant][stage] = value
                    self.assertFalse(fixture.reconcile(report, REVISION), (tenant, stage, value))
                report = valid_report()
                del report["mixed_response_counts"][tenant][stage]
                self.assertFalse(fixture.reconcile(report, REVISION), stage)
            for key in ("export_submit_transport_error", "export_submit_http_503", "join_submit_http_429", "private-source-label"):
                report = valid_report()
                report["mixed_response_counts"][tenant][key] = 1
                self.assertFalse(fixture.reconcile(report, REVISION), key)
            report = valid_report()
            report["mixed_response_counts"][tenant]["export_submit_http_201"] += 1
            self.assertFalse(fixture.reconcile(report, REVISION))
            report = valid_report()
            report["client_failures"] = ["OSError"]
            self.assertFalse(fixture.reconcile(report, REVISION))

    def test_mixed_totals_need_both_downloads_withdrawal_and_per_minute_progress(self):
        for kind in ("joins", "exports", "export_downloads", "export_withdrawals"):
            report = valid_report()
            report["progress_windows"][0]["deltas"][kind]["a"] += 1
            self.assertFalse(fixture.reconcile(report, REVISION), kind)
        for name in ("file_joins", "durable_exports"):
            report = valid_report()
            next(check for check in report["checks"] if check["test"] == name)["counts"]["a"] = 4
            self.assertFalse(fixture.reconcile(report, REVISION))
        report = valid_report()
        next(check for check in report["checks"] if check["test"] == "cleanup")["remaining_export_entries"] = 1
        self.assertFalse(fixture.reconcile(report, REVISION))

    def test_join_reference_depends_on_separate_dimension_and_tenant(self):
        rows = [{"tenant": "a", "bucket": bucket, "n": 10, "present": 9, "total": 20} for bucket in range(10)]
        original = copy.deepcopy(rows)
        a = fixture.join_expected(rows, "a")
        b = fixture.join_expected([{**row, "tenant": "b"} for row in rows], "b")
        self.assertEqual([row["bucket"] for row in a], [0, 1, 2, 3, 4, 5, 6, 8, 9])
        self.assertEqual([row["total"] for row in a], [60, 80, 100, 120, 140, 160, 180, 220, 240])
        self.assertEqual([row["total"] for row in b], [260, 280, 300, 320, 340, 360, 380, 420, 440])
        self.assertEqual(rows, original)

    def export_case(self):
        export_id = "e0-" + "a" * 32
        expected = [{"tenant": "a", "bucket": 0, "n": 10, "present": 9, "total": 60}]
        manifest = {"id": export_id, "schema_sha256": "b" * 64, "rows": 1, "encoded_bytes": 10, "decoded_bytes": 8,
            "parts": [{"index": 0, "rows": 1, "batches": 1, "encoded_bytes": 10, "decoded_bytes": 8, "sha256": "c" * 64}]}
        campaign = fixture.Campaign.__new__(fixture.Campaign)
        campaign.default_gateway = 1
        campaign.data_lock = fixture.threading.RLock()
        campaign.mixed_response_counts = {tenant: {} for tenant in fixture.TENANTS}
        campaign.workload = {kind: {tenant: 0 for tenant in fixture.TENANTS} for kind in fixture.WORKLOAD_KINDS}
        campaign.latencies = {kind: {tenant: fixture.histogram() for tenant in fixture.TENANTS} for kind in fixture.LATENCY_COUNTS}
        campaign.join_expected = {"a": expected}
        responses = [(201, json.dumps({"id": export_id}).encode()), (200, b'{"state":"ready"}'), (200, json.dumps(manifest).encode()),
            *[(404, b"hidden")] * 4, *[(200, b"same-arrow", {})] * 2, (200, b'{"state":"cancelled"}'), (409, b"withdrawn")]
        campaign.call = mock.Mock(side_effect=responses)
        return campaign, export_id, expected, manifest, responses

    def test_export_cycle_reuses_one_handle_across_gateways_and_never_resubmits_sql(self):
        campaign, export_id, expected, manifest, _ = self.export_case()
        with mock.patch.object(fixture, "verify_export_part") as verify:
            campaign.export_cycle("a")
        calls = campaign.call.call_args_list
        self.assertEqual(sum(call.args[0] == "/v1/exports" for call in calls), 1)
        self.assertTrue(all(export_id in call.args[0] for call in calls[1:]))
        self.assertEqual([call.kwargs["gateway"] for call in calls if "gateway" in call.kwargs], [1, 2])
        self.assertEqual(verify.call_args_list, [mock.call(b"same-arrow", {}, manifest, expected)] * 2)
        self.assertEqual([call.args[1] for call in calls[3:7]], ["b"] * 4)
        self.assertEqual(campaign.workload["exports"]["a"], 1)
        self.assertEqual(campaign.workload["export_downloads"]["a"], 2)
        self.assertEqual(campaign.workload["export_withdrawals"]["a"], 1)

    def test_export_loss_of_ack_terminal_state_and_changed_bytes_fail_without_replay(self):
        for index, replacement in ((0, OSError("lost acknowledgement")), (1, (200, b'{"state":"publication_uncertain"}')),
                                   (3, (200, b"foreign-visible")), (8, (200, b"changed-arrow", {})), (10, (200, b"still-readable"))):
            campaign, _, _, _, responses = self.export_case()
            responses[index] = replacement
            campaign.call.side_effect = responses
            with mock.patch.object(fixture, "verify_export_part"), self.assertRaises((OSError, fixture.ops.AcceptanceError)):
                campaign.export_cycle("a")
            self.assertEqual(campaign.call.call_count, index + 1)
            self.assertEqual(sum(call.args[0] == "/v1/exports" for call in campaign.call.call_args_list), 1)
            self.assertEqual(campaign.workload["exports"]["a"], 0)

    def test_export_manifest_rejects_unbounded_or_inconsistent_metadata(self):
        _, export_id, expected, valid, _ = self.export_case()
        self.assertEqual(fixture.verify_export_manifest(json.dumps(valid), export_id, expected), valid)
        for target, key, value in (("manifest", "id", "foreign"), ("manifest", "rows", True), ("manifest", "encoded_bytes", 11),
                                  ("manifest", "schema_sha256", "x" * 64), ("part", "rows", 2), ("part", "index", True),
                                  ("part", "batches", 2), ("part", "encoded_bytes", 2 << 20), ("part", "sha256", "")):
            manifest = copy.deepcopy(valid)
            (manifest if target == "manifest" else manifest["parts"][0])[key] = value
            with self.assertRaises(fixture.ops.AcceptanceError):
                fixture.verify_export_manifest(json.dumps(manifest), export_id, expected)
        for parts in (None, [], [valid["parts"][0]] * 2):
            with self.assertRaises(fixture.ops.AcceptanceError):
                fixture.verify_export_manifest(json.dumps({**valid, "parts": parts}), export_id, expected)

    def test_export_part_requires_manifest_digest_then_exact_completed_arrow(self):
        _, _, expected, manifest, _ = self.export_case()
        manifest["parts"][0].update(encoded_bytes=5, sha256=hashlib.sha256(b"arrow").hexdigest())
        with mock.patch.object(fixture.ops, "verify_completed_arrow") as verify:
            fixture.verify_export_part(b"arrow", {"completion": "fixture"}, manifest, expected)
            verify.assert_called_once_with(b"arrow", {"completion": "fixture"}, expected)
        for raw in (b"truncated", b"arrox"):
            with mock.patch.object(fixture.ops, "verify_completed_arrow") as verify, self.assertRaises(fixture.ops.AcceptanceError):
                fixture.verify_export_part(raw, {}, manifest, expected)
            verify.assert_not_called()

    def test_export_cleanup_counts_entries_and_retains_partial_publication_markers(self):
        with tempfile.TemporaryDirectory() as directory:
            campaign = fixture.Campaign.__new__(fixture.Campaign)
            root = Path(directory)
            campaign.export_roots = {"a1": root}
            for name in (".lock", "store.yml"):
                (root / name).touch()
            self.assertEqual(campaign.remaining_export_entries(), 0)
            (root / ("a" * 32)).mkdir()
            (root / ("a" * 32 + ".deleting.yml")).touch()
            self.assertEqual(campaign.remaining_export_entries(), 2)

    def containment_campaign(self, directory):
        campaign = fixture.Campaign.__new__(fixture.Campaign)
        campaign.directory = directory
        campaign.containment_states = set()
        campaign.containment_roots = {}
        return campaign

    def test_partial_startup_containment_only_allows_never_created_directories(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.containment_campaign(Path(temporary))
            self.assertEqual(campaign.remaining_containment_records(), 0)
            state = campaign.directory / "a1-containment"
            state.mkdir()
            campaign.containment_states.add("a1")
            (state / ".kelvo-containment.lock").touch()
            self.assertEqual(campaign.remaining_containment_records(), 0)
            (state / "pending.json").touch()
            self.assertEqual(campaign.remaining_containment_records(), 1)
            (state / "pending.json").unlink()
            (state / ".kelvo-containment.lock").unlink()
            state.rmdir()
            with self.assertRaisesRegex(fixture.ops.AcceptanceError, "CONTAINMENT_STATE_DISAPPEARED"):
                campaign.remaining_containment_records()

    def test_partial_startup_containment_rejects_links_files_and_read_errors(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.containment_campaign(Path(temporary))
            state = campaign.directory / "a1-containment"
            state.touch()
            with self.assertRaisesRegex(fixture.ops.AcceptanceError, "CONTAINMENT_STATE_NOT_DIRECTORY"):
                campaign.remaining_containment_records()
            state.unlink()
            state.symlink_to(campaign.directory, target_is_directory=True)
            with self.assertRaisesRegex(fixture.ops.AcceptanceError, "CONTAINMENT_STATE_NOT_DIRECTORY"):
                campaign.remaining_containment_records()
            state.unlink()
            with mock.patch.object(Path, "lstat", side_effect=PermissionError()), self.assertRaises(PermissionError):
                campaign.remaining_containment_records()
            state.mkdir()
            with mock.patch.object(Path, "iterdir", side_effect=FileNotFoundError()), self.assertRaises(FileNotFoundError):
                campaign.remaining_containment_records()

    def test_partial_startup_cleanup_still_stops_owned_brokers_and_samples_cgroup(self):
        with tempfile.TemporaryDirectory() as temporary:
            campaign = self.containment_campaign(Path(temporary))
            campaign.stop_clients = fixture.threading.Event()
            campaign.monitor_stop = fixture.threading.Event()
            campaign.monitor_thread = None
            campaign.group = campaign.directory
            campaign.initial_events = {"oom": 0, "oom_kill": 0}
            campaign.resource_samples = {"peak_cgroup_bytes": 0}
            (campaign.group / "memory.events").write_text("oom 0\noom_kill 0\n")
            (campaign.group / "memory.peak").write_text("1024\n")
            (campaign.group / "cpu.stat").write_text("usage_usec 100\n")
            campaign.brokers = [{"pid": 123}]
            campaign.broker_alive = mock.Mock(return_value=True)
            campaign.wait = lambda predicate, timeout: self.assertTrue(predicate())
            with mock.patch.object(fixture.loss.LossAcceptance, "cleanup", return_value={"parent_cleanup": True}), \
                    mock.patch.object(fixture.ops, "proc_identity", side_effect=[(123, "456"), None]), \
                    mock.patch.object(fixture.ops, "signal_owned") as signal_owned, \
                    mock.patch.object(fixture.os, "waitpid", return_value=(123, 0)):
                result = campaign.cleanup()
            signal_owned.assert_called_once_with((123, "456"), fixture.signal.SIGTERM)
            self.assertEqual(result, {"parent_cleanup": True, "remaining_containment_records": 0,
                                      "remaining_export_entries": 0, "all_owned_brokers_stopped": True})
            self.assertTrue(campaign.resource_samples["final_cgroup_sample_after_shutdown"])
            self.assertEqual(campaign.resource_samples["peak_cgroup_bytes"], 1024)
            report = valid_report()
            next(check for check in report["checks"] if check["test"] == "startup")["passed"] = False
            self.assertFalse(fixture.reconcile(report, REVISION))

    def test_owned_service_watchdog_bounds_controller_loss_without_shortening_workload(self):
        for duration, mode, watchdog in ((60., "smoke", 360), (600., "smoke", 900),
                                        (600.1, "smoke", 901), (7200., "sustained", 7500),
                                        (14400., "sustained", 14700)):
            with self.subTest(duration=duration, mode=mode):
                args = fixture.argparse.Namespace(duration=duration, mode=mode, expected_revision=REVISION,
                    binary="/fixture/bin/kelvo", sandbox="/fixture/bin/kelvo-landlock", go="/fixture/go",
                    output=Path("/fixture/report.json"), nats_archive="/fixture/nats.tar.gz")
                command = fixture.service_command(args, "kelvo-sustained-fixture", Path("/fixture/artifacts"))
                self.assertEqual([part for part in command if part.startswith("--property=RuntimeMaxSec=")],
                                 [f"--property=RuntimeMaxSec={watchdog}"])
                self.assertEqual(command[command.index("--duration") + 1], str(duration))
                self.assertEqual(command[command.index("--mode") + 1], mode)
                self.assertEqual(command[command.index("--expected-revision") + 1], REVISION)
                self.assertEqual(command[-2:], ["--nats-archive", "/fixture/nats.tar.gz"])
                for property_value in ("Delegate=yes", "PrivateNetwork=yes", "CPUQuota=200%", "MemoryMax=6G",
                                       "MemorySwapMax=0", "TasksMax=512", "NoNewPrivileges=yes",
                                       "CapabilityBoundingSet=", "AmbientCapabilities=", "TimeoutStopSec=20",
                                       "KillMode=control-group", "SendSIGKILL=yes"):
                    property_name = property_value.split("=", 1)[0]
                    self.assertEqual([part for part in command if part.startswith("--property=" + property_name + "=")],
                                     ["--property=" + property_value])

    def test_broker_gate_requires_completed_lease_supervision(self):
        for evidence in (None, {}, recovered_evidence()):
            report = valid_report()
            next(check for check in report['checks'] if check['test'] == 'broker_loss')['lease_supervision'] = evidence
            self.assertEqual(fixture.reconcile(report, REVISION), evidence == recovered_evidence())
        report = valid_report()
        evidence = recovered_evidence()
        evidence['nodes'][0]['replacement_ready_seconds'] = None
        next(check for check in report['checks'] if check['test'] == 'broker_loss')['lease_supervision'] = evidence
        self.assertFalse(fixture.reconcile(report, REVISION))

    def test_control_observations_require_bounded_labels_and_integer_counts(self):
        for controls in (None, {}, {"queued_status_http_200": True}, {"queued_status_http_200": 1.0},
                         {"queued_status_http_200": 1, "private-key": 1},
                         {"queued_status_http_200": 1, "queued_status_http_503": -1}):
            report = valid_report()
            report["control_response_counts"] = controls
            self.assertFalse(fixture.reconcile(report, REVISION))

    def queued_campaign(self, responses):
        campaign = fixture.Campaign.__new__(fixture.Campaign)
        campaign.call = mock.Mock(side_effect=responses)
        campaign.count_control = mock.Mock()
        campaign.resources = mock.Mock(return_value={"Active": 0, "BackgroundActive": 0})
        campaign.expected = {"a": [{"marker": 1}]}
        return campaign

    def test_fault_status_retry_keeps_original_handle_and_single_result_claim(self):
        campaign = self.queued_campaign([(503, b"unavailable"), (429, b"busy"),
                                        (200, b'{"state":"queued"}'), (200, b'{"state":"assigned"}'),
                                        (200, b"exact-arrow", {"completion": "complete"})])
        with mock.patch.object(fixture.ops, "verify_completed_arrow") as verify, \
                mock.patch.object(fixture.time, "sleep"):
            campaign.queued_result({"queued": "original-handle"}, "a")
        self.assertEqual(campaign.call.call_args_list, [
            *[mock.call("/v1/queries/original-handle", "a", timeout=2)] * 4,
            mock.call("/v1/queries/original-handle/results", "a", timeout=30, with_headers=True)])
        self.assertEqual(campaign.count_control.call_args_list, [
            mock.call("queued_status_http_503"), mock.call("queued_status_http_429"),
            mock.call("queued_status_http_200"), mock.call("queued_status_http_200")])
        verify.assert_called_once_with(b"exact-arrow", {"completion": "complete"}, [{"marker": 1}])

    def test_fault_status_does_not_retry_denial_missing_or_terminal_handles(self):
        for response in ((401, b"denied"), (403, b"denied"), (404, b"missing"), (500, b"error"),
                         (200, b'{"state":"failed"}'), (200, b'{"state":"succeeded"}'),
                         (200, b'{"state":"cancelled"}'), (200, b'{"state":"unknown"}')):
            with self.subTest(response=response):
                campaign = self.queued_campaign([response])
                with self.assertRaises(fixture.ops.AcceptanceError):
                    campaign.queued_result({"queued": "original-handle"}, "a")
                self.assertEqual(campaign.call.call_count, 1)
        campaign = self.queued_campaign([(200, b"not-json")])
        with self.assertRaises(json.JSONDecodeError):
            campaign.queued_result({"queued": "original-handle"}, "a")
        self.assertEqual(campaign.call.call_count, 1)

    def test_fault_status_unavailability_still_expires_without_result_claim(self):
        campaign = self.queued_campaign([(503, b"unavailable")])
        with mock.patch.object(fixture.time, "monotonic", side_effect=[0., 0., 0., 0., 0., 26.]), \
                mock.patch.object(fixture.time, "sleep"), \
                self.assertRaisesRegex(fixture.ops.AcceptanceError, "CONDITION_DEADLINE"):
            campaign.queued_result({"queued": "original-handle"}, "a")
        self.assertEqual(campaign.call.call_args_list, [mock.call("/v1/queries/original-handle", "a", timeout=2)])

    def test_fault_status_late_positive_cannot_extend_deadline(self):
        campaign = self.queued_campaign([(200, b'{"state":"assigned"}')])
        with mock.patch.object(fixture.time, "monotonic", side_effect=[0., 0., 0., 24., 26.]), \
                self.assertRaisesRegex(fixture.ops.AcceptanceError, "CONDITION_DEADLINE"):
            campaign.queued_result({"queued": "original-handle"}, "a")
        campaign.call.assert_called_once_with("/v1/queries/original-handle", "a", timeout=1.)

    def test_fault_status_transport_failures_are_counted(self):
        campaign = self.queued_campaign([OSError("closed"), (200, b'{"state":"assigned"}'),
                                        (200, b"exact-arrow", {})])
        with mock.patch.object(fixture.ops, "verify_completed_arrow"), \
                mock.patch.object(fixture.time, "sleep"):
            campaign.queued_result({"queued": "original-handle"}, "a")
        self.assertEqual(campaign.count_control.call_args_list, [
            mock.call("queued_status_transport_error"), mock.call("queued_status_http_200")])

    def process_stat(self, pid=123, state='S', start='456', rss='1', comm='fixture worker'):
        return f'{pid} ({comm}) ' + ' '.join([state, *(['0'] * 18), start, '4096', rss])

    def test_rss_identity_distinguishes_exit_and_missing_from_malformed_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            process = root / "123"
            self.assertEqual(fixture.sampled_process(123, 4096, root), (None, None, "disappeared"))
            process.mkdir()
            for state, expected in (("S", ((123, "456"), 4096, "live")),
                                    ("Z", (None, None, "exited")), ("X", (None, None, "exited")),
                                    ("x", (None, None, "exited"))):
                (process / "stat").write_text(self.process_stat(state=state, comm='name with ( ) brackets'))
                self.assertEqual(fixture.sampled_process(123, 4096, root), expected)
            for invalid in ("malformed", self.process_stat(pid=124), "123 (short) S 0"):
                (process / "stat").write_text(invalid)
                self.assertEqual(fixture.sampled_process(123, 4096, root), (None, None, "read_error"))
            (process / "stat").unlink()
            (process / "stat").mkdir()
            self.assertEqual(fixture.sampled_process(123, 4096, root), (None, None, "read_error"))

    def sampler_fixture(self, root, page_size=4096):
        process = root / '123'
        (process / 'task/123').mkdir(parents=True)
        (process / 'stat').write_text(self.process_stat())
        (process / 'task/123/children').write_text('')
        with mock.patch.object(fixture.os, 'sysconf', return_value=page_size):
            sampler = fixture.LockedSamples({'a1': mock.Mock(pid=123, poll=lambda: None)})
        sampler.proc_root = root
        return sampler

    def test_stat_rss_pages_convert_using_cached_validated_page_size(self):
        for page_size in (4096, 65536):
            for pages in (0, 3, fixture.PROCESS_RSS_MAX_BYTES // page_size):
                with self.subTest(page_size=page_size, pages=pages), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    sampler = self.sampler_fixture(root, page_size)
                    (root / '123/stat').write_text(self.process_stat(rss=str(pages)))
                    with mock.patch.object(fixture.os, 'sysconf', side_effect=AssertionError('page size must be cached')):
                        sampler.sample()
                    self.assertEqual(sampler.peak_rss, pages * page_size)
                    self.assertTrue(sampler.rss_available)
                    self.assertEqual(sampler.evidence()['rss_source'], fixture.PROCESS_RSS_SOURCE)
                    self.assertEqual(sampler.evidence()['page_size_bytes'], page_size)
                    self.assertEqual(sampler.evidence()['read_errors'], [])
        for invalid in (True, 0, -1, 3, 4096., '4096', 1 << 64):
            with self.subTest(page_size=invalid), mock.patch.object(fixture.os, 'sysconf', return_value=invalid):
                with self.assertRaises(ValueError):
                    fixture.LockedSamples({})

    def test_one_stat_read_binds_rss_to_identity_even_when_pid_is_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sampler = self.sampler_fixture(root)
            stat = root / '123/stat'
            reads = []
            original = Path.read_text
            def observed(path, *args, **kwargs):
                if path.name == 'status':
                    raise AssertionError('independent status read is forbidden')
                raw = original(path, *args, **kwargs)
                if path == stat:
                    reads.append(path)
                    stat.write_text(self.process_stat(start='457', rss='9'))
                return raw
            with mock.patch.object(Path, 'read_text', observed):
                sampler.sample()
                self.assertEqual(sampler.owned, {123: (123, '456')})
                self.assertEqual(sampler.peak_rss, 4096)
                self.assertEqual(len(reads), 1)
                sampler.sample()
            self.assertEqual(sampler.owned, {123: (123, '457')})
            self.assertEqual(sampler.peak_rss, 9 * 4096)
            self.assertEqual(len(reads), 2)
            self.assertEqual(sampler.evidence()['read_errors'], [])

    def test_proc_exit_errno_and_permission_errors_at_each_operation(self):
        for operation in ('stat', 'tasks', 'children'):
            for error_number in (errno.ENOENT, errno.ESRCH, errno.EACCES, errno.EPERM, errno.EIO):
                with self.subTest(operation=operation, errno=error_number), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    sampler = self.sampler_fixture(root)
                    target = root / '123' / {'stat': 'stat', 'tasks': 'task', 'children': 'task/123/children'}[operation]
                    original = Path.iterdir if operation == 'tasks' else Path.read_text
                    def observed(path, *args, **kwargs):
                        if path == target:
                            raise OSError(error_number, 'PRIVATE_DIAGNOSTIC_NOT_FOR_REPORT')
                        return original(path, *args, **kwargs)
                    with mock.patch.object(Path, 'iterdir' if operation == 'tasks' else 'read_text', observed):
                        sampler.sample()
                        sampler.sample()
                    evidence = sampler.evidence()
                    disappeared = error_number in (errno.ENOENT, errno.ESRCH)
                    self.assertEqual(evidence['read_errors'], [] if disappeared else ['PROCESS_IDENTITY_READ_FAILED' if operation == 'stat' else 'PROCESS_READ_FAILED'])
                    self.assertEqual(evidence['disappeared_or_exited_process_or_thread_samples'], 2 if disappeared else 0)
                    self.assertEqual(len(evidence['read_diagnostics']), 1)
                    diagnostic = evidence['read_diagnostics'][0]
                    self.assertEqual((diagnostic['operation'], diagnostic['outcome'], diagnostic['errno'], diagnostic['count']),
                                     (operation, 'disappeared' if disappeared else 'oserror', error_number, 2))
                    self.assertLessEqual(diagnostic['first_elapsed_seconds'], diagnostic['last_elapsed_seconds'])
                    self.assertNotIn('PRIVATE_DIAGNOSTIC', json.dumps(evidence))
                    self.assertNotIn(str(root), json.dumps(evidence))

    def test_malformed_proc_data_remains_a_hard_failure(self):
        bad_stat = ['malformed', self.process_stat(pid=124), self.process_stat(state='?'),
                    self.process_stat().rsplit(' ', 1)[0]]
        for field in ('rss', 'start'):
            bad_stat.extend(self.process_stat(**{field: value}) for value in
                ('-1', '+1', 'nonsense', '\u00b2', '9' * 5000, str(1 << 64)))
        bad_stat.extend((self.process_stat(rss=str(fixture.PROCESS_RSS_MAX_BYTES // 4096 + 1)),
                         self.process_stat(state='Z', rss='nonsense')))
        cases = [('stat', value) for value in bad_stat] + [
                 ('task/123/children', '-123'), ('task/123/children', 'not-a-pid'),
                 ('task/123/children', '\u00b2'), ('task/123/children', '9' * 5000), ('task/123/children', str(1 << 31))]
        for name, payload in cases:
            with self.subTest(name=name, payload=payload), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                sampler = self.sampler_fixture(root)
                (root / '123' / name).write_text(payload)
                sampler.sample()
                evidence = sampler.evidence()
                self.assertEqual(evidence['read_errors'], ['PROCESS_IDENTITY_READ_FAILED' if name == 'stat' else 'PROCESS_READ_FAILED'])
                self.assertEqual(evidence['read_diagnostics'][0]['outcome'], 'malformed')
                self.assertEqual(evidence['disappeared_or_exited_process_or_thread_samples'], 0)

    def test_non_utf8_stat_is_a_hard_failure_without_status_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sampler = self.sampler_fixture(root)
            (root / '123/stat').write_bytes(b'123 (invalid\xff) S 0')
            sampler.sample()
            evidence = sampler.evidence()
            self.assertEqual(evidence['read_errors'], ['PROCESS_IDENTITY_READ_FAILED'])
            self.assertEqual(evidence['read_diagnostics'][0]['outcome'], 'malformed')

    @unittest.skipUnless(sys.platform == 'linux', 'Linux procfs zombie semantics')
    def test_real_linux_zombie_retains_numeric_zero_rss_before_reaping(self):
        process = subprocess.Popen(['/bin/true'])
        try:
            stat = Path('/proc') / str(process.pid) / 'stat'
            deadline = time.monotonic() + 3
            while True:
                fields = stat.read_text().rsplit(') ', 1)[1].split()
                if fields[0] == 'Z':
                    break
                if time.monotonic() >= deadline:
                    self.fail('child did not become a zombie within deadline')
                time.sleep(.01)
            self.assertEqual(fields[21], '0')
            sampler = fixture.LockedSamples({'child': mock.Mock(pid=process.pid, poll=lambda: None)})
            sampler.sample()
            evidence = sampler.evidence()
            self.assertEqual(evidence['read_errors'], [])
            self.assertEqual(evidence['disappeared_or_exited_process_or_thread_samples'], 1)
            self.assertEqual(evidence['read_diagnostics'][0]['outcome'], 'exited')
            process.wait(timeout=3)
            self.assertEqual(fixture.sampled_process(process.pid, sampler.page_size), (None, None, 'disappeared'))
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=3)

    @unittest.skipUnless(sys.platform == 'linux', 'Linux procfs exit semantics')
    def test_open_proc_descriptors_after_child_exit_report_esrch(self):
        process = subprocess.Popen(['/bin/sleep', '10'])
        handles = {}
        try:
            root = Path('/proc') / str(process.pid)
            for name in ('stat',):
                handles[name] = (root / name).open()
            process.terminate()
            process.wait(timeout=3)
            sampler = fixture.LockedSamples({})
            original = Path.read_text
            def opened_before_exit(path, *args, **kwargs):
                if path.parent == root and path.name in handles:
                    return handles[path.name].read()
                return original(path, *args, **kwargs)
            with mock.patch.object(Path, 'read_text', opened_before_exit):
                self.assertEqual(fixture.sampled_process(process.pid, sampler.page_size, diagnostic=sampler.diagnostic),
                                 (None, None, 'disappeared'))
            evidence = sampler.evidence()
            self.assertEqual(evidence['read_errors'], [])
            self.assertEqual(evidence['disappeared_or_exited_process_or_thread_samples'], 1)
            self.assertEqual({(item['operation'], item['errno']) for item in evidence['read_diagnostics']}, {('stat', errno.ESRCH)})
        finally:
            for handle in handles.values():
                handle.close()
            if process.poll() is None:
                process.kill()
            process.wait(timeout=3)

    def test_diagnostics_are_bounded_without_hiding_failures_or_aliasing(self):
        sampler = fixture.LockedSamples({})
        for error_number in range(100, 100 + fixture.PROCESS_DIAGNOSTIC_LIMIT + 5):
            sampler.diagnostic('children', 'oserror', error_number)
        evidence = sampler.evidence()
        self.assertEqual(len(evidence['read_diagnostics']), fixture.PROCESS_DIAGNOSTIC_LIMIT)
        self.assertEqual(evidence['read_diagnostic_overflow_events'], 5)
        self.assertEqual(evidence['read_errors'], ['PROCESS_READ_FAILED'])
        evidence['read_diagnostics'][0]['operation'] = 'changed'
        self.assertEqual(sampler.evidence()['read_diagnostics'][0]['operation'], 'children')

    def test_baseline_and_every_named_gate(self):
        self.assertTrue(fixture.reconcile(valid_report(), REVISION))
        for name in fixture.REQUIRED:
            report = valid_report()
            report["checks"] = [item for item in report["checks"] if item["test"] != name]
            self.assertFalse(fixture.reconcile(report, REVISION), name)
            for state in (False, "skip", "pass", 1, None):
                report = valid_report()
                next(item for item in report["checks"] if item["test"] == name)["passed"] = state
                self.assertFalse(fixture.reconcile(report, REVISION), (name, state))
        report = valid_report()
        report["checks"].append(copy.deepcopy(report["checks"][0]))
        self.assertFalse(fixture.reconcile(report, REVISION))

    def test_short_run_never_certifies_sustained_mode(self):
        report = valid_report()
        del report["prerequisite_smoke_sha256"]
        self.assertFalse(fixture.reconcile(report, REVISION))
        for seconds in (30, 60, 300, 7199):
            report = valid_report()
            report["requested_seconds"] = seconds
            self.assertFalse(fixture.reconcile(report, REVISION))
        report = valid_report()
        report["requested_seconds"] = 120
        report["mode"] = "smoke"
        self.assertTrue(fixture.reconcile(report, REVISION))
        report["mode"] = "unknown"
        self.assertFalse(fixture.reconcile(report, REVISION))

    def test_elapsed_and_progress_windows_cannot_hide_missing_work(self):
        for field, value in (("progress_windows", []), ("interrupted", True), ("failure", "FAILED")):
            report = valid_report()
            report[field] = value
            self.assertFalse(fixture.reconcile(report, REVISION))
        report = valid_report()
        next(item for item in report["checks"] if item["test"] == "mixed_load")["observed_seconds"] = 7199
        self.assertFalse(fixture.reconcile(report, REVISION))
        for kind in fixture.PROGRESS_KINDS:
            for tenant in fixture.TENANTS:
                report = valid_report()
                report["progress_windows"][0]["deltas"][kind][tenant] = 0
                self.assertFalse(fixture.reconcile(report, REVISION))

    def test_both_tenants_need_every_workload_class(self):
        for kind in fixture.WORKLOAD_KINDS:
            for tenant in fixture.TENANTS:
                for value in (0, -1, True, "1", None):
                    report = valid_report()
                    report["workload"][kind][tenant] = value
                    self.assertFalse(fixture.reconcile(report, REVISION), (kind, tenant, value))

    def test_fault_success_needs_exact_queued_running_and_no_replay_proof(self):
        for name in ("gateway_loss", "worker_loss", "broker_loss"):
            for field in ("running_before_fault", "queued_before_fault", "surviving_tenant_exact_result", "failed_attempt_result_rejected", "queued_handles_preserved"):
                report = valid_report()
                del next(item for item in report["checks"] if item["test"] == name)[field]
                self.assertFalse(fixture.reconcile(report, REVISION), (name, field))

    def test_resource_oom_or_missing_sampling_is_failure(self):
        for field, value in (("samples", 0), ("counter_errors", ["MISSING"]), ("resource_violations", ["OVER"]),
                             ("peak_scratch_bytes", (1024 << 20) + 1), ("query_refresh_overlap_samples", 0)):
            report = valid_report()
            report["resources"][field] = value
            self.assertFalse(fixture.reconcile(report, REVISION))
        for key in ("oom", "oom_kill"):
            for value in (1, -1, False, "0", None):
                report = valid_report()
                report["resources"]["cgroup_memory_events_delta"][key] = value
                self.assertFalse(fixture.reconcile(report, REVISION))

    def test_source_binaries_and_owned_cleanup_are_required(self):
        for field, value in (("source_unchanged", False), ("source", {}), ("binary_sha256", {}),
                             ("owned_service_removed", False), ("owned_cgroup_removed", False), ("service_exit_code", 1), ("service_exit_code", False)):
            report = valid_report()
            report[field] = value
            self.assertFalse(fixture.reconcile(report, REVISION))
        for field in ("forced_application_kills", "observed_live_descendants", "worker_scratch_directories", "remaining_containment_records"):
            report = valid_report()
            next(item for item in report["checks"] if item["test"] == "cleanup")[field] = 1
            self.assertFalse(fixture.reconcile(report, REVISION))

    def test_rss_tails_and_final_cgroup_sample_are_mandatory(self):
        for field, value in (("process_tree_rss_available", False), ("samples", 0),
                             ("sampled_process_tree_rss_peak_bytes", None), ("read_errors", ["READ_FAILED"]),
                             ("rss_source", None), ("rss_source", "proc_pid_status"),
                             ("page_size_bytes", None), ("page_size_bytes", True),
                             ("page_size_bytes", 0), ("page_size_bytes", 3), ("page_size_bytes", 4096.)):
            report = valid_report()
            report["process_sampling"][field] = value
            self.assertFalse(fixture.reconcile(report, REVISION))
        report = valid_report()
        report["resources"]["final_cgroup_sample_after_shutdown"] = False
        self.assertFalse(fixture.reconcile(report, REVISION))
        for kind in valid_report()["latencies"]:
            for tenant in fixture.TENANTS:
                report = valid_report()
                del report["latencies"][kind][tenant]
                self.assertFalse(fixture.reconcile(report, REVISION))
        report = valid_report()
        report["latencies"]["query"]["a"]["count"] = 0
        self.assertFalse(fixture.reconcile(report, REVISION))

    def test_binary_build_source_is_separate_and_must_match_runtime_inputs(self):
        report = valid_report()
        report["build_source"]["files"]["go.mod"] = "d" * 64
        canonical = json.dumps(report["build_source"]["files"], sort_keys=True, separators=(",", ":")).encode()
        report["build_source"]["sha256"] = hashlib.sha256(canonical).hexdigest()
        self.assertFalse(fixture.reconcile(report, REVISION))
        report = valid_report()
        report["source"]["files"]["scripts/post-build-change.py"] = "d" * 64
        report["source"]["file_count"] += 1
        canonical = json.dumps(report["source"]["files"], sort_keys=True, separators=(",", ":")).encode()
        report["source"]["sha256"] = hashlib.sha256(canonical).hexdigest()
        self.assertTrue(fixture.identity.valid_source_identity(report["source"]))
        self.assertFalse(fixture.reconcile(report, REVISION))

    def test_expected_revision_is_external_and_must_match_every_identity(self):
        for expected in (None, "", "a" * 39, "A" * 40, "b" * 40, 1, True):
            self.assertFalse(fixture.reconcile(valid_report(), expected))
        for field in ("revision", "source", "build_source"):
            report = valid_report()
            if field == "revision":
                report[field] = "b" * 40
            else:
                report[field]["base_revision"] = "b" * 40
            self.assertFalse(fixture.reconcile(report, REVISION), field)

    def test_histogram_retains_tail_overflow_and_reports_bounds(self):
        histogram = fixture.histogram()
        for value in (.001, .1, 3., 81.):
            fixture.observe(histogram, value)
        result = fixture.histogram_evidence(histogram)
        self.assertEqual(result["count"], 4)
        self.assertEqual(result["buckets"][-1], 4)
        self.assertEqual(result["maximum_seconds"], 81.)
        self.assertEqual(result["quantile_upper_bounds_seconds"]["0.5"], .1)
        self.assertIsNone(result["quantile_upper_bounds_seconds"]["0.99"])
        for value in (-1, float("nan"), 301, True, False, "1", None, float("inf"), 10 ** 1000):
            with self.assertRaises(fixture.ops.AcceptanceError):
                fixture.observe(histogram, value)

    def test_zero_cleanup_and_fault_counts_require_integers(self):
        for field in ("forced_application_kills", "observed_live_descendants", "worker_scratch_directories", "remaining_containment_records"):
            for value in (False, True, 0.0, "0", None, float("nan")):
                report = valid_report()
                next(item for item in report["checks"] if item["test"] == "cleanup")[field] = value
                self.assertFalse(fixture.reconcile(report, REVISION), (field, value))
        for name in ("gateway_loss", "worker_loss", "broker_loss"):
            for field in ("running_before_fault", "queued_before_fault"):
                for value in (True, False, 2.0, "2", None, float("nan")):
                    report = valid_report()
                    next(item for item in report["checks"] if item["test"] == name)[field] = value
                    self.assertFalse(fixture.reconcile(report, REVISION), (name, field, value))

    def test_resource_and_progress_counts_reject_coerced_values(self):
        for field in ("samples", "query_refresh_overlap_samples", "peak_scratch_bytes"):
            for value in (True, False, 1.0, -1, "1", None, float("nan"), float("inf")):
                report = valid_report()
                report["resources"][field] = value
                self.assertFalse(fixture.reconcile(report, REVISION), (field, value))
        for kind in fixture.WORKLOAD_KINDS:
            for tenant in fixture.TENANTS:
                for value in (True, False, 1.0, -1, "1", None, float("nan"), float("inf")):
                    report = valid_report()
                    report["progress_windows"][0]["deltas"][kind][tenant] = value
                    self.assertFalse(fixture.reconcile(report, REVISION), (kind, tenant, value))

    def test_duration_and_window_boundaries_require_finite_numbers(self):
        for field in ("requested_seconds", "observed_seconds", "start_seconds", "end_seconds"):
            for value in (True, False, "7200", None, float("nan"), float("inf"), -1, 10 ** 1000):
                report = valid_report()
                target = report if field == "requested_seconds" else (next(item for item in report["checks"] if item["test"] == "mixed_load") if field == "observed_seconds" else report["progress_windows"][0])
                target[field] = value
                self.assertFalse(fixture.reconcile(report, REVISION), (field, value))
        report = valid_report()
        report["progress_windows"][0]["end_seconds"] = 7201.
        self.assertFalse(fixture.reconcile(report, REVISION))

    def test_latency_counter_types_and_bounds_cannot_be_coerced(self):
        for field in ("count", "sum_seconds", "maximum_seconds"):
            for value in (True, False, "1", None, float("nan"), float("inf"), -1):
                report = valid_report()
                report["latencies"]["query"]["a"][field] = value
                self.assertFalse(fixture.reconcile(report, REVISION), (field, value))
        for field, value in (("count", 5.0), ("maximum_seconds", 301), ("sum_seconds", 1501)):
            report = valid_report()
            report["latencies"]["query"]["a"][field] = value
            self.assertFalse(fixture.reconcile(report, REVISION), (field, value))
        report = valid_report()
        report["latencies"]["query"]["a"]["bucket_upper_seconds"][7] = True
        self.assertFalse(fixture.reconcile(report, REVISION))
        report = valid_report()
        report["latencies"]["query"]["a"]["quantile_upper_bounds_seconds"]["0.5"] = True
        self.assertFalse(fixture.reconcile(report, REVISION))


if __name__ == "__main__":
    unittest.main()
