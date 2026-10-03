#!/usr/bin/env python3
"""Negative controls: smoke, duration, missing/fault gates, ownership and limits."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import sustained_acceptance as fixture

REVISION = "a" * 40


def valid_report():
    files = {name: "a" * 64 for name in fixture.identity.SOURCE_REQUIRED}
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    checks = [{"test": name, "passed": True} for name in fixture.REQUIRED]
    for check in checks:
        if check["test"] in ("gateway_loss", "worker_loss", "broker_loss"):
            check.update(running_before_fault=2, queued_before_fault=2, surviving_tenant_exact_result=True,
                         failed_attempt_result_rejected=True, queued_handles_preserved=True)
        if check["test"] == "mixed_load":
            check["observed_seconds"] = 7200.
        if check["test"] == "cleanup":
            check.update(forced_application_kills=0, observed_live_descendants=0, worker_scratch_directories=0,
                         remaining_containment_records=0, all_owned_brokers_stopped=True, original_configurations_verified=True)
    report = {"revision": REVISION, "checks": checks, "requested_seconds": 7200., "mode": "sustained", "interrupted": False,
            "prerequisite_smoke_sha256": "d" * 64,
            "service_exit_code": 0, "source_unchanged": True, "owned_service_removed": True, "owned_cgroup_removed": True,
            "source": {"verified": True, "file_count": len(files), "files": files, "sha256": hashlib.sha256(canonical).hexdigest(),
                       "base_revision": REVISION, "excluded_metadata": [".DS_Store", "._*"]},
            "binary_sha256": {"kelvo": "b" * 64, "kelvo-landlock": "c" * 64},
            "workload": {kind: {tenant: 5 for tenant in fixture.TENANTS} for kind in ("queries", "refreshes", "slow_readers", "cancellations")},
            "resources": {"samples": 7200, "counter_errors": [], "resource_violations": [], "peak_scratch_bytes": 1,
                          "cgroup_memory_events_delta": {"oom": 0, "oom_kill": 0}, "query_refresh_overlap_samples": 1, "final_cgroup_sample_after_shutdown": True},
            "progress_windows": [{"start_seconds": 0., "end_seconds": 7200., "deltas": {kind: {tenant: 5 for tenant in fixture.TENANTS} for kind in ("queries", "refreshes")}}]}
    report["build_source"] = copy.deepcopy(report["source"])
    report["control_response_counts"] = {"queued_status_http_200": 6}
    report["process_sampling"] = {"process_tree_rss_available": True, "samples": 100, "sampled_process_tree_rss_peak_bytes": 1024, "read_errors": []}
    report["latencies"] = {}
    for kind in ("query", "dispatch_observation", "first_byte", "slow_delivery", "cancel"):
        report["latencies"][kind] = {}
        for tenant in fixture.TENANTS:
            observed = fixture.histogram()
            for _ in range(5):
                fixture.observe(observed, .01)
            report["latencies"][kind][tenant] = fixture.histogram_evidence(observed)
    return report


class SustainedControls(unittest.TestCase):
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

    def test_rss_identity_distinguishes_exit_and_missing_from_malformed_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            process = root / "123"
            self.assertEqual(fixture.sampled_identity(123, root), (None, "disappeared"))
            process.mkdir()
            fields = ["S", *(["0"] * 18), "456"]
            for state, expected in (("S", ((123, "456"), "live")), ("Z", (None, "exited")), ("X", (None, "exited"))):
                fields[0] = state
                (process / "stat").write_text("123 (name with ) brackets) " + " ".join(fields))
                self.assertEqual(fixture.sampled_identity(123, root), expected)
            for invalid in ("malformed", "124 (wrong) " + " ".join(fields), "123 (short) S 0"):
                (process / "stat").write_text(invalid)
                self.assertEqual(fixture.sampled_identity(123, root), (None, "read_error"))
            (process / "stat").unlink()
            (process / "stat").mkdir()
            self.assertEqual(fixture.sampled_identity(123, root), (None, "read_error"))

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
        for kind in ("queries", "refreshes"):
            for tenant in fixture.TENANTS:
                report = valid_report()
                report["progress_windows"][0]["deltas"][kind][tenant] = 0
                self.assertFalse(fixture.reconcile(report, REVISION))

    def test_both_tenants_need_every_workload_class(self):
        for kind in ("queries", "refreshes", "slow_readers", "cancellations"):
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
        for field, value in (("process_tree_rss_available", False), ("samples", 0), ("sampled_process_tree_rss_peak_bytes", None), ("read_errors", ["READ_FAILED"])):
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
        for kind in ("queries", "refreshes"):
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
