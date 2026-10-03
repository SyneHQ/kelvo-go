#!/usr/bin/env python3
"""Negative evidence controls and real gated-source protocol checks; VM only."""
import concurrent.futures
import copy
import http.client
import unittest
import urllib.parse

import rolling_upgrade_acceptance as fixture


class EvidenceControls(unittest.TestCase):
    def valid(self):
        checks = [{"test": name, "passed": True} for name in fixture.REQUIRED]
        for check in checks:
            if check["test"] in fixture.WAVES:
                check.update(running_before_transition=2, queued_before_transition=2, exact_running_results=2,
                             exact_queued_results=2, original_handles_preserved=True, consumed_results_rejected=True,
                             one_observed_source_request_per_marker=True, revoked_keys_denied=True, foreign_handles_hidden=True)
                if check["test"].startswith("broker_restart_"):
                    check.update(replicas_after_rejoin=3, one_broker_at_a_time=True,
                                 broker_version_before=fixture.ops.cf.VERSION, broker_version_after=fixture.ops.cf.VERSION)
                else:
                    check.update(readiness_rejected_during_drain=True, graceful_exit=True)
            elif check["test"] == "key_rotation_and_floor":
                check.update(both_replica_floors=3, overlap_and_revocation_on_both_replicas=True,
                             rollback_requires_current_keys_and_policy=True)
            elif check["test"] == "stale_startup_rejected":
                check.update(rejected_probes=8, current_metadata_revalidated=True, current_configuration_hashes_preserved=True,
                             revoked_keys_remain_denied=True)
            elif check["test"] == "ambiguous_gateway_loss":
                check.update(failed_result_rejected=True, failed_attempt_not_replayed=True, surviving_tenant_exact_result=True,
                             queued_handles_preserved=True, one_observed_source_request_per_marker=True,
                             running_before_fault=2, queued_before_fault=2)
            elif check["test"] == "cleanup":
                check.update(forced_application_kills=0, observed_live_descendants=0, worker_scratch_directories=0,
                             original_configurations_verified=True, all_owned_brokers_stopped=True,
                             fixture_gate_errors=0, input_artifact_hashes_preserved=True)
        return {"interrupted": False, "checks": checks}

    def test_missing_duplicate_failed_and_interrupted_gates_reject(self):
        report = self.valid()
        self.assertTrue(fixture.reconcile(report))
        for index in range(len(report["checks"])):
            for operation in ("missing", "duplicate", "failed"):
                changed = copy.deepcopy(report)
                if operation == "missing":
                    changed["checks"].pop(index)
                elif operation == "duplicate":
                    changed["checks"].append(changed["checks"][index])
                else:
                    changed["checks"][index]["passed"] = False
                self.assertFalse(fixture.reconcile(changed), (index, operation))
        report["interrupted"] = True
        self.assertFalse(fixture.reconcile(report))

    def test_every_transition_security_and_cleanup_measurement_required(self):
        report = self.valid()
        for index, check in enumerate(report["checks"]):
            for field, value in check.items():
                if field in ("test", "passed"):
                    continue
                invalid = (None, False, "different") if type(value) is str else (None, False, value+1)
                if type(value) is int:
                    invalid += (True, float(value), str(value))
                for replacement in invalid:
                    changed = copy.deepcopy(report)
                    changed["checks"][index][field] = replacement
                    self.assertFalse(fixture.reconcile(changed), (check["test"], field))

    def test_only_declared_binary_broker_client_and_sandbox_pair_is_accepted(self):
        artifact = {"source_archive_sha256": "a"*64, "binary_sha256": "b"*64,
                    "launcher_sha256": "c"*64, "nats_client": "github.com/nats-io/nats.go v1.54.0"}
        matrix = {"old_revision": fixture.OLD_REVISION, "new_revision": fixture.NEW_REVISION,
                  "nats_server_version": fixture.ops.cf.VERSION, "nats_client_unchanged": True,
                  "old": copy.deepcopy(artifact), "new": copy.deepcopy(artifact)}
        fixture.validate_matrix(matrix)
        for field in ("old_revision", "new_revision", "nats_server_version", "nats_client_unchanged"):
            changed = copy.deepcopy(matrix)
            changed[field] = "different"
            with self.assertRaises(fixture.ops.AcceptanceError):
                fixture.validate_matrix(changed)
        for field in ("nats_client", "launcher_sha256", "binary_sha256", "source_archive_sha256"):
            changed = copy.deepcopy(matrix)
            changed["new"][field] = "different"
            with self.assertRaises(fixture.ops.AcceptanceError):
                fixture.validate_matrix(changed)


class GatedSource(unittest.TestCase):
    def setUp(self):
        self.gate = fixture.ArrowGate()
        self.gate.start()

    def tearDown(self):
        self.gate.close()

    def request(self, tenant="a", marker=17, settings=None):
        settings = settings or {"readonly": "1", "default_format": "ArrowStream",
                                "cancel_http_readonly_queries_on_client_close": "1", "result_overflow_mode": "throw"}
        connection = http.client.HTTPConnection("127.0.0.1", self.gate.server.server_port, timeout=3)
        try:
            connection.request("POST", "/"+tenant+"?"+urllib.parse.urlencode(settings), body=f"SELECT {marker} AS marker")
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def test_result_stays_blocked_until_release_and_values_are_exact(self):
        entry = self.gate.register("a", 17)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.request)
            self.assertTrue(entry["started"].wait(2))
            self.assertFalse(future.done())
            self.assertEqual(entry["requests"], 1)
            entry["release"].set()
            code, body = future.result(timeout=3)
        self.assertEqual(code, 200)
        fixture.ops.verify_arrow(body, [{"tenant": "a", "marker": 17}])
        self.assertFalse(self.gate.errors)
        with self.assertRaises(http.client.RemoteDisconnected):
            self.request()
        self.assertEqual(entry["requests"], 2)
        self.assertEqual(len(self.gate.errors), 1)

    def test_unregistered_tenant_and_bad_settings_never_start_source(self):
        entry = self.gate.register("a", 17)
        with self.assertRaises(http.client.RemoteDisconnected):
            self.request(tenant="b")
        with self.assertRaises(http.client.RemoteDisconnected):
            self.request(settings={"readonly": "0"})
        self.assertFalse(entry["started"].is_set())
        self.assertEqual(entry["requests"], 0)
        self.assertEqual(len(self.gate.errors), 2)

    def test_changed_integer_width_cannot_pass_exact_result_check(self):
        import pyarrow as pa
        table = pa.table({"tenant": pa.array(["a"], type=pa.string()), "marker": pa.array([17], type=pa.int32())})
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        with self.assertRaisesRegex(fixture.ops.AcceptanceError, "GATE_RESULT_SCHEMA_CHANGED"):
            fixture.verify_gate_result(sink.getvalue().to_pybytes(), {"kelvo-result-completion": "durable-eos-v1"}, "a", 17)


if __name__ == "__main__":
    unittest.main()
