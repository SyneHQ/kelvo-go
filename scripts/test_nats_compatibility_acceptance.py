#!/usr/bin/env python3
"""Exact compatibility declarations and evidence cannot silently widen."""
import copy
import json
from pathlib import Path
import unittest

import nats_compatibility_acceptance as compat
import operational_acceptance as ops


def matrix(mode):
    artifact = dict.fromkeys(("source_archive_sha256", "binary_sha256", "launcher_sha256", "module_manifest_sha256",
                             "effective_modfile_sha256", "effective_sumfile_sha256"), "a"*64)
    artifact["nats_client"] = "v1.54.0"
    result = dict(mode=mode, old_revision="b"*40, new_revision="b"*40, old=copy.deepcopy(artifact),
                  new=copy.deepcopy(artifact), default_dependencies_unchanged=True)
    if mode == "client":
        result["old"]["nats_client"] = "v1.53.1"
    return result


class MatrixTests(unittest.TestCase):
    def test_broker_mode_holds_engine_and_client_constant(self):
        compat.validate_matrix(matrix("broker"))
        for field in ("binary_sha256", "source_archive_sha256", "effective_modfile_sha256", "nats_client"):
            value = matrix("broker")
            value["new"][field] = "c"*64
            with self.subTest(field=field), self.assertRaises(ops.AcceptanceError):
                compat.validate_matrix(value)

    def test_client_matrix_requires_exact_source_launcher_and_client_pair(self):
        compat.validate_matrix(matrix("client"))
        for field, change in (("old_revision", "c"*40), ("default_dependencies_unchanged", False)):
            value = matrix("client")
            value[field] = change
            with self.subTest(field=field), self.assertRaises(ops.AcceptanceError):
                compat.validate_matrix(value)
        for field, change in (("source_archive_sha256", "c"*64), ("launcher_sha256", "c"*64),
                              ("nats_client", "v1.54.1"), ("module_manifest_sha256", "")):
            value = matrix("client")
            value["old"][field] = change
            with self.subTest(field=field), self.assertRaises(ops.AcceptanceError):
                compat.validate_matrix(value)

    def test_missing_or_duplicate_evidence_does_not_pass(self):
        for mode in ("broker", "client"):
            for checks in ([], [dict(test=name, passed=True) for name in compat.expected_steps(mode)]):
                self.assertFalse(compat.reconcile(dict(mode=mode, interrupted=False, checks=checks)))
            self.assertFalse(compat.reconcile(dict(mode=mode, interrupted=True, checks=[])))

    def test_broker_matrix_requires_real_upgrade_and_rollback_cells(self):
        steps = compat.expected_steps("broker")
        self.assertTrue(all("broker_upgrade_"+str(i) in steps and "broker_rollback_"+str(i) in steps for i in range(3)))
        self.assertEqual(len(steps), len(set(steps)))


class RetainedReportMutationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        evidence = json.loads((Path(__file__).resolve().parents[1]/"docs/evidence/nats-compatibility.json").read_text())
        cls.reports = [evidence["broker_upgrade_and_rollback"],
                       *evidence["client_upgrade_and_rollback"].values()]

    def test_retained_real_reports_pass(self):
        for report in self.reports:
            with self.subTest(mode=report["mode"], server=report["start_server_version"]):
                self.assertTrue(compat.reconcile(report))

    def test_out_of_order_or_duplicate_transitions_are_rejected(self):
        for original in self.reports:
            for mutation in ("swap", "duplicate"):
                report = copy.deepcopy(original)
                if mutation == "swap":
                    report["checks"][3], report["checks"][4] = report["checks"][4], report["checks"][3]
                else:
                    report["checks"][4] = copy.deepcopy(report["checks"][3])
                with self.subTest(mode=report["mode"], mutation=mutation):
                    self.assertFalse(compat.reconcile(report))

    def test_each_broker_wave_requires_its_exact_cumulative_peer_map(self):
        original = self.reports[0]
        for index, item in enumerate(original["checks"]):
            if not item["test"].startswith("broker_"):
                continue
            for peer in compat.ops.cf.BROKER_NAMES.values():
                report = copy.deepcopy(original)
                versions = report["checks"][index]["expected_peer_versions"]
                versions[peer] = "2.15.0" if versions[peer] == "2.14.7" else "2.14.7"
                with self.subTest(wave=item["test"], peer=peer):
                    self.assertFalse(compat.reconcile(report))

    def test_each_client_wave_requires_exact_role_target_and_cumulative_map(self):
        for original in self.reports[1:]:
            for index, item in enumerate(original["checks"]):
                if not item["test"].startswith("client_"):
                    continue
                for role in compat.roll.APP_ROLES:
                    report = copy.deepcopy(original)
                    roles = report["checks"][index]["roles_after_transition"]
                    roles[role] = "new" if roles[role] == "old" else "old"
                    with self.subTest(server=original["start_server_version"], wave=item["test"], role=role):
                        self.assertFalse(compat.reconcile(report))
                for field, value in (("role", "unrecorded-worker"), ("destination", "unknown")):
                    report = copy.deepcopy(original)
                    report["checks"][index][field] = value
                    with self.subTest(wave=item["test"], field=field):
                        self.assertFalse(compat.reconcile(report))

    def test_key_floor_requires_integer_even_when_numerically_equal(self):
        for original in self.reports:
            for value in (3.0, "3", True, None):
                report = copy.deepcopy(original)
                report["checks"][1]["both_replica_floors"] = value
                with self.subTest(mode=report["mode"], value=value):
                    self.assertFalse(compat.reconcile(report))

    def test_valid_transitions_cannot_hide_an_invalid_matrix(self):
        for original in self.reports:
            for field, value in (("new_revision", "f"*40), ("default_dependencies_unchanged", False),
                                 ("mode", "client" if original["mode"] == "broker" else "broker")):
                report = copy.deepcopy(original)
                report["matrix"][field] = value
                with self.subTest(mode=original["mode"], field=field):
                    self.assertFalse(compat.reconcile(report))
            for field in ("binary_sha256", "source_archive_sha256", "module_manifest_sha256", "effective_modfile_sha256"):
                report = copy.deepcopy(original)
                report["matrix"]["old"][field] = "unverified"
                self.assertFalse(compat.reconcile(report))
            report = copy.deepcopy(original)
            report["matrix"]["old"]["nats_client"] = "v1.52.0"
            self.assertFalse(compat.reconcile(report))

    def test_wrong_start_version_and_malformed_reports_fail_closed(self):
        for original in self.reports:
            for start in (None, "2.14.6", "2.15.1"):
                report = copy.deepcopy(original)
                report["start_server_version"] = start
                self.assertFalse(compat.reconcile(report))
            for matrix in (None, {}, [], {"mode": "client", "old_revision": 123}):
                report = copy.deepcopy(original)
                report["matrix"] = matrix
                self.assertFalse(compat.reconcile(report))
        report = copy.deepcopy(self.reports[0])
        report["start_server_version"] = "2.15.0"
        self.assertFalse(compat.reconcile(report))


if __name__ == "__main__":
    unittest.main()
