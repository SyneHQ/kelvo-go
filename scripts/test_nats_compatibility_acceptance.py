#!/usr/bin/env python3
"""Exact compatibility declarations and evidence cannot silently widen."""
import copy
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


if __name__ == "__main__":
    unittest.main()
