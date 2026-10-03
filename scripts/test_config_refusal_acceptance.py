#!/usr/bin/env python3
"""Security controls must isolate one unsupported option at a time."""
import copy
from pathlib import Path
import unittest

from config_refusal_acceptance import add_feature


class RefusalConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.policy = {"access": {"revision": 1, "principals": {
            "analyst": {"kind": "user", "federated_sources": ["sample"]}}}}

    def test_row_policy_isolated_for_each_role_without_mutating_baseline(self):
        for baseline in ({"policy": self.policy}, {"tenants": [{"policy": self.policy}]}):
            original = copy.deepcopy(baseline)
            changed = add_feature(baseline, "row_column_policy", Path("unused"))
            self.assertEqual(baseline, original)
            self.assertNotIn("audit", changed)
            policy = changed.get("policy") or changed["tenants"][0]["policy"]
            self.assertEqual(policy["access"]["principals"]["analyst"]["row_column_policy"]
                             ["sources"]["sample"]["tables"]["sample"]["rows"]["value"], "1")

    def test_audit_isolated_without_row_policy_or_baseline_mutation(self):
        baseline = {"policy": self.policy}
        changed = add_feature(baseline, "audit", Path("/private/journal"))
        self.assertNotIn("audit", baseline)
        self.assertEqual(changed["policy"], self.policy)
        self.assertEqual(changed["audit"]["directory"], "/private/journal")
        self.assertNotIn("row_column_policy", changed["policy"]["access"]["principals"]["analyst"])


if __name__ == "__main__":
    unittest.main()
