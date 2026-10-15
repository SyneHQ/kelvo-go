#!/usr/bin/env python3
"""A warm adapter gate that skips must fail its CI step."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("warm", Path(__file__).with_name("warm_adapter_acceptance.py"))
warm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(warm)
GATE = "TestContainedReadWorkerServesExplorerPageOpen"


class GatePassed(unittest.TestCase):
    def test_pass(self):
        self.assertTrue(warm.gate_passed("--- PASS: %s (0.35s)\nPASS\n" % GATE, GATE, 0)[0])

    def test_skip_fails(self):
        # go test exits 0 when a test skips; the gate must still fail.
        self.assertFalse(warm.gate_passed("--- SKIP: %s (0.00s)\nPASS\n" % GATE, GATE, 0)[0])

    def test_missing_or_other_gate_fails(self):
        self.assertFalse(warm.gate_passed("ok\n", GATE, 0)[0])
        self.assertFalse(warm.gate_passed("--- PASS: %sSuffix (0.1s)\n" % GATE, GATE, 0)[0])

    def test_nonzero_exit_fails(self):
        self.assertFalse(warm.gate_passed("--- PASS: %s (0.35s)\n" % GATE, GATE, 1)[0])


if __name__ == "__main__":
    unittest.main()
