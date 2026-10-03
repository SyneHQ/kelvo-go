#!/usr/bin/env python3
"""Negative controls for key-rotation evidence and owned atomic file updates."""
import copy
from pathlib import Path
import tempfile
import threading
import time
import json
import unittest

import gateway_key_rotation_acceptance as fixture


class RotationEvidence(unittest.TestCase):
    def valid(self):
        report = {"interrupted": False, "checks": [{"test": name, "passed": True} for name in fixture.REQUIRED]}
        report["checks"][-1]["original_configurations_verified"] = True
        return report

    def test_every_gate_and_uninterrupted_cleanup_required(self):
        report = self.valid()
        self.assertTrue(fixture.reconcile(report))
        for index in range(len(report["checks"])):
            changed = copy.deepcopy(report)
            changed["checks"][index]["passed"] = False
            self.assertFalse(fixture.reconcile(changed))
            changed = copy.deepcopy(report)
            changed["checks"].pop(index)
            self.assertFalse(fixture.reconcile(changed))
        report["interrupted"] = True
        self.assertFalse(fixture.reconcile(report))
        report = self.valid()
        report["checks"][-1]["original_configurations_verified"] = False
        self.assertFalse(fixture.reconcile(report))

    def test_atomic_rotation_preserves_complete_private_document(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"keys.yml"
            first = fixture.key_document(1, {"a": ["a"*32], "b": []})
            second = fixture.key_document(2, {"a": ["b"*32], "b": []})
            fixture.atomic_private(path, first)
            self.assertEqual(path.read_bytes(), first)
            fixture.atomic_private(path, second)
            self.assertEqual(path.read_bytes(), second)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(list(Path(directory).iterdir()), [path])


class RotationWaiterScheduling(unittest.TestCase):
    def scheduled(self, parked_rejection):
        acceptance = fixture.RotationAcceptance.__new__(fixture.RotationAcceptance)
        ids = ["job-"+str(index) for index in range(8)]
        states = {query_id: "queued" for query_id in ids}
        states[ids[-1]] = "assigned"  # assignment order differs from submissions
        acceptance.state = lambda query_id: states[query_id]
        released = threading.Event()
        held = [threading.Event(), threading.Event()]
        attempts = {ids[0]: 0, ids[1]: 0}
        mutex = threading.Lock()
        def response(message):
            return 429, json.dumps({"error": {"code": "RESOURCE_EXHAUSTED", "message": message}}).encode(), {}
        def call(path, **kwargs):
            query_id = path.split("/")[3]
            if query_id in attempts:
                with mutex:
                    attempts[query_id] += 1
                    first = query_id == ids[0] and attempts[query_id] == 1
                if first:
                    return response("Request capacity unavailable")
                held[0 if query_id == ids[0] else 1].set()
                if not released.wait(2):
                    raise AssertionError("fixture release did not run")
                return 200, b"", {}
            if query_id == ids[2]:
                ready = all(event.is_set() for event in held)
                code, raw, _ = response("Queued result wait capacity unavailable" if ready and parked_rejection else "Request capacity unavailable")
                return code, raw
            raise AssertionError("unexpected query handle")
        acceptance.call = call
        acceptance.remove_temporary_key = lambda token: released.set()
        def wait(predicate, timeout=3):
            deadline = time.monotonic()+0.3
            while time.monotonic() < deadline:
                value = predicate()
                if value:
                    return value
                time.sleep(0.005)
            released.set()
            raise fixture.ops.AcceptanceError("CONDITION_DEADLINE")
        acceptance.wait = wait
        with tempfile.TemporaryDirectory() as directory:
            acceptance.directory = Path(directory)
            if parked_rejection:
                evidence = acceptance.queued_revocation(ids, "private-fixture-token")
                self.assertEqual(evidence["waiter_rejection_source"], "parked_waiter_pool")
                self.assertEqual(attempts[ids[0]], 2)
                self.assertTrue(all(state == "queued" for query_id, state in states.items() if query_id != ids[-1]))
            else:
                with self.assertRaisesRegex(fixture.ops.AcceptanceError, "CONDITION_DEADLINE"):
                    acceptance.queued_revocation(ids, "private-fixture-token")

    def test_transient_active_rejection_does_not_lose_rotation_waiter(self):
        self.scheduled(True)

    def test_generic_429_cannot_certify_parked_revocation(self):
        self.scheduled(False)


if __name__ == "__main__":
    unittest.main()
