#!/usr/bin/env python3
"""Checks the latency gate of pooled_read_load.py: python3 scripts/test_pooled_read_load_gate.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from pooled_read_load import latency_gate

clean = {"all_requests": {"errors": 0, "p95_ms": 2000}, "errors_by_code": {}, "classes": {"explorer_read": {"p95_ms": 2000}}}
scenario = {"gate": {"p95_ms": 3000, "class_p95_ms": {"explorer_read": 2600}}}
assert all(latency_gate(clean, scenario).values())
assert all(latency_gate(clean, {}).values())  # no ceilings: only the failure checks
failed = dict(clean, all_requests={"errors": 1, "p95_ms": 2000}, errors_by_code={"CLIENT_ADMISSION_LIMIT": 1})
assert not latency_gate(failed, scenario)["no_failed_requests"]
deadline = dict(clean, errors_by_code={"DEADLINE_EXCEEDED": 2})
assert not latency_gate(deadline, scenario)["no_deadline_or_unavailable"]
slow = dict(clean, all_requests={"errors": 0, "p95_ms": 3500})
assert not latency_gate(slow, scenario)["p95_within_ceiling"]
slow_class = dict(clean, classes={"explorer_read": {"p95_ms": 2700}})
assert not latency_gate(slow_class, scenario)["p95_explorer_read_within_ceiling"]
print("ok")
