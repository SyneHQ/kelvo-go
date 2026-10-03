import json
import unittest

from export_diagnostics import (ERROR_CLASSES, FIELDS, MARKER, MAX_COUNTER, MAX_OUTPUT,
                                MAX_PAYLOAD, MAX_RECORDS, PACKAGE, SOURCE, STATES, TEST,
                                TESTS, ExportDiagnostics, parse_wait_observation)


GOOD = {"wanted": "cancelled", "observed": "stored", "deadline_reached": True,
        "receipt_present": True, "error_class": "none", "executions": 3}
SUBTEST = TEST + "/lost-completion-is-not-replayed"


def event(text=None, **changes):
    if text is None:
        text = MARKER + json.dumps(GOOD)
    value = {"Action": "output", "Package": PACKAGE, "Test": SUBTEST,
             "Output": "    " + SOURCE + ":329: " + text + "\n"}
    value.update(changes)
    return value


class ExportDiagnosticControls(unittest.TestCase):
    def test_valid_closed_schema_and_all_finite_values(self):
        self.assertEqual(parse_wait_observation(json.dumps(GOOD)), GOOD)
        for wanted in STATES:
            for observed in STATES | {"unknown"}:
                value = dict(GOOD, wanted=wanted, observed=observed)
                self.assertEqual(parse_wait_observation(json.dumps(value)), value)
        for code in ERROR_CLASSES:
            value = dict(GOOD, error_class=code)
            self.assertEqual(parse_wait_observation(json.dumps(value)), value)
        for count in (0, 2**31 - 1):
            self.assertEqual(parse_wait_observation(json.dumps(dict(GOOD, executions=count)))["executions"], count)

    def test_rejects_missing_extra_or_malformed_payloads(self):
        invalid = ["not-json PRIVATE_VALUE", "null", "[]", "true", "0", "{}",
                   json.dumps(dict(GOOD, token="PRIVATE_VALUE")),
                   json.dumps(GOOD) + " PRIVATE_VALUE", " " * (MAX_PAYLOAD + 1),
                   "[" * 1100 + "]" * 1100, json.dumps(GOOD) + "\u2603"]
        for field in FIELDS:
            invalid.append(json.dumps({key: value for key, value in GOOD.items() if key != field}))
        for field, values in {
            "wanted": ("unknown", "PRIVATE_VALUE", 1, None, []),
            "observed": ("PRIVATE_VALUE", True, {}, None),
            "error_class": ("PRIVATE_VALUE", 0, [], None),
            "deadline_reached": (1, 0, "true", None, []),
            "receipt_present": (1, "false", {}, None),
            "executions": (-1, 2**31, True, False, 3.0, "3", None, float("nan"), float("inf")),
        }.items():
            invalid.extend(json.dumps(dict(GOOD, **{field: value})) for value in values)
        invalid += [json.dumps(GOOD).replace('"executions": 3', '"executions": 2, "executions": 3'),
                    json.dumps(GOOD).replace('"observed": "stored"', '"observed": "PRIVATE_VALUE", "observed": "stored"')]
        for payload in invalid:
            with self.subTest(payload_type=type(payload).__name__, size=len(payload)):
                with self.assertRaises((ValueError, TypeError, RecursionError)):
                    parse_wait_observation(payload)
                collector = ExportDiagnostics()
                collector.observe(event(MARKER + payload))
                report = collector.report()
                self.assertEqual(report["records"], [])
                self.assertEqual(report["rejected"], 1)
                self.assertNotIn("PRIVATE_VALUE", json.dumps(report))

    def test_only_known_tests_package_and_exact_basename_locations(self):
        for name in TESTS:
            collector = ExportDiagnostics()
            collector.observe(event(Test=name))
            self.assertEqual(collector.report()["records"][0]["test"], name)
        for bad in (event(Test=TEST + "/PRIVATE_VALUE"), event(Test=[TEST]), event(Test=None),
                    event(Package="PRIVATE_VALUE"), event(Package=None),
                    event(Output="/private/PRIVATE_VALUE/" + SOURCE + ":329: " + MARKER + json.dumps(GOOD)),
                    event(Output="../" + SOURCE + ":329: " + MARKER + json.dumps(GOOD)),
                    event(Output=SOURCE + ":0: " + MARKER + json.dumps(GOOD)),
                    event(Output=SOURCE + ":0329: " + MARKER + json.dumps(GOOD)),
                    event(Output=SOURCE + ":1000000: " + MARKER + json.dumps(GOOD)),
                    event(Output="other.go:329: " + MARKER + json.dumps(GOOD)),
                    event(Output=MARKER + json.dumps(GOOD)), event(Output=None)):
            collector = ExportDiagnostics()
            collector.observe(bad)
            self.assertEqual(collector.report(), {"records": [], "rejected": 1, "truncated": 0})
            self.assertNotIn("PRIVATE_VALUE", json.dumps(collector.report()))

    def test_assertion_text_is_discarded_and_marker_has_exact_placement(self):
        collector = ExportDiagnostics()
        collector.observe(event("await cancelled: PRIVATE_VALUE /private/key SELECT sql"))
        self.assertEqual(collector.report()["records"], [
            {"test": SUBTEST, "file": SOURCE, "line": 329, "kind": "source_location"}])
        for text in ("prefix " + MARKER + json.dumps(GOOD), MARKER.rstrip(),
                     MARKER.rstrip() + "\t" + json.dumps(GOOD)):
            collector.observe(event(text))
        self.assertEqual(collector.report()["rejected"], 3)
        self.assertNotIn("PRIVATE_VALUE", json.dumps(collector.report()))

    def test_record_count_output_size_and_counters_are_bounded(self):
        collector = ExportDiagnostics()
        for _ in range(MAX_RECORDS + 3):
            collector.observe(event())
        self.assertEqual(len(collector.report()["records"]), MAX_RECORDS)
        self.assertEqual(collector.report()["truncated"], 3)
        collector.observe(event(Output="x" * (MAX_OUTPUT + 1)))
        self.assertEqual(collector.report()["rejected"], 1)
        collector.rejected = collector.truncated = MAX_COUNTER
        collector.observe(event(Output=None))
        collector.observe(event())
        self.assertEqual(collector.report()["rejected"], MAX_COUNTER)
        self.assertEqual(collector.report()["truncated"], MAX_COUNTER)

    def test_valid_marker_retains_only_typed_fields_and_source_location(self):
        collector = ExportDiagnostics()
        collector.observe(event())
        expected = dict(GOOD, test=SUBTEST, file=SOURCE, line=329, kind="wait_state")
        self.assertEqual(collector.report(), {"records": [expected], "rejected": 0, "truncated": 0})
        collector.observe({"Action": "pass", "Test": SUBTEST})
        collector.observe(event(Output="=== RUN   " + SUBTEST + "\n"))
        collector.observe([])
        self.assertEqual(collector.report(), {"records": [expected], "rejected": 0, "truncated": 0})


if __name__ == "__main__":
    unittest.main()
