"""VM-only tests of CSV acceptance evidence and Arrow correctness checks."""
import copy
import importlib.util
import io
from pathlib import Path
import unittest

import pyarrow as pa
import pyarrow.ipc as ipc

spec = importlib.util.spec_from_file_location("csv_memory_acceptance", Path(__file__).with_name("csv_memory_acceptance.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


def arrow_output(value=3, datatype=pa.int64()):
    output = io.BytesIO()
    table = pa.table({"total": pa.array([value], type=datatype)})
    with ipc.new_stream(output, table.schema) as writer:
        writer.write_table(table)
    return output.getvalue()


def evidence():
    cases = []
    for memory in (16, 32, 64):
        for profile in fixture.PROFILES:
            case = {"profile": profile, "duckdb_memory_limit_mb": memory,
                    "rss_samples": 2, "exit_code": 0, "outcome_code": "OK"}
            case.update(fixture.decode_result(arrow_output()))
            if profile == "csv_default":
                case["outcome_code"] = "RESOURCE_EXHAUSTED"
                case["exit_code"] = 1
                for key in ("schema", "rows", "row_count", "arrow_eos", "wire_bytes", "result_sha256"):
                    case.pop(key)
            cases.append(case)
    return cases


class CSVAcceptanceTests(unittest.TestCase):
    def test_exact_arrow_type_value_and_framing(self):
        self.assertEqual(fixture.decode_result(arrow_output())["rows"], [{"total": 3}])
        for data in (arrow_output(4), arrow_output(3, pa.int32()), arrow_output()[:-8],
                     arrow_output() + arrow_output(), b"not arrow", arrow_output(None)):
            with self.subTest(size=len(data)):
                with self.assertRaises(fixture.FixtureError):
                    fixture.decode_result(data)

    def test_static_outcome_only(self):
        self.assertEqual(fixture.decode_outcome(b'{"stats":{},"error":{"code":"RESOURCE_EXHAUSTED","message":"private source"}}'), "RESOURCE_EXHAUSTED")
        for data in (b'{"stats":{},"error":{"code":"private source"}}',
                     b'{"stats":{},"error":"private source"}', b'[]', b'not json'):
            with self.assertRaises(fixture.FixtureError) as result:
                fixture.decode_outcome(data)
            self.assertEqual(str(result.exception), "INVALID_WORKER_OUTCOME")

    def test_default64_resource_failure_is_not_a_required_success(self):
        passed, improvements, failures = fixture.acceptance(evidence())
        self.assertTrue(passed)
        self.assertEqual(len(improvements), 4)
        self.assertEqual(failures, [])

    def test_cannot_credit_unrelated_error_as_memory_improvement(self):
        cases = evidence()
        for case in cases:
            if case["profile"] == "csv_default":
                case["outcome_code"] = "CONFIGURATION_ERROR"
        passed, improvements, failures = fixture.acceptance(cases)
        self.assertFalse(passed)
        self.assertEqual(improvements, [])
        self.assertIn("UNEXPECTED_WORKER_FAILURE", failures)
        self.assertIn("NO_LOW_MEMORY_IMPROVEMENT", failures)

    def test_control_must_succeed_at_same_low_memory(self):
        cases = evidence()
        for case in cases:
            if case["profile"] == "parquet_control" and case["duckdb_memory_limit_mb"] in (16, 32):
                case["outcome_code"] = "RESOURCE_EXHAUSTED"
        passed, improvements, failures = fixture.acceptance(cases)
        self.assertFalse(passed)
        self.assertEqual(improvements, [])
        self.assertIn("PARQUET_CONTROL_FAILED", failures)

    def test_reject_missing_case_bad_success_schema_or_no_rss(self):
        bad = evidence()[:-1]
        self.assertEqual(fixture.acceptance(bad)[2], ["INCOMPLETE_MATRIX"])
        for key, value, failure in (("schema", [], "INVALID_SUCCESS_EVIDENCE"),
                                    ("rows", [{"total": 4}], "INVALID_SUCCESS_EVIDENCE"),
                                    ("rss_samples", 0, "RSS_NOT_OBSERVED"),
                                    ("wire_bytes", 0, "INVALID_SUCCESS_EVIDENCE")):
            cases = copy.deepcopy(evidence())
            cases[1][key] = value
            self.assertIn(failure, fixture.acceptance(cases)[2])

    def test_required_high_memory_optins_cannot_fail(self):
        cases = evidence()
        for case in cases:
            if case["profile"] == "csv_1mib_256kib" and case["duckdb_memory_limit_mb"] == 64:
                case["outcome_code"] = "RESOURCE_EXHAUSTED"
        self.assertIn("OPT_IN_64MB_FAILED", fixture.acceptance(cases)[2])


if __name__ == "__main__":
    unittest.main()
