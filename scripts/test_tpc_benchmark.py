#!/usr/bin/env python3
"""Unit tests for the TPC answer comparator and stats helpers (no engines needed)."""
import decimal
import sys
import unittest
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import tpc_benchmark as tpc  # noqa: E402

D = decimal.Decimal


def rows(*columns):
    return tpc.canonical_rows(list(columns))


class CompareTest(unittest.TestCase):
    def test_exact_integers_and_strings(self):
        ref = rows((0, [1, 2]), (None, ["a", "b"]))
        self.assertEqual(tpc.compare(ref, rows((0, [1, 2]), (None, ["a", "b"])))[0], "exact_order")
        self.assertEqual(tpc.compare(ref, rows((0, [1, 3]), (None, ["a", "b"])))[0], "mismatch")
        self.assertEqual(tpc.compare(ref, rows((0, [1, 2]), (None, ["a", "B"])))[0], "mismatch")

    def test_integers_never_get_tolerance(self):
        big = 10**12
        self.assertEqual(tpc.compare(rows((0, [big])), rows((0, [big + 1])))[0], "mismatch")

    def test_decimal_vs_double_uses_half_unit_of_scale(self):
        ref = rows((None, [1234.5649999]))  # DuckDB AVG -> DOUBLE
        self.assertEqual(tpc.compare(ref, rows((2, [D("1234.56")])))[0], "exact_order")
        self.assertEqual(tpc.compare(ref, rows((2, [D("1234.58")])))[0], "mismatch")

    def test_float_relative_tolerance(self):
        self.assertEqual(tpc.compare(rows((None, [1e6])), rows((None, [1e6 + 1e-4])))[0], "exact_order")
        self.assertEqual(tpc.compare(rows((None, [1e6])), rows((None, [1e6 + 1])))[0], "mismatch")

    def test_nulls_and_row_counts(self):
        self.assertEqual(tpc.compare(rows((0, [None])), rows((0, [0])))[0], "mismatch")
        self.assertEqual(tpc.compare(rows((0, [1])), rows((0, [1, 1])))[0], "mismatch")

    def test_tie_reorder_is_reported_not_hidden(self):
        verdict, detail = tpc.compare(rows((0, [1, 2])), rows((0, [2, 1])))
        self.assertEqual(verdict, "matched_unordered")
        self.assertIn("row 0", detail)

    def test_percentile_and_stats(self):
        self.assertEqual(tpc.percentile([1, 2, 3, 4, 5], 50), 3)
        self.assertAlmostEqual(tpc.percentile([1, 2, 3, 4, 5], 95), 4.8)
        self.assertEqual(tpc.summary_stats([3, 1, 2]), {"median": 2, "min": 1, "max": 3, "n": 3})
        self.assertIsNone(tpc.summary_stats([]))


if __name__ == "__main__":
    unittest.main()
