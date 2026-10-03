"""VM-only negative controls for backup evidence and exact recovered Arrow values."""
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa

spec = importlib.util.spec_from_file_location("backup_acceptance", Path(__file__).with_name("backup_acceptance.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


def snapshot(status="ready"):
    return (f"{status}: true\nsnapshot:\n"
            f"    generation: {'a' * 32}\n    fingerprint: {'b' * 64}\n"
            f"    schema_hash: {'c' * 64}\n    sha256: {'d' * 64}\n"
            '    rows: 4\n    bytes: 512\n    refreshed_at: 2026-10-03T00:00:00Z\n').encode()


def encode(table):
    output = io.BytesIO()
    with pa.ipc.new_stream(output, table.schema) as writer:
        writer.write_table(table)
    return output.getvalue()


class BackupEvidenceTests(unittest.TestCase):
    def test_complete_identity_and_explicit_status(self):
        original = fixture.scalar_snapshot(snapshot())
        copied = fixture.scalar_snapshot(snapshot("verified"), "verified")
        self.assertTrue(copied["verified"])
        self.assertEqual(fixture.identity(original), fixture.identity(copied))
        self.assertFalse(fixture.scalar_snapshot(snapshot().replace(b"ready: true", b"ready: false"))["ready"])

    def test_identity_keeps_authorization_freshness_and_content(self):
        original = fixture.scalar_snapshot(snapshot())
        for key in ("generation", "fingerprint", "schema_hash", "sha256", "rows", "bytes", "refreshed_at"):
            changed = dict(original)
            changed[key] = "changed"
            with self.subTest(key=key):
                self.assertNotEqual(fixture.identity(original), fixture.identity(changed))

    def test_rejects_incomplete_ambiguous_or_malformed_cli_evidence(self):
        good = snapshot()
        for raw in (good.replace(b"ready: true\n", b""), good + b"ready: true\n",
                    good + b"    rows: 4\n", good.replace(b"schema_hash:", b"missing:"),
                    good.replace(b"a" * 32, b"not-generation"), good.replace(b"c" * 64, b"bad-hash"),
                    good.replace(b"snapshot:", b"missing:"), good + b" " * (1 << 20)):
            with self.subTest(size=len(raw)):
                with self.assertRaises(fixture.AcceptanceError):
                    fixture.scalar_snapshot(raw)

    def test_checks_all_values_nulls_types_and_empty_schema(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "result.arrow"
            for rows in (0, 20):
                expected = fixture.source_table(rows)
                path.write_bytes(encode(expected))
                fixture.verify_arrow(path, expected)
                wrong_type = expected.set_column(0, "row_id", pa.array(range(rows), type=pa.int32()))
                path.write_bytes(encode(wrong_type))
                with self.assertRaises(fixture.AcceptanceError):
                    fixture.verify_arrow(path, expected)
            expected = fixture.source_table(20)
            for values in ([None] * 20, list(range(1, 21)), list(reversed(range(20)))):
                changed = expected.set_column(0, "row_id", pa.array(values, type=pa.int64()))
                path.write_bytes(encode(changed))
                with self.assertRaises(fixture.AcceptanceError):
                    fixture.verify_arrow(path, expected)

    def test_rejects_truncation_and_concatenated_arrow_streams(self):
        expected = fixture.source_table(20)
        good = encode(expected)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "result.arrow"
            for raw in (b"", b"bad" + fixture.EOS, good[:-8], good + good, good + fixture.EOS):
                with self.subTest(size=len(raw)):
                    path.write_bytes(raw)
                    with self.assertRaises(fixture.AcceptanceError):
                        fixture.verify_arrow(path, expected)


if __name__ == "__main__":
    unittest.main()
