"""VM-only negative controls for release compatibility and failure evidence."""
import copy
import os
from pathlib import Path
import sys
import tempfile
import unittest

import release_upgrade_acceptance as gate


def original():
    return {"ready": True, "generation": "a" * 32, "fingerprint": "b" * 64,
            "schema_hash": "c" * 64, "sha256": "d" * 64, "rows": 3, "bytes": 512,
            "refreshed_at": "2026-10-03T00:00:00Z"}


def migration():
    return (f"verified: true\nsource_fingerprint: {'b' * 64}\nsource_generation_sha256: {'d' * 64}\nsnapshot:\n"
            f"    generation: {'a' * 32}\n    fingerprint: {'e' * 64}\n    schema_hash: {'c' * 64}\n"
            f"    sha256: {'f' * 64}\n    rows: 3\n    bytes: 512\n"
            "    refreshed_at: 2026-10-03T00:00:00Z\n").encode()


class CompatibilityEvidenceTests(unittest.TestCase):
    def test_identity_cannot_hide_freshness_authorization_or_content_change(self):
        gate.require_identity(original(), dict(original(), verified=True))
        for key in ("generation", "fingerprint", "schema_hash", "sha256", "rows", "bytes", "refreshed_at"):
            changed = dict(original(), **{key: "changed"})
            with self.subTest(field=key), self.assertRaises(gate.AcceptanceError):
                gate.require_identity(changed, original())

    def test_future_format_changes_only_one_unambiguous_version(self):
        source = b"version: 4\ndataset: saved\ncommitted:\n    rows: 3\n"
        self.assertEqual(gate.future_manifest(source), source.replace(b"version: 4", b"version: 999"))
        for bad in (b"dataset: saved\n", source + b"version: 4\n", source.replace(b"version: 4", b"version: bad")):
            with self.subTest(raw=bad), self.assertRaises(gate.AcceptanceError):
                gate.future_manifest(bad)

    def test_migration_preserves_age_schema_content_accounting_and_provenance(self):
        gate.migration_identity(migration(), original())
        for before, after in ((b"a" * 32, b"a" * 31 + b"0"), (b"c" * 64, b"0" * 64),
                              (b"rows: 3", b"rows: 4"), (b"bytes: 512", b"bytes: 513"),
                              (b"2026-10-03", b"2026-10-04"), (b"e" * 64, b"b" * 64),
                              (b"source_fingerprint: " + b"b" * 64, b"source_fingerprint: " + b"0" * 64),
                              (b"source_generation_sha256: " + b"d" * 64, b"source_generation_sha256: " + b"0" * 64),
                              (b"verified: true", b"verified: false")):
            with self.subTest(field=before), self.assertRaises(gate.AcceptanceError):
                gate.migration_identity(migration().replace(before, after), original())
        for extra in (b"source_fingerprint: " + b"b" * 64 + b"\n",
                      b"source_generation_sha256: " + b"d" * 64 + b"\n"):
            with self.assertRaises(gate.AcceptanceError):
                gate.migration_identity(migration() + extra, original())

    def test_read_compatibility_requires_actual_reader_identity_and_no_writes(self):
        gate.assert_reader_events([{"method": "GET", "identity": "reader"}, {"method": "HEAD", "identity": "reader"}])
        for invalid in ([], [{"method": "GET", "identity": "writer"}],
                        [{"method": "PUT", "identity": "reader"}], [{"method": "DELETE", "identity": "reader"}]):
            with self.subTest(events=invalid), self.assertRaises(gate.AcceptanceError):
                gate.assert_reader_events(invalid)

    def test_release_matrix_rejects_missing_duplicate_and_failed_scenarios(self):
        complete = [{"provider": p, "layout": layout, "passed": True} for p in ("local", *gate.PROVIDERS)
                    for layout in ("single", "multipart", "single_empty", "multipart_empty")]
        gate.validate_case_set(complete)
        failed = copy.deepcopy(complete)
        failed[-1]["passed"] = False
        for invalid in (complete[:-1], complete + [complete[0]], complete[:-1] + [complete[0]], failed):
            with self.assertRaises(gate.AcceptanceError):
                gate.validate_case_set(invalid)

    def test_payload_fingerprints_detect_mutation_without_treating_locks_as_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            payload = directory / "data.parquet"
            payload.write_bytes(b"original")
            before = gate.fingerprint_files(directory)
            (directory / ".lock").write_bytes(b"lock state")
            self.assertEqual(gate.fingerprint_files(directory), before)
            payload.write_bytes(b"changed")
            self.assertNotEqual(gate.fingerprint_files(directory), before)


class CommandControlTests(unittest.TestCase):
    def execute(self, source, *, success=False, timeout=5):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            program = directory / "program"
            program.write_text("#!" + sys.executable + "\n" + source)
            program.chmod(0o700)
            run = gate.UpgradeRun(program, program, program, directory)
            return run.cli(["version"], success=success, timeout=timeout)

    def test_clean_nonzero_exit_is_required_for_expected_rejection(self):
        self.execute("raise SystemExit(1)\n")
        for source in ("raise SystemExit(0)\n", "import os, signal\nos.kill(os.getpid(), signal.SIGKILL)\n"):
            with self.assertRaises(gate.AcceptanceError):
                self.execute(source)

    def test_failed_command_cannot_claim_verified_or_ready_success(self):
        for marker in ("ready: true", "verified: true"):
            with self.assertRaises(gate.AcceptanceError):
                self.execute("print(" + repr(marker) + ")\nraise SystemExit(1)\n")

    def test_output_and_runtime_have_bounds(self):
        with self.assertRaises(gate.AcceptanceError):
            self.execute("print('x' * (2 << 20))\nraise SystemExit(1)\n")
        with self.assertRaises(gate.AcceptanceError):
            self.execute("import time\ntime.sleep(30)\n", timeout=0.1)


if __name__ == "__main__":
    unittest.main()
