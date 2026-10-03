#!/usr/bin/env python3
"""Control-flow tests for notebook acceptance; these are not notebook evidence."""
from argparse import Namespace
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import nbformat

SPEC = importlib.util.spec_from_file_location("notebook_acceptance", Path(__file__).with_name("notebook_acceptance.py"))
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def notebook():
    value = nbformat.v4.new_notebook(cells=[
        nbformat.v4.new_code_cell('KELVO_NOTEBOOK_REF = "' + 'a' * 40 + '"\nKELVO_HELPER_SHA256 = "' + 'b' * 64 + '"',
                                 metadata={"tags": ["bootstrap"]}),
        nbformat.v4.new_code_cell("assert 1 == 1"),
        nbformat.v4.new_code_cell("pass", metadata={"tags": ["cleanup"]}),
    ])
    return value


class NotebookAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.path = self.root / "lesson.ipynb"
        self.value = notebook()
        self.write()

    def write(self):
        nbformat.write(self.value, self.path)

    def test_clean_pinned_source_is_accepted(self):
        value, pins = gate.validate_notebook(self.path, helper_sha256="b" * 64)
        self.assertTrue(pins["pins_verified"])
        self.assertEqual(len(gate.expected_code_cells(value)), 3)

    def test_source_outputs_and_counts_are_rejected(self):
        for field, value in (("execution_count", 1), ("outputs", [nbformat.v4.new_output("stream", text="secret")])):
            with self.subTest(field=field):
                self.value = notebook()
                self.value.cells[1][field] = value
                self.write()
                with self.assertRaises(gate.ValidationError):
                    gate.validate_notebook(self.path)

    def test_skip_and_permitted_error_tags_are_rejected(self):
        for tag in gate.FORBIDDEN_TAGS:
            with self.subTest(tag=tag):
                self.value.cells[1].metadata.tags = [tag]
                self.write()
                with self.assertRaises(gate.ValidationError):
                    gate.validate_notebook(self.path)

    def test_placeholders_require_explicit_draft_mode(self):
        self.value.cells[0].source = 'KELVO_NOTEBOOK_REF = "NOTEBOOK_COMMIT"\nKELVO_HELPER_SHA256 = "HELPER_SHA256"'
        self.write()
        with self.assertRaises(gate.ValidationError):
            gate.validate_notebook(self.path)
        _, pins = gate.validate_notebook(self.path, allow_unpinned=True)
        self.assertFalse(pins["pins_verified"])

    def test_incorrect_helper_digest_cannot_be_publish_ready(self):
        with self.assertRaises(gate.ValidationError):
            gate.validate_notebook(self.path, helper_sha256="c" * 64)
        _, pins = gate.validate_notebook(self.path, helper_sha256="c" * 64, allow_unpinned=True)
        self.assertFalse(pins["pins_verified"])

    def test_missing_execution_count_fails_even_without_error_output(self):
        for count, cell in enumerate(self.value.cells, 1):
            cell.execution_count = count
        self.value.cells[1].execution_count = None
        with self.assertRaises(gate.ValidationError):
            gate.verify_execution(self.value)

    def test_error_output_cannot_pass(self):
        for count, cell in enumerate(self.value.cells, 1):
            cell.execution_count = count
        self.value.cells[1].outputs = [nbformat.v4.new_output("error", ename="AssertionError", evalue="", traceback=[])]
        with self.assertRaises(gate.ValidationError):
            gate.verify_execution(self.value)

    def test_failure_emits_executed_copy_but_preserves_source(self):
        original = self.path.read_bytes()
        output = self.root / "executed"
        output.mkdir()
        def fail(value, **kwargs):
            value.cells[0].execution_count = 1
            raise RuntimeError("DO_NOT_PUBLISH_PRIVATE_TOKEN")
        report = gate.execute_case(self.path, self.value, directory=output, environment={},
                                   cell_timeout=1, pins={}, execute=fail)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["executed_code_cells"], 1)
        self.assertEqual(self.path.read_bytes(), original)
        self.assertTrue((output / self.path.name).exists())
        self.assertNotIn("DO_NOT_PUBLISH_PRIVATE_TOKEN", json.dumps(report))

    def test_fake_success_with_no_cells_executed_is_failure(self):
        output = self.root / "executed"
        output.mkdir()
        report = gate.execute_case(self.path, self.value, directory=output, environment={},
                                   cell_timeout=1, pins={}, execute=lambda *args, **kwargs: None)
        self.assertEqual(report["status"], "failed")

    def test_artifacts_cannot_overwrite_notebooks_or_existing_runs(self):
        with self.assertRaises(gate.ValidationError):
            gate.prepare_output(self.root, self.root / "notebooks")
        created = gate.prepare_output(self.root, "artifacts/run")
        self.assertTrue(created.is_dir())
        with self.assertRaises(FileExistsError):
            gate.prepare_output(self.root, "artifacts/run")
        external = self.root / "outside"
        external.mkdir()
        (self.root / "artifacts" / "link").symlink_to(external, target_is_directory=True)
        with self.assertRaises(gate.ValidationError):
            gate.prepare_output(self.root, "artifacts/link/run")

    def test_remote_environment_cannot_enable_external_requests(self):
        with mock.patch.dict(os.environ, {"KELVO_REMOTE_URL": "https://private.invalid", "KELVO_REMOTE_TOKEN": "SECRET"}):
            env = gate.kernel_environment(Path("binary"), Path("helper"), Path("cache"))
        self.assertNotIn("KELVO_REMOTE_URL", env)
        self.assertNotIn("KELVO_REMOTE_TOKEN", env)
        self.assertEqual(env["KELVO_NOTEBOOK_SKIP_INSTALL"], "1")
        self.assertEqual(env["KELVO_BINARY"], "binary")

    def test_dataset_hash_mismatch_rejects_untrusted_cache(self):
        (self.root / "data.csv").write_text("changed")
        with self.assertRaises(gate.ValidationError):
            gate.dataset_provenance(self.root, {"data.csv": ("https://public.invalid/data.csv", "0" * 64, 1000)})

    def test_setup_failure_still_has_nonzero_failure_evidence(self):
        args = Namespace(output_dir=Path("artifacts/failure"), binary=Path("missing"),
                         data_cache=self.root, notebook=[], cell_timeout=1, allow_unpinned=True)
        with mock.patch.object(gate.importlib.metadata, "version", return_value="test"):
            result = gate.run(args, repo=self.root)
        report = json.loads((self.root / "artifacts/failure/report.json").read_text())
        self.assertEqual(result, 1)
        self.assertEqual(report["status"], "failed")
        self.assertFalse(report["complete_curriculum_passed"])
        self.assertFalse(report["frozen_curriculum_passed"])


if __name__ == "__main__":
    unittest.main()
