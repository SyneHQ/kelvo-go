#!/usr/bin/env python3
"""Exercise provisioning in clean checkouts without downloads or Go builds."""
import contextlib
import io
import json
import os
import pathlib
import subprocess
import sys
import tarfile
import tempfile
import types
import unittest
from unittest import mock

import provision_duckbridge


SCRIPTS = pathlib.Path(__file__).resolve().parent
HELPER = vars(provision_duckbridge)


class ProvisionDriverTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="kelvo-provision-test-")
        self.addCleanup(self.temporary.cleanup)
        root = pathlib.Path(self.temporary.name)
        self.source = root / "checkout"
        (self.source / "scripts").mkdir(parents=True)
        (self.source / "go.mod").write_text("module fixture\n")
        (self.source / "go.sum").write_text("")
        (self.source / "scripts/duckbridge-driver.patch").write_bytes(
            (SCRIPTS / "duckbridge-driver.patch").read_bytes())
        self.module = root / "module"
        self.module.mkdir()
        (self.module / "go.mod").write_text("module " + HELPER["MODULE"] + "\n")
        self.artifact = self.source / "artifacts/duckbridge"
        self.artifact.mkdir(parents=True)
        self.archive = self.artifact / "duckdb-v1.5.6.tar.gz"
        with tarfile.open(self.archive, "w:gz") as archive:
            names = ["duckdb/function/table/arrow.hpp"] + [f"fixture_{i}.hpp" for i in range(100)]
            for name in names:
                member = tarfile.TarInfo("duckdb-1.5.6/src/include/" + name)
                member.size = 1
                archive.addfile(member, io.BytesIO(b"\n"))
        self.accessor = self.artifact / "duckdb-go/native_connection.go"
        self.real_run = subprocess.run
        self.mod_edits = 0

    def provision(self):
        def run(command, **kwargs):
            if command[0] == "fixture-go":
                self.assertEqual(command[1:3], ["mod", "edit"])
                self.mod_edits += 1
                return subprocess.CompletedProcess(command, 0)
            # Preserve real Git behavior while keeping expected failures quiet.
            return self.real_run(command, capture_output=True, **kwargs)

        module = {"Sum": HELPER["MODULE_SUM"], "Version": HELPER["MODULE_VERSION"],
                  "Dir": str(self.module)}
        arguments = ["provision_duckbridge.py", str(self.artifact), "--go", "fixture-go",
                     "--source-directory", str(self.source)]
        with mock.patch.dict(HELPER, {"ARCHIVE_SHA256": HELPER["digest"](self.archive)}), \
                mock.patch.object(sys, "argv", arguments), \
                mock.patch.object(os, "uname", return_value=types.SimpleNamespace(sysname="Linux", machine="x86_64")), \
                mock.patch.object(subprocess, "check_output", return_value=json.dumps(module)), \
                mock.patch.object(subprocess, "run", side_effect=run), \
                contextlib.redirect_stdout(io.StringIO()):
            HELPER["main"]()

    def init_checkout(self):
        self.real_run(["git", "init", "--quiet", str(self.source)], check=True)

    def test_nested_checkout_applies_the_driver_patch(self):
        self.init_checkout()
        self.provision()
        self.assertIn("func (conn *Conn) WithNativeConnection", self.accessor.read_text())
        self.assertFalse((self.source / "native_connection.go").exists())
        self.assertEqual(self.mod_edits, 1)

    def test_inherited_git_repository_does_not_redirect_the_patch(self):
        self.init_checkout()
        with mock.patch.dict(os.environ, {"GIT_DIR": str(self.source / ".git"),
                                          "GIT_WORK_TREE": str(self.source)}):
            self.provision()
        self.assertTrue(self.accessor.is_file())

    def test_git_free_source_still_applies_the_patch(self):
        self.provision()
        self.assertTrue(self.accessor.is_file())

    def test_matching_marker_cannot_hide_a_missing_accessor(self):
        self.init_checkout()
        self.provision()
        marker = self.artifact / "driver-patch.sha256"
        self.assertEqual(marker.read_text().strip(), HELPER["digest"](
            self.source / "scripts/duckbridge-driver.patch"))
        self.accessor.unlink()
        with self.assertRaisesRegex(ValueError, "patch is missing or changed"):
            self.provision()
        self.assertEqual(self.mod_edits, 1)

    def test_matching_marker_cannot_hide_a_changed_accessor(self):
        self.init_checkout()
        self.provision()
        self.accessor.write_text(self.accessor.read_text().replace("WithNativeConnection", "ChangedAccessor"))
        with self.assertRaisesRegex(ValueError, "patch is missing or changed"):
            self.provision()
        self.assertEqual(self.mod_edits, 1)


if __name__ == "__main__":
    unittest.main()
