#!/usr/bin/env python3
"""Negative controls for kernel acceptance, source identity and owned cleanup."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import subprocess
import tempfile
import unittest
from unittest import mock

import containment_acceptance as fixture


def valid_report():
    files = {name: "a" * 64 for name in fixture.SOURCE_REQUIRED}
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return {
        "gates": {name: "pass" for name in fixture.REQUIRED},
        "non_root": True, "capability_sets_zero": True,
        "source": {"verified": True, "file_count": len(files), "files": files,
                   "sha256": hashlib.sha256(canonical).hexdigest(), "base_revision": "c" * 40,
                   "excluded_metadata": [".DS_Store", "._*"]},
        "source_unchanged": True,
        "binary_sha256": {name: "b" * 64 for name in fixture.BINARIES},
        "remaining_job_groups": 0, "remaining_ownership_records": 0,
        "service_exit_code": 0, "owned_service_removed": True, "owned_cgroup_removed": True,
    }


class ContainmentControls(unittest.TestCase):
    def test_every_gate_is_required_and_must_pass(self):
        self.assertTrue(fixture.reconcile(valid_report()))
        for name in fixture.REQUIRED:
            for state in ("missing", "skip", "fail", "duplicate", True, None):
                report = valid_report()
                report["gates"][name] = state
                self.assertFalse(fixture.reconcile(report), (name, state))
            report = valid_report()
            del report["gates"][name]
            self.assertFalse(fixture.reconcile(report), name)

    def test_parser_never_overwrites_failed_or_duplicate_attempts_with_pass(self):
        name = fixture.KERNEL_GATES[0]
        for first in ("PASS", "FAIL", "SKIP"):
            output = f"--- {first}: {name} (0.01s)\n--- PASS: {name} (0.01s)\n"
            self.assertEqual(fixture.gates(output, [name]), {name: "duplicate"})
        self.assertEqual(fixture.gates("", [name]), {name: "missing"})
        self.assertEqual(fixture.gates(f"--- SKIP: {name} (0s)\n", [name]), {name: "skip"})

    def test_unproven_cleanup_or_process_status_never_passes(self):
        for field in ("remaining_job_groups", "remaining_ownership_records", "service_exit_code"):
            for value in (1, -1, None, False, "0"):
                report = valid_report()
                report[field] = value
                self.assertFalse(fixture.reconcile(report), (field, value))
        for field in ("owned_service_removed", "owned_cgroup_removed", "non_root", "capability_sets_zero"):
            for value in (False, None, 1, "true"):
                report = valid_report()
                report[field] = value
                self.assertFalse(fixture.reconcile(report), (field, value))
        for field in ("failure", "execution_failed"):
            report = valid_report()
            report[field] = True
            self.assertFalse(fixture.reconcile(report))

    def test_source_and_binary_identity_are_required(self):
        for field, value in (("source_unchanged", False), ("source", None), ("binary_sha256", None), ("gates", [])):
            report = valid_report()
            report[field] = value
            self.assertFalse(fixture.reconcile(report))
        for value in ("", "A" * 64, "f" * 63, None, 123):
            report = valid_report()
            report["source"]["sha256"] = value
            self.assertFalse(fixture.reconcile(report))
        for name in fixture.BINARIES:
            report = valid_report()
            del report["binary_sha256"][name]
            self.assertFalse(fixture.reconcile(report))
        report = valid_report()
        report["source"]["file_count"] = False
        self.assertFalse(fixture.reconcile(report))

    def test_reconciliation_rejects_tampered_or_inconsistent_source_inventory(self):
        for field, value in (("files", None), ("file_count", 3), ("file_count", 5),
                             ("base_revision", "invalid"), ("excluded_metadata", [])):
            report = valid_report()
            report["source"][field] = value
            self.assertFalse(fixture.reconcile(report), field)
        report = valid_report()
        report["source"]["files"]["go.mod"] = "d" * 64
        self.assertFalse(fixture.reconcile(report))
        for name, value in (("../escape", "a" * 64), ("/absolute", "a" * 64),
                            (".DS_Store", "a" * 64), ("internal/._metadata", "a" * 64),
                            ("invalid-digest", "x" * 64)):
            report = valid_report()
            files = report["source"]["files"]
            files[name] = value
            report["source"]["file_count"] = len(files)
            canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
            report["source"]["sha256"] = hashlib.sha256(canonical).hexdigest()
            self.assertFalse(fixture.reconcile(report), name)
        report = valid_report()
        files = report["source"]["files"]
        del files["go.mod"]
        report["source"]["file_count"] = len(files)
        canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
        report["source"]["sha256"] = hashlib.sha256(canonical).hexdigest()
        self.assertFalse(fixture.reconcile(report))

    def test_source_inventory_detects_changes_and_excludes_metadata_explicitly(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            names = ["go.mod", "go.sum", "sandbox/launcher.c", "internal/containment/manager_linux.go", ".DS_Store", "internal/containment/._manager_linux.go"]
            for name in names:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fixture")
            def git(command, **kwargs):
                output = "\0".join(names) + "\0" if "ls-files" in command else "c" * 40 + "\n"
                return subprocess.CompletedProcess(command, 0, output)
            with mock.patch.object(fixture, "run", side_effect=git):
                original = fixture.source_manifest(root)
                self.assertEqual(original["file_count"], 4)
                self.assertEqual(original["excluded_metadata"], [".DS_Store", "._*"])
                self.assertFalse(any(Path(name).name.startswith(".") for name in original["files"]))
                (root / "go.sum").write_text("changed")
                self.assertNotEqual(fixture.source_manifest(root)["sha256"], original["sha256"])
                (root / "go.sum").unlink()
                with self.assertRaises(RuntimeError):
                    fixture.source_manifest(root)

    def test_source_inventory_rejects_symlinks_and_escape_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "outside").write_text("fixture")
            (root / "go.mod").symlink_to(root / "outside")
            for name in ("go.mod", "../outside", "/absolute"):
                with mock.patch.object(fixture, "run", return_value=subprocess.CompletedProcess([], 0, name + "\0")):
                    with self.assertRaises(RuntimeError):
                        fixture.source_manifest(root)

    def test_cleanup_targets_only_the_exact_owned_unit_and_checks_cgroup(self):
        unit = "kelvo-containment-testfixture"
        with mock.patch.object(fixture, "run", side_effect=[subprocess.CompletedProcess([], 0, ""), subprocess.CompletedProcess([], 0, "not-found\n")]) as run, \
                mock.patch.object(Path, "exists", return_value=False):
            self.assertEqual(fixture.cleanup_owned(unit), {"owned_service_removed": True, "owned_cgroup_removed": True})
            self.assertEqual(run.call_args_list[0].args[0], ["sudo", "-n", "systemctl", "stop", unit + ".service"])
            self.assertEqual(run.call_args_list[1].args[0][2], unit + ".service")
        with mock.patch.object(fixture, "run", side_effect=subprocess.TimeoutExpired([], 20)):
            result = fixture.cleanup_owned(unit)
            self.assertFalse(result["owned_service_removed"])
            self.assertFalse(result["owned_cgroup_removed"])
            self.assertTrue(result["cleanup_failure"])

    def test_cleanup_does_not_equate_failed_status_lookup_with_absent_unit(self):
        with mock.patch.object(fixture, "run", side_effect=[subprocess.CompletedProcess([], 0, ""), subprocess.CompletedProcess([], 1, "not-found\n")]), \
                mock.patch.object(Path, "exists", return_value=False):
            self.assertFalse(fixture.cleanup_owned("kelvo-containment-testfixture")["owned_service_removed"])

    def test_unowned_service_fails_with_retained_missing_gate_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            args = SimpleNamespace(artifact=temporary, unit="kelvo-containment-testfixture")
            with mock.patch.object(Path, "read_text", return_value="0::/user.slice/unrelated.service\n"), \
                    mock.patch.object(fixture, "run") as run:
                self.assertEqual(fixture.inside(args), 1)
                run.assert_not_called()
            report = json.loads((Path(temporary) / "inside.json").read_text())
            self.assertTrue(report["failure"])
            self.assertTrue(all(value == "missing" for value in report["gates"].values()))

    def test_wrong_fixture_identity_never_runs_outside_probe(self):
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            (artifact / "fixture-ready.json").write_text(json.dumps({"jobs": "/unrelated/jobs", "state": str(artifact / "state")}))
            with mock.patch.object(fixture, "wait_for_file"), mock.patch.object(fixture, "run") as run:
                with self.assertRaisesRegex(RuntimeError, "ownership mismatch"):
                    fixture.outside_check(artifact, "kelvo-containment-testfixture", mock.Mock())
                run.assert_not_called()

    def test_all_five_capability_sets_must_be_zero(self):
        names = ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb")
        status = "".join(name + ":\t0000000000000000\n" for name in names)
        self.assertTrue(fixture.zero_capabilities(status))
        for name in names:
            self.assertFalse(fixture.zero_capabilities(status.replace(name + ":\t0000000000000000\n", "")))
            self.assertFalse(fixture.zero_capabilities(status.replace(name + ":\t0000000000000000", name + ":\t0000000000000001")))

    def test_valid_inside_probe_does_not_inherit_negative_probe_flag(self):
        with mock.patch.dict(fixture.os.environ, {"KELVO_TEST_EXPECT_PLACEMENT_REJECTION": "yes"}):
            env = fixture.test_environment(Path("/fixture"), "/owned/jobs", "/owned/state")
        self.assertNotIn("KELVO_TEST_EXPECT_PLACEMENT_REJECTION", env)


if __name__ == "__main__":
    unittest.main()
