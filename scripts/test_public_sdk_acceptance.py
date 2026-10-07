#!/usr/bin/env python3
"""Linux process-lifecycle controls for the public SDK acceptance runner."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

sys.dont_write_bytecode = True
import public_sdk_acceptance as fixture


FORK_CHILD = textwrap.dedent("""\
    import os
    from pathlib import Path
    import signal
    import sys
    import time

    ready = Path(sys.argv[1])
    if os.fork() == 0:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        ready.write_text(str(os.getpid()))
        while True:
            time.sleep(60)
    while not ready.exists():
        time.sleep(0.01)
    if sys.argv[2] == "timeout":
        while True:
            time.sleep(60)
    sys.exit(7 if sys.argv[2] == "failure" else 0)
""")


@unittest.skipUnless(sys.platform == "linux", "Linux subreaper lifecycle controls")
class ProcessLifecycleTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.report = {"stages": []}
        self.addCleanup(self.cleanup_failed_fixture)

    def cleanup_failed_fixture(self):
        # A failing assertion or runner regression must not leave the test's
        # deliberately stubborn child alive. Never address another stage/group.
        for stage in self.report["stages"]:
            group = stage.get("process_group")
            if group is None:
                continue
            try:
                os.killpg(group, signal.SIGKILL)
            except ProcessLookupError:
                continue
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    child, _ = os.waitpid(-group, os.WNOHANG)
                except ChildProcessError:
                    break
                if child == 0:
                    time.sleep(0.01)

    def run_stage(self, command, timeout=5):
        fixture.run_stage(self.report, self.directory, "fixture", command,
                          self.directory, os.environ.copy(), timeout=timeout,
                          settle_timeout=0.05, term_timeout=0.1, kill_timeout=2)

    def assert_group_reaped(self):
        stage = self.report["stages"][-1]
        self.assertTrue(stage["cleanup"]["parent_reaped"])
        self.assertTrue(stage["cleanup"]["process_group_gone"])
        with self.assertRaises(ProcessLookupError):
            os.killpg(stage["process_group"], 0)
        child_file = self.directory / "child.pid"
        if child_file.exists():
            with self.assertRaises(ProcessLookupError):
                os.kill(int(child_file.read_text()), 0)
        return stage

    def fork_command(self, mode):
        return [sys.executable, "-c", FORK_CHILD, str(self.directory / "child.pid"), mode]

    def test_success_requires_an_empty_process_group(self):
        self.run_stage([sys.executable, "-c", "print('complete')"])
        stage = self.assert_group_reaped()
        self.assertTrue(stage["passed"])
        self.assertEqual(stage["exit"], 0)
        self.assertFalse(stage["timed_out"])
        self.assertFalse(stage["cleanup"]["term_sent"])
        self.assertFalse(stage["cleanup"]["kill_sent"])
        self.assertEqual((self.directory / "fixture.log").read_text(), "complete\n")

    def test_successful_parent_with_descendant_fails_after_reaping(self):
        with self.assertRaisesRegex(RuntimeError, "left descendant processes"):
            self.run_stage(self.fork_command("success"))
        stage = self.assert_group_reaped()
        self.assertFalse(stage["passed"])
        self.assertEqual(stage["exit"], 0)
        self.assertTrue(stage["cleanup"]["descendants_after_exit"])
        self.assertTrue(stage["cleanup"]["term_sent"])
        self.assertTrue(stage["cleanup"]["kill_sent"])

    def test_failed_parent_preserves_exit_and_reaps_descendant(self):
        with self.assertRaisesRegex(RuntimeError, "failed; see retained log"):
            self.run_stage(self.fork_command("failure"))
        stage = self.assert_group_reaped()
        self.assertFalse(stage["passed"])
        self.assertEqual(stage["exit"], 7)
        self.assertEqual(stage["failure"], "RuntimeError")
        self.assertTrue(stage["cleanup"]["kill_sent"])

    def test_timeout_preserves_exception_and_kills_stubborn_descendant(self):
        start = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            self.run_stage(self.fork_command("timeout"), timeout=1)
        stage = self.assert_group_reaped()
        self.assertTrue((self.directory / "child.pid").exists())
        self.assertFalse(stage["passed"])
        self.assertTrue(stage["timed_out"])
        self.assertEqual(stage["failure"], "TimeoutExpired")
        self.assertTrue(stage["cleanup"]["term_sent"])
        self.assertTrue(stage["cleanup"]["kill_sent"])
        self.assertLess(time.monotonic() - start, 5)

    def test_start_failure_is_recorded_without_claiming_a_started_process(self):
        with self.assertRaises(FileNotFoundError):
            self.run_stage([str(self.directory / "missing-command")])
        stage = self.report["stages"][-1]
        self.assertFalse(stage["passed"])
        self.assertFalse(stage["process_started"])
        self.assertIsNone(stage["exit"])
        self.assertEqual(stage["failure"], "FileNotFoundError")
        self.assertTrue(stage["cleanup"]["process_group_gone"])


class SourceProvenanceTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.source = self.directory / "source"
        self.source.mkdir()
        self.output = self.directory / "output"
        self.output.mkdir()
        self.report = {"provenance": {}}
        (self.source / "go.mod").write_text("module example.test/application\n")
        (self.source / "main.go").write_text("package main\n")

    def test_source_manifest_ignores_only_ephemeral_metadata_and_verifies_unchanged(self):
        (self.source / ".git").write_text("gitdir: private metadata")
        cached = self.source / "__pycache__"
        cached.mkdir()
        (cached / "cache.pyc").write_bytes(b"generated bytecode")
        before = fixture.capture_source(self.report, self.output, "sdk", self.source)
        self.assertEqual(set(before), {"go.mod", "main.go"})
        fixture.verify_source(self.report, self.output, "sdk", self.source, before)
        self.assertTrue(self.report["provenance"]["sdk"]["unchanged_after_execution"])
        self.assertEqual(self.report["provenance"]["sdk"]["manifest_sha256"],
                         self.report["provenance"]["sdk"]["after_manifest_sha256"])

    def test_changed_added_and_removed_source_fail_the_gate(self):
        before = fixture.capture_source(self.report, self.output, "application", self.source)
        (self.source / "go.mod").write_text("module changed.test/application\n")
        (self.source / "main.go").unlink()
        (self.source / "extra.go").write_text("package changed\n")
        with self.assertRaisesRegex(RuntimeError, "source changed"):
            fixture.verify_source(self.report, self.output, "application", self.source, before)
        evidence = self.report["provenance"]["application"]
        self.assertFalse(evidence["unchanged_after_execution"])
        self.assertEqual(evidence["changed"], ["go.mod"])
        self.assertEqual(evidence["added"], ["extra.go"])
        self.assertEqual(evidence["removed"], ["main.go"])
        self.assertTrue((self.output / evidence["after_manifest_file"]).is_file())

    def test_symlinked_source_is_rejected_without_following_external_content(self):
        outside = self.directory / "outside.go"
        outside.write_text("package outside\n")
        (self.source / "linked.go").symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "nonregular"):
            fixture.source_manifest(self.source)

    def test_effective_application_manifest_can_capture_intentional_pin_before_build(self):
        template = fixture.source_manifest(self.source)
        (self.source / "go.mod").write_text("module example.test/application\nrequire example.test/sdk v1.0.0\n")
        pinned = fixture.capture_source(self.report, self.output, "application", self.source)
        self.assertNotEqual(fixture.manifest_digest(template), fixture.manifest_digest(pinned))
        fixture.verify_source(self.report, self.output, "application", self.source, pinned)
        self.assertTrue(self.report["provenance"]["application"]["unchanged_after_execution"])


class DependencyBoundaryTests(unittest.TestCase):
    def test_dbapi_and_postgres_driver_packages_are_rejected(self):
        for path in ("syne_db", "syne_db/api/handlers", "syne_db [syne_db.test]",
                     "github.com/SYNEHQ/db.api.go", "github.com/SYNEHQ/db.api.go/api/handlers",
                     "github.com/synehq/db.api.go/api/utils", "github.com/lib/pq",
                     "github.com/lib/pq/auth/scram"):
            with self.subTest(path=path):
                self.assertTrue(fixture.forbidden_package({"ImportPath": path}))

    def test_declared_module_identity_is_checked_independently_of_import_path(self):
        for module in ("syne_db", "github.com/SYNEHQ/db.api.go", "github.com/lib/pq"):
            with self.subTest(module=module):
                package = {"ImportPath": "example.com/renamed/package", "Module": {"Path": module}}
                self.assertTrue(fixture.forbidden_package(package))

    def test_similarly_named_packages_do_not_cross_the_boundary(self):
        for path in ("syne_db_tools", "syne_db_tools/api", "github.com/lib/pquery",
                     "github.com/SYNEHQ/db.api.go-tools", "github.com/synehq/db.api.go2",
                     "github.com/jackc/pgxhelper"):
            with self.subTest(path=path):
                self.assertFalse(fixture.forbidden_package({"ImportPath": path, "Module": {"Path": path}}))

    def test_normal_public_sdk_dependencies_remain_allowed(self):
        for name in ("client", "query", "delegation", "resolver", "operations"):
            with self.subTest(name=name):
                self.assertFalse(fixture.forbidden_package({"ImportPath": fixture.MODULE + "/" + name,
                                                            "Module": {"Path": fixture.MODULE}}))
        for path, module in (("github.com/apache/arrow-go/v18/arrow", "github.com/apache/arrow-go/v18"),
                             ("go.yaml.in/yaml/v3", "go.yaml.in/yaml/v3"), ("net/http", "")):
            with self.subTest(path=path):
                self.assertFalse(fixture.forbidden_package({"ImportPath": path, "Module": {"Path": module}}))

    def test_existing_runtime_connector_and_broker_boundaries_remain_rejected(self):
        for path in (fixture.MODULE + "/internal/cluster", "github.com/duckdb/duckdb-go/v2",
                     "github.com/jackc/pgx/v5", "github.com/go-sql-driver/mysql",
                     "github.com/nats-io/nats.go", "go.mongodb.org/mongo-driver/v2/mongo"):
            with self.subTest(path=path):
                self.assertTrue(fixture.forbidden_package({"ImportPath": path}))


if __name__ == "__main__":
    unittest.main()
