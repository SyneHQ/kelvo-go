#!/usr/bin/env python3
"""VM-safe runner tests: synthetic commands only, no Go build or trust mutation."""

import contextlib
import io
import json
import os
from pathlib import Path
import signal
import stat
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import storage_conformance as gate


def event(action, test=None, package=gate.PACKAGE, **extra):
    value = {"Package": package, "Action": action, **extra}
    if test is not None:
        value["Test"] = test
    return json.dumps(value).encode() + b"\n"


class StorageConformanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()

    def tearDown(self):
        self.temporary.cleanup()

    def proof(self, *events):
        result = gate.GoProof(("TestRequired",))
        for item in events:
            result.observe(item)
        return result

    def test_named_pass_and_package_pass_are_both_required(self):
        self.assertTrue(self.proof(event("pass", "TestRequired"), event("pass")).complete())
        self.assertFalse(self.proof(event("pass")).complete())  # no tests selected
        self.assertFalse(self.proof(event("pass", "TestRequired")).complete())
        self.assertFalse(self.proof(event("pass", "TestOther"), event("pass")).complete())
        self.assertFalse(self.proof(event("pass", "TestRequired", package="other"), event("pass")).complete())

    def test_skipped_or_failed_children_cannot_hide_behind_parent_pass(self):
        for action in ("skip", "fail"):
            proof = self.proof(event(action, "TestRequired/subcase"), event("pass", "TestRequired"), event("pass"))
            self.assertFalse(proof.complete())
            self.assertEqual(proof.public()["missing_or_rejected_tests"], ["TestRequired"])
        self.assertFalse(self.proof(event("skip", "TestRequired"), event("pass")).complete())

    def test_malformed_and_private_events_are_not_public_evidence(self):
        proof = self.proof(b"private-token=SECRET\n", b"[]\n", b"null\n",
                           event("output", "TestRequired", Output="/private/source SECRET"),
                           event("pass", "TestRequired"), event("pass"))
        encoded = json.dumps(proof.public())
        self.assertTrue(proof.complete())
        self.assertNotIn("SECRET", encoded)
        self.assertNotIn("/private", encoded)

    def test_paths_reject_relative_symlink_existing_report_and_unsafe_work(self):
        with self.assertRaises(gate.GateError):
            gate.directory(Path("relative"))
        link = self.root / "link"
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(gate.GateError):
            gate.directory(link)
        output = self.root / "report.json"
        self.assertEqual(gate.output_path(output), output)
        output.write_text("preserve me")
        with self.assertRaises(gate.GateError):
            gate.output_path(output)
        self.assertEqual(output.read_text(), "preserve me")
        work = self.root / "work"
        work.mkdir(mode=0o777)
        work.chmod(0o777)
        with self.assertRaises(gate.GateError):
            gate.directory(work, private=True)
        work.chmod(0o700)
        self.assertEqual(gate.directory(work, private=True), work)
        with self.assertRaises(gate.GateError):
            gate.regular_file(output, executable=True)
        with self.assertRaises(OSError):
            gate.directory(self.root / "missing")

    def test_fake_command_logs_are_private_bounded_and_drained(self):
        proof = gate.GoProof(("TestRequired",))
        code = "import sys; print('SECRET /private/source ' * 10000); sys.stdout.write(" + repr((event("pass", "TestRequired") + event("pass")).decode()) + ")"
        log = self.root / "fake.log"
        result = gate.run_process([sys.executable, "-c", code], self.root, os.environ.copy(), log,
                                  timeout=5, grace=1, observer=proof.observe, log_limit=256)
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(proof.complete())
        self.assertTrue(result.log_truncated)
        self.assertLessEqual(log.stat().st_size, 256)
        self.assertEqual(stat.S_IMODE(log.stat().st_mode), 0o600)
        public = json.dumps({**result.public(), **proof.public()})
        self.assertNotIn("SECRET", public)
        self.assertNotIn(str(self.root), public)
        self.assertNotIn("child_pid", public)

    @unittest.skipUnless(sys.platform == "linux", "Linux signal lifecycle")
    def test_timeout_allows_fixture_sigterm_cleanup(self):
        code = """import signal,sys,time
def finish(*args):
    time.sleep(0.05)
    print('TRUST_CLEANUP_COMPLETED', flush=True)
    sys.exit(0)
signal.signal(signal.SIGTERM, finish)
print('ready', flush=True)
while True: time.sleep(0.01)
"""
        log = self.root / "cleanup.log"
        result = gate.run_process([sys.executable, "-c", code], self.root, os.environ.copy(), log,
                                  timeout=0.3, grace=2, fixture=True)
        self.assertTrue(result.timed_out)
        self.assertEqual(result.exit_code, 0)
        self.assertFalse(result.cleanup_pending)
        self.assertIn("TRUST_CLEANUP_COMPLETED", log.read_text())

    @unittest.skipUnless(sys.platform == "linux", "Linux signal lifecycle")
    def test_stalled_fake_fixture_is_not_force_killed_by_runner(self):
        # This subprocess never installs trust; the TEST cleans up its own fake.
        code = "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready',flush=True); time.sleep(20)"
        log = self.root / "stalled.log"
        try:
            result = gate.run_process([sys.executable, "-c", code], self.root, os.environ.copy(), log,
                                      timeout=0.3, grace=0.2, fixture=True)
            self.assertTrue(result.cleanup_pending)
            self.assertIsNone(result.exit_code)
        finally:
            if log.exists():
                pid = int(log.read_text().splitlines()[0].split("=")[1])
                try:
                    os.killpg(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
                except ProcessLookupError:
                    pass

    def test_fixture_proof_requires_fresh_all_provider_checks_and_cleanup(self):
        report = self.root / "fixture.json"
        value = {"passed": True, "binary_sha256": "a" * 64, "providers": list(gate.PROVIDERS),
                 "checks": [{"provider": provider, "test": "expected", "passed": True}
                            for provider in gate.PROVIDERS], "private_extra": "SECRET"}
        report.write_text(json.dumps(value))
        self.assertTrue(gate.fixture_proof(report, None, ("expected",), "a" * 64))
        info = report.stat()
        before = (info.st_ino, info.st_size, info.st_mtime_ns)
        self.assertFalse(gate.fixture_proof(report, before, ("expected",), "a" * 64))
        self.assertFalse(gate.fixture_proof(report, None, ("missing",), "a" * 64))
        self.assertFalse(gate.fixture_proof(report, None, ("expected",), "b" * 64))
        value["cleanup_failure"] = {"private": "SECRET"}
        report.write_text(json.dumps(value))
        self.assertFalse(gate.fixture_proof(report, None, ("expected",), "a" * 64))

    def test_source_digest_tracks_runtime_changes_and_excludes_generated_outputs(self):
        source = self.root / "source"
        source.mkdir()
        runtime = source / "engine.go"
        runtime.write_text("package engine\n")
        (source / "go.mod").write_text("module example.invalid/test\n")
        (source / "scripts").mkdir()
        (source / "scripts/gate.py").write_text("print('test')\n")
        (source / "artifacts").mkdir()
        artifact = source / "artifacts/generated.go"
        artifact.write_text("ignored generated source")
        (source / "docs/evidence").mkdir(parents=True)
        evidence = source / "docs/evidence/report.json"
        evidence.write_text("{}")
        before = gate.source_input_digest(source)
        artifact.write_text("updated generated source")
        evidence.write_text('{"passed": true}')
        self.assertEqual(before, gate.source_input_digest(source))
        runtime.write_text("package changed\n")
        self.assertNotEqual(before["input_sha256"], gate.source_input_digest(source)["input_sha256"])
        self.assertNotIn(str(source), json.dumps(before))
        with mock.patch.object(gate, "INPUT_LIMIT", 1):
            with self.assertRaises(gate.GateError):
                gate.source_input_digest(source)
        link = source / "escape.go"
        link.symlink_to(runtime)
        with self.assertRaises(gate.GateError):
            gate.source_input_digest(source)


    def test_main_uses_offline_go_defaults_and_sanitized_summary(self):
        source = self.root / "source"
        source.mkdir()
        (source / "go.mod").write_text("module example.invalid/test\n")
        work = self.root / "work"
        work.mkdir(mode=0o700)
        output = self.root / "summary.json"
        commands = []

        def fake_process(command, cwd, env, log_path, timeout, grace, observer=None, **kwargs):
            commands.append(command)
            self.assertEqual(env["GOPROXY"], "off")
            self.assertEqual(env["GOTOOLCHAIN"], "local")
            self.assertEqual(env["GOWORK"], "off")
            self.assertEqual(env["GOENV"], "off")
            self.assertNotIn("GIT_DIR", env)
            self.assertNotIn("KELVO_TEST_MULTIPART_LARGE", env)
            for name in gate.REQUIRED:
                observer(event("pass", name))
            observer(event("pass"))
            observer(event("output", Output="SECRET /private/source"))
            return gate.ProcessResult(0, 0.1)

        with mock.patch.dict(os.environ, {"GIT_DIR": "/private/unrelated", "GOWORK": "/private/workspace"}), \
             mock.patch.object(gate.sys, "platform", "linux"), \
             mock.patch.object(gate.shutil, "which", return_value="/preinstalled/tool"), \
             mock.patch.object(gate, "source_provenance", return_value={"revision": "a" * 40, "dirty": True, "input_sha256": "b" * 64}), \
             mock.patch.object(gate, "run_process", side_effect=fake_process), \
             contextlib.redirect_stdout(io.StringIO()):
            result = gate.main(["--dedicated-test-machine", "--source-directory", str(source),
                                "--work-directory", str(work), "--output", str(output)])
        self.assertEqual(result, 0)
        self.assertEqual(len(commands), 1)
        data = output.read_text()
        self.assertNotIn("SECRET", data)
        self.assertNotIn(str(self.root), data)
        self.assertNotIn("/preinstalled", data)
        parsed = json.loads(data)
        self.assertTrue(parsed["passed"])
        self.assertRegex(parsed["runner_sha256"], r"^[0-9a-f]{64}$")
        self.assertIsNone(parsed["binary_sha256"])
        self.assertEqual(parsed["skipped_scope"], ["large_local", "large_remote", "tls_fixtures"])


if __name__ == "__main__":
    unittest.main()
