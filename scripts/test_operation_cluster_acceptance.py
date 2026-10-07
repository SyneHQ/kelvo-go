#!/usr/bin/env python3
"""Cleanup controls; run on the VM with python3 -m unittest discover -s scripts -p test_operation_cluster_acceptance.py."""
import os
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import operation_cluster_acceptance as fixture


class CleanupControls(unittest.TestCase):
    def scope(self, directory):
        scope = fixture.OwnedWorkloads.__new__(fixture.OwnedWorkloads)
        scope.pid = os.getpid()
        scope.relative = "/system.slice/kelvo-operation-live-control.service"
        scope.group = Path(directory) / "owned"
        scope.group.mkdir()
        scope.supervisor = scope.group / "supervisor"
        scope.jobs = scope.group / "jobs"
        info = scope.group.stat()
        scope.identity = (info.st_dev, info.st_ino)
        return scope

    def identity(self, scope, pid=42):
        return {"pid": pid, "start_ticks": 123, "cgroup": scope.relative + "/supervisor"}

    def test_scope_never_includes_ancestor_sibling_or_prefix_collision(self):
        with tempfile.TemporaryDirectory() as directory:
            scope = self.scope(directory)
            self.assertTrue(scope.contains(scope.relative))
            self.assertTrue(scope.contains(scope.relative + "/jobs/worker"))
            for foreign in ("/system.slice", scope.relative + "-other", "/system.slice/other.service"):
                self.assertFalse(scope.contains(foreign))
            with mock.patch.object(scope, "assert_owner"), mock.patch.object(fixture.os, "pidfd_open", create=True) as opened:
                for identity in (self.identity(scope, scope.pid), {**self.identity(scope), "cgroup": "/system.slice/other.service"}):
                    with self.assertRaisesRegex(RuntimeError, "foreign"):
                        scope.signal_process(identity, signal.SIGKILL)
                opened.assert_not_called()

    def test_replaced_cgroup_identity_aborts_before_opening_pidfd(self):
        with tempfile.TemporaryDirectory() as directory:
            scope = self.scope(directory)
            scope.group.rename(Path(directory) / "previous")
            scope.group.mkdir()
            with mock.patch.object(fixture, "unified_cgroup", return_value=scope.relative), mock.patch.object(fixture.os, "pidfd_open", create=True) as opened:
                with self.assertRaisesRegex(RuntimeError, "identity changed"):
                    scope.signal_process(self.identity(scope), signal.SIGTERM)
                opened.assert_not_called()

    def test_pid_reuse_and_cgroup_escape_after_pidfd_open_are_not_signalled(self):
        with tempfile.TemporaryDirectory() as directory:
            scope = self.scope(directory)
            identity = self.identity(scope)
            for changed in ({**identity, "start_ticks": 456}, {**identity, "cgroup": "/system.slice/other.service"}, None):
                with self.subTest(changed=changed), mock.patch.object(scope, "assert_owner"), \
                     mock.patch.object(fixture, "process_identity", side_effect=[identity, changed]), \
                     mock.patch.object(fixture.os, "pidfd_open", return_value=91, create=True), \
                     mock.patch.object(fixture.signal, "pidfd_send_signal", create=True) as sent, \
                     mock.patch.object(fixture.os, "close") as closed:
                    self.assertFalse(scope.signal_process(identity, signal.SIGKILL))
                    sent.assert_not_called()
                    closed.assert_called_once_with(91)

    def test_snapshot_includes_nested_workloads_and_excludes_foreign_membership(self):
        with tempfile.TemporaryDirectory() as directory:
            scope = self.scope(directory)
            scope.pid = 100000001  # No real /proc task directory is consulted.
            scope.supervisor.mkdir()
            worker = scope.jobs / "worker" / "nested"
            worker.mkdir(parents=True)
            for group in [scope.group] + scope.directories():
                (group / "cgroup.procs").write_text("")
            (scope.supervisor / "cgroup.procs").write_text(str(scope.pid) + "\n42\n43\n")
            (worker / "cgroup.procs").write_text("44\n")
            identities = {42: self.identity(scope, 42), 43: {**self.identity(scope, 43), "cgroup": scope.relative + "-other"},
                          44: {**self.identity(scope, 44), "cgroup": scope.relative + "/jobs/worker/nested"}}
            with mock.patch.object(scope, "assert_owner"), mock.patch.object(fixture, "process_identity", side_effect=identities.get):
                self.assertEqual([value["pid"] for value in scope.snapshot()], [42, 44])

    def test_cleanup_failure_cannot_overwrite_original_failure_or_claim_success(self):
        with tempfile.TemporaryDirectory() as directory:
            scope = self.scope(directory)
            with mock.patch.object(scope, "snapshot", side_effect=PermissionError("private diagnostic")):
                evidence = scope.cleanup()
            report = {"passed": False, "failure": "live-test:TimeoutExpired", "failures": [], "cleanup": {}}
            fixture.record_owned_cleanup(report, evidence)
            self.assertFalse(report["passed"])
            self.assertEqual(report["failure"], "live-test:TimeoutExpired")
            self.assertFalse(report["cleanup"]["owned_processes_reaped"])
            self.assertEqual(evidence["errors"], ["PermissionError"])
            self.assertNotIn("private diagnostic", str(report))

    def test_adopted_zombie_can_be_reaped_after_cgroup_detachment_but_never_signalled(self):
        with tempfile.TemporaryDirectory() as directory:
            scope = self.scope(directory)
            identity = {**self.identity(scope), "cgroup": "/"}
            with mock.patch.object(scope, "assert_owner"), mock.patch.object(fixture, "process_identity", return_value=identity), \
                 mock.patch.object(fixture, "adopted_zombie", return_value=True), mock.patch.object(fixture.os, "waitpid", return_value=(42, 9)) as waited, \
                 mock.patch.object(fixture.os, "pidfd_open", create=True) as opened:
                self.assertTrue(scope.reap(identity))
                waited.assert_called_once_with(42, os.WNOHANG)
                with self.assertRaisesRegex(RuntimeError, "foreign"):
                    scope.signal_process(identity, signal.SIGKILL)
                opened.assert_not_called()

    def test_leftover_empty_job_group_fails_gate_even_when_cleanup_succeeds(self):
        with tempfile.TemporaryDirectory() as directory:
            scope = self.scope(directory)
            with mock.patch.object(scope, "snapshot", return_value=[]), \
                 mock.patch.object(scope, "directories", return_value=[scope.supervisor, scope.jobs, scope.jobs / "leaked-job"]), \
                 mock.patch.object(scope, "remove_groups", return_value=True):
                evidence = scope.cleanup()
            report = {"passed": True, "failures": [], "cleanup": {}}
            fixture.record_owned_cleanup(report, evidence)
            self.assertTrue(all(report["cleanup"].values()))
            self.assertFalse(report["passed"])
            self.assertEqual(evidence["initial_job_groups"], ["jobs/leaked-job"])
            self.assertIn("owned-workloads-required-cleanup", report["failures"])

    def test_one_process_error_does_not_skip_other_owned_children(self):
        with tempfile.TemporaryDirectory() as directory:
            scope = self.scope(directory)
            first, second = self.identity(scope, 42), self.identity(scope, 43)
            with mock.patch.object(scope, "snapshot", side_effect=[[first, second], [first, second], [], [], []]), \
                 mock.patch.object(scope, "directories", return_value=[]), mock.patch.object(scope, "remove_groups", return_value=True), \
                 mock.patch.object(scope, "reap", side_effect=[False, True]) as reaped, \
                 mock.patch.object(scope, "signal_process", side_effect=PermissionError), mock.patch.object(fixture.time, "sleep"):
                evidence = scope.cleanup()
            self.assertEqual(reaped.call_count, 2)
            self.assertEqual(evidence["reaped"], [second])
            self.assertIn("PermissionError", evidence["errors"])
            report = {"passed": True, "failures": [], "cleanup": {}}
            fixture.record_owned_cleanup(report, evidence)
            self.assertFalse(report["passed"])

    def test_empty_verified_scope_preserves_success(self):
        with tempfile.TemporaryDirectory() as directory:
            scope = self.scope(directory)
            with mock.patch.object(scope, "snapshot", return_value=[]), mock.patch.object(scope, "directories", return_value=[]), \
                 mock.patch.object(scope, "remove_groups", return_value=True):
                evidence = scope.cleanup()
            report = {"passed": True, "failures": [], "cleanup": {}}
            fixture.record_owned_cleanup(report, evidence)
            self.assertTrue(report["passed"])
            self.assertTrue(all(report["cleanup"].values()))

    @unittest.skipUnless(sys.platform == "linux" and hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"), "Linux pidfd/reaper control")
    def test_crash_and_timeout_reap_term_resistant_orphans_and_keep_gate_failed(self):
        fixture.child_subreaper()
        child_code = "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready',flush=True); time.sleep(30)"
        for mode in ("crash", "timeout"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                parent_code = ("import os,subprocess,sys,time\n"
                               "child=subprocess.Popen([sys.executable,'-c'," + repr(child_code) + "],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)\n"
                               "assert child.stdout.readline()==b'ready\\n'\n"
                               "print(child.pid,flush=True)\n" + ("os._exit(23)\n" if mode == "crash" else "time.sleep(30)\n"))
                if mode == "crash":
                    result = subprocess.run([sys.executable, "-c", parent_code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
                    self.assertEqual(result.returncode, 23)
                    pid = int(result.stdout.strip())
                else:
                    with self.assertRaises(subprocess.TimeoutExpired) as timed_out:
                        subprocess.run([sys.executable, "-c", parent_code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=1)
                    pid = int(timed_out.exception.output.strip())
                identity = fixture.process_identity(pid)
                self.assertIsNotNone(identity)
                scope = self.scope(directory)
                # The real child and pidfd/reaping operations run on Linux.
                # Supply only this spawned PID as the inventory; hierarchy and
                # membership races have separate controls above, so no test can
                # signal other processes in the VM test runner's actual cgroup.
                scope.relative = identity["cgroup"]
                def snapshot():
                    current = fixture.process_identity(pid)
                    return [current] if current is not None else []
                try:
                    with mock.patch.object(scope, "assert_owner"), mock.patch.object(scope, "snapshot", side_effect=snapshot), \
                         mock.patch.object(scope, "directories", return_value=[]), mock.patch.object(scope, "remove_groups", return_value=True):
                        evidence = scope.cleanup(term_seconds=.15, kill_seconds=3)
                    report = {"passed": True, "failures": [], "cleanup": {}, "failure": "live-test:" + mode}
                    fixture.record_owned_cleanup(report, evidence)
                    self.assertTrue(evidence["processes_reaped"], evidence)
                    self.assertTrue(evidence["child_groups_removed"])
                    self.assertEqual([item["pid"] for item in evidence["reaped"]], [pid])
                    self.assertIn(signal.SIGKILL, [item["signal"] for item in evidence["signals"]])
                    self.assertIsNone(fixture.process_identity(pid))
                    self.assertFalse(report["passed"])
                    self.assertEqual(report["failure"], "live-test:" + mode)
                finally:
                    if fixture.process_identity(pid) == identity:
                        descriptor = os.pidfd_open(pid)
                        try:
                            if fixture.process_identity(pid) == identity:
                                signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                        finally:
                            os.close(descriptor)
                        os.waitpid(pid, 0)


class ApplicationProvenanceControls(unittest.TestCase):
    def fixture(self, directory):
        source, artifacts = Path(directory) / "source", Path(directory) / "artifacts"
        source.mkdir(); artifacts.mkdir()
        names = [name + "/contract.go" for name in ("client", "query", "delegation", "resolver", "operations")]
        names += ["go.mod", "go.sum", "examples/application/go.mod", "examples/application/main.go", "examples/application/app.yaml"]
        entries = {}
        for name in names:
            path = source / name; path.parent.mkdir(parents=True, exist_ok=True)
            raw = ("fixture " + name + "\n").encode(); path.write_bytes(raw)
            entries[name] = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw), "mode": 0o644}
        report = {"mode": "candidate", "provenance": {}, "public_packages": ["github.com/SYNEHQ/kelvo-go/" + name for name in ("client", "query", "delegation", "resolver", "operations")]}
        for label, data in (("checkout", entries), ("sdk", entries), ("application", {name.removeprefix("examples/application/"): value for name, value in entries.items() if name.startswith("examples/application/")})):
            before, after = "source-" + label + "-before.json", "source-" + label + "-after.json"
            raw = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
            (artifacts / before).write_bytes(raw); (artifacts / after).write_bytes(raw)
            digest = hashlib.sha256(raw).hexdigest()
            report["provenance"][label] = {"manifest_file": before, "after_manifest_file": after, "manifest_sha256": digest,
                                             "after_manifest_sha256": digest, "unchanged_after_execution": True}
        report["provenance"]["template_copy"] = {"matches": True, "template_sha256": "a" * 64, "copied_sha256": "a" * 64}
        manifest = {"files": {name: value["sha256"] for name, value in entries.items()}}
        return source, artifacts, report, manifest

    def test_current_candidate_sources_pass_without_mode_bit_coupling(self):
        with tempfile.TemporaryDirectory() as directory:
            source, artifacts, report, manifest = self.fixture(directory)
            result = fixture.verify_application_provenance(artifacts, report, source, manifest)
            self.assertTrue(result["source_unchanged"])
            self.assertEqual(result["public_files"], 7)

    def test_stale_public_sdk_or_example_source_cannot_use_old_passed_artifacts(self):
        for name in ("client/contract.go", "examples/application/main.go", "examples/application/go.mod"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                source, artifacts, report, manifest = self.fixture(directory)
                raw = b"new source revision\n"; (source / name).write_bytes(raw)
                manifest["files"][name] = hashlib.sha256(raw).hexdigest()
                with self.assertRaisesRegex(RuntimeError, "differs"):
                    fixture.verify_application_provenance(artifacts, report, source, manifest)

    def test_unverified_or_modified_build_manifests_are_refused(self):
        for mode in ("not_unchanged", "changed_manifest", "changed_copy"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                source, artifacts, report, manifest = self.fixture(directory)
                if mode == "not_unchanged": report["provenance"]["sdk"]["unchanged_after_execution"] = False
                elif mode == "changed_manifest": (artifacts / "source-sdk-after.json").write_text("{}")
                else: report["provenance"]["template_copy"]["copied_sha256"] = "b" * 64
                with self.assertRaises(RuntimeError):
                    fixture.verify_application_provenance(artifacts, report, source, manifest)

    def test_published_module_requires_external_version_and_sum_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            source, artifacts, report, manifest = self.fixture(directory)
            version, checksum = "v0.1.0-preview.2", "h1:" + "a" * 43 + "="
            report.update(mode="published", version=version, resolved_module={"Path": "github.com/SYNEHQ/kelvo-go", "Version": version, "Sum": checksum})
            with self.assertRaisesRegex(RuntimeError, "authoritative"):
                fixture.verify_application_provenance(artifacts, report, source, manifest)
            self.assertTrue(fixture.verify_application_provenance(artifacts, report, source, manifest, version, checksum)["source_unchanged"])
            with self.assertRaisesRegex(RuntimeError, "authoritative"):
                fixture.verify_application_provenance(artifacts, report, source, manifest, version, "h1:" + "b" * 43 + "=")


class SourceCAControls(unittest.TestCase):
    def run_openssl(self, argv, check=True):
        return subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=check)

    def test_generated_source_ca_has_explicit_native_trust_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            key, certificate = Path(directory) / "ca.key", Path(directory) / "ca.crt"
            fixture.create_source_ca(self.run_openssl, key, certificate)
            profile = fixture.source_ca_profile(self.run_openssl, certificate)
            for name in ("accepted", "is_ca", "certificate_signing", "crl_signing", "critical_basic_constraints", "critical_key_usage", "currently_valid"):
                self.assertTrue(profile[name], name)
            self.assertEqual(profile["sha256"], fixture.digest(certificate))
            self.assertNotIn("BEGIN", json.dumps(profile))
            self.assertNotIn(directory, json.dumps(profile))

    def test_missing_or_non_signing_key_usage_fails_before_source_startup(self):
        for usage in (None, "digitalSignature"):
            with self.subTest(usage=usage), tempfile.TemporaryDirectory() as directory:
                key, certificate = Path(directory) / "ca.key", Path(directory) / "ca.crt"
                # An isolated profile makes omission deterministic on every VM,
                # including hosts whose default openssl.cnf supplies keyUsage.
                config = Path(directory) / "openssl.cnf"
                config.write_text("[req]\ndistinguished_name=dn\n[dn]\n")
                command = ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-config", str(config),
                           "-subj", "/CN=fixture-control", "-addext", "basicConstraints=critical,CA:TRUE", "-keyout", str(key), "-out", str(certificate)]
                if usage:
                    command.extend(["-addext", "keyUsage=critical," + usage])
                self.run_openssl(command)
                profile = fixture.source_ca_profile(self.run_openssl, certificate)
                self.assertTrue(profile["is_ca"])
                self.assertFalse(profile["certificate_signing"])
                self.assertFalse(profile["accepted"])


if __name__ == "__main__":
    unittest.main()
