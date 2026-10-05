#!/usr/bin/env python3
"""Negative controls for kernel acceptance, source identity and owned cleanup."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import subprocess
import stat
import tempfile
import unittest
from unittest import mock

import containment_acceptance as fixture

TEST_UNIT = "kelvo-containment-0123456789ab"
TEST_DESCRIPTION = "Kelvo containment acceptance " + "a" * 32
TEST_USER = "fixture-user"


def live_service(parent=None):
    properties = {key: str(value) for key, value in fixture.LIVE_NUMERIC_LIMITS.items()}
    properties.update(CPUQuotaPerSecUSec="1s", RuntimeMaxUSec="10min", TimeoutStopUSec="30s",
                      Id=TEST_UNIT + ".service", Description=TEST_DESCRIPTION, InvocationID="b" * 32, MainPID="1234",
                      User=TEST_USER, Transient="yes", ControlGroup="/system.slice/" + TEST_UNIT + ".service",
                      ActiveState="active", LoadState="loaded", Delegate="yes", KillMode="control-group",
                      Requisite=parent or "", BindsTo=parent or "", After="sysinit.target" + (" " + parent if parent else ""))
    return {"verified": True, "properties": properties}


def valid_report(protected=False, queries=False):
    protected = protected or queries
    files = {name: "a" * 64 for name in fixture.SOURCE_REQUIRED | (fixture.PROTECTED_SOURCE_REQUIRED if protected else set())
             | (fixture.protected_query.SOURCE_REQUIRED if queries else set())}
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    report = {
        "schema": 3, "owned_unit": TEST_UNIT, "service_description": TEST_DESCRIPTION,
        "service_user": TEST_USER, "parent_unit": None, "live_service": live_service(),
        "mode": fixture.QUERY_MODE if queries else fixture.PROTECTED_MODE if protected else fixture.BASE_MODE,
        "gates": {name: "pass" for name in fixture.required_gates(protected, queries)},
        "non_root": True, "capability_sets_zero": True,
        "source": {"verified": True, "file_count": len(files), "files": files,
                   "sha256": hashlib.sha256(canonical).hexdigest(), "base_revision": "c" * 40,
                   "excluded_metadata": [".DS_Store", "._*"]},
        "source_unchanged": True,
        "binary_sha256": {name: "b" * 64 for name in fixture.BINARIES},
        "remaining_job_groups": 0, "remaining_ownership_records": 0,
        "service_exit_code": 0, "owned_service_removed": True, "owned_cgroup_removed": True,
    }
    if protected:
        report["bridge"] = {"duckdb": fixture.bridge.VERSION, "driver": fixture.bridge.MODULE_VERSION,
                            "module_sum": fixture.bridge.MODULE_SUM, "source_sha256": fixture.bridge.ARCHIVE_SHA256,
                            "tags": fixture.BRIDGE_TAGS} | {name: "a" * 64 for name in fixture.BRIDGE_HASHES}
        report["bridge_inputs_unchanged"] = True
        report["resource_limits"] = dict(fixture.RESOURCE_LIMITS)
        report["protected_gates"] = {name: "pass" for name in ("outer", "inner", *fixture.PROTECTED_LEAVES)}
    if queries:
        report["binary_sha256"]["nats-server"] = "b" * 64
        report["broker_binary"] = {"version": fixture.protected_query.VERSION, "sha256": "b" * 64, "bytes": 1000}
        report["broker_binary_unchanged"] = True
        report["protected_query_gates"] = {name: "pass" for name in ("outer", "inner", *fixture.protected_query.LEAVES)}
        report["broker_custody"] = {"created_pid": 23, "started": True, "reaped": True, "pid_absent": True, "alive_before_stop": True,
            "returncode": 0, "forced_kill": False, "identity": {"pid": 23, "start_ticks": 111, "cgroup": "/system.slice/" + TEST_UNIT + ".service/supervisor",
                                         "argv_sha256": "c" * 64}}
    return report


def protected_output():
    root = fixture.PROTECTED_GATE
    return "".join(f"        --- PASS: {name} (0s)\n" for name in fixture.PROTECTED_LEAVES) + \
        f"    --- PASS: {root} (0s)\n--- PASS: {root} (0s)\n"


class ContainmentControls(unittest.TestCase):
    def test_direct_runner_without_bytecode_flag_keeps_imports_out_of_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runner = root / "containment_acceptance.py"
            runner.write_bytes(Path(fixture.__file__).read_bytes())
            (root / "provision_duckbridge.py").write_text("# Import-only fixture; help must not provision anything.\n")
            (root / "protected_query_acceptance.py").write_text("# Import-only fixture; help must not start a broker.\n")
            env = {key: value for key, value in fixture.os.environ.items() if not key.startswith("PYTHON")}
            result = subprocess.run([fixture.sys.executable, str(runner), "--help"],
                                    env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse((root / "__pycache__").exists())

    def test_python_provision_and_delegated_commands_disable_bytecode_explicitly(self):
        for protected in (False, True):
            with self.subTest(protected=protected), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                artifact = root / "artifact"
                artifact.mkdir()
                report = root / "report.json"
                args = ["containment_acceptance.py", "--repo", str(root), "--report", str(report), "--unit", TEST_UNIT]
                if protected:
                    args.append("--protected-objects")
                process = mock.Mock()
                process.wait.return_value = 0
                process.poll.return_value = 0
                identity = valid_report(protected)["source"]
                with mock.patch.object(fixture.sys, "argv", args), \
                        mock.patch.object(fixture.sys, "platform", "linux"), \
                        mock.patch.object(fixture.os, "geteuid", return_value=1000), \
                        mock.patch.object(fixture.os, "chdir"), \
                        mock.patch.object(fixture.pwd, "getpwuid", return_value=SimpleNamespace(pw_name=TEST_USER)), \
                        mock.patch.object(fixture, "prepare_artifact", return_value=artifact), \
                        mock.patch.object(fixture, "require_fresh_unit"), \
                        mock.patch.object(fixture, "source_manifest", return_value=identity), \
                        mock.patch.object(fixture, "bridge_provenance", return_value={}), \
                        mock.patch.object(fixture, "digest", return_value="a" * 64), \
                        mock.patch.object(fixture, "run", return_value=subprocess.CompletedProcess([], 0, "go fixture\n")), \
                        mock.patch.object(fixture, "run_logged", return_value=subprocess.CompletedProcess([], 0, "")) as logged, \
                        mock.patch.object(fixture.subprocess, "Popen", return_value=process) as launch, \
                        mock.patch.object(fixture, "checked_outside_probe", return_value="pass"), \
                        mock.patch.object(fixture, "cleanup_owned", return_value={"owned_service_removed": True, "owned_cgroup_removed": True}), \
                        mock.patch.object(fixture, "reconcile", return_value=True), mock.patch("builtins.print"):
                    self.assertEqual(fixture.main(), 0)
                delegated = launch.call_args.args[0]
                python = delegated.index(fixture.sys.executable)
                self.assertEqual(delegated[python:python + 4],
                                 [fixture.sys.executable, "-B", str(Path(fixture.__file__).resolve()), "--inside"])
                provisioning = [call.args[0] for call in logged.call_args_list if call.args[0][0] == fixture.sys.executable]
                self.assertEqual(len(provisioning), int(protected))
                if protected:
                    self.assertEqual(provisioning[0][:3], [fixture.sys.executable, "-B", str(root / "scripts/provision_duckbridge.py")])

    def test_protected_mode_requires_its_gate_and_cannot_be_downgraded(self):
        self.assertTrue(fixture.reconcile(valid_report(protected=True)))
        for state in ("missing", "skip", "fail", "duplicate", True, None):
            report = valid_report(protected=True)
            report["gates"][fixture.PROTECTED_GATE] = state
            self.assertFalse(fixture.reconcile(report), state)
        for mode in (None, "", "native", fixture.BASE_MODE):
            report = valid_report(protected=True)
            report["mode"] = mode
            self.assertFalse(fixture.reconcile(report), mode)
        report = valid_report(protected=True)
        del report["gates"][fixture.PROTECTED_GATE]
        self.assertFalse(fixture.reconcile(report))
        report = valid_report()
        report["mode"] = fixture.PROTECTED_MODE
        self.assertFalse(fixture.reconcile(report))
        report = valid_report()
        del report["mode"]
        report["schema"] = 2
        del report["live_service"]
        self.assertTrue(fixture.reconcile(report), "existing baseline reports remain compatible")

    def test_new_reports_require_actual_service_readback_in_both_modes(self):
        for protected in (False, True):
            report = valid_report(protected=protected)
            del report["live_service"]
            self.assertFalse(fixture.reconcile(report))
            for field in ("owned_unit", "service_description", "service_user"):
                report = valid_report(protected=protected)
                del report[field]
                self.assertFalse(fixture.reconcile(report), field)
            for key in fixture.LIVE_PROPERTIES:
                report = valid_report(protected=protected)
                del report["live_service"]["properties"][key]
                self.assertFalse(fixture.reconcile(report), key)
                report = valid_report(protected=protected)
                report["live_service"]["properties"][key] = "wrong"
                if key not in ("Requisite", "BindsTo", "After"):
                    self.assertFalse(fixture.reconcile(report), key)
        report = valid_report(protected=True)
        report["schema"] = 2
        del report["live_service"]
        self.assertFalse(fixture.reconcile(report), "legacy schema cannot bypass protected readback")

    def test_live_readback_checks_parent_dependencies_and_never_trusts_verified_flag_alone(self):
        parent = "kelvo-protected-readers-validation-0123456789ab.service"
        report = valid_report(protected=True)
        report.update(parent_unit=parent, live_service=live_service(parent))
        self.assertTrue(fixture.reconcile(report))
        for key in ("Requisite", "BindsTo", "After"):
            for value in ("", parent + "-other", parent + " " + parent):
                report["live_service"] = live_service(parent)
                report["live_service"]["properties"][key] = value
                self.assertFalse(fixture.reconcile(report), (key, value))
        report["live_service"] = live_service(parent)
        report["parent_unit"] = None
        self.assertFalse(fixture.reconcile(report))
        report["parent_unit"] = parent
        for value in (False, 1, "true", None):
            report["live_service"]["verified"] = value
            self.assertFalse(fixture.reconcile(report))

    def test_live_time_readback_normalizes_units_and_rejects_unbounded_or_ambiguous_values(self):
        for value, expected in (("1s", 1_000_000), ("1000ms", 1_000_000), ("1000000us", 1_000_000),
                                ("10min", 600_000_000), ("0h 10min 0s", 600_000_000)):
            self.assertEqual(fixture.duration_usec(value), expected)
        for value in (None, False, "infinity", "600", "-1s", "10minjunk", "1e3ms", "1s\n", "1s  1s"):
            self.assertIsNone(fixture.duration_usec(value), value)

    def test_live_readback_requires_an_original_positive_main_pid(self):
        for value in ("0", "-1", "01", "+1", "1.0", "1e3", " 1234", "1234\n", "", None, True, 1234):
            report = valid_report(protected=True)
            report["live_service"]["properties"]["MainPID"] = value
            self.assertFalse(fixture.reconcile(report), value)

    def test_live_readback_is_retained_and_rejects_duplicate_unknown_missing_or_failed_output(self):
        evidence = live_service()
        output = "".join(key + "=" + value + "\n" for key, value in evidence["properties"].items())
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            for raw, code, passed in ((output, 0, True), (output + "MemoryMax=1073741824\n", 0, False),
                                      (output + "Unexpected=value\n", 0, False),
                                      (output.replace("MemorySwapMax=0\n", ""), 0, False),
                                      (output.replace("MemoryMax=1073741824", "MemoryMax=infinity"), 0, False),
                                      (output, 1, False)):
                with mock.patch.object(fixture, "run", return_value=subprocess.CompletedProcess([], code, raw)) as run:
                    result = fixture.capture_live_service(artifact, TEST_UNIT, TEST_DESCRIPTION, TEST_USER)
                self.assertEqual(result["verified"], passed)
                self.assertEqual(json.loads((artifact / "live-service.json").read_text()), result)
                self.assertEqual((artifact / "live-service.log").read_text(), raw)
                command = run.call_args.args[0]
                self.assertEqual(command[:3], ["systemctl", "show", TEST_UNIT + ".service"])
            with mock.patch.object(fixture, "run", side_effect=subprocess.TimeoutExpired([], 10, output=b"partial properties\n")):
                result = fixture.capture_live_service(artifact, TEST_UNIT, TEST_DESCRIPTION, TEST_USER)
            self.assertFalse(result["verified"])
            self.assertEqual(result["failure"], "TimeoutExpired")
            self.assertFalse(json.loads((artifact / "live-service.json").read_text())["verified"])
            self.assertEqual((artifact / "live-service.log").read_bytes(), b"partial properties\n")

    def test_no_probe_or_inside_release_runs_when_live_limits_are_unproven(self):
        with tempfile.TemporaryDirectory() as temporary:
            artifact, report = Path(temporary), {}
            failure = {"verified": False, "properties": {}}
            with mock.patch.object(fixture, "wait_for_file"), \
                    mock.patch.object(fixture, "capture_live_service", return_value=failure), \
                    mock.patch.object(fixture, "outside_check") as outside:
                with self.assertRaisesRegex(RuntimeError, "live delegated service"):
                    fixture.checked_outside_probe(artifact, TEST_UNIT, mock.Mock(), report,
                        description=TEST_DESCRIPTION, user=TEST_USER, protected=True)
                outside.assert_not_called()
            self.assertIs(report["live_service"], failure)
            self.assertFalse((artifact / "outside-complete").exists())
            with mock.patch.object(fixture, "wait_for_file"), \
                    mock.patch.object(fixture, "capture_live_service", return_value=live_service()), \
                    mock.patch.object(fixture, "outside_check", return_value="pass") as outside:
                self.assertEqual(fixture.checked_outside_probe(artifact, TEST_UNIT, mock.Mock(), report,
                    description=TEST_DESCRIPTION, user=TEST_USER, protected=True), "pass")
                outside.assert_called_once()

    def test_protected_gate_rejects_nested_skip_or_failure_behind_outer_pass(self):
        root = fixture.PROTECTED_GATE
        for status in ("SKIP", "FAIL"):
            for name in (root, root + "/cte-join-types-policy-and-shared-manager"):
                output = f"    --- {status}: {name} (0s)\n--- PASS: {root} (0s)\n"
                self.assertEqual(fixture.gates(output, [root]), {root: "fail"})
        self.assertEqual(fixture.gates(protected_output(), [root]), {root: "pass"})

    def test_protected_child_evidence_requires_both_roots_and_every_exact_leaf(self):
        root, output = fixture.PROTECTED_GATE, protected_output()
        self.assertEqual(set(fixture.protected_gates(output)), {"inner", "outer", *fixture.PROTECTED_LEAVES})
        self.assertEqual(fixture.gates(f"--- PASS: {root} (0s)\n", [root]), {root: "fail"})
        for line in output.splitlines(keepends=True):
            for changed in (output.replace(line, ""), output + line):
                self.assertEqual(fixture.gates(changed, [root]), {root: "fail"})
        for name in ("inner", "outer", *fixture.PROTECTED_LEAVES):
            report = valid_report(protected=True)
            del report["protected_gates"][name]
            self.assertFalse(fixture.reconcile(report), name)
        report = valid_report(protected=True)
        report["protected_gates"][root + "/unexpected"] = "pass"
        self.assertFalse(fixture.reconcile(report))

    def test_query_gate_requires_exact_children_and_broker_custody(self):
        root, leaves = fixture.protected_query.ROOT, fixture.protected_query.LEAVES
        output = "".join(f"        --- PASS: {name} (0s)\n" for name in leaves) + f"    --- PASS: {root} (0s)\n--- PASS: {root} (0s)\n"
        self.assertEqual(fixture.gates(output, [root]), {root: "pass"})
        self.assertTrue(fixture.reconcile(valid_report(queries=True)))
        for line in output.splitlines(keepends=True):
            for changed in (output.replace(line, ""), output + line, output.replace(line, line.replace("PASS", "SKIP"))):
                self.assertEqual(fixture.gates(changed, [root]), {root: "fail"})
        for suffix in ("unknown", "revocation-none-length/hidden-skip"):
            self.assertEqual(fixture.gates(output + f"    --- SKIP: {root}/{suffix} (0s)\n", [root]), {root: "fail"})
        for key in ("broker_custody", "broker_binary", "broker_binary_unchanged", "protected_query_gates"):
            report = valid_report(queries=True)
            del report[key]
            self.assertFalse(fixture.reconcile(report), key)
        for key in ("started", "reaped", "pid_absent", "alive_before_stop"):
            report = valid_report(queries=True)
            report["broker_custody"][key] = False
            self.assertFalse(fixture.reconcile(report), key)
        for code in (None, True, 1, -9):
            report = valid_report(queries=True)
            report["broker_custody"]["returncode"] = code
            self.assertFalse(fixture.reconcile(report), code)

    def test_query_evidence_cannot_be_downgraded_or_retargeted(self):
        for mode in (fixture.BASE_MODE, fixture.PROTECTED_MODE, None):
            report = valid_report(queries=True)
            report["mode"] = mode
            self.assertFalse(fixture.reconcile(report), mode)
        for value in ("/unrelated", "/system.slice/" + TEST_UNIT + ".service", ""):
            report = valid_report(queries=True)
            report["broker_custody"]["identity"]["cgroup"] = value
            self.assertFalse(fixture.reconcile(report), value)
        for key, value in (("version", "unknown"), ("sha256", "f" * 64), ("bytes", 0), ("bytes", True)):
            report = valid_report(queries=True)
            report["broker_binary"][key] = value
            self.assertFalse(fixture.reconcile(report), (key, value))
        report = valid_report(queries=True)
        report["broker_custody"]["identity"]["pid"] += 1
        self.assertFalse(fixture.reconcile(report))

    def test_protected_report_requires_pinned_unchanged_bridge_provenance(self):
        for field in ("bridge", "bridge_inputs_unchanged"):
            report = valid_report(protected=True)
            del report[field]
            self.assertFalse(fixture.reconcile(report), field)
        for field in ("duckdb", "driver", "module_sum", "source_sha256", "tags", *fixture.BRIDGE_HASHES):
            for value in (None, "", "wrong", False):
                report = valid_report(protected=True)
                report["bridge"][field] = value
                self.assertFalse(fixture.reconcile(report), (field, value))
        report = valid_report(protected=True)
        report["bridge"]["driver_patch_sha256"] = "f" * 64
        self.assertFalse(fixture.reconcile(report), "bridge patch must bind to source inventory")
        report = valid_report(protected=True)
        report["bridge_inputs_unchanged"] = 1
        self.assertFalse(fixture.reconcile(report))
        for name in fixture.RESOURCE_LIMITS:
            report = valid_report(protected=True)
            report["resource_limits"][name] = "unbounded"
            self.assertFalse(fixture.reconcile(report), name)

    def test_native_test_binaries_and_cli_use_one_exact_bridge_configuration(self):
        artifact = Path("/owned/fixture")
        native = fixture.build_commands("fixture-go", artifact, protected=True)
        self.assertEqual(len(native), len(fixture.BINARIES))
        for command in native[:5]:
            self.assertEqual(command[0], "fixture-go")
            self.assertEqual(command[command.index("-modfile") + 1], str(artifact / "duckbridge/duckbridge.mod"))
            self.assertEqual(command[command.index("-tags") + 1], fixture.BRIDGE_TAGS)
            self.assertIn("-mod=readonly", command)
        baseline = fixture.build_commands("fixture-go", artifact)
        for command in baseline[:4]:
            self.assertNotIn("-modfile", command)
            self.assertNotIn("-tags", command)
        self.assertEqual(baseline[4][baseline[4].index("-tags") + 1], "duckdb_arrow")
        self.assertNotIn("-modfile", baseline[4])

    def test_protected_environment_is_explicit_and_cannot_inherit_child_bypass(self):
        with mock.patch.dict(fixture.os.environ, {"KELVO_TEST_PROTECTED_OBJECTS": "1", "KELVO_PROTECTED_FIXTURE_CHILD": "1",
                                                  "KELVO_TEST_PROTECTED_QUERIES": "1", "KELVO_PROTECTED_QUERY_CHILD": "1",
                                                  "KELVO_TEST_QUERY_NATS_A_GATEWAY_PASSWORD": "fixture",
                                                  "KELVO_SOURCE_REGISTRY_SECRET": "fixture", "AWS_SECRET_ACCESS_KEY": "fixture", "LD_PRELOAD": "/fixture"}):
            for protected in (False, True):
                env = fixture.test_environment(Path("/owned/fixture"), "/owned/jobs", "/owned/state", protected)
                self.assertNotIn("KELVO_PROTECTED_FIXTURE_CHILD", env)
                self.assertNotIn("KELVO_PROTECTED_QUERY_CHILD", env)
                self.assertNotIn("KELVO_TEST_PROTECTED_QUERIES", env)
                self.assertNotIn("KELVO_TEST_QUERY_NATS_A_GATEWAY_PASSWORD", env)
                self.assertEqual(env.get("KELVO_TEST_PROTECTED_OBJECTS"), "1" if protected else None)
                for name in ("KELVO_SOURCE_REGISTRY_SECRET", "AWS_SECRET_ACCESS_KEY", "LD_PRELOAD"):
                    self.assertNotIn(name, env)

    def test_cached_bridge_seed_is_pinned_bounded_regular_and_never_adopts_destination(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            source = root / "cached.tar.gz"
            source.write_bytes(b"fixture archive")
            artifact = root / "artifact"
            artifact.mkdir()
            with mock.patch.object(fixture.bridge, "ARCHIVE_SHA256", fixture.digest(source)):
                fixture.seed_bridge_archive(source, artifact)
                seeded = artifact / "duckbridge" / ("duckdb-" + fixture.bridge.VERSION + ".tar.gz")
                self.assertEqual(seeded.read_bytes(), source.read_bytes())
                with self.assertRaises(FileExistsError):
                    fixture.seed_bridge_archive(source, artifact)
            wrong = root / "wrong"
            wrong.mkdir()
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                fixture.seed_bridge_archive(source, wrong)
            self.assertFalse((wrong / "duckbridge" / seeded.name).exists())
            for name in ("symlink", "oversized", "directory"):
                candidate = root / name
                candidate.mkdir()
                if name == "symlink":
                    linked = root / "linked.tar.gz"
                    linked.symlink_to(source)
                    with self.assertRaises(OSError):
                        fixture.seed_bridge_archive(linked, candidate)
                elif name == "oversized":
                    with mock.patch.object(fixture.os, "fstat", return_value=SimpleNamespace(st_mode=stat.S_IFREG, st_size=(128 << 20) + 1)):
                        with self.assertRaisesRegex(RuntimeError, "bounded regular"):
                            fixture.seed_bridge_archive(source, candidate)
                else:
                    with self.assertRaisesRegex(RuntimeError, "bounded regular"):
                        fixture.seed_bridge_archive(root, candidate)

    def test_external_artifact_root_is_fresh_private_and_owned(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            root.chmod(0o700)
            target = root / "fresh"
            self.assertEqual(fixture.prepare_artifact(root, target), target)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)
            with self.assertRaises(FileExistsError):
                fixture.prepare_artifact(root, target)
            linked = root / "linked"
            linked.symlink_to(target, target_is_directory=True)
            with self.assertRaises(FileExistsError):
                fixture.prepare_artifact(root, linked)
            with self.assertRaisesRegex(RuntimeError, "absolute path"):
                fixture.prepare_artifact(root, Path("relative"))
            with mock.patch.object(fixture.os, "geteuid", return_value=root.stat().st_uid + 1):
                with self.assertRaisesRegex(RuntimeError, "private and owned"):
                    fixture.prepare_artifact(root, root / "foreign")
            root.chmod(0o755)
            with self.assertRaisesRegex(RuntimeError, "private and owned"):
                fixture.prepare_artifact(root, root / "public-parent")

    def test_fresh_unit_refuses_existing_unknown_or_unsafe_identity(self):
        unit = "kelvo-containment-0123456789ab"
        for status, result, exists in (("loaded", 0, False), ("not-found", 1, False), ("not-found", 0, True)):
            with mock.patch.object(fixture, "run", return_value=subprocess.CompletedProcess([], result, status)), \
                    mock.patch.object(Path, "exists", return_value=exists):
                with self.assertRaisesRegex(RuntimeError, "already exists|cannot be checked"):
                    fixture.require_fresh_unit(unit)
        with mock.patch.object(fixture, "run") as run:
            with self.assertRaisesRegex(RuntimeError, "invalid owned"):
                fixture.require_fresh_unit("unrelated-production")
            run.assert_not_called()

    def test_parent_dependency_requires_current_membership_and_active_exact_service(self):
        parent = "kelvo-protected-readers-validation-0123456789ab.service"
        for member in ("/system.slice/" + parent, "/system.slice/" + parent + "/supervisor"):
            with mock.patch.object(Path, "read_text", return_value="0::" + member + "\n"), \
                    mock.patch.object(fixture, "run", return_value=subprocess.CompletedProcess([], 0, "active\n")):
                fixture.require_parent_unit(parent)
        for membership in ("0::/unrelated\n", "0::/system.slice/" + parent + "-other\n", "1:cpu:/unrelated\n"):
            with mock.patch.object(Path, "read_text", return_value=membership), mock.patch.object(fixture, "run") as run:
                with self.assertRaises(RuntimeError):
                    fixture.require_parent_unit(parent)
                run.assert_not_called()
        for status, code in (("inactive", 0), ("activating", 0), ("active", 1)):
            with mock.patch.object(Path, "read_text", return_value="0::/system.slice/" + parent + "\n"), \
                    mock.patch.object(fixture, "run", return_value=subprocess.CompletedProcess([], code, status)):
                with self.assertRaisesRegex(RuntimeError, "not active"):
                    fixture.require_parent_unit(parent)

    def test_timeout_preserves_bounded_partial_diagnostics_without_returning_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "worker.test.log"
            failure = subprocess.TimeoutExpired(["fixture"], 10, output=b"x" * (2 << 20) + b"partial cancellation trace\n", stderr=b"partial stderr trace\n")
            with mock.patch.object(fixture, "run", side_effect=failure):
                with self.assertRaises(subprocess.TimeoutExpired):
                    fixture.run_logged(["fixture"], log, timeout=10)
            self.assertLessEqual(log.stat().st_size, 1 << 20)
            self.assertTrue(log.read_text().startswith("[timeout diagnostic truncated;"))
            self.assertIn("partial cancellation trace", log.read_text())
            self.assertTrue(log.read_text().endswith("partial stderr trace\n"))
            with mock.patch.object(fixture, "run", side_effect=subprocess.TimeoutExpired(["fixture"], 10, output=b"short trace\n")):
                with self.assertRaises(subprocess.TimeoutExpired):
                    fixture.run_logged(["fixture"], log, timeout=10)
            self.assertEqual(log.read_bytes(), b"short trace\n")

    def test_bridge_provenance_rejects_wrong_paths_pins_and_changed_inputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            artifact = repo / "artifacts/fixture"
            directory = artifact / "duckbridge"
            headers, driver = directory / "headers", directory / "duckdb-go"
            headers.mkdir(parents=True)
            driver.mkdir()
            (headers / "header.hpp").write_text("fixture header")
            (driver / "native_connection.go").write_text("fixture driver")
            (directory / "duckbridge.mod").write_text("fixture mod")
            (directory / "duckbridge.sum").write_text("fixture sums")
            archive = directory / ("duckdb-" + fixture.bridge.VERSION + ".tar.gz")
            archive.write_bytes(b"fixture archive")
            (repo / "scripts").mkdir()
            patch = repo / "scripts/duckbridge-driver.patch"
            patch.write_text("fixture patch")
            pinned = fixture.digest(archive)
            build = {"duckdb": fixture.bridge.VERSION, "driver": fixture.bridge.MODULE_VERSION,
                     "module_sum": fixture.bridge.MODULE_SUM, "source_sha256": pinned,
                     "driver_patch_sha256": fixture.digest(patch), "tags": fixture.BRIDGE_TAGS,
                     "headers": str(headers), "header_count": 101,
                     "modfile": str(directory / "duckbridge.mod"), "CGO_CXXFLAGS": "-I" + str(headers)}
            manifest = directory / "build.json"
            manifest.write_text(json.dumps(build))
            with mock.patch.object(fixture.bridge, "ARCHIVE_SHA256", pinned):
                original = fixture.bridge_provenance(repo, artifact)
                for field, value in (("source_sha256", "f" * 64), ("module_sum", "wrong"),
                                     ("headers", "/other/headers"), ("modfile", "/other/duckbridge.mod"),
                                     ("CGO_CXXFLAGS", "-I/other/headers"), ("header_count", True)):
                    manifest.write_text(json.dumps(build | {field: value}))
                    with self.assertRaisesRegex(RuntimeError, "provenance mismatch"):
                        fixture.bridge_provenance(repo, artifact)
                manifest.write_text(json.dumps(build))
                (headers / "header.hpp").write_text("changed header")
                self.assertNotEqual(original, fixture.bridge_provenance(repo, artifact))
                (headers / "link.hpp").symlink_to(headers / "header.hpp")
                with self.assertRaisesRegex(RuntimeError, "symlink"):
                    fixture.bridge_provenance(repo, artifact)

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
        unit, description = "kelvo-containment-0123456789ab", "Kelvo containment acceptance " + "a" * 32
        outputs = ["loaded\n", description + "\n", "", "not-found\n"]
        with mock.patch.object(fixture, "run", side_effect=[subprocess.CompletedProcess([], 0, value) for value in outputs]) as run, \
                mock.patch.object(Path, "exists", return_value=False):
            self.assertEqual(fixture.cleanup_owned(unit, description), {"owned_service_removed": True, "owned_cgroup_removed": True})
            self.assertEqual(run.call_args_list[2].args[0], ["sudo", "-n", "systemctl", "stop", unit + ".service"])
            self.assertEqual(run.call_args_list[1].args[0][3], "--property=Description")
            self.assertEqual(run.call_args_list[3].args[0][2], unit + ".service")
        with mock.patch.object(fixture, "run", side_effect=subprocess.TimeoutExpired([], 20)):
            result = fixture.cleanup_owned(unit, description)
            self.assertFalse(result["owned_service_removed"])
            self.assertFalse(result["owned_cgroup_removed"])
            self.assertTrue(result["cleanup_failure"])

    def test_cleanup_does_not_equate_failed_status_lookup_with_absent_unit(self):
        with mock.patch.object(fixture, "run", return_value=subprocess.CompletedProcess([], 1, "not-found\n")), \
                mock.patch.object(Path, "exists", return_value=False):
            self.assertFalse(fixture.cleanup_owned("kelvo-containment-0123456789ab", "fixture")["owned_service_removed"])

    def test_cleanup_never_stops_a_foreign_description_or_adopts_unsafe_unit_name(self):
        unit = "kelvo-containment-0123456789ab"
        with mock.patch.object(fixture, "run", side_effect=[subprocess.CompletedProcess([], 0, "loaded\n"),
                                                             subprocess.CompletedProcess([], 0, "unrelated fixture\n")]) as run:
            self.assertTrue(fixture.cleanup_owned(unit, "owned fixture")["cleanup_failure"])
            self.assertFalse(any("stop" in call.args[0] for call in run.call_args_list))
        with mock.patch.object(fixture, "run") as run:
            self.assertTrue(fixture.cleanup_owned("unrelated-production", "fixture")["cleanup_failure"])
            run.assert_not_called()

    def test_already_removed_owned_service_needs_no_stop(self):
        with mock.patch.object(fixture, "run", return_value=subprocess.CompletedProcess([], 0, "not-found\n")) as run, \
                mock.patch.object(Path, "exists", return_value=False):
            result = fixture.cleanup_owned("kelvo-containment-0123456789ab", "fixture")
            self.assertEqual(result, {"owned_service_removed": True, "owned_cgroup_removed": True})
            self.assertFalse(any("stop" in call.args[0] for call in run.call_args_list))

    def test_unowned_service_fails_with_retained_missing_gate_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            args = SimpleNamespace(artifact=temporary, unit="kelvo-containment-testfixture", protected_objects=False, protected_queries=False)
            with mock.patch.object(Path, "read_text", return_value="0::/user.slice/unrelated.service\n"), \
                    mock.patch.object(fixture, "run") as run:
                self.assertEqual(fixture.inside(args), 1)
                run.assert_not_called()
            report = json.loads((Path(temporary) / "inside.json").read_text())
            self.assertTrue(report["failure"])
            self.assertTrue(all(value == "missing" for value in report["gates"].values()))

    def test_unowned_protected_service_retains_its_required_gate_and_mode(self):
        with tempfile.TemporaryDirectory() as temporary:
            args = SimpleNamespace(artifact=temporary, unit="kelvo-containment-testfixture", protected_objects=True, protected_queries=False)
            with mock.patch.object(Path, "read_text", return_value="0::/user.slice/unrelated.service\n"), \
                    mock.patch.object(fixture, "run") as run:
                self.assertEqual(fixture.inside(args), 1)
                run.assert_not_called()
            report = json.loads((Path(temporary) / "inside.json").read_text())
            self.assertEqual(report["mode"], fixture.PROTECTED_MODE)
            self.assertEqual(report["gates"][fixture.PROTECTED_GATE], "missing")

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
