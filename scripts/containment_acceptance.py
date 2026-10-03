#!/usr/bin/env python3
"""Run bounded cgroup-v2 acceptance in one disposable delegated Linux service.

Requires a non-root test account with noninteractive systemd-run/stop permission.
Missing kernel capabilities, skipped tests, changed sources and uncertain cleanup
fail the report. This runner never adopts another service or session cgroup.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import pwd
import re
import subprocess
import sys
import tempfile
import time
import uuid

KERNEL_GATES = [
    "TestKernelPrestartMembershipAndControls", "TestKernelNativeMemoryOOM",
    "TestKernelCPUQuota", "TestKernelPIDsLimit", "TestKernelDetachedDescendantCleanup",
    "TestKernelFailedCleanupQuarantinesCustody", "TestKernelRestartRecoversOwnedProcess",
    "TestKernelRestartRecoversIncompleteIntent", "TestKernelHierarchyOwnershipLock",
    "TestKernelStaleRootIdentityRejected", "TestKernelSandboxRuntime",
    "TestKernelMissingControllersFailsClosed",
]
WORKER_GATES = [
    "TestContainedWorkerSuccessAndCancellation", "TestContainedWorkerRefreshCustody",
    "TestContainedWorkerStartFailureCleansOwnership", "TestContainedWorkerRealDuckDBCTE",
]
STARTUP_GATE = "TestContainedNodeStartupPlacement"
EXPORT_GATE = "TestExportWorkerConstructorBindsKernelContainment"
OUTSIDE_GATE = "StartupPlacementOutsideDelegation"
REQUIRED = KERNEL_GATES + WORKER_GATES + [STARTUP_GATE, EXPORT_GATE, OUTSIDE_GATE]
BINARIES = ["containment.test", "worker.test", "startup.test", "export.test", "kelvo", "kelvo-landlock", "sandbox-runtime"]
HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
METADATA_NAMES = {".DS_Store"}
SOURCE_REQUIRED = {"go.mod", "go.sum", "sandbox/launcher.c", "internal/containment/manager_linux.go"}


def run(command, *, env=None, timeout=180):
    return subprocess.run(command, env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, timeout=timeout)


def gates(output, expected):
    observed = {}
    for status, name in re.findall(r"^--- (PASS|FAIL|SKIP): (\w+)\b", output, re.M):
        if name in expected:
            observed[name] = "duplicate" if name in observed else status.lower()
    return {name: observed.get(name, "missing") for name in expected}


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def source_manifest(repo):
    """Fingerprint tracked and nonignored source files; metadata is not source.

    Generated artifacts are excluded by Git's effective ignore rules.
    The same enumeration after execution detects added, deleted or changed input.
    """
    result = run(["git", "-C", str(repo), "ls-files", "--cached", "--others", "--exclude-standard", "-z"], timeout=15)
    if result.returncode:
        raise RuntimeError("source inventory unavailable")
    names = set(result.stdout.split("\0")) - {""}
    files = {}
    for name in sorted(names):
        relative = PurePosixPath(name)
        if relative.name in METADATA_NAMES or relative.name.startswith("._"):
            continue
        if relative.is_absolute() or ".." in relative.parts or str(relative) != name:
            raise RuntimeError("unsafe source path")
        path = repo / name
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(repo.resolve()):
            raise RuntimeError("source input is not a regular in-tree file")
        files[name] = digest(path)
    if not SOURCE_REQUIRED.issubset(files):
        raise RuntimeError("incomplete source inventory")
    revision = run(["git", "-C", str(repo), "rev-parse", "HEAD"], timeout=10)
    if revision.returncode or not re.fullmatch(r"[0-9a-f]{40,64}\n?", revision.stdout):
        raise RuntimeError("source revision unavailable")
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return {"base_revision": revision.stdout.strip(), "file_count": len(files),
            "sha256": hashlib.sha256(canonical).hexdigest(), "files": files,
            "excluded_metadata": [".DS_Store", "._*"], "verified": True}


def valid_source_identity(source):
    if not isinstance(source, dict) or source.get("verified") is not True:
        return False
    files = source.get("files")
    if (not isinstance(files, dict) or not SOURCE_REQUIRED.issubset(files)
            or type(source.get("file_count")) is not int or source["file_count"] != len(files)
            or source.get("excluded_metadata") != [".DS_Store", "._*"]
            or not isinstance(source.get("base_revision"), str)
            or not re.fullmatch(r"[0-9a-f]{40,64}", source["base_revision"])):
        return False
    for name, value in files.items():
        if not isinstance(name, str) or not isinstance(value, str) or not HEX_SHA256.fullmatch(value):
            return False
        relative = PurePosixPath(name)
        if (relative.is_absolute() or ".." in relative.parts or str(relative) != name
                or relative.name in METADATA_NAMES or relative.name.startswith("._")):
            return False
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return source.get("sha256") == hashlib.sha256(canonical).hexdigest()


def reconcile(report):
    source = report.get("source", {})
    binaries = report.get("binary_sha256", {})
    if not isinstance(source, dict) or not isinstance(binaries, dict) or not isinstance(report.get("gates"), dict):
        return False
    return (set(report["gates"]) == set(REQUIRED)
            and all(report["gates"][name] == "pass" for name in REQUIRED)
            and report.get("non_root") is True and report.get("capability_sets_zero") is True
            and report.get("source_unchanged") is True and valid_source_identity(source)
            and set(binaries) == set(BINARIES)
            and all(isinstance(value, str) and HEX_SHA256.fullmatch(value) for value in binaries.values())
            and all(type(report.get(key)) is int and report[key] == 0
                    for key in ("remaining_job_groups", "remaining_ownership_records", "service_exit_code"))
            and report.get("owned_service_removed") is True
            and report.get("owned_cgroup_removed") is True
            and not report.get("failure") and not report.get("execution_failed"))


def zero_capabilities(status):
    caps = dict(re.findall(r"^(Cap(?:Inh|Prm|Eff|Bnd|Amb)):\s+([0-9a-f]+)$", status, re.M))
    return len(caps) == 5 and not any(int(value, 16) for value in caps.values())


def test_environment(artifact, jobs, state):
    env = dict(os.environ, GOMAXPROCS="1", KELVO_TEST_CGROUP_ROOT=str(jobs),
               KELVO_TEST_CGROUP_STATE=str(state), KELVO_TEST_SANDBOX=str(artifact / "kelvo-landlock"),
               KELVO_TEST_RUNTIME_HELPER=str(artifact / "sandbox-runtime"),
               KELVO_TEST_BINARY=str(artifact / "kelvo"))
    env.pop("KELVO_TEST_EXPECT_PLACEMENT_REJECTION", None)
    return env


def wait_for_file(path, process=None, timeout=30):
    deadline = time.monotonic() + timeout
    while not path.exists():
        if process is not None and process.poll() is not None:
            raise RuntimeError("service exited before fixture readiness")
        if time.monotonic() >= deadline:
            raise TimeoutError("fixture readiness deadline")
        time.sleep(0.05)


def inside(args):
    artifact = Path(args.artifact)
    report = {"gates": {name: "missing" for name in REQUIRED if name != OUTSIDE_GATE}, "observations": {}}
    try:
        relative = Path("/proc/self/cgroup").read_text().strip().split(":", 2)[2]
        if relative != "/system.slice/" + args.unit + ".service":
            raise RuntimeError("runner does not own expected transient service")
        root = Path("/sys/fs/cgroup") / relative.lstrip("/")
        supervisor, jobs = root / "supervisor", root / "jobs"
        supervisor.mkdir(mode=0o755)
        (supervisor / "cgroup.procs").write_text(str(os.getpid()))
        (root / "cgroup.subtree_control").write_text("+cpu +memory +pids")
        jobs.mkdir(mode=0o755)
        state = artifact / "state"
        state.mkdir(mode=0o700)
        status = Path("/proc/self/status").read_text()
        if os.geteuid() == 0 or not zero_capabilities(status):
            raise RuntimeError("acceptance requires non-root with zero capabilities")
        report.update(non_root=True, capability_sets_zero=True)
        (artifact / "fixture-ready.json").write_text(json.dumps({"jobs": str(jobs), "state": str(state)}))
        wait_for_file(artifact / "outside-complete", timeout=60)
        env = test_environment(artifact, jobs, state)
        for binary, pattern, required in [("containment.test", "^TestKernel", KERNEL_GATES),
                                          ("worker.test", "^TestContainedWorker", WORKER_GATES),
                                          ("startup.test", "^" + STARTUP_GATE + "$", [STARTUP_GATE]),
                                          ("export.test", "^" + EXPORT_GATE + "$", [EXPORT_GATE])]:
            result = run([str(artifact / binary), "-test.v", "-test.run=" + pattern, "-test.timeout=90s"], env=env, timeout=100)
            (artifact / (binary + ".log")).write_text(result.stdout)
            report["gates"].update(gates(result.stdout, required))
            if result.returncode:
                report["execution_failed"] = True
            for key in ["charged_native_peak_bytes", "oom_kills", "cpu_usage_usec", "throttled_periods"]:
                match = re.search(r"\b" + key + r"=(\d+)", result.stdout)
                if match:
                    report["observations"][key] = int(match[1])
        report["remaining_job_groups"] = sum(item.is_dir() for item in jobs.iterdir())
        report["remaining_ownership_records"] = sum(item.name != ".kelvo-containment.lock" for item in state.iterdir())
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
        report["failure"] = "kernel-service:" + type(exc).__name__
    finally:
        (artifact / "inside.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if (all(value == "pass" for value in report["gates"].values())
                 and not report.get("failure") and not report.get("execution_failed")
                 and report.get("remaining_job_groups") == 0
                 and report.get("remaining_ownership_records") == 0) else 1


def outside_check(artifact, unit, process):
    wait_for_file(artifact / "fixture-ready.json", process)
    fixture = json.loads((artifact / "fixture-ready.json").read_text())
    expected = "/sys/fs/cgroup/system.slice/" + unit + ".service/jobs"
    if fixture != {"jobs": expected, "state": str(artifact / "state")}:
        raise RuntimeError("fixture ownership mismatch")
    env = test_environment(artifact, fixture["jobs"], fixture["state"])
    env["KELVO_TEST_EXPECT_PLACEMENT_REJECTION"] = "yes"
    try:
        result = run([str(artifact / "startup.test"), "-test.v", "-test.run=^" + STARTUP_GATE + "$", "-test.timeout=20s"], env=env, timeout=25)
        (artifact / "startup-outside.log").write_text(result.stdout)
        return gates(result.stdout, [STARTUP_GATE])[STARTUP_GATE] if result.returncode == 0 else "fail"
    finally:
        (artifact / "outside-complete").touch()


def cleanup_owned(unit):
    # Only this invocation's unpredictable exact unit is ever stopped.
    result = {"owned_service_removed": False, "owned_cgroup_removed": False}
    try:
        run(["sudo", "-n", "systemctl", "stop", unit + ".service"], timeout=20)
        state = run(["systemctl", "show", unit + ".service", "--property=LoadState", "--value"], timeout=10)
        result["owned_service_removed"] = state.returncode == 0 and state.stdout.strip() == "not-found"
        result["owned_cgroup_removed"] = not Path("/sys/fs/cgroup/system.slice", unit + ".service").exists()
    except (OSError, subprocess.TimeoutExpired):
        result["cleanup_failure"] = True
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--go", default="go")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--artifact", help=argparse.SUPPRESS)
    parser.add_argument("--unit", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if sys.platform != "linux" or os.geteuid() == 0:
        parser.error("run on the designated Linux test machine as a non-root account")
    if args.inside:
        return inside(args)
    args.report = args.report.resolve()
    if args.report.exists():
        parser.error("preserve prior reports; choose a new report path")
    repo = args.repo.resolve()
    (repo / "artifacts").mkdir(exist_ok=True)
    artifact = Path(tempfile.mkdtemp(prefix="containment-", dir=repo / "artifacts"))
    artifact.chmod(0o700)
    unit = "kelvo-containment-" + uuid.uuid4().hex[:12]
    report = {"schema": 2, "scope": "single Linux node process-tree containment and query/refresh custody",
              "passed": False, "gates": {name: "missing" for name in REQUIRED}, "limitations": [
                  "No hostile native escape reproduction was executed.",
                  "Native charged memory is not parent RSS or whole-node memory.",
                  "Stale root identity rejection is tested; arbitrary host reboot recovery is not promised.",
                  "No throughput or multi-host production capacity claim is made."],
              "kernel": os.uname().release, "architecture": os.uname().machine}
    started, stage, process = time.monotonic(), "source", None
    try:
        os.chdir(repo)
        report["source"] = source_manifest(repo)
        env = dict(os.environ, GOMAXPROCS="1")
        version = run([args.go, "version"], env=env, timeout=15)
        if version.returncode:
            raise RuntimeError("Go toolchain unavailable")
        report["go_version"] = version.stdout.strip()
        commands = [[args.go, "test", "-p", "1", "-c", "-o", str(artifact / "containment.test"), "./internal/containment"],
                    [args.go, "test", "-p", "1", "-c", "-o", str(artifact / "worker.test"), "./internal/worker"],
                    [args.go, "test", "-p", "1", "-c", "-o", str(artifact / "startup.test"), "./cmd/kelvo"],
                    [args.go, "test", "-p", "1", "-c", "-o", str(artifact / "export.test"), "./internal/cluster"],
                    [args.go, "build", "-tags", "duckdb_arrow", "-p", "1", "-o", str(artifact / "kelvo"), "./cmd/kelvo"],
                    ["cc", "-O2", "-Wall", "-Wextra", "-Werror", "sandbox/launcher.c", "-o", str(artifact / "kelvo-landlock")],
                    ["c++", "-O2", "-std=c++17", "-pthread", "internal/containment/testdata/sandbox_runtime.cc", "-o", str(artifact / "sandbox-runtime")]]
        for number, command in enumerate(commands):
            stage = "build-" + str(number)
            result = run(command, env=env, timeout=300)
            (artifact / (stage + ".log")).write_text(result.stdout)
            if result.returncode:
                raise RuntimeError("bounded build failed")
        report["binary_sha256"] = {name: digest(artifact / name) for name in BINARIES}
        account = pwd.getpwuid(os.geteuid()).pw_name
        command = ["sudo", "-n", "systemd-run", "--unit=" + unit, "--uid=" + account,
                   "--property=Delegate=yes", "--property=MemoryMax=1G", "--property=CPUQuota=100%",
                   "--property=TasksMax=256", "--property=NoNewPrivileges=yes",
                   "--property=CapabilityBoundingSet=", "--property=AmbientCapabilities=", "--collect", "--wait", "--pipe",
                   sys.executable, str(Path(__file__).resolve()), "--inside", "--artifact", str(artifact),
                   "--unit", unit, "--report", str(args.report)]
        stage = "kernel-service"
        with (artifact / "service.log").open("w") as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            report["gates"][OUTSIDE_GATE] = outside_check(artifact, unit, process)
            report["service_exit_code"] = process.wait(timeout=330)
        if (artifact / "inside.json").exists():
            inner = json.loads((artifact / "inside.json").read_text())
            report["gates"].update(inner.pop("gates", {}))
            report.update(inner)
    except (OSError, RuntimeError, subprocess.TimeoutExpired, ValueError) as exc:
        report["failure"] = stage + ":" + type(exc).__name__
    finally:
        report.update(cleanup_owned(unit))
        if process is not None:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.terminate()
                report["failure"] = "service-control-did-not-exit"
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    report["failure"] = "service-control-unreaped"
        try:
            report["source_unchanged"] = source_manifest(repo) == report.get("source")
        except (OSError, RuntimeError, subprocess.TimeoutExpired):
            report["source_unchanged"] = False
        report["passed"] = reconcile(report)
        report["duration_seconds"] = round(time.monotonic() - started, 3)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "source"}, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
