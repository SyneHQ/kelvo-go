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
import stat
import subprocess
import sys
import tempfile
import time
import uuid

# Direct invocation must not write a cache before the source identity is sampled.
sys.dont_write_bytecode = True
import provision_duckbridge as bridge
import protected_query_acceptance as protected_query

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
PROTECTED_GATE = "TestContainedWorkerProtectedObjects"
PROTECTED_LEAVES = [PROTECTED_GATE + "/" + name for name in (
    "publish-single-and-multipart", "verify-and-inventory-single-and-descriptor-layouts",
    "resolved-child-catalog-has-no-provider-identity",
    "cte-join-types-policy-and-shared-manager", "hidden-column-and-uncontained-refusal",
    "immutable-runtime-refuses-retargeting", "cancellation-joins-ranges-and-child",
    "binding-loss-after-last-batch-refuses-completion")]
BASE_MODE, PROTECTED_MODE = "containment", "protected-objects"
QUERY_MODE = "protected-queries"
BRIDGE_TAGS = "duckdb_arrow,duckbridge"
BRIDGE_HASHES = {"driver_patch_sha256", "headers_sha256", "driver_tree_sha256", "modfile_sha256", "sumfile_sha256"}
BINARIES = ["containment.test", "worker.test", "startup.test", "export.test", "kelvo", "kelvo-landlock", "sandbox-runtime"]
HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
METADATA_NAMES = {".DS_Store"}
SOURCE_REQUIRED = {"go.mod", "go.sum", "sandbox/launcher.c", "internal/containment/manager_linux.go"}
PROTECTED_SOURCE_REQUIRED = {"scripts/provision_duckbridge.py", "scripts/duckbridge-driver.patch",
                             "internal/worker/protected_object_worker_linux_test.go"}
UNIT_PATTERN = re.compile(r"^kelvo-containment-[0-9a-f]{12}$")
PARENT_PATTERN = re.compile(r"^kelvo-protected-readers-validation-[0-9a-f]{12}\.service$")
RESOURCE_LIMITS = {"MemoryMax": "1G", "MemorySwapMax": "0", "CPUQuota": "100%", "TasksMax": "256",
                   "RuntimeMaxSec": "600", "TimeoutStopSec": "30", "KillMode": "control-group", "LimitFSIZE": "5G"}
LIVE_NUMERIC_LIMITS = {"MemoryMax": 1 << 30, "MemorySwapMax": 0, "TasksMax": 256,
                       "LimitFSIZE": 5 << 30, "LimitFSIZESoft": 5 << 30}
LIVE_TIME_LIMITS = {"CPUQuotaPerSecUSec": 1_000_000, "RuntimeMaxUSec": 600_000_000, "TimeoutStopUSec": 30_000_000}
LIVE_PROPERTIES = {*LIVE_NUMERIC_LIMITS, *LIVE_TIME_LIMITS, "Id", "Description", "InvocationID", "MainPID", "User",
                   "Transient", "ControlGroup", "ActiveState", "LoadState", "Delegate", "KillMode",
                   "Requisite", "BindsTo", "After"}


def run(command, *, env=None, timeout=180):
    return subprocess.run(command, env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, timeout=timeout)


def run_logged(command, log, *, env=None, timeout=180):
    try:
        result = run(command, env=env, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        chunks = [value.encode("utf-8") if isinstance(value, str) else value
                  for value in (exc.stdout, exc.stderr) if value]
        # subprocess merges stderr into stdout. Bound the retained diagnostic
        # even if the timed-out process flooded its capture pipe.
        maximum = 1 << 20
        output = b"\n".join(chunk[-maximum:] for chunk in chunks)
        if sum(len(chunk) for chunk in chunks) + max(0, len(chunks) - 1) > maximum:
            marker = b"[timeout diagnostic truncated; only the final output bytes follow]\n"
            output = marker + output[-(maximum - len(marker)):]
        log.write_bytes(output)
        raise
    log.write_text(result.stdout)
    return result


def gates(output, expected):
    observed = {}
    for status, name in re.findall(r"^--- (PASS|FAIL|SKIP): (\w+)\b", output, re.M):
        if name in expected:
            observed[name] = "duplicate" if name in observed else status.lower()
    for root, leaves in ((PROTECTED_GATE, PROTECTED_LEAVES), (protected_query.ROOT, protected_query.LEAVES)):
        if root in expected:
            children = child_gates(output, root, leaves)
            if set(children) != {"outer", "inner", *leaves} or not all(value == "pass" for value in children.values()):
                observed[root] = "fail"
    return {name: observed.get(name, "missing") for name in expected}


def required_gates(protected=False, queries=False):
    return REQUIRED + ([PROTECTED_GATE] if protected else []) + ([protected_query.ROOT] if queries else [])


def protected_gates(output):
    return child_gates(output, PROTECTED_GATE, PROTECTED_LEAVES)


def child_gates(output, root, leaves):
    # Child output is indented by t.Log. Require both terminal root records and
    # every frozen leaf: an outer PASS cannot hide a skip or truncated capture.
    expected = ["outer", "inner", *leaves]
    observed = {}
    for indent, status, name in re.findall(
            r"^([ \t]*)--- (PASS|FAIL|SKIP): (" + re.escape(root) + r"(?:/[^\s]+)?)\s", output, re.M):
        key = ("inner" if indent else "outer") if name == root else name
        observed[key] = "duplicate" if key in observed else status.lower()
    return {name: observed.get(name, "missing") for name in expected} | {
        name: value for name, value in observed.items() if name not in expected}


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def tree_digest(root):
    files = {}
    if root.is_symlink() or not root.is_dir():
        raise RuntimeError("bridge input directory is invalid")
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise RuntimeError("bridge input contains a symlink")
        if path.is_file():
            files[str(path.relative_to(root))] = digest(path)
        elif not path.is_dir():
            raise RuntimeError("bridge input is not a regular file")
    if not files:
        raise RuntimeError("bridge input directory is empty")
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def seed_bridge_archive(source, artifact):
    if not source.is_absolute():
        raise RuntimeError("cached bridge archive path must be absolute")
    destination = artifact / "duckbridge" / ("duckdb-" + bridge.VERSION + ".tar.gz")
    destination.parent.mkdir(mode=0o700)
    descriptor = os.open(source, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= 128 << 20:
            raise RuntimeError("cached bridge archive is not a bounded regular file")
        checksum, size = hashlib.sha256(), 0
        with os.fdopen(descriptor, "rb", closefd=False) as incoming, destination.open("xb") as outgoing:
            while block := incoming.read(1 << 20):
                size += len(block)
                if size > 128 << 20:
                    raise RuntimeError("cached bridge archive exceeds its size bound")
                checksum.update(block)
                outgoing.write(block)
        if size != info.st_size or checksum.hexdigest() != bridge.ARCHIVE_SHA256:
            raise RuntimeError("cached bridge archive checksum mismatch")
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    finally:
        os.close(descriptor)


def prepare_artifact(repo, requested=None):
    if requested is None:
        (repo / "artifacts").mkdir(exist_ok=True)
        artifact = Path(tempfile.mkdtemp(prefix="containment-", dir=repo / "artifacts"))
        artifact.chmod(0o700)
        return artifact
    parent = requested.parent
    if not requested.is_absolute() or parent.resolve() != parent or not parent.is_dir():
        raise RuntimeError("artifact root requires an absolute path under an existing private directory")
    info = parent.stat()
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise RuntimeError("artifact parent must be private and owned by the test account")
    requested.mkdir(mode=0o700) # Existing directories, including symlinks, are never adopted.
    return requested


def require_fresh_unit(unit):
    if not UNIT_PATTERN.fullmatch(unit):
        raise RuntimeError("invalid owned service name")
    state = run(["systemctl", "show", unit + ".service", "--property=LoadState", "--value"], timeout=10)
    if state.returncode or state.stdout.strip() != "not-found" or Path("/sys/fs/cgroup/system.slice", unit + ".service").exists():
        raise RuntimeError("requested service or cgroup already exists or ownership cannot be checked")


def require_parent_unit(unit):
    if not PARENT_PATTERN.fullmatch(unit):
        raise RuntimeError("invalid validation parent service name")
    rows = Path("/proc/self/cgroup").read_text().splitlines()
    expected = "/system.slice/" + unit
    if len(rows) != 1 or not rows[0].startswith("0::"):
        raise RuntimeError("validation parent requires unified cgroup membership")
    current = rows[0][3:]
    if current != expected and not current.startswith(expected + "/"):
        raise RuntimeError("runner is outside the declared validation parent")
    state = run(["systemctl", "show", unit, "--property=ActiveState", "--value"], timeout=10)
    if state.returncode or state.stdout.strip() != "active":
        raise RuntimeError("validation parent is not active")


def duration_usec(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]+(?:us|ms|min|s|h)(?: [0-9]+(?:us|ms|min|s|h))*", value):
        return None
    units = {"us": 1, "ms": 1_000, "s": 1_000_000, "min": 60_000_000, "h": 3_600_000_000}
    return sum(int(amount) * units[unit] for amount, unit in re.findall(r"([0-9]+)(us|ms|min|s|h)", value))


def valid_live_service(evidence, unit, description, user, parent=None):
    if (not isinstance(evidence, dict) or evidence.get("verified") is not True or evidence.get("failure")
            or not isinstance(unit, str) or not UNIT_PATTERN.fullmatch(unit)
            or not isinstance(description, str) or not re.fullmatch(r"Kelvo containment acceptance [0-9a-f]{32}", description)
            or not isinstance(user, str) or not user or user == "root"
            or parent is not None and (not isinstance(parent, str) or not PARENT_PATTERN.fullmatch(parent))):
        return False
    properties = evidence.get("properties")
    if not isinstance(properties, dict) or set(properties) != LIVE_PROPERTIES or not all(isinstance(value, str) for value in properties.values()):
        return False
    expected = {"Id": unit + ".service", "Description": description, "User": user,
                "ControlGroup": "/system.slice/" + unit + ".service", "ActiveState": "active", "LoadState": "loaded",
                "Transient": "yes", "Delegate": "yes", "KillMode": "control-group"}
    if (any(properties[key] != value for key, value in expected.items())
            or not re.fullmatch(r"[0-9a-f]{32}", properties["InvocationID"])
            or not re.fullmatch(r"[1-9][0-9]*", properties["MainPID"])
            or any(properties[key] != str(value) for key, value in LIVE_NUMERIC_LIMITS.items())
            or any(duration_usec(properties[key]) != value for key, value in LIVE_TIME_LIMITS.items())):
        return False
    for key in ("Requisite", "BindsTo", "After"):
        dependencies = properties[key].split()
        if len(set(dependencies)) != len(dependencies) or (parent is not None and parent not in dependencies):
            return False
        if parent is None and any(PARENT_PATTERN.fullmatch(value) for value in dependencies):
            return False
    return True


def capture_live_service(artifact, unit, description, user, parent=None):
    evidence = {"verified": False, "properties": {}}
    try:
        result = run_logged(["systemctl", "show", unit + ".service", "--all",
                             "--property=" + ",".join(sorted(LIVE_PROPERTIES))], artifact / "live-service.log", timeout=10)
        if result.returncode:
            return evidence
        properties = evidence["properties"]
        for line in result.stdout.splitlines():
            key, separator, value = line.partition("=")
            if not separator or key not in LIVE_PROPERTIES or key in properties:
                return evidence
            properties[key] = value
        evidence["verified"] = True
        evidence["verified"] = valid_live_service(evidence, unit, description, user, parent)
        return evidence
    except (OSError, subprocess.TimeoutExpired) as exc:
        evidence["failure"] = type(exc).__name__
        return evidence
    finally:
        (artifact / "live-service.json").write_text(json.dumps(evidence, indent=2) + "\n")


def checked_outside_probe(artifact, unit, process, report, *, description, user, parent=None, protected=False):
    wait_for_file(artifact / "fixture-ready.json", process)
    report["live_service"] = capture_live_service(artifact, unit, description, user, parent)
    if not report["live_service"]["verified"]:
        # The inside service stays at its rendezvous. Do not create the release
        # marker or run even the outside-placement gate under unproven bounds.
        raise RuntimeError("live delegated service identity, limits or dependencies do not match")
    return outside_check(artifact, unit, process, protected)


def bridge_provenance(repo, artifact):
    directory = artifact / "duckbridge"
    headers, modfile = directory / "headers", directory / "duckbridge.mod"
    manifest = json.loads((directory / "build.json").read_text())
    expected = {"duckdb": bridge.VERSION, "driver": bridge.MODULE_VERSION,
                "module_sum": bridge.MODULE_SUM, "source_sha256": bridge.ARCHIVE_SHA256,
                "driver_patch_sha256": digest(repo / "scripts/duckbridge-driver.patch"),
                "tags": BRIDGE_TAGS, "headers": str(headers), "modfile": str(modfile),
                "CGO_CXXFLAGS": "-I" + str(headers)}
    if (not isinstance(manifest, dict) or any(manifest.get(key) != value for key, value in expected.items())
            or type(manifest.get("header_count")) is not int or manifest["header_count"] < 100
            or digest(directory / ("duckdb-" + bridge.VERSION + ".tar.gz")) != bridge.ARCHIVE_SHA256):
        raise RuntimeError("pinned bridge provenance mismatch")
    return {key: expected[key] for key in ("duckdb", "driver", "module_sum", "source_sha256", "driver_patch_sha256", "tags")} | {
        "headers_sha256": tree_digest(headers), "driver_tree_sha256": tree_digest(directory / "duckdb-go"),
        "modfile_sha256": digest(modfile), "sumfile_sha256": digest(modfile.with_suffix(".sum")),
    }


def valid_bridge_provenance(report):
    provenance = report.get("bridge")
    source = report.get("source", {})
    if not isinstance(provenance, dict) or not isinstance(source, dict) or not isinstance(source.get("files"), dict):
        return False
    return (PROTECTED_SOURCE_REQUIRED.issubset(source["files"])
            and provenance.get("duckdb") == bridge.VERSION and provenance.get("driver") == bridge.MODULE_VERSION
            and provenance.get("module_sum") == bridge.MODULE_SUM and provenance.get("source_sha256") == bridge.ARCHIVE_SHA256
            and provenance.get("tags") == BRIDGE_TAGS and report.get("bridge_inputs_unchanged") is True
            and report.get("resource_limits") == RESOURCE_LIMITS
            and provenance.get("driver_patch_sha256") == source["files"].get("scripts/duckbridge-driver.patch")
            and all(isinstance(provenance.get(name), str) and HEX_SHA256.fullmatch(provenance[name]) for name in BRIDGE_HASHES))


def build_commands(go, artifact, protected=False):
    flags = (["-mod=readonly", "-modfile", str(artifact / "duckbridge/duckbridge.mod"), "-tags", BRIDGE_TAGS]
             if protected else [])
    commands = [[go, "test", "-p", "1", *flags, "-c", "-o", str(artifact / binary), package]
                for binary, package in [("containment.test", "./internal/containment"), ("worker.test", "./internal/worker"),
                                        ("startup.test", "./cmd/kelvo"), ("export.test", "./internal/cluster")]]
    commands += [[go, "build", *(flags if protected else ["-tags", "duckdb_arrow"]), "-p", "1", "-o", str(artifact / "kelvo"), "./cmd/kelvo"],
                 ["cc", "-O2", "-Wall", "-Wextra", "-Werror", "sandbox/launcher.c", "-o", str(artifact / "kelvo-landlock")],
                 ["c++", "-O2", "-std=c++17", "-pthread", "internal/containment/testdata/sandbox_runtime.cc", "-o", str(artifact / "sandbox-runtime")]]
    return commands


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
    mode = report.get("mode", BASE_MODE) # Existing schema-2 reports describe the baseline gate only.
    if mode not in (BASE_MODE, PROTECTED_MODE, QUERY_MODE):
        return False
    queries, protected = mode == QUERY_MODE, mode in (PROTECTED_MODE, QUERY_MODE)
    schema = report.get("schema", 2)
    if type(schema) is not int or schema not in (2, 3):
        return False
    if protected or schema == 3 or "live_service" in report:
        if not valid_live_service(report.get("live_service"), report.get("owned_unit"), report.get("service_description"),
                                  report.get("service_user"), report.get("parent_unit")):
            return False
    required = required_gates(protected, queries)
    if protected:
        expected = {"outer", "inner", *PROTECTED_LEAVES}
        children = report.get("protected_gates")
        if not isinstance(children, dict) or set(children) != expected or any(value != "pass" for value in children.values()):
            return False
    if queries:
        children = report.get("protected_query_gates")
        broker = report.get("broker_binary")
        if (not isinstance(children, dict) or set(children) != {"outer", "inner", *protected_query.LEAVES}
                or any(value != "pass" for value in children.values()) or not isinstance(broker, dict)
                or broker.get("version") != protected_query.VERSION or broker.get("sha256") != binaries.get("nats-server")
                or type(broker.get("bytes")) is not int or not 0 < broker["bytes"] <= protected_query.MAX_BINARY
                or report.get("broker_binary_unchanged") is not True
                or not protected_query.SOURCE_REQUIRED.issubset(source.get("files", {}))
                or not protected_query.valid_broker(report.get("broker_custody"), "/system.slice/" + report["owned_unit"] + ".service/supervisor")):
            return False
    elif any(key in report for key in ("protected_query_gates", "broker_binary", "broker_custody", "broker_binary_unchanged")):
        return False
    return (set(report["gates"]) == set(required)
            and all(report["gates"][name] == "pass" for name in required)
            and (valid_bridge_provenance(report) if protected else "bridge" not in report)
            and report.get("non_root") is True and report.get("capability_sets_zero") is True
            and report.get("source_unchanged") is True and valid_source_identity(source)
            and set(binaries) == set(BINARIES + (["nats-server"] if queries else []))
            and all(isinstance(value, str) and HEX_SHA256.fullmatch(value) for value in binaries.values())
            and all(type(report.get(key)) is int and report[key] == 0
                    for key in ("remaining_job_groups", "remaining_ownership_records", "service_exit_code"))
            and report.get("owned_service_removed") is True
            and report.get("owned_cgroup_removed") is True
            and not report.get("failure") and not report.get("execution_failed"))


def zero_capabilities(status):
    caps = dict(re.findall(r"^(Cap(?:Inh|Prm|Eff|Bnd|Amb)):\s+([0-9a-f]+)$", status, re.M))
    return len(caps) == 5 and not any(int(value, 16) for value in caps.values())


def test_environment(artifact, jobs, state, protected=False):
    env = {key: value for key, value in os.environ.items() if key in ("PATH", "HOME", "LANG", "LC_ALL", "TMPDIR")}
    env.update(GOMAXPROCS="1", KELVO_TEST_CGROUP_ROOT=str(jobs),
               KELVO_TEST_CGROUP_STATE=str(state), KELVO_TEST_SANDBOX=str(artifact / "kelvo-landlock"),
               KELVO_TEST_RUNTIME_HELPER=str(artifact / "sandbox-runtime"),
               KELVO_TEST_BINARY=str(artifact / "kelvo"))
    if protected:
        env["KELVO_TEST_PROTECTED_OBJECTS"] = "1"
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
    protected, queries = args.protected_objects, args.protected_queries
    report = {"mode": QUERY_MODE if queries else PROTECTED_MODE if protected else BASE_MODE,
              "gates": {name: "missing" for name in required_gates(protected, queries) if name != OUTSIDE_GATE}, "observations": {}}
    if protected:
        report["protected_gates"] = protected_gates("")
    if queries:
        report["protected_query_gates"] = child_gates("", protected_query.ROOT, protected_query.LEAVES)
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
        env = test_environment(artifact, jobs, state, protected)
        for binary, pattern, required in [("containment.test", "^TestKernel", KERNEL_GATES),
                                          ("worker.test", "^TestContainedWorker", WORKER_GATES + ([PROTECTED_GATE] if protected else [])),
                                          ("startup.test", "^" + STARTUP_GATE + "$", [STARTUP_GATE]),
                                          ("export.test", "^" + EXPORT_GATE + "$", [EXPORT_GATE])]:
            result = run_logged([str(artifact / binary), "-test.v", "-test.run=" + pattern, "-test.timeout=90s"],
                                artifact / (binary + ".log"), env=env, timeout=100)
            report["gates"].update(gates(result.stdout, required))
            if protected and binary == "worker.test":
                report["protected_gates"] = protected_gates(result.stdout)
            if result.returncode:
                report["execution_failed"] = True
            for key in ["charged_native_peak_bytes", "oom_kills", "cpu_usage_usec", "throttled_periods"]:
                match = re.search(r"\b" + key + r"=(\d+)", result.stdout)
                if match:
                    report["observations"][key] = int(match[1])
        if queries:
            identity = json.loads((artifact / "broker-binary.json").read_text())
            broker = protected_query.QueryBroker(artifact / "query-broker", artifact / "nats-server", identity["sha256"], relative + "/supervisor")
            try:
                with broker:
                    env.update(broker.environment, KELVO_TEST_PROTECTED_QUERIES="1")
                    result = run_logged([str(artifact / "export.test"), "-test.v", "-test.run=^" + protected_query.ROOT + "$", "-test.timeout=330s"],
                                        artifact / "protected-query.test.log", env=env, timeout=340)
                    report["gates"].update(gates(result.stdout, [protected_query.ROOT]))
                    report["protected_query_gates"] = child_gates(result.stdout, protected_query.ROOT, protected_query.LEAVES)
                    if result.returncode:
                        report["execution_failed"] = True
            finally:
                report["broker_custody"] = broker.receipt
            if not protected_query.valid_broker(broker.receipt, relative + "/supervisor"):
                raise RuntimeError("owned broker cleanup incomplete")
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


def outside_check(artifact, unit, process, protected=False):
    wait_for_file(artifact / "fixture-ready.json", process)
    fixture = json.loads((artifact / "fixture-ready.json").read_text())
    expected = "/sys/fs/cgroup/system.slice/" + unit + ".service/jobs"
    if fixture != {"jobs": expected, "state": str(artifact / "state")}:
        raise RuntimeError("fixture ownership mismatch")
    env = test_environment(artifact, fixture["jobs"], fixture["state"], protected)
    env["KELVO_TEST_EXPECT_PLACEMENT_REJECTION"] = "yes"
    try:
        result = run_logged([str(artifact / "startup.test"), "-test.v", "-test.run=^" + STARTUP_GATE + "$", "-test.timeout=20s"],
                            artifact / "startup-outside.log", env=env, timeout=25)
        return gates(result.stdout, [STARTUP_GATE])[STARTUP_GATE] if result.returncode == 0 else "fail"
    finally:
        (artifact / "outside-complete").touch()


def cleanup_owned(unit, description):
    # Only this invocation's unpredictable exact unit and ownership marker may
    # be stopped, including after a failed or interrupted systemd-run launch.
    result = {"owned_service_removed": False, "owned_cgroup_removed": False}
    try:
        if not UNIT_PATTERN.fullmatch(unit):
            raise RuntimeError("invalid owned service name")
        state = run(["systemctl", "show", unit + ".service", "--property=LoadState", "--value"], timeout=10)
        if state.returncode:
            raise RuntimeError("service ownership lookup failed")
        if state.stdout.strip() != "not-found":
            owner = run(["systemctl", "show", unit + ".service", "--property=Description", "--value"], timeout=10)
            if owner.returncode or owner.stdout.strip() != description:
                raise RuntimeError("refusing to stop an unowned service")
            run(["sudo", "-n", "systemctl", "stop", unit + ".service"], timeout=40)
        state = run(["systemctl", "show", unit + ".service", "--property=LoadState", "--value"], timeout=10)
        result["owned_service_removed"] = state.returncode == 0 and state.stdout.strip() == "not-found"
        result["owned_cgroup_removed"] = not Path("/sys/fs/cgroup/system.slice", unit + ".service").exists()
    except (OSError, RuntimeError, subprocess.TimeoutExpired):
        result["cleanup_failure"] = True
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--go", default="go")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--protected-objects", action="store_true", help="require native protected-object refresh/query acceptance using the pinned bridge")
    parser.add_argument("--protected-queries", action="store_true", help="also require authenticated protected queries through a private local TLS broker")
    parser.add_argument("--nats-server", type=Path, help="existing cached NATS binary; never downloaded")
    parser.add_argument("--nats-sha256", help="exact SHA-256 of the cached NATS binary")
    parser.add_argument("--bridge-archive", type=Path, help="checksum-pinned cached DuckDB source archive; protected mode only")
    parser.add_argument("--artifact-root", type=Path, help="fresh absolute artifact directory beneath a private owned parent")
    parser.add_argument("--parent-unit", help="active kelvo-protected-readers-validation-<12 hex digits>.service containing this runner")
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--artifact", help=argparse.SUPPRESS)
    parser.add_argument("--unit", help="fresh kelvo-containment-<12 hex digits> service name")
    args = parser.parse_args()
    if sys.platform != "linux" or os.geteuid() == 0:
        parser.error("run on the designated Linux test machine as a non-root account")
    if args.unit is not None and not UNIT_PATTERN.fullmatch(args.unit):
        parser.error("unit must be kelvo-containment- followed by 12 lowercase hex digits")
    if args.bridge_archive is not None and not args.protected_objects:
        parser.error("--bridge-archive requires --protected-objects")
    if args.protected_queries and not args.protected_objects:
        parser.error("--protected-queries requires --protected-objects")
    if (args.nats_server is not None or args.nats_sha256 is not None) and not args.protected_queries:
        parser.error("broker inputs require --protected-queries")
    if args.protected_queries and not args.inside and (args.nats_server is None or args.nats_sha256 is None):
        parser.error("protected queries require the cached broker and its SHA-256")
    if args.parent_unit is not None and not PARENT_PATTERN.fullmatch(args.parent_unit):
        parser.error("invalid validation parent service name")
    if args.inside:
        if args.artifact is None or args.unit is None:
            parser.error("inside service requires its exact artifact directory and unit")
        return inside(args)
    args.report = args.report.resolve()
    if args.report.exists():
        parser.error("preserve prior reports; choose a new report path")
    repo = args.repo.resolve()
    try:
        artifact = prepare_artifact(repo, args.artifact_root)
    except (OSError, RuntimeError) as exc:
        parser.error(str(exc))
    unit = args.unit or "kelvo-containment-" + uuid.uuid4().hex[:12]
    description = "Kelvo containment acceptance " + uuid.uuid4().hex
    (artifact / "run.json").write_text(json.dumps({"unit": unit, "description": description, "parent_unit": args.parent_unit,
        "artifact": str(artifact), "resource_limits": RESOURCE_LIMITS}, indent=2) + "\n")
    report = {"schema": 3, "mode": QUERY_MODE if args.protected_queries else PROTECTED_MODE if args.protected_objects else BASE_MODE,
              "scope": "single Linux node process-tree containment and query/refresh custody",
              "passed": False, "gates": {name: "missing" for name in required_gates(args.protected_objects, args.protected_queries)}, "limitations": [
                  "No hostile native escape reproduction was executed.",
                  "Native charged memory is not parent RSS or whole-node memory.",
                  "Stale root identity rejection is tested; arbitrary host reboot recovery is not promised.",
                  "No throughput or multi-host production capacity claim is made."],
              "kernel": os.uname().release, "architecture": os.uname().machine}
    report.update(owned_unit=unit, service_description=description, parent_unit=args.parent_unit, resource_limits=RESOURCE_LIMITS)
    if args.protected_objects:
        report["protected_gates"] = protected_gates("")
        report["limitations"].append("Protected objects use a local TLS fixture; live S3, R2, Azure Blob and GCS certification is separate.")
    if args.protected_queries:
        report["protected_query_gates"] = child_gates("", protected_query.ROOT, protected_query.LEAVES)
        report["limitations"].append("The private broker shares the delegated service cap; revocation is local to the gateway replica and cannot recall delivered bytes.")
    started, stage, process = time.monotonic(), "source", None
    try:
        os.chdir(repo)
        stage = "service-ownership"
        require_fresh_unit(unit)
        if args.parent_unit is not None:
            require_parent_unit(args.parent_unit)
        stage = "source"
        report["source"] = source_manifest(repo)
        env = dict(os.environ, GOMAXPROCS="1")
        version = run([args.go, "version"], env=env, timeout=15)
        if version.returncode:
            raise RuntimeError("Go toolchain unavailable")
        report["go_version"] = version.stdout.strip()
        if args.protected_objects:
            stage = "provision-bridge"
            if args.bridge_archive is not None:
                seed_bridge_archive(args.bridge_archive, artifact)
            env.update(CGO_ENABLED="1", CGO_CXXFLAGS="-I" + str(artifact / "duckbridge/headers"), GOWORK="off", GOFLAGS="")
            result = run_logged([sys.executable, "-B", str(repo / "scripts/provision_duckbridge.py"), str(artifact / "duckbridge"),
                                "--go", args.go, "--source-directory", str(repo)], artifact / (stage + ".log"), env=env, timeout=300)
            if result.returncode:
                raise RuntimeError("pinned bridge provisioning failed")
            report["bridge"] = bridge_provenance(repo, artifact)
        for number, command in enumerate(build_commands(args.go, artifact, args.protected_objects)):
            stage = "build-" + str(number)
            result = run_logged(command, artifact / (stage + ".log"), env=env, timeout=300)
            if result.returncode:
                raise RuntimeError("bounded build failed")
        report["binary_sha256"] = {name: digest(artifact / name) for name in BINARIES}
        if args.protected_queries:
            stage = "cached-broker"
            report["broker_binary"] = protected_query.seed_binary(args.nats_server, artifact, args.nats_sha256)
            report["binary_sha256"]["nats-server"] = digest(artifact / "nats-server")
            (artifact / "broker-binary.json").write_text(json.dumps(report["broker_binary"]) + "\n")
        account = pwd.getpwuid(os.geteuid()).pw_name
        report["service_user"] = account
        require_fresh_unit(unit)
        dependencies = []
        if args.parent_unit is not None:
            require_parent_unit(args.parent_unit)
            dependencies = ["--property=" + key + "=" + args.parent_unit for key in ("Requisite", "BindsTo", "After")]
        command = ["sudo", "-n", "systemd-run", "--unit=" + unit, "--uid=" + account,
                   "--description=" + description, "--property=Delegate=yes",
                   *["--property=" + key + "=" + value for key, value in RESOURCE_LIMITS.items()], "--property=NoNewPrivileges=yes",
                   *dependencies,
                   "--property=CapabilityBoundingSet=", "--property=AmbientCapabilities=", "--collect", "--wait", "--pipe",
                   sys.executable, "-B", str(Path(__file__).resolve()), "--inside", "--artifact", str(artifact),
                   "--unit", unit, "--report", str(args.report)]
        if args.protected_objects:
            command.append("--protected-objects")
        if args.protected_queries:
            command.append("--protected-queries")
        stage = "kernel-service"
        with (artifact / "service.log").open("w") as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            report["gates"][OUTSIDE_GATE] = checked_outside_probe(artifact, unit, process, report,
                description=description, user=account, parent=args.parent_unit, protected=args.protected_objects)
            report["service_exit_code"] = process.wait(timeout=550 if args.protected_queries else 330)
        if (artifact / "inside.json").exists():
            inner = json.loads((artifact / "inside.json").read_text())
            if inner.get("mode") != report["mode"]:
                raise RuntimeError("service acceptance mode mismatch")
            report["gates"].update(inner.pop("gates", {}))
            report.update(inner)
    except (OSError, RuntimeError, subprocess.TimeoutExpired, ValueError) as exc:
        report["failure"] = stage + ":" + type(exc).__name__
    finally:
        report.update(cleanup_owned(unit, description))
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
        if args.protected_objects:
            try:
                report["bridge_inputs_unchanged"] = bridge_provenance(repo, artifact) == report.get("bridge")
            except (OSError, RuntimeError, ValueError):
                report["bridge_inputs_unchanged"] = False
        if args.protected_queries:
            try:
                report["broker_binary_unchanged"] = digest(artifact / "nats-server") == report.get("broker_binary", {}).get("sha256")
            except OSError:
                report["broker_binary_unchanged"] = False
        report["passed"] = reconcile(report)
        report["duration_seconds"] = round(time.monotonic() - started, 3)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "source"}, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
