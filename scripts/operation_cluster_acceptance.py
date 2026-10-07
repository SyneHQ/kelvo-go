#!/usr/bin/env python3
"""Run the real operation lifecycle in a delegated, bounded Linux service.

Uses cached PostgreSQL/MySQL images, private TLS and one owned loopback broker.
All fixture secrets and diagnostic logs remain in a new mode-0700 output root.
The caller supplies pinned binaries and a frozen source manifest; no downloads.
"""
import argparse
import ctypes
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import subprocess
import sys
import time
import threading
import socketserver
import select
import shutil
import stat
from urllib.parse import quote

sys.dont_write_bytecode = True
from nats_export_permissions import tenant_permissions


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def create_source_ca(run, key, certificate):
    # Do not inherit the host openssl.cnf CA profile: its v3_ca section can
    # omit keyUsage, which the native PostgreSQL trust-root validator rejects.
    run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-subj", "/CN=Kelvo operation fixture CA", "-addext", "basicConstraints=critical,CA:TRUE",
         "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-keyout", str(key), "-out", str(certificate)])


def source_ca_profile(run, certificate):
    """Retain only public certificate facts, never PEM, keys or source details."""
    result = run(["openssl", "x509", "-in", str(certificate), "-noout", "-ext", "basicConstraints,keyUsage"])
    extensions = result.stdout.decode("ascii")
    basic = re.search(r"X509v3 Basic Constraints:([^\n]*)\n\s*([^\n]*)", extensions)
    usage = re.search(r"X509v3 Key Usage:([^\n]*)\n\s*([^\n]*)", extensions)
    basic_values = set(value.strip() for value in basic[2].split(",")) if basic else set()
    usage_values = set(value.strip() for value in usage[2].split(",")) if usage else set()
    verified = run(["openssl", "verify", "-CAfile", str(certificate), str(certificate)], check=False)
    profile = {"sha256": digest(certificate), "is_ca": "CA:TRUE" in basic_values,
               "certificate_signing": "Certificate Sign" in usage_values, "crl_signing": "CRL Sign" in usage_values,
               "critical_basic_constraints": bool(basic and basic[1].strip() == "critical"),
               "critical_key_usage": bool(usage and usage[1].strip() == "critical"),
               "currently_valid": verified.returncode == 0}
    profile["accepted"] = profile["is_ca"] and profile["certificate_signing"] and profile["currently_valid"]
    return profile


def unified_cgroup(pid, proc_root=Path("/proc")):
    entries = (proc_root / str(pid) / "cgroup").read_text().splitlines()
    unified = [line[3:] for line in entries if line.startswith("0::/")]
    if len(unified) != 1:
        raise RuntimeError("unified process cgroup unavailable")
    return unified[0]


def process_identity(pid, proc_root=Path("/proc")):
    try:
        fields = (proc_root / str(pid) / "stat").read_text().rsplit(") ", 1)[1].split()
        return {"pid": pid, "start_ticks": int(fields[19]), "cgroup": unified_cgroup(pid, proc_root)}
    except (FileNotFoundError, ProcessLookupError):
        return None


def adopted_zombie(identity, parent, proc_root=Path("/proc")):
    try:
        fields = (proc_root / str(identity["pid"]) / "stat").read_text().rsplit(") ", 1)[1].split()
        return fields[0] == "Z" and int(fields[1]) == parent and int(fields[19]) == identity["start_ticks"]
    except (FileNotFoundError, ProcessLookupError):
        return False


def child_subreaper():
    # Adopt descendants when test2json or a Go test exits without its defers.
    # Otherwise an exited orphan can remain a zombie outside our wait custody.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "cannot establish child reaping custody")


class OwnedWorkloads:
    """Cleanup authority is one initially empty delegated service, never a PID alone."""

    def __init__(self, unit):
        if not re.fullmatch(r"kelvo-operation-live-[a-zA-Z0-9.-]+\.service", unit):
            raise RuntimeError("invalid owned service name")
        self.pid = os.getpid()
        self.relative = "/system.slice/" + unit
        self.group = Path("/sys/fs/cgroup") / self.relative.lstrip("/")
        self.supervisor = self.group / "supervisor"
        self.jobs = self.group / "jobs"
        info = self.group.lstat()
        if not stat.S_ISDIR(info.st_mode) or unified_cgroup(self.pid) != self.relative:
            raise RuntimeError("runner does not own expected delegated service")
        self.identity = (info.st_dev, info.st_ino)
        if set((self.group / "cgroup.procs").read_text().split()) != {str(self.pid)} or self.directories():
            raise RuntimeError("delegated service is not exclusively owned")
        if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
            raise RuntimeError("pidfd cleanup support required")

    def assert_owner(self):
        info = self.group.lstat()
        if not stat.S_ISDIR(info.st_mode) or (info.st_dev, info.st_ino) != self.identity or unified_cgroup(self.pid) not in (self.relative, self.relative + "/supervisor"):
            raise RuntimeError("delegated service identity changed")

    def contains(self, relative):
        return relative == self.relative or relative.startswith(self.relative + "/")

    def prepare(self):
        self.assert_owner()
        child_subreaper()
        self.supervisor.mkdir(mode=0o755)
        (self.supervisor / "cgroup.procs").write_text(str(self.pid))
        (self.group / "cgroup.subtree_control").write_text("+cpu +memory +pids")
        self.jobs.mkdir(mode=0o755)

    def directories(self):
        found, pending = [], [self.group]
        while pending:
            parent = pending.pop()
            try:
                entries = list(parent.iterdir())
            except FileNotFoundError:
                continue
            for path in entries:
                try:
                    mode = path.lstat().st_mode
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(mode):
                    raise RuntimeError("unexpected cgroup symlink")
                if stat.S_ISDIR(mode):
                    found.append(path)
                    pending.append(path)
        return found

    def snapshot(self):
        self.assert_owner()
        pids = set()
        for group in [self.group] + self.directories():
            try:
                pids.update(int(value) for value in (group / "cgroup.procs").read_text().split())
            except FileNotFoundError:
                continue
        # cgroup.procs omits zombies. Include adopted children so cleanup must
        # reap their exit status too, and never leave that task to outer systemd.
        children = set()
        for path in (Path("/proc") / str(self.pid) / "task").glob("*/children"):
            try:
                children.update(int(value) for value in path.read_text().split())
            except FileNotFoundError:
                continue
        pids.update(children)
        result = []
        for pid in sorted(pids - {self.pid}):
            identity = process_identity(pid)
            # Exited tasks may already have left their live cgroup. Reaping an
            # adopted zombie uses exact start time plus parent custody; it never
            # grants permission to signal a live process in another cgroup.
            if identity is not None and (self.contains(identity["cgroup"]) or (pid in children and adopted_zombie(identity, self.pid))):
                result.append(identity)
        return result

    def signal_process(self, identity, sig):
        self.assert_owner()
        pid = identity["pid"]
        if pid == self.pid or not self.contains(identity["cgroup"]):
            raise RuntimeError("refusing foreign process cleanup")
        if process_identity(pid) != identity:
            return False
        try:
            descriptor = os.pidfd_open(pid, 0)
        except ProcessLookupError:
            return False
        try:
            # Pin the kernel process, then recheck start time and membership.
            # A reused PID or task moved outside this unit is never signalled.
            self.assert_owner()
            if process_identity(pid) != identity:
                return False
            signal.pidfd_send_signal(descriptor, sig)
            return True
        except ProcessLookupError:
            return False
        finally:
            os.close(descriptor)

    def reap(self, identity):
        self.assert_owner()
        if identity["pid"] == self.pid or process_identity(identity["pid"]) != identity:
            return False
        if not self.contains(identity["cgroup"]) and not adopted_zombie(identity, self.pid):
            return False
        try:
            return os.waitpid(identity["pid"], os.WNOHANG)[0] == identity["pid"]
        except ChildProcessError:
            return False

    def remove_groups(self):
        self.assert_owner()
        for path in sorted(self.directories(), key=lambda p: len(p.parts), reverse=True):
            if path == self.supervisor:
                continue
            self.assert_owner()
            try:
                path.rmdir()
            except FileNotFoundError:
                pass
            except OSError:
                # Still attempt the other owned directories; the inventory
                # below keeps cleanup false if any group could not be removed.
                continue
        return self.directories() in ([], [self.supervisor])

    def cleanup(self, term_seconds=2, kill_seconds=5):
        evidence = {"initial_processes": [], "initial_job_groups": [], "observed_processes": [],
                    "reaped": [], "signals": [], "remaining_processes": [], "errors": [],
                    "processes_reaped": False, "child_groups_removed": False}
        observed, signalled = {}, set()
        try:
            evidence["initial_processes"] = self.snapshot()
            evidence["initial_job_groups"] = [str(path.relative_to(self.group)) for path in self.directories()
                                               if path not in (self.supervisor, self.jobs)]
            started = time.monotonic()
            deadline = started + term_seconds + kill_seconds
            while True:
                current = self.snapshot()
                for identity in current:
                    key = (identity["pid"], identity["start_ticks"])
                    observed[key] = identity
                    try:
                        if self.reap(identity):
                            evidence["reaped"].append(identity)
                            continue
                        sig = signal.SIGTERM if time.monotonic() < started + term_seconds else signal.SIGKILL
                        if (key, sig) not in signalled and self.signal_process(identity, sig):
                            signalled.add((key, sig))
                            evidence["signals"].append({**identity, "signal": int(sig)})
                    except OSError as error:
                        category = type(error).__name__
                        if category not in evidence["errors"]:
                            evidence["errors"].append(category)
                if not current or time.monotonic() >= deadline:
                    break
                time.sleep(.05)
            evidence["remaining_processes"] = self.snapshot()
            if not evidence["remaining_processes"]:
                evidence["child_groups_removed"] = self.remove_groups()
                evidence["remaining_processes"] = self.snapshot()
                evidence["processes_reaped"] = not evidence["remaining_processes"]
        except BaseException as error:
            evidence["errors"].append(type(error).__name__)
        evidence["observed_processes"] = list(observed.values())
        return evidence


def record_owned_cleanup(report, evidence):
    report["owned_workload_cleanup"] = evidence
    report["cleanup"]["owned_processes_reaped"] = evidence["processes_reaped"]
    report["cleanup"]["owned_child_groups_removed"] = evidence["child_groups_removed"]
    if evidence["initial_processes"] or evidence["initial_job_groups"] or evidence["observed_processes"]:
        report["failures"].append("owned-workloads-required-cleanup")
        report["passed"] = False
    if evidence["errors"] or not evidence["processes_reaped"] or not evidence["child_groups_removed"]:
        report["passed"] = False


def verify_application_provenance(artifacts, report, source, source_manifest, published_version=None, published_sum=None):
    """Bind standalone executables to unchanged SDK and example build inputs."""
    manifests = {}
    for label in ("checkout", "sdk", "application"):
        evidence = report.get("provenance", {}).get(label, {})
        before_name, after_name = "source-" + label + "-before.json", "source-" + label + "-after.json"
        if evidence.get("unchanged_after_execution") is not True or evidence.get("manifest_file") != before_name or evidence.get("after_manifest_file") != after_name:
            raise RuntimeError("application source provenance incomplete")
        loaded = []
        for name, key in ((before_name, "manifest_sha256"), (after_name, "after_manifest_sha256")):
            path = artifacts / name
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 32 << 20:
                raise RuntimeError("application source manifest unavailable")
            entries = json.loads(path.read_text())
            if not isinstance(entries, dict) or len(entries) > 100000:
                raise RuntimeError("application source manifest invalid")
            actual = hashlib.sha256(json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            if actual != evidence.get(key):
                raise RuntimeError("application source manifest hash changed")
            loaded.append(entries)
        if loaded[0] != loaded[1]:
            raise RuntimeError("application build inputs changed during execution")
        manifests[label] = loaded[0]
    template = report.get("provenance", {}).get("template_copy", {})
    if template.get("matches") is not True or template.get("template_sha256") != template.get("copied_sha256"):
        raise RuntimeError("application template copy was not verified")
    module = "github.com/SYNEHQ/kelvo-go"
    packages = report.get("public_packages", [])
    required = {module + "/" + name for name in ("client", "query", "delegation", "resolver", "operations")}
    if not isinstance(packages, list) or not required.issubset(packages) or any(not isinstance(p, str) or not p.startswith(module + "/") or ".." in p or "/internal/" in p for p in packages):
        raise RuntimeError("application public dependency provenance invalid")
    directories = {p[len(module) + 1:] for p in packages}
    current = {name: {"sha256": value, "bytes": (source / name).stat().st_size} for name, value in source_manifest["files"].items()}
    def selected(entries, choose):
        return {name: {"sha256": value["sha256"], "bytes": value["bytes"]} for name, value in entries.items() if choose(name)}
    def sdk_file(name):
        return name in ("go.mod", "go.sum") or (str(Path(name).parent) in directories and not name.endswith(".md"))
    if selected(current, sdk_file) != selected(manifests["sdk"], sdk_file):
        raise RuntimeError("application SDK differs from current frozen public sources")
    prefix = "examples/application/"
    expected_example = {name[len(prefix):]: value for name, value in current.items() if name.startswith(prefix) and not name.endswith(".md")}
    original_example = {name[len(prefix):]: {"sha256": value["sha256"], "bytes": value["bytes"]} for name, value in manifests["checkout"].items() if name.startswith(prefix) and not name.endswith(".md")}
    if not expected_example or original_example != expected_example:
        raise RuntimeError("application example differs from current frozen source")
    # The copied module is deliberately pinned/tidied before compilation.
    application_file = lambda name: name not in ("go.mod", "go.sum") and not name.endswith(".md")
    if selected(expected_example, application_file) != selected(manifests["application"], application_file):
        raise RuntimeError("compiled application differs from current example")
    mode = report.get("mode")
    if mode == "published":
        resolved = report.get("resolved_module", {})
        if not published_version or not published_sum or not re.fullmatch(r"h1:[A-Za-z0-9+/]{43}=", published_sum) or report.get("version") != published_version or resolved.get("Path") != module or resolved.get("Version") != published_version or resolved.get("Sum") != published_sum:
            raise RuntimeError("published SDK requires authoritative version and module sum")
    elif mode != "candidate" or published_version or published_sum:
        raise RuntimeError("application provenance mode mismatch")
    return {"source_unchanged": True, "mode": mode, "sdk_manifest_sha256": report["provenance"]["sdk"]["manifest_sha256"],
            "application_manifest_sha256": report["provenance"]["application"]["manifest_sha256"], "public_files": len(selected(current, sdk_file))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--unit", required=True)
    parser.add_argument("--go", type=Path, required=True)
    for name in ("binary", "adapter", "launcher", "nats"):
        parser.add_argument("--" + name, type=Path, required=True)
        parser.add_argument("--" + name + "-sha256", required=True)
    parser.add_argument("--application-artifacts", type=Path, help="passed public_sdk_acceptance.py output containing pinned authority/exercise binaries")
    parser.add_argument("--application-module-version", help="authoritative published SDK version, required in published mode")
    parser.add_argument("--application-module-sum", help="authoritative published SDK h1 module sum, required in published mode")
    parser.add_argument("--api-source", type=Path)
    parser.add_argument("--api-manifest", type=Path)
    parser.add_argument("--source-ca-cert", type=Path)
    parser.add_argument("--source-ca-key", type=Path)
    args = parser.parse_args()
    if bool(args.api_source) != bool(args.api_manifest) or bool(args.source_ca_cert) != bool(args.source_ca_key):
        parser.error("paired API source manifest and fixture CA paths required")
    if args.application_artifacts and args.api_source:
        parser.error("application artifacts and API source are separate acceptance modes")
    if bool(args.application_module_version) != bool(args.application_module_sum) or ((args.application_module_version or args.application_module_sum) and not args.application_artifacts):
        parser.error("published application version and module sum must be supplied together")
    root = Path(__file__).resolve().parents[1]
    if sys.platform != "linux" or not re.fullmatch(r"kelvo-operation-live-[a-zA-Z0-9.-]+\.service", args.unit):
        parser.error("explicit owned Linux fixture service required")
    if not args.output.is_absolute() or args.output.exists() or args.output.is_symlink() or args.output.is_relative_to(root):
        parser.error("output must be a fresh absolute directory outside frozen source")
    os.umask(0o077)
    out = args.output
    out.mkdir(mode=0o700)
    manifest = json.loads(args.source_manifest.read_text())
    report = {"scope": "real Node, TLS PostgreSQL/MySQL, tenant-scoped NATS, on-demand resolver, retained Arrow",
              "source_digest": manifest["source_digest"], "passed": False, "stages": [], "skips": [], "failures": [], "cleanup": {}}
    docker = ["sudo", "-n", "docker"]
    nonce = secrets.token_hex(8)
    network = "kelvo-operation-" + nonce
    names = {kind: network + "-" + kind for kind in (("postgres", "mysql", "metadata") if args.api_source else ("postgres", "mysql"))}
    created = {}
    network_id = None
    broker = None
    relay = None
    relay_thread = None
    relay_sockets = set()
    relay_lock = threading.Lock()
    owned = None
    state = None
    passwords = {key: secrets.token_hex(32) for key in ("admin", "postgres", "mysql")}
    log_number = 0
    stage = "source"

    def write(path, value):
        path.write_text(value)
        path.chmod(0o600)

    def run(argv, data=None, timeout=30, check=True):
        nonlocal log_number
        result = subprocess.run(argv, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        if result.returncode:
            log_number += 1
            (out / ("private-command-%03d.log" % log_number)).write_bytes(result.stdout + b"\n" + result.stderr)
        if check and result.returncode:
            raise RuntimeError("owned fixture command failed")
        return result

    def verify():
        files = manifest["files"]
        if {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()} != set(files):
            raise RuntimeError("source inventory changed")
        for name, expected in files.items():
            path = root / name
            if path.is_symlink() or digest(path) != expected:
                raise RuntimeError("source hash changed")

    def admin(kind, statement, check=True):
        argv = (["psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "kelvo_fixture", "-At"] if kind in ("postgres", "metadata") else
                ["mysql", "--defaults-extra-file=/tmp/kelvo-operation-admin.cnf", "--batch", "--silent", "--skip-column-names"])
        return run(docker + ["exec", "-i", names[kind]] + argv, statement.encode(), check=check)

    def stopped(_signal, _frame):
        raise RuntimeError("fixture deadline or cancellation")

    signal.signal(signal.SIGTERM, stopped)
    signal.signal(signal.SIGINT, stopped)
    try:
        verify()
        binaries = {}
        for name in ("binary", "adapter", "launcher", "nats"):
            path, expected = getattr(args, name), getattr(args, name + "_sha256")
            if not path.is_absolute() or path.is_symlink() or not path.is_file() or not re.fullmatch(r"[0-9a-f]{64}", expected) or digest(path) != expected:
                raise RuntimeError("pinned binary mismatch")
            binaries[name] = expected
        report["binaries"] = binaries
        stage = "containment"
        owned = OwnedWorkloads(args.unit)
        owned.prepare()
        jobs = owned.jobs
        state = out / "containment"
        state.mkdir(mode=0o700)
        application_report = None
        application_binding = None
        def verify_application():
            nonlocal application_binding
            if not args.application_artifacts:
                return
            artifacts = args.application_artifacts
            if not artifacts.is_absolute() or artifacts.is_symlink() or not artifacts.is_dir():
                raise RuntimeError("application artifact directory is invalid")
            current_report = json.loads((artifacts / "report.json").read_text())
            if current_report.get("passed") is not True or current_report.get("forbidden_imports") or current_report.get("cgo_packages"):
                raise RuntimeError("standalone application boundary did not pass")
            if application_report is not None and current_report != application_report:
                raise RuntimeError("application report changed during acceptance")
            application_binding = verify_application_provenance(artifacts, current_report, root, manifest, args.application_module_version, args.application_module_sum)
            for name in ("authority", "exercise"):
                path = artifacts / name
                pin = current_report.get("binaries", {}).get(name, {})
                if path.is_symlink() or not path.is_file() or not os.access(path, os.X_OK) or not re.fullmatch(r"[0-9a-f]{64}", pin.get("sha256", "")) or digest(path) != pin["sha256"] or path.stat().st_size != pin.get("bytes"):
                    raise RuntimeError("standalone application binary pin mismatch")
            return current_report
        if args.application_artifacts:
            application_report = verify_application()
            report["application"] = {"mode": application_report.get("mode"), "version": application_report.get("version"), "report_sha256": digest(args.application_artifacts / "report.json"), "binaries": application_report["binaries"]}
            report["application"]["source_binding"] = application_binding
            report["scope"] = "standalone public application authority and SDK, real Node/Gateway, TLS PostgreSQL, tenant-scoped NATS, live authorization revocation and fresh credentials"

        stage = "build-test"
        (out / "tmp").mkdir(mode=0o700)
        build_env = {key: value for key, value in os.environ.items() if key in ("PATH", "HOME", "LANG", "GOCACHE", "GOPATH", "GOMODCACHE")}
        build_env.update(GOMAXPROCS="2", GOMEMLIMIT="2GiB", CGO_ENABLED="1", GOTOOLCHAIN="local", GOENV="off", GOWORK="off", GOPROXY="off", GOSUMDB="off", GOVCS="*:off", GOFLAGS="", GOTMPDIR=str(out / "tmp"), TMPDIR=str(out / "tmp"))
        with (out / "test-build.log").open("wb") as log:
            command = [str(args.go), "test", "-c", "-mod=readonly", "-p=1", "-tags", "duckdb_arrow", "-o", str(out / "cluster.test"), "./internal/cluster"]
            code = subprocess.run(command, cwd=root, env=build_env, stdout=log, stderr=subprocess.STDOUT, timeout=600).returncode
        report["stages"].append({"name": stage, "exit": code, "argv": command})
        if code:
            raise RuntimeError("live fixture test did not compile")
        report["test_binary_sha256"] = digest(out / "cluster.test")
        if args.api_source:
            api_manifest = json.loads(args.api_manifest.read_text())
            api_root = args.api_source.resolve()
            def verify_api():
                if {str(p.relative_to(api_root)) for p in api_root.rglob("*") if p.is_file()} != set(api_manifest["files"]):
                    raise RuntimeError("API source inventory changed")
                for name, expected in api_manifest["files"].items():
                    if (api_root / name).is_symlink() or digest(api_root / name) != expected:
                        raise RuntimeError("API source hash changed")
            verify_api()
            report["api_source_digest"] = api_manifest["source_digest"]
            mod = out / "api.mod"
            shutil.copyfile(api_root / "go.mod", mod)
            shutil.copyfile(api_root / "go.sum", mod.with_suffix(".sum"))
            run([str(args.go), "mod", "edit", "-modfile=" + str(mod), "-replace=github.com/SYNEHQ/kelvo-go=" + str(root)])
            command = [str(args.go), "test", "-c", "-mod=mod", "-modfile=" + str(mod), "-p=1", "-o", str(out / "api.test"), "./api/handlers"]
            with (out / "api-build.log").open("wb") as log:
                code = subprocess.run(command, cwd=api_root, env={**build_env, "CGO_ENABLED": "0"}, stdout=log, stderr=subprocess.STDOUT, timeout=600).returncode
            report["stages"].append({"name": "api-test-build", "exit": code})
            if code:
                raise RuntimeError("API cross-service fixture did not compile")
            verify_api()
            report["api_test_binary_sha256"] = digest(out / "api.test")
        stage = "images"
        images = {}
        for kind, image in (("postgres", "postgres:17.6"), ("mysql", "mysql:8.4")):
            images[kind] = json.loads(run(docker + ["image", "inspect", image]).stdout)[0]["Id"]
        if args.api_source:
            images["metadata"] = images["postgres"]
        report["images"] = images
        stage = "network"
        network_id = run(docker + ["network", "create", "--internal", "--label", "kelvo.operation.fixture=" + nonce, network]).stdout.decode().strip()
        details = json.loads(run(docker + ["network", "inspect", network_id]).stdout)[0]
        subnet = ipaddress.ip_network(details["IPAM"]["Config"][0]["Subnet"])
        addresses = {"postgres": str(subnet.network_address + 10), "mysql": str(subnet.network_address + 11), "metadata": str(subnet.network_address + 12)}
        stage = "certificates"
        certs = out / "certs"
        certs.mkdir(mode=0o755)
        certs.chmod(0o755)
        if args.source_ca_cert:
            shutil.copyfile(args.source_ca_cert, certs / "ca.crt")
            shutil.copyfile(args.source_ca_key, out / "ca.key")
        else:
            create_source_ca(run, out / "ca.key", certs / "ca.crt")
        report["source_ca_profile"] = source_ca_profile(run, certs / "ca.crt")
        if not report["source_ca_profile"]["accepted"]:
            raise RuntimeError("fixture CA requires current CA constraints and certificate-signing key usage")
        for kind in ("postgres", "mysql", "broker"):
            leaf = certs / kind
            leaf.mkdir(mode=0o755)
            leaf.chmod(0o755)
            (leaf / "ca.crt").write_bytes((certs / "ca.crt").read_bytes())
            (leaf / "ca.crt").chmod(0o644)
            key, csr, cert = leaf / (kind + ".key"), out / (kind + ".csr"), leaf / (kind + ".crt")
            ext = out / (kind + ".ext")
            write(ext, "subjectAltName=IP:" + addresses.get(kind, "127.0.0.1") + "\nextendedKeyUsage=serverAuth\n")
            run(["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=Kelvo fixture " + kind, "-keyout", str(key), "-out", str(csr)])
            run(["openssl", "x509", "-req", "-days", "1", "-in", str(csr), "-CA", str(certs / "ca.crt"), "-CAkey", str(out / "ca.key"), "-CAcreateserial", "-extfile", str(ext), "-out", str(cert)])
            key.chmod(0o600)
            if kind != "broker":
                run(["sudo", "-n", "chown", "999:999", str(key)])
            cert.chmod(0o644)
            csr.unlink()
            ext.unlink()
        (certs / "ca.crt").chmod(0o644)
        (out / "ca.key").unlink()
        stage = "databases"
        env_file = out / "database.env"
        write(env_file, "POSTGRES_PASSWORD=" + passwords["admin"] + "\nPOSTGRES_DB=kelvo_fixture\nMYSQL_ROOT_PASSWORD=" + passwords["admin"] + "\nMYSQL_DATABASE=kelvo_fixture\n")
        common = ["--network", network_id, "--memory", "1g", "--memory-swap", "1g", "--cpus", "1", "--pids-limit", "256", "--security-opt", "no-new-privileges:true", "--label", "kelvo.operation.fixture=" + nonce, "--env-file", str(env_file)]
        options = {"postgres": ["-c", "ssl=on", "-c", "ssl_cert_file=/certs/postgres.crt", "-c", "ssl_key_file=/certs/postgres.key", "-c", "ssl_ca_file=/certs/ca.crt", "-c", "shared_buffers=128MB"],
                   "mysql": ["--require-secure-transport=ON", "--ssl-ca=/certs/ca.crt", "--ssl-cert=/certs/mysql.crt", "--ssl-key=/certs/mysql.key", "--innodb-buffer-pool-size=128M", "--log-bin-trust-function-creators=ON"]}
        options["metadata"] = ["-c", "shared_buffers=64MB", "-c", "max_connections=32"]
        for kind in names:
            created[kind] = None  # Retain creation intent even if Docker loses its reply.
            created[kind] = run(docker + ["create", "--pull=never", "--name", names[kind], "--ip", addresses[kind]] + common + (["--mount", "type=bind,src=" + str(certs / kind) + ",dst=/certs,readonly"] if kind != "metadata" else []) + [images[kind]] + options[kind]).stdout.decode().strip()
            run(docker + ["start", created[kind]])
        admin_file = out / "mysql-admin.cnf"
        write(admin_file, "[client]\nuser=root\npassword=" + passwords["admin"] + "\n")
        run(docker + ["cp", str(admin_file), names["mysql"] + ":/tmp/kelvo-operation-admin.cnf"])
        deadline = time.monotonic() + 150
        while True:
            ready = all(admin(kind, "SELECT 1", check=False).returncode == 0 for kind in names)
            if ready:
                try:
                    for kind, port in (("postgres", 5432), ("mysql", 3306)):
                        with socket.create_connection((addresses[kind], port), timeout=.3):
                            pass
                    break
                except OSError:
                    pass
            if time.monotonic() >= deadline:
                raise RuntimeError("owned databases did not become ready")
            time.sleep(.5)
        table = "CREATE TABLE kelvo_operation_fixture (id INTEGER PRIMARY KEY, marker BIGINT NOT NULL); INSERT INTO kelvo_operation_fixture VALUES (1,0);"
        if args.application_artifacts:
            admin("postgres", "CREATE TABLE public.team_a_rows(id TEXT PRIMARY KEY,value BIGINT NOT NULL); CREATE TABLE public.team_b_rows(id TEXT PRIMARY KEY,value BIGINT NOT NULL); CREATE ROLE kelvo_operator LOGIN PASSWORD '" + passwords["postgres"] + "'; GRANT USAGE ON SCHEMA public TO kelvo_operator; GRANT SELECT,INSERT ON public.team_a_rows,public.team_b_rows TO kelvo_operator;")
        else:
            admin("postgres", table + "CREATE ROLE kelvo_operator LOGIN PASSWORD '" + passwords["postgres"] + "'; GRANT USAGE ON SCHEMA public TO kelvo_operator; GRANT SELECT,INSERT,UPDATE,DELETE ON kelvo_operation_fixture TO kelvo_operator;")
            admin("postgres", "CREATE SCHEMA kelvo_ingestion_fixture AUTHORIZATION kelvo_operator; GRANT CREATE ON DATABASE kelvo_fixture TO kelvo_operator; SET ROLE kelvo_operator; CREATE TABLE kelvo_ingestion_fixture.watch_source (id BIGINT PRIMARY KEY, amount NUMERIC(30,3));")
            admin("postgres", "CREATE SCHEMA kelvo_migration_fixture AUTHORIZATION kelvo_operator;")
            admin("postgres", "CREATE SCHEMA kelvo_migration_status_fixture AUTHORIZATION kelvo_operator; SET ROLE kelvo_operator; CREATE TABLE kelvo_migration_status_fixture.status_write_probe(id bigint); CREATE FUNCTION kelvo_migration_status_fixture.status_write() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN INSERT INTO kelvo_migration_status_fixture.status_write_probe VALUES(1); RETURN 1; END; $$; CREATE VIEW kelvo_migration_status_fixture.schema_migrations AS SELECT kelvo_migration_status_fixture.status_write() AS version,false AS dirty;")
        admin("mysql", "USE kelvo_fixture; " + table + "CREATE TABLE watch_source (id BIGINT PRIMARY KEY,amount DECIMAL(30,3)) ENGINE=InnoDB; CREATE USER 'kelvo_operator'@'%' IDENTIFIED BY '" + passwords["mysql"] + "' REQUIRE SSL; GRANT SELECT,INSERT,UPDATE,DELETE,CREATE,DROP,ALTER,TRIGGER ON kelvo_fixture.* TO 'kelvo_operator'@'%';")
        if args.api_source:
            admin("metadata", "CREATE DATABASE kelvo_metadata")
            permits = threading.BoundedSemaphore(16)
            class RelayHandler(socketserver.BaseRequestHandler):
                def handle(self):
                    if not permits.acquire(blocking=False):
                        return
                    upstream = None
                    try:
                        upstream = socket.create_connection((addresses["metadata"], 5432), timeout=2)
                        pair = (self.request, upstream)
                        with relay_lock:
                            relay_sockets.update(pair)
                        while True:
                            readable, _, _ = select.select(pair, [], [], 1)
                            for src in readable:
                                data = src.recv(65536)
                                if not data:
                                    return
                                (upstream if src is self.request else self.request).sendall(data)
                    except (OSError, ValueError):
                        pass
                    finally:
                        with relay_lock:
                            relay_sockets.discard(self.request)
                            if upstream is not None:
                                relay_sockets.discard(upstream)
                        if upstream is not None:
                            upstream.close()
                        permits.release()
            class Relay(socketserver.ThreadingTCPServer):
                daemon_threads = True
            relay = Relay(("127.0.0.1", 0), RelayHandler)
            relay_thread = threading.Thread(target=lambda: relay.serve_forever(poll_interval=.2), daemon=True)
            relay_thread.start()
            sources = []
            for kind, port in (("postgres", 5432), ("mysql", 3306)):
                sources.append({"id": kind + "-saved", "engine": kind, "host": addresses[kind], "port": port, "database": "kelvo_fixture", "schema": "public" if kind == "postgres" else "", "username": "kelvo_operator", "password": passwords[kind],
                                "query_sql": "SELECT 9007199254740993::bigint AS value" if kind == "postgres" else "SELECT CAST(9007199254740993 AS SIGNED) AS value", "write_sql": "UPDATE kelvo_operation_fixture SET marker=marker+7 WHERE id=1", "verify_sql": "SELECT marker AS value FROM kelvo_operation_fixture WHERE id=1", "expected_value": "9007199254740993", "write_expected_value": "7", "metadata_table": "kelvo_operation_fixture"})
            private = {"metadata_dsn": "host=127.0.0.1 port=" + str(relay.server_address[1]) + " user=postgres dbname=kelvo_metadata password=" + passwords["admin"] + " sslmode=disable connect_timeout=5", "sources": sources,
                       "source_endpoints": [addresses[kind] + ":" + str(port) for kind, port in (("postgres", 5432), ("mysql", 3306))]}
            write(out / "api-source-config.json", json.dumps(private))
        stage = "broker"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        env = {"KELVO_TEST_OPERATION_NATS_URL": "tls://127.0.0.1:" + str(port), "KELVO_TEST_OPERATION_NATS_CA_FILE": str(certs / "ca.crt")}
        users = []
        for role in ("initializer", "gateway", "worker"):
            permissions = tenant_permissions(role, False)
            allowed = permissions["publish"]["allow"]
            stream = "KV_KELVO_OPERATIONS_GATEWAY"
            allowed += ["$JS.API.STREAM.INFO." + stream, "$JS.API.STREAM.MSG.GET." + stream]
            if role == "initializer":
                allowed += ["$JS.API.STREAM." + action + "." + stream for action in ("CREATE", "UPDATE", "DELETE")]
            else:
                allowed += ["$KV.KELVO_OPERATIONS_GATEWAY.operation.gateway.*"]
            password = secrets.token_hex(32)
            users.append({"user": role, "password": password, "permissions": permissions})
            prefix = "KELVO_TEST_OPERATION_NATS" + ("" if role == "initializer" else "_" + role.upper())
            env[prefix + "_USER"], env[prefix + "_PASSWORD"] = role, password
        broker_config = {"host": "127.0.0.1", "port": port, "max_payload": 1 << 20, "max_connections": 32, "max_subscriptions": 256, "max_pending": 4 << 20,
                         "accounts": {"a": {"jetstream": {"max_memory": 16 << 20, "max_file": 96 << 20, "max_streams": 4, "max_consumers": 1}, "users": users}},
                         "jetstream": {"store_dir": str(out / "broker-state"), "max_mem_store": 32 << 20, "max_file_store": 128 << 20},
                         "tls": {"cert_file": str(certs / "broker/broker.crt"), "key_file": str(certs / "broker/broker.key"), "min_version": "1.3", "timeout": 2}}
        write(out / "broker.conf", json.dumps(broker_config))
        broker_log = (out / "broker.log").open("wb")
        broker = subprocess.Popen([str(args.nats), "-c", str(out / "broker.conf")], stdout=broker_log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 10
        while True:
            if broker.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError("owned broker did not start")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=.2):
                    break
            except OSError:
                time.sleep(.1)
        env.update({"KELVO_TEST_OPERATION_BINARY": str(args.binary), "KELVO_TEST_OPERATION_ADAPTER_BINARY": str(args.adapter), "KELVO_TEST_OPERATION_ADAPTER_SHA256": args.adapter_sha256,
                    "KELVO_TEST_OPERATION_SANDBOX": str(args.launcher), "KELVO_TEST_OPERATION_DATABASE": "kelvo_fixture", "KELVO_TEST_OPERATION_SOURCE_CA_FILE": str(certs / "ca.crt"),
                    "KELVO_TEST_OPERATION_POSTGRES_DSN": "postgres://kelvo_operator:" + quote(passwords["postgres"]) + "@" + addresses["postgres"] + ":5432/kelvo_fixture?sslmode=verify-full&connect_timeout=5",
                    "KELVO_TEST_OPERATION_MYSQL_DSN": "kelvo_operator:" + passwords["mysql"] + "@tcp(" + addresses["mysql"] + ":3306)/kelvo_fixture?tls=true&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27&timeout=5s",
                    "KELVO_TEST_CGROUP_ROOT": str(jobs), "KELVO_TEST_CGROUP_STATE": str(state)})
        if args.api_source:
            env.update({"KELVO_TEST_OPERATION_API_BINARY": str(out / "api.test"), "KELVO_TEST_OPERATION_API_WORKDIR": str(args.api_source / "api/handlers"), "KELVO_TEST_OPERATION_API_SOURCE_CONFIG": str(out / "api-source-config.json"), "KELVO_TEST_OPERATION_API_LOG": str(out / "api-test.log")})
        if args.application_artifacts:
            app_logs = out / "application"
            app_logs.mkdir(mode=0o700)
            env.update({"KELVO_TEST_OPERATION_APPLICATION_ARTIFACTS": str(args.application_artifacts), "KELVO_TEST_OPERATION_APPLICATION_LOG_DIRECTORY": str(app_logs)})
        write(out / "environment.json", json.dumps(env))
        stage = "live-test"
        environment = {key: value for key, value in os.environ.items() if key in ("PATH", "HOME", "LANG", "GOCACHE", "GOPATH", "GOMODCACHE")}
        environment.update(env)
        environment.update(GOMAXPROCS="2", GOMEMLIMIT="2GiB", CGO_ENABLED="1", GOTOOLCHAIN="local", GOENV="off", GOWORK="off", GOPROXY="off", GOSUMDB="off", GOVCS="*:off", GOFLAGS="", GOTMPDIR=str(out / "tmp"), TMPDIR=str(out / "tmp"))
        command = [str(args.go), "tool", "test2json", "-p", "github.com/SYNEHQ/kelvo-go/internal/cluster", str(out / "cluster.test"), "-test.v", "-test.timeout=" + ("6m" if args.application_artifacts else "4m"), "-test.run=^TestOperationClusterActualNodeLifecycle$"]
        started = time.monotonic()
        with (out / "test.log").open("wb") as log:
            code = subprocess.run(command, cwd=root, env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=600).returncode
        report["stages"].append({"name": stage, "exit": code, "seconds": time.monotonic() - started, "argv": command})
        events = []
        for line in (out / "test.log").read_text().splitlines():
            if line.startswith("{"):
                events.append(json.loads(line))
        report["skips"] = [event.get("Test", "package") for event in events if event.get("Action") == "skip"]
        report["failures"] = [event.get("Test", "package") for event in events if event.get("Action") == "fail"]
        report["tests"] = [event["Test"] for event in events if event.get("Action") == "pass" and "Test" in event]
        report["remaining_job_groups"] = sum(p.is_dir() for p in jobs.iterdir())
        report["remaining_ownership_records"] = sum(p.name != ".kelvo-containment.lock" for p in state.iterdir())
        verify()
        report["source_unchanged"] = True
        required_tests = {"TestOperationClusterActualNodeLifecycle/" + name for name in
                          ("postgres", "mysql", "postgres-ingestion", "postgres-watch", "mysql-watch", "postgres-migrations", "mysql-migrations", "retained-result-survives-worker-restart")}
        if args.api_source:
            required_tests = {"TestOperationClusterActualNodeLifecycle/api-on-demand-cross-service"}
            verify_api()
            report["api_source_unchanged"] = True
            report["api_network_policy"] = "IPAddressDeny=any; IPAddressAllow=localhost; source TCP probes must fail"
        if args.application_artifacts:
            required_tests = {"TestOperationClusterActualNodeLifecycle/standalone-application/" + name for name in ("team-a", "team-b", "revoked-membership", "membership-restored", "fresh-credentials-required", "credentials-restored", "cross-team-forged-caller", "cross-team-restored")}
            verify_application()
            report["application_artifacts_unchanged"] = True
            application_evidence = out / "application" / "report.json"
            if not application_evidence.is_file():
                raise RuntimeError("standalone application acceptance evidence missing")
            app_evidence = json.loads(application_evidence.read_text())
            report["application_evidence"] = {"sha256": digest(application_evidence), "passed": app_evidence.get("passed"), "authority_reaped": app_evidence.get("authority_reaped")}
            if app_evidence.get("passed") is not True or app_evidence.get("authority_reaped") is not True:
                raise RuntimeError("standalone application lifecycle did not pass")
        report["required_tests"] = sorted(required_tests)
        report["passed"] = code == 0 and required_tests.issubset(report["tests"]) and not report["skips"] and not report["failures"] and report["remaining_job_groups"] == 0 and report["remaining_ownership_records"] == 0
    except BaseException as error:
        report["failure"] = stage + ":" + type(error).__name__
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        if relay is not None:
            try:
                relay.shutdown()
                relay.server_close()
                relay_thread.join(2)
                with relay_lock:
                    for sock in list(relay_sockets):
                        try:
                            sock.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        sock.close()
                report["cleanup"]["metadata_relay"] = not relay_thread.is_alive()
            except BaseException:
                report["cleanup"]["metadata_relay"] = False
        if broker is not None:
            try:
                if broker.poll() is None:
                    broker.terminate()
                    try:
                        broker.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        broker.kill()
                        broker.wait(timeout=5)
                        report["passed"] = False
                report["cleanup"]["broker_reaped"] = broker.poll() is not None
            except BaseException:
                report["cleanup"]["broker_reaped"] = False
        for kind, identity in created.items():
            try:
                inspected = run(docker + ["inspect", identity or names[kind]], check=False)
                if inspected.returncode and b"No such object" in inspected.stderr:
                    report["cleanup"][kind] = True
                    continue
                current = json.loads(inspected.stdout)[0]
                if (identity is not None and current["Id"] != identity) or current["Config"]["Labels"].get("kelvo.operation.fixture") != nonce:
                    raise RuntimeError("owned container identity changed")
                identity = current["Id"]
                run(docker + ["rm", "-f", "-v", identity])
                report["cleanup"][kind] = run(docker + ["inspect", identity], check=False).returncode != 0
            except BaseException:
                report["cleanup"][kind] = False
        if network_id:
            try:
                current = json.loads(run(docker + ["network", "inspect", network_id]).stdout)[0]
                if current["Id"] != network_id or current["Labels"].get("kelvo.operation.fixture") != nonce:
                    raise RuntimeError("owned network identity changed")
                run(docker + ["network", "rm", network_id])
                report["cleanup"]["network"] = run(docker + ["network", "inspect", network_id], check=False).returncode != 0
            except BaseException:
                report["cleanup"]["network"] = False
        report["cleanup"]["owned_scope_verified"] = owned is not None
        if owned is not None:
            # This also runs after subprocess.TimeoutExpired, Go panic, failed
            # broker shutdown or failed Docker cleanup. Outer systemd remains
            # the final bound, but is never counted as successful local cleanup.
            evidence = owned.cleanup()
            record_owned_cleanup(report, evidence)
            report["remaining_job_groups"] = max(report.get("remaining_job_groups", 0), len(evidence["initial_job_groups"]))
        if state is not None:
            try:
                remaining = sum(p.name != ".kelvo-containment.lock" for p in state.iterdir())
                report["cleanup"]["ownership_inventory_verified"] = True
                report["remaining_ownership_records"] = max(report.get("remaining_ownership_records", 0), remaining)
                if remaining:
                    report["failures"].append("retained-containment-ownership-records")
                    report["passed"] = False
            except BaseException:
                report["cleanup"]["ownership_inventory_verified"] = False
        if not all(report["cleanup"].values()):
            report["passed"] = False
        if all(report["cleanup"].values()):
            for name in ("database.env", "mysql-admin.cnf", "environment.json", "api-source-config.json", "broker.conf", "certs/postgres/postgres.key", "certs/mysql/mysql.key", "certs/broker/broker.key"):
                (out / name).unlink(missing_ok=True)
        report["evidence_sha256"] = {p.name: digest(p) for p in out.glob("*.log")}
        write(out / "report.json", json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "report": str(out / "report.json"), "failure": report.get("failure"), "cleanup": report["cleanup"]}), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
