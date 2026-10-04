#!/usr/bin/env python3
"""Offline, loopback TLS broker fixture for export namespace ACL acceptance.

Run inside a bounded disposable Linux service. Requires an existing NATS binary,
its exact SHA-256 and an explicitly supported server version; never downloads.
Private credentials, TLS keys and broker state stay under the fresh fixture root.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import secrets
import signal
import socket
import subprocess
import time

from nats_export_permissions import tenant_permissions
from export_diagnostics import ExportDiagnostics

SUPPORTED_SERVERS = ("2.14.7", "2.15.0")
ACCEPTANCE_TESTS = {
    "acl": "TestNATSExportRuntimeACLAndAccountIsolation",
    "lifecycle": "TestExportClusterActualWorkerLifecycle",
    "dispatch": "TestNATSExportDispatchRenewalConflictAndDelayedRedelivery",
}
FIXTURE_PREFIXES = {"acl": "KELVO_TEST_EXPORT_ACL", "lifecycle": "KELVO_TEST_EXPORT_E2E_NATS",
                    "dispatch": "KELVO_TEST_EXPORT_DISPATCH_NATS"}
SCOPES = {
    "acl": "single broker; exact runtime and initializer ACLs in two separate tenant accounts",
    "lifecycle": "single broker; actual sandboxed worker and gateway lifecycle using separate restricted broker roles",
    "dispatch": "single broker; queued renewal conflict, deduplicated republish and delayed redelivery; synthetic Arrow executor; separate restricted roles",
}


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        while block := source.read(1 << 20):
            digest.update(block)
    return digest.hexdigest()


class ExportBrokerFixture:
    def __init__(self, directory, binary, binary_sha256, version, mode="acl"):
        self.directory = Path(directory).absolute()
        self.binary = Path(binary).absolute()
        self.binary_sha256, self.version = binary_sha256, version
        self.process, self.log = None, None
        self.environment = {}
        self.created = False
        if mode not in ACCEPTANCE_TESTS:
            raise ValueError("unknown export broker acceptance mode")
        self.mode = mode

    def __enter__(self):
        try:
            return self.start()
        except BaseException:
            self.close()
            if self.created and self.process is None:
                for name in ("server.key", "broker.conf", "environment.json"):
                    path = self.directory / name
                    if path.is_file() and not path.is_symlink():
                        path.unlink()
            raise

    def start(self):
        if platform.system() != "Linux" or self.version not in SUPPORTED_SERVERS:
            raise RuntimeError("fixture requires Linux and an explicitly supported broker")
        if self.binary.is_symlink() or not self.binary.is_file() or not 0 < self.binary.stat().st_size <= 64 << 20:
            raise RuntimeError("broker binary must be a bounded regular file")
        if not re.fullmatch(r"[0-9a-f]{64}", self.binary_sha256) or sha256(self.binary) != self.binary_sha256:
            raise RuntimeError("broker binary identity mismatch")
        version = subprocess.run([str(self.binary), "--version"], capture_output=True, text=True, timeout=5, check=True).stdout.strip()
        if version != "nats-server: v" + self.version:
            raise RuntimeError("broker version mismatch")
        if self.directory.exists() or self.directory.is_symlink():
            raise RuntimeError("fixture directory must be fresh")
        self.directory.mkdir(mode=0o700, parents=True)
        self.created = True
        os.umask(0o077)
        cert, key = self.directory / "server.crt", self.directory / "server.key"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key),
                        "-out", str(cert), "-days", "1", "-subj", "/CN=Kelvo export ACL fixture",
                        "-addext", "subjectAltName=IP:127.0.0.1"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True, timeout=20)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        accounts = {}
        for tenant in (("a", "b") if self.mode == "acl" else ("a",)):
            users = []
            roles = [("initializer", True), ("gateway", True), ("worker", True)]
            if self.mode == "acl":
                roles.append(("gateway", False))
            for role, exports in roles:
                label = role if exports else "base"
                user, password = tenant + "-" + label, secrets.token_hex(32)
                prefix = ("KELVO_TEST_EXPORT_ACL_" + tenant.upper() + "_" + label.upper() if self.mode == "acl" else
                          FIXTURE_PREFIXES[self.mode] + ("" if role == "initializer" else "_" + role.upper()))
                self.environment[prefix + "_USER"] = user
                self.environment[prefix + "_PASSWORD"] = password
                users.append({"user": user, "password": password, "permissions": tenant_permissions(role, exports)})
            accounts[tenant] = {"jetstream": {"max_memory": 16 << 20, "max_file": 64 << 20,
                                              "max_streams": 5, "max_consumers": 2}, "users": users}
        config = {"host": "127.0.0.1", "port": port, "accounts": accounts,
                  "jetstream": {"store_dir": str(self.directory / "state"), "max_mem_store": 32 << 20, "max_file_store": 128 << 20},
                  "tls": {"cert_file": str(cert), "key_file": str(key), "min_version": "1.3", "timeout": 2}}
        config_path = self.directory / "broker.conf"
        config_path.write_text(json.dumps(config, indent=2) + "\n")
        prefix = FIXTURE_PREFIXES[self.mode]
        self.environment.update({prefix + "_URL": "tls://127.0.0.1:" + str(port), prefix + "_CA_FILE": str(cert)})
        (self.directory / "environment.json").write_text(json.dumps(self.environment, indent=2) + "\n")
        self.log = (self.directory / "broker.log").open("w")
        self.process = subprocess.Popen([str(self.binary), "-c", str(config_path)], stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 10
        try:
            while True:
                if self.process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("owned loopback broker did not start")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.2):
                        break
                except OSError:
                    time.sleep(.1)
        except BaseException:
            self.close()
            raise
        return self

    def close(self):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log is not None:
            self.log.close()
        return self.process is None or self.process.poll() is not None

    def __exit__(self, kind, value, traceback):
        self.close()


def source_manifest(root, path=None):
    if path:
        manifest = json.loads(Path(path).read_text())
    else:
        names = subprocess.run(["git", "ls-files", "-z"], cwd=root, capture_output=True, check=True, timeout=5).stdout.decode().split("\0")
        files = {name: sha256(root / name) for name in names if name}
        revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=True, timeout=5).stdout.strip()
        manifest = {"files": files, "revision": revision,
                    "sha256": hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
    files = manifest.get("files")
    if not isinstance(files, dict) or not 1 <= len(files) <= 20000:
        raise ValueError("source manifest requires a bounded file map")
    for name, digest in files.items():
        if not isinstance(name, str) or not name or Path(name).is_absolute() or ".." in Path(name).parts or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("invalid source manifest entry")
    digest = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if manifest.get("sha256") != digest:
        raise ValueError("source manifest identity mismatch")
    return manifest


def verify_source(root, manifest):
    return all(not (root / name).is_symlink() and (root / name).is_file() and sha256(root / name) == digest
               for name, digest in manifest["files"].items())


def run(args):
    os.umask(0o077)
    root = Path(__file__).resolve().parents[1]
    output = Path(args.output).absolute()
    if output.exists() or output.is_symlink():
        raise RuntimeError("acceptance output must be fresh")
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    test = ACCEPTANCE_TESTS[args.mode]
    report = {"mode": args.mode, "server_version": args.server_version, "server_binary_sha256": args.server_sha256, "build_tags": args.tags,
              "scope": SCOPES[args.mode],
              "tests": [], "failures": [], "skips": [], "passed": False}
    diagnostics = ExportDiagnostics()
    fixture = ExportBrokerFixture(args.fixture, args.nats_server, args.server_sha256, args.server_version, args.mode)
    process = None
    try:
        manifest = source_manifest(root, args.source_manifest)
        report.update(source_sha256=manifest["sha256"], source_revision=manifest.get("revision"), source_verified_before=verify_source(root, manifest))
        if not report["source_verified_before"]:
            raise RuntimeError("source snapshot mismatch")
        if not re.fullmatch(r"[a-zA-Z0-9_]+(,[a-zA-Z0-9_]+)*", args.tags):
            raise ValueError("invalid build tags")
        artifacts = {}
        if args.mode == "lifecycle":
            for label, value in (("binary", args.export_binary), ("sandbox", args.export_sandbox)):
                if not value:
                    raise RuntimeError("lifecycle requires export binary and sandbox")
                path = Path(value).absolute()
                if path.is_symlink() or not path.is_file() or not os.access(path, os.X_OK):
                    raise RuntimeError("lifecycle artifact must be an existing executable regular file")
                artifacts[label] = str(path)
                report["export_" + label + "_sha256"] = sha256(path)
        elif args.export_binary or args.export_sandbox:
            raise RuntimeError("worker artifacts apply only to lifecycle mode")
        with fixture:
            env = dict(os.environ, GOMAXPROCS="1", GOTOOLCHAIN="local", GOPROXY="off", GOSUMDB="off", CGO_ENABLED="1", PYTHONDONTWRITEBYTECODE="1")
            for name in list(env):
                if name.startswith("KELVO_TEST_"):
                    del env[name]
            env.update(fixture.environment)
            for label, path in artifacts.items():
                env["KELVO_TEST_EXPORT_" + label.upper()] = path
            command = [args.go, "test", "-race", "-json", "-count=1", "-p", "1", "-tags", args.tags]
            if args.modfile:
                command += ["-modfile", str(Path(args.modfile).absolute())]
            command += ["-run", "^" + test + "$", "./internal/cluster"]
            log_path = fixture.directory / "acceptance.log"
            started = time.monotonic()
            with log_path.open("w") as log:
                process = subprocess.Popen(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    code = process.wait(timeout=180)
                except BaseException:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                    raise
            report.update(exit_code=code, seconds=round(time.monotonic() - started, 3), log_sha256=sha256(log_path))
            for line in log_path.read_text().splitlines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                diagnostics.observe(event)
                if event.get("Test") and event.get("Action") in ("pass", "fail", "skip"):
                    report["tests"].append({"name": event["Test"], "outcome": event["Action"]})
                    if event["Action"] == "fail":
                        report["failures"].append(event["Test"])
                    if event["Action"] == "skip":
                        report["skips"].append(event["Test"])
            report["passed"] = code == 0 and {"name": test, "outcome": "pass"} in report["tests"] and not report["failures"] and not report["skips"]
            report["source_verified_after"] = verify_source(root, manifest)
            report["passed"] = report["passed"] and report["source_verified_after"]
    except BaseException as error:
        report["error"] = type(error).__name__
    finally:
        report["diagnostics"] = diagnostics.report()
        report["broker_stopped"] = fixture.close()
        report["passed"] = report["passed"] and report["broker_stopped"]
        output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({key: value for key, value in report.items() if key != "tests"}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=tuple(ACCEPTANCE_TESTS), default="acl")
    for name in ("fixture", "nats-server", "server-sha256", "server-version", "go", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--modfile")
    parser.add_argument("--tags", default="duckdb_arrow", help="comma-separated tags matching the qualified worker build")
    parser.add_argument("--source-manifest", help="verified file-map JSON for extracted source; defaults to current tracked Git files")
    parser.add_argument("--export-binary", help="existing qualified DuckDB bridge binary; lifecycle only")
    parser.add_argument("--export-sandbox", help="existing Linux sandbox launcher; lifecycle only")
    raise SystemExit(run(parser.parse_args()))
