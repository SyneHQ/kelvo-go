#!/usr/bin/env python3
"""Compile the independent application and enforce the public SDK boundary.

Candidate mode uses an explicit temporary module replacement. Published mode
uses the supplied public version with GOWORK=off and no local replacements.
Builds, logs and dependency reports stay outside the source checkout.
"""
import argparse
from contextlib import contextmanager
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import time

MODULE = "github.com/SYNEHQ/kelvo-go"
FORBIDDEN = (MODULE + "/internal", "syne_db", "github.com/SYNEHQ/db.api.go", "github.com/synehq/db.api.go",
             "github.com/duckdb", "github.com/jackc/pgx", "github.com/lib/pq", "github.com/go-sql-driver/mysql",
             "github.com/nats-io", "go.mongodb.org/mongo-driver")


def forbidden_package(package):
    # Go reports declared module identities, which need not be repository URLs.
    # Trim the annotation attached to packages rebuilt for an external test.
    package_path = package["ImportPath"].split(" ", 1)[0]
    module_path = (package.get("Module") or {}).get("Path", "")
    return any(path == root or path.startswith(root + "/")
               for path in (package_path, module_path) for root in FORBIDDEN)


def objects(raw):
    decoder = json.JSONDecoder()
    while raw.strip():
        item, end = decoder.raw_decode(raw.lstrip())
        yield item
        raw = raw.lstrip()[end:]


@contextmanager
def reap_orphaned_children():
    """Keep Linux orphaned grandchildren waitable until their group is empty."""
    if sys.platform != "linux":
        yield
        return
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    if libc.prctl(37, ctypes.byref(previous), 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "cannot read child-subreaper state")
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "cannot enable child reaping")
    try:
        yield
    finally:
        if libc.prctl(36, previous.value, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "cannot restore child-subreaper state")


def wait_process_group(process, timeout):
    deadline = time.monotonic() + timeout
    while True:
        # Let Popen own its direct child's wait status. Reap adopted children
        # only after that parent has been collected, and only in this group.
        parent_reaped = process.poll() is not None
        if parent_reaped:
            while True:
                try:
                    child, _ = os.waitpid(-process.pid, os.WNOHANG)
                except ChildProcessError:
                    break
                if child == 0:
                    break
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            if parent_reaped:
                return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.05, remaining))


def cleanup_process_group(process, cleanup, settle_timeout, term_timeout, kill_timeout):
    previous = {sig: signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        complete = wait_process_group(process, settle_timeout if process.returncode is not None else 0)
        cleanup["descendants_after_exit"] = not complete and process.returncode is not None
        for sig, timeout, key in ((signal.SIGTERM, term_timeout, "term_sent"),
                                  (signal.SIGKILL, kill_timeout, "kill_sent")):
            if complete:
                break
            try:
                os.killpg(process.pid, sig)
                cleanup[key] = True
            except ProcessLookupError:
                pass
            complete = wait_process_group(process, timeout)
        cleanup["parent_reaped"] = process.poll() is not None
        cleanup["process_group_gone"] = complete
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def run_stage(report, out, name, command, cwd, env, timeout=600,
              settle_timeout=0.25, term_timeout=5, kill_timeout=3):
    start = time.monotonic()
    cleanup = {"parent_reaped": True, "process_group_gone": True,
               "descendants_after_exit": False, "term_sent": False, "kill_sent": False}
    stage = {"name": name, "exit": None, "passed": False, "timed_out": False,
             "process_started": False, "cleanup": cleanup}
    report["stages"].append(stage)
    try:
        with reap_orphaned_children(), (out / (name + ".log")).open("wb") as log:
            process = None
            try:
                process = subprocess.Popen(command, cwd=cwd, env=env, stdout=log,
                                           stderr=subprocess.STDOUT, start_new_session=True)
                stage.update(process_started=True, process_group=process.pid)
                cleanup.update(parent_reaped=False, process_group_gone=False)
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                stage["timed_out"] = True
                raise
            finally:
                if process is not None:
                    try:
                        cleanup_process_group(process, cleanup, settle_timeout, term_timeout, kill_timeout)
                    except OSError as error:
                        cleanup["error"] = str(error)
                    stage["exit"] = process.returncode
            if not cleanup["parent_reaped"] or not cleanup["process_group_gone"] or cleanup.get("error"):
                raise RuntimeError(name + " process cleanup failed; see retained report")
            if process.returncode:
                raise RuntimeError(name + " failed; see retained log")
            if cleanup["descendants_after_exit"]:
                raise RuntimeError(name + " left descendant processes; see retained report")
        stage["passed"] = True
    except BaseException as error:
        stage["failure"] = type(error).__name__
        raise
    finally:
        stage["seconds"] = time.monotonic() - start


def source_manifest(root):
    """Hash regular source files without following links or reading Git caches."""
    root = Path(root).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("source snapshot root is not a directory")
    entries = {}
    total = 0
    ignored = {".git", "__pycache__", ".pytest_cache"}
    for directory, subdirs, files in os.walk(root):
        subdirs[:] = sorted(name for name in subdirs if name not in ignored)
        for name in subdirs:
            if (Path(directory) / name).is_symlink():
                raise ValueError("source snapshot contains a directory symlink")
        for name in sorted(files):
            if name in ignored:
                continue
            path = Path(directory) / name
            before = path.lstat()
            if not stat.S_ISREG(before.st_mode) or before.st_size > 256 << 20:
                raise ValueError("source snapshot contains a nonregular or oversized file")
            total += before.st_size
            if total > 1 << 30 or len(entries) >= 100000:
                raise ValueError("source snapshot exceeds its bound")
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                opened = os.fstat(stream.fileno())
                if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                    raise ValueError("source file changed while opening")
                count = 0
                while True:
                    raw = stream.read(65536)
                    if not raw:
                        break
                    count += len(raw)
                    if count > before.st_size:
                        raise ValueError("source file grew while hashing")
                    digest.update(raw)
                after = os.fstat(stream.fileno())
            if count != before.st_size or (after.st_size, after.st_mtime_ns, after.st_mode) != (before.st_size, before.st_mtime_ns, before.st_mode):
                raise ValueError("source file changed while hashing")
            entries[path.relative_to(root).as_posix()] = {
                "sha256": digest.hexdigest(), "bytes": count, "mode": stat.S_IMODE(before.st_mode)}
    return entries


def manifest_digest(manifest):
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def capture_source(report, out, label, root):
    manifest = source_manifest(root)
    name = "source-" + label + "-before.json"
    (out / name).write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
    report["provenance"][label] = {"files": len(manifest),
                                  "bytes": sum(value["bytes"] for value in manifest.values()),
                                  "manifest_sha256": manifest_digest(manifest), "manifest_file": name,
                                  "unchanged_after_execution": None}
    return manifest


def verify_source(report, out, label, root, expected):
    current = source_manifest(root)
    name = "source-" + label + "-after.json"
    (out / name).write_text(json.dumps(current, sort_keys=True, indent=2) + "\n")
    evidence = report["provenance"][label]
    evidence.update(after_manifest_file=name, after_manifest_sha256=manifest_digest(current),
                    unchanged_after_execution=current == expected,
                    added=sorted(current.keys() - expected.keys()), removed=sorted(expected.keys() - current.keys()),
                    changed=sorted(name for name in current.keys() & expected.keys() if current[name] != expected[name]))
    if current != expected:
        raise RuntimeError(label + " source changed during acceptance; see provenance manifests")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default="go")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--published-version")
    args = parser.parse_args()
    if os.name != "posix":
        parser.error("process-group acceptance requires a POSIX host")
    source = Path(__file__).resolve().parents[1]
    out = args.output.resolve()
    if out.exists() or out.is_relative_to(source):
        parser.error("output must be a fresh directory outside the checkout")
    if args.published_version and not re.fullmatch(r"v[0-9][A-Za-z0-9.+-]*", args.published_version):
        parser.error("invalid public module version")
    out.mkdir(parents=True, mode=0o700)
    example = out / "application"
    env = os.environ | {"GOWORK": "off", "GOENV": "off", "GOFLAGS": "", "GOTOOLCHAIN": "local",
                        "CGO_ENABLED": "0", "GOMAXPROCS": "2"}
    report = {"mode": "published" if args.published_version else "candidate", "passed": False,
              "version": args.published_version, "stages": [], "forbidden_imports": [], "provenance": {}}
    snapshots = {}

    def run(name, command, variables=None):
        run_stage(report, out, name, command, example, variables or env)

    def interrupted(_signum, _frame):
        raise RuntimeError("acceptance interrupted")

    previous = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        checkout_before = capture_source(report, out, "checkout", source)
        snapshots["checkout"] = (source, checkout_before)
        template = source_manifest(source / "examples/application")
        shutil.copytree(source / "examples/application", example)
        copied = source_manifest(example)
        report["provenance"]["template_copy"] = {"template_sha256": manifest_digest(template),
                                                  "copied_sha256": manifest_digest(copied),
                                                  "matches": copied == template}
        if copied != template:
            raise RuntimeError("copied application differs from source template")
        go = args.go
        if args.published_version:
            run("pin", [go, "mod", "edit", "-require=" + MODULE + "@" + args.published_version])
            # An example may never silently resolve to a local checkout.
            run("module-before", [go, "mod", "edit", "-json"])
            if json.loads((out / "module-before.log").read_text()).get("Replace"):
                raise RuntimeError("published application contains module replacements")
        else:
            run("candidate", [go, "mod", "edit", "-replace=" + MODULE + "=" + str(source)])
        run("tidy", [go, "mod", "tidy"])
        run("resolved-module", [go, "list", "-mod=readonly", "-m", "-json", MODULE])
        resolved = json.loads((out / "resolved-module.log").read_text())
        report["resolved_module"] = {key: resolved[key] for key in ("Path", "Version", "Sum", "GoModSum") if key in resolved}
        if resolved.get("Path") != MODULE:
            raise RuntimeError("unexpected Kelvo module identity")
        if args.published_version:
            if resolved.get("Version") != args.published_version or resolved.get("Replace"):
                raise RuntimeError("published module version or replacement mismatch")
        elif Path(resolved.get("Replace", {}).get("Dir", "")).resolve() != source:
            raise RuntimeError("candidate module replacement mismatch")
        if args.published_version and not resolved.get("Dir"):
            raise RuntimeError("published module source directory is missing")
        sdk_source = source if not args.published_version else Path(resolved["Dir"]).resolve(strict=True)
        sdk_before = capture_source(report, out, "sdk", sdk_source)
        snapshots["sdk"] = (sdk_source, sdk_before)
        application_before = capture_source(report, out, "application", example)
        snapshots["application"] = (example, application_before)
        run("dependencies", [go, "list", "-mod=readonly", "-deps", "-test", "-json", "./..."])
        packages = list(objects((out / "dependencies.log").read_text()))
        imports = sorted({p["ImportPath"] for p in packages})
        report["forbidden_imports"] = sorted({p["ImportPath"] for p in packages if forbidden_package(p)})
        report["cgo_packages"] = sorted(p["ImportPath"] for p in packages if p.get("CgoFiles"))
        report["public_packages"] = [p for p in imports if p.startswith(MODULE + "/")]
        required = {MODULE + "/" + name for name in ("client", "query", "delegation", "resolver", "operations")}
        if report["forbidden_imports"] or report["cgo_packages"] or not required.issubset(imports):
            raise RuntimeError("independent application dependency boundary failed")
        run("test", [go, "test", "-mod=readonly", "-p=2", "-count=1", "-timeout=5m", "./..."])
        run("race", [go, "test", "-mod=readonly", "-p=2", "-race", "-count=1", "-timeout=5m", "./..."], env | {"CGO_ENABLED": "1"})
        run("vet", [go, "vet", "-mod=readonly", "-p=2", "./..."])
        for name in ("authority", "exercise"):
            run("build-" + name, [go, "build", "-mod=readonly", "-p=2", "-trimpath", "-o", str(out / name), "./cmd/" + name])
        report["binaries"] = {name: {"sha256": hashlib.sha256((out / name).read_bytes()).hexdigest(),
                                     "bytes": (out / name).stat().st_size} for name in ("authority", "exercise")}
        report["passed"] = True
    except (RuntimeError, subprocess.TimeoutExpired, ValueError, OSError) as error:
        report["failure"] = str(error)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        for label, (directory, expected) in snapshots.items():
            try:
                verify_source(report, out, label, directory, expected)
            except (RuntimeError, ValueError, OSError) as error:
                report["passed"] = False
                report["provenance"][label]["unchanged_after_execution"] = False
                report.setdefault("provenance_failures", []).append(str(error))
                report.setdefault("failure", str(error))
    report["cleanup"] = {"all_parents_reaped": bool(report["stages"]) and all(
        stage["cleanup"]["parent_reaped"] for stage in report["stages"]),
        "all_process_groups_gone": bool(report["stages"]) and all(
        stage["cleanup"]["process_group_gone"] for stage in report["stages"])}
    if not all(report["cleanup"].values()):
        report["passed"] = False
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "report": str(out / "report.json"), "failure": report.get("failure")}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
