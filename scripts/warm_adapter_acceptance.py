#!/usr/bin/env python3
"""Run the real warm adapter against a real PostgreSQL server on Linux.

Builds the worker test binary and the Landlock launcher, then runs one gate in
an owned, delegated and bounded systemd service as a non-root account. The
caller supplies a prebuilt kelvo-adapter-go and a file holding a PostgreSQL URL
for a disposable fixture database. Installs nothing and starts no database.
"""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys

GATE = "TestContainedWarmAdapterReusesPostgresConnection"
LIMITS = {"MemoryMax": "1G", "MemorySwapMax": "0", "CPUQuota": "100%", "TasksMax": "256", "RuntimeMaxSec": "300", "KillMode": "control-group"}


def gate_passed(log, gate, returncode):
    """A gate passes only on exit 0 with exactly one PASS line for it: a SKIP,
    a missing line or a second result is a failure."""
    statuses = re.findall(r"^--- (PASS|FAIL|SKIP): " + re.escape(gate) + r"\b", log, re.M)
    return returncode == 0 and statuses == ["PASS"], statuses


def inside(args):
    out = Path(args.output)
    relative = Path("/proc/self/cgroup").read_text().strip().split(":", 2)[2]
    if relative != "/system.slice/" + args.unit + ".service" or os.geteuid() == 0:
        raise SystemExit("runner does not own the expected non-root service")
    root = Path("/sys/fs/cgroup") / relative.lstrip("/")
    supervisor, jobs, state = root / "supervisor", root / "jobs", out / "state"
    supervisor.mkdir(mode=0o755)
    (supervisor / "cgroup.procs").write_text(str(os.getpid()))
    (root / "cgroup.subtree_control").write_text("+cpu +memory +pids")
    jobs.mkdir(mode=0o755)
    state.mkdir(mode=0o700)
    env = {key: value for key, value in os.environ.items() if key in ("PATH", "HOME", "LANG", "LC_ALL", "TMPDIR")}
    env.update(GOMAXPROCS="1", KELVO_TEST_CGROUP_ROOT=str(jobs), KELVO_TEST_CGROUP_STATE=str(state),
               KELVO_TEST_SANDBOX=str(out / "kelvo-landlock"), KELVO_TEST_WARM_ADAPTER_BINARY=str(out / "kelvo-adapter-go"),
               KELVO_TEST_WARM_POSTGRES_DSN=(out / "postgres.dsn").read_text().strip())
    result = subprocess.run([str(out / "worker.test"), "-test.v", "-test.run=^" + args.gate + "$", "-test.timeout=120s"],
                            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=150)
    (out / "worker.test.log").write_bytes(result.stdout)
    log = result.stdout.decode(errors="replace")
    passed, statuses = gate_passed(log, args.gate, result.returncode)
    # A gate may report measurements on one "GATE_METRICS {json}" log line.
    metrics = [json.loads(m) for m in re.findall(r"GATE_METRICS (\{.*\})$", log, re.M)]
    remaining = sum(p.is_dir() for p in jobs.iterdir())
    inside = {"gate": args.gate, "status": statuses, "exit": result.returncode, "remaining_job_groups": remaining, "passed": passed and remaining == 0}
    if metrics:
        inside["metrics"] = metrics[-1]
    (out / "inside.json").write_text(json.dumps(inside))
    return 0 if passed and remaining == 0 else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--go", default="go")
    parser.add_argument("--adapter", type=Path, help="prebuilt kelvo-adapter-go from this source")
    parser.add_argument("--postgres-dsn-file", type=Path, help="file holding one PostgreSQL URL for a disposable fixture")
    parser.add_argument("--output", type=Path, required=True, help="new private directory for binaries, logs and the report")
    parser.add_argument("--gate", default=GATE, help="Test name in ./internal/worker to run inside the service")
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--unit", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.inside:
        return inside(args)
    if sys.platform != "linux" or os.geteuid() == 0 or args.adapter is None or args.postgres_dsn_file is None:
        parser.error("run on Linux as a non-root account with --adapter and --postgres-dsn-file")
    os.umask(0o022)
    out = args.output.resolve()
    out.mkdir(mode=0o700)
    repo = Path(__file__).resolve().parents[1]
    shutil.copyfile(args.adapter, out / "kelvo-adapter-go")
    (out / "kelvo-adapter-go").chmod(0o555)
    dsn = out / "postgres.dsn"
    dsn.touch(mode=0o600)
    dsn.write_text(args.postgres_dsn_file.read_text().strip() + "\n")
    for command in ([args.go, "test", "-p", "1", "-c", "-o", str(out / "worker.test"), "./internal/worker"],
                    ["cc", "-O2", "-Wall", "-Wextra", "-Werror", "sandbox/launcher.c", "-o", str(out / "kelvo-landlock")]):
        subprocess.run(command, cwd=repo, check=True, timeout=900)
    unit = "kelvo-warm-adapter-" + secrets.token_hex(6)
    command = ["sudo", "-n", "systemd-run", "--unit=" + unit, "--uid=" + str(os.geteuid()), "--property=Delegate=yes",
               *["--property=" + key + "=" + value for key, value in LIMITS.items()], "--property=NoNewPrivileges=yes",
               "--property=CapabilityBoundingSet=", "--property=AmbientCapabilities=", "--collect", "--wait", "--pipe",
               sys.executable, "-B", str(Path(__file__).resolve()), "--inside", "--gate", args.gate, "--unit", unit, "--output", str(out)]
    with (out / "service.log").open("wb") as log:
        code = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=360).returncode
    report = {"schema": 1, "gate": args.gate, "unit": unit, "resource_limits": LIMITS, "service_exit_code": code, "passed": False,
              "scope": "one node, one real adapter, one real PostgreSQL connection; no capacity or tunnel claim"}
    if (out / "inside.json").exists():
        report.update(json.loads((out / "inside.json").read_text()))
    report["passed"] = bool(report.get("passed")) and code == 0
    dsn.unlink()
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    if not report["passed"] and (out / "worker.test.log").exists():
        sys.stdout.write((out / "worker.test.log").read_text(errors="replace")[-4000:])
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
