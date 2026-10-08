#!/usr/bin/env python3
"""Verify packaged binaries and real Arrow reads in bounded Linux containers.

Requires Docker, Landlock ABI 3+ and pyarrow. Uses synthetic data, no network,
no host ports and no production configuration. Does not build or publish images.
"""
import argparse
import decimal
import hashlib
import io
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import tempfile
import time

import pyarrow.ipc as ipc


SQL = "SELECT region, SUM(CAST(amount AS DECIMAL(18,2))) AS revenue FROM sales GROUP BY region ORDER BY region"
SALES = b"region,amount\neast,10.25\neast,14.75\nwest,19.75\nnorth,\n"
EXPECTED = [{"region": "east", "revenue": decimal.Decimal("25.00")},
            {"region": "north", "revenue": None},
            {"region": "west", "revenue": decimal.Decimal("19.75")}]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def verify_arrow(raw):
    require(raw.endswith(b"\xff\xff\xff\xff\x00\x00\x00\x00"), "Arrow EOS is missing")
    with ipc.open_stream(io.BytesIO(raw)) as reader:
        table = reader.read_all()
    require(table.to_pylist() == EXPECTED, "Arrow decimal or NULL values changed")
    return {"rows": table.num_rows, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def operation_payload(bad_digest=False):
    # This ASCII fixture preserves operations.Request field order and Go JSON
    # encoding. It is not a replacement for the public signing SDK.
    request = {"version": 1, "kind": "query.read", "connection": {"id": "image-fixture", "database": "sales"},
               "idempotency_key": "", "spec": {"query": {"sql": SQL}}}
    encoded = json.dumps(request, separators=(",", ":")).encode()
    request_hash = hashlib.sha256(b"kelvo.database.operation.v1\x00" + encoded).hexdigest()
    source_hash = "a" * 64 if bad_digest else hashlib.sha256(SALES).hexdigest()
    now = int(time.time())
    return {"version": 1, "operation_id": "image-fixture-operation", "request_sha256": request_hash,
            "request": request, "limits": {"max_rows": 100, "max_bytes": 1 << 20, "batch_rows": 2,
                "timeout_ms": 10000, "memory_mb": 128, "threads": 1, "max_temp_mb": 32},
            "source": {"engine": "csv", "tenant_id": "image-fixture-tenant", "connection_id": "image-fixture",
                "revision": "fixture-1", "database": "sales", "schema": "",
                "options": {"file_format": "csv", "file_bytes": str(len(SALES)), "file_sha256": source_hash}},
            "source_file": {"version": 1, "format": "csv", "bytes": len(SALES), "sha256": source_hash},
            "credentials_valid_until": now + 5, "expires_at": now + 20}


def run(runtime_image, worker_image, run_id=None):
    command = None
    for candidate in (["docker"], ["sudo", "-n", "docker"]):
        result = subprocess.run(candidate + ["info", "--format", "{{.OSType}}"], capture_output=True, timeout=15)
        if result.returncode == 0 and result.stdout.strip() == b"linux":
            command = candidate
            break
    require(command is not None, "A Linux Docker engine is required")
    token = run_id or secrets.token_hex(8)
    require(len(token) == 16 and all(c in "0123456789abcdef" for c in token), "Invalid acceptance run identifier")
    label = "io.kelvo.worker-image-test"
    owned = []
    report = {"ok": False, "scope": "Image packaging, native DuckDB/Arrow and fd6 adapter protocol; no cluster admission or live-provider qualification", "gates": {}, "runs": []}

    def docker(*args, check=True, payload=None, timeout=30):
        result = subprocess.run(command + list(args), input=payload, capture_output=True, timeout=timeout)
        if check:
            require(result.returncode == 0, "Docker acceptance command failed: " + args[0])
        return result

    def inspect(cid):
        return json.loads(docker("inspect", cid).stdout)[0]

    def execute(image, entrypoint, args, mounts=(), payload=None, success=True):
        options = ["create", "-i", "--pull=never", "--label", label + "=" + token,
                   "--network", "none", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                   "--pids-limit", "128", "--memory", "512m", "--memory-swap", "512m", "--cpus", "1",
                   "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=128m,uid=65532,gid=65532,mode=0700",
                   "--env", "GOMAXPROCS=1", "--env", "KELVO_OPERATION_PROCESS=1", "--entrypoint", entrypoint]
        for source, destination in mounts:
            options += ["--mount", "type=bind,src=" + str(source) + ",dst=" + destination + ",readonly"]
        cid = docker(*options, image, *args).stdout.decode().strip()
        owned.append(cid)
        info = inspect(cid)
        host = info["HostConfig"]
        require(info["Config"]["User"] == "65532:65532", "Image does not default to the nonroot service UID")
        require(host["ReadonlyRootfs"] and not host["Privileged"] and "ALL" in host["CapDrop"], "Container privilege policy changed")
        require(host["Memory"] == 512 << 20 and host["MemorySwap"] == 512 << 20 and host["PidsLimit"] == 128 and host["NanoCpus"] == 10**9, "Container resource limits changed")
        require(host["NetworkMode"] == "none" and host["PidMode"] == "" and not host["PortBindings"], "Container isolation changed")
        require(any(value.startswith("no-new-privileges") for value in host["SecurityOpt"]), "No-new-privileges is missing")
        require(all(not item["RW"] for item in info["Mounts"] if item["Type"] == "bind"), "Fixture mount is writable")
        wire = json.dumps(payload(), separators=(",", ":")).encode() if callable(payload) else payload
        result = docker("start", "-a", "-i", cid, check=False, payload=wire, timeout=25)
        state = inspect(cid)["State"]
        # Fixtures are synthetic and contain no credentials. Keep bounded
        # diagnostics before asserting so a failed package is reviewable.
        report["runs"].append({"image": image, "entrypoint": entrypoint,
            "expected_success": success, "exit_code": state["ExitCode"],
            "docker_exit_code": result.returncode, "oom_killed": state["OOMKilled"],
            "running": state["Running"], "stdout_bytes": len(result.stdout),
            "stdout_sha256": hashlib.sha256(result.stdout).hexdigest(),
            "stderr": result.stderr[-8192:].decode(errors="replace")})
        require(not state["Running"] and not state["OOMKilled"], "Container did not finish without OOM")
        require((state["ExitCode"] == 0 and result.returncode == 0) == success, "Container exit status did not match the expected outcome")
        return result

    try:
        images = {name: json.loads(docker("image", "inspect", tag).stdout)[0]["Id"]
                  for name, tag in (("runtime", runtime_image), ("worker", worker_image))}
        report["images"] = images
        # All subsequent runs use immutable local image IDs, not mutable tags.
        common = "sha256sum /usr/local/bin/kelvo /usr/local/bin/kelvo-landlock"
        base = execute(images["runtime"], "/bin/sh", ["-ec", "test ! -e /usr/local/bin/kelvo-adapter-go; " + common])
        packaged = execute(images["worker"], "/bin/sh", ["-ec", "cd /usr/local/bin; sha256sum -c /usr/share/kelvo/kelvo-adapter-go.sha256 >/dev/null; " + common + "; sha256sum /usr/local/bin/kelvo-adapter-go; test \"$(stat -c '%a:%u:%g' /usr/local/bin/kelvo-adapter-go)\" = 555:0:0"])
        require(base.stdout.splitlines() == packaged.stdout.splitlines()[:2], "Core binaries differ between runtime and worker images")
        report["gates"]["packaged_binaries"] = {"passed": True, "sha256": dict(line.decode().split()[::-1] for line in packaged.stdout.splitlines())}
        policy = execute(images["worker"], "/bin/sh", ["-ec", "test \"$(id -u)\" = 65532; touch /tmp/allowed; if touch /var/tmp/forbidden 2>/dev/null; then exit 9; fi; cat /proc/self/status"])
        status = dict(line.split(":", 1) for line in policy.stdout.decode().splitlines() if ":" in line)
        require(int(status["CapEff"].strip(), 16) == 0 and status["NoNewPrivs"].strip() == "1", "Live privilege restrictions are missing")
        report["gates"]["runtime_policy"] = {"passed": True, "nonroot": True, "readonly_root": True, "no_new_privileges": True, "capabilities": 0}
        with tempfile.TemporaryDirectory(prefix="kelvo-worker-image-") as temporary:
            source = Path(temporary) / "sales.csv"
            source.write_bytes(SALES)
            source.chmod(0o444)
            mounts = [(source, "/data/sales.csv"), (source, "/private/unselected.csv")]
            limits = {"max_rows": 100, "max_bytes": 1 << 20, "timeout": 10_000_000_000, "memory_mb": 128, "threads": 1, "max_temp_mb": 32}
            def native(path):
                return json.dumps({"config": {"sources": [{"id": "sales", "type": "csv", "path": path}]}, "limits": limits,
                                   "request": {"mode": "federated", "sources": ["sales"], "sql": SQL}}).encode()
            launcher = ["--read", "/data/sales.csv", "--write", "/tmp", "--", "/usr/local/bin/kelvo", "worker"]
            result = execute(images["worker"], "/usr/local/bin/kelvo-landlock", launcher, mounts, native("/data/sales.csv"))
            require(not json.loads(result.stderr).get("error"), "Native sandboxed query failed")
            report["gates"]["native_arrow"] = {"passed": True, **verify_arrow(result.stdout)}
            denied = execute(images["worker"], "/usr/local/bin/kelvo-landlock", launcher, mounts, native("/private/unselected.csv"))
            require(not denied.stdout and json.loads(denied.stderr).get("error"), "Landlock allowed an unselected file")
            report["gates"]["landlock_denial"] = {"passed": True}
            # The adapter accepts an unlinked 0400 snapshot, not a caller path.
            shell = "cp /data/sales.csv /tmp/source; chmod 0400 /tmp/source; exec 6</tmp/source; rm /tmp/source; exec 5</usr/local/bin/kelvo-adapter-go; exec 3>&2; exec /usr/local/bin/kelvo-landlock --write /tmp -- /proc/self/fd/5 worker"
            result = execute(images["worker"], "/bin/sh", ["-ec", shell], mounts, operation_payload)
            receipt = json.loads(result.stderr)
            observed = verify_arrow(result.stdout)
            require(receipt["operation_id"] == "image-fixture-operation" and receipt["request_sha256"] == operation_payload()["request_sha256"] and receipt["outcome"] == "completed" and receipt["effect"] == "none", "Adapter receipt does not bind the completed read")
            require(all(receipt["result"][key] == value for key, value in observed.items()) and receipt["result"]["format"] == "arrow_ipc", "Adapter receipt does not bind Arrow bytes")
            report["gates"]["adapter_fd6_arrow"] = {"passed": True, **observed}
            bad = execute(images["worker"], "/bin/sh", ["-ec", shell], mounts, lambda: operation_payload(True), success=False)
            rejected, _ = json.JSONDecoder().raw_decode(bad.stderr.decode())
            require(not bad.stdout and rejected["outcome"] == "rejected" and rejected["error_code"] == "SOURCE_FAILED", "Adapter accepted changed snapshot bytes")
            report["gates"]["adapter_digest_rejection"] = {"passed": True}
        report["ok"] = True
    except Exception as error:
        report["error"] = str(error)
    finally:
        failures = []
        discovery_failed = False
        try:
            # Also find a create that succeeded before its acknowledgement was lost.
            discovered = docker("ps", "-aq", "--no-trunc", "--filter", "label=" + label + "=" + token).stdout.decode().split()
            owned = list(dict.fromkeys(owned + discovered))
        except Exception:
            discovery_failed = True
        for cid in reversed(owned):
            try:
                require(inspect(cid)["Config"]["Labels"].get(label) == token, "Container ownership changed")
                docker("rm", "-f", cid)
                require(docker("inspect", cid, check=False).returncode != 0, "Owned container remains")
            except Exception:
                failures.append(cid)
        try:
            remaining = docker("ps", "-aq", "--no-trunc", "--filter", "label=" + label + "=" + token).stdout.decode().split()
        except Exception:
            remaining = []
            discovery_failed = True
        clean = not failures and not remaining and not discovery_failed
        report["cleanup"] = {"ok": clean, "failed_removals": failures, "remaining_owned_containers": remaining,
                             "inventory_failed": discovery_failed, "ownership_label": label + "=" + token}
        report["ok"] = report["ok"] and clean
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--run-id", help="Optional ownership label for a supervising test controller")
    args = parser.parse_args()
    require(not args.output.exists(), "Preserve existing evidence; use a new output path")
    os.umask(0o077)
    def interrupted(signum, frame):
        raise InterruptedError("Image acceptance interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    report = run(args.runtime_image, args.worker_image, args.run_id)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as target:
        json.dump(report, target, indent=2)
        target.write("\n")
    print(json.dumps(report, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
