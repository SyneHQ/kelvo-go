#!/usr/bin/env python3
"""Run real sandboxed Arrow queries and controlled tenant-network probes.

Requires Docker, host Landlock support and pyarrow. Uses synthetic files and
unique temporary resources only; no host ports or private data mounts.
"""
import argparse
import decimal
import hashlib
import io
import json
from pathlib import Path
import secrets
import subprocess
import tempfile

import pyarrow.ipc as ipc
from container_network_acceptance import run_network_checks


def docker_command():
    for command in (["docker"], ["sudo", "-n", "docker"]):
        result = subprocess.run(command + ["info", "--format", "{{.ServerVersion}}"], capture_output=True, timeout=15)
        if result.returncode == 0:
            return command
    raise RuntimeError("Docker is unavailable to this test process")


def run_acceptance(image, binary, nats_binary):
    command = docker_command()
    run_id = secrets.token_hex(5)
    prefix = "kelvo-container-" + run_id
    network = prefix + "-tenant"
    containers, networks = [], []
    results = {"scope": "Single-query tenant containers, OS sandbox, Arrow values and controlled network reachability"}

    def docker(*args, check=True, input=None, timeout=30):
        result = subprocess.run(command + list(args), input=input, capture_output=True, timeout=timeout)
        if check and result.returncode:
            # Never echo command output that may contain query diagnostics.
            raise RuntimeError("Docker acceptance command failed: " + args[0])
        return result

    def inspect(name):
        return json.loads(docker("inspect", name).stdout)[0]

    def runtime(name, entrypoint):
        return ["create", "-i", "--pull=never", "--name", name, "--network", network,
                "--user", "65532:65532", "--read-only", "--cap-drop", "ALL",
                "--security-opt", "no-new-privileges:true", "--pids-limit", "256",
                "--memory", "512m", "--memory-swap", "512m", "--cpus", "1",
                "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=128m,uid=65532,gid=65532,mode=0700",
                "--entrypoint", entrypoint]

    def create(name, args):
        docker(*args)
        containers.append(name)

    def assert_policy(info):
        host = info["HostConfig"]
        assert info["Config"]["User"] == "65532:65532", "unexpected runtime user"
        assert host["ReadonlyRootfs"] and not host["Privileged"], "root/privilege policy differs"
        assert "ALL" in host["CapDrop"], "capabilities were not dropped"
        assert any(x.startswith("no-new-privileges") for x in host["SecurityOpt"]), "privilege policy absent"
        assert host["Memory"] == 512 << 20 and host["MemorySwap"] == host["Memory"], "memory/swap limits differ"
        assert host["PidsLimit"] == 256 and host["NanoCpus"] == 1000000000, "PID/CPU limits differ"
        assert host["PidMode"] == "" and host["NetworkMode"] == network, "unexpected PID/network mode"
        assert set(info["NetworkSettings"]["Networks"]) == {network}, "container joined another network"
        tmp = host["Tmpfs"]["/tmp"]
        assert all(x in tmp.split(",") for x in ("noexec", "nosuid", "nodev", "size=128m")), "scratch policy differs"
        return {"user": info["Config"]["User"], "readonly_root": True, "dropped_capabilities": host["CapDrop"],
                "no_new_privileges": True, "memory_bytes": host["Memory"], "swap_total_bytes": host["MemorySwap"],
                "pid_limit": host["PidsLimit"], "cpu_limit": 1, "tmpfs": tmp, "separate_pid_namespace": True}

    try:
        results["image"] = image
        results["image_id"] = json.loads(docker("image", "inspect", image).stdout)[0]["Id"]
        hash_name = prefix + "-hash"
        create(hash_name, ["create", "--pull=never", "--name", hash_name, "--network", "none", "--read-only",
                           "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                           "--entrypoint", "/usr/bin/sha256sum", image, "/usr/local/bin/kelvo"])
        actual = docker("start", "-a", hash_name).stdout.decode().split()[0]
        assert actual == hashlib.sha256(Path(binary).read_bytes()).hexdigest(), "runtime image has a stale host binary"
        results["binary_sha256"] = actual
        docker("network", "create", "--internal", network)
        networks.append(network)
        assert json.loads(docker("network", "inspect", network).stdout)[0]["Internal"], "tenant bridge is not internal"

        with tempfile.TemporaryDirectory(prefix=prefix + "-") as temporary:
            work = Path(temporary)
            sales, forbidden = work / "sales.csv", work / "forbidden.csv"
            sales.write_text("region,amount\neast,10.25\neast,14.75\nwest,19.75\nnorth,\n")
            forbidden.write_text("marker\nsynthetic-unselected-data\n")
            for path in (sales, forbidden):
                path.chmod(0o444)

            def execute(label, path, sql, sandboxed=True):
                name = prefix + "-" + label
                args = runtime(name, "/usr/local/bin/kelvo-landlock" if sandboxed else "/usr/local/bin/kelvo")
                args += ["--mount", f"type=bind,src={sales},dst=/data/sales.csv,readonly",
                         "--mount", f"type=bind,src={forbidden},dst=/private/forbidden.csv,readonly", image]
                args += (["--read", "/data/sales.csv", "--write", "/tmp", "--", "/usr/local/bin/kelvo", "worker"]
                         if sandboxed else ["worker"])
                create(name, args)
                info = inspect(name)
                policy = assert_policy(info)
                assert all(not x["RW"] for x in info["Mounts"] if x["Type"] == "bind"), "writable data mount"
                payload = {"config": {"sources": [{"id": "sales", "type": "csv", "path": path}]},
                           "limits": {"max_rows": 100, "max_bytes": 1048576, "timeout": 5000000000,
                                      "memory_mb": 256, "threads": 1, "max_temp_mb": 32},
                           "request": {"mode": "federated", "sources": ["sales"], "sql": sql}}
                completed = docker("start", "-a", "-i", name, check=False, input=json.dumps(payload).encode(), timeout=15)
                state = inspect(name)["State"]
                if state["OOMKilled"] or state["ExitCode"] != 0:
                    diagnostic = completed.stderr.decode(errors="replace").strip()
                    if not diagnostic.startswith("kelvo-landlock:"):
                        diagnostic = "worker did not exit normally"
                    raise AssertionError(f"exit={state['ExitCode']} oom={state['OOMKilled']}: {diagnostic}")
                try:
                    outcome = json.loads(completed.stderr)
                except (ValueError, UnicodeDecodeError) as exc:
                    raise AssertionError("worker returned no structured outcome") from exc
                return completed.stdout, outcome, policy

            data, outcome, policy = execute("arrow", "/data/sales.csv",
                "SELECT region, SUM(CAST(amount AS DECIMAL(18,2))) AS revenue FROM sales GROUP BY region ORDER BY region")
            assert not outcome.get("error"), "sandboxed selected-source query failed: " + json.dumps(outcome.get("error"))
            assert data.endswith(b"\xff\xff\xff\xff\x00\x00\x00\x00"), "Arrow result has no explicit EOS"
            with ipc.open_stream(io.BytesIO(data)) as reader:
                table = reader.read_all()
            assert table.to_pylist() == [{"region": "east", "revenue": decimal.Decimal("25.00")},
                                         {"region": "north", "revenue": None},
                                         {"region": "west", "revenue": decimal.Decimal("19.75")}], "Arrow values changed"
            results["container_policy"] = policy
            results["sandboxed_arrow_query"] = {"passed": True, "rows": table.num_rows, "decimal_exact": True, "null_preserved": True}

            # DuckDB's catalog permits this synthetic file in both runs. Only the
            # launcher's path policy differs, proving the OS boundary separately.
            control, outcome, _ = execute("file-control", "/private/forbidden.csv", "SELECT marker FROM sales", False)
            assert not outcome.get("error"), "unconfined synthetic-file control failed"
            with ipc.open_stream(io.BytesIO(control)) as reader:
                assert reader.read_all().to_pylist() == [{"marker": "synthetic-unselected-data"}], "control differs"
            denied, outcome, _ = execute("file-denied", "/private/forbidden.csv", "SELECT marker FROM sales")
            assert outcome.get("error") and not denied, "sandbox permitted an unselected synthetic file"
            results["filesystem_sandbox"] = {"unselected_file_denied": True, "same_file_control_succeeded": True}

            name = prefix + "-runtime-policy"
            args = runtime(name, "/bin/sh") + [image, "-c",
                "set -eu; test \"$(id -u)\" = 65532; touch /tmp/scratch; "
                "if touch /var/tmp/kelvo-acceptance-write 2>/dev/null; then exit 7; fi; cat /proc/self/status"]
            create(name, args)
            completed = docker("start", "-a", name)
            assert inspect(name)["State"]["ExitCode"] == 0, "runtime filesystem probe failed"
            status = dict(line.split(":", 1) for line in completed.stdout.decode().splitlines() if ":" in line)
            assert int(status["CapEff"].strip(), 16) == 0 and status["NoNewPrivs"].strip() == "1", "runtime capabilities differ"
            results["runtime_enforcement"] = {"root_write_denied": True, "scratch_write_allowed": True,
                                              "effective_capabilities_zero": True, "no_new_privileges": True}

        results["network"] = run_network_checks(image, str(nats_binary), run_id)
        return results
    finally:
        for name in reversed(containers):
            docker("rm", "-f", name, check=False)
        for name in reversed(networks):
            docker("network", "rm", name, check=False)


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="kelvo-go:accept")
    parser.add_argument("--binary", type=Path, default=root / "bin/kelvo")
    parser.add_argument("--nats-binary", type=Path, default=root / "artifacts/cluster-private/nats-server")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    results = run_acceptance(args.image, args.binary, args.nats_binary)
    encoded = json.dumps(results, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
