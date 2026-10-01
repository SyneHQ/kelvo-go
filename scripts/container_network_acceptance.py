#!/usr/bin/env python3
"""Controlled tenant-network acceptance; never publishes host ports."""
import ipaddress
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time


def run_network_checks(image: str, nats_binary: str, run_id: str) -> dict:
    if not re.fullmatch(r"[a-f0-9]{6,24}", run_id):
        raise ValueError("invalid network acceptance run ID")
    command = None
    for candidate in (["docker"], ["sudo", "-n", "docker"]):
        p = subprocess.run(candidate + ["info", "--format", "{{.ServerVersion}}"], capture_output=True, timeout=15)
        if p.returncode == 0:
            command = candidate
            break
    if command is None:
        raise RuntimeError("Docker is unavailable")
    prefix = "kelvo-net-" + run_id
    a, b, external = prefix + "-a", prefix + "-b", prefix + "-external"
    networks, containers = [], []
    probe_number = 0

    def run(*args, check=True):
        p = subprocess.run(command + list(args), capture_output=True, text=True, timeout=20)
        if check and p.returncode:
            raise RuntimeError("network acceptance Docker command failed: " + args[0])
        return p

    def inspect(name):
        return json.loads(run("inspect", name).stdout)[0]

    def create(name, net, entrypoint, args, mounts=()):
        run("create", "--pull=never", "--name", name, "--network", net, "--user", "65532:65532",
            "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
            "--memory", "128m", "--memory-swap", "128m", "--cpus", "0.5", "--pids-limit", "64",
            "--entrypoint", entrypoint, *mounts, image, *args)
        containers.append(name)

    def address(name, net):
        raw = inspect(name)["NetworkSettings"]["Networks"][net]["IPAddress"]
        return str(ipaddress.IPv4Address(raw))

    def probe(net, destination):
        nonlocal probe_number
        probe_number += 1
        name = prefix + "-probe-" + str(probe_number)
        target = str(ipaddress.IPv4Address(destination))
        create(name, net, "/usr/bin/timeout", ["3", "/bin/bash", "-c", "exec 3<>/dev/tcp/" + target + "/4222"])
        run("start", "-a", name, check=False)
        state = inspect(name)["State"]
        if state["Status"] != "exited" or state.get("Error") or state["ExitCode"] not in (0, 1, 124):
            raise RuntimeError("network probe did not execute correctly")
        return state["ExitCode"] == 0

    def ready(net, name):
        for attempt in range(5):
            if not inspect(name)["State"]["Running"]:
                raise RuntimeError("synthetic NATS listener stopped")
            if probe(net, address(name, net)):
                return True
            time.sleep(0.1 * (attempt + 1))
        raise RuntimeError("synthetic NATS listener was not reachable")

    try:
        for name, internal in ((a, True), (b, True), (external, False)):
            run("network", "create", *(["--internal"] if internal else []), name)
            networks.append(name)
        internal_flags = [json.loads(run("network", "inspect", name).stdout)[0]["Internal"] for name in (a, b)]
        assert all(internal_flags), "tenant networks are not internal"
        assert not json.loads(run("network", "inspect", external).stdout)[0]["Internal"], "external control is not external"

        with tempfile.TemporaryDirectory(prefix=prefix + "-") as work:
            # Copy only the official fixture executable, never its credential
            # directory. The nonroot listeners need a readable/executable inode.
            binary = Path(work) / "nats-server"
            shutil.copyfile(Path(nats_binary).resolve(strict=True), binary)
            binary.chmod(0o555)
            servers = {}
            for label, net in (("a", a), ("b", b), ("external", external)):
                name = prefix + "-server-" + label
                create(name, net, "/usr/local/bin/nats-server", ["-a", "0.0.0.0", "-p", "4222"],
                       ["--mount", f"type=bind,src={binary},dst=/usr/local/bin/nats-server,readonly"])
                run("start", name)
                servers[label] = name
            same_a = ready(a, servers["a"])
            same_b = ready(b, servers["b"])
            external_control = ready(external, servers["external"])
            cross_denied = not probe(a, address(servers["b"], b))
            external_denied = not probe(a, address(servers["external"], external))
            assert cross_denied and external_denied, "tenant network allowed a forbidden destination"
            return {"same_tenant": same_a, "other_tenant_control": same_b,
                    "cross_tenant_denied": cross_denied, "external_denied": external_denied,
                    "external_control": external_control, "internal_networks": internal_flags}
    finally:
        for name in reversed(containers):
            run("rm", "-f", name, check=False)
        for name in reversed(networks):
            run("network", "rm", name, check=False)
