#!/usr/bin/env python3
"""Small capacity fixture layered on cluster_fixture; run only on the test VM.

provision starts three brokers. start requires an explicit final binary and a
private tenant-sources.json manifest; it starts no benchmark queries. stop only
signals recorded process identities and their observed descendants, preserving
all source data and fixture state for inspection.
"""
import argparse
import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import socket
import ssl
import subprocess
import time
import urllib.request

import cluster_fixture as cf

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "artifacts/federation-capacity-cluster"
PUBLIC = DIR / "capacity-manifest.json"
PROCESSES = DIR / "processes.json"
LIMITS = {"max_rows": 50_000_000, "max_bytes": 1 << 30, "timeout": "120s",
          "memory_mb": 256, "threads": 2, "max_temp_mb": 4096}
SLOTS = {"a1": 1, "a2": 1, "b1": 2}
PORTS = {port: port + 10000 for port in (
    14222, 14223, 14224, 16222, 16223, 16224, 18222, 18223, 18224,
    14440, 14441, 14443, 14444, 14445)}
cf.DIR = DIR


def private_json(path, value):
    cf.write(path, json.dumps(value, indent=2) + "\n")


def read_private(path):
    path = Path(path).resolve()
    if not path.is_file() or path.stat().st_mode & 0o077:
        raise RuntimeError("fixture input must be a private regular file")
    return json.loads(path.read_text())


def configuration(value):
    if isinstance(value, list):
        return [configuration(item) for item in value]
    if isinstance(value, str):
        for old, new in PORTS.items():
            value = value.replace("127.0.0.1:" + str(old), "127.0.0.1:" + str(new))
        return value
    if not isinstance(value, dict):
        return value
    value = {key: configuration(item) for key, item in value.items()}
    if "tenant_id" in value and "workers" in value and "limits" in value:
        value.update(max_queries=16, job_ttl="5m", limits=copy.deepcopy(LIMITS))
        value["workers"] = {name: SLOTS[name] for name in value["workers"]}
    if "tenants" in value and "max_concurrent" in value:
        value.update(max_queries=32, max_concurrent=4)
    return value


def identity(pid):
    try:
        process = Path(f"/proc/{pid}")
        stat = process.joinpath("stat").read_text().rsplit(") ", 1)[1].split()
        command = process.joinpath("cmdline").read_bytes()
        if stat[0] in ("Z", "X") or not command:
            return None
        return {"pid": pid, "ppid": int(stat[1]), "started": stat[19],
                "command_sha256": hashlib.sha256(command).hexdigest()}
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None


def matches(item):
    current = identity(item["pid"])
    return bool(current and all(current[key] == item[key]
                               for key in ("pid", "started", "command_sha256")))


def process_records():
    return json.loads(PROCESSES.read_text()) if PROCESSES.exists() else []


def save_process(name, pid, **extra):
    item = identity(pid)
    if item is None:
        raise RuntimeError(f"fixture process {name} exited before registration")
    item.update(name=name, **extra)
    records = [old for old in process_records() if old["name"] != name]
    private_json(PROCESSES, records + [item])
    return item


def descendants(parent):
    known, pending = {}, {parent}
    while pending:
        following = set()
        for path in Path("/proc").iterdir():
            if not path.name.isdigit() or int(path.name) in known:
                continue
            item = identity(int(path.name))
            if item and item["ppid"] in pending:
                known[item["pid"]] = item
                following.add(item["pid"])
        pending = following
    return list(known.values())


def wait(predicate, description, timeout=30):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            result = predicate()
            if result:
                return result
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.1)
    raise RuntimeError("fixture timed out: " + description)


def check_ports(ports):
    held = []
    try:
        for port in ports:
            sock = socket.socket()
            held.append(sock)
            sock.bind(("127.0.0.1", port))
    finally:
        for sock in held:
            sock.close()


def base_environment(name):
    runtime = DIR / ("runtime-" + name)
    runtime.mkdir(mode=0o700, exist_ok=True)
    return {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": str(runtime),
            "TMPDIR": str(runtime), "GOMAXPROCS": "2"}


def manifest(**updates):
    value = json.loads(PUBLIC.read_text()) if PUBLIC.exists() else {
        "version": 1, "gateway_urls": ["https://127.0.0.1:24440", "https://127.0.0.1:24441"],
        "ca_file": str(DIR / "ca.pem"), "environment_file": str(DIR / "client-environment.json"),
        "token_envs": {"a": "KELVO_TOKEN_A", "b": "KELVO_TOKEN_B"},
        "limits": LIMITS, "total_slots": 4, "tenant_source_ids": {}, "nodes": [],
        "gateway_limits": {"max_queries": 32, "max_concurrent": 4, "max_http_requests": 32},
        "tenant_policy": {"max_queries": 16, "job_ttl": "5m", "lease_duration": "5s"}}
    value.update(updates)
    value.setdefault("gateway_limits", {"max_queries": 32, "max_concurrent": 4, "max_http_requests": 32})
    value.setdefault("tenant_policy", {"max_queries": 16, "job_ttl": "5m", "lease_duration": "5s"})
    value["processes"] = [{"name": item["name"], "pid": item["pid"], "alive": matches(item)}
                          for item in process_records()]
    PUBLIC.write_text(json.dumps(value, indent=2) + "\n")
    PUBLIC.chmod(0o644)
    return value


def provision():
    check_ports(PORTS.values())
    original_yaml, original_write = cf.yaml, cf.write

    def yaml(value, level=0):
        return original_yaml(configuration(value) if level == 0 else value, level)

    def write(path, content):
        if path.suffix in (".conf", ".json"):
            content = json.dumps(configuration(json.loads(content)), indent=2)
        original_write(path, content)

    cf.yaml, cf.write = yaml, write
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            cf.provision()
    finally:
        cf.yaml, cf.write = original_yaml, original_write
    for item in json.loads((DIR / "pids.json").read_text()):
        save_process(item["name"], item["pid"])
    environment = cf.fixture_env()
    private_json(DIR / "client-environment.json", {key: environment[key] for key in ("KELVO_TOKEN_A", "KELVO_TOKEN_B")})
    manifest(state="brokers_only", nats_version=cf.VERSION, nats_archive_sha256=cf.DIGEST)
    print(str(PUBLIC))


def source_environment(tenant):
    catalog, supplied = tenant["catalog"], tenant["environment"]
    references = {source[key] for source in catalog["sources"]
                  for key in ("dsn_env", "url_env", "username_env", "password_env", "token_env")
                  if source.get(key)}
    if not references or set(supplied) != references:
        raise RuntimeError("tenant environment must contain exactly its catalog references")
    for key in references:
        if not key.startswith("KELVO_SOURCE_") or not all(char.isupper() or char.isdigit() or char == "_" for char in key):
            raise RuntimeError("capacity source references must use KELVO_SOURCE_ names")
        if not isinstance(supplied[key], str):
            raise RuntimeError("source environment values must be strings")
    return dict(supplied)


def launch(name, command, environment, expected):
    with (DIR / (name + ".log")).open("ab") as log:
        process = subprocess.Popen(command, env=environment, stdout=log, stderr=log,
                                   start_new_session=True, cwd=DIR)

    def app():
        candidates = [identity(process.pid)] + descendants(process.pid)
        for item in candidates:
            if item is None:
                continue
            try:
                arguments = Path(f"/proc/{item['pid']}/cmdline").read_bytes().split(b"\0")
            except (FileNotFoundError, ProcessLookupError):
                continue
            if arguments[:len(expected)] == [part.encode() for part in expected]:
                return item["pid"]
        if process.poll() is not None:
            raise RuntimeError("fixture application exited; inspect its private log")
        return None

    try:
        pid = wait(app, name + " final application")
        if process.pid != pid:
            save_process(name + "-launcher", process.pid)
        return save_process(name, pid)
    except Exception:
        current = identity(process.pid)
        if current:
            save_process(name + "-launcher", process.pid)
        raise


def validate_node_environment(pid, expected):
    raw = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
    observed = dict(item.decode().split("=", 1) for item in raw if b"=" in item)
    keys = {key for key in observed if key.startswith("KELVO_")}
    wanted = {key for key in expected if key.startswith("KELVO_")}
    if keys != wanted or any(observed.get(key) != expected[key] for key in wanted):
        raise RuntimeError("node inherited missing or unauthorized fixture credentials")


def start(binary, sources_path, wrapper, sandbox=None):
    binary = str(Path(binary).resolve(strict=True))
    sandbox = Path(sandbox).resolve(strict=True) if sandbox else Path(binary).parent / "kelvo-landlock"
    if not sandbox.is_file() or not os.access(sandbox, os.X_OK) or sandbox.stat().st_mode & 0o022:
        raise RuntimeError("the paired sandbox launcher must be executable and not writable by other users")
    if any(matches(item) and not item["name"].startswith("nats-") for item in process_records()):
        raise RuntimeError("capacity applications already running")
    brokers = [item for item in process_records() if item["name"].startswith("nats-")]
    if len(brokers) != 3 or not all(matches(item) for item in brokers):
        raise RuntimeError("provisioned capacity brokers must be running")
    check_ports([24440, 24441, 24443, 24444, 24445])
    sources = read_private(sources_path)
    if sources.get("version") != 1 or set(sources.get("tenants", {})) != {"a", "b"}:
        raise RuntimeError("source manifest must explicitly select tenants a and b")
    wrapper = wrapper or sources.get("trust_wrapper")
    fixture_environment = cf.fixture_env()
    node_environments, source_ids = {}, {}
    for tenant, selected in sources["tenants"].items():
        selected_environment = source_environment(selected)
        catalog = copy.deepcopy(selected["catalog"])
        if any(source["id"] == "tenant_marker" for source in catalog["sources"]):
            raise RuntimeError("tenant_marker is reserved for the synthetic identity fixture")
        marker_path = DIR / ("tenant-" + tenant + "-marker.csv")
        cf.write(marker_path, "tenant\n" + tenant + "\n")
        catalog["sources"].append({"id": "tenant_marker", "type": "csv", "path": str(marker_path)})
        source_ids[tenant] = [source["id"] for source in catalog["sources"]]
        for name in ("a1", "a2") if tenant == "a" else ("b1",):
            cf.write(DIR / (name + "-catalog.yml"), cf.yaml(catalog))
            node_config = DIR / (name + ".yml")
            lines = node_config.read_text().splitlines()
            if sum(line.startswith("sandbox_path:") for line in lines) != 1:
                raise RuntimeError("fixture node configuration has no unique sandbox path")
            cf.write(node_config, "\n".join("sandbox_path: " + json.dumps(str(sandbox)) if line.startswith("sandbox_path:") else line for line in lines) + "\n")
            environment = base_environment(name)
            environment.update(selected_environment)
            key = "KELVO_NATS_" + name.upper()
            environment[key] = fixture_environment[key]
            node_environments[name] = environment
            private_json(DIR / (name + "-environment.json"), environment)
    init_environment = base_environment("init")
    init_environment.update({key: fixture_environment[key] for key in ("KELVO_NATS_AADMIN", "KELVO_NATS_BADMIN")})
    with (DIR / "init.log").open("ab") as log:
        subprocess.run([binary, "cluster-init", "--config", str(DIR / "init.yml")],
                       env=init_environment, stdout=log, stderr=log, check=True, timeout=45)
    nodes = []
    node_context = ssl.create_default_context(cafile=str(DIR / "ca.pem"))
    node_context.load_cert_chain(str(DIR / "gateway.pem"), str(DIR / "gateway.key"))
    for name, environment in node_environments.items():
        command = [binary, "node", "--config", str(DIR / (name + ".yml"))]
        prefix = [str(Path(wrapper).resolve(strict=True)), "--env-file", str(DIR / (name + "-environment.json"))] if wrapper else []
        item = launch(name, prefix + command, environment, command)
        validate_node_environment(item["pid"], environment)

        def node_ready():
            port = {"a1": 24443, "a2": 24444, "b1": 24445}[name]
            with urllib.request.urlopen(f"https://127.0.0.1:{port}/health", context=node_context, timeout=2) as response:
                return response.status == 200

        wait(node_ready, name + " mutual TLS health")
        nodes.append({"name": name, "pid": item["pid"], "tenant": name[0], "slots": SLOTS[name],
                      "limits": LIMITS, "sources": source_ids[name[0]],
                      "environment_names": sorted(key for key in environment if key.startswith("KELVO_"))})
    gateway_environment = base_environment("gateway")
    gateway_environment.update({key: fixture_environment[key] for key in (
        "KELVO_NATS_AGATEWAY", "KELVO_NATS_BGATEWAY", "KELVO_TOKEN_A", "KELVO_TOKEN_B")})
    context = ssl.create_default_context(cafile=str(DIR / "ca.pem"))
    for number in (1, 2):
        name = "gateway" + str(number)
        command = [binary, "gateway", "--config", str(DIR / (name + ".yml"))]
        launch(name, command, gateway_environment, command)
        url = f"https://127.0.0.1:{24439 + number}/ready"

        def ready():
            with urllib.request.urlopen(url, context=context, timeout=2) as response:
                return response.status == 200

        wait(ready, name + " readiness")
    digest = hashlib.sha256()
    with Path(binary).open("rb") as executable:
        for chunk in iter(lambda: executable.read(1 << 20), b""):
            digest.update(chunk)
    manifest(state="ready", binary=binary, binary_sha256=digest.hexdigest(), nodes=nodes,
             sandbox_binary=str(sandbox), sandbox_sha256=hashlib.sha256(sandbox.read_bytes()).hexdigest(),
             tenant_source_ids=source_ids, source_provenance=sources.get("provenance_path"),
             synthetic_tenant_marker={"source_id": "tenant_marker", "column": "tenant", "expected": {"a": "a", "b": "b"}},
             node_trust_wrapper=str(Path(wrapper).resolve()) if wrapper else None)
    print(str(PUBLIC))


def stop(apps_only=False):
    if not DIR.exists():
        print("Capacity fixture has not been provisioned")
        return
    records = [item for item in process_records() if matches(item)
               and (not apps_only or not item["name"].startswith("nats-"))]
    children = {item["pid"]: item for parent in records for item in descendants(parent["pid"])}
    owned = {item["pid"]: item for item in records}
    owned.update(children)
    for sig, duration in ((signal.SIGTERM, 10), (signal.SIGKILL, 3)):
        for item in owned.values():
            if matches(item):
                try:
                    os.kill(item["pid"], sig)
                except (ProcessLookupError, PermissionError):
                    pass
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline and any(matches(item) for item in owned.values()):
            time.sleep(0.1)
    remaining = [item["pid"] for item in owned.values() if matches(item)]
    manifest(state="stop_failed" if remaining else "brokers_only" if apps_only else "stopped", nodes=[])
    if remaining:
        raise RuntimeError("owned fixture processes remain after stop: " + str(remaining))
    print("Owned capacity fixture applications stopped; brokers and data preserved" if apps_only else "Owned capacity fixture processes stopped; data preserved")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("provision", "start", "status", "stop", "stop-apps"))
    parser.add_argument("--binary")
    parser.add_argument("--source-manifest")
    parser.add_argument("--node-wrapper")
    parser.add_argument("--sandbox")
    args = parser.parse_args()
    if os.uname().sysname != "Linux":
        parser.error("run this fixture on the designated Linux VM")
    if args.action == "provision":
        provision()
    elif args.action == "start":
        if not args.binary or not args.source_manifest:
            parser.error("start requires --binary and --source-manifest")
        start(args.binary, args.source_manifest, args.node_wrapper, args.sandbox)
    elif args.action in ("stop", "stop-apps"):
        stop(apps_only=args.action == "stop-apps")
    else:
        print(json.dumps(manifest(), indent=2))


if __name__ == "__main__":
    main()
