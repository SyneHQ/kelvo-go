#!/usr/bin/env python3
"""Disposable, loopback-only JetStream fixture. Run on a Linux test VM.

Downloads a checksum-pinned official NATS release. Generated credentials, keys,
broker state, logs and process IDs stay under ignored artifacts/cluster-private.
This host-process fixture tests the protocol; it is not a tenant deployment.
"""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import subprocess
import tarfile
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "artifacts/cluster-private"
VERSION = "2.15.0"
DIGEST = "5d2c51caca950333aba84911df7d377f826f3a59ec36061c6539105084f65c92"
RELEASES = {
    "2.14.7": "e5c20b1cb2c0566b54c544312e91e011f9e130c5c80f16a14f4cf28ef30b8be2",
    VERSION: DIGEST,
}
BROKER_NAMES = {f"nats-{i}": f"kelvo-test-{i}" for i in range(3)}
READINESS_TIMEOUT = 30.0
MONITOR_TIMEOUT = 1.0
MONITOR_MAX_BYTES = 65536
BROKER_TERM_TIMEOUT = 3.0
BROKER_KILL_TIMEOUT = 2.0


class FixtureReadinessError(RuntimeError):
    pass


class FixtureCleanupError(RuntimeError):
    pass


class MonitorDeadline(Exception):
    pass


def broker_alive(item):
    """A recorded PID must still be our exact broker, not a reused PID."""
    try:
        command = Path(f"/proc/{item['pid']}/cmdline").read_bytes().split(b"\0")
    except OSError:
        return False
    expected = [str(DIR / "nats-server"), "-c", str(DIR / (item["name"] + ".conf"))]
    return command == [part.encode() for part in expected] + [b""]


def broker_monitor(port, path, timeout):
    """Bound the entire localhost HTTP request, including headers and JSON."""
    # This Linux fixture runs on the main thread. A socket timeout alone resets
    # on each read, so it cannot enforce our total request/readiness deadline.
    if any(signal.getitimer(signal.ITIMER_REAL)):
        raise FixtureReadinessError("fixture monitor cannot replace an active alarm")
    previous_handler = signal.getsignal(signal.SIGALRM)

    def expired(signum, frame):
        raise MonitorDeadline()

    timeout = min(MONITOR_TIMEOUT, timeout)
    if timeout <= 0:
        raise MonitorDeadline()
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    signal.signal(signal.SIGALRM, expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, timeout)
        connection.request("GET", path)
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError("monitor HTTP status")
        body = response.read(MONITOR_MAX_BYTES + 1)
        if len(body) > MONITOR_MAX_BYTES:
            raise ValueError("monitor response size")
        data = json.loads(body)
        if not isinstance(data, dict):
            raise ValueError("monitor response shape")
        return data
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        connection.close()


def metadata_readiness(reports, expected_versions=None):
    """Read-only monitor gate; callers must separately prove broker writes.

    Compatibility fixtures must declare an exact supported version per peer.
    2.14.7 lacks quorum_needed; its explicit policy still requires a healthy
    three-member consensus with both current replicas, followed by real writes
    in the acceptance harness. Missing fields never imply a current version.
    """
    names = set(BROKER_NAMES.values())
    if expected_versions is None:
        expected_versions = dict.fromkeys(names, VERSION)
    if (not isinstance(expected_versions, dict) or set(expected_versions) != names
            or not all(isinstance(v, str) and v in RELEASES for v in expected_versions.values())):
        return "unsupported_expected_versions"
    if set(reports) != names:
        return "incomplete_observation"
    leaders, server_ids = set(), set()
    for name, report in reports.items():
        server, health, jetstream = (report[key] for key in ("server", "health", "jetstream"))
        version = server.get("version")
        if version != expected_versions[name]:
            return "monitor_version"
        server_id = server.get("server_id")
        if (server.get("server_name") != name or not isinstance(server_id, str)
                or not server_id or jetstream.get("server_id") != server_id):
            return "monitor_identity"
        server_ids.add(server_id)
        if health.get("status") != "ok" or jetstream.get("disabled", False) is not False:
            return "metadata_health"
        meta = jetstream.get("meta_cluster")
        if (not isinstance(meta, dict) or meta.get("name") != "kelvo-test"
                or type(meta.get("cluster_size")) is not int or meta["cluster_size"] != 3
                or meta.get("rescue", False) is not False):
            return "metadata_membership"
        if version == VERSION and (type(meta.get("quorum_needed")) is not int or meta["quorum_needed"] != 2):
            return "metadata_membership"
        leader = meta.get("leader")
        if not isinstance(leader, str) or leader not in names:
            return "metadata_leader"
        leaders.add(leader)
    if len(server_ids) != 3:
        return "monitor_identity"
    if len(leaders) != 1:
        return "metadata_leader_disagreement"
    leader = leaders.pop()
    replicas = reports[leader]["jetstream"]["meta_cluster"].get("replicas")
    if (not isinstance(replicas, list) or len(replicas) != 2
            or not all(isinstance(peer, dict) and isinstance(peer.get("name"), str) for peer in replicas)
            or {peer["name"] for peer in replicas} != names - {leader}
            or not all(peer.get("current") is True and peer.get("offline", False) is False for peer in replicas)):
        return "metadata_replicas"
    # Followers can report other followers as non-current. Only the agreed
    # leader's view establishes that both expected metadata replicas are ready.
    return "ready"


def wait_for_brokers(items, timeout=READINESS_TIMEOUT, expected_versions=None):
    """Wait for the fixture's three owned brokers without retrying store work."""
    deadline = time.monotonic() + min(READINESS_TIMEOUT, max(0.0, timeout))
    if (not isinstance(items, list) or len(items) != 3
            or not all(isinstance(item, dict) and isinstance(item.get("name"), str)
                       and type(item.get("pid")) is int and item["pid"] > 0 for item in items)
            or {item["name"] for item in items} != set(BROKER_NAMES)
            or len({item["pid"] for item in items}) != 3):
        raise FixtureReadinessError("fixture broker inventory is invalid")
    ports = {}
    for item in items:
        try:
            config = json.loads((DIR / (item["name"] + ".conf")).read_text())
            host, port = config["http"].rsplit(":", 1)
            if (host != "127.0.0.1" or not port.isdecimal() or not 1 <= int(port) <= 65535
                    or config["server_name"] != BROKER_NAMES[item["name"]]
                    or config["cluster"]["name"] != "kelvo-test"):
                raise ValueError()
            ports[item["name"]] = int(port)
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            raise FixtureReadinessError("fixture broker monitor configuration is invalid") from None
    if len(set(ports.values())) != 3:
        raise FixtureReadinessError("fixture broker monitor ports are not distinct")

    states = {item["name"]: "pending" for item in items}
    reason = "not_observed"
    while time.monotonic() < deadline:
        reports = {}
        for item in items:
            if not broker_alive(item):
                raise FixtureReadinessError("fixture broker process is dead or unowned: " + item["name"])
            report = {}
            for key, path in (("server", "/varz"), ("health", "/healthz?js-meta-only=true"), ("jetstream", "/jsz")):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    states[item["name"]] = "deadline"
                    break
                try:
                    report[key] = broker_monitor(ports[item["name"]], path, min(MONITOR_TIMEOUT, remaining))
                except (MonitorDeadline, TimeoutError):
                    states[item["name"]] = key + "_timeout"
                    break
                except (OSError, ValueError, http.client.HTTPException):
                    states[item["name"]] = key + "_unavailable"
                    break
            if len(report) == 3:
                reports[BROKER_NAMES[item["name"]]] = report
                meta = report["jetstream"].get("meta_cluster")
                leader = meta.get("leader") if isinstance(meta, dict) else None
                # Never emit response bodies, config, logs, IDs, or unexpected names.
                states[item["name"]] = "observed_leader=" + (leader if leader in BROKER_NAMES.values() else "missing_or_unexpected")
        reason = metadata_readiness(reports, expected_versions)
        # A broker can exit while another member's monitor calls are in flight.
        for item in items:
            if not broker_alive(item):
                raise FixtureReadinessError("fixture broker process is dead or unowned: " + item["name"])
        if reason == "ready" and time.monotonic() < deadline:
            return
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(0.1, remaining))
    raise FixtureReadinessError("fixture metadata readiness timed out: " + reason + "; " + json.dumps(states, sort_keys=True))


def write(path, data):
    path.write_text(data)
    path.chmod(0o600)


def publish_pid_records(items):
    """Keep the last complete private ownership record if publication fails."""
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=DIR, prefix=".pids-", delete=False) as stream:
            temporary = Path(stream.name)
            os.fchmod(stream.fileno(), 0o600)
            json.dump(items, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, DIR / "pids.json")
        descriptor = os.open(DIR, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def reap_started_brokers(processes):
    """Reap only retained direct children, even when no PID file was written.

    This main-thread fixture is the sole reaper of these exact Popen children;
    an unreaped child keeps its PID reserved. Never reconstruct a cleanup target
    from a PID file, discover processes, or signal a process group.
    Both grace periods are shared across the whole owned set.
    """
    for process in processes:
        try:
            process.terminate()
        except OSError:
            pass  # Still attempt wait and bounded kill below.
    pending = []
    deadline = time.monotonic() + BROKER_TERM_TIMEOUT
    for process in processes:
        try:
            process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except (OSError, subprocess.TimeoutExpired):
            pending.append(process)
    for process in pending:
        try:
            process.kill()
        except OSError:
            pass
    failed = 0
    deadline = time.monotonic() + BROKER_KILL_TIMEOUT
    for process in pending:
        try:
            process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except (OSError, subprocess.TimeoutExpired):
            failed += 1
    if failed:
        raise FixtureCleanupError(f"failed to reap {failed} owned fixture broker(s)")


def yaml(value, level=0):
    """Small emitter for fixture mappings, lists and scalar values."""
    pad = "  " * level
    if isinstance(value, dict):
        return "".join(pad + str(k) + (":\n" + yaml(v, level + 1) if isinstance(v, (dict, list)) and v else ": " + json.dumps(v) + "\n") for k, v in value.items())
    if isinstance(value, list):
        return "".join(pad + "-\n" + yaml(v, level + 1) for v in value)
    raise TypeError(value)


def cert(name, uri=None):
    key, csr, pem = [DIR / (name + suffix) for suffix in (".key", ".csr", ".pem")]
    ext = DIR / (name + ".ext")
    san = "DNS:localhost,IP:127.0.0.1" + (",URI:" + uri if uri else "")
    write(ext, "subjectAltName=" + san + "\nextendedKeyUsage=serverAuth,clientAuth\n")
    subprocess.run(["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key), "-out", str(csr), "-subj", "/CN=" + name], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["openssl", "x509", "-req", "-in", str(csr), "-CA", str(DIR / "ca.pem"), "-CAkey", str(DIR / "ca.key"), "-CAcreateserial", "-out", str(pem), "-days", "2", "-extfile", str(ext)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    key.chmod(0o600)
    return {"cert_file": str(pem), "key_file": str(key), "ca_file": str(DIR / "ca.pem")}


def fixture_env():
    path = DIR / "environment.json"
    if not path.exists():
        raise SystemExit("Fixture is not provisioned")
    return json.loads(path.read_text())


def stop():
    p = DIR / "pids.json"
    if not p.exists():
        return
    for item in json.loads(p.read_text()):
        try:
            # Only stop our own recorded processes; protect against PID reuse.
            command = Path(f"/proc/{item['pid']}/cmdline").read_bytes()
            if str(DIR).encode() in command:
                os.kill(item["pid"], signal.SIGTERM)
        except (ProcessLookupError, FileNotFoundError):
            pass
    time.sleep(1)


def provision(nats_archive=None, server_version=VERSION):
    if server_version not in RELEASES:
        raise SystemExit("Unsupported NATS fixture version")
    DIR.mkdir(parents=True, exist_ok=True)
    DIR.chmod(0o700)
    if (DIR / "manifest.json").exists() or (DIR / "pids.json").exists():
        raise SystemExit("Fixture exists; stop it and use its existing configuration or a clean checkout")
    os.umask(0o077)
    archive = DIR / "nats.tar.gz"
    if nats_archive is None:
        archive.write_bytes(urllib.request.urlopen(f"https://github.com/nats-io/nats-server/releases/download/v{server_version}/nats-server-v{server_version}-linux-amd64.tar.gz", timeout=60).read())
    else:
        source = Path(nats_archive)
        if source.is_symlink() or not source.is_file() or not 0 < source.stat().st_size <= 64 << 20:
            raise SystemExit("Cached NATS archive must be a bounded regular file")
        shutil.copyfile(source, archive)
    if hashlib.sha256(archive.read_bytes()).hexdigest() != RELEASES[server_version]:
        raise SystemExit("NATS release checksum mismatch")
    with tarfile.open(archive) as tar:
        member = tar.getmember(f"nats-server-v{server_version}-linux-amd64/nats-server")
        binary = DIR / "nats-server"
        binary.write_bytes(tar.extractfile(member).read())
        binary.chmod(0o700)
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(DIR / "ca.key"), "-out", str(DIR / "ca.pem"), "-days", "2", "-subj", "/CN=Kelvo acceptance CA",
                    "-addext", "basicConstraints=critical,CA:true",
                    "-addext", "keyUsage=critical,keyCertSign,cRLSign",
                    "-addext", "subjectKeyIdentifier=hash"],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    broker = cert("broker")
    gateway = cert("gateway", "spiffe://kelvo/gateway")
    workers = {k: cert(k, f"spiffe://kelvo/tenant/{k[0]}/worker/{k}") for k in ("a1", "a2", "b1")}
    cert("wrong", "spiffe://kelvo/untrusted")
    env, accounts, tenants, nodes = {}, {}, [], []
    for tenant, ids in (("a", ["a1", "a2"]), ("b", ["b1"])):
        policy = {"tenant_id": tenant, "max_queries": 8, "job_ttl": "40s", "lease_duration": "5s", "replicas": 3, "limits": {"max_rows": 10000000, "max_bytes": 268435456, "timeout": "8s", "memory_mb": 256, "threads": 1, "max_temp_mb": 256}, "workers": {k: 1 for k in ids}}
        names = [tenant + "admin", tenant + "gateway"] + ids
        users = []
        for name in names:
            key = "KELVO_NATS_" + name.upper()
            env[key] = secrets.token_hex(24)
            user = {"user": name, "password": env[key]}
            if not name.endswith("admin"):
                pub = ["$JS.API.INFO", "$JS.API.STREAM.INFO.*", "$JS.API.STREAM.MSG.GET.KV_KELVO_META", "$JS.API.STREAM.MSG.GET.KV_KELVO_JOBS", "$KV.KELVO_JOBS.>", "$JS.API.CONSUMER.INFO.KELVO_QUEUE.dispatch"]
                pub += ["job.ready"] if name.endswith("gateway") else ["$JS.API.CONSUMER.MSG.NEXT.KELVO_QUEUE.dispatch", "$JS.ACK.>"]
                if not name.endswith("gateway"):
                    pub += ["$JS.API.STREAM.MSG.GET.KV_KELVO_ACCEL_STATUS", "$KV.KELVO_ACCEL_STATUS.>", "$JS.API.STREAM.MSG.GET.KV_KELVO_SOURCE_QUOTAS", "$KV.KELVO_SOURCE_QUOTAS.>", "acceleration.refresh", "$JS.API.CONSUMER.INFO.KELVO_ACCEL_QUEUE.refresh", "$JS.API.CONSUMER.MSG.NEXT.KELVO_ACCEL_QUEUE.refresh"]
                user["permissions"] = {"publish": {"allow": pub}, "subscribe": {"allow": ["_INBOX.>"]}}
            users.append(user)
        accounts[tenant] = {"jetstream": {"max_memory": 16777216, "max_file": 67108864, "max_streams": 6, "max_consumers": 4}, "users": users}

        def nats(name):
            return {"url": "tls://127.0.0.1:14222", "username": name, "password_env": "KELVO_NATS_" + name.upper(), "ca_file": str(DIR / "ca.pem")}

        token = "KELVO_TOKEN_" + tenant.upper()
        env[token] = secrets.token_hex(32)
        endpoints = [{"id": name, "url": f"https://127.0.0.1:{14443 + len(nodes) + i}"} for i, name in enumerate(ids)]
        tenants.append({"policy": policy, "token_env": token, "nats": nats(tenant + "gateway"), "workers": endpoints})
        for name, endpoint in zip(ids, endpoints):
            catalog = DIR / (name + "-catalog.yml")
            data = DIR / (name + "-data.csv")
            write(data, "id,label\n1," + tenant + "\n2," + tenant + "\n")
            write(catalog, yaml({"sources": [{"id": "sample", "type": "csv", "path": str(data)}]}))
            node = {"listen": endpoint["url"].split("//")[1], "tls": workers[name], "nats": nats(name), "policy": policy, "worker_id": name, "catalog_file": str(catalog), "sandbox_path": str(ROOT / "bin/kelvo-landlock")}
            node["resources"] = {"max_concurrent": 2, "memory_mb": 2048, "baseline_mb": 256, "overhead_mb": 128, "scratch_mb": 2048}
            write(DIR / (name + ".yml"), yaml(node))
            nodes.append(name)
    # Store integration tests use their own account. It has only an administrator
    # credential so the tests can exercise create-only bootstrap without granting
    # those powers to a gateway or node fixture identity.
    test_user = "storetestadmin"
    test_password_env = "KELVO_NATS_STORETESTADMIN"
    env[test_password_env] = secrets.token_hex(24)
    accounts["storetest"] = {
        "jetstream": {"max_memory": 16777216, "max_file": 67108864, "max_streams": 6, "max_consumers": 4},
        "users": [{"user": test_user, "password": env[test_password_env]}],
    }
    env.update({
        "KELVO_TEST_NATS_URL": "tls://127.0.0.1:14222",
        "KELVO_TEST_NATS_CA_FILE": str(DIR / "ca.pem"),
        "KELVO_TEST_NATS_USERNAME": test_user,
        "KELVO_TEST_NATS_PASSWORD_ENV": test_password_env,
        "KELVO_TEST_NATS_REPLICAS": "3",
        "KELVO_TEST_NATS_A_USERNAME": "aadmin",
        "KELVO_TEST_NATS_A_PASSWORD_ENV": "KELVO_NATS_AADMIN",
        "KELVO_TEST_NATS_B_USERNAME": "badmin",
        "KELVO_TEST_NATS_B_PASSWORD_ENV": "KELVO_NATS_BADMIN",
    })
    base = {"listen": "127.0.0.1:14440", "tls": gateway, "worker_tls": gateway, "max_queries": 16, "max_concurrent": 3, "max_http_requests": 32, "tenants": tenants}
    write(DIR / "gateway1.yml", yaml(base))
    base["listen"] = "127.0.0.1:14441"
    write(DIR / "gateway2.yml", yaml(base))
    for tc in base["tenants"]:
        name = tc["policy"]["tenant_id"] + "admin"
        tc["nats"]["username"] = name
        tc["nats"]["password_env"] = "KELVO_NATS_" + name.upper()
    write(DIR / "init.yml", yaml(base))
    pids, started = [], []
    try:
        for i in range(3):
            routes = [f"nats-route://127.0.0.1:{16222+j}" for j in range(3) if j != i]
            cfg = {"server_name": f"kelvo-test-{i}", "listen": f"127.0.0.1:{14222+i}", "http": f"127.0.0.1:{18222+i}", "max_payload": 2 << 20, "tls": dict(broker, min_version="1.3"), "jetstream": {"store_dir": str(DIR / f"state-{i}"), "max_file_store": 536870912, "max_memory_store": 67108864}, "accounts": accounts, "cluster": {"name": "kelvo-test", "listen": f"127.0.0.1:{16222+i}", "routes": routes, "tls": dict(broker, verify=True, min_version="1.3")}}
            cfgpath = DIR / f"nats-{i}.conf"
            write(cfgpath, json.dumps(cfg, indent=2))
            with open(DIR / f"nats-{i}.log", "wb") as log:
                process = subprocess.Popen([str(DIR / "nats-server"), "-c", str(cfgpath)], stdout=log, stderr=log, start_new_session=True)
                started.append(process)
            pids.append({"name": f"nats-{i}", "pid": process.pid})
            publish_pid_records(pids)
        write(DIR / "environment.json", json.dumps(env))
        write(DIR / "manifest.json", json.dumps({"nats_version": server_version, "sha256": RELEASES[server_version], "nodes": nodes}))
        wait_for_brokers(pids, expected_versions=dict.fromkeys(BROKER_NAMES.values(), server_version))
    except BaseException as error:
        # Keep durable records for diagnostics; in-memory ownership also covers
        # failures before (or during) the first PID publication.
        reap_started_brokers(started)
        if isinstance(error, FixtureReadinessError):
            raise SystemExit(str(error)) from None
        raise
    print("Loopback fixture provisioned; private state is under artifacts/cluster-private")


def test_store(go_binary):
    """Run only cluster store tests with the private fixture environment."""
    env = dict(os.environ)
    env.update(fixture_env())
    env.setdefault("GOMAXPROCS", "2")
    subprocess.run([go_binary, "test", "./internal/cluster"], cwd=ROOT, env=env, check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["provision", "stop", "test-store"])
    parser.add_argument("--go", default="go", dest="go_binary", help="Go binary for test-store (use the VM toolchain path)")
    parser.add_argument("--nats-archive", type=Path, help="Use a cached checksum-pinned NATS archive without downloads")
    parser.add_argument("--server-version", choices=RELEASES, default=VERSION)
    args = parser.parse_args()
    if args.action == "provision":
        provision(args.nats_archive, args.server_version)
    elif args.action == "stop":
        stop()
    else:
        test_store(args.go_binary)
