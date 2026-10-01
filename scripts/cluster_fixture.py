#!/usr/bin/env python3
"""Disposable, loopback-only JetStream fixture. Run on a Linux test VM.

Downloads a checksum-pinned official NATS release. Generated credentials, keys,
broker state, logs and process IDs stay under ignored artifacts/cluster-private.
This host-process fixture tests the protocol; it is not a tenant deployment.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import tarfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "artifacts/cluster-private"
VERSION = "2.15.0"
DIGEST = "5d2c51caca950333aba84911df7d377f826f3a59ec36061c6539105084f65c92"


def write(path, data):
    path.write_text(data)
    path.chmod(0o600)


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


def provision():
    DIR.mkdir(parents=True, exist_ok=True)
    DIR.chmod(0o700)
    if (DIR / "manifest.json").exists():
        raise SystemExit("Fixture exists; stop it and use its existing configuration or a clean checkout")
    os.umask(0o077)
    archive = DIR / "nats.tar.gz"
    archive.write_bytes(urllib.request.urlopen(f"https://github.com/nats-io/nats-server/releases/download/v{VERSION}/nats-server-v{VERSION}-linux-amd64.tar.gz", timeout=60).read())
    if hashlib.sha256(archive.read_bytes()).hexdigest() != DIGEST:
        raise SystemExit("NATS release checksum mismatch")
    with tarfile.open(archive) as tar:
        member = tar.getmember(f"nats-server-v{VERSION}-linux-amd64/nats-server")
        binary = DIR / "nats-server"
        binary.write_bytes(tar.extractfile(member).read())
        binary.chmod(0o700)
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(DIR / "ca.key"), "-out", str(DIR / "ca.pem"), "-days", "2", "-subj", "/CN=Kelvo acceptance CA"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
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
                user["permissions"] = {"publish": {"allow": pub}, "subscribe": {"allow": ["_INBOX.>"]}}
            users.append(user)
        accounts[tenant] = {"jetstream": {"max_memory": 16777216, "max_file": 67108864, "max_streams": 3, "max_consumers": 4}, "users": users}

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
            write(DIR / (name + ".yml"), yaml(node))
            nodes.append(name)
    # Store integration tests use their own account. It has only an administrator
    # credential so the tests can exercise create-only bootstrap without granting
    # those powers to a gateway or node fixture identity.
    test_user = "storetestadmin"
    test_password_env = "KELVO_NATS_STORETESTADMIN"
    env[test_password_env] = secrets.token_hex(24)
    accounts["storetest"] = {
        "jetstream": {"max_memory": 16777216, "max_file": 67108864, "max_streams": 3, "max_consumers": 4},
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
    pids = []
    for i in range(3):
        routes = [f"nats-route://127.0.0.1:{16222+j}" for j in range(3) if j != i]
        cfg = {"server_name": f"kelvo-test-{i}", "listen": f"127.0.0.1:{14222+i}", "http": f"127.0.0.1:{18222+i}", "max_payload": 2 << 20, "tls": dict(broker, min_version="1.3"), "jetstream": {"store_dir": str(DIR / f"state-{i}"), "max_file_store": 536870912, "max_memory_store": 67108864}, "accounts": accounts, "cluster": {"name": "kelvo-test", "listen": f"127.0.0.1:{16222+i}", "routes": routes, "tls": dict(broker, verify=True, min_version="1.3")}}
        cfgpath = DIR / f"nats-{i}.conf"
        write(cfgpath, json.dumps(cfg, indent=2))
        log = open(DIR / f"nats-{i}.log", "wb")
        process = subprocess.Popen([str(DIR / "nats-server"), "-c", str(cfgpath)], stdout=log, stderr=log, start_new_session=True)
        pids.append({"name": f"nats-{i}", "pid": process.pid})
    write(DIR / "pids.json", json.dumps(pids))
    write(DIR / "environment.json", json.dumps(env))
    write(DIR / "manifest.json", json.dumps({"nats_version": VERSION, "sha256": DIGEST, "nodes": nodes}))
    time.sleep(3)
    for item in pids:
        os.kill(item["pid"], 0)
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
    args = parser.parse_args()
    if args.action == "provision":
        provision()
    elif args.action == "stop":
        stop()
    else:
        test_store(args.go_binary)
