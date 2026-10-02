#!/usr/bin/env python3
"""Exercise real Kelvo processes against scripts/cluster_fixture.py's broker.

Requires Linux Landlock ABI >=3, built binaries and pyarrow. Never writes private
configuration into evidence. On failure, logs stay in the ignored fixture dir.

--result-compression lz4_frame runs a separate, small compression acceptance on
an idle, freshly provisioned fixture. It preserves original configuration files.
"""
import argparse
import concurrent.futures
import ctypes
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import signal
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / "artifacts/cluster-private"
RUN_DIR = DIR
ENV = dict(os.environ, **json.loads((DIR / "environment.json").read_text()))
BIN = ROOT / "bin/kelvo"
CTX = ssl.create_default_context(cafile=str(DIR / "ca.pem"))
CTX.minimum_version = ssl.TLSVersion.TLSv1_3
PROCESS = {}
CHECKS = []


def record(name, **data):
    CHECKS.append(dict(test=name, passed=True, **data))
    print(name + ": passed", flush=True)


def start(name, command):
    with open(RUN_DIR / (name + ".log"), "ab") as log:
        proc = subprocess.Popen(command, env=ENV, stdout=log, stderr=log, start_new_session=True)
    PROCESS[name] = proc
    return proc


def kill(name):
    proc = PROCESS[name]
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=5)


def call(path, tenant="a", gateway=1, body=None, headers=None, timeout=15):
    hdr = {"Authorization": "Bearer " + ENV["KELVO_TOKEN_" + tenant.upper()]}
    hdr.update(headers or {})
    data = None if body is None else json.dumps(body).encode()
    if data is not None:
        hdr["Content-Type"] = "application/json"
    req = urllib.request.Request(f"https://127.0.0.1:{14439+gateway}" + path, data=data, headers=hdr)
    try:
        with urllib.request.urlopen(req, context=CTX, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as err:
        return err.code, err.read()


def submit(sql, tenant="a", gateway=1, sources=None):
    status, body = call("/v1/queries", tenant, gateway, {"sql": sql, "mode": "federated", "sources": sources or []})
    assert status == 201, (status, body)
    return json.loads(body)["id"]


def state(id, tenant="a", gateway=1):
    status, body = call("/v1/queries/" + id, tenant, gateway)
    assert status == 200, (status, body)
    return json.loads(body)["state"]


def wait(fn, description, timeout=15):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            value = fn()
            if value:
                return value
        except (OSError, urllib.error.URLError, http.client.IncompleteRead):
            pass
        time.sleep(0.05)
    raise AssertionError("Timed out: " + description)


def result(id, tenant="a", gateway=1):
    code, body = call("/v1/queries/" + id + "/results", tenant, gateway)
    assert code == 200, (code, body[:200])
    assert body[-8:] == b"\xff\xff\xff\xff\0\0\0\0", "missing Arrow EOS"
    return pa.ipc.open_stream(body).read_all(), len(body)


def broker_alive(item):
    try:
        command = Path(f"/proc/{item['pid']}/cmdline").read_bytes().split(b"\0")
    except FileNotFoundError:
        return False
    expected = [str(DIR / "nats-server"), "-c", str(DIR / (item["name"] + ".conf"))]
    return command[:3] == [part.encode() for part in expected]


def broker_configuration(item):
    return json.loads((DIR / (item["name"] + ".conf")).read_text())


def broker_ports_closed(item):
    config = broker_configuration(item)
    for address in (config["listen"], config["http"], config["cluster"]["listen"]):
        host, port = address.rsplit(":", 1)
        try:
            with socket.create_connection((host, int(port)), timeout=0.1):
                return False
        except ConnectionRefusedError:
            pass
    return True


def broker_monitor(item, path):
    config = broker_configuration(item)
    with urllib.request.urlopen("http://" + config["http"] + path, timeout=1) as response:
        return json.load(response)


def broker_healthy(item):
    return broker_alive(item) and broker_monitor(item, "/healthz").get("status") == "ok"


def broker_cluster_ready(items):
    # Only a Raft leader has authoritative follower-current information.
    # Followers can report other followers as non-current even in a healthy cluster.
    names = {broker_configuration(item)["server_name"] for item in items}
    metadata_leaders, streams, ready_streams = set(), set(), set()
    for item in items:
        if not broker_healthy(item):
            return False
        name = broker_configuration(item)["server_name"]
        data = broker_monitor(item, "/jsz?accounts=true&streams=true&raft=true")
        meta = data.get("meta_cluster", {})
        if meta.get("leader") not in names or meta.get("cluster_size") != len(names):
            return False
        if meta.get("quorum_needed") != len(names) // 2 + 1:
            return False

        def current(cluster):
            replicas = cluster.get("replicas", [])
            return ({replica.get("name") for replica in replicas} == names - {name}
                    and all(replica.get("current") and not replica.get("offline", False) for replica in replicas))

        if meta["leader"] == name:
            if not current(meta):
                return False
            metadata_leaders.add(name)
        for account in data.get("account_details", []):
            for stream in account.get("stream_detail", []):
                key = (account["name"], stream["name"])
                streams.add(key)
                cluster = stream.get("cluster", {})
                if cluster.get("leader") == name and current(cluster):
                    ready_streams.add(key)
    return len(metadata_leaders) == 1 and streams == ready_streams


def compression_configurations():
    """Copy only the known fixture YAML shape; never rewrite active policies."""
    for port in (14440, 14441, 14443, 14444, 14445):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                raise AssertionError("compression acceptance requires idle gateway and node ports")
        except ConnectionRefusedError:
            pass
    prepared = {}
    expected_limits = {"init": 2, "gateway1": 2, "gateway2": 2, "a1": 1, "a2": 1, "b1": 1}
    for name, expected in expected_limits.items():
        text = (DIR / (name + ".yml")).read_text()
        assert not re.search(r"(?m)^\s*result_compression:", text), "fixture already has a result compression policy"
        # cluster_fixture.py emits each policy's limits as a block mapping. All
        # other values and absolute resource references remain byte-for-byte.
        updated, count = re.subn(r"(?m)^( +)limits:\n", lambda match:
            match.group(0) + match.group(1) + '  result_compression: "lz4_frame"\n', text)
        assert count == expected, "fixture policy layout differs from the known template"
        prepared[name] = updated
    directory = Path(tempfile.mkdtemp(prefix="compression-acceptance-", dir=DIR))
    for name, text in prepared.items():
        fd = os.open(directory / (name + ".yml"), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as output:
            output.write(text)
    return directory


def compressed_result_acceptance():
    rows = 16384
    label = "Kelvo α" * 16
    sql = ("SELECT i::BIGINT AS id, CASE WHEN i % 7 = 0 THEN NULL "
           "ELSE repeat('Kelvo α', 16) END::VARCHAR AS label "
           f"FROM range({rows}) t(i) ORDER BY i")
    identifier = submit(sql)
    code, body = call("/v1/queries/" + identifier + "/results", gateway=2)
    assert code == 200, "compressed cluster result request failed"
    assert body[-8:] == b"\xff\xff\xff\xff\0\0\0\0", "compressed result is missing Arrow EOS"
    reader = pa.ipc.open_stream(body)
    batches = list(reader)
    assert len(batches) > 1, "compression fixture did not exercise multiple Arrow batches"
    table = pa.Table.from_batches(batches, schema=reader.schema)
    assert table.column_names == ["id", "label"] and table.num_rows == rows
    assert table.column("id").combine_chunks().equals(pa.array(range(rows), type=pa.int64())), "compressed integer values changed"
    expected_labels = pa.array([None if i % 7 == 0 else label for i in range(rows)], type=pa.string())
    assert table.column("label").combine_chunks().equals(expected_labels), "compressed Unicode or NULL values changed"

    code, status_body = call("/v1/queries/" + identifier, gateway=1)
    assert code == 200, "compressed query status unavailable"
    status = json.loads(status_body)
    stats = status["stats"]
    assert status["state"] == "succeeded", "compressed query did not commit success"
    assert stats["backend"] == "duckdb" and not stats.get("federation"), "compression fixture used an unexpected source backend"
    assert stats["rows"] == rows and stats["batches"] == len(batches), "compressed row or batch accounting differs"
    assert stats["wire_bytes"] == len(body), "gateway byte count differs from the node's encoded result"
    assert call("/v1/queries/" + identifier + "/results")[0] == 409, "compressed results were replayable"

    # Compare with an explicitly uncompressed encoding of these exact decoded
    # batches. This is a compression check, not a cross-encoder byte identity test.
    uncompressed = pa.BufferOutputStream()
    with pa.ipc.new_stream(uncompressed, reader.schema, options=pa.ipc.IpcWriteOptions(compression=None)) as writer:
        for batch in batches:
            writer.write_batch(batch)
    uncompressed_bytes = uncompressed.getvalue().size
    assert len(body) * 2 < uncompressed_bytes, "repetitive result did not meaningfully compress"
    record("cluster_lz4_multi_batch_values_and_completion", result_compression="lz4_frame",
           backend=stats["backend"], rows=rows, batches=len(batches),
           node_wire_bytes=stats["wire_bytes"], received_bytes=len(body),
           received_sha256=hashlib.sha256(body).hexdigest(), arrow_eos=True,
           pyarrow_uncompressed_bytes=uncompressed_bytes,
           note="Real DuckDB range query; exact decoded integers, Unicode and NULLs; node/client byte counts agree. Byte identity through the gateway is covered separately by the Go relay regression.")


def main(result_compression="none"):
    global RUN_DIR
    abi = ctypes.CDLL(None).syscall(444, None, 0, 1)
    assert abi >= 3, "Landlock ABI 3 or later is required"
    config_dir = DIR
    if result_compression == "lz4_frame":
        config_dir = RUN_DIR = compression_configurations()
    # A previous fault test may have stopped a broker. Preserve its disk state
    # and restart only the fixture's own recorded processes.
    broker_pids = json.loads((DIR / "pids.json").read_text())
    for item in broker_pids:
        if not broker_alive(item):
            wait(lambda: broker_ports_closed(item), item["name"] + " sockets closed")
            p = start(item["name"], [str(DIR / "nats-server"), "-c", str(DIR / (item["name"] + ".conf"))])
            item["pid"] = p.pid
            PROCESS.pop(item["name"])
    (DIR / "pids.json").write_text(json.dumps(broker_pids))
    wait(lambda: broker_cluster_ready(broker_pids), "three current broker replicas", timeout=30)
    subprocess.run([str(BIN), "cluster-init", "--config", str(config_dir / "init.yml")], env=ENV, check=True)
    wait(lambda: broker_cluster_ready(broker_pids), "initialized stream replicas", timeout=30)
    for name in ("a1", "a2", "b1"):
        start(name, [str(BIN), "node", "--config", str(config_dir / (name + ".yml"))])
    for number in (1, 2):
        name = "gateway" + str(number)
        start(name, [str(BIN), "gateway", "--config", str(config_dir / (name + ".yml"))])
        wait(lambda: call("/ready", gateway=number)[0] == 200, name + " readiness")
    time.sleep(1)
    assert all(p.poll() is None for p in PROCESS.values()), "a cluster process exited"
    record("startup", landlock_abi=abi, gateways=2, workers=3, tenants=2, broker_replicas=3)
    if result_compression == "lz4_frame":
        compressed_result_acceptance()
        return

    for tenant in ("a", "b"):
        id = submit("SELECT label, sum(id) AS n FROM sample GROUP BY label", tenant, sources=["sample"])
        table, _ = result(id, tenant, gateway=2)
        assert table.to_pylist() == [{"label": tenant, "n": 3}]
        assert state(id, tenant, 1) == "succeeded"
        assert call("/v1/queries/" + id, "b" if tenant == "a" else "a")[0] == 404
        assert call("/v1/queries/" + id, "b" if tenant == "a" else "a", headers={"X-Kelvo-Tenant": tenant})[0] == 404
        assert call("/v1/queries/" + id + "/results", tenant)[0] == 409
    record("cross_gateway_arrow_and_tenant_isolation")

    id = submit("SELECT 42 AS n")
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        responses = list(pool.map(lambda g: call("/v1/queries/" + id + "/results", gateway=g), [1, 2]))
    assert sorted(code for code, _ in responses) == [200, 409]
    record("atomic_single_consumer_claim")

    client = ssl.create_default_context(cafile=str(DIR / "ca.pem"))
    for identity in (None, "wrong"):
        if identity:
            client.load_cert_chain(str(DIR / (identity + ".pem")), str(DIR / (identity + ".key")))
        try:
            urllib.request.urlopen("https://127.0.0.1:14443/health", context=client, timeout=3)
        except (OSError, urllib.error.URLError):
            continue
        raise AssertionError("worker accepted missing or incorrect gateway mTLS identity")
    record("worker_mutual_tls_identity")

    # A gateway is an ephemeral HTTP frontend; its exit must not destroy handles.
    id = submit("SELECT 99 AS n")
    kill("gateway1")
    table, _ = result(id, gateway=2)
    assert table.to_pylist() == [{"n": 99}]
    start("gateway1", [str(BIN), "gateway", "--config", str(DIR / "gateway1.yml")])
    wait(lambda: call("/ready")[0] == 200, "replacement gateway")
    record("gateway_restart_preserves_handle")

    # Stop one broker and verify a three-replica majority can still commit work.
    pids = json.loads((DIR / "pids.json").read_text())
    broker = next(p for p in pids if p["name"] == "nats-2")
    assert broker_alive(broker), "fixture broker identity changed before fault injection"
    os.kill(broker["pid"], signal.SIGTERM)
    wait(lambda: not broker_alive(broker) and broker_ports_closed(broker), "stopped broker and released sockets")
    try:
        wait(lambda: call("/ready")[0] == 200, "quorum readiness")
        id = submit("SELECT 123 AS n")
        table, _ = result(id, gateway=2)
        assert table.to_pylist() == [{"n": 123}]
    finally:
        broker_proc = start("nats-2", [str(DIR / "nats-server"), "-c", str(DIR / "nats-2.conf")])
        PROCESS.pop("nats-2")  # This broker belongs to the fixture lifecycle.
        broker["pid"] = broker_proc.pid
        (DIR / "pids.json").write_text(json.dumps(pids))
        wait(lambda: broker_healthy(broker), "replacement broker health", timeout=30)
        wait(lambda: broker_cluster_ready(pids), "replacement broker replica recovery", timeout=30)
    id = submit("SELECT 124 AS n")
    table, _ = result(id, gateway=2)
    assert table.to_pylist() == [{"n": 124}]
    record("one_broker_loss_keeps_quorum", stopped_before_restart=True, recovered_current_replicas=3)

    id = submit("SELECT sum(i) FROM range(1000000000000) t(i)")
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        future = pool.submit(call, "/v1/queries/" + id + "/results")
        wait(lambda: state(id) == "running", "running query")
        code, _ = call("/v1/queries/" + id + "/cancel", gateway=2, body={})
        assert code == 200
        try:
            code, data = future.result(timeout=10)
            assert code != 200 or data[-8:] != b"\xff\xff\xff\xff\0\0\0\0"
        except (OSError, urllib.error.URLError, http.client.IncompleteRead):
            pass
    assert state(id) == "cancelled"
    record("cross_gateway_cancellation")

    # Broker contains slots/IDs/SQL, never a result stream. Transfer a million
    # rows over worker mTLS and check correctness at the client.
    start_time = time.monotonic()
    id = submit("SELECT i::BIGINT AS id FROM range(1000000) t(i)")
    table, size = result(id, gateway=2)
    elapsed = time.monotonic() - start_time
    assert table.num_rows == 1000000
    assert table.column("id")[999999].as_py() == 999999
    record("million_row_arrow", rows=table.num_rows, wire_bytes=size, seconds=round(elapsed, 4), rows_per_second=round(table.num_rows / elapsed), note="One loopback trial including submit, subprocess startup and Python decoding; not a production benchmark")

    # Tenant B has one worker: loss after assignment must fail, never requeue.
    id = submit("SELECT 1", "b")
    wait(lambda: state(id, "b") == "assigned", "tenant B assignment")
    kill("b1")
    wait(lambda: state(id, "b") == "failed", "lost worker terminal state", timeout=12)
    start("b1", [str(BIN), "node", "--config", str(DIR / "b1.yml")])
    time.sleep(1)
    assert PROCESS["b1"].poll() is None
    assert state(id, "b") == "failed"
    assert call("/v1/queries/" + id + "/results", "b")[0] == 409
    record("worker_loss_no_execution_replay")

    # Disconnect a worker after actual Arrow bytes have crossed the gateway.
    # The remaining body must fail transport completion and have no final EOS.
    id = submit("SELECT i::BIGINT AS id FROM range(10000000) t(i)", "b")
    request = urllib.request.Request("https://127.0.0.1:14440/v1/queries/" + id + "/results", headers={"Authorization": "Bearer " + ENV["KELVO_TOKEN_B"]})
    with urllib.request.urlopen(request, context=CTX, timeout=15) as response:
        first = response.read(64)
        assert first, "worker failed before first result bytes"
        kill("b1")
        try:
            response.read()
        except (OSError, http.client.IncompleteRead):
            pass
        else:
            raise AssertionError("worker death produced an apparently complete HTTP stream")
    wait(lambda: state(id, "b") == "failed", "partial stream failed state")
    assert call("/v1/queries/" + id + "/results", "b")[0] == 409
    record("partial_arrow_worker_death_has_no_success_or_replay")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-compression", choices=("none", "lz4_frame"), default="none")
    args = parser.parse_args()
    outcome = {"passed": False, "checks": CHECKS}
    if args.result_compression != "none":
        outcome["result_compression"] = args.result_compression
    try:
        main(args.result_compression)
        outcome["passed"] = True
    finally:
        for name, proc in PROCESS.items():
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
        for proc in PROCESS.values():
            try:
                proc.wait(timeout=12)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=3)
        filename = "cluster-acceptance.json" if args.result_compression == "none" else "cluster-compression-acceptance.json"
        evidence = ROOT / "docs/evidence" / filename
        evidence.write_text(json.dumps(outcome, indent=2) + "\n")
