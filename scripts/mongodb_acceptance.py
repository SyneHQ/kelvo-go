#!/usr/bin/env python3
"""Run native MongoDB acceptance against an isolated, disposable official server.

Creates no public listener. Secrets stay in a private temporary env file and
subprocess stdin/environment. Only resources created by this run are removed.
"""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="mongo:8.0.32")
    parser.add_argument("--go", default="go")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--acceleration", action="store_true",
                        help="also verify YAML pipelines through Parquet acceleration and DuckDB")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    command = None
    for candidate in (["docker"], ["sudo", "-n", "docker"]):
        if subprocess.run(candidate + ["info", "--format", "{{.ServerVersion}}"], capture_output=True, timeout=15).returncode == 0:
            command = candidate
            break
    if command is None:
        raise RuntimeError("Docker unavailable")
    name = "kelvo-mongo-" + secrets.token_hex(5)
    network = name + "-net"
    created_network = created_container = False
    password, reader_password = secrets.token_hex(24), secrets.token_hex(24)

    def docker(*argv, check=True, input=None, timeout=30):
        result = subprocess.run(command + list(argv), input=input, capture_output=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError("MongoDB fixture Docker command failed: " + argv[0])
        return result

    def shell(script):
        return docker("exec", "-i", name, "mongosh", "--quiet", "--norc", "--file", "/dev/stdin", input=script.encode(), check=False, timeout=15)

    try:
        docker("network", "create", "--internal", network)
        created_network = True
        with tempfile.TemporaryDirectory(prefix=name + "-") as work:
            env_file = Path(work) / "mongo.env"
            env_file.write_text("MONGO_INITDB_ROOT_USERNAME=kelvo_admin\nMONGO_INITDB_ROOT_PASSWORD=" + password + "\n")
            env_file.chmod(0o600)
            docker("create", "--pull=never", "--name", name, "--network", network,
                   "--memory", "1g", "--memory-swap", "1g",
                   "--cpus", "1", "--pids-limit", "256", "--security-opt", "no-new-privileges:true",
                   "--env-file", str(env_file), "--tmpfs", "/data/db:rw,nosuid,nodev,size=256m",
                   "--tmpfs", "/data/configdb:rw,nosuid,nodev,size=16m", args.image,
                   "--wiredTigerCacheSizeGB", "0.25", "--bind_ip_all")
            created_container = True
            docker("start", name)
            auth = "const authResult = db.getSiblingDB('admin').auth('kelvo_admin', " + json.dumps(password) + "); if (!(authResult === 1 || authResult.ok === 1)) quit(3);\n"
            deadline = time.monotonic() + 90
            while True:
                probe = shell(auth + "if (db.getSiblingDB('admin').runCommand({ping:1}).ok !== 1) quit(3);\n")
                if probe.returncode == 0:
                    # The driver below waits for the final server to accept
                    # connections on its private bridge after initialization.
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("MongoDB fixture did not become ready")
                time.sleep(0.5)
            script = auth + "db.getSiblingDB('kelvo_native_test').createUser({user:'kelvo_reader',pwd:" + json.dumps(reader_password) + ",roles:[{role:'read',db:'kelvo_native_test'}]});\n"
            if shell(script).returncode:
                raise RuntimeError("MongoDB read-only fixture principal could not be created")
            details = json.loads(docker("inspect", name).stdout)[0]
            address = str(ipaddress.IPv4Address(details["NetworkSettings"]["Networks"][network]["IPAddress"]))
            environment = os.environ.copy()
            environment.update({"GOMAXPROCS": "2",
                "KELVO_TEST_MONGO_ADMIN_URI": f"mongodb://kelvo_admin:{password}@{address}:27017/?authSource=admin&directConnection=true",
                "KELVO_TEST_MONGO_READER_URI": f"mongodb://kelvo_reader:{reader_password}@{address}:27017/?authSource=kelvo_native_test&directConnection=true"})
            test = subprocess.run([args.go, "test", "-p", "2", "-count=1", "-v", "-run", "TestMongoLive(ReadOnlyAggregation|SQLSemantics)", "./internal/sources/mongodb"], cwd=root, env=environment, capture_output=True, text=True, timeout=120)
            if test.returncode:
                # Tests deliberately use sanitized fixture failures. Still redact
                # generated secrets defensively before printing bounded output.
                diagnostic = (test.stdout + test.stderr).replace(password, "<redacted>").replace(reader_password, "<redacted>")
                raise RuntimeError("MongoDB live acceptance failed:\n" + diagnostic[-6000:])
            report = {"image": args.image, "image_id": details["Image"],
                      "server_memory_bytes": details["HostConfig"]["Memory"], "published_ports": False, "internal_network": True,
                      "driver": "go.mongodb.org/mongo-driver/v2 v2.9.1",
                      "passed": ["native aggregation", "lossless heterogeneous BSON in Arrow", "row overflow rejected",
                                 "database read-only grants enforced", "write stage rejected", "real cursor paging",
                                 "cancellation after first batch", "no server cursor leak after cancellation", "SQL filter aliases order and limit",
                                 "SQL exact int64 and Decimal128 literals", "SQL LIKE regex escaping and newline semantics",
                                 "SQL three-valued NULL predicates", "SQL COUNT SUM AVG MIN MAX grouping",
                                 "SQL empty and all-null aggregate semantics", "SQL exact large-integer SUM",
                                 "SQL nonnumeric aggregate rejected", "SQL incompatible comparison types rejected"],
                      "test_output": test.stdout.strip()}
            if args.acceleration:
                accelerated = subprocess.run(
                    [args.go, "test", "-tags", "duckdb_arrow", "-p", "2", "-count=1", "-v",
                     "-run", "^TestMongoPipelineLiveAcceleration$", "./internal/acceleration"],
                    cwd=root, env=environment, capture_output=True, text=True, timeout=240)
                if accelerated.returncode or "--- PASS: TestMongoPipelineLiveAcceleration " not in accelerated.stdout:
                    diagnostic = (accelerated.stdout + accelerated.stderr).replace(password, "<redacted>").replace(reader_password, "<redacted>")
                    raise RuntimeError("MongoDB acceleration acceptance failed:\n" + diagnostic[-6000:])
                report["passed"].extend([
                    "YAML-configured read-only MongoDB pipeline refresh",
                    "exact YAML Int64 filter boundaries",
                    "filtered heterogeneous BSON bytes preserved through Parquet and DuckDB",
                    "grouped Decimal128 and Int64 values preserved through Parquet and DuckDB",
                    "empty pipeline and zero-row result retain BSON schema",
                    "$out and $merge rejection preserves the last committed snapshot",
                    "rejected write stages create no MongoDB output collection"])
                report["acceleration_test_output"] = accelerated.stdout.strip()
            encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(encoded)
            print(encoded, end="")
    finally:
        if created_container:
            docker("rm", "-f", "-v", name, check=False)
        if created_network:
            docker("network", "rm", network, check=False)


if __name__ == "__main__":
    main()
