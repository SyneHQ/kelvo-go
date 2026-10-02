#!/usr/bin/env python3
"""Real ClickHouse/PostgreSQL/MySQL custom federation acceptance on the test VM.

Provisioning uses cached database images and unique private fixtures. PostgreSQL
and MySQL require verified TLS. A private mount namespace supplies the fixture
CA through the ordinary system trust path, then drops to the invoking user;
neither production source configuration nor the host trust store is changed.
Fixtures remain available for capacity trials until the explicit stop action.
All generated credentials and diagnostic output stay under ignored artifacts.
"""

from __future__ import annotations

import argparse
import base64
import copy
import csv
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import pwd
import re
import secrets
import shlex
import signal
import socket
import stat
import subprocess
import sys
import threading
import time
import traceback
from urllib.parse import quote
import uuid

import pyarrow as pa


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "artifacts/federation-relational-private"
DOCKER = ["sudo", "-n", "docker"]
PYTHON = sys.executable
EOS = b"\xff\xff\xff\xff\0\0\0\0"
KINDS = ("clickhouse", "postgres", "mysql")
IDS = {"clickhouse": "ch", "postgres": "pg", "mysql": "my"}
SOURCE_ROWS = {"clickhouse": [1, 2, 3, 4], "postgres": [1, 2, 3, 5], "mysql": [1, 2, 3, 6]}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def private_directory(path):
    require(not path.is_symlink(), "private directory cannot be a symlink")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(stat.S_IMODE(path.stat().st_mode) == 0o700, "private directory mode must be 0700")
    return path


def private_write(path, data):
    require(not path.is_symlink(), "private file cannot be a symlink")
    path.write_bytes(data.encode() if isinstance(data, str) else data)
    path.chmod(0o600)


def wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


class Fixture:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        require(self.directory.is_relative_to(BASE.resolve()), "fixture must belong to this scratch checkout")
        self.manifest = json.loads((self.directory / "manifest.json").read_text())
        self.environment = json.loads((self.directory / "environment.json").read_text())

    @classmethod
    def selected(cls, argument):
        directory = argument or json.loads((BASE / "current.json").read_text())["directory"]
        return cls(directory)

    def run(self, argv, *, data=None, timeout=30, check=True):
        result = subprocess.run(argv, input=data, capture_output=True, timeout=timeout)
        if check and result.returncode:
            private_write(self.directory / "fixture-command-error.log", result.stdout + b"\n" + result.stderr)
            raise RuntimeError("owned fixture command failed")
        return result

    def docker(self, *arguments, **kwargs):
        return self.run(DOCKER + list(arguments), **kwargs)

    def admin(self, kind, sql, *, check=True, timeout=30):
        if kind == "clickhouse":
            args = ["clickhouse-client", "--multiquery"]
        elif kind == "postgres":
            args = ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", self.manifest["database"], "-At"]
        else:
            args = ["mysql", "--defaults-extra-file=/tmp/kelvo-relational-admin.cnf", "--batch", "--silent", "--skip-column-names"]
        return self.docker("exec", "-i", self.manifest["containers"][kind], *args,
                           data=sql.encode(), check=check, timeout=timeout)

    def query_value(self, kind, sql):
        return self.admin(kind, sql).stdout.decode().strip()

    def table(self, kind, name):
        return ({"name": name, "schema": "acceptance", "table": name}
                if kind == "postgres" else
                {"name": name, "database": self.manifest["database"], "table": name})

    def source(self, kind, tables=("fact",), max_rows=2_000_000, max_bytes=128 << 20):
        source = {"id": IDS[kind], "type": kind, "federation": {
            "tables": [self.table(kind, name) for name in tables],
            "max_scan_rows": max_rows, "max_scan_bytes": max_bytes}}
        if kind == "clickhouse":
            source.update(url_env="KELVO_SOURCE_REL_CH_URL", username_env="KELVO_SOURCE_REL_CH_USER",
                          password_env="KELVO_SOURCE_REL_CH_PASSWORD")
        else:
            source["dsn_env"] = "KELVO_SOURCE_REL_" + ("PG" if kind == "postgres" else "MY") + "_DSN"
        return source

    def configuration(self, directory, *, tables=None, max_rows=2_000_000, max_bytes=128 << 20):
        sources = [self.source(kind, (tables or {}).get(kind, ("fact",)), max_rows, max_bytes) for kind in KINDS]
        hidden = copy.deepcopy(sources[1])
        hidden["id"] = "unselected"
        hidden["dsn_env"] = "KELVO_SOURCE_REL_UNSELECTED_DSN"
        sources.append(hidden)
        path = directory / ("catalog-" + uuid.uuid4().hex + ".json")
        private_write(path, json.dumps({"sources": sources}, indent=2))
        return path

    def public(self):
        return {"directory": str(self.directory), "manifest": str(self.directory / "manifest.json"),
                "environment_file": str(self.directory / "environment.json"),
                "catalog_file": str(self.directory / "catalog.json"),
                "trust_wrapper": str(self.directory / "with-fixture-trust.sh"),
                "endpoints": self.manifest["endpoints"], "database": self.manifest["database"],
                "postgres_schema": "acceptance", "published_ports": False,
                "fixtures_retained": True}


def write_trust_wrapper(fixture):
    directory = fixture.directory
    helper = directory / "exec-selected-environment.py"
    private_write(helper, '''import json, os, pathlib, stat, sys
args = sys.argv[1:]
environment = dict(os.environ)
if args and args[0] == "--env-file":
    path = pathlib.Path(args[1])
    details = path.lstat()
    if not stat.S_ISREG(details.st_mode) or stat.S_IMODE(details.st_mode) != 0o600 or details.st_uid != os.getuid() or details.st_size > 1048576:
        raise SystemExit("selected environment file is not private")
    environment = json.loads(path.read_text())
    if not isinstance(environment, dict) or any(not isinstance(k, str) or not isinstance(v, str) or "=" in k or "\\0" in k + v for k, v in environment.items()):
        raise SystemExit("selected environment is invalid")
    args = args[2:]
if not args:
    raise SystemExit("a program is required")
os.execvpe(args[0], args, environment)
''')
    inner = 'set -eu; mount --bind "$1" /etc/ssl/certs/ca-certificates.crt; shift; user="$1"; shift; exec runuser -u "$user" -- "$@"'
    wrapper = "#!/bin/sh\nset -eu\nexec sudo -n unshare --mount --propagation private -- sh -c " + shlex.quote(inner)
    wrapper += " fixture-trust " + shlex.quote(str(directory / "combined-ca-bundle.pem"))
    wrapper += " " + shlex.quote(pwd.getpwuid(os.getuid()).pw_name) + " " + shlex.quote(PYTHON) + " " + shlex.quote(str(helper)) + ' "$@"\n'
    private_write(directory / "with-fixture-trust.sh", wrapper)
    (directory / "with-fixture-trust.sh").chmod(0o700)


def provision(clickhouse):
    require(sys.platform.startswith("linux"), "fixtures require the designated Linux VM")
    require(re.fullmatch(r"[A-Za-z0-9_.-]+", clickhouse), "invalid existing ClickHouse container")
    os.umask(0o077)
    directory = private_directory(private_directory(BASE) / ("run-" + uuid.uuid4().hex))
    suffix = secrets.token_hex(6)
    network = "kelvo-fed-rel-" + suffix
    database = "kelvo_rel_" + suffix
    containers = {"clickhouse": clickhouse, "postgres": network + "-pg", "mysql": network + "-my"}
    passwords = {name: secrets.token_hex(32) for name in ("admin", "postgres", "mysql", "clickhouse")}
    user = "kelvo_reader"
    manifest = {"database": database, "network": network, "containers": containers,
                "reader": user, "clickhouse_reader": database + "_reader", "endpoints": {},
                "images": {}, "created_containers": [], "owned_clickhouse_database": False,
                "owned_clickhouse_reader": False, "host_mount_namespace": os.readlink("/proc/self/ns/mnt")}
    private_write(directory / "manifest.json", json.dumps(manifest))
    private_write(directory / "environment.json", "{}")
    fixture = Fixture(directory)
    try:
        for kind, image in (("postgres", "postgres:17.6"), ("mysql", "mysql:8.4")):
            inspected = json.loads(fixture.docker("image", "inspect", image).stdout)[0]
            manifest["images"][kind] = {"tag": image, "id": inspected["Id"]}
        fixture.docker("network", "create", "--internal", network)
        network_info = json.loads(fixture.docker("network", "inspect", network).stdout)[0]
        subnet = ipaddress.ip_network(network_info["IPAM"]["Config"][0]["Subnet"])
        addresses = {"postgres": str(subnet.network_address + 10), "mysql": str(subnet.network_address + 11)}
        ch = json.loads(fixture.docker("inspect", clickhouse).stdout)[0]
        ch_addresses = [net["IPAddress"] for net in ch["NetworkSettings"]["Networks"].values() if net.get("IPAddress")]
        require(ch_addresses, "existing ClickHouse has no reachable dedicated address")
        addresses["clickhouse"] = str(ipaddress.ip_address(ch_addresses[0]))
        manifest["images"]["clickhouse"] = {"id": ch["Image"]}
        manifest["endpoints"] = {"clickhouse": "http://" + addresses["clickhouse"] + ":8123/",
                                 "postgres": addresses["postgres"] + ":5432", "mysql": addresses["mysql"] + ":3306"}
        certs = directory / "certs"
        certs.mkdir(mode=0o755)
        certs.chmod(0o755)
        fixture.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "3",
                     "-subj", "/CN=Kelvo isolated relational fixture CA", "-keyout", str(directory / "ca.key"),
                     "-out", str(certs / "ca.crt")])
        fixture.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "3",
                     "-subj", "/CN=Kelvo unrelated fixture CA", "-keyout", str(directory / "wrong-ca.key"),
                     "-out", str(certs / "wrong-ca.crt")])
        for kind in ("postgres", "mysql"):
            key, request, certificate = certs / (kind + ".key"), directory / (kind + ".csr"), certs / (kind + ".crt")
            extension = directory / (kind + ".ext")
            private_write(extension, "subjectAltName=IP:" + addresses[kind] + "\nextendedKeyUsage=serverAuth\n")
            fixture.run(["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=Kelvo fixture " + kind,
                         "-keyout", str(key), "-out", str(request)])
            fixture.run(["openssl", "x509", "-req", "-days", "3", "-in", str(request), "-CA", str(certs / "ca.crt"),
                         "-CAkey", str(directory / "ca.key"), "-CAcreateserial", "-extfile", str(extension), "-out", str(certificate)])
            key.chmod(0o600)
            fixture.run(["sudo", "-n", "chown", "999:999", str(key)])
            certificate.chmod(0o644)
            request.unlink()
            extension.unlink()
        for name in ("ca.crt", "wrong-ca.crt"):
            (certs / name).chmod(0o644)
        (directory / "ca.key").unlink()
        (directory / "wrong-ca.key").unlink()
        (certs / "ca.srl").unlink(missing_ok=True)
        bundle = directory / "combined-ca-bundle.pem"
        private_write(bundle, Path("/etc/ssl/certs/ca-certificates.crt").read_bytes() + b"\n" + (certs / "ca.crt").read_bytes())
        manifest["ca_bundle_sha256"] = digest(bundle)
        manifest["host_ca_bundle_sha256"] = digest("/etc/ssl/certs/ca-certificates.crt")
        manifest["ca_signing_keys_removed"] = True
        write_trust_wrapper(fixture)
        environment_file = directory / "database.env"
        private_write(environment_file, "POSTGRES_PASSWORD=" + passwords["admin"] + "\nPOSTGRES_DB=" + database +
                      "\nMYSQL_ROOT_PASSWORD=" + passwords["admin"] + "\nMYSQL_DATABASE=" + database + "\n")
        common = ["--network", network, "--memory", "1g", "--memory-swap", "1g", "--cpus", "1", "--pids-limit", "256",
                  "--security-opt", "no-new-privileges:true", "--env-file", str(environment_file),
                  "--mount", "type=bind,src=" + str(certs) + ",dst=/certs,readonly"]
        fixture.docker("create", "--pull=never", "--name", containers["postgres"], "--ip", addresses["postgres"],
                       *common, "postgres:17.6", "-c", "ssl=on", "-c", "ssl_cert_file=/certs/postgres.crt",
                       "-c", "ssl_key_file=/certs/postgres.key", "-c", "ssl_ca_file=/certs/ca.crt", "-c", "shared_buffers=128MB")
        manifest["created_containers"].append("postgres")
        fixture.docker("start", containers["postgres"])
        fixture.docker("create", "--pull=never", "--name", containers["mysql"], "--ip", addresses["mysql"],
                       *common, "mysql:8.4", "--require-secure-transport=ON", "--ssl-ca=/certs/ca.crt",
                       "--ssl-cert=/certs/mysql.crt", "--ssl-key=/certs/mysql.key", "--innodb-buffer-pool-size=128M")
        manifest["created_containers"].append("mysql")
        fixture.docker("start", containers["mysql"])
        admin = directory / "mysql-admin.cnf"
        private_write(admin, "[client]\nuser=root\npassword=" + passwords["admin"] + "\n")
        fixture.docker("cp", str(admin), containers["mysql"] + ":/tmp/kelvo-relational-admin.cnf")
        fixture.manifest = manifest
        deadline = time.monotonic() + 150
        while True:
            pg = fixture.docker("exec", containers["postgres"], "pg_isready", "-U", "postgres", check=False)
            my = fixture.admin("mysql", "SELECT 1", check=False)
            connected = True
            for kind, port in (("postgres", 5432), ("mysql", 3306)):
                try:
                    with socket.create_connection((addresses[kind], port), timeout=0.2):
                        pass
                except OSError:
                    connected = False
            if pg.returncode == 0 and my.returncode == 0 and connected:
                break
            require(time.monotonic() < deadline, "owned relational fixtures did not become ready")
            time.sleep(1)
        fixture.admin("postgres", "CREATE SCHEMA acceptance; CREATE ROLE " + user + " LOGIN PASSWORD " + literal(passwords["postgres"]) + "; GRANT USAGE ON SCHEMA acceptance TO " + user + ";")
        fixture.admin("mysql", "CREATE USER " + literal(user) + "@'%' IDENTIFIED BY " + literal(passwords["mysql"]) + " REQUIRE SSL; GRANT SELECT ON `" + database + "`.* TO " + literal(user) + "@'%';")
        manifest["existing_clickhouse_rows"] = int(fixture.query_value("clickhouse", "SELECT coalesce(sum(rows),0) FROM system.parts WHERE active AND database='kelvo_bench' AND table='fact_events'"))
        fixture.admin("clickhouse", "CREATE DATABASE `" + database + "`")
        manifest["owned_clickhouse_database"] = True
        fixture.admin("clickhouse", "CREATE USER `" + manifest["clickhouse_reader"] + "` IDENTIFIED WITH sha256_password BY " + literal(passwords["clickhouse"]))
        manifest["owned_clickhouse_reader"] = True
        fixture.admin("clickhouse", "GRANT SELECT ON `" + database + "`.* TO `" + manifest["clickhouse_reader"] + "`")
        environment = {
            "KELVO_SOURCE_REL_PG_DSN": "postgres://" + user + ":" + quote(passwords["postgres"]) + "@" + manifest["endpoints"]["postgres"] + "/" + database + "?sslmode=verify-full&connect_timeout=5",
            "KELVO_SOURCE_REL_MY_DSN": user + ":" + passwords["mysql"] + "@tcp(" + manifest["endpoints"]["mysql"] + ")/" + database + "?tls=true&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27&timeout=5s",
            "KELVO_SOURCE_REL_CH_URL": manifest["endpoints"]["clickhouse"],
            "KELVO_SOURCE_REL_CH_USER": manifest["clickhouse_reader"], "KELVO_SOURCE_REL_CH_PASSWORD": passwords["clickhouse"],
        }
        private_write(directory / "manifest.json", json.dumps(manifest, indent=2))
        private_write(directory / "environment.json", json.dumps(environment, indent=2))
        fixture.environment = environment
        seed(fixture)
        generated = fixture.configuration(directory)
        generated.replace(directory / "catalog.json")
        private_write(BASE / "current.json", json.dumps({"directory": str(directory)}))
        print(json.dumps(fixture.public(), indent=2))
    except BaseException:
        private_write(directory / "manifest.json", json.dumps(manifest, indent=2))
        private_write(directory / "provision-failure.log", traceback.format_exc())
        raise RuntimeError("fixture provisioning failed; private diagnostics retained") from None


def seed(fixture):
    database = fixture.manifest["database"]
    definitions = {
        "postgres": "CREATE TABLE acceptance.fact (row_id INTEGER NOT NULL, i16 SMALLINT, i64 BIGINT, u64 NUMERIC(20,0), amount NUMERIC(30,10), day DATE, wall TIMESTAMP(6), instant TIMESTAMPTZ(6), label TEXT, payload TEXT);",
        "mysql": "CREATE TABLE `" + database + "`.fact (row_id INTEGER NOT NULL, i16 SMALLINT, i64 BIGINT, u64 BIGINT UNSIGNED, amount DECIMAL(30,10), day DATE, wall DATETIME(6), instant TIMESTAMP(6), label TEXT, payload TEXT);",
        "clickhouse": "CREATE TABLE `" + database + "`.fact (row_id Int32, i16 Nullable(Int16), i64 Nullable(Int64), u64 Nullable(UInt64), amount Nullable(Decimal(30,10)), day Nullable(Date32), wall Nullable(DateTime64(6)), instant Nullable(DateTime64(6,'UTC')), label Nullable(String), payload String) ENGINE=MergeTree ORDER BY row_id;",
    }
    for kind in KINDS:
        fixture.admin(kind, definitions[kind])
        target = "acceptance.fact" if kind == "postgres" else "`" + database + "`.fact"
        temporal = "2026-10-01 07:00:00.123456+00" if kind == "postgres" else "2026-10-01 07:00:00.123456"
        rows = ["(1,-32768,-9223372036854775808,0,-12345678901234567890.0123456789,'1969-12-31','2026-10-01 12:30:00.123456'," + literal(temporal) + ",'first'," + literal("x" * 2048) + ")",
                "(2,32767,9223372036854775807,18446744073709551615,12345678901234567890.0123456789,'2026-10-02','2026-10-02 00:00:00.654321','2026-10-02 00:00:00.654321','second'," + literal("y" * 2048) + ")",
                "(3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL," + literal("z" * 2048) + ")",
                "(" + str(SOURCE_ROWS[kind][-1]) + ",5,5,5,1.2500000000,'2020-01-01','2020-01-01 00:00:00','2020-01-01 00:00:00'," + literal(IDS[kind] + "_only") + "," + literal("w" * 2048) + ")"]
        prefix = "SET time_zone='+00:00'; " if kind == "mysql" else ""
        fixture.admin(kind, prefix + "INSERT INTO " + target + " VALUES " + ",".join(rows) + ";")
        if kind == "postgres":
            fixture.admin(kind, "CREATE TABLE acceptance.hidden(marker TEXT); INSERT INTO acceptance.hidden VALUES('unregistered'); CREATE VIEW acceptance.transport AS SELECT ssl,version,cipher,current_setting('transaction_read_only') AS read_only FROM pg_stat_ssl WHERE pid=pg_backend_pid(); CREATE VIEW acceptance.slow AS SELECT g::BIGINT AS row_id, CASE WHEN pg_sleep(0.01+g*0) IS NULL THEN 1 ELSE 1 END::INTEGER AS delayed FROM generate_series(1,100000) g; GRANT SELECT ON ALL TABLES IN SCHEMA acceptance TO kelvo_reader; ALTER DEFAULT PRIVILEGES IN SCHEMA acceptance GRANT SELECT ON TABLES TO kelvo_reader;")
        elif kind == "mysql":
            fixture.admin(kind, "USE `" + database + "`; CREATE TABLE hidden(marker TEXT); INSERT INTO hidden VALUES('unregistered'); CREATE VIEW transport AS SELECT CAST(VARIABLE_VALUE AS CHAR(128)) AS cipher FROM performance_schema.session_status WHERE VARIABLE_NAME='Ssl_cipher'; CREATE TABLE scan_rows(row_id BIGINT); INSERT INTO scan_rows WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM seq WHERE n<500) SELECT n FROM seq; CREATE VIEW slow AS SELECT row_id, CAST(SLEEP(0.05) AS SIGNED) AS `delayed` FROM scan_rows;")
        else:
            fixture.admin(kind, "CREATE TABLE `" + database + "`.hidden(marker String) ENGINE=TinyLog; INSERT INTO `" + database + "`.hidden VALUES('unregistered');")


def stop_fixture(fixture):
    manifest = fixture.manifest
    if manifest.get("owned_clickhouse_reader"):
        name = manifest["clickhouse_reader"]
        fixture.admin("clickhouse", "KILL QUERY WHERE user=" + literal(name) + " SYNC; DROP USER IF EXISTS `" + name + "`;")
    if manifest.get("owned_clickhouse_database"):
        fixture.admin("clickhouse", "DROP DATABASE IF EXISTS `" + manifest["database"] + "` SYNC")
    for kind in reversed(manifest["created_containers"]):
        fixture.docker("rm", "-f", "-v", manifest["containers"][kind])
    fixture.docker("network", "rm", manifest["network"])
    require(digest("/etc/ssl/certs/ca-certificates.crt") == manifest["host_ca_bundle_sha256"], "host trust bundle changed")
    final_rows = int(fixture.query_value("clickhouse", "SELECT coalesce(sum(rows),0) FROM system.parts WHERE active AND database='kelvo_bench' AND table='fact_events'"))
    require(final_rows == manifest["existing_clickhouse_rows"], "existing ClickHouse source changed")
    for path in (fixture.directory / "certs").iterdir():
        path.unlink()
    for name in ("combined-ca-bundle.pem", "database.env", "mysql-admin.cnf", "environment.json", "with-fixture-trust.sh", "exec-selected-environment.py"):
        (fixture.directory / name).unlink(missing_ok=True)
    manifest["stopped"] = True
    private_write(fixture.directory / "manifest.json", json.dumps(manifest, indent=2))
    print(json.dumps({"owned_fixtures_removed": True, "temporary_ca_removed": True, "existing_clickhouse_rows": final_rows, "host_trust_unchanged": True}))


def load_capacity_csv(fixture, path, expected_hash, taxi):
    require(path is not None and path.is_file(), "capacity CSV is unavailable")
    require(expected_hash and re.fullmatch(r"[0-9a-f]{64}", expected_hash), "capacity CSV requires its frozen SHA256")
    require(digest(path) == expected_hash, "capacity CSV does not match its frozen SHA256")
    name = "taxi_sample" if taxi else "zones"
    source = path
    if taxi:
        with path.open(newline="") as data:
            require(next(csv.reader(data)) == ["trip_id", "pickup_zone_id", "fare_cents"], "taxi projection columns differ")
        definition = "trip_id BIGINT, pickup_zone_id INTEGER, fare_cents BIGINT NULL"
    else:
        source = fixture.directory / "normalized-zones.csv"
        with path.open(newline="", encoding="utf-8-sig") as data, source.open("w", newline="") as normalized:
            reader = csv.reader(data)
            require(next(reader) in (["LocationID", "Borough", "Zone", "service_zone"],
                                    ["zone_id", "borough", "zone", "service_zone"]), "zone lookup columns differ")
            writer = csv.writer(normalized, lineterminator="\n")
            writer.writerow(["zone_id", "borough", "zone", "service_zone"])
            for row in reader:
                require(len(row) == 4 and row[0].isdigit(), "zone lookup row is invalid")
                writer.writerow(row)
        definition = "zone_id INTEGER, borough TEXT, zone TEXT, service_zone TEXT"
    database = fixture.manifest["database"]
    for kind in ("postgres", "mysql"):
        target = "acceptance." + name if kind == "postgres" else "`" + database + "`." + name
        fixture.admin(kind, "CREATE TABLE " + target + " (" + definition + ")")
        if kind == "postgres":
            command = DOCKER + ["exec", "-i", fixture.manifest["containers"][kind], "psql", "-X", "-v", "ON_ERROR_STOP=1",
                                "-U", "postgres", "-d", database, "-c", "COPY " + target + " FROM STDIN WITH (FORMAT csv, HEADER true)"]
            with source.open("rb") as data:
                result = subprocess.run(command, stdin=data, capture_output=True, timeout=240)
            if result.returncode:
                private_write(fixture.directory / "capacity-copy-error.log", result.stderr)
            require(result.returncode == 0, "PostgreSQL fixture bulk load failed")
        else:
            remote = "/var/lib/mysql-files/kelvo-" + name + ".csv"
            fixture.docker("cp", str(source), fixture.manifest["containers"][kind] + ":" + remote, timeout=90)
            try:
                fixture.docker("exec", fixture.manifest["containers"][kind], "chmod", "0644", remote)
                columns = "(trip_id,pickup_zone_id,@fare_cents) SET fare_cents=NULLIF(@fare_cents,'')" if taxi else "(zone_id,borough,zone,service_zone)"
                fixture.admin(kind, "LOAD DATA INFILE '" + remote + "' INTO TABLE " + target +
                              " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES " + columns, timeout=240)
            finally:
                fixture.docker("exec", fixture.manifest["containers"][kind], "rm", "-f", remote)
    aggregates = {}
    for kind in ("postgres", "mysql"):
        target = "acceptance." + name if kind == "postgres" else "`" + database + "`." + name
        sql = ("SELECT count(*),sum(trip_id),sum(pickup_zone_id),sum(fare_cents),count(*)-count(fare_cents) FROM " + target if taxi else
               "SELECT count(*),sum(zone_id) FROM " + target)
        raw = fixture.query_value(kind, sql)
        aggregates[kind] = [int(value) for value in re.split(r"[|\t]", raw)]
    require(aggregates["postgres"] == aggregates["mysql"], "loaded source aggregates differ")
    if taxi:
        require(aggregates["postgres"] == [1_000_000, 100_499_999_500_000, 162_903_877, 1_582_868_084, 0],
                "taxi sample differs from the frozen native ClickHouse reference")
    else:
        require(aggregates["postgres"][0] == 265, "official zone lookup row count differs")
    receipt = {"table": name, "source_sha256": expected_hash, "aggregates": aggregates,
               "scope": "Same frozen public data loaded by fixture administrators; production readers remain SELECT-only"}
    private_write(fixture.directory / ("load-" + name + ".json"), json.dumps(receipt, indent=2))
    private_write(fixture.directory / "capacity-catalog.json", json.dumps({"sources": [
        fixture.source(kind, ("zones", "taxi_sample"), max_rows=2_000_000, max_bytes=256 << 20)
        for kind in ("postgres", "mysql")]}, indent=2))
    print(json.dumps(receipt, indent=2))


class TCPRelay:
    """Opaque loopback TCP relay: same TLS server, intentionally wrong hostname."""
    def __init__(self, endpoint):
        host, port = endpoint.rsplit(":", 1)
        self.endpoint = (host, int(port))
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.2)
        self.port = self.listener.getsockname()[1]
        self.stopped = threading.Event()
        self.sockets = []
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stopped.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                upstream = socket.create_connection(self.endpoint, timeout=5)
            except OSError:
                client.close()
                continue
            self.sockets.extend((client, upstream))
            for source, target in ((client, upstream), (upstream, client)):
                threading.Thread(target=self.pipe, args=(source, target), daemon=True).start()

    def pipe(self, source, target):
        try:
            while not self.stopped.is_set():
                data = source.recv(32768)
                if not data:
                    break
                target.sendall(data)
        except OSError:
            pass
        finally:
            try:
                target.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def close(self):
        self.stopped.set()
        self.listener.close()
        for connection in self.sockets:
            connection.close()
        self.thread.join(timeout=2)


class Acceptance:
    def __init__(self, fixture, args):
        self.fixture, self.args = fixture, args
        self.directory = private_directory(args.run_directory) if args.run_directory else private_directory(
            private_directory(fixture.directory / "acceptance") / ("run-" + uuid.uuid4().hex))
        require(self.directory.is_relative_to(fixture.directory), "acceptance directory must belong to its fixture")
        self.binary = args.binary.resolve()
        self.binary_sha = digest(self.binary)
        self.children = []
        self.checks = []
        self.stage = "prerequisites"
        self.counter = 0
        self.config = fixture.configuration(self.directory)
        self.environment = dict(os.environ, **fixture.environment, GOMAXPROCS="2",
            PGOPTIONS="-c statement_timeout=1", PGPASSFILE="/nonexistent/ambient-passfile",
            MYSQL_PWD="unselected-ambient-canary", SSL_CERT_FILE="/nonexistent/ambient-ca",
            KELVO_SOURCE_REL_UNSELECTED_CANARY="unselected-credential-canary")

    def record(self, name, **data):
        self.checks.append({"test": name, "passed": True, **data})
        print(name + ": passed", flush=True)

    def start(self, sql, sources, *, config=None, environment=None, timeout="20s", native=False):
        self.counter += 1
        output = self.directory / ("query-" + str(self.counter) + ".arrow")
        selection = ["--mode", "native", "--connection", sources] if native else ["--sources", sources]
        command = [str(self.binary), "query", "--config", str(config or self.config), *selection,
                   "--sql", sql, "--out", str(output), "--threads", "2", "--memory-mb", "256",
                   "--temp-mb", "64", "--timeout", timeout, "--max-rows", "2000000", "--max-bytes", str(128 << 20)]
        process = subprocess.Popen(command, env=environment or self.environment, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        self.children.append(process)
        return process, output, self.counter

    def finish(self, running, success=True, native=False):
        process, output, index = running
        try:
            stdout, stderr = process.communicate(timeout=35)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
            raise AssertionError("acceptance query exceeded its deadline") from None
        private_write(self.directory / ("query-" + str(index) + ".log"), stdout + b"\n" + stderr)
        require((process.returncode == 0) == success, "CLI query returned unexpected status")
        if not success:
            require(not output.exists(), "failed query published an Arrow result")
            return None, {"nonzero_exit": True, "no_export": True}
        wire = output.read_bytes()
        require(wire.endswith(EOS), "Arrow result omitted explicit EOS")
        table = pa.ipc.open_stream(wire).read_all()
        stats = json.loads(stderr)
        if native:
            require(stats.get("backend") == "mysql", "native MySQL statistics absent")
            return table, {"rows": table.num_rows, "backend": stats["backend"]}
        require(stats.get("backend") == "duckdb" and stats.get("federation"), "custom federation statistics absent")
        return table, {"rows": table.num_rows, "federation": stats["federation"]}

    def query(self, sql, sources, *, success=True, native=False, **kwargs):
        return self.finish(self.start(sql, sources, native=native, **kwargs), success, native=native)

    def unknown_ca_controls(self):
        self.stage = "unknown_ca_controls"
        require(digest("/etc/ssl/certs/ca-certificates.crt") == self.fixture.manifest["host_ca_bundle_sha256"],
                "unknown-CA control must use unchanged host trust")
        for kind in ("postgres", "mysql"):
            _, detail = self.query("SELECT row_id FROM " + IDS[kind] + ".fact", IDS[kind], success=False)
            self.record(kind + "_unknown_ca_rejected", **detail)
        private_write(self.directory / "unknown-ca-controls.json", json.dumps(self.checks))

    def transport(self):
        self.stage = "verified_tls_transport"
        require(os.geteuid() != 0, "acceptance query process must not be root")
        require(os.readlink("/proc/self/ns/mnt") != self.fixture.manifest["host_mount_namespace"], "private trust mount namespace absent")
        require(digest("/etc/ssl/certs/ca-certificates.crt") == self.fixture.manifest["ca_bundle_sha256"], "fixture trust bundle not mounted")
        for kind in ("postgres", "mysql"):
            config = self.fixture.configuration(self.directory, tables={kind: ("transport",)})
            table, detail = self.query("SELECT * FROM " + IDS[kind] + ".transport", IDS[kind], config=config)
            row = table.to_pylist()[0]
            require(bool(row["cipher"]), "native relational connection has no TLS cipher")
            if kind == "postgres":
                require(row["ssl"] and row["read_only"] == "on", "PostgreSQL TLS/read-only transaction missing")
            self.record(kind + "_verified_tls_native_scan", cipher=row["cipher"], **detail)
            relay = TCPRelay(self.fixture.manifest["endpoints"][kind])
            try:
                env = dict(self.environment)
                key = self.fixture.source(kind)["dsn_env"]
                env[key] = env[key].replace(self.fixture.manifest["endpoints"][kind], "127.0.0.1:" + str(relay.port))
                _, detail = self.query("SELECT row_id FROM " + IDS[kind] + ".fact", IDS[kind], environment=env, success=False)
                self.record(kind + "_hostname_mismatch_rejected", **detail)
            finally:
                relay.close()

    def typed_values(self):
        self.stage = "exact_relational_arrow_values"
        for kind in KINDS:
            source = IDS[kind]
            table, detail = self.query("SELECT row_id,i16,i64,u64,amount,day,wall,instant,label FROM " + source + ".fact WHERE row_id<=3 ORDER BY row_id", source)
            types = {field.name: field.type for field in table.schema}
            require(types["row_id"] == pa.int32() and types["i16"] == pa.int16() and types["i64"] == pa.int64(), "integer width changed")
            require(types["u64"] == (pa.decimal128(20, 0) if kind == "postgres" else pa.uint64()), "unsigned-range source type changed")
            require(types["amount"] == pa.decimal128(30, 10) and types["day"] == pa.date32(), "exact decimal/date type changed")
            require(pa.types.is_timestamp(types["wall"]) and types["wall"].unit == "us" and types["instant"].unit == "us", "timestamp precision changed")
            rows = table.to_pylist()
            require(len(rows) == 3, "typed source row count changed")
            require(rows[0]["i64"] == -(1 << 63) and rows[1]["i64"] == (1 << 63) - 1, "signed extrema changed")
            require(rows[0]["u64"] == 0 and rows[1]["u64"] == (1 << 64) - 1, "unsigned extrema changed")
            require(rows[0]["amount"] == Decimal("-12345678901234567890.0123456789") and rows[1]["amount"] == Decimal("12345678901234567890.0123456789"), "decimal values changed")
            require(rows[0]["day"] == date(1969, 12, 31), "pre-epoch date changed")
            require(rows[0]["wall"].replace(tzinfo=None) == datetime(2026, 10, 1, 12, 30, 0, 123456), "wall timestamp changed")
            require(rows[0]["instant"] == datetime(2026, 10, 1, 7, 0, 0, 123456, tzinfo=timezone.utc), "UTC instant changed")
            require(all(value is None for key, value in rows[2].items() if key != "row_id"), "NULL values changed")
            self.record(kind + "_exact_arrow_values", arrow_types={key: str(value) for key, value in types.items()}, **detail)

    def readonly_principals(self):
        self.stage = "database_reader_write_denial"
        before = self.fixture.query_value("postgres", "SELECT count(*) FROM acceptance.hidden")
        denied = self.fixture.admin("postgres", "SET ROLE kelvo_reader; INSERT INTO acceptance.hidden VALUES('forbidden')", check=False)
        require(denied.returncode != 0 and self.fixture.query_value("postgres", "SELECT count(*) FROM acceptance.hidden") == before,
                "PostgreSQL reader principal permits writes")
        self.record("postgres_reader_principal_write_denied")
        endpoint = self.fixture.manifest["endpoints"]["mysql"]
        password = self.fixture.environment["KELVO_SOURCE_REL_MY_DSN"].split(":", 1)[1].split("@tcp(", 1)[0]
        configuration = self.directory / "mysql-reader.cnf"
        private_write(configuration, "[client]\nuser=kelvo_reader\npassword=" + password + "\nhost=" + endpoint.split(":")[0] +
                      "\nprotocol=TCP\nssl-mode=VERIFY_IDENTITY\nssl-ca=/certs/ca.crt\n")
        container = self.fixture.manifest["containers"]["mysql"]
        remote = "/tmp/kelvo-readonly-acceptance.cnf"
        self.fixture.docker("cp", str(configuration), container + ":" + remote)
        try:
            target = "`" + self.fixture.manifest["database"] + "`.hidden"
            command = ["exec", "-i", container, "mysql", "--defaults-extra-file=" + remote, "--batch", "--silent", "--skip-column-names"]
            control = self.fixture.docker(*command, data=("SELECT count(*) FROM " + target).encode())
            denied = self.fixture.docker(*command, data=("INSERT INTO " + target + " VALUES('forbidden')").encode(), check=False)
            require(control.stdout.strip() == b"1" and denied.returncode != 0 and self.fixture.query_value("mysql", "SELECT count(*) FROM " + target) == "1",
                    "MySQL reader principal permits writes or its verified-TLS read control failed")
            self.record("mysql_verified_tls_reader_principal_write_denied")
        finally:
            self.fixture.docker("exec", container, "rm", "-f", remote)
            configuration.unlink()

    def joins(self):
        self.stage = "pairwise_threeway_and_self_joins"
        for left_index, left in enumerate(KINDS):
            for right in KINDS[left_index + 1:]:
                for mode in ("INNER", "LEFT"):
                    sql = "SELECT a.row_id AS a_id,b.row_id AS b_id FROM " + IDS[left] + ".fact a " + mode + " JOIN " + IDS[right] + ".fact b ON a.row_id=b.row_id ORDER BY a.row_id"
                    table, detail = self.query(sql, IDS[left] + "," + IDS[right])
                    expected = [{"a_id": value, "b_id": value if value in SOURCE_ROWS[right] else None}
                                for value in SOURCE_ROWS[left] if mode == "LEFT" or value in SOURCE_ROWS[right]]
                    require(table.to_pylist() == expected, "pairwise join values changed")
                    self.record(IDS[left] + "_" + IDS[right] + "_" + mode.lower() + "_join", **detail)
        for kind in KINDS:
            source = IDS[kind]
            table, detail = self.query("SELECT a.row_id AS a_id,b.row_id AS b_id FROM " + source + ".fact a JOIN " + source + ".fact b ON a.row_id=b.row_id ORDER BY a.row_id", source)
            require(table.to_pylist() == [{"a_id": value, "b_id": value} for value in SOURCE_ROWS[kind]], "self-join values changed")
            require(sum(entry["scans"] for entry in detail["federation"]) >= 2, "self-join did not create independent scans")
            self.record(source + "_independent_self_join", **detail)
        for mode in ("INNER", "LEFT"):
            sql = "SELECT c.row_id AS ch_id,p.row_id AS pg_id,m.row_id AS my_id FROM ch.fact c " + mode + " JOIN pg.fact p ON c.row_id=p.row_id " + mode + " JOIN my.fact m ON c.row_id=m.row_id ORDER BY c.row_id"
            table, detail = self.query(sql, "ch,pg,my")
            expected = [{"ch_id": value, "pg_id": value if value <= 3 else None, "my_id": value if value <= 3 else None}
                        for value in ([1, 2, 3] if mode == "INNER" else [1, 2, 3, 4])]
            require(table.to_pylist() == expected, "three-source join values changed")
            self.record("three_source_" + mode.lower() + "_join", **detail)

    def boundaries(self):
        self.stage = "selected_sources_tables_and_scan_budgets"
        for index, kind in enumerate(KINDS):
            source = IDS[kind]
            other = IDS[KINDS[(index + 1) % len(KINDS)]]
            _, detail = self.query("SELECT row_id FROM " + other + ".fact", source, success=False)
            self.record(source + "_unselected_source_denied", **detail)
            _, detail = self.query("SELECT marker FROM " + source + ".hidden", source, success=False)
            self.record(source + "_unregistered_table_denied", **detail)
            config = self.fixture.configuration(self.directory, max_rows=2)
            _, detail = self.query("SELECT count(*) FROM " + source + ".fact", source, config=config, success=False)
            self.record(source + "_row_scan_budget_fails_closed", **detail)
            config = self.fixture.configuration(self.directory, max_bytes=1024)
            _, detail = self.query("SELECT sum(length(payload)) FROM " + source + ".fact", source, config=config, success=False)
            self.record(source + "_byte_scan_budget_fails_closed", **detail)
        self.stage = "unsafe_relational_options"
        for kind, replacements in (("postgres", [("sslmode=verify-full", "sslmode=disable"), ("", "&passfile=/nonexistent"), ("", "&sslmode=verify-full")]),
                                   ("mysql", [("tls=true", "tls=skip-verify"), ("", "&multiStatements=true"), ("", "&allowAllFiles=true")])):
            for index, (before, after) in enumerate(replacements):
                env = dict(self.environment)
                key = self.fixture.source(kind)["dsn_env"]
                env[key] = env[key].replace(before, after) if before else env[key] + after
                _, detail = self.query("SELECT row_id FROM " + IDS[kind] + ".fact", IDS[kind], environment=env, success=False)
                self.record(kind + "_unsafe_option_" + str(index) + "_rejected", **detail)

    def active_queries(self, kind):
        if kind == "postgres":
            sql = "SELECT count(*) FROM pg_stat_activity WHERE usename='kelvo_reader' AND state='active' AND query LIKE '%slow%' AND query NOT LIKE '%LIMIT 0%'"
        else:
            sql = "SELECT count(*) FROM information_schema.processlist WHERE USER='kelvo_reader' AND INFO LIKE '%slow%' AND INFO NOT LIKE '%LIMIT 0%'"
        return int(self.fixture.query_value(kind, sql))

    def worker_environment(self, process):
        pids = set()
        for path in Path("/proc/" + str(process.pid) + "/task").glob("*/children"):
            try:
                pids.update(int(value) for value in path.read_text().split())
            except (FileNotFoundError, ProcessLookupError):
                pass
        for pid in pids:
            try:
                command = Path("/proc/" + str(pid) + "/cmdline").read_bytes().split(b"\0")
                if len(command) >= 2 and command[1] == b"worker":
                    values = Path("/proc/" + str(pid) + "/environ").read_bytes().split(b"\0")
                    return pid, {item.split(b"=", 1)[0].decode() for item in values if b"=" in item}
            except (FileNotFoundError, ProcessLookupError):
                pass
        return None

    def cancellation(self, kinds=("postgres", "mysql")):
        self.stage = "live_scan_cancellation_and_selected_credentials"
        for kind in kinds:
            source = IDS[kind]
            config = self.fixture.configuration(self.directory, tables={kind: ("slow",)})
            timeout = "3s" if kind == "mysql" else "30s"
            if kind == "mysql":
                table, detail = self.query("SELECT CAST(@@session.max_execution_time AS SIGNED) AS timeout_ms", source,
                                           native=True, timeout=timeout)
                require(table.to_pylist() == [{"timeout_ms": 3000}], "MySQL source SELECT deadline was not installed")
                self.record("mysql_source_select_deadline_control", timeout_ms=3000, **detail)
                _, detail = self.query("SELECT /*+ MAX_EXECUTION_TIME(0) */ 1", source, native=True, success=False)
                self.record("mysql_source_deadline_hint_override_rejected", **detail)
            launched = time.monotonic()
            running = self.start('SELECT sum("delayed") FROM ' + source + ".slow", source, config=config, timeout=timeout)
            require(wait_until(lambda: self.active_queries(kind) > 0, 10), "no live relational scan was observed")
            observed = self.worker_environment(running[0])
            require(observed is not None, "active query worker was not observed")
            pid, names = observed
            selected = {self.fixture.source(kind)["dsn_env"]}
            require({name for name in names if name.startswith("KELVO_SOURCE_")} == selected, "worker received unselected source credentials")
            require(not names.intersection({"PGOPTIONS", "PGPASSFILE", "MYSQL_PWD", "SSL_CERT_FILE"}), "worker inherited ambient database/TLS options")
            started = time.monotonic()
            running[0].send_signal(signal.SIGINT)
            _, detail = self.finish(running, success=False)
            client_seconds = time.monotonic() - started
            require(client_seconds < 2, "cancelled CLI exceeded bounded worker cleanup grace")
            require(wait_until(lambda: not Path("/proc/" + str(pid)).exists()), "cancelled worker remained alive")
            require(wait_until(lambda: self.active_queries(kind) == 0, 5 if kind == "mysql" else 2),
                    "cancelled relational query remained active beyond the source cancellation bound")
            upstream_seconds = time.monotonic() - started
            elapsed = time.monotonic() - launched
            if kind == "mysql":
                require(elapsed < 5, "MySQL source SELECT outlived its three-second execution timer")
            self.record(kind + "_live_cancellation_and_selected_environment", worker_environment_names=sorted(names),
                        upstream_stopped=True, worker_stopped=True, client_cancellation_seconds=client_seconds,
                        upstream_cancellation_seconds=upstream_seconds, query_elapsed_seconds=elapsed,
                        upstream_stop_mechanism="source SELECT execution timer" if kind == "mysql" else "PostgreSQL CancelRequest",
                        server_timeout_ms=3000 if kind == "mysql" else None, **detail)

    def sandbox(self):
        self.stage = "three_source_landlock"
        workspace = private_directory(self.directory / "sandbox")
        sources = [self.fixture.source(kind) for kind in KINDS]
        environment = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(workspace), "TMPDIR": str(workspace), "GOMAXPROCS": "2", **self.fixture.environment}
        payload = {"config": {"sources": sources}, "limits": {"max_rows": 1000, "max_bytes": 8 << 20,
                   "timeout": 20_000_000_000, "memory_mb": 256, "threads": 2, "max_temp_mb": 64},
                   "request": {"mode": "federated", "sources": ["ch", "pg", "my"], "sql":
                   "SELECT c.row_id FROM ch.fact c JOIN pg.fact p ON p.row_id=c.row_id JOIN my.fact m ON m.row_id=c.row_id ORDER BY c.row_id"}}
        launcher = self.binary.parent / "kelvo-landlock"
        process = subprocess.Popen([str(launcher), "--write", str(workspace), "--", str(self.binary), "worker"],
                                   cwd=workspace, env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        self.children.append(process)
        stdout, stderr = process.communicate(json.dumps(payload).encode(), timeout=30)
        private_write(self.directory / "sandbox.log", stderr)
        outcome = json.loads(stderr)
        require(process.returncode == 0 and not outcome.get("error") and stdout.endswith(EOS), "sandboxed three-source federation failed")
        require(pa.ipc.open_stream(stdout).read_all().to_pylist() == [{"row_id": 1}, {"row_id": 2}, {"row_id": 3}], "sandboxed three-source values changed")
        self.record("three_source_real_landlock", rows=3, launcher_sha256=digest(launcher),
                    scope="Actual unprivileged worker under Landlock in the private fixture trust namespace")

    def cleanup(self):
        for process in self.children:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)


def run_acceptance(fixture, args):
    run = Acceptance(fixture, args)
    if not args.inside_trust_namespace:
        run.unknown_ca_controls()
        command = [str(fixture.directory / "with-fixture-trust.sh"), PYTHON, str(Path(__file__).resolve()), "test",
                   "--fixture", str(fixture.directory), "--binary", str(run.binary), "--report", str(args.report),
                   "--run-directory", str(run.directory), "--inside-trust-namespace"]
        result = subprocess.run(command)
        require(digest("/etc/ssl/certs/ca-certificates.crt") == fixture.manifest["host_ca_bundle_sha256"], "host CA trust changed")
        return result.returncode
    failure = None
    run.checks.extend(json.loads((run.directory / "unknown-ca-controls.json").read_text()))
    try:
        run.transport()
        run.typed_values()
        run.readonly_principals()
        run.joins()
        run.boundaries()
        run.cancellation()
        run.sandbox()
        require(digest(run.binary) == run.binary_sha, "binary changed during acceptance")
    except BaseException as error:
        failure = {"stage": run.stage, "exception_type": type(error).__name__}
        private_write(run.directory / "failure.log", traceback.format_exc())
    finally:
        run.cleanup()
    report = {"passed": failure is None, "failure": failure, "checked_at_utc": datetime.now(timezone.utc).isoformat(),
              "binary_sha256": run.binary_sha, "checks": run.checks, "images": fixture.manifest["images"],
              "fixture_retained_for_capacity_trials": True, "ca_signing_keys_removed": True,
              "trust_scope": "PostgreSQL/MySQL verify certificates and hostnames using a temporary CA bundle inside a private mount namespace, with CLI/workers running as the ordinary VM user. Host trust and production source configuration are unchanged. Existing ClickHouse uses its dedicated HTTP endpoint.",
              "scope": "Real CLI, Arrow and Landlock correctness/security acceptance on synthetic fixture rows. Capacity trials are recorded separately."}
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": report["passed"], "checks": len(run.checks), "failure": failure, "report": str(args.report)}))
    return 0 if failure is None else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("provision", "status", "test", "stop", "load-zones", "load-taxi"))
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--clickhouse-container", default="kelvo-clickhouse")
    parser.add_argument("--binary", type=Path, default=ROOT / "bin/kelvo")
    parser.add_argument("--report", type=Path, default=ROOT / "docs/evidence/federation-relational.json")
    parser.add_argument("--csv", type=Path)
    parser.add_argument("--sha256")
    parser.add_argument("--inside-trust-namespace", action="store_true")
    parser.add_argument("--run-directory", type=Path)
    args = parser.parse_args()
    if args.action == "provision":
        provision(args.clickhouse_container)
        return 0
    fixture = Fixture.selected(args.fixture)
    if args.action == "status":
        print(json.dumps(fixture.public(), indent=2))
    elif args.action == "stop":
        stop_fixture(fixture)
    elif args.action in ("load-zones", "load-taxi"):
        load_capacity_csv(fixture, args.csv, args.sha256, args.action == "load-taxi")
    elif args.action == "test":
        return run_acceptance(fixture, args)
    else:
        raise RuntimeError("acceptance/loading implementation is being prepared")
    return 0


if __name__ == "__main__":
    sys.exit(main())
