#!/usr/bin/env python3
"""Source fixture for the pipeline-scale evidence (Linux test VM only).

provision  Reuse federation_relational_acceptance (PostgreSQL 17.6, MySQL 8.4,
           TLS, read-only principals, existing kelvo-clickhouse) with a fixed
           user-configured subnet (Docker 28 refuses --ip without one).
load       Load benchmarks/pipelines/generate.py Parquet output into the
           owned databases; record rows, bytes and load seconds.
sources    Write the private tenant-sources.json consumed by
           federation_capacity_cluster.py start / pipeline_cluster.py.

Credentials stay in the relational fixture's private environment.json; this
script writes only environment variable names into public evidence.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time

import federation_relational_acceptance as fra

ROOT = Path(__file__).resolve().parents[1]
PG = {"orders": "order_id BIGINT PRIMARY KEY, tenant_id INTEGER NOT NULL, customer_id BIGINT NOT NULL, seller_id INTEGER NOT NULL, warehouse_id INTEGER NOT NULL, created_at TIMESTAMP(6) NOT NULL, status TEXT NOT NULL, payment_method TEXT NOT NULL, amount_cents BIGINT NOT NULL",
      "customers": "customer_id BIGINT PRIMARY KEY, tenant_id INTEGER NOT NULL, city_id INTEGER NOT NULL, signup_date DATE NOT NULL, segment TEXT NOT NULL"}
MY = {"shipments": "shipment_id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL, carrier_id INT NOT NULL, rider_id INT NOT NULL, warehouse_id INT NOT NULL, promised_at DATETIME(6) NOT NULL, picked_at DATETIME(6) NOT NULL, delivered_at DATETIME(6) NULL, status VARCHAR(16) NOT NULL",
      "sellers": "seller_id INT PRIMARY KEY, tenant_id INT NOT NULL, category VARCHAR(16) NOT NULL, city_id INT NOT NULL",
      "warehouses": "warehouse_id INT PRIMARY KEY, city_id INT NOT NULL, zone VARCHAR(8) NOT NULL"}
CH = {"order_items": ("order_id Int64, line_no Int32, sku_id Int32, category String, qty Int32, price_cents Int64", "order_id, line_no"),
      "events_hot": ("event_id Int64, shipment_id Int64, rider_id Int32, event_time DateTime64(6), event_type String, lat_e6 Int32, lon_e6 Int32", "event_time, event_id")}
FED = {"max_scan_rows": 100_000_000, "max_scan_bytes": 16 << 30}


def subnet_docker(original):
    """Give the owned --internal network an explicit subnet so --ip works."""
    def docker(self, *arguments, **kwargs):
        arguments = list(arguments)
        if arguments[:3] == ["network", "create", "--internal"]:
            arguments[2:2] = ["--subnet", "172.31.240.0/24"]
        return original(self, *arguments, **kwargs)
    return docker


def provision(clickhouse):
    fra.Fixture.docker = subnet_docker(fra.Fixture.docker)
    fra.provision(clickhouse)
    fixture = fra.Fixture.selected(None)
    # The acceptance defaults (1 CPU, 1 GiB, 128 MiB caches) are sized for tiny fixtures.
    for kind in ("postgres", "mysql"):
        fixture.docker("update", "--cpus", "1.5", "--memory", "3g", "--memory-swap", "3g", fixture.manifest["containers"][kind])
    fixture.admin("mysql", "SET GLOBAL innodb_buffer_pool_size=1073741824;")


def csv(data, table, out):
    import duckdb
    path = out / (table + ".csv")
    duckdb.connect().execute(f"COPY (SELECT * FROM read_parquet('{data / (table + '.parquet')}')) TO '{path}' (HEADER false, NULLSTR '\\N', TIMESTAMPFORMAT '%Y-%m-%d %H:%M:%S.%f')")
    return path


def load(data, fixture_dir):
    fixture = fra.Fixture.selected(fixture_dir)
    database, containers = fixture.manifest["database"], fixture.manifest["containers"]
    staging = Path.home() / "pipeline-csv"
    staging.mkdir(exist_ok=True)
    report = {"database_kind": {}, "tables": {}}
    for table, columns in PG.items():
        started = time.monotonic()
        fixture.admin("postgres", f"DROP TABLE IF EXISTS acceptance.{table}; CREATE TABLE acceptance.{table} ({columns});")
        path = csv(data, table, staging)
        with path.open("rb") as stream:
            subprocess.run(fra.DOCKER + ["exec", "-i", containers["postgres"], "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", database,
                                         "-c", f"\\copy acceptance.{table} FROM STDIN (FORMAT csv, NULL '\\N')"], stdin=stream, check=True, stdout=subprocess.DEVNULL)
        if table == "orders":
            fixture.admin("postgres", "CREATE INDEX orders_created_at ON acceptance.orders (created_at);", timeout=1800)
        fixture.admin("postgres", f"ANALYZE acceptance.{table};", timeout=600)
        report["tables"][table] = {"source": "postgres", "load_seconds": round(time.monotonic() - started, 3)}
        path.unlink()
    for table, columns in MY.items():
        started = time.monotonic()
        fixture.admin("mysql", f"USE `{database}`; DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({columns});")
        path = csv(data, table, staging)
        fixture.docker("cp", str(path), containers["mysql"] + ":/var/lib/mysql-files/" + path.name, timeout=1800)
        fixture.admin("mysql", f"SET time_zone='+00:00'; SET unique_checks=0; USE `{database}`; LOAD DATA INFILE '/var/lib/mysql-files/{path.name}' INTO TABLE {table} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"';", timeout=3600)
        fixture.docker("exec", containers["mysql"], "rm", "/var/lib/mysql-files/" + path.name)
        if table == "shipments":
            fixture.admin("mysql", f"USE `{database}`; CREATE INDEX shipments_promised ON shipments (promised_at);", timeout=3600)
        report["tables"][table] = {"source": "mysql", "load_seconds": round(time.monotonic() - started, 3)}
        path.unlink()
    for table, (columns, order) in CH.items():
        started = time.monotonic()
        fixture.admin("clickhouse", f"DROP TABLE IF EXISTS `{database}`.{table}; CREATE TABLE `{database}`.{table} ({columns}) ENGINE=MergeTree ORDER BY ({order});")
        with (data / (table + ".parquet")).open("rb") as stream:
            subprocess.run(fra.DOCKER + ["exec", "-i", containers["clickhouse"], "clickhouse-client", "-q", f"INSERT INTO `{database}`.{table} FORMAT Parquet"], stdin=stream, check=True)
        report["tables"][table] = {"source": "clickhouse", "load_seconds": round(time.monotonic() - started, 3)}
    counts = {
        "postgres": {t: fixture.query_value("postgres", f"SELECT count(*) FROM acceptance.{t}") for t in PG},
        "mysql": {t: fixture.query_value("mysql", f"SELECT count(*) FROM `{database}`.{t}") for t in MY},
        "clickhouse": {t: fixture.query_value("clickhouse", f"SELECT count() FROM `{database}`.{t}") for t in CH}}
    sizes = {
        "postgres": fixture.query_value("postgres", "SELECT sum(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='acceptance' AND c.relkind='r'"),
        "mysql": fixture.query_value("mysql", f"SELECT sum(data_length+index_length) FROM information_schema.tables WHERE table_schema='{database}'"),
        "clickhouse": fixture.query_value("clickhouse", f"SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database='{database}'")}
    for kind, tables in counts.items():
        for table, rows in tables.items():
            report["tables"][table]["rows"] = int(rows)
    report["database_kind"] = {kind: {"bytes_on_disk": int(value)} for kind, value in sizes.items()}
    report["generator_manifest"] = json.loads((data / "manifest.json").read_text())
    print(json.dumps(report, indent=2))


def table(kind, name, database):
    return {"name": name, "schema": "acceptance", "table": name} if kind == "postgres" else {"name": name, "database": database, "table": name}


def sources(data, fixture_dir, out, accel_dir, snapshot_memory_mb=768):
    fixture = fra.Fixture.selected(fixture_dir)
    database, env = fixture.manifest["database"], fixture.environment
    names = {"pg": "KELVO_SOURCE_PIPE_PG_DSN", "my": "KELVO_SOURCE_PIPE_MY_DSN", "ch_url": "KELVO_SOURCE_PIPE_CH_URL",
             "ch_user": "KELVO_SOURCE_PIPE_CH_USER", "ch_password": "KELVO_SOURCE_PIPE_CH_PASSWORD"}
    values = {names["pg"]: env["KELVO_SOURCE_REL_PG_DSN"], names["my"]: env["KELVO_SOURCE_REL_MY_DSN"],
              names["ch_url"]: env["KELVO_SOURCE_REL_CH_URL"], names["ch_user"]: env["KELVO_SOURCE_REL_CH_USER"],
              names["ch_password"]: env["KELVO_SOURCE_REL_CH_PASSWORD"]}
    pg = {"id": "pg", "type": "postgres", "dsn_env": names["pg"], "federation": {"tables": [table("postgres", t, database) for t in PG], **FED}}
    my = {"id": "my", "type": "mysql", "dsn_env": names["my"], "federation": {"tables": [table("mysql", t, database) for t in MY], **FED}}
    ch = {"id": "ch", "type": "clickhouse", "url_env": names["ch_url"], "username_env": names["ch_user"], "password_env": names["ch_password"],
          "federation": {"tables": [table("clickhouse", t, database) for t in CH], **FED}}
    cold = {"id": "cold", "type": "parquet", "path": str((data / "events_cold.parquet").resolve())}
    limits = {"max_rows": 60_000_000, "max_bytes": 4 << 30, "timeout": "600s", "memory_mb": snapshot_memory_mb, "threads": 2, "max_temp_mb": 4096}

    def dataset(alias, source, name, interval="0s"):
        return {"id": alias, "query": {"mode": "federated", "sources": [source], "sql": f"SELECT * FROM {source}.{name}"},
                "authorization_version": "pipeline-v1", "refresh_interval": interval, "max_age": "24h",
                "on_unavailable": "refresh", "limits": limits,
                # Single-part 20M-row snapshots hit the Parquet row-group metadata limit.
                "multipart": {"max_part_bytes": 256 << 20, "max_parts": 64}}
    # Small and hot snapshots refresh on the node scheduler (soak refresh load);
    # the 20M/50M-row copies refresh on demand only.
    every = {"events_hot": "5m", "sellers": "5m", "warehouses": "5m", "customers": "10m"}
    snaps = [dataset(t + "_snap", src, t, every.get(t, "0s")) for src, group in (("pg", PG), ("my", MY), ("ch", CH)) for t in group]
    catalog_a = {"sources": [pg, my, ch, cold], "acceleration": {"tenant_id": "a", "directory": str(accel_dir / "a"), "datasets": snaps}}
    # Tenant b is a second company (a 3PL) on the same VM: only shipments and hot events.
    catalog_b = {"sources": [dict(my, federation={"tables": [table("mysql", "shipments", database)], **FED}),
                             dict(ch, federation={"tables": [table("clickhouse", "events_hot", database)], **FED})]}
    manifest = {"version": 1, "trust_wrapper": str(fixture.directory / "with-fixture-trust.sh"), "tenants": {
        "a": {"catalog": catalog_a, "environment": values},
        "b": {"catalog": catalog_b, "environment": {k: v for k, v in values.items() if k != names["pg"]}}}}
    for tenant in ("a", "b"):
        (accel_dir / tenant).mkdir(mode=0o700, parents=True, exist_ok=True)
    fra.private_write(out, json.dumps(manifest, indent=2))
    print(str(out))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=("provision", "load", "sources"))
    parser.add_argument("--clickhouse-container", default="kelvo-clickhouse")
    parser.add_argument("--data", type=Path, help="generate.py output directory")
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--out", type=Path, default=ROOT / "artifacts/pipeline-private/tenant-sources.json")
    parser.add_argument("--snapshot-memory-mb", type=int, default=768, help="per-refresh DuckDB memory")
    parser.add_argument("--accel-dir", type=Path, default=ROOT / "artifacts/pipeline-private/acceleration")
    args = parser.parse_args()
    if args.action == "provision":
        provision(args.clickhouse_container)
    elif args.action == "load":
        load(args.data.resolve(), args.fixture)
    else:
        args.out.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        sources(args.data.resolve(), args.fixture, args.out, args.accel_dir.resolve(), args.snapshot_memory_mb)


if __name__ == "__main__":
    main()
