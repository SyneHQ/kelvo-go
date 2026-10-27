#!/usr/bin/env python3
"""Deterministic synthetic e-commerce/logistics dataset (seeded, DuckDB).

Every value is a pure function of (seed, table, row ordinal) through DuckDB's
hash(), so two runs with the same seed, scale and DuckDB version produce the
same rows. `manifest.json` records row counts, file bytes and an
order-independent content checksum (sum of per-row hashes) per table; compare
checksums, not Parquet bytes, because writer layout may vary with threads.

Shape (scale = number of orders):
  customers  scale/20     PostgreSQL   tenant-scoped buyers (8 seller platforms)
  sellers    50,000       MySQL        Shiprocket-style multi-tenant sellers (tenant = 1 + seller_id % 8;
                                       orders carry their seller's tenant_id)
  warehouses 200          MySQL
  orders     scale        PostgreSQL
  order_items ~2.5*scale  ClickHouse
  shipments  scale        MySQL        Swiggy/Zepto-style promise/pick/deliver
  events     8*scale      Parquet (cold, before HOT_FROM) + ClickHouse (hot)
"""
import argparse
import json
from pathlib import Path

import duckdb

HOT_FROM = "2025-12-01"  # events at/after this are "hot" (ClickHouse); before are cold Parquet
SPAN_SECONDS = 730 * 86400  # orders span 2024-01-01 .. 2025-12-31


def tables(seed, scale):
    h = lambda tag, expr="i": f"CAST(hash({seed}, '{tag}', {expr}) >> 1 AS BIGINT)"  # noqa: E731
    customers = max(scale // 20, 1)
    return {
        "customers": f"""
SELECT i AS customer_id, CAST(1 + {h('ct')} % 8 AS INTEGER) AS tenant_id,
       CAST(1 + {h('cc')} % 40 AS INTEGER) AS city_id,
       DATE '2023-01-01' + CAST({h('cd')} % 1000 AS INTEGER) AS signup_date,
       ['retail','prime','b2b'][1 + {h('cs')} % 3] AS segment
FROM range(1, {customers} + 1) t(i)""",
        "sellers": f"""
SELECT CAST(i AS INTEGER) AS seller_id, CAST(1 + i % 8 AS INTEGER) AS tenant_id,
       ['fashion','grocery','electronics','home','beauty','books'][1 + {h('sc')} % 6] AS category,
       CAST(1 + {h('sy')} % 40 AS INTEGER) AS city_id
FROM range(1, 50001) t(i)""",
        "warehouses": f"""
SELECT CAST(i AS INTEGER) AS warehouse_id, CAST(1 + (i - 1) % 40 AS INTEGER) AS city_id,
       ['north','south','east','west'][1 + {h('wz')} % 4] AS zone
FROM range(1, 201) t(i)""",
        "orders": f"""
SELECT i AS order_id, CAST(1 + (1 + {h('os')} % 50000) % 8 AS INTEGER) AS tenant_id,
       CAST(1 + {h('oc')} % {customers} AS BIGINT) AS customer_id,
       CAST(1 + {h('os')} % 50000 AS INTEGER) AS seller_id,
       CAST(1 + {h('ow')} % 200 AS INTEGER) AS warehouse_id,
       TIMESTAMP '2024-01-01' + to_seconds(CAST((i - 1) * {SPAN_SECONDS} // {scale} AS BIGINT)) AS created_at,
       CASE WHEN {h('ox')} % 100 < 4 THEN 'cancelled' WHEN {h('ox')} % 100 < 9 THEN 'returned' ELSE 'completed' END AS status,
       CASE WHEN {h('op')} % 10 < 4 THEN 'cod' WHEN {h('op')} % 10 < 8 THEN 'upi' ELSE 'card' END AS payment_method,
       CAST(1000 + {h('oa')} % 500000 AS BIGINT) AS amount_cents
FROM range(1, {scale} + 1) t(i)""",
        # 1..4 lines per order (mean 2.5): line j exists when j <= 1 + hash % 4.
        "order_items": f"""
SELECT o AS order_id, CAST(j AS INTEGER) AS line_no,
       CAST(1 + {h('is', 'o, j')} % 200000 AS INTEGER) AS sku_id,
       ['fashion','grocery','electronics','home','beauty','books'][1 + {h('ic', 'o, j')} % 6] AS category,
       CAST(1 + {h('iq', 'o, j')} % 3 AS INTEGER) AS qty,
       CAST(100 + {h('ip', 'o, j')} % 200000 AS BIGINT) AS price_cents
FROM range(1, {scale} + 1) a(o), range(1, 5) b(j)
WHERE j <= 1 + {h('il', 'o')} % 4""",
        # Promise = created + 24h..72h; pick 1..12h after create; ~6% never delivered.
        "shipments": f"""
WITH o AS ({{orders}})
SELECT order_id AS shipment_id, order_id, CAST(1 + {h('sk', 'order_id')} % 12 AS INTEGER) AS carrier_id,
       CAST(1 + {h('sr', 'order_id')} % 20000 AS INTEGER) AS rider_id, warehouse_id,
       created_at + to_seconds(CAST(86400 + {h('sp', 'order_id')} % 172800 AS BIGINT)) AS promised_at,
       created_at + to_seconds(CAST(3600 + {h('sq', 'order_id')} % 39600 AS BIGINT)) AS picked_at,
       CASE WHEN {h('sn', 'order_id')} % 100 < 6 THEN NULL
            ELSE created_at + to_seconds(CAST(7200 + {h('sd', 'order_id')} % 259200 AS BIGINT)) END AS delivered_at,
       CASE WHEN {h('sn', 'order_id')} % 100 < 3 THEN 'rto' WHEN {h('sn', 'order_id')} % 100 < 6 THEN 'lost' ELSE 'delivered' END AS status
FROM o""",
        # Eight tracking pings per shipment, spread over 48h after order creation.
        "events": f"""
SELECT i AS event_id, i // 8 + 1 AS shipment_id,
       CAST(1 + {h('sr', 'i // 8 + 1')} % 20000 AS INTEGER) AS rider_id,
       TIMESTAMP '2024-01-01' + to_seconds(CAST((i // 8) * {SPAN_SECONDS} // {scale} + (i % 8) * 21600 + {h('et')} % 3600 AS BIGINT)) AS event_time,
       ['created','assigned','picked','in_transit','in_transit','out_for_delivery','attempted','delivered'][1 + i % 8] AS event_type,
       CAST(12000000 + {h('ea')} % 16000000 AS INTEGER) AS lat_e6,
       CAST(72000000 + {h('eo')} % 16000000 AS INTEGER) AS lon_e6
FROM range(0, {scale} * 8) t(i)""",
    }


def checksum(con, relation):
    rows, digest = con.execute(f"SELECT count(*), coalesce(sum(hash(t)), 0)::VARCHAR FROM {relation} t").fetchone()
    return int(rows), digest


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scale", type=int, required=True, help="number of orders")
    parser.add_argument("--seed", type=int, default=20261027)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory", default="6GB")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads={args.threads}; SET memory_limit='{args.memory}'; SET preserve_insertion_order=false; SET temp_directory='{args.out}/.tmp'")
    specs = tables(args.seed, args.scale)
    specs["shipments"] = specs["shipments"].replace("{orders}", specs["orders"])
    manifest = {"seed": args.seed, "scale": args.scale, "duckdb": duckdb.__version__, "hot_from": HOT_FROM, "tables": {}}
    for name, sql in specs.items():
        targets = {name: f"SELECT * FROM ({sql}) s"}
        if name == "events":
            targets = {"events_cold": f"SELECT * FROM ({sql}) s WHERE event_time < TIMESTAMP '{HOT_FROM}'",
                       "events_hot": f"SELECT * FROM ({sql}) s WHERE event_time >= TIMESTAMP '{HOT_FROM}'"}
        for target, query in targets.items():
            path = args.out / f"{target}.parquet"
            con.execute(f"COPY ({query}) TO '{path}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)")
            rows, digest = checksum(con, f"read_parquet('{path}')")
            manifest["tables"][target] = {"rows": rows, "bytes": path.stat().st_size, "content_checksum": digest}
            print(json.dumps({"table": target, **manifest["tables"][target]}), flush=True)
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
