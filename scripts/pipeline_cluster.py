#!/usr/bin/env python3
"""Pipeline-scale cluster: federation_capacity_cluster with principals.

Same owned topology (three NATS brokers, two gateways, nodes a1/a2 for
tenant a and b1 for tenant b, Landlock children, mutual TLS), plus:
  * larger per-query limits and slots for 20M-row workflows,
  * file-backed version-2 gateway keys bound to principals
    a/ops (whole sources + snapshots), a/seller3 (row/column restricted to
    tenant_id = 3 and a column subset), b/ops,
  * identical policy.access on gateway, init and worker policies.
Keys are generated into a private file; only names reach evidence.

  pipeline_cluster.py provision
  pipeline_cluster.py start --binary bin/kelvo --source-manifest artifacts/pipeline-private/tenant-sources.json
  pipeline_cluster.py stop
"""
import json
import secrets
import sys

import yaml

import federation_capacity_cluster as fcc

KEYS = fcc.DIR / "gateway-keys.yml"
CLIENT_KEYS = fcc.DIR / "pipeline-client-keys.json"
SNAPSHOTS = ["orders_snap", "customers_snap", "shipments_snap", "sellers_snap", "warehouses_snap", "order_items_snap", "events_hot_snap"]
SELLER3 = {"sources": {
    "pg": {"tables": {"orders": {"columns": ["order_id", "seller_id", "created_at", "status", "payment_method", "amount_cents"],
                                 "rows": {"kind": "comparison", "column": "tenant_id", "type": "int32", "op": "eq", "value": "3"}}}},
    "my": {"tables": {"shipments": {"columns": ["order_id", "promised_at", "delivered_at", "status"], "all_rows": True}}}}}
ACCESS = {
    "a": {"revision": 1, "principals": {
        "ops": {"kind": "service", "native_sources": ["pg", "my", "ch"],
                "federated_sources": ["pg", "my", "ch", "cold", "tenant_marker"] + SNAPSHOTS, "allow_literal_queries": True},
        "seller3": {"kind": "user", "federated_sources": ["pg", "my"], "row_column_policy": SELLER3}}},
    "b": {"revision": 1, "principals": {
        "ops": {"kind": "service", "native_sources": ["my", "ch"], "federated_sources": ["my", "ch", "tenant_marker"],
                "allow_literal_queries": True}}},
}
fcc.LIMITS.update(max_rows=60_000_000, max_bytes=8 << 30, timeout="600s", memory_mb=768, threads=2, max_temp_mb=8192)
fcc.SLOTS.update(a1=2, a2=2, b1=1)
_configuration = fcc.configuration
# cluster_fixture.yaml cannot emit scalar lists (grants, dataset sources); PyYAML
# keeps top-level keys such as sandbox_path on their own line for start().
fcc.cf.yaml = lambda value, level=0: yaml.safe_dump(value, sort_keys=False)


def configuration(value):
    value = _configuration(value)
    if isinstance(value, dict):
        if "tenant_id" in value and "workers" in value and "limits" in value:
            value.update(max_queries=64, job_ttl="15m", access=json.loads(json.dumps(ACCESS[value["tenant_id"]])))
        if "tenants" in value and "max_concurrent" in value:
            value.update(max_queries=128, max_concurrent=5, max_http_requests=256,
                         authentication={"keys_file": str(KEYS), "reload_interval": "1s", "min_revision": 1})
            for tenant in value["tenants"]:
                tenant.pop("token_env", None)
        if "accounts" in value:  # job KV = (max_queries + slots) * 512 KiB, three replicas
            for account in value["accounts"].values():
                account["jetstream"].update(max_file=256 << 20, max_memory=64 << 20)
        if "resources" in value:
            value["resources"].update(max_concurrent=2, memory_mb=3072, scratch_mb=16384)
    return value


def provision():
    keys = {"a": {"ops": secrets.token_hex(32), "seller3": secrets.token_hex(32)}, "b": {"ops": secrets.token_hex(32)}}
    fcc.cf.DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    document = "version: 2\nrevision: 1\nprincipals:\n" + "".join(
        f"  {tenant}:\n" + "".join(f"    {name}:\n      - {json.dumps(key)}\n" for name, key in principals.items())
        for tenant, principals in keys.items())
    fcc.cf.write(KEYS, document)
    fcc.private_json(CLIENT_KEYS, keys)
    fcc.configuration = configuration
    fcc.provision()


if __name__ == "__main__":
    if sys.argv[1:2] == ["provision"]:
        provision()
    else:
        fcc.main()
