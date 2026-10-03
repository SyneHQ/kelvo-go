# Principal source access

Cluster gateways can bind each API key to a user or service principal. A principal can use only its configured sources and its own query handles. This is opt-in; legacy tenant keys retain tenant-wide access.

Grants cover whole sources by default. Add [row/column rules](row-column-access.md) for callback federation or [guarded snapshots](guarded-snapshots.md); other restricted execution paths fail closed.

## 1. Grant sources

Add identical `policy.access` to the gateway tenant and every worker serving it:

```yaml
policy:
  # Keep the existing tenant, scheduling and resource fields.
  access:
    revision: 1
    principals:
      analyst:
        kind: user
        native_sources: [sales_readonly]
        federated_sources: [sales, daily_sales]
      scheduled-reports:
        kind: service
        federated_sources: [daily_sales]
        allow_literal_queries: true
```

Grant source IDs from the worker catalog. A snapshot dataset can appear in `federated_sources`; that grants the whole published dataset. Unlisted sources are denied. `allow_literal_queries` permits queries selecting no catalog sources, such as `SELECT 1`; it does not attach additional sources.

Limits: 64 principals per tenant, 64 source IDs per mode, and explicit `user` or `service` kinds. No wildcards. A principal with empty grants is disabled for queries.

## 2. Bind keys

Use [file authentication](gateway-key-rotation.md), remove every `token_env`, and provision a private version-2 key document:

```yaml
version: 2
revision: 1
principals:
  analytics:
    analyst:
      - REPLACE_WITH_A_NEW_RANDOM_KEY
    scheduled-reports:
      - REPLACE_WITH_ANOTHER_RANDOM_KEY
  disabled-tenant: {}
```

List exactly the gateway's tenant IDs. Keys have the existing 32–256-byte bound, with at most four overlapping keys per principal. Unknown principals and tenant-only keys cannot use a principal-enabled policy. Request headers cannot override identity.

## 3. Roll out safely

1. Upgrade every gateway and worker before enabling access policies.
2. Drain existing jobs. Provision the new policy in a fresh tenant NATS account and cut over its worker/gateway configuration together; preserve the old account until retirement is verified.
3. Test allowed and denied sources, then verify foreign query status, cancellation and results return `404`.
4. Rotate keys using higher key-file revisions. Verify every replica and raise `authentication.min_revision` before restarts.

Tenant policy metadata must match exactly across replicas. Access-policy changes require another drained cutover; do not edit broker metadata underneath running workers. Older binaries reject the new policy metadata. There is no automatic policy migration or mixed-policy rollout.

Key removal cancels active requests. Completion also checks current key membership and expiry before releasing final Arrow EOS. Already delivered bytes or committed results cannot be recalled. Revocation reaches each replica only after its operator-managed key-file update.

Never reuse a key for another identity. A gateway retains up to 8,192 token-to-identity bindings, including retired keys, for its process lifetime. Exceeding that bound rejects new bindings without eviction; restart with the current file/revision floor when needed. Binding history is not persisted across restarts.

## Boundaries

- Native SQL and federation are checked before durable admission and again on the worker. Only selected source credentials enter query children.
- Jobs retain an immutable principal and policy digest. Another key for the same principal can use its unconsumed handles; another principal cannot.
- The digest identifies the source policy, not a signature. Gateway/worker processes, operator configuration and NATS state writers remain trusted.
- Scheduled refreshes use operator service authority. Standalone `serve`, export/cache policy integration and coordinated global revocation remain separate capabilities.

[Validation record](evidence/principal-authority.json) covers package/race and TLS relay fixtures; real broker/provider rollout gates remain separate.

[Security model](../SECURITY.md) · [Cluster setup](cluster.md) · [Production gates](production-status.md)
