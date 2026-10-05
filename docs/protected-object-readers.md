# Protected object snapshots

Opt into durable reader leases for one new object namespace. A contained node shares one runtime across queries, exports, refreshes and dataset diagnostics. Existing namespaces keep their legacy behavior.

## Enable

1. Choose a **fresh prefix**. Protected v5 and legacy v2–v4 manifests cannot share a namespace; there is no in-place cutover.
2. Configure [object storage](object-storage.md) with three separate identities: data reader, publisher and reader registry. Use dedicated environment references, never secret values in YAML.
3. Add this fragment inside `acceleration.object_storage` for S3, R2 or GCS:

```yaml
reader_registry:
  credentials:
    access_key_id_env: KELVO_SOURCE_REGISTRY_ID
    secret_access_key_env: KELVO_SOURCE_REGISTRY_SECRET
```

For Azure, use `sas_token_env: KELVO_SOURCE_REGISTRY_SAS` instead. S3 temporary credentials can also set a dedicated `session_token_env`.

4. Restrict registry rights to exact reads and conditional create/replace under `<prefix>/<tenant>/<dataset>/reader-leases/`. Data readers remain read-only; no identity needs list/delete rights. YAML rejects shared reference names; administrators must also provision distinct, scoped identities.
5. Configure the node's [Linux containment](process-containment.md), sandbox and [managed scratch](worker-scratch.md). Run refreshes through the node and submit queries through its gateway.

All three identities load in the parent runtime, including on query nodes. Provider identity and endpoints never enter the query child. Changing the catalog, credentials, endpoint or trust requires draining and restarting the node; live retargeting is refused.

## Supported paths

| Operation | Protected namespace |
| --- | --- |
| Full refresh, single-file or multipart | Stage before upload; seal verified metadata before fenced v5 publication |
| Contained query or export | Select roots once; acquire all leases before descriptors, generation metadata or ranges |
| Status and previous-schema comparison | Read under a lease and check ownership before returning |
| `accelerate refresh`, `watch`, `status` | Runtime-aware operator commands; contained node refresh is the managed path |
| Standalone `query` / `serve` | Refused; use a contained node |
| Verify and inventory | [Explicit byte budget](protected-verification.md); one pinned selection and deadline; [Linux acceptance](evidence/protected-verification.json) passed |
| Historical restore | [Verify both generations and swap only the pointer](protected-restore.md); [Linux acceptance](evidence/protected-restore.json) passed |
| Migration backup | Refused before generation access; integration remains pending |
| Remote pruning or garbage collection | Disabled |

Principal, row and column authorization still apply. A lease protects a selected generation; it does not grant access to its data. Protected reads use the parent range bridge; no cloud extension is installed during a query.

## Cleanup and limits

Cancellation stops execution while leases stay owned until range handlers, the contained process tree and scratch cleanup finish. A stalled cleanup retains capacity and returns an error. Upstream close failures and lease loss after the last batch prevent successful completion.

| Shared runtime bound | Limit |
| --- | ---: |
| Active operations | 64 |
| Unfinished guards / reserved generation leases | 128 / 128 |
| Generations acquired together | 64 |
| Requests or open bodies per transport | 64 |
| Owned dials per transport | 4 |

Data and registry traffic use separate transports. These bounds describe admission, not total RSS or throughput. Native execution still needs measured headroom.

## Validation and remaining work

[Linux acceptance](evidence/protected-object-readers.json) passed all ten stages on `c8bae34`, including 36 runner controls and seven protected worker cases. The fixture exercises TLS object publication and real contained DuckDB queries; it does not validate cloud IAM, signatures, throughput or footprint. Refresh input is deterministic Arrow data.

The worker checks include catalog-bound executor copies and externally owned export reservations. [Authenticated query acceptance](protected-query-acceptance.md) now covers the complete gateway, NATS, node and contained-worker path with two tenants and real multipart data. All 11 VM stages passed on `15bd93f`; the public protected-export lifecycle remains a separate gate.

[Protected verification and inventory](protected-verification.md) also passed [Linux acceptance](evidence/protected-verification.json) on `c8e4199`. [Historical restore](protected-restore.md) passed all 13 [Linux stages](evidence/protected-restore.json) on `9d8789e`. Migration, legacy cutover and provider acceptance remain under [#14](https://github.com/SYNEHQ/kelvo-go/issues/14). No lease expiry, empty registry or node restart permits deletion. Retirement needs a separate protocol.

[Registry contract](durable-reader-registry.md) · [Reader ownership](reader-owner.md) · [Production checklist](production-status.md)
