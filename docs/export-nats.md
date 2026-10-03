# Export broker permissions

Durable exports are opt-in. Keep each tenant in a separate NATS account without
cross-account imports or exports. Use distinct initializer, gateway and worker
identities. An initializer provisions the declared namespace; runtime identities
can read its configuration and operate retained jobs but cannot create, update,
purge or delete streams or consumers.

Merge the additive grants in
[`deploy/examples/nats-export-permissions.yml`](../deploy/examples/nats-export-permissions.yml)
into the corresponding existing tenant-account permissions. This file is a YAML
permission-block template, not a complete NATS configuration. Preserve existing
query, refresh and source-quota grants. Apply the additions only when exports are
enabled. Keep initializer credentials out of gateway and worker deployments.

| Role | Optional export grants |
| --- | --- |
| Initializer | Inspect and provision `KV_KELVO_EXPORT_JOBS`, `KELVO_EXPORT_QUEUE` and the exact `exports` consumer |
| Gateway | Read export stream/consumer configuration, read/write retained export slots and publish `export.ready` |
| Worker | Read export stream/consumer configuration, read/write retained export slots, pull and acknowledge the exact `exports` consumer |

No role needs `$JS.API.>` or a global stream/consumer wildcard. Runtime KV direct
reads remain disabled. The initializer alone can read the exact bootstrap
metadata key before it disables direct access. Authentication and tenant
boundaries are enforced by NATS;
principal, authority, state-transition and retention checks remain Kelvo's
responsibility. Broker subject permissions do not validate a stream-create
request body: treat the initializer as a trusted administrator **within its
tenant account**, and never share one account between unrelated tenants.

## Offline acceptance

[`scripts/export_broker_acceptance.py`](../scripts/export_broker_acceptance.py)
uses an existing, checksum-verified NATS server. It never downloads software.
Supported fixture versions are explicitly `2.14.7` and `2.15.0`; `2.12` is not
covered. Every invocation starts its own loopback TLS broker in a fresh private
directory, runs one selected Go acceptance gate and stops the owned broker.

Run on a disposable Linux VM with the qualified Go and native bridge artifacts,
cached dependencies and OpenSSL. Execute inside a bounded non-root service: one
CPU quota, 2 GiB memory, no swap, 128 tasks, a 300-second watchdog and
`KillMode=control-group`. These are fixture limits, not production sizing.

```sh
python3 scripts/export_broker_acceptance.py --mode acl \
  --fixture /private/exports-acl --output /private/exports-acl.json \
  --nats-server /qualified/nats-server --server-version 2.15.0 \
  --server-sha256 <verified-server-sha256> --go /qualified/go \
  --modfile /private/offline.mod
```

The ACL gate provisions two accounts with separate restricted identities. It
executes submission, dispatch, assignment, acknowledgement, claim, running and
cancellation with the actual store APIs. Both accounts use identical namespace
subjects; foreign export IDs and dispatch remain inaccessible. Forbidden
management probes require a broker permission-violation event for the exact
subject, followed by unchanged retained state. A request timeout alone does not
count as denial.

The separate `--mode lifecycle` gate additionally requires
`--export-binary /qualified/kelvo-duckbridge` and
`--export-sandbox /qualified/kelvo-sandbox`. It launches its own fresh account
and runs `TestExportClusterActualWorkerLifecycle`, using separate initializer,
gateway and worker identities. It does not reuse ACL/provisioning-test state.
The launcher reports binary hashes, test outcomes, duration and broker cleanup;
the enclosing build must record source and module identities as well.

Keep fixture directories private: generated credentials, TLS keys, broker state
and raw logs are deliberately not publication artifacts. Publish only reviewed,
credential-free receipts. Neither gate demonstrates multi-host broker
availability, live-provider throughput or arbitrary-version compatibility.

[Durable export storage](export-storage.md) · [NATS compatibility](nats-compatibility.md)
