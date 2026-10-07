# Database operations

Kelvo can execute database operations submitted by a trusted application gateway. The gateway owns user authorization and saved connection lookup; a contained worker opens the customer connection.

Adapters reject unsupported operations explicitly. The application gateway does
not need customer database drivers.

Use the [public Go SDK](application-sdk.md) for signing, submission, status,
validated Arrow results and uncertain-write lookup. The
[independent application](../examples/application/) demonstrates a transactional
write followed by exact-value reads through your own credential authority.

## Request flow

1. Authorize the saved connection ID and sign the exact operation, team, subject and deadline.
2. `POST /v1/operations` retains a credential-free grant and a reference to the private request body.
3. An assigned worker reserves resources and result storage, then fetches fresh credentials over private mTLS.
4. The adapter receives credentials through stdin, returns Arrow through stdout and a bounded receipt through fd 3.
5. Poll `GET /v1/operations/{id}`. Download a completed result from `GET /v1/operations/{id}/results`.

Status and result requests never execute SQL. Credentials do not enter the ledger, queue, argv or child environment.

## Ingestion inputs

1. Authorize the batch's exact SHA-256, size, connection and caller with an input-upload grant.
2. Upload to `POST /v1/operation-inputs`; retain the returned `InputRef`.
3. Sign `ingestion.commit` with that reference, batch ID and expected sequence. Retain this exact request before submitting it.

The worker verifies sealed bytes under resource admission before resolving credentials. Uploads are capped at 1 MiB; they grant no source execution. Re-uploading creates a new reference, so never replace a submitted operation's input when reconciling it.

The optional PostgreSQL adapter commits records, content history, checkpoint and batch receipt in one transaction. A changed retry or sequence conflicts. `ingestion.state` only reads state; `ingestion.install` requires separate write authority. Ingestion responses contain one Arrow binary column named `ingestion`, holding the validated receipt or state object.

The application gateway retains durable pending-operation records and rechecks
ingestion-job authority before resolving credentials. The adapter owns source
transactions and checkpoints.

## Enable a worker

Add this to a contained cluster worker with configured audit, scratch, resource budgets and an on-demand connection resolver. Paths must be private and persistent where indicated.

```yaml
operations:
  input_url: https://gateway.internal:8444
  tls:
    ca_file: /etc/kelvo/ca.pem
    cert_file: /etc/kelvo/worker.pem
    key_file: /etc/kelvo/worker-key.pem
  adapter:
    binary: /opt/kelvo/bin/kelvo-adapter-go
    sha256: <verified-release-sha256>
  results:
    directory: /var/lib/kelvo/operation-results
    max_entries: 128
    max_stored_bytes: 2147483648
  max_result_bytes: 8388608
  max_concurrent: 2
  max_downloads: 2
  poll_interval: 100ms
```

Provision `policy.operations` with `shards`, `slots_per_shard`, `retention` and `execution_timeout`. Grant each service principal an explicit `operations` allowlist; ordinary query grants cannot authorize operations. Execution is capped at five minutes.

The gateway also needs a separate `operations.listen` mTLS listener and per-tenant `operations.inputs` storage budgets. Run `cluster-init` with provisioning credentials before starting runtime workers. Allow one additional tenant-account NATS stream, `KV_KELVO_OPERATIONS_GATEWAY`; runtime identities need stream inspection, message reads and CAS writes, not provisioning rights.

Operation TLS uses pinned, verified service identities and static certificate files. Restart with a valid replacement before certificate expiry. Adapter binary content is verified again on the descriptor used for execution.

## Admission capacity

A read without an idempotency key can try up to 16 distinct shards after a definite capacity rejection, within one storage deadline. Writes and keyed reads keep their original shard. Retained records and policy limits stay unchanged.

A verified `429` means this submission was not admitted. It does not settle an earlier attempt with the same key. The SDK returns `OperationRejectedError`; lost acknowledgements and failures after admission remain `OperationUncertainError`. Neither error triggers automatic replay.

## Retry and outcomes

| Outcome | Meaning | Next action |
| --- | --- | --- |
| `completed` | Source effect is confirmed | Read the receipt; a write may succeed even if result delivery fails |
| `rejected` | Execution was prevented; no effects | Correct the cause before a new attempt |
| `failed` | Read failure or known partial effects | Inspect step receipts |
| `outcome_unknown` | Source effects cannot be established | Reconcile with the source; never replay automatically |

Use a stable idempotency key for retries of the same mutation. A new key creates a new operation. Retention is bounded; keys are not permanent deduplication records.

If the submission ID is lost, `POST /v1/operations/lookup` with `version: 1`, the original `idempotency_key` and `request_sha256`, plus a current operation grant. Lookup reads existing custody only. A `404` can mean retention expired; reconcile the source before deciding what to do next.

Results remain on the assigned worker's private volume. A restart with that volume can serve them; loss of the volume does not replay the source statement. Encoded bytes, rows and SHA-256 are verified before download completion. For large analytical extracts, use [exports](exports.md).

See [migration operations](migration-operations.md), [cloud SQL bindings](cloud-sql-bindings.md),
[Go adapters](../adapters/go/README.md), [process containment](process-containment.md), [resource admission](node-capacity.md) and [on-demand connections](on-demand-connections.md).

Check [validation coverage](database-operations-validation.md) before choosing an adapter.
