# Integrate Kelvo into your application

Kelvo runs as a separate service. Your application chooses its identity provider,
connection store, secret store and deployment layout. The Go SDK has no dependency
on a particular application, metadata schema or KMS.

[Runnable application](../examples/application/) · [HTTP API](usage.md#http-api) · [On-demand trust](on-demand-connections.md) · [Operations](database-operations.md)

Build the server from the SDK's pinned Kelvo revision. The `v0.1.0-preview.1`
notebook binary predates this integration. [Deployment guide](../deploy/README.md)

## Choose a connection model

| Model | Your application sends | Setup |
| --- | --- | --- |
| Operator-configured sources | Query, source IDs and service token | Register sources in Kelvo YAML |
| On-demand saved connections | Query or operation, service token and signed grant | Register your authority once; resolve current credentials per execution |

The second model requires cluster mode. Credentials travel from your authority to
the assigned worker over private mTLS. Query bodies and NATS contain no credentials.

## Use the public packages

| Package | Purpose |
| --- | --- |
| [`client`](../client/) | Bounded HTTPS query, operation, result and custody client |
| [`query`](../query/) | Query requests, parameters, Arrow sink and statistics |
| [`delegation`](../delegation/) | Sign and verify analytical saved-connection grants |
| [`operations`](../operations/) | Typed operations, grants, receipts and reconciliation |
| [`resolver`](../resolver/) | Credential callback DTOs and mTLS authority handler |

Import these packages directly. Go `internal/` packages are runtime details.
Public client and authority applications build with `CGO_ENABLED=0`; they do not
link DuckDB, source database drivers or NATS.

```go
c, err := client.New(client.Config{
    URL:         "https://kelvo.example.com",
    BearerToken: os.Getenv("KELVO_SERVICE_TOKEN"),
})
if err != nil { return err }
defer c.Close()

stats, err := c.Query(ctx, query.Request{
    Mode: "native", ConnectionID: "warehouse",
    SQL: "SELECT region, SUM(amount) FROM sales GROUP BY region",
}, client.Authority{}, sink)
```

`sink` implements `query.Sink`. Each Arrow batch is borrowed until `Write` returns.
Consume it synchronously or explicitly retain and release it. A nil error confirms
physical Arrow EOS, HTTP EOF and the gateway's successful terminal result counts.

## Add on-demand authority

1. Authenticate the caller and authorize their saved connection in your application.
2. Sign the exact request with `delegation.Sign` or `operations.SignGrant`.
3. Pass it in per-call `client.Authority`; never put signing keys in the client UI.
4. Run `resolver.NewHandler` on a TLS 1.3 listener requiring verified worker certificates.
5. Supply current policy and credential lookup hooks. Use the public client as its `LeaseVerifier`.

The [separate-module example](../examples/application/) implements these steps with
two application teams and YAML policy. Replace its file store with your own database
or secret manager; the Kelvo protocol stays the same.

The handler verifies the signed request, tenant-bound worker identity and live
owner/claim lease before loading credentials. It rechecks policy revision and
custody afterwards. Each material result must specify its credential `ValidUntil`;
delivery is also capped by grant, policy, certificate and short lease expiry.

## Implement only the callbacks you need

| Endpoint | Required application hooks |
| --- | --- |
| `/internal/kelvo/resolve` | `AuthorizeQuery`, `ResolveQuery` |
| `/internal/kelvo/resolve-operation` | `AuthorizeOperation`, `ResolveOperation` |
| `/internal/kelvo/complete-operation` | `AuthorizeCompletion`, `CompleteOperation` |
| `/internal/kelvo/operation-file` | Operation authorization and `OpenFile` |
| `/internal/kelvo/operation-file-commit` | Operation authorization and `LockPublication` |

These are private network endpoints of your authority, despite the SDK being public.
Absent hooks reject requests. Query and operation request DTOs keep the existing v1
wire format; credential responses explicitly carry `version: 1`.

`Authorization.Revision` must cover every metadata or permission change that revokes
access. Hooks must check current membership, saved-connection ownership, selected
database/schema and any job or approval scope. A valid signature cannot replace
those checks. Do not log credentials, grants or request bodies.

Completion releases retained custody only after the exact worker reports physical
cleanup. Compare the complete retained request/digests/owner/claim atomically when
releasing it. Expired grants or terminal ledger states alone do not prove cleanup.
File hooks use immutable descriptors, staged checksum verification and publication
locks; they never accept caller-selected filesystem paths. See the
[`resolver` contracts](../resolver/contract.go) and [file hooks](../resolver/file.go).

## Bound work and handle failure

- Defaults: 8 active data calls, 8 control calls, 30-second timeout, 1 million result
  rows, 256 MiB encoded and decoded limits. Set explicit bounds for your workload.
- Admission fails immediately when full. Control calls remain available for cancellation
  and custody checks. One client is safe for concurrent callers with separate grants.
- HTTPS certificates and hostnames are verified. Redirects, ambient proxies and automatic
  request replay are disabled. Reads use fresh HTTP connections with TLS session resumption.
- `Close` cancels network work. Your hooks and sinks must honor cancellation; Go cannot
  forcibly stop arbitrary application code while it owns a borrowed batch.
- Preserve a mutation's request, idempotency key and grant until its outcome is known.
  Reconcile an uncertain write with operation lookup; do not resubmit it automatically.

On-demand analytics currently exclude acceleration, durable exports and mixed
static/dynamic source queries. Connector coverage and deployment acceptance remain
separate from SDK availability. [Coverage](source-coverage.md) · [Production gates](production-status.md)

## Verify an independent build

```sh
python3 scripts/public_sdk_acceptance.py --output /tmp/kelvo-sdk-check
```

This copies the example outside the checkout, builds it with `GOWORK=off`, checks
its dependency boundary, and runs its normal/race tests. Candidate mode explicitly
replaces only the Kelvo module. Add `--published-version <version>` to verify a
public module without local replacements. Use a new output directory for each run.

[Recorded Linux validation](evidence/public-application-sdk.json) includes the
eight-scenario PostgreSQL application fixture and its retained failed trials.
