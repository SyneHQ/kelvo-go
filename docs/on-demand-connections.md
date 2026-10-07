# On-demand connections

This guide covers analytical queries. For supported writes, metadata and background jobs, use the separate [database operation protocol](database-operations.md).

Use a trusted resolver when your application discovers tenant connections at
request time. Configure deployment trust once; queries select saved IDs.
The resolver owns metadata authorization and credential decryption.
Your application chooses its identity provider, connection store and secret store.

```mermaid
sequenceDiagram
    participant App as Application / scheduled job
    participant API as Your application API
    participant Gateway as Kelvo gateway
    participant Worker as Assigned worker
    participant DB as Customer databases
    App->>API: SQL + saved connection IDs
    API->>API: Authenticate and authorize selections
    API->>Gateway: SQL + signed, credential-free grant
    Gateway->>Worker: Durable job assignment
    Worker->>API: Resolve selected connections over mTLS
    API->>Gateway: Verify current job and worker custody
    API->>API: Recheck access; decrypt current metadata
    API-->>Worker: Selected sources and secrets
    Worker->>DB: Native query / selected federation scans
    DB-->>Worker: Data
    Worker-->>API: Arrow through gateway
    API-->>App: Arrow or bounded JSON
```

## Integration status

The configured-source [CLI and HTTP API](usage.md) work independently. The
on-demand path currently requires a custom authority service implementing Kelvo's
resolver and worker-custody protocol.

Public [operation requests and grant helpers](../operations/) are available.
Analytical signing and resolver payload types still live under Go `internal/`
packages. A complete public SDK, callback specification and standalone example
remain open work. This guide covers the deployment trust and execution boundaries.

## Configure trust

The tenant service principal explicitly permits one resolver. Include this in
the immutable policy used by the initializer, gateways and workers:

```yaml
access:
  revision: 1
  principals:
    application:
      kind: service
      delegated_resolver:
        issuer: app-gateway
        audience: kelvo-production
        url: https://gateway.internal:9443/internal/kelvo/resolve
        public_key: BASE64_ED25519_PUBLIC_KEY
```

Add the matching worker transport configuration:

```yaml
connection_resolvers:
  app-gateway:
    url: https://gateway.internal:9443/internal/kelvo/resolve
    ca_file: /etc/kelvo/resolver-ca.pem
    cert_file: /etc/kelvo/worker.pem
    key_file: /etc/kelvo/worker-key.pem
    timeout: 15s
    max_concurrent: 4
```

Worker certificates require client-auth usage and the URI
`spiffe://kelvo/tenant/<tenant>/worker/<worker>`. URLs, keys and trust are operator
settings. Restart workers to reload resolver TLS files; policy updates require
the existing drained cutover.

Principal names, issuers and hostnames are operator choices. The query resolver
must implement the `/internal/kelvo/resolve` endpoint and verify current worker
custody before returning credentials.

## Execution boundary

- Grants bind deployment, principal, application team, subject, query and selected
  tables. They expire within five minutes.
- Submission and every handle request require the same `X-Kelvo-Delegation`.
  A shared service token alone cannot access another team's delegated results.
- The resolver checks the Running job and current worker custody before releasing
  credentials. A worker certificate alone is insufficient.
- Workers build a detached catalog and pass only selected secrets to the sandbox.
  No ambient-secret fallback or static catalog mutation.
- Normal process admission and query limits apply. Stable connection identities
  include issuer/team/saved ID; SQL aliases cannot impersonate static source quotas.

Credential responses cross the private network over mTLS and stay in process memory;
the resolver does not create shared credential files. Credentials never enter query
payloads or JetStream. Grants contain saved IDs and subject scope: treat those as
application metadata. Use read-only source credentials
and network egress controls appropriate to your database topology.

## Current scope

Cluster mode only. Native sources and selected federation tables are supported;
federation needs the [callback bridge build](federation.md#build-and-update).
Static/dynamic mixing, delegated exports, acceleration and delegated row/column
principal policies are rejected. Dynamic per-connection quota configuration and
source-health reporting are not yet exposed.

Each execution resolves fresh credentials. Receipt expiry bounds credential
delivery; query duration uses grant expiry and normal worker timeout.
Cancellation allows five seconds of grant grace solely for matching-job cleanup.
