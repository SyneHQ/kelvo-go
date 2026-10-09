# Private database operations

Private PostgreSQL and MySQL operations can use a parent-owned Rabbit tunnel. Public sources keep their direct native connection path.

This feature is opt-in. Production activation requires the complete deployment qualification. Private analytical federation is not supported.

## Configure a worker

1. Provision a delegated service principal and its connection resolver.
2. Use the same worker certificate for resolver delivery, ticket issuance, and the proxy.
3. Add `private_sources` under the worker's existing `operations` configuration:

```yaml
operations:
  # Retain the existing adapter, input, result, and concurrency settings.
  private_sources:
    analytics-api: # Must match a provisioned service principal.
      route_id: private-route
      proxy_address: rabbit.internal:14443
      proxy_server_name: rabbit.internal
      proxy_ca_file: ./pki/rabbit-ca.pem
      proof_public_key: BASE64_ED25519_PUBLIC_KEY
      ticket_public_key: BASE64_ED25519_PUBLIC_KEY
      max_sessions: 1
      max_data_connections: 2
      max_data_per_session: 1
      accepted_key_id: rabbit-cleanup-v1
      accepted_public_key: BASE64_ED25519_ACCEPTANCE_PUBLIC_KEY
```

The acceptance key verifies Rabbit receipts for physical connections. Use its separate public key, not the proof or ticket key.

Configure both acceptance fields together. Cleanup requires `max_data_per_session: 1`.

The issuer endpoint comes from the configured resolver origin. Requests cannot change the endpoint, route, trust roots, or worker identity.

The sum of `max_sessions` across principals must not exceed `operations.max_concurrent`. Connection limits include opens and closes that have not finished.

## Request flow

1. The cluster verifies the grant and reserves process, memory, scratch, and result capacity.
2. The resolver checks the current source and execution lease. It returns a signed private-source proof when a tunnel is required.
3. The parent verifies the proof and refreshes it before each physical connection.
4. The native adapter receives connected file descriptors. It retains the database's original TLS hostname.
5. Shutdown waits for operation and transport cleanup before releasing capacity.

An invalid private proof fails the operation. The worker never retries that source through a direct connection. Without `private_sources`, private responses remain rejected.

## Current limits

- Private operations support PostgreSQL and MySQL. Watcher, ingestion, and approved-change grants remain excluded.
- SQL read operations use this path. Analytical queries, federated queries, and tangent federation need separate qualification.
- Admission expiry and running-operation renewal are separate checks.
- Deployment acceptance remains required. A fixture pass does not qualify production credentials, routes or customer clients.
- MySQL source cancellation remains unqualified. Keep private MySQL disabled.
- Restart the worker to change these transport settings or its client certificate.

## PostgreSQL cancellation

The parent records Rabbit's accepted connection before the child receives it. On cancellation, ordinary execution stops and separate cleanup authority expires within five seconds.

The parent sends `CancelRequest` with the original source TLS policy. The trusted adapter must decode cancellation on the original session. Capacity remains held until the child, transport and scratch cleanup finish.

Missing confirmation, source revocation or a child crash returns `cleanup_unknown`. A transaction-local statement timeout limits each PostgreSQL read using the remaining operation budget. See [read deadline limits](postgres-read-deadlines.md). A closed socket alone never proves termination.

Queued TLS fixtures passed normal cancellation, actual child crash, source rebinding and near-expiry cancellation. Duplicate cancellation preserved the original cutoff. The earlier false-completion defect is corrected. Complete [deployment acceptance](private-transport-activation.md) before enabling traffic.

## MySQL cancellation

The relational adapter has an optional cancellation dialer for reads. It obtains
`CONNECTION_ID()` from the authenticated data session, uses the same source account
on a separate control connection, and joins `KILL QUERY` before it closes data.
A failed control request remains unconfirmed. This does not add write rollback guarantees.

This path requires trusted composition to supply `DialCancellation`. The private
worker does not supply it for MySQL yet. Rabbit cancellation tickets and capacity
reservation must pass live qualification before that runtime path is enabled.

## Diagnose a private connection

Operators can temporarily enable parent-side diagnostics on a worker:

```yaml
operations:
  adapter:
    # Keep the existing binary and sha256 values.
    private_open_diagnostics: true
```

Check the worker configuration before restarting it:

```sh
kelvo node --check-config --config node.yml
```

This read-only check validates YAML, policies, local artifact hashes and filesystem metadata. It does not start services, make network calls or resolve source secrets. A passing check does not prove runtime dependencies are available.

The default is `false`. The worker logs one `kelvo_private_open_diagnostic` JSON summary after its cleanup attempt. Each summary contains a validated operation ID, fixed stage/result codes, timing, HTTP status and `cleanup_confirmed`. It contains no source address, SQL, credentials, proof, ticket or error text.

Each operation retains at most 64 events. One process-local writer uses a 16-summary queue and drops new summaries when full. Logging never waits for the output sink on the request path. Reports can be lost during overload or process exit. These operator diagnostics are not audit records or proof of source effects.

A failed stage identifies the next investigation point. It does not relax TLS, tenant checks, timeouts or cleanup. Parent diagnostics end at descriptor delivery; they do not report database TLS or authentication inside the child. Disable the setting after investigation.
