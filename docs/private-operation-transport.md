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
      max_data_per_session: 2
```

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
- Private production activation remains blocked for both engines. A real queued PostgreSQL test returned a terminal cancellation receipt while `pg_sleep(10)` remained active for about ten seconds. An earlier MySQL fixture also left server work active after cancellation. Local process cleanup does not prove that source queries stopped.
- Rabbit v2 cancellation capacity remains disabled. The issuer and source must confirm cancellation before this gate can pass.
- Restart the worker to change these transport settings or its client certificate.

## MySQL cancellation

The relational adapter has an optional cancellation dialer for reads. It obtains
`CONNECTION_ID()` from the authenticated data session, uses the same source account
on a separate control connection, and joins `KILL QUERY` before it closes data.
A failed control request remains unconfirmed. This does not add write rollback guarantees.

This path requires trusted composition to supply `DialCancellation`. The private
worker does not supply it for MySQL yet. Rabbit cancellation tickets and capacity
reservation must pass live qualification before that runtime path is enabled.
