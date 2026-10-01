# Arrow Flight SQL source

Kelvo supports the **Arrow Flight SQL** protocol for native, read-only SQL queries. It does not treat arbitrary Arrow Flight services as SQL endpoints: a plain Flight service remains unsupported until a separate no-SQL adapter is implemented.

Configure an `arrow_flight` source with `protocol: flightsql`, an environment variable containing a TLS endpoint, and an environment variable containing the bearer token:

```yaml
sources:
  - id: warehouse_flight
    type: arrow_flight
    url_env: KELVO_SOURCE_FLIGHTSQL_URL
    token_env: KELVO_SOURCE_FLIGHTSQL_TOKEN
    options:
      protocol: flightsql
```

`KELVO_SOURCE_FLIGHTSQL_URL` must be exactly a secure `grpcs://host:port` endpoint. User information, paths, query strings, fragments, plaintext transport, and endpoints without an explicit port are rejected. Kelvo verifies the TLS certificate and hostname, with TLS 1.2 as the minimum version. The token is resolved for each query and sent only as the gRPC `authorization: Bearer …` metadata; it is never added to URLs or errors.

Use an account that is read-only at the Flight SQL service. Kelvo also applies its conservative read-only SQL guard, but database permissions are the authorization boundary. Results use the source Arrow schema and batches directly. The connector enforces query deadlines plus row, byte, and Arrow-memory limits while delivering borrowed batches synchronously.

A Flight SQL `FlightInfo` must contain exactly one result endpoint. It may omit endpoint locations, which means the configured server. Any supplied location must identify that same configured TLS host and port. Cross-endpoint and external locations are rejected, so credentials are never forwarded to a server selected by a result response.

This connector has no writes or ingest, metadata browser, transactions, prepared statements, CDC, or user-exposed action APIs.

When a local deadline, cancellation, limit, or delivery error occurs after Flight SQL has returned a `FlightInfo`, Kelvo makes a bounded best-effort `CancelFlightInfo` request to the same source. Servers may not implement that optional action, so cancellation cannot be guaranteed remotely; source-side read-only resource quotas remain required.
