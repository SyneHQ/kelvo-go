# Arrow Flight SQL source

Read typed Arrow results from a Flight SQL service. Plain Flight services need a separate adapter and are not accepted as SQL endpoints.

1. Provision a read-only service account and bearer token.
2. Register the Flight SQL endpoint:

```yaml
sources:
  - id: warehouse_flight
    type: arrow_flight
    url_env: KELVO_SOURCE_FLIGHTSQL_URL
    token_env: KELVO_SOURCE_FLIGHTSQL_TOKEN
    options:
      protocol: flightsql
```

3. Set the URL to `grpcs://host:port`, then submit native SQL for `warehouse_flight`.

The endpoint requires an explicit port, TLS 1.2+ and verified hostname/certificate. Userinfo, paths, queries, fragments and plaintext are rejected. Each query resolves its token and sends it only as gRPC `authorization: Bearer …` metadata.

Results retain their Arrow schema and use synchronous borrowed batches under deadline, row, byte and Arrow-memory limits. Service grants remain the read-only boundary alongside Kelvo's conservative SQL guard.

`FlightInfo` must contain exactly one endpoint. Locations must be absent or match the configured TLS host and port; external locations cannot redirect credentials.

Writes, ingest, metadata browsing, transactions, prepared statements, CDC and user-exposed actions are unsupported. After `FlightInfo` arrives, failures trigger bounded best-effort `CancelFlightInfo`; servers may not implement it, so provider quotas remain necessary. The [worker cancellation grace](usage.md#native-cancellation-and-remote-cleanup) can end before remote cleanup finishes.
