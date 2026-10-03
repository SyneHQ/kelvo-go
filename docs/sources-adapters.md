# Optional external adapters

Connect an unsupported engine through a separately operated Flight SQL or `db.api.go` service. Configure the route explicitly; Kelvo does not bundle that service, its drivers, or an automatic fallback.

## Arrow Flight SQL

1. Provision a real Flight SQL service with read-only backend access. A JDBC driver or plain Flight server is not enough.
2. Bind each source to its own endpoint or narrowly scoped identity:

```yaml
sources:
  - id: legacy_warehouse
    type: db2
    adapter: flightsql
    url_env: KELVO_SOURCE_DB2_FLIGHT_URL
    token_env: KELVO_SOURCE_DB2_FLIGHT_TOKEN
```

Use verified `grpcs://host:port` TLS and a source-scoped bearer token. This route preserves Arrow types, accepts one result endpoint and SQL without parameters, and rejects credential redirection. See [Flight SQL limits](sources-flight.md).

## Database gateway compatibility

1. Provision an HTTPS `db.api.go` gateway with a read-only API key restricted to the intended connection.
2. Bind the remote connection ID in the catalog:

```yaml
sources:
  - id: analytics_events
    type: ga4
    adapter: dbapi
    url_env: KELVO_SOURCE_GATEWAY_URL
    token_env: KELVO_SOURCE_GATEWAY_READ_TOKEN
    options:
      remote_connection_id: configured-connection-id
```

Kelvo sends only `id` and read-only SELECT/WITH `query` to `POST /api/v1/metadata/query`, authenticated with `X-API-KEY`. The gateway must resolve and authorize the stored connection. Callers cannot replace its ID or supply credentials. Impersonation headers, parameters, MongoDB pipelines, writes and vendor commands are unsupported.

A complete response must contain `results` and a matching `rowCount`; null results with zero rows are valid. Each nonempty result must be an object. Its original JSON bytes become Arrow Binary `document_json` with `content_type=application/json`; precision already lost upstream cannot be recovered.

The response cap is the smallest of 32 MiB, the output-byte limit and one quarter of query memory. Batches contain at most 1,024 documents. These are buffer limits; enforce process memory separately. Use native or typed Arrow routes for large exports.

Cancellation closes the local HTTP request; this gateway contract has no remote cancel operation. TLS fixtures verify the adapter contract, not every backend. The two SAP entries in [source coverage](source-coverage.md) need a custom adapter because the reference gateway lacks complete connection builders.
