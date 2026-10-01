# Optional external adapters

Kelvo can use a separately operated adapter service for an engine without a
built-in driver. The service must implement the selected protocol and enforce
read-only access to the intended backend. Kelvo does not ship that service or
its JDBC/vendor drivers. There is no automatic fallback from a native connector.

## Arrow Flight SQL

```yaml
sources:
  - id: legacy_warehouse
    type: db2
    adapter: flightsql
    url_env: KELVO_SOURCE_DB2_FLIGHT_URL
    token_env: KELVO_SOURCE_DB2_FLIGHT_TOKEN
```

Use a `grpcs://host:port` endpoint with verified TLS and a source-scoped bearer
token. The adapter must be a real Flight SQL service; a JDBC driver or generic
Flight server alone is not sufficient. Configure a separate endpoint or narrowly
scoped identity for each source. The source name remains bound in Kelvo; endpoint
tickets cannot redirect its credentials elsewhere. This route preserves Arrow
schema/types and uses the limits documented in [Flight SQL](sources-flight.md).
It currently accepts one result endpoint and SQL without parameters.

## Database gateway compatibility

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

`adapter: dbapi` sends read-only SELECT/WITH SQL to a separately operated
`db.api.go` gateway. It is a compatibility path with bounded JSON materialization.
A source binds the configured remote connection ID; callers cannot provide
connection details or replace that ID. Use an HTTPS origin and a read-only API
key whose upstream connection permissions are restricted to the intended scope.
No database passwords or DSNs enter the Kelvo adapter configuration.

The adapter sends only top-level `id` and `query` to
`POST /api/v1/metadata/query`, authenticates with `X-API-KEY`, and never sends
user, service, or administrator impersonation headers. The gateway's middleware
must resolve that ID into authorized stored connection details. SQL parameters,
MongoDB aggregation payloads, writes and arbitrary vendor commands are currently
unsupported through this route.

The gateway materializes its response. Kelvo accepts only a complete JSON
response containing `results` and a matching `rowCount`. A null results value
with zero rows is accepted as an empty result. Every nonempty result must be a
JSON object. Each original object becomes an Arrow Binary `document_json` value
with `content_type=application/json`. Number/value bytes are preserved as
received; types or precision already lost upstream cannot be recovered.

The response is capped at the minimum of 32 MiB, the output-byte limit, and one
quarter of the configured memory budget. Arrow batches are also bounded and
contain at most 1,024 documents. These allocation budgets are not a process RSS
limit. Large exports should use a native or typed Arrow route. Upstream has no
query-cancel operation in this contract: canceling Kelvo closes the local request
but cannot guarantee the remote query stops.

TLS protocol tests cover source binding, exact JSON integer bytes, response
validation, batch delivery, local cancellation and limits. They are not live acceptance
against every database or proof of source-side query isolation. The two declared
SAP entries in the [coverage matrix](source-coverage.md) need a custom adapter;
the reference gateway does not have a complete connection builder for them.
