# Elasticsearch SQL

Read Elasticsearch SQL results through its synchronous API, with cursor pagination and Arrow delivery. The separately operated server must expose SQL; availability and licensing depend on its distribution/version.

1. Create an API key restricted to SQL/read access on the intended indices.
2. Register a verified HTTPS origin and encoded token:

```yaml
sources:
  - id: search
    type: elasticsearch
    url_env: KELVO_SOURCE_ELASTICSEARCH_URL
    token_env: KELVO_SOURCE_ELASTICSEARCH_TOKEN
    options:
      authentication: ApiKey
```

3. Submit one native SELECT with `connection_id: search`. Positional `?` values bind through the API's `params` array.

`Bearer` authentication is also supported. Parameters accept strings, Boolean, signed int64, finite float64 and explicit NULL; uint64 must fit int64.

Pages contain at most 1,000 rows, use UTC timestamps and reject multivalued fields. Results preserve NULLs and integer widths. Supported types include Boolean, byte/short/integer/long, finite floats, keyword/text/IP/version strings, binary and nanosecond-range date/datetime. Objects, unsigned_long and other unsupported types fail.

Responses must fit the shared HTTP budget. Partial/asynchronous results are rejected. Failure, cancellation or sink errors trigger bounded cursor cleanup once a cursor is known. Before then, only HTTP cancellation/provider timeout is available. See [worker cleanup limits](usage.md#native-cancellation-and-remote-cleanup).

[Live Elasticsearch 8.19.0 evidence](evidence/elasticsearch-native.json) covers verified TLS, restricted index access, exact values, parameters, pagination, limits and zero remaining search contexts. Reproduce with `scripts/elasticsearch_acceptance.py` on a disposable Linux VM. Cluster execution and throughput remain unverified; search DSL, indexing and CDC are unsupported.

References: [SQL API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query), [parameters](https://www.elastic.co/docs/reference/query-languages/sql/sql-rest-params), [cursor cleanup](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-clear-cursor).
