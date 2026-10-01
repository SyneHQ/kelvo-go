# Elasticsearch SQL

Kelvo uses Elasticsearch's synchronous SQL API, with cursor pagination and Arrow
delivery. The Elasticsearch deployment must expose the SQL API; availability and
licensing depend on the separately operated server distribution and version.

```yaml
sources:
  - id: search
    type: elasticsearch
    url_env: KELVO_SOURCE_ELASTICSEARCH_URL
    token_env: KELVO_SOURCE_ELASTICSEARCH_TOKEN
    options:
      authentication: ApiKey
```

Use a verified HTTPS origin. The token is the encoded API key expected by the
`Authorization: ApiKey` header. `Bearer` is also supported. Restrict credentials
to SQL search and read access on the intended indices. Native requests use a
single SELECT and `connection_id: search`; parameters are currently unsupported.

The connector requests row-oriented JSON pages of at most 1,000 rows, UTC
timestamps, and strict rejection of multivalued fields. NULLs and signed 64-bit
integers remain exact. Supported SQL result types include Boolean, byte, short,
integer, long, finite floating-point values, keyword/text/IP/version strings,
binary, and date/datetime within Arrow's nanosecond timestamp range. Unsupported
types, including objects and unsigned_long, fail explicitly.

Each response must fit the shared bounded HTTP budget. Partial and asynchronous
results are rejected. Cursor cleanup runs on a separate short deadline after
failure, cancellation or a sink error. Before the initial response supplies a
cursor, cancellation relies on the HTTP request and provider request timeout;
there is no confirmed server cancellation handle in that state.

Protocol tests verify pagination, exact large numbers, NULLs, partial-result
rejection, limits and cursor cleanup. Live Elasticsearch acceptance and
throughput measurements remain pending. This is a SQL connector; it does not
currently expose arbitrary search DSL, indexing, or CDC.

References: [SQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query),
[cursor cleanup](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-clear-cursor).
