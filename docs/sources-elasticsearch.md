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
single SELECT and `connection_id: search`. Positional `?` parameters are bound in
the SQL API's `params` array; values are never interpolated into SQL. String,
Boolean, signed int64, finite float64 and explicit NULL are supported. uint64
parameters must fit signed int64.

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
rejection, limits and cursor cleanup. [Live Elasticsearch 8.19.0 acceptance](evidence/elasticsearch-native.json) also
passed against a verified-TLS server with an index-restricted API key: 1,205
rows across multiple pages, integers above 2^53, NULLs, bound parameters,
output limits, denied index access, and zero open search contexts afterward.
Run `scripts/elasticsearch_acceptance.py` on a disposable Linux test VM to
reproduce it. This tests the connector package, not cluster execution or throughput. This is a SQL connector; it does not
currently expose arbitrary search DSL, indexing, or CDC.

References: [SQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query),
[bound parameters](https://www.elastic.co/docs/reference/query-languages/sql/sql-rest-params),
[cursor cleanup](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-clear-cursor).

Worker boundary: the cleanup described here requires the connector process to
remain alive. CLI/HTTP/cluster cancellation or an outer deadline can kill that
process before remote cleanup runs. See [native cancellation and remote cleanup](usage.md#native-cancellation-and-remote-cleanup).
