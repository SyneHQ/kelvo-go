# Azure Cosmos DB for NoSQL

## Official API references

The connector design follows official REST documentation reviewed before
implementation:

- [Query documents](https://learn.microsoft.com/en-us/rest/api/cosmos-db/query-documents): POST the configured container's documents collection using `application/query+json`, continuation headers, and explicit cross-partition queries.
- [Access control and authorization signatures](https://learn.microsoft.com/en-us/rest/api/cosmos-db/access-control-on-cosmosdb-resources): URL-encoded AAD authorization, or SHA-256 HMAC over the lower-case verb/resource type/date and case-preserved parent resource link for master-key queries.
- [Pagination](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/pagination): empty pages can still have continuation tokens; completion requires exhausting the token chain.
- [Querying a container](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-query-container): cross-partition execution and its resource-unit cost.

## Configuration contract

Use source type `cosmosdb`, `url_env`, `token_env`, and required `database`,
`container`, and `auth` options. The origin must use HTTPS. Authentication is
explicitly `aad` or `master_key`; credentials are supplied only through the
configured environment reference. Kelvo does not discover Azure credentials.

```yaml
sources:
  - id: documents
    type: cosmosdb
    url_env: KELVO_SOURCE_COSMOSDB_URL
    token_env: KELVO_SOURCE_COSMOSDB_TOKEN
    options:
      database: Analytics
      container: Events
      auth: aad
      max_pages: "1000"
      max_request_units: "10000"
```

The URL is an account origin such as
`https://example-account.documents.azure.com`, without credentials, path, query,
or fragment. Database/container IDs preserve case and accept an ASCII letter or
digit followed by up to 127 ASCII letters, digits, dots, underscores, or hyphens.
This is a deliberate subset of service resource names. Unknown options fail.

For `auth: aad`, supply a valid externally refreshed OAuth token and a principal
with data-reader/query permissions for the intended container. The Authorization
header uses the provider's encoded `type=aad&ver=1.0&sig=...` format, not a Bearer
header. For `auth: master_key`, supply a base64 account key, preferably a read-only
key. Kelvo signs the `docs` query against `dbs/<database>/colls/<container>` with
HMAC-SHA256 and the request date. It never sends the raw key. Both modes require
verified HTTPS; redirects and environment HTTP proxies are disabled.

```sh
bin/kelvo query --mode native --connection documents --config kelvo.yml \
  --sql 'SELECT * FROM c WHERE c.active = true' --out documents.arrow
```

Only conservative single-statement SELECT syntax is admitted; the provider
remains responsible for validating its SQL dialect. Writes, administrative
commands, multiple statements, MongoDB pipelines, and query parameters fail
before HTTP execution. The request supplies an empty parameter array, without
interpolation. Every request is a query POST to the configured container;
Kelvo has no create/update/delete document path.

The query endpoint is bound to the configured database and container. Result
pages, including empty pages with continuation tokens, are read under the query
deadline and resource budgets. There are no automatic request retries.

Kelvo sets `x-ms-documentdb-query-enablecrosspartition: True`, requests at most
1,000 items per page, and echoes continuation and the latest session token to
the same endpoint. Repeated/cyclic or oversized tokens, inconsistent item counts,
missing charge headers, malformed JSON, and incomplete responses fail explicitly.
It consumes the REST query result; it does not implement SDK query-plan discovery,
client-side partition fan-out, global sorting, or partial-aggregate merging.
The admitted subset is simple projections and filters. ORDER BY, GROUP BY,
DISTINCT, aggregates (AVG/COUNT/MIN/MAX/SUM), TOP, OFFSET/LIMIT, ranking, joins,
set operators, WITH, and subqueries are refused before any HTTP request, even
when a particular query might target one partition. The conservative check also
rejects these words as unquoted property names; bracket-quoted property names
such as `c["count"]` remain available. Provider errors for other unsupported
queries are returned without rewriting or routing. No cross-source snapshot is
created.

`max_pages` defaults to 1,000 and accepts integers from 1 through 10,000.
`max_request_units` defaults to 10,000 and accepts integers from 1 through
1,000,000. Response charges are accumulated with exact decimal arithmetic.
The RU check happens after a page executes, so it prevents further requests
after an overrun; it cannot prevent or undo the charge for that page. Throttling
(HTTP 429) fails without automatic retry. The query deadline and Arrow row/byte
limits also apply. Each complete JSON page is capped at the smaller of 32 MiB
and one quarter of the configured memory budget; this is not a process-RSS cap.

Schemaless result values are preserved as exact received JSON bytes in Arrow
Binary, rather than inferred scalar columns. This retains the source's numeric
text and nested values; it cannot restore information already lost by Cosmos DB.

The Arrow schema contains one non-nullable Binary column, `document`, with
`source_type=cosmosdb`, `native_type=JSON`, and `encoding=json` metadata. Each cell
contains the corresponding received JSON value, including its internal
whitespace and numeric spelling. `SELECT VALUE` scalar results are supported;
a JSON `null` is the bytes `null`, not an absent Arrow cell. Consumers can decode
these values with a JSON implementation that preserves their required precision.

## Validation

TLS protocol fixtures cover AAD and master-key authentication, Microsoft's fixed
HMAC test vector, resource case binding, cross-partition/continuation headers,
empty pages, session-token handoff, exact nested/large-number JSON, cancellation,
no retries, malformed/incomplete replies, and page/RU/row/byte/response budgets.
These tests do not establish live Azure account acceptance, all cross-partition
SQL-plan support, or production throughput.

Worker boundary: the cleanup described here requires the connector process to
remain alive. CLI/HTTP/cluster cancellation or an outer deadline can kill that
process before remote cleanup runs. See [native cancellation and remote cleanup](usage.md#native-cancellation-and-remote-cleanup).
