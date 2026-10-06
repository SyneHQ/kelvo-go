# Azure Cosmos DB for NoSQL

Query one configured Cosmos DB container and preserve its JSON values in Arrow Binary. The connector supports simple projections and filters; it does not perform client-side cross-partition query-plan merging.

## Official API references

[Query documents](https://learn.microsoft.com/en-us/rest/api/cosmos-db/query-documents), [authorization signatures](https://learn.microsoft.com/en-us/rest/api/cosmos-db/access-control-on-cosmosdb-resources), [pagination](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/pagination) and [cross-partition queries](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-query-container) define the REST contract.

## Configuration contract

1. Provision an externally refreshed AAD token with container read/query permissions, or preferably a read-only base64 account key.
2. Register the source with explicit `auth: aad` or `auth: master_key`:

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

3. Set an HTTPS account origin such as `https://example-account.documents.azure.com`, then query:

```sh
bin/kelvo query --mode native --connection documents --config kelvo.yml \
  --sql 'SELECT * FROM c WHERE c.active = true' --out documents.arrow
```

Origins reject credentials, paths, queries and fragments. Resource IDs preserve case: one ASCII letter/digit followed by up to 127 letters, digits, dots, underscores or hyphens. Unknown options fail. Credentials are not discovered from Azure defaults; verified TLS, no redirects and disabled environment proxies are enforced.

AAD uses encoded `type=aad&ver=1.0&sig=...` authorization. Master-key requests use HMAC-SHA256 over the dated `docs` query and case-preserved `dbs/<database>/colls/<container>`; the raw key is never sent.

The analytics query endpoint admits conservative single SELECT projections/filters. Parameters, writes, admin commands, multiple statements and MongoDB pipelines are rejected there. Every query POST targets the configured container with an empty parameter array; no interpolation or retries occur.

ORDER BY, GROUP BY, DISTINCT, aggregates, TOP, OFFSET/LIMIT, ranking, joins, set operators, WITH and subqueries are refused before HTTP execution. Those words are also rejected as unquoted property names; use bracket quoting such as `c["count"]`. Kelvo does not implement SDK plan discovery, partition fan-out, global sorting or partial-aggregate merging.

Cross-partition queries request at most 1,000 items per page and echo continuation plus the latest session token to the same endpoint. Empty pages can continue. Cyclic/oversized tokens, inconsistent counts, missing charge headers and malformed/incomplete replies fail. Reads do not create a cross-source snapshot.

| Limit | Default / allowed range |
| --- | --- |
| `max_pages` | 1,000 / 1–10,000 |
| `max_request_units` | 10,000 / 1–1,000,000 |
| Buffered page | Smaller of 32 MiB and one quarter of query memory |
| Query output | Deadline and Arrow row/byte limits |

RU charges use exact decimal accounting **after** each page executes; an overrun stops further pages but cannot undo that charge. HTTP 429 fails without retry. Buffer budgets do not cap RSS.

Results use non-null Binary `document` with `source_type=cosmosdb`, `native_type=JSON`, `encoding=json`. Each cell retains the received JSON value's bytes, whitespace and numeric spelling. `SELECT VALUE` scalars are supported; JSON `null` remains bytes, not an absent Arrow cell. This cannot restore precision already lost by Cosmos DB.

## Validation

TLS fixtures cover authentication/signatures, source binding, continuation/session tokens, exact values, errors and budgets. Live Azure acceptance and throughput remain unverified. See [worker cancellation limits](usage.md#native-cancellation-and-remote-cleanup).

## Authorized writes

The optional Go operation adapter translates a small mutation grammar to [document REST calls](https://learn.microsoft.com/en-us/rest/api/cosmos-db/documents); it never sends mutation SQL to the query API. The resolver supplies current credentials after a worker leases the operation.

```sql
INSERT INTO Events (id,tenant,total) VALUES ('ride-1','team-a',123.45)
UPSERT INTO Events (id,tenant,total) VALUES ('ride-1','team-a',125.00)
UPDATE Events SET total=130.00 WHERE id='ride-1' AND tenant='team-a'
DELETE FROM Events WHERE id='ride-1' AND tenant='team-a'
CREATE COLLECTION Events WITH PK=/tenant WITH RU=400
DROP COLLECTION IF EXISTS Events
```

- Point writes require a selected container and an explicit string `id`. UPDATE uses atomic PATCH, with up to 10 top-level fields; it cannot change `id` or partition fields.
- Each point write reads the container's partition definition. WHERE must contain only `id` and every partition key; extra filters fail. Hierarchical keys and nested paths such as `tenant.name` are supported.
- Values accept single-quoted strings, JSON numbers/booleans/null and JSON objects/arrays. Legacy double-quoted JSON envelopes remain supported. Numbers retain their spelling before the provider processes them.
- CREATE/DROP COLLECTION (alias TABLE) stays inside the saved database and selected container, when present. CREATE/DROP DATABASE requires database-level selection and can affect only that saved database. CREATE supports `IF NOT EXISTS`, `WITH PK`, and one of `WITH RU` or `WITH MAXRU`.
- Transactions, placeholders, computed assignments, automatic IDs, unique-key DDL and ALTER throughput are unsupported. These require explicit APIs; none are silently approximated.

Writes use verified TLS and no automatic retries. An expected success status confirms the mutation even if its optional document body is missing; a lost acknowledgement remains an unknown outcome. The metadata RU budget is checked before dispatch. A committed write's final RU charge cannot be undone or used as a reason to replay it. TLS fixtures cover this contract; live Azure write acceptance remains an open gate.
