# Cloud SQL sources

Query Databricks, Snowflake or Cloudflare D1 with native `connection_id` requests; see [sources.yml](../deploy/examples/sources.yml). Databricks and Snowflake also support selected-table joins through the optional [federation bridge](federation-adapters.md); D1 remains native-only.

1. Set `url_env` to an HTTPS origin and `token_env` to a bearer-token reference.
2. Grant the provider identity only the intended reads, then submit one SELECT/WITH statement. Parameters are unsupported.

Origins have no path, query or embedded credentials: `https://<workspace-host>`, `https://<account>.snowflakecomputing.com`, or `https://api.cloudflare.com`. TLS 1.2+, certificate verification, disabled proxies and no redirects are enforced. Operators renew OAuth tokens and generate Snowflake JWTs. SQL filtering does not replace provider grants, including function permissions.

## Databricks

Set `options.warehouse_id`; `catalog` and `schema` are optional. The [Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution/executestatement) runs an asynchronous statement and returns `JSON_ARRAY` / `INLINE` chunks. Kelvo validates the handle, offsets, counts and truncation flags, fetching only internal paths for that statement.

Inline results have a provider [25 MiB ceiling](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial). Kelvo's smaller output/response budgets also apply. External Arrow-result links are not followed, so this path is unsuitable for larger raw exports.

## Snowflake

The [SQL API v2](https://docs.snowflake.com/en/developer-guide/sql-api/reference) accepts optional `database`, `schema`, `warehouse`, `role` and `token_type`. Token types are `OAUTH` (default), `KEYPAIR_JWT` and `PROGRAMMATIC_ACCESS_TOKEN`.

Kelvo submits one statement, polls its handle and reads ordered partitions with bounded decompression and matching row counts. [Result mapping](https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses) preserves exact decimals, binary hex, epoch-day dates and nanosecond NTZ/LTZ timestamps. TIME, TIMESTAMP_TZ, VARIANT, ARRAY and OBJECT require an explicit supported cast or projection.

## Cloudflare D1

Set `account_id` and `database_id`. D1's [raw query endpoint](https://developers.cloudflare.com/api/resources/d1/subresources/database/methods/raw/) supplies ordered column names and array rows.

Kelvo checks request/statement success and infers Arrow types from the bounded result. Mixed incompatible values fail; empty or all-NULL results may lack usable type metadata.

## Limits and validation

These APIs buffer JSON before Arrow conversion. Exact decimals, declared integer widths and NULLs are preserved; unsupported types, overflow, malformed pages and incomplete results fail. Timestamps must fit the selected Arrow nanosecond range.

Each response is capped at the smaller of 32 MiB and one quarter of query memory; D1 also applies the output-byte ceiling. Enforce process limits and provider query policies separately.

Snowflake/Databricks cancellation is best effort once a statement handle is known. A lost submission response may leave an unknown remote statement; provider timeouts remain necessary. Cleanup is also limited by the [worker cancellation grace](usage.md#native-cancellation-and-remote-cleanup).

HTTP fixtures cover conversion, paging, cancellation and limits. Live provider acceptance and cloud-source throughput remain unverified.
