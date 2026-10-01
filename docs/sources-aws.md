# Native AWS Athena and DynamoDB

Kelvo uses the AWS APIs directly and the official AWS SDK for Go v2 SigV4 signer. These are built-in native sources. They do not require an adapter, a DSN, or the AWS CLI.

## Configuration and credentials

Register one source per native connection. Environment variable *names* belong in the catalog; their values stay in the server environment.

```yaml
sources:
  - id: warehouse
    type: athena
    url_env: KELVO_SOURCE_ATHENA_URL
    username_env: KELVO_SOURCE_ATHENA_ACCESS_KEY_ID
    password_env: KELVO_SOURCE_ATHENA_SECRET_ACCESS_KEY
    token_env: KELVO_SOURCE_ATHENA_SESSION_TOKEN # omit for long-lived access keys
    options:
      region: us-east-1
      workgroup: analytics
      database: sales
      output_location: s3://example-query-results/kelvo/
  - id: documents
    type: dynamodb
    url_env: KELVO_SOURCE_DYNAMODB_URL
    username_env: KELVO_SOURCE_DYNAMODB_ACCESS_KEY_ID
    password_env: KELVO_SOURCE_DYNAMODB_SECRET_ACCESS_KEY
    options:
      region: us-east-1
```

Set `KELVO_SOURCE_ATHENA_URL` to an HTTPS origin such as `https://athena.us-east-1.amazonaws.com` and `KELVO_SOURCE_DYNAMODB_URL` to `https://dynamodb.us-east-1.amazonaws.com`. Endpoint paths, userinfo, query strings, fragments, and HTTP are rejected. The configured endpoint is trusted administrator configuration; credentials are signed only for that origin, configured region, and service. Redirects are never followed. TLS certificate verification is enabled; TLS 1.2 is the minimum. Ambient proxy settings are ignored.

`username_env` selects the access key ID, `password_env` the secret access key, and optional `token_env` the session token. If a token variable is named, it must be populated. The connector never loads profiles, the default credential chain, web identity, container credentials, instance metadata, or ambient `AWS_*` credentials. Credential rotation requires recreating the connection.

Use dedicated IAM credentials with read-only table/catalog access. Athena also needs Start/Get/Stop query permissions for the workgroup, and access to its query-result S3 location. Athena SELECT execution writes result files to S3; workgroup policy can override the configured result location. SQL syntax filtering is conservative and does not replace AWS permissions or provide table-level isolation from other resources granted to the same principal.

## Athena execution and types

Submit a native request with `connection_id: warehouse` and one read-only SELECT or WITH statement. Parameters are explicitly rejected. The connector issues one `StartQueryExecution` with a new random idempotency token, polls `GetQueryExecution`, then fetches `GetQueryResults` pages for that execution ID. Result reuse is disabled in the request. It never replays query submission or retries a failed API operation. Failed, cancelled, missing, and unknown execution states fail explicitly.

Only the first result page's first row is treated as the header, and its labels are validated. Later pages retain their first row, including when the initial page was empty. Missing VarCharValue is SQL NULL; an empty string remains an empty string. Repeated metadata must match the original Arrow schema.

| Athena type | Arrow representation |
| --- | --- |
| TINYINT / SMALLINT / INTEGER / BIGINT | Int8 / Int16 / Int32 / Int64 |
| REAL / FLOAT; DOUBLE | Float32; Float64 |
| BOOLEAN | Boolean |
| CHAR / VARCHAR / STRING | UTF-8 String |
| BINARY / VARBINARY | Binary, decoded from hexadecimal byte text |
| DATE | Date32 |
| DECIMAL(p,s), p up to 38 | Decimal128 with exact precision and scale |
| TIMESTAMP, TIMESTAMP(p) | Timestamp without timezone; millisecond, microsecond, or nanosecond unit |
| TIMESTAMP(p) WITH TIME ZONE | Timestamp with UTC metadata and the exact instant |

Decimal conversion rejects rounding and overflow. Timestamp conversion rejects precision or range loss; timestamp precision above nine is unsupported. Zoned timestamps accept explicit numeric offsets or UTC/UT/GMT/Z; IANA zone names are currently unsupported. Ordinary TIMESTAMP values do not acquire a timezone. Nonfinite floats, complex types, intervals, and unlisted types fail explicitly; select supported casts when needed.

On any error after obtaining the execution ID, including a deadline, cancellation, result limit, conversion failure, or sink failure, Kelvo attempts `StopQueryExecution` on an independent two-second cleanup context. Cleanup is best effort: a network failure before the initial response provides an execution ID cannot be cancelled, and failed Stop calls cannot guarantee server-side termination. Result delivery follows materialized Athena execution; it is not streaming engine execution.

## DynamoDB PartiQL and lossless documents

Submit a native request with `connection_id: documents` and one PartiQL SELECT statement of at most 8192 bytes. WITH, write statements, multiple statements, and parameters are explicitly rejected. `ExecuteStatement` uses a bounded item-evaluation limit and follows opaque NextToken values, including across empty filtered pages. Tokens stay in the JSON request body and never become request URLs. No failed execution is retried.

DynamoDB is schemaless. Each returned item becomes one non-null Arrow Binary value in the `document` column, with field metadata `encoding=dynamodb-attributevalue-json`. Its bytes contain the original AWS item object with tagged AttributeValue values. There is no inferred column schema or conversion to floating point. For example:

```json
{"price":{"N":"12345678901234567890.123456789"},"missing":{"NULL":true},"payload":{"B":"AP8="},"tags":{"SS":["a","b"]}}
```

Numbers retain their decimal/exponent strings; binary retains base64; BOOL, typed NULL, S, N, B, SS, NS, BS, M, and L retain their type tags and nested contents. Different document shapes and an empty result use the same Arrow schema. Attribute unions, scalar types, base64, numeric syntax, duplicate JSON keys, and nesting limits are validated. Empty strings, empty lists/maps, typed NULL, and absent attributes remain distinct.

AWS documents both NextToken and LastEvaluatedKey in ExecuteStatement responses, but provides no ExclusiveStartKey request parameter for that operation. If a response has a nonempty LastEvaluatedKey without NextToken, Kelvo returns an explicit unsupported-continuation error. It does not report a truncated result as complete or invent a new PartiQL predicate. A NextToken response is followed normally, even if a LastEvaluatedKey is also present.

DynamoDB reads use the API's default eventual consistency. Query cancellation stops the client request; ExecuteStatement has no separate cancellation API. Paginated reads are not a transactionally consistent snapshot and may observe concurrent changes.

## Resource bounds and validation status

Both connectors enforce the request deadline, Arrow row and byte limits, per-response buffering bounded by the smallest of 32 MiB, MaxBytes, and one quarter of MemoryMB, and aggregate response-body bytes capped by MaxBytes. Metadata and status responses count toward the network budget, so a small byte limit may fail before reaching the same amount of Arrow data. Pages are capped at 10,000 and Athena polls at 10,000. Repeated continuation tokens fail. Provider response bodies and credentials do not appear in public errors. Arrow batches are borrowed synchronously by the sink.

Unit and lifecycle validation uses TLS HTTP fixtures on the designated Linux VM. Fixtures cover signing and credential scope, HTTPS configuration, redirect rejection, bounded/error responses, Athena lifecycle and cancellation, exact scalar/decimal/timestamp conversion, first-page headers and NULLs, schema changes, DynamoDB tagged documents and empty-page pagination, read-only/source restrictions, limits, and sink failures. No real AWS account was available; these fixtures are not AWS cloud interoperability or performance validation.

DynamoDB Local 3.1.0 acceptance also passed on the designated Linux VM through a disposable CA-verified TLS endpoint. Five live tests covered the complete tagged union and exact large numbers, 1,205-row real PartiQL pagination, empty filtered continuation pages, source/read-only/parameter restrictions and limits, and rejection of an untrusted certificate. The proxy observed two continuation requests and one empty page with NextToken. The loopback-only fixture was limited to 512 MiB, one CPU and 128 processes and removed after testing. [Machine-readable evidence](evidence/dynamodb-native.json) records its image digest and results. Run `python3 scripts/dynamodb_acceptance.py --sudo --go /path/to/go` on the designated test VM to reproduce. DynamoDB Local accepts dummy credentials, so this does not prove AWS IAM enforcement or cloud SigV4 interoperability; Athena remains validated with protocol fixtures only.

Official references consulted:

- [StartQueryExecution](https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html)
- [GetQueryExecution](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryExecution.html)
- [GetQueryResults](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html)
- [StopQueryExecution](https://docs.aws.amazon.com/athena/latest/APIReference/API_StopQueryExecution.html)
- [Athena data types](https://docs.aws.amazon.com/athena/latest/ug/data-types.html)
- [ExecuteStatement](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html)
- [AttributeValue](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html)
- [AWS SigV4](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html)

- [DynamoDB Local deployment](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html)

Worker boundary: the cleanup described here requires the connector process to
remain alive. CLI/HTTP/cluster cancellation or an outer deadline can kill that
process before remote cleanup runs. See [native cancellation and remote cleanup](usage.md#native-cancellation-and-remote-cleanup).
