# Native AWS Athena and DynamoDB

Query Athena or DynamoDB directly through AWS APIs with the official Go v2 SigV4 signer. No external adapter or AWS CLI is required.

## Configuration and credentials

1. Provision dedicated read-only IAM credentials. Athena also needs workgroup Start/Get/Stop query permissions and access to its result S3 location.
2. Store credentials in the environment and register their names:

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

Use HTTPS origins such as `https://athena.us-east-1.amazonaws.com` and `https://dynamodb.us-east-1.amazonaws.com`. Paths, userinfo, queries, fragments, redirects and environment proxies are rejected. TLS 1.2+ and certificate verification are required; signing is bound to the configured origin, region and service.

A named session-token variable must be populated. Credentials come only from these references; profiles, default chains, web identity, containers, metadata and ambient `AWS_*` credentials are not loaded. Recreate the connection after rotation.

Database/IAM grants remain the authorization boundary. Athena SELECT writes result files to S3, and workgroup policy may override `output_location`.

## Athena execution and types

Submit one native SELECT/WITH with `connection_id: warehouse`; parameters are unsupported. Kelvo starts one query with a random idempotency token and result reuse disabled, polls its ID, then reads result pages. Failed API calls are not retried or resubmitted.

Only the first page's first row is a header, with validated labels. Later pages retain their first row even if the initial page was empty. Missing `VarCharValue` means NULL; empty strings stay empty. Repeated metadata must match.

| Athena type | Arrow representation |
| --- | --- |
| TINYINT / SMALLINT / INTEGER / BIGINT | Int8 / Int16 / Int32 / Int64 |
| REAL / FLOAT; DOUBLE | Float32; Float64 |
| BOOLEAN | Boolean |
| CHAR / VARCHAR / STRING | UTF-8 String |
| BINARY / VARBINARY | Binary decoded from hexadecimal text |
| DATE | Date32 |
| DECIMAL(p,s), p ≤ 38 | Exact Decimal128 |
| TIMESTAMP, TIMESTAMP(p) | Timestamp without timezone; ms, µs or ns |
| TIMESTAMP(p) WITH TIME ZONE | UTC timestamp preserving the instant |

Rounding, overflow, nonfinite floats and timestamp precision above nine fail. Zoned timestamps accept numeric offsets or UTC/UT/GMT/Z, not IANA names. Complex types, intervals and unlisted types require a supported SQL cast.

After an execution ID is known, any error triggers best-effort `StopQueryExecution` with a separate two-second deadline. A lost submission response or failed stop can leave remote work running. Athena materializes execution before result delivery.

## DynamoDB PartiQL and lossless documents

Submit one native PartiQL SELECT with `connection_id: documents`, at most 8192 bytes. WITH, writes, multiple statements and parameters are unsupported. `ExecuteStatement` uses a bounded item-evaluation limit and follows `NextToken` through empty pages; failed calls are not retried.

Each item becomes a non-null Arrow Binary `document` with `encoding=dynamodb-attributevalue-json`:

```json
{"price":{"N":"12345678901234567890.123456789"},"missing":{"NULL":true},"payload":{"B":"AP8="},"tags":{"SS":["a","b"]}}
```

Original AttributeValue tags, numeric strings, base64 and nested bytes are retained. Shape changes and empty results keep the same schema. Validation checks unions, scalar types, numbers, base64, duplicate keys and depth; absent attributes, typed NULL and empty values remain distinct.

A nonempty `LastEvaluatedKey` without `NextToken` fails as unsupported continuation: this API has no `ExclusiveStartKey` request field. Kelvo never reports such a partial result as complete. Reads use default eventual consistency, and pagination has no shared snapshot. Cancellation stops the HTTP request; there is no separate cancel API.

## Resource bounds and validation status

| Limit | Behavior |
| --- | --- |
| One response | Smallest of 32 MiB, `MaxBytes`, and one quarter of `MemoryMB` |
| All response bodies | Capped by `MaxBytes`, including status and metadata |
| Pagination / Athena polling | At most 10,000 pages / 10,000 polls; repeated tokens fail |
| Result delivery | Deadline, Arrow rows and bytes enforced; borrowed batches delivered synchronously |

These buffer limits do not cap process RSS or AWS execution cost. Public errors omit credentials and response bodies. Cleanup also depends on the [worker remaining alive](usage.md#native-cancellation-and-remote-cleanup).

[Live DynamoDB Local 3.1.0 evidence](evidence/dynamodb-native.json) covers tagged values, exact numbers, pagination, empty continuation pages, restrictions and verified TLS. It does not establish AWS IAM or cloud SigV4 acceptance. Athena has TLS protocol-fixture coverage only; neither has a cloud throughput claim.

Reproduce DynamoDB Local acceptance on the designated VM:

```sh
python3 scripts/dynamodb_acceptance.py --sudo --go /path/to/go
```

References: [Start](https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html), [status](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryExecution.html), [results](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html), [stop](https://docs.aws.amazon.com/athena/latest/APIReference/API_StopQueryExecution.html), [Athena types](https://docs.aws.amazon.com/athena/latest/ug/data-types.html), [ExecuteStatement](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html), [AttributeValue](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html), [SigV4](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html), [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html).
