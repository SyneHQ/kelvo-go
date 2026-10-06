# ClickHouse on AWS Lambda

The optional Go adapter supports the existing `clickhouse_lambda` saved-connection format through synchronous AWS Invoke. It supports connection tests, SQL reads and explicit autocommit statements.

| Saved field | Meaning |
| --- | --- |
| `host` | Lambda function name |
| `region` | AWS region |
| `database` | Logical bucket selection bound to the operation |
| `filePath` | Lambda event `rawPath`; decoded using the existing driver contract |
| `username`, `password` | Explicit AWS access key and secret key |

The gateway derives the regional HTTPS endpoint. Workers use SigV4 and never load ambient AWS credentials. The event contains `rawPath`, `requestContext.http.method: POST` and the SQL `body`. The bucket name is not prepended to `rawPath`.

The function must return `{ "statusCode": 200, "body": "..." }`. A successful write has no affected-row count. Function errors, incomplete responses and lost acknowledgements never trigger a write retry; reconcile an unknown outcome with the source.

Reads preserve the function's bare TSV as exact text columns named `col1`, `col2`, and so on. Empty strings, NULL (`\N`) and escaped control characters remain distinct. This format carries no column names or types, so automatic metadata discovery and bound parameters are unavailable. Use the native ClickHouse adapter for typed Arrow results and large transfers.

Synchronous payloads are capped at 6 MiB and also obey the operation's lower byte, row and memory limits. HTTPS/SigV4 fixtures cover exact values, errors, limits and no replay; a live AWS function remains an external acceptance gate.

References: [AWS Invoke API](https://docs.aws.amazon.com/lambda/latest/api/API_Invoke.html), [existing driver v1.0.0](https://github.com/SYNEHQ/lambda-clickhouse-go-driver/tree/v1.0.0), [database operations](database-operations.md).
