# Native PostgreSQL and MySQL families

Run read-only native queries through pgx v5.11.0 or go-sql-driver/mysql v1.10.1. PostgreSQL, CockroachDB, AlloyDB, Redshift, MySQL and MariaDB retain their configured identities; shared protocols do not prove every vendor's compatibility.

1. Create an account with only the intended SELECT grants and no unsafe function/file permissions.
2. Register environment references:

```yaml
sources:
  - id: warehouse
    type: postgres
    dsn_env: KELVO_SOURCE_POSTGRES_DSN
  - id: commerce
    type: mysql
    dsn_env: KELVO_SOURCE_MYSQL_DSN
```

3. Submit `mode: native`, `connection_id` and one SELECT/WITH. Optional typed parameters use PostgreSQL `$1`, `$2`, … or MySQL `?` markers.

Queries run in rollback-only read-only transactions. The syntax gate rejects writes, session/admin commands, ambiguous quoting and multiple statements; database grants remain the authorization boundary.

## Connection policy

PostgreSQL requires a TCP URL with explicit user, nonempty password, database and `sslmode=verify-full`:

```text
postgres://USER:PASSWORD@HOST:5432/DATABASE?sslmode=verify-full
```

Only `sslmode` and optional integer `connect_timeout` (1–60 seconds) are allowed. Duplicate parameters, identity overrides, multi-host URLs, services, password files, certificate/key paths and arbitrary runtime settings are rejected. Workers must not inherit `PG*` variables: Kelvo rejects them before pgx setup, clears default home credential paths, disables plaintext fallback and sets UTC. pgx may inspect default paths while building configuration but does not load their credential contents into the connection.

TLS 1.2+ uses system roots by default. Native PostgreSQL accepts
`options.tls_ca_pem` with at most eight currently valid CA certificates. Static
YAML and catalog authority cap each option at 4 KiB; on-demand authorities allow
64 KiB. The PEM replaces the root pool and preserves hostname verification.
Certificate paths, client keys, verification bypass and federation use of this
option remain unsupported.

MySQL requires explicit user, nonempty password, database and `tcp(HOST:PORT)`, with:

```text
tls=true&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27
```

`time_zone` encodes `'+00:00'`; Go-side `loc=UTC` alone is insufficient. Optional settings are `charset=utf8mb4`, `timeout`, `readTimeout` and `writeTimeout`; timeouts must be positive and at most one minute.

Duplicate/unknown options, other session variables, custom TLS registrations, verification bypass, plaintext fallback, old/cleartext authentication, multi-statements, parameter interpolation and unrestricted local-file access are rejected. TLS 1.2+ uses system roots.

Native MySQL uses Go-driver DSN syntax. DuckDB's signed MySQL extension uses URI/key=value syntax; configure separate source IDs and environment references when using both.

## Exact result types

| Source type | Arrow representation |
| --- | --- |
| PostgreSQL INT2 / INT4 / INT8 | Int16 / Int32 / Int64 |
| PostgreSQL FLOAT4 / FLOAT8 | Float32 / Float64 |
| PostgreSQL DATE; TIMESTAMP; TIMESTAMPTZ | Date32; naive microseconds; UTC microseconds |
| PostgreSQL UUID / JSON / JSONB; BYTEA | Annotated text; Binary |
| MySQL signed/unsigned integer widths | Matching Arrow width; MEDIUMINT uses Int32/Uint32 |
| MySQL TINYINT(1); BIT | Integer; raw Binary with `native_type=BIT` |
| MySQL FLOAT / DOUBLE | Float32 / Float64 |
| MySQL DATETIME / TIMESTAMP | Naive / UTC microseconds |
| MySQL JSON and text | String with native metadata |

Every field keeps `native_type` and `source_type`. TIMESTAMPTZ preserves the instant, not its original input zone. BIT metadata lacks declared bit length. Zero MySQL dates fail. Unknown, nested, array, interval and standalone time types fail explicitly.

Decimals require usable precision/scale metadata and exact conversion: Decimal128 up to 38 digits, Decimal256 up to PostgreSQL's supported 76 or MySQL's 65. Rounding and negative/ambiguous scales fail. Unconstrained PostgreSQL NUMERIC or expressions may have unusable typmod; provide an explicit contract such as `CAST(SUM(amount) AS NUMERIC(38, 2))`.

## Custom DuckDB federation

1. Use a `duckbridge` build and the native DSN policy above.
2. Register `federation.tables` with local `name`, remote `table`, and PostgreSQL `schema` or MySQL `database`.
3. Submit `mode: federated` with explicit source IDs. See the [three-source join guide](federation.md).

Scans own independent transactions, Arrow readers and budgets. Supported projections/filters run remotely; DuckDB handles joins, aggregates and ordering. Drivers still decode row protocols into Arrow. Unsupported required predicates fail. Other protocol-family products remain native-only through these routes.

Without explicit table registrations, PostgreSQL/MySQL federation uses signed extensions and their connection strings. Custom Go adapters keep DuckDB external access disabled and preserve exact object-range restrictions for snapshot joins. Separate scans have no shared transaction snapshot.

## Resource and validation boundaries

Each query owns one `database/sql` connection. Deadlines cover acquisition/execution; synchronous batches enforce row/byte limits. Conversion errors, extra result sets and incomplete reads fail the query.

Drivers may allocate wire packets before row validation. MySQL's advertised packet budget is the smaller of 16 MiB and one quarter of client memory. Enforce process memory and database CPU/memory/scan policies separately.

MySQL also sets `max_execution_time` to the query timeout rounded up to milliseconds and rejects executable optimizer hints that could override it. This bounds supported SELECT execution when socket closure is insufficient; it is not immediate cancellation or a stored-program guarantee. The setting is not applied to MariaDB. See [MySQL timeout semantics](https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html#sysvar_max_execution_time) and [worker cleanup limits](usage.md#native-cancellation-and-remote-cleanup).

Run disposable TLS acceptance on the designated Linux VM with cached official images:

```sh
python3 scripts/relational_acceptance.py --go /path/to/go --output artifacts/relational-acceptance.json
```

The fixture uses a private network, temporary process-scoped CA and read-only users; it publishes no host ports and removes only its own resources. The report records versions and assertions. See [validation](validation.md) for evidence; family compatibility and fixture limits are not production sizing.

References: [pgx metadata](https://github.com/jackc/pgx/blob/v5.11.0/stdlib/sql.go), [pgx configuration](https://github.com/jackc/pgx/blob/v5.11.0/pgconn/config.go), [MySQL DSNs](https://github.com/go-sql-driver/mysql/tree/v1.10.1#dsn-data-source-name), [MySQL metadata](https://github.com/go-sql-driver/mysql/blob/v1.10.1/fields.go).
