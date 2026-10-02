# Native PostgreSQL and MySQL families

Kelvo uses pgx v5.11.0 for PostgreSQL protocol sources and go-sql-driver/mysql
v1.10.1 for MySQL protocol sources. Constructors preserve the configured family
identity: PostgreSQL, CockroachDB, AlloyDB, Redshift, MySQL, and MariaDB. A family
name shares its wire protocol; it does not assert complete SQL/type compatibility
or live acceptance on every vendor service.

```yaml
sources:
  - id: warehouse
    type: postgres
    dsn_env: KELVO_SOURCE_POSTGRES_DSN
  - id: commerce
    type: mysql
    dsn_env: KELVO_SOURCE_MYSQL_DSN
```

Sources use credential environment references and no extra options. Native MySQL uses the Go driver DSN syntax; the DuckDB MySQL extension uses its own URI/key=value syntax. Configure separate source IDs and environment references when using both modes against the same MySQL database. Requests
use `mode: native`, `connection_id`, SQL, and optional typed parameters.
PostgreSQL-family queries bind `$1`, `$2`, and subsequent positional parameters;
MySQL-family queries bind `?`. The shared syntax gate accepts one conservative
SELECT/WITH and rejects writes, session/admin commands, ambiguous quoting, and
multiple statements. Database grants are the authorization boundary. Each query
runs inside a read-only transaction and rolls it back on completion or failure.
Use accounts with only the intended SELECT privileges and no unsafe function or
file access grants.

## Connection policy

PostgreSQL credentials must be an explicit TCP URL with a user, nonempty
password, database, and `sslmode=verify-full`. A structural example is
`postgres://USER:PASSWORD@HOST:5432/DATABASE?sslmode=verify-full`.
Only `sslmode` and an optional integer `connect_timeout` from 1 through 60 seconds
are accepted as source query parameters. Duplicate parameters, host/user/database
overrides, services, password files, client certificate/key paths, arbitrary
runtime settings, multi-host URLs, and weaker SSL modes are rejected.

The worker must not inherit `PG*` environment variables. Kelvo rejects them
before invoking pgx rather than allowing ambient settings to change the source
identity. Its internally constructed pgx configuration clears home-directory
client certificate/key and password-file settings, uses the system CA pool,
requires TLS 1.2 or newer, disables plaintext fallbacks, and fixes the session
timezone to UTC. It does not accept per-source custom CA files or client
certificates. pgx may inspect default file locations while building defaults,
but no home credential contents are loaded into the connection.

MySQL credentials must specify an explicit `tcp(HOST:PORT)` endpoint, user,
nonempty password and database. Required options are:

```text
tls=true&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27
```

The last value is the URL-encoded SQL literal `'+00:00'`. Both Go-side UTC parsing
and the server session timezone are required: `loc=UTC` alone does not make a
MySQL TIMESTAMP an unambiguous UTC instant. Optional options are
`charset=utf8mb4`, `timeout`, `readTimeout`, and `writeTimeout`; each timeout must
be positive and at most one minute. Duplicate or unknown options, arbitrary
session variables, custom TLS registrations, certificate-verification bypass,
plaintext fallback, old/cleartext authentication plugins, multi-statements,
client-side parameter interpolation, and unrestricted local-file access are
rejected. MySQL also requires TLS 1.2 or newer and system CA verification.

These policies intentionally fail on unsupported configuration rather than
silently weakening transport or inheriting another source's credentials.

## Exact result types

PostgreSQL `INT2`, `INT4`, and `INT8` retain their Arrow widths. `FLOAT4` and
`FLOAT8` map to Float32 and Float64. `DATE` uses Date32. `TIMESTAMP` uses a
microsecond Arrow timestamp without timezone; `TIMESTAMPTZ` uses a microsecond
UTC timestamp. This preserves wall-clock values versus absolute instants.
PostgreSQL's original input timezone name/offset is not retained by TIMESTAMPTZ
storage. UUID, JSON, and JSONB are returned as text with native type metadata;
BYTEA remains binary. Unknown, nested, array, interval, and standalone time types
fail explicitly.

MySQL signed TINYINT/SMALLINT/INT/BIGINT map to Int8/Int16/Int32/Int64; unsigned
variants map to matching unsigned widths. MEDIUMINT uses Int32 or Uint32 because
Arrow has no 24-bit integer. `TINYINT(1)` is still an integer, not an inferred
Boolean. BIT is raw binary with `native_type=BIT`, preserving arbitrary bitsets;
its declared bit length is not supplied by the current driver metadata. FLOAT
uses Float32 and DOUBLE uses Float64. DATETIME preserves naive microsecond wall
time, and TIMESTAMP is microseconds in UTC under the enforced session policy.
Zero MySQL dates fail explicitly instead of becoming a valid year-one date.
JSON and text types remain strings, with native type metadata retained.

Each Arrow field records `native_type` and `source_type`. Decimal precision and
scale must be established by driver metadata; values never pass through binary
floating point. Arrow Decimal128 handles precision up to 38 and Decimal256 handles
larger supported precision, up to 76 for PostgreSQL and 65 for MySQL. Values that
would require rounding fail. pgx derives NUMERIC metadata from PostgreSQL typmod:
unconstrained NUMERIC and some expressions return sentinel metadata, not a usable
precision/scale. Those results fail as unsupported. An explicit SQL cast such as
`CAST(SUM(amount) AS NUMERIC(38, 2))` supplies an intentional output contract.
Negative or otherwise ambiguous decimal scales are currently rejected.

## Custom DuckDB federation

The optional `duckbridge` build can reuse the Go PostgreSQL and MySQL drivers
for selected-table federation. Configure `federation.tables` on a source using
the native DSN policy above; then use `mode: federated` with explicit source IDs.
PostgreSQL registrations specify `schema` and `table`, while MySQL registrations
specify `database` and `table`. Each also supplies the local table alias `name`.
See [configuration and a three-source join](federation.md).

Each scan has an independent read-only transaction, Arrow reader and scan budget.
Projection and supported typed predicates run at the database; DuckDB performs
joins, aggregates and ordering. The Go drivers convert rows into Arrow, so this
does not turn PostgreSQL/MySQL wire protocols into Arrow transport. Matching
widths, NULLs and native metadata are retained. A required predicate outside the
supported semantic subset fails rather than being omitted. Other protocol-family
products such as MariaDB and CockroachDB remain native-only through these routes.

Without explicit table registrations, PostgreSQL/MySQL federation continues to
use the existing signed DuckDB extensions and their connection-string format.
Custom Go adapters keep DuckDB external access disabled and can retain exact
object-range restrictions when joining an object snapshot.

## Resource and validation boundaries

The adapters keep one database/sql connection per query, apply its deadline to
connection acquisition and execution, and stream rows into synchronous bounded
Arrow batches. Row/byte overflow, conversion errors, extra result sets, and
incomplete cursor reads fail the query. Client limits do not limit server-side
query memory, CPU, or scanning work; configure database resource policies
separately. The driver can allocate a wire packet before the row writer inspects
its values. The MySQL connector caps its advertised packet budget at the smaller
of 16 MiB and one quarter of the client memory budget. Container memory remains
the process RSS boundary.

MySQL sessions also set the operator-owned `max_execution_time` to the query
timeout rounded up to milliseconds. Native SQL rejects executable optimizer
hints, including attempts to override that limit. This bounds supported
read-only SELECT execution at the server when closing a cancelled client socket
does not promptly stop work. It is not immediate remote cancellation, a server
memory limit, or a guarantee for stored programs; see the upstream
[MySQL timeout semantics](https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html#sysvar_max_execution_time).
The MySQL-only session setting is not applied to MariaDB or other native engines.

Focused tests cover DSN isolation, signed/unsigned widths, timezone metadata,
ambiguous NUMERIC typmod, zero dates, logical-type metadata, rollback-only
execution, cancellation, and result errors. Run disposable live TLS acceptance
on the designated Linux VM with cached official images:

```sh
python3 scripts/relational_acceptance.py --go /path/to/go --output artifacts/relational-acceptance.json
```

The fixture creates its own internal Docker network and temporary certificate
authority, publishes no host ports, creates separate read-only database users,
and removes only its own containers/network. Its generated CA is trusted only
by the test process. The server versions and successful assertions are recorded
in the output report; fixture resource allocations are not production sizing.

References: [pgx stdlib metadata](https://github.com/jackc/pgx/blob/v5.11.0/stdlib/sql.go),
[pgx connection configuration](https://github.com/jackc/pgx/blob/v5.11.0/pgconn/config.go),
[MySQL driver DSN](https://github.com/go-sql-driver/mysql/tree/v1.10.1#dsn-data-source-name),
and [MySQL driver type metadata](https://github.com/go-sql-driver/mysql/blob/v1.10.1/fields.go).
