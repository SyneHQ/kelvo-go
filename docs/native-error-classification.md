# Native source error classification

Kelvo classifies PostgreSQL and MySQL-family failures using typed driver metadata.
This lets scheduled acceleration stop on a known access or configuration failure
without exposing a server error message or repeatedly querying a broken source.
It is a bounded classification policy, not complete coverage of every vendor error.

## Public error boundary

The native SQL engine checks the caller context first. An expired or cancelled
context wins over driver metadata; wrapped `context.DeadlineExceeded` and
`context.Canceled` also retain their public timeout/cancellation classification.
Otherwise, the adapter maps recognized typed driver codes. Raw driver text,
SQL, object names, usernames and connection strings do not enter public messages.
Messages describe the failed stage, such as `Could not connect to source` or
`Source rejected query`. The public code carries the classification separately.

Unknown codes, untyped errors and conflicting protocol metadata remain
`QUERY_FAILED`. Kelvo does not guess from error text or broad SQLSTATE classes.
Trusted configuration callbacks and result sinks can return explicit public
errors; unsupported result types retain their `UNSUPPORTED` code.

## PostgreSQL

The adapter uses `errors.As` to inspect `pgconn.PgError`, including wrapped errors.
Only the following exact SQLSTATE values are mapped:

| SQLSTATE | Public code |
| --- | --- |
| `28000`, `28P01` | `UNAUTHENTICATED` |
| `42501` | `PERMISSION_DENIED` |
| `3D000`, `3F000` | `CONFIGURATION_ERROR` |
| `42601`, `42703`, `42883`, `42P01`, `42P02` | `INVALID_ARGUMENT` |
| `08000`, `08001`, `08003`, `08006`, `40001`, `40P01`, `53300`, `55P03`, `57P01`, `57P02`, `57P03`, `57014` | `UNAVAILABLE` |

`57014` indicates server cancellation, which alone does not distinguish statement
timeout from administrative cancellation. It therefore maps to `UNAVAILABLE`
unless the context establishes cancellation or a deadline. Compatible adapters
using this PostgreSQL driver path receive the same exact-code policy; that does
not establish live acceptance against every compatible engine.

Reference: [PostgreSQL error codes](https://www.postgresql.org/docs/current/errcodes-appendix.html).

## MySQL and MariaDB

For configured `mysql` and `mariadb` sources, the adapter inspects typed
`mysql.MySQLError`. The numeric code must be recognized. If the server supplies
a nonzero SQLSTATE, it must also match the expected value below; an omitted
SQLSTATE is accepted. A conflicting SQLSTATE stays unknown.

| Server number | Expected SQLSTATE | Public code |
| --- | --- | --- |
| `1045` | `28000` | `UNAUTHENTICATED` |
| `1044`, `1142`, `1143`, `1227` | `42000` | `PERMISSION_DENIED` |
| `1046` | `3D000` | `CONFIGURATION_ERROR` |
| `1049` | `42000` | `CONFIGURATION_ERROR` |
| `1054` | `42S22` | `INVALID_ARGUMENT` |
| `1064`, `1065` | `42000` | `INVALID_ARGUMENT` |
| `1146` | `42S02` | `INVALID_ARGUMENT` |
| `1040` | `08004` | `UNAVAILABLE` |
| `1053` | `08S01` | `UNAVAILABLE` |
| `1205` | `HY000` | `UNAVAILABLE` |
| `1213` | `40001` | `UNAVAILABLE` |
| `1317` | `70100` | `UNAVAILABLE` |
| `3024` (MySQL only) | `HY000` | `DEADLINE_EXCEEDED` |

MariaDB assigns a different meaning to number `3024`; it remains unknown for a
MariaDB source. Code `1317` alone does not establish a deadline. Other adapters
that share the generic SQL engine do not inherit this MySQL-family mapper.

References: [MySQL server error reference](https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html)
and [MariaDB error 3024](https://mariadb.com/docs/server/reference/error-codes/mariadb-error-codes-3000-to-3099/e3024).

## Scheduled acceleration consequences

| Failure | Durable refresh policy |
| --- | --- |
| Authentication or permission denied | Permanent `access` failure |
| Invalid query, invalid configuration, unsupported result type | Permanent `configuration` failure |
| Incompatible snapshot schema | Permanent `schema` failure |
| Resource limit exceeded | Permanent `resource` failure |
| Cancellation or deadline | Retryable `timeout` failure |
| Source unavailable | Retryable `unavailable` failure |
| Unknown driver error | Retryable `unknown` failure |

Retryable failures share a five-failure budget for the exact dataset/configuration
fingerprint across scheduled jobs, with exponential jitter capped at five minutes.
Permanent failures stop immediately and persist across worker restarts. Inspect
status, repair the cause, and explicitly reset the current fingerprint using the
[operator procedure](operations.md#source-refresh-failures-and-recovery). This
policy applies to clustered scheduled refreshes, not automatic replay of user
queries or durable retries in standalone `accelerate watch`.

A failed refresh preserves the last committed snapshot. **Source authentication
failure or revoked source grants do not revoke access to retained snapshots.**
If readers must lose that access, operators must bump the dataset's
`authorization_version` and propagate the updated catalog to serving nodes. The
new fingerprint makes old snapshots ineligible; changing source credentials or
resetting retry state alone is not snapshot revocation.

## Validation scope

The [recorded TLS acceptance](evidence/native-error-relational-acceptance.json)
passed all eight required test/subtest outcomes against isolated PostgreSQL 17.6
and MySQL 8.4 servers. Wrong-password and revoked-SELECT failures retained their
public access codes through direct engine execution and in-process Manager
refreshes. Failed refreshes preserved generation identity, digest, row count and
original freshness. Both fixture containers and their network were removed
before the harness emitted success; no host trust or published ports were used.

Full ordinary and pinned bridge suites, focused native/cluster race tests,
`go vet` and the binary build passed on Azure. Ten synthetic harness-control
checks also passed, covering cleanup failures, stale output refusal, named-test
proof and private diagnostic redaction. CI includes the control checks; the
workflow changes remain local and have not run on GitHub.

Validation first exposed two test-fixture errors: a source-secret environment
reference outside the permitted namespace, and a snapshot root created with
nonprivate test-directory permissions. Both were corrected without relaxing the
runtime checks. Earlier failed live runs cleaned up their fixtures and produced
failure evidence. Private bounded logs now preserve diagnostics needed to debug
those failures without publishing driver output.

These results do not establish live worker IPC, real NATS retry/admission
integration for these driver errors, MariaDB/provider-wide compatibility,
sustained load or production deployment. The native runtime milestone is
`c3a3935`; the evidence records the exact database image IDs and tested scope.
