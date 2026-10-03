# Native source error classification

PostgreSQL/MySQL adapters classify exact typed driver codes so scheduled refreshes can stop on known access/configuration failures. Raw server messages remain private.

## Public error boundary

Caller cancellation/deadlines take precedence. Otherwise, recognized driver metadata maps to public codes; unknown, untyped or conflicting metadata stays `QUERY_FAILED`. No text matching is used.

Trusted configuration/sink errors keep their public code, including `UNSUPPORTED` result types. Messages identify the failed stage without SQL, names or connection strings.

## PostgreSQL

Wrapped `pgconn.PgError` values use these exact SQLSTATE mappings:

| SQLSTATE | Public code |
| --- | --- |
| `28000`, `28P01` | `UNAUTHENTICATED` |
| `42501` | `PERMISSION_DENIED` |
| `3D000`, `3F000` | `CONFIGURATION_ERROR` |
| `42601`, `42703`, `42883`, `42P01`, `42P02` | `INVALID_ARGUMENT` |
| `08000`, `08001`, `08003`, `08006`, `40001`, `40P01`, `53300`, `55P03`, `57P01`, `57P02`, `57P03`, `57014` | `UNAVAILABLE` |

`57014` alone cannot distinguish timeout from administrative cancellation; context can override it. Compatible PostgreSQL adapters inherit this policy, not automatic product validation. [PostgreSQL reference](https://www.postgresql.org/docs/current/errcodes-appendix.html)

## MySQL and MariaDB

Configured MySQL/MariaDB sources inspect `mysql.MySQLError`. The number must match; an omitted SQLSTATE is accepted, but a conflicting supplied state remains unknown.

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

MariaDB's `3024` means something different and stays unknown. `1317` alone does not establish a deadline. Other generic SQL adapters do not inherit this mapper. [MySQL codes](https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html) · [MariaDB 3024](https://mariadb.com/docs/server/reference/error-codes/mariadb-error-codes-3000-to-3099/e3024)

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

Retries share five failures per exact dataset/configuration, with jitter capped at five minutes. Permanent errors persist across restarts. Repair the cause, then [reset explicitly](operations.md#source-refresh-failures-and-recovery). This governs cluster refreshes, not user-query replay or standalone `watch`.

**Revoked source grants do not revoke retained snapshots.** Change `authorization_version` and propagate the catalog when readers must lose access. Resetting retries or changing secret values alone is insufficient.

## Validation scope

[TLS acceptance](evidence/native-error-relational-acceptance.json) passed eight outcomes on isolated PostgreSQL 17.6/MySQL 8.4: wrong passwords and revoked SELECT preserved public codes and previous snapshot identity, bytes, rows and age.

Ordinary/bridge suites, focused race checks, vet and build passed on Azure. Ten harness checks covered cleanup, stale outputs and redaction. Earlier fixture failures—invalid secret namespace and nonprivate directories—remain in the evidence; runtime rules were not relaxed.

Runtime milestone: `c3a3935`. This proves direct engine/in-process refresh behavior, not live worker IPC/NATS retry integration, MariaDB compatibility, sustained load or deployment readiness.
