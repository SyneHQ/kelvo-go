# SQL Server and Oracle sources

Query SQL Server or Oracle through pure-Go `database/sql` drivers: `go-mssqldb` (BSD-3-Clause) and `go-ora/v2` (MIT). No Oracle client runtime is required.

1. Provision credentials with only the intended SELECT grants.
2. Keep DSNs in environment variables:

```yaml
sources:
  - id: reporting
    type: sqlserver
    dsn_env: KELVO_SOURCE_SQLSERVER_DSN
  - id: ledger
    type: oracle
    dsn_env: KELVO_SOURCE_ORACLE_DSN
```

3. Submit native SELECT/WITH with the source's `connection_id`. Bind typed values in order using SQL Server `@pN` or Oracle `:N` markers.

SQL Server accepts `sqlserver://` or ADO/ODBC DSNs with `encrypt=true` or `strict`, verified TLS 1.2+, and no `TrustServerCertificate=true`. `ApplicationIntent=ReadOnly` can route Availability Group reads; grants still enforce access.

Oracle uses `oracle://` with TCPS (`SSL=enable`, `SSL VERIFY=true`). Certificates must chain to runtime CA roots. Custom CA/wallet provisioning is not supported.

Each execution uses a bounded one-connection pool and rollback-only transaction. Oracle sends `SET TRANSACTION READ ONLY`; SQL Server relies on database grants. Writes and session-control statements are rejected. Context cancellation covers connection, transaction and query operations, subject to the [worker cancellation grace](usage.md#native-cancellation-and-remote-cleanup) and provider cleanup behavior.

Results preserve NULLs, strings, binary, integer widths, floats, Boolean, dates, timestamps and supported exact decimals. Naive temporal values keep wall-clock components; Oracle DATE keeps its time component. Unsupported nanosecond ranges, per-value timezone offsets, standalone TIME, unusable decimal metadata and decimals already rounded through float fail. Public errors omit DSNs and driver diagnostics.

For cross-source joins, configure the [selected-table federation bridge](federation-adapters.md).

## Validation status

[Live SQL Server 2022 acceptance](evidence/federation-sqlserver.json) covers typed values, TLS, permissions and cross-source joins; [the validation record](validation.md#expanded-federation-and-public-adapter-sdk) gives its scope. Oracle has development tests but still needs live TCPS, permissions and bind-marker acceptance in the target environment.
