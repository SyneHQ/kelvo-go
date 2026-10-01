# SQL Server and Oracle sources

Kelvo supports native, single-source read queries against Microsoft SQL Server
and Oracle through Go `database/sql` drivers. SQL Server uses the official
pure-Go `github.com/microsoft/go-mssqldb` driver (BSD-3-Clause); Oracle uses
pure-Go `github.com/sijms/go-ora/v2` (MIT). Neither connector needs an Oracle
client library or native runtime.

Use a `dsn_env` source configuration. The DSN stays in an environment variable
and never appears in the catalog or query responses.

```yaml
sources:
  - id: reporting
    type: sqlserver
    dsn_env: KELVO_SOURCE_SQLSERVER_DSN
  - id: ledger
    type: oracle
    dsn_env: KELVO_SOURCE_ORACLE_DSN
```

Run these through native mode with `connection_id` set to the configured source
ID. SQL Server accepts the driver’s `sqlserver://` URL or ADO/ODBC-style DSN;
set `encrypt=true` (or `strict`) with verified TLS 1.2 or newer. Connections
without encryption or with `TrustServerCertificate=true` are rejected. Use
`ApplicationIntent=ReadOnly` when connecting to an Availability Group; it is a
routing hint, not an authorization policy. Oracle accepts the `oracle://` DSN
supported by go-ora and requires TCPS (`SSL=enable`, `SSL VERIFY=true`). Server
certificates must chain to trusted runtime CA roots. Custom CA/wallet files need
an explicitly designed sandbox provisioning path and are not enabled here.

Provision database credentials with only the required `SELECT` permissions.
The connector accepts one SELECT or WITH statement, rejects write and
session-control statements, and creates a bounded one-connection pool per
execution. Oracle additionally sends `SET TRANSACTION READ ONLY`. SQL Server
does not have an equivalent session command, so its database grants and any
read-only routing policy are the enforcement boundary.

Context cancellation is passed through connection, transaction, and query
operations. Result conversion supports nulls, strings, binary values, signed
and unsigned integers, floats, booleans, dates, timestamps, and decimal values
within Arrow’s precision bounds. Naive DATE/DATETIME/TIMESTAMP values preserve
wall-clock components; Oracle DATE retains its time component. Nanosecond values
outside Arrow's representable range, per-value timezone offsets, standalone
TIME, unknown decimal precision/scale, and decimal values already decoded into
floating point are rejected. Source errors are returned without DSNs or driver
diagnostics.

Use the driver’s documented parameter markers: SQL Server normally uses named
`@pN` parameters; Oracle normally uses `:N` bind parameters. Kelvo passes the
request’s typed parameter values directly to `database/sql` in order.


## Validation status

The connector packages are development-validated with focused unit tests for
Arrow conversion, request validation, and the conservative SQL syntax envelope.
They have not been verified against a live SQL Server or Oracle instance in this
repository; validate driver-specific DSNs, permissions, TLS, and bind markers
in the target environment before production use.
