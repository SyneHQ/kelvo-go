# JDBC worker

Optional Java 21 adapters for H2, Hive, Spark, DB2 and SAP. The main Kelvo
binary stays Go; each JDBC operation gets a bounded, isolated JVM.

| Profile | Driver | Validation |
| --- | --- | --- |
| H2 | Bundled H2 2.5.252 | TLS query, metadata, exact decimal Arrow, cancellation and rejection tests pass |
| Hive / Spark | Operator-pinned Hive JDBC JARs | Live provider gate required |
| DB2 | Operator-pinned IBM JCC JARs | Live provider gate required |
| SAP HANA | Operator-pinned `ngdbc` JAR | Live provider gate required |
| SAP ASE | Operator-pinned `jconn4` JAR | Live provider gate required |

Build on your Linux build host with JDK 21 and Maven:

```sh
cd adapters/jdbc
mvn verify
```

Install the JVM and JARs under a root-owned directory. Pin every runtime file in
a YAML inventory and each profile JAR by SHA-256. The worker rejects writable
ancestors, symlinks, unexpected files and changed artifacts.

```yaml
java_home: /opt/kelvo/java-21
manifest: /opt/kelvo/java-21.yml
manifest_sha256: <inventory-sha256>
profiles:
  h2:
    - path: /opt/kelvo/kelvo-jdbc-adapter.jar
      sha256: <jar-sha256>
```

Configure this object in the worker's `operations.adapter.jdbc` field. Runtime
configuration belongs to the operator; saved connections cannot select JARs,
classes, executable paths or JVM flags. Start with at least 256 MiB per operation
and include the worker's separate runtime overhead in its memory pool.

Connections require verified TLS, a saved database and fresh credentials. The
private source uses `tls://hostname:port`; user-provided JDBC URLs and connection
options are rejected. Credentials travel through inherited pipes.

Results stream as Arrow IPC. Exceeding a row or byte limit fails the operation.
H2 and DB2 support required transactions for DML; Hive, Spark and SAP use
autocommit. Unknown write outcomes require reconciliation before retrying.

Run every provider's live acceptance before enabling it. A declared profile is
not evidence that a vendor driver works inside the sandbox. See
[database operations](../../docs/database-operations.md) for the caller contract.

ASE uses the vendor's `SybSocketFactory` interface with a fixed TLS bridge.
It validates the source host before returning a socket. The vendor JAR must
expose one of the supported interface packages; incompatible JARs fail closed.
Redis uses the native Go adapter.
