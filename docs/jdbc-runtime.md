# Optional JDBC runtime

Use JDBC only for an enabled compatibility profile: H2, Hive, Spark, DB2,
SAP HANA or SAP ASE. Redis uses the native Go adapter. Vendor-driver acceptance remains
separate from runtime and protocol tests.

1. Install a dedicated JRE and approved driver JARs under a root-owned path.
2. Remove symlinks by copying their targets. Keep every ancestor non-writable
   by group or other users.
3. Create a YAML inventory of **every** regular JRE file and its SHA-256.
4. Configure the ordered adapter/driver JAR list, then restart the worker.

```yaml
operations:
  adapter:
    binary: /opt/kelvo/bin/kelvo-adapter-go
    sha256: <verified-go-adapter-sha256>
    jdbc:
      java_home: /opt/kelvo/jre
      manifest: /opt/kelvo/jre.yml
      manifest_sha256: <verified-manifest-sha256>
      profiles:
        h2:
          - path: /opt/kelvo/jars/kelvo-jdbc-adapter.jar
            sha256: <verified-jar-sha256>
```

The JRE inventory uses `version: 1` and a `files` list containing relative
`path` and `sha256` entries. It must include `bin/java`, `lib/modules`,
`lib/libjli.so` and `lib/server/libjvm.so`, plus all remaining regular files.
Unlisted files, unknown fields and symlinks fail startup.

The node pins the runtime once. JDBC operations check its inode identities and
configuration before use; native operations skip the JVM path. Shutdown closes
the pins after active operation workers stop. Credentials remain per-operation
private pipe input, with no environment or command-line handoff.

JDBC requires at least 256 MiB of operation memory, plus the configured node
admission overhead. Heap and direct-memory limits leave a 128 MiB reserve; the
cgroup bounds the process tree. This is not a guarantee of total JVM RSS.

Sources cannot select Java flags, class names, JARs or local paths. H2 and DB2
declare transaction support; Hive, Spark and SAP expose autocommit only.
Uncertain writes require reconciliation and are never replayed automatically.
