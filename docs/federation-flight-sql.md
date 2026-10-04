# Flight SQL federation

Join registered Flight SQL tables with other Kelvo sources using the [DuckDB bridge build](federation.md#build-and-update).

## Connect

1. Grant a service identity read access to the selected tables.
2. Set `KELVO_SOURCE_FLIGHT_URL` to `grpcs://host:port` and `KELVO_SOURCE_FLIGHT_TOKEN` to its bearer token.
3. Register the tables in your YAML configuration:

```yaml
sources:
  - id: lakehouse
    type: arrow_flight
    url_env: KELVO_SOURCE_FLIGHT_URL
    token_env: KELVO_SOURCE_FLIGHT_TOKEN
    options:
      protocol: flightsql
      federation_dialect: ansi
    federation:
      max_scan_rows: 1000000
      max_scan_bytes: 268435456
      tables:
        - name: orders
          schema: reporting
          table: orders
```

4. Select `lakehouse` and query `lakehouse.orders`. Add other registered sources to join them:

```sql
SELECT customer_id, SUM(amount) AS revenue
FROM lakehouse.orders
WHERE status = 'paid'
GROUP BY customer_id;
```

The `ansi` profile requires double-quoted identifiers and `SELECT * FROM "schema"."table" WHERE 1 = 0` discovery. Use explicit schemas; `database` is unsupported. Flight SQL defines a wire protocol, so compatibility with this SQL profile must be verified for each service.

## Execution and limits

| Work | Runs in |
| --- | --- |
| Selected columns | Flight SQL source |
| User filters, joins, aggregates, ordering and LIMIT | DuckDB |
| Mandatory row/column policy | Kelvo's existing local policy guard, before DuckDB sees rows |

TLS certificate and hostname verification are required. Only one result endpoint on the configured server is accepted; credentials never follow a server-supplied redirect. Discovery and each scan use independent bounded operations. Scans fail on row/decoded-byte overflow, cancellation or schema change; they never silently truncate join input. Supported Arrow scalar fields retain NULLs and precise values; schemas unsupported by the bridge fail explicitly.

No remote predicate, aggregate or join pushdown is advertised. A required source filter is rejected explicitly. Local filtering can transfer more rows; source grants, scan limits and worker containment remain necessary. Memory limits here do not cap total process RSS. Scans do not share a transactional source snapshot, and encoded Flight wire-byte counts are unavailable.

## Coverage

The [Linux acceptance record](evidence/flight-sql-federation.json) verifies two SQL-backed TLS fixture services, exact CTE/cross-source joins, policies, limits and cancellation. Regular, race, native, strict-cgo race, vet and CLI build passed on `a6a11b1`; the earlier test-collector failure and expected skips remain recorded.

The adapter reuses Kelvo's outgoing [Flight SQL connector](sources-adapters.md) and public [federation contract](../federation/federation.go). It does not provide an incoming Flight SQL server. Provider-specific acceptance and performance require separate measurements; a protocol fixture cannot certify all Flight SQL services.
