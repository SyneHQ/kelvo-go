# Database operation validation

Validation separates protocol fixtures from real database acceptance. A passing
fixture does not certify an account, deployment or throughput target.

| Boundary | Evidence |
| --- | --- |
| Gateway to worker | PostgreSQL/MySQL queries and writes with API source egress denied; exact integers, current credentials, revocation and foreign-team denial |
| Cluster lifecycle | Real PostgreSQL/MySQL ingestion, watchers and migrations; retained results after worker restart |
| Process isolation | Linux resource admission, filesystem containment, cleanup and pinned JDBC execution; regular and race checks |
| Federation | Provider protocol fixtures, isolated source catalogs and DuckDB C++ bridge checks with `cgocheck2` |
| Native database fixtures | PostgreSQL, MySQL, MariaDB, SQL Server, CockroachDB, ClickHouse, Cassandra, Ignite, MongoDB and H2; tested operations vary by engine |
| Cloud providers | Verified-TLS protocol fixtures; live account acceptance remains separate |

The combined suite exposed an outdated Databricks parameter fixture and an
uncached transitive module in offline validation. The fixture was corrected and
the module checksum verified; affected checks were rerun. Failed receipts are
retained alongside successful follow-ups.

Review also tightened MySQL transaction scope and uncertain rollback outcomes.
Oracle cancellation tests now wait for asynchronous connection cleanup while
still requiring exactly one connection and rollback, with no commit.

Live Oracle TCPS, Exasol, ScyllaDB and cloud-provider gates remain open where
dedicated accounts are unavailable. Watcher support does not imply incremental
acceleration. Capacity and failover require deployment-specific testing.

See [capabilities](../adapters/go/README.md), [operation outcomes](database-operations.md#retry-and-outcomes)
and the [production roadmap](production-roadmap.md).
