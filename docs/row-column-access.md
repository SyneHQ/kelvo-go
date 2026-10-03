# Row and column access

Use this with [principal keys](principal-access.md) to restrict callback federation tables and [local/object accelerated snapshots](guarded-snapshots.md) before SQL runs. Kelvo hides unlisted tables and columns, then filters Arrow rows before joins, CTEs, aggregates and windows.

## Configure a grant

Add `row_column_policy` to a principal in the identical gateway, worker and provisioned tenant policy:

```yaml
access:
  revision: 8
  principals:
    analyst:
      kind: user
      federated_sources: [sales]
      row_column_policy:
        sources:
          sales:
            tables:
              orders:
                columns: [order_id, amount]
                rows:
                  kind: comparison
                  column: account_id
                  type: int64
                  op: eq
                  value: "42"
```

The table alias must exist in the source's `federation.tables`. Here, `account_id` is read only to evaluate policy; SQL cannot select it. To allow every row while restricting columns, replace `rows` with `all_rows: true`. Exactly one is required.

## Supported rules

| Rule | Fields |
| --- | --- |
| Comparison | `kind: comparison`, `column`, `type`, `op`, quoted `value` |
| NULL check | `kind: is_null` or `is_not_null`, `column` |
| Combined rules | `kind: and` or `or`, nonempty `children` list |

Comparisons support `eq`, `ne`, `lt`, `le`, `gt`, `ge` with exact signed/unsigned integer widths, booleans and UTF-8 strings. No implicit casts: an `int32` column needs `type: int32`. NULL comparisons never authorize a row; use a NULL check. Strings use byte ordering, independent of database collation.

Projected columns support flat Arrow types, including decimals and timestamps. Nested, dictionary and extension columns are rejected. Schema and field metadata are removed. Unsupported predicates fail explicitly.

## Boundaries

- Restrictions apply to selected callback federation sources. Missing tables are denied; sources with explicit whole-source grants keep that authority.
- Restricted sources cannot also have native SQL grants. Restricted queries cannot mix direct attachments, raw files or unguarded object readers; scan diagnostics are disabled. Snapshots require their own dataset-ID policy and the [guarded reader](guarded-snapshots.md).
- Policies come from authenticated, versioned operator configuration. Public query fields cannot supply or override them. Each worker checks the authority digest and the child checks its trusted policy envelope before source setup.
- Source adapters receive required columns without filters. Kelvo applies policy and optimizer filters locally, so an adapter ignoring pushdown cannot bypass the boundary. This can increase source traffic.
- Raw source budgets still apply, even when few rows are authorized. Filtering adds a mask and selected-column buffers per batch. Timing and resource usage can reflect raw data; this is not a timing-isolation guarantee.
- Equivalent views, aliases and snapshots need their own grants. Kelvo does not infer data lineage between separately granted sources.

Native SQL, export redownloads and cache policy integration remain in [#28](https://github.com/SyneHQ/kelvo-go/issues/28). Standalone `query`/`serve` do not authenticate these cluster principal grants.

## Change or revoke access

Follow the [drained policy cutover](principal-access.md): stop admission, cancel or drain old jobs, provision a fresh tenant broker account, and start gateways/workers with identical policy. Old or mismatched policy metadata is rejected. Do not edit broker metadata in place.

The effective policy digest includes every row/column rule. Limits: 64 principals, 256 KiB serialized access policy per tenant; each row/column policy allows 64 sources, 32 tables total, 1,024 columns per table, 1,024 predicate nodes and 64 KiB of policy text.

[Combined validation](evidence/row-policy-combined.json) · [Guard and serialization checks](evidence/row-policy-guards.json)
