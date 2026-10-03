# CSV reader memory options

Native CSV sources can opt into smaller DuckDB reader buffers. This helps small
workers avoid allocating a large scanner buffer before a query processes even a
small file. The pinned DuckDB reader defaults to a 32,000,000-byte buffer; each
query can need other scanner and analytical allocations as well.

Configure both options as decimal byte strings:

```yaml
sources:
  - id: events
    type: csv
    path: /data/events.csv
    options:
      buffer_size: "1048576"
      maximum_line_size: "262144"
```

This requests a 1 MiB buffer and a 256 KiB supported line length. For data that
needs the pinned default 2,000,000-byte line setting, use `buffer_size: "8000000"`
and `maximum_line_size: "2000000"`. Test the actual input format and longest
records before choosing a smaller line setting.

| Setting | Accepted values |
| --- | --- |
| `buffer_size` | 262144 through 268435456 bytes |
| `maximum_line_size` | 1 through 67108864 bytes |
| Relationship | Buffer must be at least four times the line setting |

Both options are required together. Leading signs, leading zeroes, units,
whitespace, missing partners and unknown native CSV options are rejected before
source access. Kelvo passes validated integer literals to trusted source setup.
Omitting options keeps the pinned defaults. External adapters retain their own
option contract.

DuckDB couples these defaults: changing just the buffer can also change the line
setting, and lowering just the line setting need not shrink the buffer. Explicit
paired settings make that choice reviewable. These settings do not enable
`ignore_errors`, truncate values or skip malformed rows. The parser can accept
some lines larger than its setting; `maximum_line_size` is not an exact security
boundary. A successful query must preserve complete values and rows.

CSV options participate in accelerated dataset fingerprints. Changing them
invalidates snapshots produced with the previous configuration until a full
refresh succeeds. SQL row/byte/time limits and tenant admission continue to apply.

## Validation and measurement

Focused tests cover strict YAML/programmatic validation, SQL escaping, a small
correlated aggregate at a low memory limit, quoted multiline strings, UTF-8,
long values and explicit failure rather than silent truncation. The dedicated
Linux runner compares three CSV settings and a Parquet control at 16, 32 and
64 MiB managed limits through the actual sandbox launcher:

```sh
python3 scripts/csv_memory_acceptance.py --binary bin/kelvo \
  --launcher bin/kelvo-landlock --output artifacts/csv-memory-new.json
```

The runner requires a new report path, validates exact Arrow values, samples
worker RSS and cleans its private fixtures before writing success evidence.
Managed memory settings and sampled RSS are separate observations. This tiny
input comparison does not establish general memory bounds, throughput, concurrent
tenant capacity or performance on an Oracle micro VM.

The [recorded Azure run](evidence/csv-memory-acceptance.json) passed all twelve
cases with runtime changes through `f3886b7` and completed fixture cleanup:

| DuckDB memory limit | Default CSV | 1 MiB buffer / 256 KiB line | 8,000,000 buffer / 2,000,000 line | Parquet control |
| --- | --- | --- | --- | --- |
| 16 MiB | Resource exhausted | Correct result | Resource exhausted | Correct result |
| 32 MiB | Resource exhausted | Correct result | Correct result | Correct result |
| 64 MiB | Resource exhausted | Correct result | Correct result | Correct result |

The successful 16 MiB / 1 MiB-buffer case sampled about **80.61 MiB worker RSS**.
It demonstrates that the smaller scanner request lets this query fit the managed
budget; it does not demonstrate a 16 MiB process or lower RSS than a query that
failed before doing the same work. This is one tiny-input run with startup and
10 ms sampling, without cold-cache control or throughput statistics. Seven
synthetic harness-control tests also passed.

References: [DuckDB CSV options](https://duckdb.org/docs/stable/data/csv/overview.html),
[pinned reader defaults](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/execution/operator/csv_scanner/csv_reader_options.hpp),
and [pinned buffer allocation](https://github.com/duckdb/duckdb/blob/v1.5.6/src/execution/operator/csv_scanner/buffer_manager/csv_buffer.cpp).
