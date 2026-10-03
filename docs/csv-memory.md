# CSV reader memory options

Smaller DuckDB CSV buffers can help constrained workers. The pinned default buffer is 32,000,000 bytes; other query allocations still apply.

Set both values as decimal byte strings:

```yaml
sources:
  - id: events
    type: csv
    path: /data/events.csv
    options:
      buffer_size: "1048576"
      maximum_line_size: "262144"
```

| Setting | Range |
| --- | --- |
| `buffer_size` | 262144–268435456 bytes |
| `maximum_line_size` | 1–67108864 bytes |
| Relationship | Buffer is at least four times the line setting |

For the default 2,000,000-byte line setting, use an 8,000,000-byte buffer. Test multiline and longest records before lowering it.

Both options are required together. Signs, leading zeroes, units, whitespace and unknown CSV options fail validation. Omit both to keep defaults. Changing them invalidates accelerated fingerprints until refresh.

The settings never enable row skipping or truncation. DuckDB can accept some lines beyond the setting, so it is not an exact security boundary. Complete values and rows must still be preserved.

## Validation and measurement

Run the sandboxed comparison on the designated Linux host with a new report path:

```sh
python3 scripts/csv_memory_acceptance.py --binary bin/kelvo \
  --launcher bin/kelvo-landlock --output artifacts/csv-memory-new.json
```

The [Azure run](evidence/csv-memory-acceptance.json), through runtime `f3886b7`, passed all 12 cases and cleanup:

| DuckDB memory limit | Default CSV | 1 MiB buffer / 256 KiB line | 8,000,000 buffer / 2,000,000 line | Parquet control |
| --- | --- | --- | --- | --- |
| 16 MiB | Resource exhausted | Correct result | Resource exhausted | Correct result |
| 32 MiB | Resource exhausted | Correct result | Correct result | Correct result |
| 64 MiB | Resource exhausted | Correct result | Correct result | Correct result |

The successful 16 MiB / 1 MiB-buffer case sampled **80.61 MiB worker RSS**. It fit DuckDB's managed budget, not a 16 MiB process cap.

This was one tiny-input run with startup and 10ms RSS sampling, without cold-cache control. It does not establish throughput, concurrency, general memory bounds or Oracle micro-VM capacity. Seven harness-control checks also passed.

[CSV options](https://duckdb.org/docs/stable/data/csv/overview.html) · [Pinned defaults](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/execution/operator/csv_scanner/csv_reader_options.hpp) · [Buffer allocation](https://github.com/duckdb/duckdb/blob/v1.5.6/src/execution/operator/csv_scanner/buffer_manager/csv_buffer.cpp)
