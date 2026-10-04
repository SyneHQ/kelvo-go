"""Deterministic source, CTEs and complete Arrow validation; no import side effects."""
from pathlib import Path

ROWS = 1_000_000
BLOCK = 65_536
EOS = b'\xff\xff\xff\xff\0\0\0\0'
MAX_BODY = 96 << 20
SQL = {
    'aggregate': '''WITH base AS (
 SELECT id, metric, CAST(id % 10 AS BIGINT) AS bucket FROM sample
), filtered AS (
 SELECT * FROM base WHERE id % 11 <> 0
), grouped AS (
 SELECT bucket, CAST(count(*) AS BIGINT) AS n,
 CAST(count(metric) AS BIGINT) AS present,
 CAST(sum(metric) AS BIGINT) AS total FROM filtered GROUP BY bucket
) SELECT bucket, n, present, total FROM grouped ORDER BY bucket''',
    'transfer': '''WITH base AS (
 SELECT id, metric, CAST(id % 10 AS BIGINT) AS bucket FROM sample
), scored AS (
 SELECT id, metric, bucket, CAST((id % 1000) * 3 + 7 AS BIGINT) AS score FROM base
) SELECT id, metric, bucket, score FROM scored ORDER BY id''',
}


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def generate(path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    require(not Path(path).exists(), 'FRESH_DATA_REQUIRED')
    schema = pa.schema([('id', pa.int64()), ('metric', pa.int64())])
    with pq.ParquetWriter(path, schema, compression='zstd') as writer:
        for start in range(0, ROWS, BLOCK):
            ids = range(start, min(start + BLOCK, ROWS))
            writer.write_table(pa.table({'id': pa.array(ids, type=pa.int64()),
                'metric': pa.array([i % 101 if i % 17 else None for i in ids], type=pa.int64())}, schema=schema))
    expected = [{'bucket': i, 'n': 0, 'present': 0, 'total': 0} for i in range(10)]
    for i in range(ROWS):
        if i % 11:
            row = expected[i % 10]
            row['n'] += 1
            if i % 17:
                row['present'] += 1
                row['total'] += i % 101
    return expected


def verify(body, workload, expected):
    import pyarrow as pa
    require(0 < len(body) <= MAX_BODY and body.endswith(EOS), 'ARROW_EOS_OR_BOUND')
    source = pa.BufferReader(body)
    reader = pa.ipc.open_stream(source)
    names = ['bucket', 'n', 'present', 'total'] if workload == 'aggregate' else ['id', 'metric', 'bucket', 'score']
    require(reader.schema.names == names and all(f.type == pa.int64() for f in reader.schema), 'ARROW_TYPES')
    count, batches = 0, 0
    actual = []
    for batch in reader:
        batches += 1
        require(batches <= 16_384 and count + batch.num_rows <= ROWS, 'ARROW_ROW_BOUND')
        if workload == 'aggregate':
            require(count + batch.num_rows <= 10, 'AGGREGATE_ROW_BOUND')
            actual.extend(batch.to_pylist())
        else:
            ids = range(count, count + batch.num_rows)
            wanted = [pa.array(ids, type=pa.int64()),
                pa.array([i % 101 if i % 17 else None for i in ids], type=pa.int64()),
                pa.array([i % 10 for i in ids], type=pa.int64()),
                pa.array([(i % 1000) * 3 + 7 for i in ids], type=pa.int64())]
            require(all(batch.column(i).equals(column) for i, column in enumerate(wanted)), 'TRANSFER_VALUES')
        count += batch.num_rows
    require(source.tell() == len(body), 'TRAILING_ARROW_BYTES')
    require(actual == expected if workload == 'aggregate' else count == ROWS, 'EXACT_RESULT')
    return {'rows': count, 'batches': batches, 'wire_bytes': len(body), 'exact_values': True,
            'exact_types': True, 'explicit_eos': True, 'no_trailing_bytes': True}
