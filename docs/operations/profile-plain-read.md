# Profile of one plain read

One ordinary read, followed end to end: a PostgreSQL `SELECT * FROM t ORDER BY id LIMIT 50` (50 rows, 3.1 KB of Arrow
on the wire), from the Go client through the gateway's single-call path, the node, a warm read worker and the source,
at 80 ms round trip to the source and 300 ms per credential resolve. Measured at **low load** (one reader, 3 reads per
second) and at **50 concurrent** (50 closed-loop readers on one connection).

Status: measured on the integration build (#223) **with measurement patch 0001** (the receipt-batch defect), result cache
off so every read reaches the worker, 4 vCPU / 8 GB node, 20 workers. Two runs: CI run 38791531066 (run 1) and
38793567524 (run 2, adds an idle window and the ed25519 cost); raw profiles are the artifact `pooled-read-profile` of each.
The CI runners are shared, and **absolute CPU differed by 2x between the two runs for the same work** (50 concurrent: 47 s vs 24 s
of node CPU for 900 reads). Shares and counts agree; treat absolute CPU as a range. Code-reading facts are marked *(code)*;
everything else was measured.

## Ranked cost: where one read goes

### Wall time at low load (client p50 334 ms in run 2, 386 ms in run 1)

| Rank | Stage | Time | How known |
| --- | --- | --- | --- |
| 1 | **Source round trips: 4 x 80 ms** | **320 ms** (96% / 83%) | proxy counts 4.06 request/response turns per read; PostgreSQL's own statistics show exactly four statements per read: `begin read only`, `SELECT set_config('statement_timeout', ...)`, the query, `rollback` (179 calls each) |
| 2 | Gateway before the node: audit start, job store create/enqueue, wait for the node to take the job, claim | 6 ms (run 2), 56 ms (run 1) | gateway first byte minus node handler time. It contains a 50 ms poll tick (about 25 ms on average) and varies with the runner |
| 3 | Node and adapter work: auth, worker acquire, frames, Arrow decode/encode, hash, verify | 6 ms | node handler p50 326 ms minus 320 ms; the adapter alone, run in process against the same source, took 323.8 ms p50, so **the adapter is 4 ms** and the node 2 ms |
| 4 | Gateway after the node: commit, trailer | 1.3 ms | gateway total minus first byte |
| 5 | Client: TLS, send, decode, receipt check | 0.6 ms | client p50 minus gateway total |
| (cold) | First read on a new connection: resolve 300 ms, two custody checks (2.5 ms each), worker spawn, a new TLS connection to the source (16.8 turns) | **1.81 s** p50 (5 cold reads: 1.64-2.01 s) | `plain-cold.json` |

Warm credentials cost nothing: 2 resolves in a 64 s window at low load, 12 in the whole run. The cold read is 5.4x a warm one.

### Wall time at 50 concurrent (client p50 2.66 s, p95 5.6 s; 14.1 reads/s)

| Stage | Time | How known |
| --- | --- | --- |
| **Waiting for one of the worker's 4 source connections** | **2.6 s** | node handler p50 2.93 s minus the 326 ms of work |
| Gateway before the node | 20 ms | gateway first byte minus node handler |
| Source round trips | 320 ms | same 4 turns |

Throughput is bounded by the adapter's fixed pool: 4 connections / (4 turns x 80 ms) = **12.5 reads/s**; measured 14.1 (client)
and 12.1 (adapter alone, in process). Fifty readers on one connection therefore wait about 50 / 12.5 = 4 s in a closed loop.
**14% of reads (125 of 891) failed** with 503 `Worker unavailable` at this concurrency; the classes of the node's own failure
log are in `node_failed_reads_logged` of run 3's report (see the table at the end if filled in).

### CPU at 50 concurrent, per read (run 2; run 1 shares in brackets)

Net of an idle node, which costs **0.083 cores (2.6 s of CPU per 31 s)** with no read at all, 26% of it TLS writes to the broker
(lease renewal, consumer fetch, key-value watches).

Node (gateway, node, resolver stub and NATS client in one process): 20.75 s of profile for 896 reads, 18.2 ms per read net.
Worker processes: 0.93 s of cgroup CPU, **1.0 ms per read**. In-process adapter profile: 1.7-2.1 ms per read.

| Rank | Where (cumulative, so rows overlap) | ms per read | Share of node profile |
| --- | --- | --- | --- |
| 1 | **Gateway `streamQuery`** | 7.05 | 30% [37%] |
| | of which `submitJob` (`NATSStore.Submit`: slot scan, create, get, update, JSON) | 6.06 | 26% [31%] |
| | of which `relayResults` (polling and relay) | 0.89 | 4% [5%] |
| | of which `commitResult` | 0.32 | 1.4% [1.9%] |
| 2 | `json.Unmarshal` (job records read back from the broker; strict decode of grants) | 4.87 | 21% [23%] |
| 3 | `tls.Conn.Write`, 98% of it the broker client | 3.68 | 16% [19%] |
| 4 | Delegation verification (all callers) | 1.33 | 5.7% [7.3%] |
| | of which the Ed25519 arithmetic | 0.73 | 3.1% [3.7%] |
| 5 | Node `ServeHTTP` (the whole read on the node) | 0.83 | 3.6% [4.2%] |
| | of which **the read-worker path** (`executeOnReadWorker`) | **0.20** | **0.9%** [0.8%] |
| 6 | Audit journal goroutines (gateway and node) | 0.49 | 2.1% [1.5%] |
| 7 | Node `dispatch` (fetch, assign) and `watch` (lease) | 0.40 + 0.27 | 1.7% + 1.2% |
| 8 | GC mark workers | 0.46 | 2.0% [1.8%] |

**The pooled read worker is about 1.2 ms of about 19 ms**: 0.2 ms in the node and 1.0 ms in the adapter. The rest, about 94%, is the
control plane around one read: the job store on JetStream over TLS, grants, audit. At low load the same read costs about 6 ms net (the node
was mostly idle: idle baseline 5.3 of 5.4 s of profile in a 64 s window is the larger part, so per-read numbers are small differences).

Signature arithmetic is not the cost of a verification: `delegation.Verify` is 0.89 ms per read (low) of which `ed25519.Verify` is
0.34 ms (a verification takes 37 microseconds on this node). The other 62% (45% at 50 concurrent) is the strict JSON decode of the token, the
query digest and base64.

### Allocations (alloc_space)

| | Per read | Largest sources |
| --- | --- | --- |
| Node, gateway and resolver stub (one process) | **1.93 MB per read** at 50 concurrent (1.73 GB over 896 reads); 3 KB of result | `Gateway.streamQuery` 53% (cumulative); NATS `getMsg` 29%; `nats.(*Conn).processMsg` 22% (flat); `json.Unmarshal` 27%; `VerifyClaims` 9%. Heap in use 48-52 MiB, GC 60 cycles, 6.7 ms of pause in 66 s |
| Adapter, in process | **89 KB and 2,362 mallocs per read**; heap in use 9.7-12.1 MiB | `math/big` 18% (decimal `numeric` columns are converted through big numbers), Arrow allocator 7.5%, JSON decoder 11% (the frame header), frame read 4%; one-time TLS certificate parsing |

The harness's own sampler (`rssOf`) is 5% of the low-load allocations; it is harness cost, not product.

### Memory of a warm worker (8 workers sampled, both windows)

| | KiB |
| --- | --- |
| Resident total (`VmRSS`) | 62,000-67,600 |
| Anonymous: Go heap, stacks, runtime | 19,000-24,200 (`RssAnon` 19,700-24,200) |
| Binary text (`r-x`) | 18,420 |
| Binary read-only data (`r--`) | 20,000-21,100 |
| Binary data (`rw-`) | 872 |
| C libraries (libstdc++ 1,448, libc 1,428, libm 364, others 336) | about 3,600 |
| cgroup `memory.current` of one worker | 20.7-26.7 MB |

The adapter binary is **154.6 MB** on disk; 42 MB of it is resident and **shared by every worker** (page cache is charged once), so a
worker's own memory is the 21 MiB of anonymous pages and 7 threads. Summing `VmRSS` over workers counts the shared 42 MiB once per worker.
The `libstdc++` mapping shows the build carries a C++ runtime (the `duckdb_arrow` build tag) into a relational read worker.

## Repeat work along adapter, node, gateway, client

### Result bytes *(code, from a read of the path; frame count measured)*

For this read, one batch and a cache miss. The adapter wrote **14 result frames for 3,128 bytes** (measured, `result_frames_per_read`): Arrow IPC
writes each metadata block, buffer and padding separately and nothing buffers them (`process.go:319`, `frameResultWriter`), so each costs two
syscalls on the adapter's stdout, one allocation and one read on the node.

| Operation | Times | Where |
| --- | --- | --- |
| Arrow record built from rows | 1 | adapter `rowarrow.go:57` |
| **IPC encode** | **2** | adapter `process.go:319`; node `IPCSink`, `worker.go:712` (re-encodes what it just decoded) |
| **IPC decode** | **2** | node `ipc.go:65` (validates and decodes); client `ipc.go:228` |
| **SHA-256 over the wire bytes** | **2** | adapter `boundedHashWriter` (`process.go:281`); node `framedResultReader` (`warm_pool.go:1016`), compared with the receipt's digest |
| Body copies | node 4 (frame allocation, reader copy, cache tee `result_cache.go:164` on a miss, Arrow read buffers), gateway 1 (`io.CopyBuffer`, `gateway.go:808`), client 1 | |
| Verifies of the result | adapter row count; node receipt cross-check (`warm_pool.go:866`: rows, bytes, digest); gateway `WireBytes` and EOS (`gateway.go:820-826`); client receipt trailer (`query_stream.go:101`) | |
| Other hashing | `operations.Digest` twice (`read_worker.go:93`, `:105`), `resultCacheKey` once, `QueryDigest` in `delegatedExecution` twice (`worker.go:205`, `:224`) | |

A cache hit replaces all adapter work, both hashes and the receipt check with one decode and one encode on the node.

### Ed25519 verifications and JSON decodes per read *(code), with measured counts*

`verifies per read = 6 + polls`, where polls is the number of 50 ms ticks the gateway waits: once in `delegatedHTTPContext`, twice in
submission (`submitJob` and again inside `NATSStore.Submit`), `polls + 1` in the relay loop (`principalSnapshot`), once in
`commitResult`, once on the node (`delegatedAuthority`). The client verifies nothing. Each verification decodes the token twice (header
and payload, each walked and then decoded strictly).

Measured from the profile (`ed25519.Verify` time / 37 microseconds): **9 verifications per read at low load, 20 at 50 concurrent** (the
extra are not poll ticks: relay waits are short at 50 concurrent; the slot scan in `NATSStore.Submit` revalidates, which was not isolated).
A resolve adds **10** verifications and two gateway round trips (the resolver runs the custody check twice, each 4 verifications and 3 key-value
gets), but it happens once per 60 s per connection. With the verify-once change of #220, the floor is one verification at the gateway and one on
the node, **2 per read**, if the node must still verify independently.

### Job store and audit round trips per read *(code)*

| | Round trips |
| --- | --- |
| Gateway key-value and stream operations | polls + 10 (polls + 12 when a slot is reused) |
| Node | 11 + one lease `Get` per second held |
| **Total** | about **polls + 21** per read, over a TLS connection to the broker |
| Audit journal | **6 `fsync`s per read**: gateway submit start and finish, gateway results start and finish, node execution start and finish |
| Gateway slot search (`NATSStore.Submit`) | starts at slot 0 and costs a create and a get for each busy or fresh-terminal slot: O(occupied slots). `NATSStore.Submit` costs 1.2 ms per read at low load and 5.7 ms at 50 concurrent (4.7x for the same read), 25% of the node profile [30% in run 1], which is the signature of a scan, not of one create |

Measured: the broker client is the largest single consumer of node CPU after JSON: 16-20% in TLS writes at load and 26% when idle.

### Source round trips per read: **4** (measured, matches the code)

`begin read only` (1), `set_config('statement_timeout')` as an extended-protocol statement (1), the query (1), `rollback` (1); no pipelining, no
commit, and a connection idle over 1 s adds a ping (+1). The first use of a statement on a connection adds a parse/describe (+1). At 80 ms each
one is 80 ms of wall time and the reason a 4-connection worker serves at most 12.5 reads per second.

## What the numbers say about the three follow-ups

1. **Slim relational-only adapter binary.** It would remove nothing from a warm read's time or CPU (the adapter is 4 ms and 1 ms). It would
   cut the shared resident binary from 42 MiB toward the relational part (the build has 620 packages, 312 for the relational connector alone, and a
   C++ runtime for DuckDB) and the on-disk 154.6 MB; per worker the saving is zero (anonymous memory is 21 MiB either way). Cold spawn time was not separated
   from the 1.8 s cold read and is the likely gain.
2. **Fewer source round trips per read.** Four to two would take warm wall time at 80 ms from 320 ms to 160 ms (a 2.0x gain at low load, bigger than anything else on the table),
   and raise a 4-connection worker's ceiling from 12.5 to 25 reads/s. Candidates, from the trace: send `begin read only` with the query in one message or
   drop the explicit transaction for a read-only connection; send `statement_timeout` as a connection option instead of per read; avoid the
   separate rollback. Which of these is safe for a read-only guarantee was not examined here.
3. **Hash and verify once per result.** Two SHA-256 passes over 3 KB cost under 0.1 ms per read: not measurable here. The measured
   repeat work that matters is not in the result bytes but around them: the 14 tiny frames (about 28 syscalls and 14 allocations per read), two decode/encode passes, and
   the grant verification and job-record JSON decoding that dominate the node (about 94% of CPU is outside the worker path). Those are the ones worth fixing first.

## Reproduce

Before and after a fix, `scripts/pooled_read_profile_compare.py before=<artifact dir> after=<artifact dir>` prints CPU per read (net of idle), allocations per read,
warm p50/p95/p99, failures at 50 concurrent, verifications per read and worker memory side by side. `source turns per read` at 50 concurrent divides by all reads including failures; the executed-read value is in the text above.

Workflow `Pooled read profile` (`.github/workflows/pooled-read-profile.yml`), or `scripts/pooled_read_load.py --profile 4c8g --profile-plain
--adapter-profile-test <go test -c ./adapters/go/internal/runtime> --set warm_pool.cache_disabled=true ...`. Artifacts: `window-{idle,low,c50}.json`
(stage durations, worker memory), `window-*.cpu.pprof`, `window-*.allocs.pprof` with `window-*.base.allocs.pprof` (use `go tool pprof -sample_index=alloc_space -base`),
`window-*.heap.pprof`, `adapter-{low,c50}.cpu.pprof` / `.allocs.pprof` / `.json`, `plain-*.json`, `report.json`.
