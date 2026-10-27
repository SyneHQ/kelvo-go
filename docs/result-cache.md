# Result cache

The node keeps complete, verified Arrow IPC results of native reads served by [pooled read workers](operations/pooled-read-workers.md). Concurrent identical reads share one execution; later identical reads within the TTL replay the kept result without reaching the source.

## Key

An entry is keyed by:

* tenant, app team and principal (subject and service principal);
* effective policy fingerprint: row and column policy (none when unrestricted), catalog binding digest and the source authorization revision;
* connection, database, schema and source revision;
* mode, SQL with surrounding whitespace removed, parameters with their declared type and compact JSON value (`int32 1`, `int64 1` and `"1"` are three keys), and result limits;
* for snapshot reads, the immutable generation of every dataset read.

Two principals or two tenants never share an entry. Restricted principals (row or column policies) do not use read workers today, so they do not reach this cache.

## Fencing

Each read carries a fence: the current authorization revision for its principal and source (for snapshot reads, also the current generations). A fill records the fence when it starts.

* A fill whose fence changed before it completed (a revocation, a credential rotation, a refresh publication) is not kept, and reads waiting on it run their own execution instead of replaying it.
* A kept entry is checked against the current fence before it is served. A mismatch removes the entry and the read executes.
* When the fence is unknown (the authorization was revoked or retired), nothing is kept or served.

The leader of a fill still receives its own streamed result, as an uncached read started before the revocation would.

## Bounds

| Setting | Default | Limit |
| --- | --- | --- |
| `cache_ttl` | 10s | 10m |
| `cache_rules` | none | 64 rules, ttl 0-10m |
| `cache_max_entry_bytes` | 4 MiB | 64 MiB; larger results are not kept |
| `cache_max_total_bytes` | 64 MiB | 1 GiB; kept entries and in-flight buffers; a tenant uses at most a quarter |
| `cache_max_entries` | 4096 | 65536 |
| `cache_max_fills` | 256 | 4096; reads beyond it run uncached |

Least recently used entries are evicted first. `cache_disabled: true` turns the cache off. `X-Refresh: true` (`bypass_cache`) skips kept results and replaces the entry.

```yaml
operations:
  warm_pool:
    enabled: true
    cache_ttl: 10s
    cache_max_entry_bytes: 4194304
    cache_max_total_bytes: 67108864
    cache_max_entries: 4096
    cache_max_fills: 256
```

## Not yet delivered

Snapshot (accelerated dataset) reads are not cached. The key and fence accept dataset generations, but the snapshot read path does not consult the cache yet. The cache is in memory and ends with the node.
