# Filters resolved on the GPU node

`ann_hybrid_search` evaluates a SQL filter on the PostgreSQL host and ships every matching key to the GPU node. For a 1% filter on 10M rows, that's an index scan of 100,000 keys, about 10 ms of CPU per query. It caps a 64-core host at about 7,000 QPS, while one B200 worker can serve about 34,000 QPS. Two more routes remove that work from the PostgreSQL host:

| Function | Where the filter is resolved | Per query, PostgreSQL sends |
| --- | --- | --- |
| `ann_hybrid_search(name, query, k, filter text)` | PostgreSQL: SQL scan for every query | Varint key list (about 150 KB at 1%) |
| `ann_attribute_search(name, query, k, attribute, values int[])` | GPU node: inverted attribute columns stored with the index | The predicate (a few bytes) |
| `ann_bitmap_search(name, query, k, filter text)` | Once, ahead of time, into the `ann_filter_bitmap` table; GPU nodes cache the bitmaps | The filter's id (8 bytes) |

All three return `(id bigint, distance real)` with exact distances, and on the default routes they return exactly what SQL returns.

## Worker-side attributes

Build the index with one or more integer columns stored per row:

```sh
python3 scripts/ann/build_cuvs_index.py --dsn 'dbname=app' --table public.docs \
    --key id --vector embedding --out /data/ann/docs --attribute tenant --attribute category
```

```sql
SELECT d.* FROM ann_attribute_search('docs', $1, 10, 'category', ARRAY[$2]) s
JOIN docs d USING (id) ORDER BY s.distance;

-- IN lists of up to 64 values; NULL elements are ignored
SELECT * FROM ann_attribute_search('docs', $1, 10, 'tenant', ARRAY[3, 7, 9]);
```

- **Files.** Each `--attribute COLUMN` writes `attr-COLUMN.i32` (one little-endian int32 per row, in key order) and a manifest line `attribute COLUMN attr-COLUMN.i32`. NULL is stored as -2147483648 and never matches. Up to 8 attributes per index.
- **Load.** The worker inverts each column into sorted per-value row lists (keys and ordinals). Loading the 10M-row index with two attributes took 6.5–7 s. Values with more than `--bitset-min` rows (default 4,000,000) also get one cuVS bitset per shard, built once.
- **Search.** One value hands its row list to the existing routes with no copying: the exact CPU scan, the GPU gather scan, or filtered CAGRA with its prebuilt bitsets. Several values are merged per query.

## Bitmap table and GPU-node cache

`ann_filter_bitmap` maps a filter's text to a bitmap over the index's rows. Bit *i* is global ordinal *i*, the *i*-th smallest key; words are little-endian u32, the layout of a cuVS bitset. The table is the source of truth: GPU nodes keep each bitmap in memory under the row's `id`, and a search sends only that id.

```sql
-- once per filter (superuser): evaluate, store, and cache on every GPU node
SELECT ann_filter_bitmap_build('docs', 'category = ' || c) FROM generate_series(0, 99) c;

-- per query: one primary-key lookup of the id; the bitmap is already on the GPU node
SELECT * FROM ann_bitmap_search('docs', $1, 10, 'category = 7');

-- after GPU nodes restart (ann_index_load does this itself)
SELECT ann_filter_bitmap_push('docs');
```

| Column | Content |
| --- | --- |
| `id` | Identity; the key GPU nodes cache the bitmap under. Rebuilding a filter keeps its id. |
| `index_name`, `filter` | Primary key. A search must use the filter's exact text. |
| `eligible` | Rows in the bitmap |
| `bitmap` | `bytea`, `(rows + 31) / 32 * 4` bytes, stored uncompressed (`STORAGE EXTERNAL`) |
| `built_at` | Build time; rebuilding a filter replaces its row |

**Build.** `ann_filter_bitmap_build` runs the filter on the index's source table and sends the matching keys to a GPU node, which maps them to ordinals and returns the bitmap. PostgreSQL stores it, then pushes it to every node in `ann.worker_address` (or to the local worker). On 10M rows, building and pushing took about 7 ms per 0.1% filter, 18 ms per 1% filter and 0.8 s per 10% filter, mostly the SQL scan. Keys missing from the index are dropped. A node that can't be reached gets a warning, not an error: the row is stored and reaches that node later.

**When GPU nodes start.** A restarted worker has no bitmaps. `ann_index_load` pushes every stored bitmap of the index to every node right after loading it. 1,110 bitmaps (1.4 GB) took 4.5 s. `ann_filter_bitmap_push(name)` does the same on demand. If a node still lacks a bitmap, for example because another host loaded the index, it answers the search with a "missing" status. PostgreSQL then pushes that bitmap from the table to every node and retries once. Measured: 59 ms for the first search after a restart, 2.3 ms for the next.

**On the GPU node.** Each cached bitmap holds the bitmap, one cuVS bitset per shard (pointing into the bitmap when the shard starts on a 32-row boundary, otherwise a shifted copy), and, for filters within `--gather-limit` rows (all filters on indexes without CAGRA shards), its ordinals and keys. Searches take a reference, so replacing a bitmap never frees one in use. The 1,110 benchmark bitmaps took 1.87 GB of host memory. `bitmaps N bitmap_bytes B` in the index-info reply (operation 15) shows the cache.

**Validity.** Indexes are immutable, so a bitmap stays valid for the index it was built for. Bitmaps belong to an index name: after loading different files under a name, rebuild that name's bitmaps. A node refuses a bitmap whose size doesn't match the loaded index, and a search with a filter that has no row is an error.

On the worker, filters within `--gather-limit` rows take the exact routes. Broader ones go straight to filtered CAGRA with their per-shard bitsets.

## Filtered CAGRA settings

cuVS's default single-CTA search loses most of its recall when a bitset masks out nearly all of the graph. Multi-CTA search with a wider search fixes that, and was faster too. On 10M × 128 with k = 10, measured over 50 queries per filter against exact SQL (serial latency end to end through PostgreSQL):

| Filtered CAGRA setting | 0.1% recall | 1% recall | 10% recall | Latency |
| --- | --- | --- | --- | --- |
| single-CTA, itopk 512 (the previous behaviour) | 0.094 | 0.666 | 0.938 | 4.7–4.9 ms |
| multi-CTA, itopk 512, search width 4 (**new default**) | 0.986 | 1.000 | 1.000 | 2.2–2.5 ms |
| multi-CTA, itopk 1024–4096, width 8–32 | 0.992 | 1.000 | 1.000 | 2.2–3.0 ms |

| Worker option | Effect |
| --- | --- |
| `--filtered-algo auto\|single\|multi` | CAGRA algorithm for bitset-filtered searches (default `multi`; `auto` is cuVS's own choice). Unfiltered searches always use `auto`. |
| `--filtered-width N` | Search width for bitset-filtered searches (default 4). |
| `--filtered-iterations N` | Maximum iterations for them (default 0, automatic). |
| `--filtered-itopk N` | Internal top-k for selective filters; up to 512 with single-CTA, 4096 with multi-CTA. |
| `--bitset-min ROWS` | Attribute values with more rows get prebuilt per-shard bitsets (default 4,000,000). |

## Throughput

10M × 128 rows, 1-shard FP32 index. PostgreSQL host A had 64 cores, with pgbench on the same cores. The GPU node was one worker on one B200 with 28 cores. Best of 128 and 512 clients, in QPS:

| Filter (eligible rows) | SQL keys (`ann_hybrid_search`) | Attribute | Bitmap sent with each query | Cached bitmap | Cached bitmap, `--gather-limit 500000` |
| --- | --- | --- | --- | --- | --- |
| 0.1% (10k) | 31,767 | 68,036 | 13,082 | **72,169** | 70,968 |
| 1% (100k) | 6,913 | 34,448 | 10,652 | **35,507** | 35,437 |
| 10% (1M) | 74 | 561 (IN list of ten values) | 7,494 (filtered CAGRA) | 5,038 (gather scan) | **8,796** (filtered CAGRA) |

The "bitmap sent with each query" column is the earlier version of the bitmap table, which read the 1.25 MB bitmap and sent it over TCP for every query. With the cache, only the id travels.

- **Cached bitmaps make the GPU node the limit.** The PostgreSQL host does one index lookup per query. 1% filters ran 5.1× faster than with SQL keys, at the worker's gather-scan capacity (about 35k QPS per B200), and 0.1% filters 2.3× faster. The attribute route reaches the same rates for equality filters on stored columns, but its IN lists are merged per query (1M ordinals sorted on the CPU at 10%).
- **The gather scan beats filtered CAGRA up to a few percent.** Filtered CAGRA runs one query per cuVS call (a bitset is shared by a whole batch), so its cost hardly depends on the filter: about 7.5–8.8k QPS. The gather scan's cost grows with the filter: 35k at 1%, 5k at 10%. The default `--gather-limit` of 4,000,000 rows sends 10% filters to the gather scan. `--gather-limit 500000` sent them to filtered CAGRA, at 8.8k QPS (1.75×), with no loss at 0.1% or 1%. Use it when broad filters are common.

**Which to use.** Use the bitmap table for filters that recur, whatever their SQL: tenants, categories, ACL groups, saved searches. Use attributes when the values are integer columns known at build time and you'd rather not build a bitmap per value. Use `ann_hybrid_search` for ad-hoc filters, and for selective ones (it scores filters under `ann.gpu_min_eligible` rows on the PostgreSQL host).

**Not measured yet:** several PostgreSQL hosts against the cached routes (one host left the worker as the limit), multi-GPU and 8-shard indexes, and bitsets kept in GPU memory (cached bitmaps live in host memory, and filtered CAGRA copies each shard's bitset to the GPU per query).

## Protocol additions

| Operation | Payload | Reply |
| --- | --- | --- |
| 17 attribute search | u32 name length, name, u32 dimensions, k, attribute name length, attribute name, value count; i32 values; FP32 query | As operation 12 |
| 18 build bitmap | u32 name length, name, u32 key count, u32 packed bytes, varint keys (as operation 14) | u64 eligible rows, then the bitmap as u32 words |
| 19 bitmap search | u32 name length, name, u32 dimensions, k, bitmap words; FP32 query; the bitmap | As operation 12 |
| 20 cache bitmap | u32 name length, name, u64 filter id, u32 words; the bitmap | Empty; replaces a cached bitmap with the same id |
| 21 cached bitmap search | u32 name length, name, u32 dimensions, k, u64 filter id; FP32 query | As operation 12, or status 11 (missing) when the id isn't cached |

Operations 17, 19 and 21 also travel over RDMA when they fit a frame. Pushes (20) use a fresh TCP connection to each node, as loads do, and continue past unreachable nodes.

## Tests

- `test/ann/test_index.py` (CPU worker, plus SQL with `ANN_PG_DSN`): attribute search equals exact for single values, IN lists, extreme int32 values, and unknown values, across 7 shards that don't start on 32-row boundaries. Built bitmaps hold exactly the eligible ordinals, and bitmap search equals exact. Cached bitmaps answer "missing" until pushed, then equal exact; a push replaces the entry with the same id; malformed requests are rejected. In SQL, `ann_attribute_search` and `ann_filter_bitmap_build` + `ann_bitmap_search` equal exact SQL, including a filter matching nothing. A second worker loaded without PostgreSQL gets bitmaps pushed on demand by searches, then all of them from `ann_filter_bitmap_push`.
- `test/ann/test_cuvs_index.py` (cuVS): an index built with `--attribute`. Attribute and bitmap search equal SQL on the gather route. On filtered CAGRA (a second worker, which gets its bitmaps by on-demand pushes), with prebuilt and sliced bitsets, recall is at least 0.9 and no row falls outside the filter.
