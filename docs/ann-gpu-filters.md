# Filters resolved on the GPU node

`ann_hybrid_search` evaluates a SQL filter on the PostgreSQL host and ships every matching key to the GPU node. For a 1% filter on 10M rows, that's an index scan of 100,000 keys, about 10 ms of CPU per query. It caps a 64-core host at about 7,000 QPS, while one B200 worker can serve about 34,000 QPS. Two more routes remove that work from the PostgreSQL host:

| Function | Where the filter is resolved | Per query, PostgreSQL sends |
| --- | --- | --- |
| `ann_hybrid_search(name, query, k, filter text)` | PostgreSQL: SQL scan for every query | Varint key list (about 150 KB at 1%) |
| `ann_attribute_search(name, query, k, attribute, values int[])` | GPU node: inverted attribute columns stored with the index | The predicate (a few bytes) |
| `ann_bitmap_search(name, query, k, filter text)` | Once, ahead of time, into the `ann_filter_bitmap` table | The filter's bitmap (rows / 8 bytes, 1.25 MB for 10M rows) |

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

## Bitmap table

`ann_filter_bitmap` maps a filter's text to a bitmap over the index's rows. Bit *i* is global ordinal *i*, the *i*-th smallest key; words are little-endian u32, the layout of a cuVS bitset.

```sql
-- once per filter (superuser): evaluates the filter, stores eligible rows and the bitmap
SELECT ann_filter_bitmap_build('docs', 'category = ' || c) FROM generate_series(0, 99) c;

-- per query: one primary-key lookup of the bitmap, sent with the query
SELECT * FROM ann_bitmap_search('docs', $1, 10, 'category = 7');
```

| Column | Content |
| --- | --- |
| `index_name`, `filter` | Primary key. A search must use the filter's exact text. |
| `eligible` | Rows in the bitmap |
| `bitmap` | `bytea`, `(rows + 31) / 32 * 4` bytes, stored uncompressed (`STORAGE EXTERNAL`) |
| `built_at` | Build time; rebuilding a filter replaces its row |

`ann_filter_bitmap_build` runs the filter on the index's source table and sends the matching keys to the GPU node. The node maps them to ordinals and returns the bitmap. On 10M rows it took about 6 ms per 0.1% filter, 15 ms per 1% filter and 0.7 s per 10% filter, mostly the SQL scan. Keys missing from the index are dropped. Indexes are immutable, so a bitmap stays valid for the index it was built for; after loading a new index under a new name, build its bitmaps. A search with a filter that has no row is an error, and so is a bitmap whose size doesn't match the loaded index.

On the worker, filters within `--gather-limit` rows, and every filter on an index without CAGRA shards, are expanded to ordinals and keys and take the exact routes. Broader ones go straight to filtered CAGRA, with each shard's slice of the bitmap used as its cuVS bitset (in place when the shard starts on a 32-row boundary, otherwise shifted into a copy).

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

10M × 128 rows, 1-shard FP32 index. PostgreSQL host A had 64 cores (pgbench on the same cores, 128 clients). The GPU node was one worker on one B200 with 28 cores. Best of 128 and 512 clients, in QPS:

| Filter (eligible rows) | SQL keys (`ann_hybrid_search`) | Attribute, exact routes | Bitmap table, exact routes | Attribute, filtered CAGRA | Bitmap table, filtered CAGRA |
| --- | --- | --- | --- | --- | --- |
| 0.1% (10k) | 31,767 | **68,036** | 13,082 | 3,483 | 3,478 |
| 1% (100k) | 6,913 | **34,448** | 10,652 | 7,444 | 7,332 |
| 10% (1M; ten values) | 74 | 561 | 1,690 | 553 | **7,494** |

- **The attribute route makes the GPU node the limit.** The PostgreSQL host does almost nothing per query: 1% filters ran 5× faster than with SQL keys, at the worker's own gather-scan capacity (about 34k QPS per B200), and 0.1% filters ran 2.1× faster. At 10%, its ten-value IN list is merged per query (1M ordinals sorted on the CPU), which is what limits it; one stored value per filter avoids that.
- **The bitmap table pays a fixed transfer.** Each bitmap is 1.25 MB whatever the filter's size. It's read from the table, copied and sent over TCP (too large for an RDMA frame) for every query. That beats key lists for broad filters (10%: 1,690 → 7,494 QPS with filtered CAGRA, about 100× the SQL route) and loses for selective ones (0.1%: 13k vs 32k).
- **The gather scan beats filtered CAGRA up to about 1%.** Filtered CAGRA runs one query per cuVS call, since a bitset is shared by a whole batch, so it doesn't batch. Its cost hardly depends on the filter, while the gather scan's grows with the filter. Between 1% (gather 34k vs CAGRA 7.4k) and 10% (gather about 1.7k vs CAGRA 7.5k) the crossover is around 5%, well below the default `--gather-limit` of 4,000,000 rows (40%). Lower it, for example to 500,000, when broad filters are common.

**Which to use.** Use attributes for equality and IN filters on integer columns known when the index is built: tenants, categories, languages, ACL groups. Use the bitmap table for arbitrary SQL filters that recur and are broad. Use `ann_hybrid_search` for ad-hoc filters, and for selective ones (it scores filters under `ann.gpu_min_eligible` rows on the PostgreSQL host).

**Not measured yet:** several PostgreSQL hosts against the attribute route (one host left the worker as the limit), multi-GPU and 8-shard indexes, and a worker-side bitmap cache, which would let the bitmap route send a filter id instead of 1.25 MB.

## Protocol additions

| Operation | Payload | Reply |
| --- | --- | --- |
| 17 attribute search | u32 name length, name, u32 dimensions, k, attribute name length, attribute name, value count; i32 values; FP32 query | As operation 12 |
| 18 build bitmap | u32 name length, name, u32 key count, u32 packed bytes, varint keys (as operation 14) | u64 eligible rows, then the bitmap as u32 words |
| 19 bitmap search | u32 name length, name, u32 dimensions, k, bitmap words; FP32 query; the bitmap | As operation 12 |

Operations 17 and 19 also travel over RDMA when they fit a frame. A 10M-row bitmap doesn't, so it goes over TCP.

## Tests

- `test/ann/test_index.py` (CPU worker, plus SQL with `ANN_PG_DSN`): attribute search equals exact for single values, IN lists, extreme int32 values, and unknown values, across 7 shards that don't start on 32-row boundaries. Built bitmaps hold exactly the eligible ordinals, and bitmap search equals exact. Malformed requests are rejected. In SQL, `ann_attribute_search` and `ann_filter_bitmap_build` + `ann_bitmap_search` equal exact SQL, including a filter matching nothing.
- `test/ann/test_cuvs_index.py` (cuVS): an index built with `--attribute`. Attribute and bitmap search equal SQL on the gather route. On filtered CAGRA, with prebuilt and sliced bitsets, recall is at least 0.9 and no row falls outside the filter.
