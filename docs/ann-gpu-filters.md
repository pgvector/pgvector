# Filters resolved on the GPU node

`ann_hybrid_search` evaluates a SQL filter on the PostgreSQL host and ships every matching key to the GPU node. For a 1% filter on 10M rows, that's an index scan of 100,000 keys, about 10 ms of CPU per query. It caps a 64-core host at about 7,000 QPS, while one B200 worker can serve about 34,000 QPS. Two more routes remove that work from the PostgreSQL host:

| Function | Where the filter is resolved | Per query, PostgreSQL sends |
| --- | --- | --- |
| `ann_hybrid_search(name, query, k, filter text)` | PostgreSQL: SQL scan for every query | Varint key list (about 150 KB at 1%) |
| `ann_attribute_search(name, query, k, attribute, values int[])` | GPU node: inverted attribute columns stored with the index | The predicate (a few bytes) |
| `ann_bitmap_search(name, query, k, filter text)` | Once, ahead of time, into the `ann_filter_bitmap` table; GPU nodes cache the bitmaps | The filter's id (8 bytes) |

All three return `(id bigint, distance real)` with exact distances, and on the default routes they return exactly what SQL returns.

## Summary

Resolving filters on the GPU node takes the per-query filter work off the PostgreSQL host. On one machine with 8× B200, filtered search went from about 7k QPS (1% filters) and 74 QPS (10%) to 140k and 108k QPS. Results still match exact SQL.

**Changes**

| Change | What it does |
| --- | --- |
| Attribute columns (`--attribute`, `ann_attribute_search`) | The index stores integer columns. PostgreSQL sends only the condition, and the GPU node looks up the matching rows. |
| Bitmap table (`ann_filter_bitmap`, `ann_filter_bitmap_build`, `ann_bitmap_search`) | A filter is evaluated once into a bitmap over the index's rows. |
| Bitmap cache on GPU nodes | Bitmaps are pushed to every GPU node at build time and when an index loads (`ann_filter_bitmap_push` on demand). A search sends an 8-byte filter id; a node missing a bitmap says so, and PostgreSQL pushes it and retries once. |
| Filter ids shared across hosts | An id is a hash of the index name and filter text, so PostgreSQL hosts sharing GPU nodes share one cached copy. |
| Multi-CTA filtered CAGRA (`--filtered-algo`, `--filtered-width`) | Recall with a bitset filter rose from 0.09–0.94 to 0.99–1.0, at half the latency. |
| Bitsets kept in GPU memory (`--device-bitmaps`) | Cached bitmaps aren't copied to the GPU per query. |
| Memory pool per GPU (`--memory-pool`) | cuVS's per-search temporaries no longer go through the CUDA driver's allocator, which serialized them across threads and GPUs. |
| Route split (`--bitset-gather-limit`) | Filters with a ready bitset switch from the exact GPU scan to filtered CAGRA above 500k rows; key lists keep `--gather-limit` (4M). |
| Overload retry (`ann.worker_overload_wait_ms`) | PostgreSQL backs off and retries when the worker's admission budget is full, instead of failing the query. |
| Gather-scan guard | A shard whose top-k selection repeats a candidate is scored again exactly on the CPU. |
| Same-filter grouping (`--filter-lanes`, `--active-lanes`, `--group-gather-limit`) | Queued searches over one cached filter are scanned together: on quantized indexes with a tensor-core kernel (exact), otherwise with one batched filtered CAGRA search. The scheduler forms the groups and lanes grow buffers only rarely, so warm-up stalls are gone. |

**Performance** (10M × 128, k = 10, QPS; PostgreSQL hosts of 64 cores; everything on one machine over loopback TCP):

| Workload | `ann_hybrid_search`, one host | Cached bitmap, 1 GPU (NVFP4, grouped) | Cached bitmap, 8 GPUs (index replicated), two hosts |
| --- | --- | --- | --- |
| 0.1% filter | 31.8k | 143k | **257k** |
| 1% filter | 6.9k | 284k | **313k** |
| 1% filter, 10 hot filters | — | 340k | **328k** |
| 10% filter | 74 | 82k | **321k** |
| Unfiltered ANN | 158k–318k | 366k | **369k** |

| Change | Measured gain |
| --- | --- |
| Memory pool per GPU | 10% filter on 8 GPUs: 6.6k → 102k QPS; unfiltered ANN: 158k → 370k |
| Sending a filter id instead of the bitmap | 1% filter on 1 GPU: 10.7k → 35k |
| Bitsets in GPU memory | 10% filter on 1 GPU: 8.5k → 15.3k |

- **Hosts per GPU node:** one 64-core PostgreSQL host gets 90–98% of an 8-GPU node's capacity on the cached-bitmap route. The SQL-filter route would need about 20 hosts to fill it.
- **Index layout:** with the whole index on every GPU (`--replicate on`), filtered search is 4–8× faster than with 8 shards.
- **The ceiling is the GPUs:** they were busy 90–100% of the time. More GPU-node cores or PostgreSQL cores barely changed the totals.
- **Not measured yet:** GPU nodes and PostgreSQL hosts on separate machines over a real network.

The sections below give the details.

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

`ann_filter_bitmap` maps a filter's text to a bitmap over the index's rows. Bit *i* is global ordinal *i*, the *i*-th smallest key; words are little-endian u32, the layout of a cuVS bitset. The table is the source of truth: GPU nodes keep each bitmap in memory under the row's `id`, and a search sends only that id. The id is a 64-bit hash of the index name and the filter's text, so every PostgreSQL host computes the same id for the same filter. Hosts sharing GPU nodes share one cached copy instead of overwriting each other's.

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
| `id` | Hash of the index name and filter text; the key GPU nodes cache the bitmap under. Rebuilding a filter, on any host, gives the same id. |
| `index_name`, `filter` | Primary key. A search must use the filter's exact text. |
| `eligible` | Rows in the bitmap |
| `bitmap` | `bytea`, `(rows + 31) / 32 * 4` bytes, stored uncompressed (`STORAGE EXTERNAL`) |
| `built_at` | Build time; rebuilding a filter replaces its row |

**Build.** `ann_filter_bitmap_build` runs the filter on the index's source table and sends the matching keys to a GPU node, which maps them to ordinals and returns the bitmap. PostgreSQL stores it, then pushes it to every node in `ann.worker_address` (or to the local worker). On 10M rows, building and pushing took about 7 ms per 0.1% filter, 18 ms per 1% filter and 0.8 s per 10% filter, mostly the SQL scan. Keys missing from the index are dropped. A node that can't be reached gets a warning, not an error: the row is stored and reaches that node later.

**When GPU nodes start.** A restarted worker has no bitmaps. `ann_index_load` pushes every stored bitmap of the index to every node right after loading it. 1,110 bitmaps (1.4 GB) took 4.5 s. `ann_filter_bitmap_push(name)` does the same on demand. If a node still lacks a bitmap, for example because another host loaded the index, it answers the search with a "missing" status. PostgreSQL then pushes that bitmap from the table to every node and retries once. Measured: 59 ms for the first search after a restart, 2.3 ms for the next.

**On the GPU node.** Each cached bitmap holds the bitmap, one cuVS bitset per shard (pointing into the bitmap when the shard starts on a 32-row boundary, otherwise a shifted copy), and, for filters within `--bitset-gather-limit` rows (all filters on indexes without CAGRA shards), its ordinals and keys. The cuVS worker also copies each shard's bitset into the memory of every GPU holding that shard (`--device-bitmaps`, default on), so filtered CAGRA reads it in place. That raised 10% filters on one GPU from 8.5k to 15.3k QPS, and costs rows / 8 bytes per bitmap per GPU (1.4 GB per GPU for the 1,110 benchmark bitmaps with the index replicated). Searches take a reference, so replacing a bitmap never frees one in use. The 1,110 benchmark bitmaps took 1.87 GB of host memory. `bitmaps N bitmap_bytes B` in the index-info reply (operation 15) shows the cache.

**Validity.** Indexes are immutable, so a bitmap stays valid for the index it was built for. Bitmaps belong to an index name: after loading different files under a name, rebuild that name's bitmaps. A node refuses a bitmap whose size doesn't match the loaded index, and a search with a filter that has no row is an error.

On the worker, filters within `--bitset-gather-limit` rows (default 500,000) take the exact routes. Broader ones go straight to filtered CAGRA with their per-shard bitsets.

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
| `--device-bitmaps on\|off` | Keep cached bitmaps' bitsets in GPU memory (default on). |
| `--memory-pool PERCENT` | RMM pool per GPU for cuVS allocations, up to this share of memory (default 80; `0` for `cudaMallocAsync`). See below. |

## Throughput

10M × 128 rows, 1-shard FP32 index. PostgreSQL host A had 64 cores, with pgbench on the same cores. The GPU node was one worker on one B200 with 28 cores. These runs came before GPU-resident bitsets and the RMM pool; the next section has the current numbers. Best of 128 and 512 clients, in QPS:

| Filter (eligible rows) | SQL keys (`ann_hybrid_search`) | Attribute | Bitmap sent with each query | Cached bitmap | Cached bitmap, `--gather-limit 500000` |
| --- | --- | --- | --- | --- | --- |
| 0.1% (10k) | 31,767 | 68,036 | 13,082 | **72,169** | 70,968 |
| 1% (100k) | 6,913 | 34,448 | 10,652 | **35,507** | 35,437 |
| 10% (1M) | 74 | 561 (IN list of ten values) | 7,494 (filtered CAGRA) | 5,038 (gather scan) | **8,796** (filtered CAGRA) |

The "bitmap sent with each query" column is the earlier version of the bitmap table, which read the 1.25 MB bitmap and sent it over TCP for every query. With the cache, only the id travels.

- **Cached bitmaps make the GPU node the limit.** The PostgreSQL host does one index lookup per query. 1% filters ran 5.1× faster than with SQL keys, at the worker's gather-scan capacity (about 35k QPS per B200), and 0.1% filters 2.3× faster. The attribute route reaches the same rates for equality filters on stored columns, but its IN lists are merged per query (1M ordinals sorted on the CPU at 10%).
- **The gather scan beats filtered CAGRA up to a few percent.** Filtered CAGRA runs one query per cuVS call (a bitset is shared by a whole batch), so its cost hardly depends on the filter: about 7.5–8.8k QPS. The gather scan's cost grows with the filter: 35k at 1%, 5k at 10%. With a single limit of 4,000,000 rows, 10% filters went to the gather scan; at 500,000 they went to filtered CAGRA, at 8.8k QPS (1.75×), with no loss at 0.1% or 1%.

  That holds only when the bitset is ready. Key-list filters (`ann_index_search` with keys, `ann_hybrid_search`) need their bitset built on the CPU for every query, and the gather scan stays ahead at 10%. Measured straight against the worker with 1M-key requests from 32 client processes, same results:

  | Key-list filter, QPS | 1 GPU, `--gather-limit` 4M | 1 GPU, 500k | 8 GPUs replicated, 4M | 8 GPUs replicated, 500k |
  | --- | --- | --- | --- | --- |
  | 1% (100k keys) | 33,049 | 32,298 | 50,723 | 50,552 |
  | 10% (1M keys) | **1,715** | 1,420 | **3,005** | 1,908 |

  So the limit is split by route. `--bitset-gather-limit` (default 500,000) applies to filters that arrive with a ready bitset: cached bitmaps, sent bitmaps, and attribute values with prebuilt bitsets. `--gather-limit` (default 4,000,000) applies to key lists.

**Which to use.** Use the bitmap table for filters that recur, whatever their SQL: tenants, categories, ACL groups, saved searches. Use attributes when the values are integer columns known at build time and you'd rather not build a bitmap per value. Use `ann_hybrid_search` for ad-hoc filters, and for selective ones (it scores filters under `ann.gpu_min_eligible` rows on the PostgreSQL host).

## Same-filter grouping

Broad filters mean few distinct filters, so at high QPS many queued queries share the same cached bitmap or attribute value. The worker scans such a group's rows once for all of them instead of once per query.

**Scheduler.** A lane that picks up a search over a cached filter takes the filter's other queued searches with it (the queue keeps a list per filter, so this costs nothing per queued job). Once `--active-lanes` lanes (default three quarters of the lanes) are working on cached filters, a filter gets at most `--filter-lanes` lanes per GPU copy of the index (default 2): further searches over it wait in the queue and form the next group, so group size follows the load, with no delay added when the queue is short. Below that, lanes start on any filter, so a few hot filters still keep the GPUs busy. Measured on one GPU with 28 lanes at 10%: 3 hot filters ran at 22k, 27k, 34k and 50k QPS with 1, 2, 4 and 8 lanes per filter, while 10 filters ran at 30k, 58k, 40k and 47k; the total-active rule gives each case its better setting. Searches that cannot share a scan (unfiltered, key lists, per-query bitmaps) are batched as before. The stats report `groups`, `grouped_jobs` and `held` (pickups that waited for a filter's lanes). Lane count no longer sets batch size: before this scheduler, 28 lanes gave batches of ~10 jobs and groups of ~3, while 4 lanes gave batches of 105 and groups of 12; now 28 lanes form groups of 5–20 under load.

**Backend.** In one batch, the cuVS backend groups filtered jobs by filter identity and routes the group once:
- Up to `--group-gather-limit` rows (default 4,000,000) on a Blackwell GPU: the module scores the filter's rows for up to 16 queries at once on the tensor cores, from the quantized copy when the index has one and otherwise from the FP16 or FP32 CAGRA dataset in place, and selects each query's best 32 with a batched top-k ([details](ann-blackwell.md#in-the-worker)). Groups larger than 16 go in turns. Candidates are re-scored in FP32 as always, so results stay exact. GPU time per query: 3.1 µs at 100k rows and 16 µs at 1M rows in groups of 16, against 41 and 156 µs alone.
- Broader filters with prebuilt bitsets, and any group on a GPU without the module: one filtered CAGRA search of the whole group. CAGRA batches queries sharing a bitset, which single queries could not use. Measured exact on 352k 10% answers at groups of ~20.
- Everything else (filters without bitsets): one search per job as before.

Cached bitmaps also keep each shard's eligible rows in GPU memory (4 bytes a row per copy), so no gather scan uploads rows per query.

**One GPU** (10M × 128, 512 clients from one 64-core host, default settings, a 4-second warm-up before each measurement, QPS):

| Workload | Distinct filters | FP32 index | NVFP4 index | Before grouping (FP32 / NVFP4) |
| --- | --- | --- | --- | --- |
| 10% filters | 10 | 47,112 | **82,058** | 16,054 / 15,941 |
| 10% filters, skewed | 3 | 52,727 | **80,998** | — |
| 1% filters | 100 | 54,115 | **284,012** | 35,782 / 51,438 |
| 1% filters, skewed | 10 | 55,755 | **339,541** | — |
| 0.1% filters | 1,000 | 130,027 | 143,133 | 76,894 / 107,788 |
| Unfiltered ANN | — | 358,846 | 365,738 | unchanged |

On the NVFP4 index every grouped filter takes the tensor-core scan: 10% filters run 5.1× faster than before grouping and 1.7× faster than batched CAGRA, exactly; 1% filters 5.5× faster, and skewed 1% traffic at 340k QPS, where one GPU answers a query in 3 µs of GPU time. The FP32 index's figures above are from before its grouped searches could use the tensor cores; with the kernel reading the FP32 dataset in place it reaches 62,030 QPS at 10%, 248,030 at 1%, 274,935 at skewed 1% and 155,393 at 0.1% (exact in 2.75M concurrent checks). Its 10% filters stay behind the NVFP4 index's 82k because the scan reads 512 bytes a row instead of 64. 0.1% filters (1,000 filters, so groups are rare and average 2.3) ran at 165–168k QPS in three back-to-back runs on the NVFP4 index, the same with the per-filter lane cap lifted (`--filter-lanes 28`), and 149k with 256 clients; single readings of 143k and 182k in other sessions were run-order and host-side effects, not the scheduler.

**Exactness.** Concurrent verification runs (512 clients, every answer compared with exact SQL) over the final kernels on the NVFP4 index: 22,031,827 grouped 1% answers, 7,348,545 10% answers and 23,227,456 0.1% answers through the tensor-core scan, all exact (52.6M in all), plus 2.75M on the FP32 index's dataset scan. An earlier multi-query kernel (one thread per row, FP32 FMAs, per-warp lists) had lost one neighbour in 1,427,107 answers: that row ranked 11th by quantized distance, so it should have been among the 32 candidates, and the kernel itself returned identical candidates over 32,000 repeated launches under contention. It was replaced before the cause was found; the replacement has shown no miss in 55M answers.

**8 GPUs, index replicated, two 64-core PostgreSQL hosts** (sum of both hosts, 512 clients each, QPS):

| Workload | FP32 index | NVFP4 index | Before grouping |
| --- | --- | --- | --- |
| 10% filters | 287,494 | **320,528** | 107,875 |
| 10% filters, skewed (3) | 308,456 | **327,385** | — |
| 1% filters | 290,808 | 312,812 | 142,191 |
| 1% filters, skewed (10) | 311,606 | 327,887 | — |
| 0.1% filters | 256,579 | 256,704 | 255,008 |
| Unfiltered ANN | 371,564 | 369,333 | 373,948 |

Every filtered workload now lands between 257k and 330k QPS on the 8-GPU node, and 1% filters on one GPU alone reach 284k, so the node's limit on these routes has moved from the GPUs to the two PostgreSQL hosts and the worker's 96 cores. More PostgreSQL hosts per GPU node would raise it; an earlier measurement found the GPUs the limit at 140k.

**Warm-up.** Lane buffers used to grow stepwise under load, and each `cudaFree`/`cudaFreeHost` synchronizes the whole device: an nsys profile showed 99 ms per `cudaFree` on average and 1.7 s at worst, 70 s of blocking across 28 lanes in a 20-second run, and the first run after a worker start could stay at a tenth of its throughput for its whole 12 seconds. Buffers now grow to twice the need with floors (1 MB of queries, 64k results, 256k gather rows), device buffers through stream-ordered `cudaMallocAsync`/`cudaFreeAsync`, so growth is rare and never stalls other lanes.


## Scaling across GPUs and hosts

The same 10M-row data, now with cached bitsets in GPU memory and filtered CAGRA above 500,000 rows (now the default for cached bitmaps). Host A and host B are two 64-core PostgreSQL clusters, with pgbench on the same cores, 512 clients each. GPU-node layouts:
- **1 GPU:** one worker on one B200 with 28 cores.
- **8 GPUs, replicated:** the 1-shard index on every GPU (`--replicate on`), with 96 cores and 96 lanes.
- **8 GPUs, 8 shards:** one shard per GPU, same cores and lanes.

Results in QPS:

| Workload | 1 GPU: A / A+B | 8 GPUs, replicated: A / A+B | 8 GPUs, 8 shards: A / A+B |
| --- | --- | --- | --- |
| `ann_bitmap_search`, 0.1% | 75,174 / 73,081 | 233,167 / **257,759** | 57,451 / 58,257 |
| `ann_bitmap_search`, 1% | 35,307 / 34,904 | 127,501 / **140,081** | 33,172 / 31,942 |
| `ann_bitmap_search`, 10% | 16,038 / 16,088 | 105,841 / **107,813** | 13,817 / 13,900 |
| `ann_hybrid_search`, 1% | 6,619 / 10,771 | 6,790 / 10,625 | 6,663 / 10,294 |
| Unfiltered ANN | 354,977 / 391,558 | 351,122 / **373,948** | 113,233 / 218,091 |

- **Replicate the index on 8 GPUs.** With every GPU holding the whole index, a query touches one GPU. With 8 shards, it touches all eight and the results are merged, which costs 4–8× in filtered throughput. Use shards only when the index doesn't fit one GPU.
- **One PostgreSQL host nearly fills an 8-GPU node on the cached routes.** Host A alone reached 90–98% of what two hosts got. The limit is the GPU node's 96 worker cores: about 260k QPS at 0.1%, 140k at 1% and 108k at 10%. On one GPU (28 cores), a second host added nothing.
- **The SQL key-list route scales only with PostgreSQL hosts**, at about 5–7k QPS per 64-core host at 1%, whatever the GPU node. Filling the 8-GPU node at 1% that way would take about 20 hosts, against one or two with cached bitmaps.

**The CUDA allocator was the multi-GPU bottleneck.** Before the RMM pool, 10% filters on 8 GPUs ran *slower* than on one GPU: 4.8–6.6k QPS. They ran faster with 24 lanes (17.9k) than with 96. Thread-stack samples of the busy worker showed the lanes waiting on locks inside `cuMemAllocFromPoolAsync` and `cuMemFreeAsync`. cuVS allocates and frees temporaries on every filtered search through the process-wide memory resource (stream-ordered allocation then), and the driver serializes those calls across all threads and GPUs. An RMM pool per GPU (`--memory-pool`, now the default) serves them without the driver:

| 8 GPUs, replicated, host A | `cudaMallocAsync` (`--memory-pool 0`) | RMM pool (`--memory-pool 40`) |
| --- | --- | --- |
| `ann_bitmap_search`, 10%, 256 clients | 6,607 | 102,492 |
| Unfiltered ANN, 256 clients | 41,586 | 290,222 |
| Unfiltered ANN, 1,024 clients | 157,779 | 369,882 |

Results stayed identical to exact SQL (50 queries per filter size, all routes). The pool also holds the index data, so its cap must leave room for the index plus cuVS temporaries. 80% of a B200 is 144 GB.

**What limits the 8-GPU node.** Moving the worker from 96 to 128 cores changed totals by only −2% to +7% (two hosts, replicated index: 0.1% 262k → 256k, 1% 140k → 144k, 10% 108k → 115k, ANN 374k → 369k). Giving the two PostgreSQL hosts 48 cores each instead of 64 left the totals unchanged too. During these runs the GPUs were busy 90–94% of the time at 1% and 100% at 10% (`nvidia-smi` utilization: the share of time a kernel runs). For filtered search, the eight B200s themselves are the ceiling: about 140k QPS of 1% filters and 110k of 10% filters on this data. More PostgreSQL hosts per node would add nothing on the cached routes; more GPU nodes would.

**Not measured yet:** GPU nodes and PostgreSQL hosts on separate machines over a real network. Every number here comes from one machine split into a worker and two PostgreSQL clusters over loopback TCP.

## Protocol additions

| Operation | Payload | Reply |
| --- | --- | --- |
| 17 attribute search | u32 name length, name, u32 dimensions, k, attribute name length, attribute name, value count; i32 values; FP32 query | As operation 12 |
| 18 build bitmap | u32 name length, name, u32 key count, u32 packed bytes, varint keys (as operation 14) | u64 eligible rows, then the bitmap as u32 words |
| 19 bitmap search | u32 name length, name, u32 dimensions, k, bitmap words; FP32 query; the bitmap | As operation 12 |
| 20 cache bitmap | u32 name length, name, u64 filter id, u32 words; the bitmap | Empty; replaces a cached bitmap with the same id |
| 21 cached bitmap search | u32 name length, name, u32 dimensions, k, u64 filter id; FP32 query | As operation 12, or status 11 (missing) when the id isn't cached |

Operation 15 (index info) now ends with `bitmaps N bitmap_bytes B`.

Operations 17, 19 and 21 also travel over RDMA when they fit a frame. Pushes (20) use a fresh TCP connection to each node, as loads do, and continue past unreachable nodes.

## Tests

- `test/ann/test_index.py` (CPU worker, plus SQL with `ANN_PG_DSN`): attribute search equals exact for single values, IN lists, extreme int32 values, and unknown values, across 7 shards that don't start on 32-row boundaries. Built bitmaps hold exactly the eligible ordinals, and bitmap search equals exact. Cached bitmaps answer "missing" until pushed, then equal exact; a push replaces the entry with the same id; malformed requests are rejected. In SQL, `ann_attribute_search` and `ann_filter_bitmap_build` + `ann_bitmap_search` equal exact SQL, including a filter matching nothing. Filter ids are stable across rebuilds and distinct per filter. A second worker loaded without PostgreSQL gets bitmaps pushed on demand by searches, then all of them from `ann_filter_bitmap_push`.
- `test/ann/test_cuvs_index.py` (cuVS): an index built with `--attribute`. Attribute and bitmap search equal SQL on the gather route. On filtered CAGRA (a second worker, which gets its bitmaps by on-demand pushes), with prebuilt and sliced bitsets, recall is at least 0.9 and no row falls outside the filter.

## Open items

- **One query over the forward, inverted and ANN indexes.** The pieces exist separately: the forward index is `keys.i64`, `vectors.f32` and the `attr-COLUMN.i32` columns (used to re-score candidates in FP32 and to map filter keys to rows); the inverted index is the per-attribute posting lists built at load (value → sorted ordinals with prebuilt bitsets) and the cached filter bitmaps; the ANN index is the CAGRA shards with the gather, grouped tensor-core and filtered-graph routes. What is missing is a combined query: a predicate over indexed attributes (`=`, `IN`, ranges, `NOT`, `AND`, `OR`, references to stored filters) sent in one operation, evaluated on the worker by intersecting/uniting posting lists or bitsets, cached under the expression's hash as an ordinary bitmap entry so it joins same-filter grouping, routed by the combined filter's size like today's searches, and able to return attribute columns (and the vector) with each hit so the caller needs no join back to the heap. Text columns would follow the same path with token postings (`--text COLUMN` at build time) and, as a second step, a text score fused with the distance before the top-k. Today a query can use one attribute `IN` list (`ann_attribute_search`) or one stored filter (`ann_bitmap_search`); other predicates go through `ann_hybrid_search` with key lists.
