# Blackwell module: quantized filtered search on B200

The cuVS worker keeps using cuVS CAGRA for graph search. On Blackwell GPUs (compute capability 10.x, such as the B200), filtered searches that go to the GPU gather scan use the worker's Blackwell module instead:

- **Quantized index.** The builder writes a 4-bit (NVFP4) or 8-bit (FP8 E4M3) copy of the vectors next to the exact FP32 ones. The worker keeps it in each shard's GPU memory.
- **Blackwell kernels.** The worker compiles the module with NVRTC for `sm_100a`, the Blackwell-specific target. It decodes with Blackwell's conversion instructions (`cvt.rn.f16x2.e2m1x2` for FP4 and `cvt.rn.f16x2.e4m3x2` for FP8), which were checked bit-exact against a host decoder on the B200s.
- **Fused search.** One kernel launch scores the eligible rows and selects the best 32, replacing the gather path's separate key lookup, scoring kernel, `cuvsSelectK` and copies.
- **Exact results.** The worker re-scores the 32 candidates with the FP32 vectors and returns the top k, so distances match `<->`.

PostgreSQL, the SQL functions and the network protocol are unchanged: a GPU node loads a quantized index like any other, and `ann_index_search`, `ann_hybrid_search`, `ann_attribute_search` and `ann_bitmap_search` use the module whenever their filter reaches the gather scan.

## Building a quantized index

```sh
python3 scripts/ann/build_cuvs_index.py --dsn 'dbname=app' --table public.docs \
    --key id --vector embedding --out /data/ann/docs --quantize nvfp4    # or fp8
```

| File | Content |
| --- | --- |
| `vectors.nvfp4` (NVFP4) | Per row, `ceil(dims / 16) * 8` bytes: two E2M1 values per byte, the even dimension in the low nibble, dimensions zero padded to a multiple of 16 |
| `scales.e4m3` (NVFP4) | Per row, one E4M3 scale per 16 dimensions, rounded up so no value clips |
| `vectors.e4m3` (FP8) | Per row, dimensions rounded up to 16 bytes of E4M3 values |
| `rowscale.f32` (both) | One FP32 scale per row |

A value decodes as `e2m1 × block scale × row scale` (NVFP4, the two-level scaling of the NVFP4 format) or `e4m3 × row scale` (FP8). The manifest gets a line `quant nvfp4` or `quant fp8`, and `vectors.f32` and the CAGRA shards are written as usual. The builder uses CuPy when it's installed, else NumPy, and both produce identical bytes. Quantizing 10M × 128 took 5 s.

| 10M × 128 | Bytes per row | GPU memory per index copy |
| --- | --- | --- |
| FP32 (`vectors.f32`) | 512 | 5.1 GB |
| FP8 | 128 + 4 | 1.26 GB |
| NVFP4 | 64 + 8 + 4 | 0.72 GB |

The CPU worker loads quantized indexes too. It validates the files and keeps answering exactly from `vectors.f32`.

## In the worker

At load, each shard's slice of the quantized copy goes to the GPU holding that shard, or to every GPU with `--replicate on`. If every GPU is compute capability 10.x and NVRTC compiles the module for `sm_100a`, the index's gather scans use the module. The worker's log says so (`Blackwell module: nvfp4 gather scan for index docs on (724.8 MB of quantized rows on the GPUs)`), and so do the replies to load and index info (`quant nvfp4 quant_scan on`). On other GPUs, or with `--quantized-scan off`, the copy isn't uploaded and the scan reads the CAGRA dataset as before.

**Fused search kernel.** For each query and shard:
1. The worker uploads the eligible rows. Filters that arrive with their ordinals (cached bitmaps, attribute values) become 4-byte shard rows on the host. Key lists are uploaded as keys and found by the existing lookup kernel.
2. One launch scores and selects. Eight threads score a row, each taking 16 dimensions with one 8-byte (NVFP4) or 16-byte (FP8) load and, for NVFP4, one block-scale decode. A warp scores four rows a step and keeps its best 32 in registers, merging new rows with warp-level bitonic sorts only when one beats both its own 32nd best and the best 32nd best any warp of its block has published. The last block to finish merges every block's list, its eight warps in parallel.
3. The worker copies back the 32 positions, re-scores them in FP32, and merges shards as before. It checks the positions for repeats, as on every gather scan.

Small lists get 128-row blocks, so enough warps hide the scattered row loads; larger ones 512-row blocks, up to four per SM, so the final merge stays short. The path serves k ≤ 24 (at least 8 extra candidates); larger k uses the earlier path with `cuvsSelectK` and `--quant-slack` extra candidates (default 256).

| Worker option | Effect |
| --- | --- |
| `--quantized-scan on\|off` | Use an index's quantized copy on Blackwell GPUs (default on). |
| `--quant-slack N` | Extra candidates re-scored in FP32 when k is too large for the fused kernel (default 256). |

## Measurements

10M × 128 rows, k = 10, one B200.

**Kernel stages, per query** (one GPU stream, CUDA events, 200 repetitions; random eligible rows):

| Stage (µs) | 10k rows (0.1%) | 100k rows (1%) | 1M rows (10%) |
| --- | --- | --- | --- |
| Earlier path: upload keys | 4.8 | 18.0 | 147 |
| Earlier path: look up rows | 8.3 | 8.2 | 27 |
| Earlier path: score (FP32) | 5.4 | 14.4 | 168 |
| Earlier path: `cuvsSelectK` | 23.7 | 33.2 | 68 |
| Earlier path: copy result | 3.1 | 3.0 | 3 |
| **Earlier path, total** | **45** | **77** | **413** |
| Fused: upload 4-byte rows | 4.0 | 10.5 | 75 |
| Fused: score and select (NVFP4) | 15.4 | 40.9 | 156 |
| Fused: copy 32 results | 3.2 | 3.1 | 3 |
| **Fused NVFP4, total** | **23 (2.0×)** | **55 (1.4×)** | **234 (1.8×)** |

FP8 runs within 2% of NVFP4: the scan is bound by scattered row loads and latency, not bytes. Scoring alone (without selection) took 10.6 µs for 100k rows and 82 µs for 1M from NVFP4, against 14.4 and 168 µs from FP32. Both formats found the exact top 10 inside their top 32 at every size.

**Recall**, measured straight against the worker (key lists from 40 queries per filter size, exact SQL ground truth): 1.000 for FP32, FP8 and NVFP4 at 0.1%, 1% and 10%, with the fused path's 22 extra candidates and also with only 16 on the earlier path.

**End to end** (cached bitmaps, `ann_bitmap_search`, 512 clients per PostgreSQL host, QPS):

| Workload | 1 GPU, FP32 | 1 GPU, FP8 | 1 GPU, NVFP4 | 8 GPUs replicated, FP32 | 8 GPUs, FP8 | 8 GPUs, NVFP4 |
| --- | --- | --- | --- | --- | --- | --- |
| 0.1% filter | 76,894 | **107,788** (+40%) | 106,860 | 255,008 | **281,936** (+11%) | 282,668 |
| 1% filter | 35,782 | **51,438** (+44%) | 48,525 | 142,191 | **175,569** (+23%) | 175,216 |
| 10% filter | 16,054 | 15,942 | 15,941 | 107,875 | 107,088 | 106,715 |

The 1-GPU runs (28 worker cores) are driven by one 64-core PostgreSQL host; the 8-GPU runs (96 worker cores, `--replicate on`) by two, and report their sum. All use default settings, with no route flags.
- **0.1% and 1% filters take the fused path.** Their cached bitmaps hold ordinals, so the worker uploads 4-byte rows and skips the key lookup and `cuvsSelectK`.
- **10% filters are unchanged.** Above `--bitset-gather-limit` (500,000 rows) a cached filter runs filtered CAGRA, which the module doesn't touch. Forcing them onto the fused gather scan (`--bitset-gather-limit 2000000`) gave 10.1–10.5k QPS on one GPU, against 16k on CAGRA and 5.0k on the FP32 gather scan.
- **FP8 and NVFP4 perform alike.** The 7× smaller NVFP4 copy is the choice when GPU memory matters: 0.72 GB instead of 5.1 GB per copy of 10M × 128.

Sending key lists straight to the worker (`ann_index_search` with keys) showed no gain: those requests carry 0.8–8 MB of keys each, and moving them over TCP, not the GPU, is the limit there.

## Not used, and why

- **Tensor cores for FP4/FP8 math.** A filtered query scores one query against its own rows: matrix-vector work, bound by scattered loads (the NVFP4 scan of 1M rows reads about 72 MB in 80 µs, 0.9 TB/s). Tensor cores pay off when many queries share rows, for example concurrent queries with the same cached filter grouped into a queries × rows block. Worker batches hold about 2–6 jobs today, so that grouping is unmeasured. Blackwell's tensor-core instructions also need CUTLASS-level kernels.
- **Triton.** It could prototype the fused kernel and a grouped tensor-core kernel (it has scaled low-precision matrix multiplies for Blackwell). It would have to ship as an ahead-of-time CUBIN loaded by the C worker, since the query path stays C. Not tried.
- **TMA, thread-block clusters, the 228 KB shared memory, NVLink 5.** Nothing measured here is limited by what they address.
- **cuVS graph search on quantized data.** cuVS 26.10 has no FP4 or FP8 CAGRA path, so broad filters (above `--bitset-gather-limit`) still run filtered CAGRA on the FP32 or FP16 dataset.

## Tests

- `test/ann/test_index.py` (CPU worker): an index with a `quant nvfp4` or `quant fp8` manifest line loads, reports `quant_scan off`, and answers exactly; truncated quantized files and unknown kinds are rejected.
- `test/ann/test_cuvs_index.py` (cuVS): indexes built with `--quantize nvfp4` and `--quantize fp8` report `quant_scan on` on compute capability 10.x GPUs; filtered searches through the module have recall ≥ 0.99, exact distances, and no row outside the filter.
