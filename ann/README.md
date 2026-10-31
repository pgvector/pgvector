# Native C search worker and scheduler

Runtime code in this directory is C. Python is used only by test helpers and benchmark launchers under `test/ann/` and `benchmarks/ann/`. The [native C PostgreSQL AM](../docs/ann-native-index.md) provides an exact L2 baseline; an [opt-in exact CustomScan](../docs/ann-filtered-search.md) scores eligible vectors, optionally [pipelined to this worker](../docs/ann-worker-pipeline.md), and [loaded cuVS indexes](../docs/ann-cuvs-index.md) serve sharded multi-GPU ANN and hybrid search from SQL; the Python frozen-table implementation is retained solely as a test reference, not an application API.

## Build and test

A C11 compiler, POSIX threads, and Make are required for the standalone worker on Linux/macOS. PostgreSQL's main build is independent of this experimental worker. Windows is not a supported worker platform.

```sh
make -C ann
make -C ann check
# GPU hosts only: also build ann-worker-cuvs (pip SDK paths: see docs/ann-cuvs-index.md)
make -C ann USE_CUVS=1 CUVS_PREFIX=/path/to/libcuvs CUDA_HOME=/path/to/cuda
make -C ann check-cuvs CUVS_PREFIX=/path/to/libcuvs
```

`USE_CUVS` defaults to 0, so hosts without a GPU SDK build and test every CPU path. Loaded indexes are served by both builds. The CPU worker scans them exactly, which makes it the test reference; the cuVS worker searches CAGRA shards on all GPUs ([guide](../docs/ann-cuvs-index.md)).

`make` builds the C executable and `libannbatch.a`, without Python or a GPU SDK. `make check` uses Python to drive C protocol and scheduler tests. Live PostgreSQL and GPU tests are explicitly gated.

## C batching and admission API

[batching.h](batching.h) exposes the native API; [batching.c](batching.c) implements scheduling, resource accounting, deadlines, cancellation, and request/result ownership. There is no Python scheduler or runtime client dependency.

```c
#include "batching.h"

int main(void)
{
    float vectors[] = {0, 1, 1, 1, 2, 1};
    float query[] = {1, 1};
    AnnBatchOptions options;
    AnnBatcher *batcher;
    AnnRequest *request;
    uint32_t count;

    AnnBatchDefaultOptions(&options);
    if (AnnBatchCreate(AnnCpuBackend(), 1, 3, 2, vectors,
                       &options, &batcher) != ANN_OK)
        return 1;
    if (AnnBatchSubmit(batcher, query, 1, 0, NULL, 0,
                       30000000, &request) != ANN_OK)
    {
        AnnBatchClose(batcher);
        return 1;
    }
    if (AnnRequestWait(request, 30000000) == ANN_OK)
    {
        const AnnCandidate *results = AnnRequestResults(request, &count);
        /* Consume results[0..count); storage belongs to request. */
        (void) results;
    }
    AnnRequestRelease(request);
    AnnBatchClose(batcher);
    return 0;
}
```

Link with `-Iann -Lann -lannbatch -pthread -lm`. The scheduler owns a canonical index copy by default. The worker uses `AnnBatchCreateBorrowed()` to share its immutable index; it closes the scheduler before unloading or replacing that index. Failed replacement builds preserve the old generation.

Admission returns `ANN_OVERLOADED` without queueing when request count, bytes, or estimated work exceed capacity. The C byte charge includes the request structure, query, filter, and reserved result array. It excludes the resident index, allocator overhead, bounded scheduler metadata, input wire frames, and results retained by callers after completion. Work is estimated as eligible count times dimension. Limits apply to queued and running work; timed-out running requests retain reservations until execution finishes.

Compatible jobs share metric, effective k, filter presence, and a coarse selectivity class. The oldest request anchors selection. The dispatcher groups jobs up to row/byte/work limits or the collection delay. CPU distance evaluation remains serial; there is no claimed GPU kernel batching or automatic cross-process aggregation. CPU worker search frames use this C scheduler directly. External protocol frames are still read serially.

A monotonic-clock watcher expires queued and running requests. Queued cancellation removes the job; running cancellation is not allowed. `AnnBatchClose()` cancels queued work, marks unfinished results canceled, waits for backend work to finish, and releases the index. It does not forcibly terminate a C thread or preempt a GPU kernel. Request handles remain valid until `AnnRequestRelease()`, even after service close. Caller-held results are released with their handle.

Lifecycle rules: coordinate producer threads before closing the service; do not use a batcher handle after `AnnBatchClose()`. Do not release a request handle while another caller is using that same handle. Backend descriptors must outlive the service. Worker threads must never call PostgreSQL backend APIs; future backend integration uses PostgreSQL's memory, lock, interrupt, and resource mechanisms in the backend process.

## Protocol

The C worker uses version 1, 32-byte little-endian headers: magic `PVAN`, u16 version, u16 operation, u64 request ID, u64 generation, u32 payload size, u32 status. Frames are capped at 64 MiB. Generation zero is reserved, and successful builds increase generation monotonically per process/session.

| Operation | Request | Success result |
| --- | --- | --- |
| 0 capabilities | Empty | JSON describing compiled backend capabilities |
| 1 build | u32 rows, dimensions; row-major FP32 vectors | Empty |
| 2 search | u32 queries, dimensions, k, metric, filter; vectors; optional sparse allowlists | Per query: u32 count and `(u32 ordinal, FP64 distance)` records |
| 3 unload | Empty | Empty |
| 4 export | Empty | Canonical build payload, not a graph artifact |
| 5 diagnostic probe | Bounded bytes | Empty; never a search result |
| 6 index info | Empty | u32 rows and dimensions |
| 7–10 | Gathered and shared-memory search for the [pipelined CustomScan](../docs/ann-worker-pipeline.md) (`--listen` mode) | |
| 11 load index | u32 name length, name, directory path | Text `rows N dimensions D shards S dtype T source NAME` |
| 12 index search | u32 name length, name, u32 dimensions, k, filter count (UINT32_MAX = none); FP32 query; i64 keys | u32 count, u32 route (0 graph, 1 filtered graph, 2 exact), then `(i64 key, f32 distance)` per hit |

Metric values: 0 L2, 1 negative inner product, 2 cosine. Filter values: 0 none, 1 shared IDs, 2 IDs per query. Allowlists are u32 count followed by strictly increasing unique u32 ordinals; an empty list differs from no filter. Capability masks use `1 << enum_value`. The CPU backend uses FP32 input and double accumulation.

Statuses 0–5 are success, invalid, unsupported, stale, allocation failure, and backend failure. The C scheduling API adds 6 pending, 7 overloaded, 8 timed out, 9 canceled, and 10 closed. Search errors have empty protocol payloads. The PostgreSQL client enforces `ann.worker_timeout_ms`, and server mode sets a 10 s reply write timeout; timeouts in the Python test client are test infrastructure, not a shipped C client feature.

## Test references and benchmarks

[test/ann/support/](../test/ann/support/) contains Python-only protocol clients and a frozen-table SQL oracle. Snapshot checksum/save/load helpers there are test conveniences; production canonical export/import is implemented in C, while a durable C artifact-file format remains pending.

```sh
python3 -m pip install -r test/ann/requirements-postgres.txt
ANN_PG_DSN='dbname=your_test_database' make -C ann check-postgres
python3 benchmarks/ann/postgres.py --rows 10000 --dimensions 32 > /tmp/ann-baseline.sql
psql -X -v ON_ERROR_STOP=1 -f /tmp/ann-baseline.sql
python3 benchmarks/ann/search.py --mode shared --batch 16
python3 benchmarks/ann/batching.py --requests 1000 --sequential
python3 benchmarks/ann/batching.py --requests 1000 --pending 1024 --batch-rows 64
```

The batching launcher runs [a C benchmark](../benchmarks/ann/batch.c); search and scheduling execute entirely in C. It reports successful/offered/rejected counts, elapsed time, and batch occupancy. The test-client search benchmark measures IPC and uses an independent distance oracle. Neither benchmark includes SQL eligibility or establishes production QPS. The PostgreSQL fixtures create temporary or uniquely named test objects and require the corresponding database permissions.

C tests cover batching/filter isolation, input copying, budgets, queued/running deadlines, concurrent producers, early handle release, shutdown during execution, backend errors, and result lifetime after close. Python protocol tests independently validate metrics and filters. Sanitizer checks:

```sh
make -C ann clean
make -C ann CFLAGS='-O1 -g -std=c11 -Wall -Wextra -Werror -pedantic -fsanitize=address,undefined -fno-omit-frame-pointer' check
```

## cuVS integration

`cuvs.c` is compiled and validated against cuVS 26.10 on NVIDIA B200 GPUs (`make -C ann check-cuvs`):

- **Gathered search** serves the [pipelined CustomScan](../docs/ann-worker-pipeline.md) with exact GPU brute force.
- **Loaded indexes** serve sharded CAGRA search across all GPUs, with bitset filters and an exact route for selective filters ([guide](../docs/ann-cuvs-index.md)).

The stdin-mode resident CAGRA build (operations 1–4 with `--backend cuvs`) is still the older unvalidated draft. Use loaded indexes instead. hipVS and NVFP4 adapters are not implemented: cuVS 26.10 CAGRA supports FP32, FP16, int8 and uint8 datasets, and no FP4 or FP8 path.

C PostgreSQL worker integration is implemented: `--listen PATH` serves pipelined gathered searches (operations 7–10) to `AnnFilteredSearch`, with a `mock-gpu` backend standing in for accelerators ([guide](../docs/ann-worker-pipeline.md)). Sustained-load validation remains. Python remains test/benchmark tooling only.
