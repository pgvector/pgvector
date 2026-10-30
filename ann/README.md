# Portable vector-search worker foundation

This implements the standalone foundation and an experimental frozen-table path from the [overall plan](../docs/cuvs-filtered-ann-plan.md). It includes a tested CPU exact backend, framed stdio transport, canonical FP32 snapshots, filtered search, and live-tested PostgreSQL eligibility integration. An optional cuVS CAGRA adapter is implemented against the 26.10 C API but has not been compiled or run with that SDK here. A native PostgreSQL AM, concurrent scheduling, and GPU graph serialization remain pending.

```sh
make -C ann
make -C ann check
python3 benchmarks/ann/search.py --mode shared --batch 16
python3 benchmarks/ann/search.py --mode unique --batch 16
```

Generate native PostgreSQL text/metadata eligibility, exact-reference, and planner-selected iterative-HNSW baselines with:

```sh
python3 benchmarks/ann/postgres.py --rows 10000 --dimensions 32 > /tmp/ann-baseline.sql
psql -X -v ON_ERROR_STOP=1 -f /tmp/ann-baseline.sql
```

The SQL uses a temporary table, performs its work in a rolled-back transaction, and needs pgvector installed. It sets complete GIN eligibility and records plans without forcing a particular index. The generated baseline ran successfully on a temporary PostgreSQL 17.6 build with this checkout's pgvector.

The benchmark reports batch requests/sec and vector searches/sec separately. Eligibility is synthetic and precomputed; the numbers include Python client encoding, local pipe transport, and CPU exact search. They are not SQL, GPU, or production throughput. An independent oracle validates each initial query outside the timed window.

## Frozen PostgreSQL prototype

Install the optional client dependency and run against a test database:

```sh
python3 -m pip install -r ann/requirements-postgres.txt
ANN_PG_DSN='dbname=your_test_database' make -C ann check-postgres
```

The live tests create/remove uniquely named schemas and require permissions to create schemas/tables and install the vector extension if absent. Use the prototype from the repository root:

```python
from psycopg import sql
from ann.postgres import FrozenIndex

with FrozenIndex('dbname=your_database', 'documents') as index:
    matches = index.search(
        [[1, 2, 3], [3, 2, 1]], k=10,
        predicate=sql.SQL("tenant = %s AND search_text @@ to_tsquery('simple', %s)"),
        params=('acme', 'database & query'))
```

This owns a private connection and lifetime `SHARE` table lock, then makes its transaction read-only. It bulk loads bounded canonical vectors and maps unique integer keys to ordinals. Each search executes the predicate in PostgreSQL and shares the resulting allowlist across its query vectors. Values are parameterized and identifiers composed through psycopg; predicates themselves are trusted application SQL.

Non-heap tables, null vectors, duplicate/noninteger keys, inconsistent dimensions, and oversized corpora are rejected. `max_rows` and the wire-byte limit bound corpus size, not exact Python RSS. Closing the context releases the worker and table freeze; no generation survives that context. The implementation is live-tested against SQL exact results, external INSERT/UPDATE/DELETE/TRUNCATE/ALTER attempts, failed-build cleanup, read-only enforcement, and loss of the lock-owning session during external search. A post-search SQL round trip verifies session liveness before returning results; include that cost when benchmarking. This is a client-side materialized prototype, not a planner hook or persistent index; it deliberately blocks source writes for the entire context.

## Optional cuVS adapter

On a supported NVIDIA Linux host with the **cuVS 26.10 C SDK**, DLPack headers, and CUDA runtime:

```sh
make -C ann ann-worker-cuvs CUVS_PREFIX=/path/to/cuvs CUDA_HOME=/usr/local/cuda
LD_LIBRARY_PATH=/path/to/cuvs/lib:/usr/local/cuda/lib64 make -C ann check-cuvs CUVS_PREFIX=/path/to/cuvs
python3 benchmarks/ann/search.py --backend cuvs --rows 4096 --dimensions 32 --survivor-fraction .25
```

Use `CPPFLAGS`/`LDFLAGS` for additional include/library paths. The separate executable links `libcuvs_c` and `libcudart`; ordinary `make` retains the CPU-only dependency boundary. Supply the library search path for both tests and benchmarks. Runtime build checks reject cuVS versions other than 26.10. API references: [pinned CAGRA header](https://github.com/NVIDIA/cuvs/blob/v26.10.00/c/include/cuvs/neighbors/cagra.h) and [dataset header](https://github.com/NVIDIA/cuvs/blob/v26.10.00/c/include/cuvs/core/dataset.h).

The adapter builds FP32 L2 CAGRA over an owned device-padded dataset, applies sparse eligibility through a device bitset, validates returned ordinals/membership, and reranks candidate scores against canonical vectors. Membership remains approximate. Minimum build size is 128 rows, graph degrees are 32/64, and k is capped at 1,024. Build errors preserve the prior generation; cleanup synchronizes before releasing referenced resources.

The current protocol worker invokes search per query row, so GPU calls are serialized even for a shared-filter input batch. Buffers/masks are allocated per search. This adapter is a correctness baseline, not the planned high-throughput microbatcher. Empty filters and k=0 return without a GPU search. L2 is the only supported metric. ANN underfill is reported without hidden CPU execution. The hardware-gated recall test is a release gate, not an outcome established on this host.

To select it for `FrozenIndex`, pass `executable='/absolute/path/to/ann/ann-worker-cuvs', backend='cuvs'`. GPU tests cover shared/per-query filters, recall, reload-by-rebuild, and failed-build atomicity. `make check-cuvs` fails if required compiler/SDK/libraries/device are missing; default tests explicitly skip hardware-only checks.

## Vendor boundary

`backend.h` defines a narrow worker-local build/search/destroy interface. `cpu.c` supplies exact FP32 input search with double accumulation. Backend selection is explicit (`--backend cpu-exact` or `--backend cuvs` in the optional executable). The CPU-only executable rejects cuVS; hipVS and fused NVFP4 remain unsupported. There is no silent substitute.

The PostgreSQL-facing wire format carries canonical query values, generation IDs, metrics, and eligible local ordinals. Native device objects, compressed layouts, streams, and library exceptions belong inside the selected worker implementation. Initially run separate worker processes per backend/device; a shared resource object or graph is not portable across vendors.

| Backend | Current implementation | Required integration evidence |
| --- | --- | --- |
| `cpu-exact` | Built and tested here. | Independent distance/filter oracle and protocol tests. |
| `cuvs` | Optional L2 CAGRA source implemented; SDK compilation and GPU validation pending. | Run hardware-gated recall/filter/rebuild tests, then implement batching, exact GPU routes, graph reload, memory and cancellation tests. |
| `hipvs` | Pending ROCm/hipVS SDK and GPU test host. | Independent filter/metric/batch probes; cuVS feature parity must not be assumed. |
| `fused-nvfp4` | Kernel repository/API unspecified. | Quantization/scaling format, supported GPU, dimensions/layout, filtering and top-k interface, error/recall bounds. |

AMD documents hipVS as aligned with cuVS 25.10, which is different from the cuVS 26.10 API used in the design. See [AMD hipVS](https://rocm.docs.amd.com/projects/hipVS/en/latest/). NVFP4 is a precision/kernel path, not by itself a complete ANN/index lifecycle. A fused implementation may provide only exact or approximate candidate scoring; it must advertise that limitation. Preserve canonical vectors for authoritative reranking and do not label quantized scores exact.

Capability negotiation must precede routing. Current CPU capabilities describe supported metrics, input dtype, exactness, filters, frame limit, and maximum query rows. Extend that response with vendor algorithm, storage/precision version, device limits, and filter support when adding an actual backend. Reject unsupported metrics/filter formats instead of dropping predicates. Match cache artifacts and batches by backend, device, precision/layout, algorithm, and generation as well as the existing query compatibility fields.

## Protocol version 1

Every integer is explicitly little-endian. The 32-byte header is `4s, u16, u16, u64, u64, u32, u32`: magic `PVAN`, version 1, operation, request ID, generation, payload bytes, and status. Requests set status to zero; replies echo request identity. Frames are limited to 64 MiB. Invalid framing, unsupported versions, and oversized frames terminate the connection without attempting to drain an attacker-controlled length.

| Operation | Request payload | Success payload |
| --- | --- | --- |
| 0: capabilities | Empty. | UTF-8 JSON. |
| 1: bulk build | `u32 rows, u32 dimensions`, then row-major IEEE binary32 vectors. | Empty. |
| 2: batch search | `u32 queries, dimensions, k, metric, filter`, query vectors, then optional allowlists. | Per query: `u32 count`, then `(u32 ordinal, binary64 distance)` records. |
| 3: unload | Empty. | Empty. |
| 4: export | Empty. | Canonical build payload; not a native graph. |
| 5: diagnostic transport probe | Arbitrary bounded bytes. | Empty; never a search result. |

Metric values are 0 for L2 (unsquared output), 1 for negative inner product, and 2 for cosine distance. Filters are 0 for unrestricted, 1 for one shared sparse-ID list, and 2 for a sparse-ID list per query. Each list is a `u32 count` followed by that many `u32` ordinals, strictly increasing and unique. An empty allowlist differs from unrestricted search.

Capability `metrics_mask` and `filters_mask` use bit `1 << enum_value`; a value of 7 advertises all three currently defined variants. Capabilities also report algorithm, storage precision, canonical input precision, exactness, minimum rows, and maximum k. Capability strings come from trusted compiled adapter metadata, not request input.

Generation zero is reserved. Successful builds require strictly increasing generations for the lifetime of one worker, including after unload. A failed build preserves the prior index. A restarted worker is a new connection/session; clients must rebuild/reload before search, and cannot reuse handles from a prior process. Ordinals identify dataset rows, not SQL primary keys or heap TIDs.

Status values are 0 success, 1 invalid input, 2 unsupported operation/capability, 3 stale/missing generation, 4 allocation failure, and 5 backend execution failure. Errors have empty payloads. The reference client serializes requests; a client must not share a connection concurrently. Up to 1,024 query rows per batch are accepted, subject to frame and result-size limits. The service reads complete bounded frames; it does not implement production admission, deadlines, or preemptive cancellation yet.

The exact route uses a bounded top-k heap and deterministic ordinal ordering for distance ties. Nonfinite vector/query values are rejected. Zero dataset vectors are excluded for cosine; a zero cosine query is rejected. Other routes may require stricter validation and dimension limits. Validate compatibility with PostgreSQL's floating-point distance behavior before native integration.

## Canonical snapshots

`client.Worker.save()` exports canonical FP32 data and atomically replaces a client-side file. The `PVAS` header stores version, payload size, and SHA-256 checksum; `load()` checks the bounded file and imports into a new generation. This is a standalone corpus format, not PostgreSQL WAL, a graph artifact, or a durable MVCC index. Row identity mappings remain the caller's responsibility.

## Validation

### Request bounds and timing

`Worker(timeout=30)` uses one persistent transport thread per client and a 30-second default wire-request timeout, including blocked writes and reads. Timeouts, transport corruption, and cancellation invalidate the whole worker process/session; callers must rebuild/reload in a new client. `cancel()` terminates the dedicated process, not an individual GPU kernel. Cleanup may take up to five additional seconds. Normal close sends EOF and allows backend cleanup before falling back to termination. Concurrent requests on one client are rejected; this is bounded synchronous transport, not a microbatch scheduler.

`FrozenIndex` accepts `worker_timeout` and `statement_timeout_ms` (defaults 30 seconds and 30,000 ms). The SQL limit applies per statement, not to the full context or whole search. Search failures close the private connection and release the table freeze. Successful searches expose `last_search` with eligible/query counts and elapsed eligibility/mapping, worker round trip, session-check/projection, and total seconds. Worker time includes client encoding and transfer, not just kernel time. These are diagnostic observations, not GPU event timings or throughput guarantees.

The Python tests exercise all metrics against an independent exhaustive oracle; shared/per-query/empty filters; ties; zero vectors; k exceeding matches; malformed and oversized requests; unsupported vendors; failed-build atomicity; stale generations; canonical reload; and checksum corruption. Run from the repository root with `make -C ann check`.

For sanitizer validation on a supported compiler:

```sh
make -C ann clean
make -C ann CFLAGS='-O1 -g -std=c11 -Wall -Wextra -Werror -pedantic -fsanitize=address,undefined -fno-omit-frame-pointer' check
```

Next implementation work is SDK compilation and GPU validation, bounded multi-lane transport/admission, and a native PostgreSQL AM/CustomScan. Complete MVCC/index storage and recovery gates before native mutable deployment.
