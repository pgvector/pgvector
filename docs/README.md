# Design documents

- [cuVS bulk build and filtered ANN implementation plan](cuvs-filtered-ann-plan.md): proposed GPU-backed indexing and search for full-text and text metadata predicates, including architecture, execution diagrams, repository code conventions, build/upgrade integration, delivery steps, and validation gates.
- [Native CPU staging index](ann-native-index.md): implemented C access method, SQL usage, storage/locking diagrams, source conventions, and lifecycle tests.
- [Native filtered exact search](ann-filtered-search.md): opt-in C CustomScan, supported query shapes, predicate-first execution, EXPLAIN, and tests.
- [Loaded cuVS indexes](ann-cuvs-index.md): offline sharded CAGRA build, multi-GPU loading, `ann_index_search` ANN and hybrid search from SQL, the `USE_CUVS` build flag, B200 throughput options, and load-test results.
- [Networked GPU nodes](ann-networked.md): many PostgreSQL hosts evaluate filters and send vector search to GPU nodes over authenticated TCP, with size-based routing, heartbeats and failover, and scaling measurements.
- [Worker pipeline for filtered search](ann-worker-pipeline.md): pipelined, shared-memory worker route for the filtered CustomScan, mocked GPU backend, fallbacks, protocol operations 7–10, and measured throughput.
- [High-throughput filtered-search analysis](cuvs-search-performance.md): CPU eligibility and GPU execution optimization tracks, capacity estimates, batching/admission design, and end-to-end benchmark gates.

The plans distinguish proposed work from the implemented features described in the native index and filtered-search guides.

The [standalone C worker and C scheduler](../ann/README.md) are implemented. Python is restricted to test helpers and benchmark scripts, including the frozen-table SQL reference; the native C exact L2 access method is implemented, with an opt-in selective exact CustomScan that can pipeline scoring to the worker. Loaded cuVS indexes serve sharded CAGRA search on GPUs, validated with cuVS 26.10 on B200s; the pipeline's mock GPU backend remains for hosts without one. Accelerator generation/delta integration remains pending.
