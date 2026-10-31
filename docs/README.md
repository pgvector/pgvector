# Design documents

- [cuVS bulk build and filtered ANN implementation plan](cuvs-filtered-ann-plan.md): proposed GPU-backed indexing and search for full-text and text metadata predicates, including architecture, execution diagrams, repository code conventions, build/upgrade integration, delivery steps, and validation gates.
- [Native CPU staging index](ann-native-index.md): implemented C access method, SQL usage, storage/locking diagrams, source conventions, and lifecycle tests.
- [Native filtered exact search](ann-filtered-search.md): opt-in C CustomScan, supported query shapes, predicate-first execution, EXPLAIN, and tests.
- [Worker pipeline for filtered search](ann-worker-pipeline.md): pipelined, shared-memory worker route for the filtered CustomScan, mocked GPU backend, fallbacks, protocol operations 7–10, and measured throughput.
- [High-throughput filtered-search analysis](cuvs-search-performance.md): CPU eligibility and GPU execution optimization tracks, capacity estimates, batching/admission design, and end-to-end benchmark gates.

The plans distinguish proposed work from the implemented features described in the native index and filtered-search guides.

The [standalone C worker and C scheduler](../ann/README.md) are implemented. Python is restricted to test helpers and benchmark scripts, including the frozen-table SQL reference; the native C exact L2 access method is implemented, with an opt-in selective exact CustomScan that can pipeline scoring to the worker. GPU execution is mocked; CAGRA integration and cuVS compilation are deferred todos, with draft source retained. Accelerator generation/delta integration remains pending.
