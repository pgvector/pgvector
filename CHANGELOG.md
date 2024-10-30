## 0.8.0 (2024-10-30)

- Added support for iterative index scans
- Added casts for arrays to `sparsevec`
- Improved cost estimation for better index selection when filtering
- Improved performance of HNSW index scans
- Improved performance of HNSW inserts and on-disk index builds
- Dropped support for Postgres 12

## 0.7.4 (2024-08-05)

- Fixed locking for parallel HNSW index builds
- Fixed compilation error with GCC 14 on i386 when SSE2 is not enabled

## 0.7.3 (2024-07-22)

- Fixed `failed to add index item` error with `sparsevec`
- Fixed compilation error with FreeBSD ARM
- Fixed compilation warning with MSVC and Postgres 16

## 0.7.2 (2024-06-11)

- Fixed initialization fork for indexes on unlogged tables

## 0.7.1 (2024-06-03)

- Improved performance of on-disk HNSW index builds
- Fixed `undefined symbol` error with GCC 8
- Fixed compilation error with universal binaries on Mac
- Fixed compilation warning with Clang < 14

## 0.7.0 (2024-04-29)

- Added `halfvec` type
- Added `sparsevec` type
- Added support for indexing `bit` type
- Added support for indexing L1 distance with HNSW
- Added `binary_quantize` function
- Added `hamming_distance` function
- Added `jaccard_distance` function
- Added `l2_normalize` function
- Added `subvector` function
- Added concatenate operator for vectors
- Added CPU dispatching for distance functions on Linux x86-64
- Updated comparison operators to support vectors with different dimensions

## 0.6.2 (2024-03-18)

- Reduced lock contention with parallel HNSW index builds

## 0.6.1 (2024-03-04)

- Fixed error with `ANALYZE` and vectors with different dimensions
- Fixed segmentation fault with `shared_preload_libraries`
- Fixed vector subtraction being marked as commutative

## 0.6.0 (2024-01-29)

If upgrading with Postgres 12 or Docker, see [these notes](https://github.com/pgvector/pgvector#060).

- Added support for parallel index builds for HNSW
- Added validation for GUC parameters
- Changed storage for vector from `extended` to `external`
- Improved performance of HNSW
- Reduced memory usage for HNSW index builds
- Reduced WAL generation for HNSW index builds
- Fixed error with logical replication
- Fixed `invalid memory alloc request size` error with HNSW index builds
- Moved Docker image to `pgvector` org
- Added Docker tags for each supported version of Postgres
- Dropped support for Postgres 11

## 0.5.1 (2023-10-10)

- Improved performance of HNSW index builds
- Added check for MVCC-compliant snapshot for index scans

## 0.5.0 (2023-08-28)

- Added HNSW index type
- Added support for parallel index builds for IVFFlat
- Added `l1_distance` function
- Added element-wise multiplication for vectors
- Added `sum` aggregate
- Improved performance of distance functions
- Fixed out of range results for cosine distance
- Fixed results for NULL and NaN distances for IVFFlat

## 0.4.4 (2023-06-12)

- Improved error message for malformed vector literal
- Fixed segmentation fault with text input
- Fixed consecutive delimiters with text input

## 0.4.3 (2023-06-10)

- Improved cost estimation
- Improved support for spaces with text input
- Fixed infinite and NaN values with binary input
- Fixed infinite values with vector addition and subtraction
- Fixed infinite values with list centers
- Fixed compilation error when `float8` is pass by reference
- Fixed compilation error on PowerPC
- Fixed segmentation fault with index creation on i386

## 0.4.2 (2023-05-13)

- Added notice when index created with little data
- Fixed dimensions check for some direct function calls
- Fixed installation error with Postgres 12.0-12.2

## 0.4.1 (2023-03-21)

- Improved performance of cosine distance
- Fixed index scan count

## 0.4.0 (2023-01-11)

If upgrading with Postgres < 13, see [this note](https://github.com/pgvector/pgvector/blob/v0.4.0/README.md#040).

- Changed text representation for vector elements to match `real`
- Changed storage for vector from `plain` to `extended`
- Increased max dimensions for vector from 1024 to 16000
- Increased max dimensions for index from 1024 to 2000
- Improved accuracy of text parsing for certain inputs
- Added `avg` aggregate for vector
- Added experimental support for Windows
- Dropped support for Postgres 10

## 0.3.2 (2022-11-22)

- Fixed `invalid memory alloc request size` error

## 0.3.1 (2022-11-02)

If upgrading from 0.2.7 or 0.3.0, [recreate](https://github.com/pgvector/pgvector/blob/v0.3.1/README.md#031) all `ivfflat` indexes after upgrading to ensure all data is indexed.

- Fixed issue with inserts silently corrupting `ivfflat` indexes (introduced in 0.2.7)
- Fixed segmentation fault with index creation when lists > 6500

## 0.3.0 (2022-10-15)

- Added support for Postgres 15
- Dropped support for Postgres 9.6

## 0.2.7 (2022-07-31)

- Fixed `unexpected data beyond EOF` error

## 0.2.6 (2022-05-22)

- Improved performance of index creation for Postgres < 12

## 0.2.5 (2022-02-11)

- Reduced memory usage during index creation
- Fixed index creation exceeding `maintenance_work_mem`
- Fixed error with index creation when lists > 1600

## 0.2.4 (2022-02-06)

- Added support for parallel vacuum
- Fixed issue with index not reusing space

## 0.2.3 (2022-01-30)

- Added indexing progress for Postgres 12+
- Improved interrupt handling during index creation

## 0.2.2 (2022-01-15)

- Fixed compilation error on Mac ARM

## 0.2.1 (2022-01-02)

- Fixed `operator is not unique` error

## 0.2.0 (2021-10-03)

- Added support for Postgres 14

## 0.1.8 (2021-09-07)

- Added cast for `vector` to `real[]`

## 0.1.7 (2021-06-13)

- Added cast for `numeric[]` to `vector`

## 0.1.6 (2021-06-09)

- Fixed segmentation fault with `COUNT`

## 0.1.5 (2021-05-25)

- Reduced memory usage during index creation

## 0.1.4 (2021-05-09)

- Fixed kmeans for inner product
- Fixed multiple definition error with GCC 10

## 0.1.3 (2021-05-06)

- Added Dockerfile
- Fixed version

## 0.1.2 (2021-04-26)

- Vectorized distance calculations
- Improved cost estimation

## 0.1.1 (2021-04-25)

- Added binary representation for `COPY`
- Marked functions as `PARALLEL SAFE`

## 0.1.0 (2021-04-20)

- First release
