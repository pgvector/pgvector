# pgvector

Open-source vector similarity search for Postgres

```sql
CREATE TABLE table (column vector(3));
CREATE INDEX ON table USING ivfflat (column);
SELECT * FROM table ORDER BY column <-> '[1,2,3]' LIMIT 5;
```

Supports L2 distance, inner product, and cosine distance

[![Build Status](https://github.com/ankane/pgvector/workflows/build/badge.svg?branch=master)](https://github.com/ankane/pgvector/actions)

## Installation

Compile and install the extension (supports Postgres 9.6+)

```sh
git clone --branch v0.1.0 https://github.com/ankane/pgvector.git
cd pgvector
make
make install # may need sudo
```

Then load it in databases where you want to use it

```sql
CREATE EXTENSION vector;
```

## Getting Started

Create a vector column with 3 dimensions (replace `table` and `column` with non-reserved names)

```sql
CREATE TABLE table (column vector(3));
```

Insert values

```sql
INSERT INTO table VALUES ('[1,2,3]'), ('[4,5,6]');
```

Get the nearest neighbor by L2 distance

```sql
SELECT * FROM table ORDER BY column <-> '[3,1,2]' LIMIT 1;
```

Also supports inner product (`<#>`) and cosine distance (`<=>`)

Note: `<#>` returns the negative inner product since Postgres only supports `ASC` order index scans on operators

## Indexing

Speed up queries with an approximate index. Add an index for each distance function you want to use.

L2 distance

```sql
CREATE INDEX ON table USING ivfflat (column);
```

Inner product

```sql
CREATE INDEX ON table USING ivfflat (column vector_ip_ops);
```

Cosine distance

```sql
CREATE INDEX ON table USING ivfflat (column vector_cosine_ops);
```

Indexes should be created after the table has data for optimal clustering. Also, unlike typical indexes which only affect performance, you may see different results for queries after adding an approximate index.

### Index Options

Specify the number of inverted lists (100 by default)

```sql
CREATE INDEX ON table USING ivfflat (column) WITH (lists = 100);
```

### Query Options

Specify the number of probes (1 by default)

```sql
SET ivfflat.probes = 1;
```

A higher value improves recall at the cost of speed.

Use `SET LOCAL` inside a transaction to set it for a single query

```sql
BEGIN;
SET LOCAL ivfflat.probes = 1;
SELECT ...
COMMIT;
```

## Reference

### Vector Type

Each vector takes `4 * dimensions + 8` bytes of storage. Each element is a float, and all elements must be finite (no `NaN`, `Infinity` or `-Infinity`). Vectors can have up to 1024 dimensions.

### Vector Operators

Operator | Description
--- | ---
\+ | element-wise addition
\- | element-wise subtraction
<-> | Euclidean distance
<#> | negative inner product
<=> | cosine distance

### Vector Functions

Function | Description
--- | ---
cosine_distance(vector, vector) | cosine distance
inner_product(vector, vector) | inner product
l2_distance(vector, vector) | Euclidean distance
vector_dims(vector) | number of dimensions
vector_norm(vector) | Euclidean norm

## Thanks

Thanks to:

- [PASE: PostgreSQL Ultra-High-Dimensional Approximate Nearest Neighbor Search Extension](https://dl.acm.org/doi/pdf/10.1145/3318464.3386131)
- [Faiss: A Library for Efficient Similarity Search and Clustering of Dense Vectors](https://github.com/facebookresearch/faiss)
- [Using the Triangle Inequality to Accelerate k-means](https://www.aaai.org/Papers/ICML/2003/ICML03-022.pdf)
- [k-means++: The Advantage of Careful Seeding](https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf)
- [Concept Decompositions for Large Sparse Text Data using Clustering](https://www.cs.utexas.edu/users/inderjit/public_papers/concept_mlj.pdf)

## History

View the [changelog](https://github.com/ankane/pgvector/blob/master/CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/pgvector/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/pgvector/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/pgvector.git
cd pgvector
make
make install
```

To run all tests:

```sh
make installcheck        # regression tests
make prove_installcheck  # TAP tests
```

To run single tests:

```sh
make installcheck REGRESS=vector                  # regression test
make prove_installcheck PROVE_TESTS=t/001_wal.pl  # TAP test
```

Directories

- `expected` - expected output for regression tests
- `sql` - regression tests
- `t` - TAP tests

Resources for contributors

- [Extension Building Infrastructure](https://www.postgresql.org/docs/current/extend-pgxs.html)
- [Index Access Method Interface Definition](https://www.postgresql.org/docs/current/indexam.html)
- [Generic WAL Records](https://www.postgresql.org/docs/13/generic-wal.html)
