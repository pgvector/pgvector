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
git clone --branch v0.2.0 https://github.com/ankane/pgvector.git
cd pgvector
make
make install # may need sudo
```

Then load it in databases where you want to use it

```sql
CREATE EXTENSION vector;
```

You can also install it with [Docker](#docker), [Homebrew](#homebrew), or [PGXN](#pgxn)

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
CREATE INDEX ON table USING ivfflat (column vector_l2_ops); -- default if no opclass specified
```

Inner product

```sql
CREATE INDEX ON table USING ivfflat (column vector_ip_ops);
```

Cosine distance

```sql
CREATE INDEX ON table USING ivfflat (column vector_cosine_ops);
```

Indexes should be created after the table has data for optimal clustering. If the distribution of data changes significantly, you can reindex without downtime:

```sql
-- Postgres 12+
REINDEX INDEX CONCURRENTLY index_name;

-- Postgres < 12 (change opclass as needed)
CREATE INDEX CONCURRENTLY temp_name ON table USING ivfflat (column vector_l2_ops);
DROP INDEX CONCURRENTLY index_name;
ALTER INDEX temp_name RENAME TO index_name;
```

Also, unlike typical indexes which only affect performance, you may see different results for queries after adding an approximate index.

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

### Partial Indexes

Consider [partial indexes](https://www.postgresql.org/docs/current/indexes-partial.html) for queries with a `WHERE` clause

```sql
CREATE INDEX ON table USING ivfflat (column) WHERE (other_column = 123);
```

To index many different values of `other_column`, consider [partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html) on `other_column`.

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

## Libraries

Libraries that use pgvector:

- [pgvector-python](https://github.com/ankane/pgvector-python) (Python)
- [Neighbor](https://github.com/ankane/neighbor) (Ruby)
- [pgvector-node](https://github.com/ankane/pgvector-node) (Node.js)
- [pgvector-go](https://github.com/ankane/pgvector-go) (Go)
- [pgvector-rust](https://github.com/ankane/pgvector-rust) (Rust)
- [pgvector-cpp](https://github.com/ankane/pgvector-cpp) (C++)

## Additional Installation Methods

### Docker

Get the [Docker image](https://hub.docker.com/repository/docker/ankane/pgvector) with:

```sh
docker pull ankane/pgvector
```

This adds pgvector to the [Postgres image](https://hub.docker.com/_/postgres).

You can also build the image manually

```sh
git clone --branch v0.2.0 https://github.com/ankane/pgvector.git
cd pgvector
docker build -t pgvector .
```

### Homebrew

On Mac with Homebrew Postgres, you can use:

```sh
brew install ankane/brew/pgvector
```

### PGXN

Install from the [PostgreSQL Extension Network](https://pgxn.org/dist/vector) with:

```sh
pgxn install vector
```

## Hosted Postgres

Some Postgres providers only support specific extensions. To request a new extension:

- Amazon RDS - follow the instructions on [this page](https://aws.amazon.com/rds/postgresql/faqs/)
- Google Cloud SQL - follow the instructions on [this page](https://cloud.google.com/sql/docs/postgres/extensions#requesting-support-for-a-new-extension)
- DigitalOcean Managed Databases - follow the instructions on [this page](https://docs.digitalocean.com/products/databases/postgresql/resources/supported-extensions/#supported-extensions)
- Azure Database for PostgreSQL - follow the instructions on [this page](https://docs.microsoft.com/en-us/azure/postgresql/concepts-extensions#next-steps)

## Upgrading

Install the latest version and run:

```sql
ALTER EXTENSION vector UPDATE;
```

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
make installcheck REGRESS=functions                    # regression test
make prove_installcheck PROVE_TESTS=test/t/001_wal.pl  # TAP test
```

Resources for contributors

- [Extension Building Infrastructure](https://www.postgresql.org/docs/current/extend-pgxs.html)
- [Index Access Method Interface Definition](https://www.postgresql.org/docs/current/indexam.html)
- [Generic WAL Records](https://www.postgresql.org/docs/13/generic-wal.html)
