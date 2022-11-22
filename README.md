# pgvector

Open-source vector similarity search for Postgres

```sql
CREATE TABLE items (embedding vector(3));
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops);
SELECT * FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 5;
```

Supports L2 distance, inner product, and cosine distance

[![Build Status](https://github.com/pgvector/pgvector/workflows/build/badge.svg?branch=master)](https://github.com/pgvector/pgvector/actions)

## Installation

Compile and install the extension (supports Postgres 10+)

```sh
git clone --branch v0.3.2 https://github.com/pgvector/pgvector.git
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

Create a vector column with 3 dimensions

```sql
CREATE TABLE items (embedding vector(3));
```

Insert values

```sql
INSERT INTO items VALUES ('[1,2,3]'), ('[4,5,6]');
```

Get the nearest neighbor by L2 distance

```sql
SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 1;
```

Also supports inner product (`<#>`) and cosine distance (`<=>`)

Note: `<#>` returns the negative inner product since Postgres only supports `ASC` order index scans on operators

## Indexing

Speed up queries with an approximate index. Add an index for each distance function you want to use.

L2 distance

```sql
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops);
```

Inner product

```sql
CREATE INDEX ON items USING ivfflat (embedding vector_ip_ops);
```

Cosine distance

```sql
CREATE INDEX ON items USING ivfflat (embedding vector_cosine_ops);
```

Indexes should be created after the table has some data for optimal clustering. Also, unlike typical indexes which only affect performance, you may see different results for queries after adding an approximate index.

### Index Options

Specify the number of inverted lists (100 by default)

```sql
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);
```

A [good place to start](https://github.com/facebookresearch/faiss/issues/112) is `4 * sqrt(rows)`

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

### Indexing Progress

Check [indexing progress](https://www.postgresql.org/docs/current/progress-reporting.html#CREATE-INDEX-PROGRESS-REPORTING) with Postgres 12+

```sql
SELECT phase, tuples_done, tuples_total FROM pg_stat_progress_create_index;
```

The phases are:

1. `initializing`
2. `sampling table`
3. `performing k-means`
4. `sorting tuples`
5. `loading tuples`

Note: `tuples_done` and `tuples_total` are only populated during the `loading tuples` phase

### Partial Indexes

Consider [partial indexes](https://www.postgresql.org/docs/current/indexes-partial.html) for queries with a `WHERE` clause

```sql
SELECT * FROM items WHERE category_id = 123 ORDER BY embedding <-> '[3,1,2]' LIMIT 5;
```

can be indexed with:

```sql
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WHERE (category_id = 123);
```

To index many different values of `category_id`, consider [partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html) on `category_id`.

```sql
CREATE TABLE items (embedding vector(3), category_id int) PARTITION BY LIST(category_id);
```

## Performance

To speed up queries without an index, increase `max_parallel_workers_per_gather`.

```sql
SET max_parallel_workers_per_gather = 4;
```

To speed up queries with an index, increase the number of inverted lists (at the expense of recall).

```sql
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 1000);
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

## Libraries

Libraries that use pgvector:

- [pgvector-python](https://github.com/pgvector/pgvector-python) (Python)
- [Neighbor](https://github.com/ankane/neighbor) (Ruby)
- [pgvector-ruby](https://github.com/pgvector/pgvector-ruby) (Ruby)
- [pgvector-node](https://github.com/pgvector/pgvector-node) (Node.js)
- [pgvector-go](https://github.com/pgvector/pgvector-go) (Go)
- [pgvector-php](https://github.com/pgvector/pgvector-php) (PHP)
- [pgvector-rust](https://github.com/pgvector/pgvector-rust) (Rust)
- [pgvector-cpp](https://github.com/pgvector/pgvector-cpp) (C++)
- [pgvector-elixir](https://github.com/pgvector/pgvector-elixir) (Elixir)

## Frequently Asked Questions

#### How many vectors can be stored in a single table?

A non-partitioned table has a limit of 32 TB by default in Postgres. A partitioned table can have thousands of partitions of that size.

#### Is replication supported?

Yes, pgvector uses the write-ahead log (WAL), which allows for replication and point-in-time recovery.

#### What if my data has more than 1024 dimensions?

Two things you can try are:

1. use dimensionality reduction
2. compile Postgres with a larger block size (`./configure --with-blocksize=32`) and edit the limit in `src/vector.h`

## Additional Installation Methods

### Docker

Get the [Docker image](https://hub.docker.com/r/ankane/pgvector) with:

```sh
docker pull ankane/pgvector
```

This adds pgvector to the [Postgres image](https://hub.docker.com/_/postgres).

You can also build the image manually

```sh
git clone --branch v0.3.2 https://github.com/pgvector/pgvector.git
cd pgvector
docker build -t pgvector .
```

### Homebrew

With Homebrew Postgres, you can use:

```sh
brew install pgvector/brew/pgvector
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
- DigitalOcean Managed Databases - vote or comment on [this page](https://ideas.digitalocean.com/app-framework-services/p/pgvector-extension-for-postgresql)
- Azure Database for PostgreSQL - follow the instructions on [this page](https://docs.microsoft.com/en-us/azure/postgresql/concepts-extensions#next-steps)

## Upgrading

Install the latest version and run:

```sql
ALTER EXTENSION vector UPDATE;
```

## Upgrade Notes

### 0.3.1

If upgrading from 0.2.7 or 0.3.0, recreate all `ivfflat` indexes after upgrading to ensure all data is indexed.

```sql
-- Postgres 12+
REINDEX INDEX CONCURRENTLY index_name;

-- Postgres < 12
CREATE INDEX CONCURRENTLY temp_name ON table USING ivfflat (column opclass);
DROP INDEX CONCURRENTLY index_name;
ALTER INDEX temp_name RENAME TO index_name;
```

## Thanks

Thanks to:

- [PASE: PostgreSQL Ultra-High-Dimensional Approximate Nearest Neighbor Search Extension](https://dl.acm.org/doi/pdf/10.1145/3318464.3386131)
- [Faiss: A Library for Efficient Similarity Search and Clustering of Dense Vectors](https://github.com/facebookresearch/faiss)
- [Using the Triangle Inequality to Accelerate k-means](https://www.aaai.org/Papers/ICML/2003/ICML03-022.pdf)
- [k-means++: The Advantage of Careful Seeding](https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf)
- [Concept Decompositions for Large Sparse Text Data using Clustering](https://www.cs.utexas.edu/users/inderjit/public_papers/concept_mlj.pdf)

## History

View the [changelog](https://github.com/pgvector/pgvector/blob/master/CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/pgvector/pgvector/issues)
- Fix bugs and [submit pull requests](https://github.com/pgvector/pgvector/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/pgvector/pgvector.git
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

To enable benchmarking:

```sh
make clean && PG_CFLAGS=-DIVFFLAT_BENCH make && make install
```

Resources for contributors

- [Extension Building Infrastructure](https://www.postgresql.org/docs/current/extend-pgxs.html)
- [Index Access Method Interface Definition](https://www.postgresql.org/docs/current/indexam.html)
- [Generic WAL Records](https://www.postgresql.org/docs/13/generic-wal.html)
