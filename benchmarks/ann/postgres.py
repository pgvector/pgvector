"""Emit a PostgreSQL baseline fixture; execute with psql -X -v ON_ERROR_STOP=1."""

import argparse
import random


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=10000)
    parser.add_argument('--dimensions', type=int, default=32)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--k', type=int, default=20)
    parser.add_argument('--filtered-native', action='store_true',
                        help='also measure the opt-in native exact filtered path')
    args = parser.parse_args()
    if not 1 <= args.dimensions <= 2000 or args.rows <= 0 or args.k <= 0:
        parser.error('rows/k must be positive and dimensions must be in 1..2000')
    rng = random.Random(args.seed)
    def vector():
        return '[' + ','.join(format(rng.random(), '.9g') for _ in range(args.dimensions)) + ']'
    print('BEGIN;')
    print('CREATE EXTENSION IF NOT EXISTS vector;')
    if args.filtered_native:
        print("LOAD 'vector';")
    print('SET LOCAL gin_fuzzy_search_limit = 0;')
    print(f'CREATE TEMP TABLE ann_bench (id integer PRIMARY KEY, tenant text, '
          f'tags text[], search_text tsvector, embedding vector({args.dimensions}));')
    print('COPY ann_bench (id, tenant, tags, search_text, embedding) FROM STDIN;')
    for i in range(args.rows):
        term = rng.choice(['database', 'query', 'storage', 'network'])
        tenant = rng.randrange(16)
        print(f"{i}\ttenant{tenant}\t{{{term}}}\t'{term}':1\t{vector()}")
    print('\\.')
    print('CREATE INDEX ON ann_bench (tenant);')
    print('CREATE INDEX ON ann_bench USING gin (tags);')
    print('CREATE INDEX ON ann_bench USING gin (search_text);')
    print('CREATE INDEX ON ann_bench USING hnsw (embedding vector_l2_ops);')
    if args.filtered_native:
        print('CREATE INDEX ON ann_bench USING ann (embedding vector_l2_ops);')
    print('ANALYZE ann_bench;')
    print('SET LOCAL hnsw.iterative_scan = strict_order;')
    query = vector()
    predicates = {
        'metadata': "tenant = 'tenant0' AND tags @> ARRAY['database']",
        'text': "search_text @@ to_tsquery('simple', 'database')",
        'combined': "tenant = 'tenant0' AND search_text @@ to_tsquery('simple', 'database')",
    }
    for label, predicate in predicates.items():
        print(f"SELECT '{label}: eligibility';")
        print(f'EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM ann_bench WHERE {predicate};')
        print(f"SELECT '{label}: exact filtered reference';")
        exact = (f'WITH eligible AS MATERIALIZED (SELECT id, embedding FROM ann_bench '
                 f'WHERE {predicate}) SELECT id, embedding <-> \'{query}\' AS distance '
                 f'FROM eligible ORDER BY distance, id LIMIT {args.k};')
        print('EXPLAIN (ANALYZE, BUFFERS) ' + exact)
        print(exact)
        print(f"SELECT '{label}: native planner choice with iterative HNSW available';")
        ann = (f'SELECT id, embedding <-> \'{query}\' AS distance FROM ann_bench '
               f'WHERE {predicate} ORDER BY distance LIMIT {args.k};')
        print('EXPLAIN (ANALYZE, BUFFERS) ' + ann)
        print(ann)
        if args.filtered_native:
            print(f"SELECT '{label}: planner choice with native filtered exact path enabled';")
            print('SET LOCAL ann.enable_filtered_search = on;')
            filtered = (f'SELECT id, embedding <-> \'{query}\' AS distance FROM ann_bench '
                        f'WHERE {predicate} ORDER BY distance, id LIMIT {args.k};')
            print('EXPLAIN (ANALYZE, BUFFERS) ' + filtered)
            print(filtered)
            print('SET LOCAL ann.enable_filtered_search = off;')
    print('ROLLBACK;')


if __name__ == '__main__':
    main()
