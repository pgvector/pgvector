#!/usr/bin/env python3
"""Load test for loaded cuVS indexes served through PostgreSQL.

    # 1. synthetic table: id, tenant (0.1% each), category (1% each), emb vector(D)
    python3 benchmarks/ann/cuvs_load.py generate --dsn ... --table bench10m --rows 10000000 --dimensions 128
    # 2. offline index build (one shard per GPU)
    python3 scripts/ann/build_cuvs_index.py --dsn ... --table public.bench10m --vector emb --out /data/bench10m
    # 3. sweep client concurrency with pgbench for ANN and hybrid queries
    python3 benchmarks/ann/cuvs_load.py bench --dsn ... --table bench10m --index bench10m \\
        --path /data/bench10m --socket /tmp/ann.sock --clients 64,128,256,512

The bench step loads the index once, measures recall@10 against exact SQL on
sample queries, then runs pgbench per workload and client count, printing
throughput and latency percentiles. Test tooling only: the query path is C.
"""

import argparse
import concurrent.futures
import os
import re
import subprocess
import sys
import tempfile

import numpy as np

WORKLOADS = {
    'ann': "SELECT id FROM ann_index_search(:idx, :v::vector, 10);",
    # 1% category filter: about 100k eligible keys per query on 10M rows (filtered CAGRA).
    'hybrid-1pct': ("SELECT id FROM ann_index_search(:idx, :v::vector, 10, "
                    "ARRAY(SELECT id FROM {table} WHERE category = :c));"),
    # 0.1% tenant filter: about 10k eligible keys (exact route).
    'hybrid-0.1pct': ("SELECT id FROM ann_index_search(:idx, :v::vector, 10, "
                      "ARRAY(SELECT id FROM {table} WHERE tenant = :t));"),
}


CLUSTERS = 1024
LATENT = 16


def sample(rng, n, dimensions):
    """Embedding-like vectors: clusters in a 16-d latent space, randomly projected, plus noise.

    Real embeddings have low intrinsic dimension; isotropic random vectors in
    128-d have no neighborhood structure and defeat every graph index.
    """
    shared = np.random.default_rng(12345)
    centroids = (shared.normal(size=(CLUSTERS, LATENT)) * 2).astype(np.float32)
    projection = (shared.normal(size=(LATENT, dimensions)) / np.sqrt(LATENT)).astype(np.float32)
    latent = centroids[rng.integers(0, CLUSTERS, n)] + rng.normal(size=(n, LATENT)).astype(np.float32)
    return (latent @ projection + rng.normal(scale=0.05, size=(n, dimensions))).astype(np.float32)


def copy_part(dsn, table, first, rows, dimensions, seed):
    import psycopg

    rng = np.random.default_rng(seed)
    layout = np.dtype([('fields', '>i2'), ('l1', '>i4'), ('id', '>i8'), ('l2', '>i4'), ('tenant', '>i4'),
                       ('l3', '>i4'), ('category', '>i4'), ('l4', '>i4'), ('dim', '>u2'), ('unused', '>u2'),
                       ('vector', '>f4', (dimensions,))])
    with psycopg.connect(dsn) as db:
        with db.cursor().copy(f'COPY {table} (id, tenant, category, emb) FROM STDIN (FORMAT binary)') as copy:
            copy.write(b'PGCOPY\n\xff\r\n\x00' + b'\x00' * 8)
            for start in range(first, first + rows, 100000):
                n = min(100000, first + rows - start)
                batch = np.zeros(n, dtype=layout)
                ids = np.arange(start + 1, start + n + 1)
                batch['fields'] = 4
                batch['l1'] = 8
                batch['id'] = ids
                batch['l2'] = batch['l3'] = 4
                batch['tenant'] = ids % 1000
                batch['category'] = (ids // 7) % 100
                batch['l4'] = 4 + 4 * dimensions
                batch['dim'] = dimensions
                batch['vector'] = sample(rng, n, dimensions)
                copy.write(batch.tobytes())
            copy.write(b'\xff\xff')


def generate(args):
    import psycopg

    with psycopg.connect(args.dsn, autocommit=True) as db:
        db.execute(f'DROP TABLE IF EXISTS {args.table}')
        db.execute(f'CREATE UNLOGGED TABLE {args.table} (id bigint, tenant int, category int, '
                   f'emb vector({args.dimensions}))')
    parts = args.jobs
    bounds = [args.rows * i // parts for i in range(parts + 1)]
    with concurrent.futures.ProcessPoolExecutor(parts) as pool:
        list(pool.map(copy_part, [args.dsn] * parts, [args.table] * parts, bounds[:-1],
                      [b - a for a, b in zip(bounds, bounds[1:])], [args.dimensions] * parts, range(parts)))
    with psycopg.connect(args.dsn, autocommit=True) as db:
        db.execute('SET maintenance_work_mem = \'8GB\'')
        db.execute(f'ALTER TABLE {args.table} ADD PRIMARY KEY (id)')
        # Covering (filter, key) indexes let eligibility arrays come from
        # index-only scans; heap-scattered keys otherwise dominate hybrid cost.
        db.execute(f'CREATE INDEX ON {args.table} (tenant, id)')
        db.execute(f'CREATE INDEX ON {args.table} (category, id)')
        db.execute(f'DROP TABLE IF EXISTS {args.table}_queries')
        # Queries come from the same distribution but are not rows of the table.
        db.execute(f'CREATE TABLE {args.table}_queries (q int PRIMARY KEY, v text)')
        queries = sample(np.random.default_rng(999), 1000, args.dimensions)
        with db.cursor().copy(f'COPY {args.table}_queries FROM STDIN') as copy:
            for i, row in enumerate(queries, 1):
                copy.write_row((i, '[' + ','.join(f'{x:.6g}' for x in row) + ']'))
        db.execute(f'VACUUM ANALYZE {args.table}')
        db.execute(f'ANALYZE {args.table}_queries')
    print(f'generated {args.rows} rows in {args.table}', file=sys.stderr)


def psql(args, sql):
    result = subprocess.run(['psql', args.dsn, '-X', '-qAt', '-v', 'ON_ERROR_STOP=1'],
                            input=f"SET ann.worker_socket = '{args.socket}';\n{sql}",
                            capture_output=True, text=True)
    if result.returncode:
        raise SystemExit(result.stderr)
    return result.stdout.strip()


def recall(args, workload):
    """Mean recall@10 over sample queries against exact SQL."""
    where = {'ann': 'true', 'hybrid-1pct': 't.category = q.c', 'hybrid-0.1pct': 't.tenant = q.t'}[workload]
    fltr = {'ann': '', 'hybrid-1pct': f', ARRAY(SELECT id FROM {args.table} t WHERE {where})',
            'hybrid-0.1pct': f', ARRAY(SELECT id FROM {args.table} t WHERE {where})'}[workload]
    return float(psql(args, f"""
        SET max_parallel_workers_per_gather = 16;
        SELECT round(avg(cardinality(ARRAY(SELECT unnest(a) INTERSECT SELECT unnest(b)))) / 10, 3) FROM (
          SELECT ARRAY(SELECT s.id FROM ann_index_search('{args.index}', q.v, 10{fltr}) s) a,
                 ARRAY(SELECT t.id FROM {args.table} t WHERE {where} ORDER BY t.emb <-> q.v LIMIT 10) b
          FROM (SELECT v::vector AS v, q % 100 AS c, q % 1000 AS t FROM {args.table}_queries
                WHERE q <= {args.recall_queries} OFFSET 0) q) x"""))


def bench(args):
    if args.path:
        print('loaded', psql(args, f"SELECT ann_index_load('{args.index}', '{args.path}')"), 'rows', flush=True)
    workloads = args.workloads.split(',')
    for workload in workloads:
        if not args.no_recall:
            print(f'{workload}: recall@10 {recall(args, workload)}', flush=True)
    with tempfile.TemporaryDirectory() as directory:
        for workload in workloads:
            script = os.path.join(directory, f'{workload}.sql')
            with open(script, 'w') as out:
                out.write('\\set q random(1, 1000)\n\\set c random(0, 99)\n\\set t random(0, 999)\n')
                out.write(f'SELECT v FROM {args.table}_queries WHERE q = :q \\gset\n')
                out.write(WORKLOADS[workload].format(table=args.table) + '\n')
            for clients in [int(c) for c in args.clients.split(',')]:
                env = dict(os.environ, PGOPTIONS=f'-c ann.worker_socket={args.socket}')
                command = ['pgbench', args.dsn, '-n', '-M', 'prepared', '-c', str(clients),
                           '-j', str(min(clients, args.threads)), '-T', str(args.seconds), '-D', f'idx={args.index}',
                           '-f', script, '--log', '--log-prefix', os.path.join(directory, f'{workload}-{clients}')]
                output = subprocess.run(command, capture_output=True, text=True, env=env).stdout
                tps = re.search(r'tps = ([\d.]+)', output)
                failed = re.search(r'number of failed transactions: (\d+)', output)
                latencies = []
                for name in os.listdir(directory):
                    if name.startswith(f'{workload}-{clients}.'):
                        with open(os.path.join(directory, name)) as log:
                            latencies += [int(line.split()[2]) for line in log]
                        os.unlink(os.path.join(directory, name))
                latencies.sort()

                def pct(p):
                    return latencies[min(len(latencies) - 1, int(len(latencies) * p))] / 1000 if latencies else 0
                print(f'{workload:14} clients={clients:<5} qps={float(tps.group(1)) if tps else 0:9.0f} '
                      f'p50={pct(0.5):7.2f}ms p95={pct(0.95):7.2f}ms p99={pct(0.99):7.2f}ms '
                      f'failed={failed.group(1) if failed else "?"}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    g = sub.add_parser('generate')
    g.add_argument('--dsn', required=True)
    g.add_argument('--table', required=True)
    g.add_argument('--rows', type=int, default=10_000_000)
    g.add_argument('--dimensions', type=int, default=128)
    g.add_argument('--jobs', type=int, default=32)
    b = sub.add_parser('bench')
    b.add_argument('--dsn', required=True)
    b.add_argument('--table', required=True)
    b.add_argument('--index', required=True)
    b.add_argument('--path', help='load this index directory first')
    b.add_argument('--socket', required=True)
    b.add_argument('--clients', default='64,128,256,512')
    b.add_argument('--threads', type=int, default=64)
    b.add_argument('--seconds', type=int, default=20)
    b.add_argument('--workloads', default='ann,hybrid-1pct,hybrid-0.1pct')
    b.add_argument('--no-recall', action='store_true')
    b.add_argument('--recall-queries', type=int, default=20)
    args = parser.parse_args()
    generate(args) if args.command == 'generate' else bench(args)


if __name__ == '__main__':
    main()
