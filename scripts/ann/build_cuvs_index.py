#!/usr/bin/env python3
"""Build an immutable, sharded cuVS CAGRA index directory from a PostgreSQL table.

    python3 scripts/ann/build_cuvs_index.py --dsn 'dbname=app' --table public.docs \\
        --key id --vector embedding --out /data/ann/docs --shards 8 --dtype float16

The directory holds a text manifest, keys.i64 (sorted int64 keys), vectors.f32
(FP32 vectors in key order, used for exact routes and reranking) and one
shard-N.cagra file per contiguous key range. Shard N is built on GPU N modulo
the number of visible devices, in parallel. Load it from SQL with
ann_index_load(name, path); see docs/ann-cuvs-index.md.

This is offline tooling: Python, NumPy, psycopg, CuPy and the cuVS Python
package are needed here only, never on the query path. --no-cagra writes the
manifest, keys and vectors only, for CPU-only testing.
"""

import argparse
import concurrent.futures
import os
import struct
import sys
import time

import numpy as np


def log(message):
    print(f'[{time.strftime("%H:%M:%S")}] {message}', file=sys.stderr, flush=True)


def export(dsn, table, key, vector, limit=None):
    """Stream (key, vector) in key order with binary COPY; returns keys and FP32 matrix."""
    import psycopg
    from psycopg import sql

    query = sql.SQL('SELECT {key}::bigint, {vector} FROM {table} WHERE {vector} IS NOT NULL ORDER BY 1').format(
        key=sql.Identifier(key), vector=sql.Identifier(vector),
        table=sql.Identifier(*table.split('.')))
    if limit:
        query = sql.SQL('{} LIMIT {}').format(query, sql.Literal(limit))
    keys = []
    chunks = []
    dimensions = None
    with psycopg.connect(dsn) as db:
        db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        with db.cursor().copy(sql.SQL('COPY ({}) TO STDOUT (FORMAT binary)').format(query)) as copy:
            buffer = bytearray()
            for data in copy:
                buffer += data
            raw = bytes(buffer)
    # Binary COPY: 19-byte header, then per row int16 fields, int32 length + bytes per field.
    offset = 19 + struct.unpack_from('>I', raw, 15)[0]
    rows = []
    while True:
        fields = struct.unpack_from('>h', raw, offset)[0]
        offset += 2
        if fields == -1:
            break
        length = struct.unpack_from('>i', raw, offset)[0]
        keys.append(struct.unpack_from('>q', raw, offset + 4)[0])
        offset += 4 + length
        length = struct.unpack_from('>i', raw, offset)[0]
        # pgvector binary: int16 dim, int16 unused, dim big-endian float4
        dim = struct.unpack_from('>H', raw, offset + 4)[0]
        if dimensions is None:
            dimensions = dim
        elif dim != dimensions:
            raise SystemExit(f'vector dimensions differ: {dim} and {dimensions}')
        rows.append(raw[offset + 8:offset + 8 + 4 * dim])
        offset += 4 + length
        if len(rows) >= 1 << 16:
            chunks.append(np.frombuffer(b''.join(rows), dtype='>f4').reshape(-1, dimensions))
            rows = []
    if rows:
        chunks.append(np.frombuffer(b''.join(rows), dtype='>f4').reshape(-1, dimensions))
    if not keys:
        raise SystemExit('table has no non-null vectors')
    keys = np.asarray(keys, dtype=np.int64)
    if np.any(keys[1:] <= keys[:-1]):
        raise SystemExit(f'key column {key} must be unique and non-null')
    return keys, np.ascontiguousarray(np.vstack(chunks).astype('<f4'))


def build_shard(index, first, vectors, out, dtype, graph_degree, intermediate_degree, devices):
    import cupy
    from cuvs.neighbors import cagra

    device = index % devices
    with cupy.cuda.Device(device):
        started = time.time()
        data = cupy.asarray(vectors, dtype=cupy.float16 if dtype == 'float16' else cupy.float32)
        params = cagra.IndexParams(metric='sqeuclidean', graph_degree=graph_degree,
                                   intermediate_graph_degree=intermediate_degree)
        built = cagra.build(params, data)
        cagra.save(os.path.join(out, f'shard-{index}.cagra'), built, include_dataset=True)
        cupy.cuda.Device(device).synchronize()
        log(f'shard {index}: {len(vectors)} rows on GPU {device} in {time.time() - started:.1f}s')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--table', required=True, help='schema-qualified table, e.g. public.docs')
    parser.add_argument('--key', default='id', help='unique integer key column')
    parser.add_argument('--vector', default='embedding', help='vector column')
    parser.add_argument('--out', required=True, help='new or empty output directory')
    parser.add_argument('--shards', type=int, default=0, help='default: number of visible GPUs')
    parser.add_argument('--dtype', choices=['float32', 'float16'], default='float32',
                        help='CAGRA dataset precision; float16 halves GPU memory traffic')
    parser.add_argument('--graph-degree', type=int, default=32)
    parser.add_argument('--intermediate-graph-degree', type=int, default=64)
    parser.add_argument('--limit', type=int, help='export at most this many rows (testing)')
    parser.add_argument('--no-cagra', action='store_true', help='write keys/vectors only (CPU tests)')
    args = parser.parse_args()

    if os.path.exists(args.out) and os.listdir(args.out):
        raise SystemExit(f'{args.out} is not empty; index directories are immutable')
    os.makedirs(args.out, exist_ok=True)
    started = time.time()
    keys, vectors = export(args.dsn, args.table, args.key, args.vector, args.limit)
    rows, dimensions = vectors.shape
    log(f'exported {rows} rows x {dimensions} dims in {time.time() - started:.1f}s')
    keys.astype('<i8').tofile(os.path.join(args.out, 'keys.i64'))
    vectors.tofile(os.path.join(args.out, 'vectors.f32'))

    shards = []
    if not args.no_cagra:
        import cupy
        devices = cupy.cuda.runtime.getDeviceCount()
        count = args.shards or devices
        bounds = [rows * i // count for i in range(count + 1)]
        shards = [(i, bounds[i], bounds[i + 1] - bounds[i]) for i in range(count) if bounds[i + 1] > bounds[i]]
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(shards), devices)) as pool:
            futures = [pool.submit(build_shard, i, first, vectors[first:first + n], args.out, args.dtype,
                                   args.graph_degree, args.intermediate_graph_degree, devices)
                       for i, first, n in shards]
            for future in futures:
                future.result()

    # The manifest is written last, so a partial build is never loadable.
    lines = ['pgvector-ann-index 1', f'dimensions {dimensions}', f'rows {rows}', 'metric l2',
             f'dtype {args.dtype}', f'source {args.table}', f'shards {len(shards)}']
    lines += [f'shard {i} {first} {n} shard-{i}.cagra' for i, first, n in shards]
    with open(os.path.join(args.out, 'manifest.tmp'), 'w') as manifest:
        manifest.write('\n'.join(lines) + '\n')
    os.replace(os.path.join(args.out, 'manifest.tmp'), os.path.join(args.out, 'manifest'))
    log(f'wrote {args.out} ({len(shards)} shards) in {time.time() - started:.1f}s')


if __name__ == '__main__':
    main()
