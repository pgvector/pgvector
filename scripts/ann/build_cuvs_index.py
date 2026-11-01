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

--attribute COLUMN (repeatable) also writes attr-COLUMN.i32, one int32 per
row in key order, so the worker can resolve "COLUMN IN (values)" filters
itself (ann_attribute_search) instead of receiving key lists. NULL is stored
as -2147483648, which therefore never matches.

--quantize nvfp4|fp8 also writes a quantized copy of the vectors for the
worker's Blackwell module, which scores filtered rows from it on compute
capability 10.x GPUs and re-scores candidates from vectors.f32:
  nvfp4  vectors.nvfp4 (two E2M1 per byte, even dimension in the low
         nibble), scales.e4m3 (one E4M3 scale per 16 dimensions) and
         rowscale.f32 (one FP32 scale per row): value = e2m1 * scale * row
  fp8    vectors.e4m3 (dims rounded up to 16, zero padded) and rowscale.f32
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


def export_attribute(dsn, table, key, vector, column, keys):
    """One int32 per row of the index, in key order; NULL becomes INT32_MIN."""
    import psycopg
    from psycopg import sql

    query = sql.SQL('SELECT {key}::bigint, coalesce({column}::integer, -2147483648) FROM {table} '
                    'WHERE {vector} IS NOT NULL ORDER BY 1').format(
        key=sql.Identifier(key), column=sql.Identifier(column), vector=sql.Identifier(vector),
        table=sql.Identifier(*table.split('.')))
    with psycopg.connect(dsn) as db:
        rows = db.execute(query).fetchall()
    if len(rows) != len(keys) or any(row[0] != k for row, k in zip(rows, keys.tolist())):
        raise SystemExit(f'table changed while exporting attribute {column}')
    return np.asarray([row[1] for row in rows], dtype='<i4')


def _e4m3_table(xp):
    """Non-negative E4M3 values for codes 0..126 (127 is NaN), increasing."""
    codes = np.arange(127)
    exponent, mantissa = codes >> 3, codes & 7
    values = np.where(exponent == 0, mantissa / 8 * 2.0 ** -6, (1 + mantissa / 8) * 2.0 ** (exponent - 7))
    return xp.asarray(values, dtype=xp.float32)


E2M1 = [0.0, 0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0]


def _nearest(xp, table, x):
    """Index of the nearest table value to each element of x (table increasing)."""
    index = xp.clip(xp.searchsorted(table, x), 1, table.size - 1)
    below, above = table[index - 1], table[index]
    return xp.where(x - below <= above - x, index - 1, index)


def quantize_fp8(vectors, xp=np):
    """E4M3 codes (rows x dims rounded up to 16, for 16-byte loads) and FP32 row scales."""
    v = xp.asarray(vectors, dtype=xp.float32)
    if v.shape[1] % 16:
        v = xp.concatenate([v, xp.zeros((v.shape[0], 16 - v.shape[1] % 16), dtype=xp.float32)], axis=1)
    amax = xp.abs(v).max(axis=1)
    scale = xp.where(amax > 0, amax / 448, 1).astype(xp.float32)
    x = v / scale[:, None]
    codes = _nearest(xp, _e4m3_table(xp), xp.abs(x)).astype(xp.uint8) | ((x < 0).astype(xp.uint8) << 7)
    return codes, scale


def quantize_nvfp4(vectors, xp=np):
    """Packed E2M1 (rows x blocks x 8 bytes), E4M3 block scales and FP32 row scales.

    Two-level NVFP4 scaling: each 16-dimension block's E4M3 scale is rounded
    up so no value clips, and the row scale keeps block scales within E4M3.
    """
    v = xp.asarray(vectors, dtype=xp.float32)
    rows, dims = v.shape
    blocks = (dims + 15) // 16
    if blocks * 16 != dims:
        v = xp.concatenate([v, xp.zeros((rows, blocks * 16 - dims), dtype=xp.float32)], axis=1)
    v = v.reshape(rows, blocks, 16)
    table = _e4m3_table(xp)
    amax = xp.abs(v).max(axis=(1, 2))
    row = xp.where(amax > 0, amax / (6 * 448), 1).astype(xp.float32)
    target = xp.abs(v).max(axis=2) / (6 * row[:, None])
    scale_codes = xp.clip(xp.searchsorted(table, target), 0, 126)
    scale = table[scale_codes] * row[:, None]
    x = xp.where(scale[:, :, None] > 0, v / xp.where(scale > 0, scale, 1)[:, :, None], 0)
    nibbles = _nearest(xp, xp.asarray(E2M1, dtype=xp.float32), xp.minimum(xp.abs(x), 6)).astype(xp.uint8)
    nibbles |= (x < 0).astype(xp.uint8) << 3
    packed = (nibbles[:, :, 0::2] | (nibbles[:, :, 1::2] << 4)).reshape(rows, blocks * 8)
    return packed, scale_codes.astype(xp.uint8), row


def write_quantized(directory, vectors, kind, chunk=1 << 18):
    """Write the --quantize files for vectors (FP32, key order), chunk by chunk."""
    try:
        import cupy as xp
    except ImportError:
        xp = np
    names = {'nvfp4': ['vectors.nvfp4', 'scales.e4m3', 'rowscale.f32'], 'fp8': ['vectors.e4m3', 'rowscale.f32']}[kind]
    outputs = [open(os.path.join(directory, name), 'wb') for name in names]
    try:
        for first in range(0, len(vectors), chunk):
            part = (quantize_nvfp4 if kind == 'nvfp4' else quantize_fp8)(vectors[first:first + chunk], xp)
            for out, array in zip(outputs, part):
                array = array.get() if hasattr(array, 'get') else array
                out.write(np.ascontiguousarray(array).tobytes())
    finally:
        for out in outputs:
            out.close()


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
    parser.add_argument('--attribute', action='append', default=[],
                        help='integer column the worker can filter on (repeatable)')
    parser.add_argument('--quantize', choices=['nvfp4', 'fp8'],
                        help='also write a quantized copy for the worker\'s Blackwell scans')
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
    if args.quantize:
        write_quantized(args.out, vectors, args.quantize)
        log(f'wrote the {args.quantize} copy')
    for column in args.attribute:
        export_attribute(args.dsn, args.table, args.key, args.vector, column, keys).tofile(
            os.path.join(args.out, f'attr-{column}.i32'))
        log(f'exported attribute {column}')

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
             f'dtype {args.dtype}', f'source {args.table}', f'key {args.key}', f'vector {args.vector}',
             f'shards {len(shards)}']
    lines += [f'shard {i} {first} {n} shard-{i}.cagra' for i, first, n in shards]
    lines += [f'attribute {column} attr-{column}.i32' for column in args.attribute]
    if args.quantize:
        lines.append(f'quant {args.quantize}')
    with open(os.path.join(args.out, 'manifest.tmp'), 'w') as manifest:
        manifest.write('\n'.join(lines) + '\n')
    os.replace(os.path.join(args.out, 'manifest.tmp'), os.path.join(args.out, 'manifest'))
    log(f'wrote {args.out} ({len(shards)} shards) in {time.time() - started:.1f}s')


if __name__ == '__main__':
    main()
