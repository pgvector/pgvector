"""Reproducible single-client baseline; this does not measure PostgreSQL or GPU QPS."""

import argparse
import json
import math
from pathlib import Path
import random
import struct
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'test/ann'))
from support.client import Worker


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=10000)
    parser.add_argument('--dimensions', type=int, default=32)
    parser.add_argument('--batch', type=int, default=16)
    parser.add_argument('--iterations', type=int, default=20)
    parser.add_argument('--survivor-fraction', type=float, default=.01)
    parser.add_argument('--mode', choices=['shared', 'unique'], default='shared')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--backend', choices=['cpu-exact', 'cuvs'], default='cpu-exact')
    parser.add_argument('--worker', type=Path)
    parser.add_argument('--min-recall', type=float, default=.9)
    args = parser.parse_args()
    if min(args.rows, args.dimensions, args.batch, args.iterations) <= 0:
        parser.error('sizes must be positive')
    if not 0 <= args.survivor_fraction <= 1:
        parser.error('survivor fraction must be between zero and one')
    if not 0 <= args.min_recall <= 1:
        parser.error('minimum recall must be between zero and one')
    rng = random.Random(args.seed)
    def vector():
        return [struct.unpack('<f', struct.pack('<f', rng.random()))[0]
                for _ in range(args.dimensions)]
    vectors = [vector() for _ in range(args.rows)]
    queries = [vector() for _ in range(args.batch)]
    passing = int(args.rows * args.survivor_fraction)
    filters = [sorted(rng.sample(range(args.rows), passing))
               for _ in range(1 if args.mode == 'shared' else args.batch)]
    allowed = filters[0] if args.mode == 'shared' else filters
    latencies = []
    executable = args.worker
    if executable is None and args.backend == 'cuvs':
        executable = Path(__file__).resolve().parents[2] / 'ann/ann-worker-cuvs'
    recalls = []
    with Worker(executable, args.backend) as worker:
        caps = worker.capabilities()
        start = time.perf_counter()
        worker.build(1, vectors)
        build_seconds = time.perf_counter() - start
        result = worker.search(1, queries, 10, allowed=allowed, per_query=args.mode == 'unique')
        # Independent exact oracle outside the timed interval.
        for i, query in enumerate(queries):
            ids = filters[0] if args.mode == 'shared' else filters[i]
            ranked = sorted(ids, key=lambda j: (
                sum((a - b) ** 2 for a, b in zip(vectors[j], query)), j))[:10]
            if caps['exact'] and [r[0] for r in result[i]] != ranked:
                raise RuntimeError('reference validation failed')
            if any(r[0] not in ids for r in result[i]):
                raise RuntimeError('ineligible result')
            recall = len(set(ranked) & {r[0] for r in result[i]}) / len(ranked) if ranked else 1
            recalls.append(recall)
            if recall < args.min_recall:
                raise RuntimeError(f'initial batch recall {recall} below required {args.min_recall}')
        start = time.perf_counter()
        for _ in range(args.iterations):
            began = time.perf_counter()
            worker.search(1, queries, 10, allowed=allowed, per_query=args.mode == 'unique')
            latencies.append(time.perf_counter() - began)
        elapsed = time.perf_counter() - start
    latencies.sort()
    options = vars(args).copy()
    options['worker'] = str(executable) if executable else None
    report = options | {
        'scope': 'single-client worker; precomputed synthetic eligibility',
        'capabilities': caps, 'oracle_validated': True,
        'initial_batch_min_recall_at_k': min(recalls),
        'quality_scope': 'initial batch only',
        'build_seconds': build_seconds, 'elapsed_seconds': elapsed,
        'batch_requests_per_second': args.iterations / elapsed,
        'vector_searches_per_second': args.iterations * args.batch / elapsed,
        'batch_p50_ms': latencies[math.ceil(.5 * len(latencies)) - 1] * 1000,
        'batch_p99_ms': latencies[math.ceil(.99 * len(latencies)) - 1] * 1000,
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
