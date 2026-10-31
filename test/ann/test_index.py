"""Loaded-index tests: file format, sharded tree merge, filters, and SQL functions.

Worker tests need only the CPU build. SQL tests also need ANN_PG_DSN for a
superuser on this host (the server connects to the worker's socket); ANN_PSQL
overrides psql. On a cuVS host, ANN_INDEX_WORKER=ann-worker-cuvs plus
ANN_INDEX_BACKEND=cuvs runs the same checks against CAGRA files built by
scripts/ann/build_cuvs_index.py (see test/ann/test_cuvs_index.py).
"""

import math
import os
import random
import struct
import subprocess
import sys
import tempfile
import unittest
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_server import ROOT, STATS, Server, frame, receive  # noqa: E402

LOAD = 11
SEARCH = 12
DSN = os.environ.get('ANN_PG_DSN')
PSQL = os.environ.get('ANN_PSQL', 'psql')


def f32(value):
    return struct.unpack('<f', struct.pack('<f', value))[0]


def write_index(directory, keys, vectors, shards=1, source='-', dtype='float32'):
    """Test fixture writer for the documented on-disk format (CPU route only)."""
    order = sorted(range(len(keys)), key=lambda i: keys[i])
    keys = [keys[i] for i in order]
    vectors = [vectors[i] for i in order]
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, 'keys.i64'), 'wb') as out:
        out.write(struct.pack('<%dq' % len(keys), *keys))
    with open(os.path.join(directory, 'vectors.f32'), 'wb') as out:
        for row in vectors:
            out.write(struct.pack('<%df' % len(row), *row))
    bounds = [len(keys) * i // shards for i in range(shards + 1)]
    lines = ['pgvector-ann-index 1', f'dimensions {len(vectors[0])}', f'rows {len(keys)}',
             'metric l2', f'dtype {dtype}', f'source {source}', f'shards {shards}']
    lines += [f'shard {i} {bounds[i]} {bounds[i + 1] - bounds[i]} shard-{i}.cagra' for i in range(shards)]
    with open(os.path.join(directory, 'manifest'), 'w') as out:
        out.write('\n'.join(lines) + '\n')
    return keys, vectors


def load_frame(request, name, directory):
    payload = struct.pack('<I', len(name)) + name.encode() + directory.encode()
    return frame(LOAD, request, payload)


def search_frame(request, name, query, k, keys=None):
    payload = struct.pack('<I', len(name)) + name.encode()
    payload += struct.pack('<III', len(query), k, 0xFFFFFFFF if keys is None else len(keys))
    payload += struct.pack('<%df' % len(query), *query)
    if keys:
        payload += struct.pack('<%dq' % len(keys), *keys)
    return frame(SEARCH, request, payload)


def decode_hits(payload):
    count, route = struct.unpack_from('<II', payload)
    return route, [struct.unpack_from('<qf', payload, 8 + i * 12) for i in range(count)]


def exact(keys, vectors, query, k, allowed=None):
    scored = sorted((math.sqrt(sum((f32(a) - f32(b)) ** 2 for a, b in zip(row, query))), key)
                    for key, row in zip(keys, vectors) if allowed is None or key in allowed)
    return scored[:k]


class IndexWorkerTests(unittest.TestCase):
    worker = os.path.join(ROOT, 'ann', os.environ.get('ANN_INDEX_WORKER', 'ann-worker'))
    backend = os.environ.get('ANN_INDEX_BACKEND', 'cpu-exact')

    @classmethod
    def setUpClass(cls):
        subprocess.run(['make', '-C', os.path.join(ROOT, 'ann'), 'ann-worker'], check=True,
                       stdout=subprocess.DEVNULL)

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.server = Server('--backend', self.backend, executable=self.worker)

    def tearDown(self):
        self.server.close()
        self.directory.cleanup()

    def fixture(self, name='docs', rows=600, dimensions=5, shards=4, seed=3):
        rng = random.Random(seed)
        keys = rng.sample(range(-10 ** 9, 10 ** 9), rows)
        vectors = [[round(rng.uniform(-3, 3), 3) for _ in range(dimensions)] for _ in range(rows)]
        path = os.path.join(self.directory.name, name)
        keys, vectors = write_index(path, keys, vectors, shards=shards)
        return path, keys, vectors

    def call(self, connection, data):
        connection.sendall(data)
        return receive(connection)

    def test_sharded_tree_merge_matches_exact(self):
        path, keys, vectors = self.fixture(shards=5)
        rng = random.Random(9)
        with self.server.connect() as connection:
            _, _, status, info = self.call(connection, load_frame(1, 'docs', path))
            self.assertEqual(status, 0, info)
            self.assertTrue(info.startswith(b'rows 600 dimensions 5 shards 5 dtype float32'))
            for request in range(2, 30):
                query = [rng.uniform(-3, 3) for _ in range(5)]
                k = rng.choice([1, 3, 10, 50, 700])
                _, _, status, payload = self.call(connection, search_frame(request, 'docs', query, k))
                self.assertEqual(status, 0)
                route, hits = decode_hits(payload)
                want = exact(keys, vectors, query, k)
                self.assertEqual([key for key, _ in hits], [key for _, key in want])
                for (_, distance), (expected, _) in zip(hits, want):
                    self.assertAlmostEqual(distance, expected, places=4)

    def test_filters_restrict_results(self):
        path, keys, vectors = self.fixture()
        rng = random.Random(4)
        with self.server.connect() as connection:
            self.assertEqual(self.call(connection, load_frame(1, 'docs', path))[2], 0)
            for request in range(2, 20):
                allowed = rng.sample(keys, rng.randint(1, 80))
                # Unknown keys and duplicates are ignored; order does not matter.
                sent = allowed + allowed[:3] + [10 ** 12 + request]
                rng.shuffle(sent)
                query = [rng.uniform(-3, 3) for _ in range(5)]
                _, _, status, payload = self.call(connection, search_frame(request, 'docs', query, 10, sent))
                self.assertEqual(status, 0)
                _, hits = decode_hits(payload)
                want = exact(keys, vectors, query, 10, set(allowed))
                self.assertEqual([key for key, _ in hits], [key for _, key in want])

    def test_errors(self):
        path, _, _ = self.fixture()
        with self.server.connect() as connection:
            self.assertEqual(self.call(connection, search_frame(1, 'docs', [0] * 5, 3))[2], 3)
            self.assertEqual(self.call(connection, load_frame(2, 'docs', path))[2], 0)
            self.assertEqual(self.call(connection, load_frame(3, 'docs', path))[2], 3)
            self.assertEqual(self.call(connection, load_frame(4, 'other', path + '-missing'))[2], 1)
            self.assertEqual(self.call(connection, search_frame(5, 'docs', [0] * 4, 3))[2], 1)
            self.assertEqual(self.call(connection, search_frame(6, 'docs', [0] * 5, 0))[2], 1)
            # The connection survives errors and still serves stats.
            self.assertEqual(self.call(connection, frame(STATS, 7))[2], 0)

    def test_corrupt_files_are_rejected(self):
        path, _, _ = self.fixture()
        with open(os.path.join(path, 'keys.i64'), 'r+b') as out:
            out.truncate(8)
        with self.server.connect() as connection:
            self.assertEqual(self.call(connection, load_frame(1, 'docs', path))[2], 1)


@unittest.skipUnless(DSN, 'set ANN_PG_DSN for live PostgreSQL tests')
class IndexSqlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(['make', '-C', os.path.join(ROOT, 'ann'), 'ann-worker'], check=True,
                       stdout=subprocess.DEVNULL)
        cls.directory = tempfile.TemporaryDirectory()
        cls.server = Server('--backend', os.environ.get('ANN_INDEX_BACKEND', 'cpu-exact'),
                            executable=IndexWorkerTests.worker)
        cls.table = 'ann_index_' + uuid.uuid4().hex[:12]
        cls.psql(f"""
            CREATE TABLE {cls.table} (id bigint PRIMARY KEY, tenant int, emb vector(4));
            SELECT setseed(0.5);
            INSERT INTO {cls.table} SELECT i * 7, i % 9,
                (SELECT array_agg((random() * 4)::real) FROM generate_series(1, 4)
                 WHERE i > 0)::vector FROM generate_series(1, 2000) i;
        """)
        rows = cls.psql(f"COPY (SELECT id, emb FROM {cls.table}) TO STDOUT").split('\n')
        keys, vectors = [], []
        for line in filter(None, rows):
            key, vector = line.split('\t')
            keys.append(int(key))
            vectors.append([float(x) for x in vector.strip('[]').split(',')])
        cls.path = os.path.join(cls.directory.name, 'index')
        write_index(cls.path, keys, vectors, shards=3, source=f'public.{cls.table}')
        cls.name = cls.table
        cls.psql(f"SELECT ann_index_load('{cls.name}', '{cls.path}')")

    @classmethod
    def tearDownClass(cls):
        cls.psql(f'DROP TABLE {cls.table}')
        cls.server.close()
        cls.directory.cleanup()

    @classmethod
    def psql(cls, script, check=True):
        result = subprocess.run([PSQL, DSN, '-X', '-q', '-At', '-v', 'ON_ERROR_STOP=1'],
                                input=f"SET ann.worker_socket = '{cls.server.path}';\n" + script,
                                capture_output=True, text=True, timeout=120)
        if check and result.returncode != 0:
            raise AssertionError(result.stderr)
        return result.stdout if check else result.stdout + result.stderr

    def test_search_matches_exact_sql(self):
        output = self.psql(f"""
            SELECT count(*) FILTER (WHERE a IS DISTINCT FROM b) FROM (
                SELECT q.id,
                    ARRAY(SELECT s.id FROM ann_index_search('{self.name}', q.emb, 10) s ORDER BY s.distance, s.id) a,
                    ARRAY(SELECT t.id FROM {self.table} t ORDER BY t.emb <-> q.emb, t.id LIMIT 10) b
                FROM {self.table} q WHERE q.id % 97 = 0) x
        """)
        self.assertEqual(output.strip(), '0')

    def test_hybrid_filter_matches_exact_sql(self):
        output = self.psql(f"""
            SELECT count(*) FILTER (WHERE a IS DISTINCT FROM b) FROM (
                SELECT q.id,
                    ARRAY(SELECT s.id FROM ann_index_search('{self.name}', q.emb, 5,
                        ARRAY(SELECT id FROM {self.table} WHERE tenant = q.tenant)) s ORDER BY s.distance, s.id) a,
                    ARRAY(SELECT t.id FROM {self.table} t WHERE t.tenant = q.tenant
                        ORDER BY t.emb <-> q.emb, t.id LIMIT 5) b
                FROM {self.table} q WHERE q.id % 89 = 0) x
        """)
        self.assertEqual(output.strip(), '0')

    def test_join_returns_rows_and_edge_cases(self):
        output = self.psql(f"""
            SELECT count(*) FROM ann_index_search('{self.name}', '[1,1,1,1]', 3) s JOIN {self.table} USING (id);
            SELECT count(*) FROM ann_index_search('{self.name}', '[1,1,1,1]', 3, '{{}}');
            SELECT count(*) FROM ann_index_search('{self.name}', '[1,1,1,1]', 3, ARRAY[NULL, -1]::bigint[]);
            SELECT count(*) FROM ann_index_search('{self.name}', NULL, 3);
        """)
        self.assertEqual(output.split(), ['3', '0', '0', '0'])

    def test_errors(self):
        self.assertIn('is not loaded', self.psql("SELECT * FROM ann_index_search('missing', '[1,1,1,1]', 3)",
                                                 check=False))
        self.assertIn('k must be between', self.psql(
            f"SELECT * FROM ann_index_search('{self.name}', '[1,1,1,1]', 0)", check=False))
        self.assertIn('rejected', self.psql(
            f"SELECT * FROM ann_index_search('{self.name}', '[1,1,1]', 3)", check=False))
        self.assertIn('unavailable', self.psql(
            "SET ann.worker_socket = '/nonexistent/ann.sock'; "
            f"SELECT * FROM ann_index_search('{self.name}', '[1,1,1,1]', 3)", check=False))

    def test_stale_index_is_refused(self):
        self.psql(f"INSERT INTO {self.table} VALUES (-1, 0, '[0,0,0,0]')")
        try:
            output = self.psql(f"SELECT ann_index_load('{self.name}_stale', '{self.path}')", check=False)
            self.assertIn('ann index is stale', output)
        finally:
            self.psql(f'DELETE FROM {self.table} WHERE id = -1')

    def test_load_requires_superuser(self):
        role = 'ann_index_' + uuid.uuid4().hex[:8]
        output = self.psql(f"CREATE ROLE {role}; SET ROLE {role}; "
                           f"SELECT ann_index_load('x', '{self.path}');", check=False)
        self.psql(f'DROP ROLE {role}')
        self.assertIn('permission denied', output)


if __name__ == '__main__':
    unittest.main()
