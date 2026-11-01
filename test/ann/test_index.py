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


def write_index(directory, keys, vectors, shards=1, source='-', dtype='float32', key='id', vector='emb',
                attributes=None):
    """Test fixture writer for the documented on-disk format (CPU route only).

    attributes maps a column name to one int32 per row, in the order of keys.
    """
    order = sorted(range(len(keys)), key=lambda i: keys[i])
    keys = [keys[i] for i in order]
    vectors = [vectors[i] for i in order]
    attributes = attributes or {}
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, 'keys.i64'), 'wb') as out:
        out.write(struct.pack('<%dq' % len(keys), *keys))
    with open(os.path.join(directory, 'vectors.f32'), 'wb') as out:
        for row in vectors:
            out.write(struct.pack('<%df' % len(row), *row))
    bounds = [len(keys) * i // shards for i in range(shards + 1)]
    lines = ['pgvector-ann-index 1', f'dimensions {len(vectors[0])}', f'rows {len(keys)}',
             'metric l2', f'dtype {dtype}', f'source {source}', f'key {key}', f'vector {vector}',
             f'shards {shards}']
    lines += [f'shard {i} {bounds[i]} {bounds[i + 1] - bounds[i]} shard-{i}.cagra' for i in range(shards)]
    for name, values in attributes.items():
        with open(os.path.join(directory, f'attr-{name}.i32'), 'wb') as out:
            out.write(struct.pack('<%di' % len(values), *[values[i] for i in order]))
        lines.append(f'attribute {name} attr-{name}.i32')
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


MATCH = 17
BITMAP = 18
SEARCH_BITMAP = 19


def match_frame(request, name, query, k, attribute, values):
    payload = struct.pack('<I', len(name)) + name.encode()
    payload += struct.pack('<III', len(query), k, len(attribute)) + attribute.encode()
    payload += struct.pack('<I%di' % len(values), len(values), *values)
    payload += struct.pack('<%df' % len(query), *query)
    return frame(MATCH, request, payload)


def varints(keys):
    """Packed keys: zigzag varint first key, then varint gaps (sorted unique)."""
    out = bytearray()
    previous = None
    for key in keys:
        value = ((key << 1) ^ (key >> 63)) & (2 ** 64 - 1) if previous is None else key - previous
        previous = key
        while True:
            byte = value & 0x7f
            value >>= 7
            out.append(byte | (0x80 if value else 0))
            if not value:
                break
    return bytes(out)


def bitmap_frame(request, name, keys):
    keys = sorted(set(keys))
    packed = varints(keys)
    payload = struct.pack('<I', len(name)) + name.encode() + struct.pack('<II', len(keys), len(packed)) + packed
    return frame(BITMAP, request, payload)


def search_bitmap_frame(request, name, query, k, bitmap):
    payload = struct.pack('<I', len(name)) + name.encode()
    payload += struct.pack('<III', len(query), k, len(bitmap) // 4)
    payload += struct.pack('<%df' % len(query), *query) + bitmap
    return frame(SEARCH_BITMAP, request, payload)


PUT_BITMAP = 20
SEARCH_CACHED = 21
MISSING = 11


def put_bitmap_frame(request, name, filter_id, bitmap):
    payload = struct.pack('<I', len(name)) + name.encode() + struct.pack('<qI', filter_id, len(bitmap) // 4) + bitmap
    return frame(PUT_BITMAP, request, payload)


def search_cached_frame(request, name, query, k, filter_id):
    payload = struct.pack('<I', len(name)) + name.encode()
    payload += struct.pack('<IIq', len(query), k, filter_id) + struct.pack('<%df' % len(query), *query)
    return frame(SEARCH_CACHED, request, payload)


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

    def fixture(self, name='docs', rows=600, dimensions=5, shards=4, seed=3, attributes=None):
        rng = random.Random(seed)
        keys = rng.sample(range(-10 ** 9, 10 ** 9), rows)
        vectors = [[round(rng.uniform(-3, 3), 3) for _ in range(dimensions)] for _ in range(rows)]
        path = os.path.join(self.directory.name, name)
        values = {column: make(keys, rng) for column, make in (attributes or {}).items()}
        sorted_keys, vectors = write_index(path, keys, vectors, shards=shards, attributes=values)
        if attributes:
            # Attribute values by key, for checking.
            return path, sorted_keys, vectors, {c: dict(zip(keys, v)) for c, v in values.items()}
        return path, sorted_keys, vectors

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

    def test_attribute_search_matches_exact(self):
        # Shards of 600 rows / 7 do not start on 32-row boundaries.
        path, keys, vectors, columns = self.fixture(shards=7, attributes={
            'color': lambda keys, rng: [rng.randrange(6) for _ in keys],
            'size': lambda keys, rng: [rng.choice([-2 ** 31, 1, 2 ** 31 - 1]) for _ in keys]})
        rng = random.Random(5)
        with self.server.connect() as connection:
            self.assertEqual(self.call(connection, load_frame(1, 'docs', path))[2], 0)
            cases = [('color', [3]), ('color', [0, 5, 5, 42]), ('size', [2 ** 31 - 1]), ('color', [99]),
                     ('color', [1, 2, 3, 4, 0, 5])]
            for request, (column, values) in enumerate(cases, 2):
                query = [rng.uniform(-3, 3) for _ in range(5)]
                _, _, status, payload = self.call(connection, match_frame(request, 'docs', query, 10, column, values))
                self.assertEqual(status, 0)
                _, hits = decode_hits(payload)
                allowed = {key for key in keys if columns[column][key] in values}
                want = exact(keys, vectors, query, 10, allowed)
                self.assertEqual([key for key, _ in hits], [key for _, key in want], (column, values))
            # Unknown attribute, no values, and too many values are rejected.
            self.assertEqual(self.call(connection, match_frame(20, 'docs', [0] * 5, 3, 'nosuch', [1]))[2], 1)
            self.assertEqual(self.call(connection, match_frame(21, 'docs', [0] * 5, 3, 'color', []))[2], 1)
            self.assertEqual(self.call(connection, match_frame(22, 'docs', [0] * 5, 3, 'color',
                                                               list(range(65))))[2], 1)
            self.assertEqual(self.call(connection, match_frame(23, 'other', [0] * 5, 3, 'color', [1]))[2], 3)

    def test_bitmap_build_and_search(self):
        path, keys, vectors = self.fixture(shards=7)
        rng = random.Random(6)
        with self.server.connect() as connection:
            self.assertEqual(self.call(connection, load_frame(1, 'docs', path))[2], 0)
            for request in range(2, 12, 2):
                allowed = rng.sample(keys, rng.randint(0, 300))
                _, _, status, reply = self.call(connection, bitmap_frame(request, 'docs', allowed + [10 ** 12]))
                self.assertEqual(status, 0)
                eligible, bitmap = struct.unpack_from('<Q', reply)[0], reply[8:]
                self.assertEqual(eligible, len(allowed))
                self.assertEqual(len(bitmap), (len(keys) + 31) // 32 * 4)
                ordinals = {i for i in range(len(keys)) if bitmap[i // 8] >> (i % 8) & 1}
                self.assertEqual(ordinals, {keys.index(key) for key in allowed})
                query = [rng.uniform(-3, 3) for _ in range(5)]
                _, _, status, payload = self.call(connection, search_bitmap_frame(request + 1, 'docs', query, 10,
                                                                                  bitmap))
                self.assertEqual(status, 0)
                _, hits = decode_hits(payload)
                want = exact(keys, vectors, query, 10, set(allowed))
                self.assertEqual([key for key, _ in hits], [key for _, key in want])
            # A bitmap of the wrong size, or for an unloaded index, is rejected.
            self.assertEqual(self.call(connection, search_bitmap_frame(30, 'docs', [0] * 5, 3, b'\0' * 4))[2], 1)
            self.assertEqual(self.call(connection, bitmap_frame(31, 'other', [1]))[2], 3)

    def test_bitmap_search_without_gather_limit(self):
        # Workers without CAGRA state expand every bitmap, however broad.
        path, keys, vectors = self.fixture(shards=3)
        self.server.close()
        self.server = Server('--backend', self.backend, '--gather-limit', '0', executable=self.worker)
        with self.server.connect() as connection:
            self.assertEqual(self.call(connection, load_frame(1, 'docs', path))[2], 0)
            allowed = keys[::2]
            _, _, status, reply = self.call(connection, bitmap_frame(2, 'docs', allowed))
            self.assertEqual(status, 0)
            query = [0.5] * 5
            _, _, status, payload = self.call(connection, search_bitmap_frame(3, 'docs', query, 10, reply[8:]))
            self.assertEqual(status, 0)
            _, hits = decode_hits(payload)
            self.assertEqual([key for key, _ in hits], [key for _, key in exact(keys, vectors, query, 10, set(allowed))])

    def test_cached_bitmaps(self):
        path, keys, vectors = self.fixture(shards=7)
        rng = random.Random(8)
        with self.server.connect() as connection:
            self.assertEqual(self.call(connection, load_frame(1, 'docs', path))[2], 0)
            query = [0.25] * 5
            self.assertEqual(self.call(connection, search_cached_frame(2, 'docs', query, 10, 5))[2], MISSING)
            for request, allowed in enumerate([rng.sample(keys, 40), rng.sample(keys, 400), []], 3):
                # Same id each time: a put replaces the cached entry.
                reply = self.call(connection, bitmap_frame(request * 10, 'docs', allowed))[3]
                self.assertEqual(self.call(connection, put_bitmap_frame(request * 10 + 1, 'docs', 5, reply[8:]))[2], 0)
                _, _, status, payload = self.call(connection, search_cached_frame(request * 10 + 2, 'docs', query, 10, 5))
                self.assertEqual(status, 0)
                want = exact(keys, vectors, query, 10, set(allowed))
                self.assertEqual([key for key, _ in decode_hits(payload)[1]], [key for _, key in want])
            _, _, status, info = self.call(connection, frame(15, 50, b'docs'))
            self.assertIn(b'bitmaps 1 ', info)
            # Wrong size, unloaded index, malformed search.
            self.assertEqual(self.call(connection, put_bitmap_frame(51, 'docs', 6, b'\0' * 4))[2], 1)
            self.assertEqual(self.call(connection, put_bitmap_frame(52, 'other', 6, b'\0' * 76))[2], 3)
            self.assertEqual(self.call(connection, search_cached_frame(53, 'other', query, 10, 5))[2], 3)
            self.assertEqual(self.call(connection, search_cached_frame(54, 'docs', [0.25] * 4, 10, 5))[2], 1)

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

    def test_quantized_copy_is_validated_and_exact_on_cpu(self):
        # The CPU worker maps a quantized copy (for Blackwell scans on cuVS
        # workers) but keeps answering exactly from vectors.f32.
        path, keys, vectors = self.fixture(shards=3)
        blocks = (5 + 15) // 16
        files = {'nvfp4': {'vectors.nvfp4': 600 * blocks * 8, 'scales.e4m3': 600 * blocks, 'rowscale.f32': 600 * 4},
                 'fp8': {'vectors.e4m3': 600 * blocks * 16, 'rowscale.f32': 600 * 4}}
        with open(os.path.join(path, 'manifest')) as manifest:
            base = manifest.read()
        for request, (kind, sizes) in enumerate(files.items(), 1):
            for name, size in sizes.items():
                with open(os.path.join(path, name), 'wb') as out:
                    out.write(b'\x11' * size)
            with open(os.path.join(path, 'manifest'), 'w') as manifest:
                manifest.write(base + f'quant {kind}\n')
            with self.server.connect() as connection:
                _, _, status, info = self.call(connection, load_frame(request * 10, kind, path))
                self.assertEqual(status, 0, info)
                self.assertIn(f'quant {kind} quant_scan off'.encode(), info)
                query = [0.5] * 5
                _, _, status, payload = self.call(connection, search_frame(request * 10 + 1, kind, query, 10))
                want = exact(keys, vectors, query, 10)
                self.assertEqual([key for key, _ in decode_hits(payload)[1]], [key for _, key in want])
                # A truncated quantized file, or an unknown kind, is refused.
                first = next(iter(sizes))
                with open(os.path.join(path, first), 'r+b') as out:
                    out.truncate(sizes[first] - 1)
                self.assertEqual(self.call(connection, load_frame(request * 10 + 2, kind + '-short', path))[2], 1)
        with open(os.path.join(path, 'manifest'), 'w') as manifest:
            manifest.write(base + 'quant int3\n')
        with self.server.connect() as connection:
            self.assertEqual(self.call(connection, load_frame(40, 'int3', path))[2], 1)

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
        tenants = [int(key) // 7 % 9 for key in keys]
        cls.path = os.path.join(cls.directory.name, 'index')
        write_index(cls.path, keys, vectors, shards=3, source=f'public.{cls.table}', attributes={'tenant': tenants})
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

    def test_attribute_search_matches_exact_sql(self):
        output = self.psql(f"""
            SELECT count(*) FILTER (WHERE a IS DISTINCT FROM b), count(*) FROM (
                SELECT ARRAY(SELECT s.id FROM ann_attribute_search('{self.name}', q.emb, 5, 'tenant',
                        ARRAY[q.tenant, (q.tenant + 4) % 9, NULL]) s ORDER BY s.distance, s.id) a,
                    ARRAY(SELECT t.id FROM {self.table} t WHERE t.tenant IN (q.tenant, (q.tenant + 4) % 9)
                        ORDER BY t.emb <-> q.emb, t.id LIMIT 5) b
                FROM {self.table} q WHERE q.id % 89 = 0) x;
            SELECT count(*) FROM ann_attribute_search('{self.name}', '[1,1,1,1]', 3, 'tenant', '{{}}');
        """)
        self.assertEqual(output.split(), ['0|22', '0'])
        self.assertIn('rejected', self.psql(
            f"SELECT * FROM ann_attribute_search('{self.name}', '[1,1,1,1]', 3, 'nosuch', ARRAY[1])", check=False))

    def test_bitmap_search_matches_exact_sql(self):
        filters = ['tenant = 3', 'tenant < 2', 'id % 5 = 0', 'false']
        built = self.psql(';'.join(f"SELECT ann_filter_bitmap_build('{self.name}', $f${f}$f$)" for f in filters))
        self.assertEqual(built.split(), self.psql(';'.join(
            f'SELECT count(*) FROM {self.table} WHERE {f}' for f in filters)).split())
        # Ids hash the index name and filter text: stable across rebuilds and
        # hosts, so hosts sharing GPU nodes share cached bitmaps.
        ids = f"SELECT string_agg(id::text, ',' ORDER BY filter) FROM ann_filter_bitmap WHERE index_name = '{self.name}'"
        before = self.psql(ids).strip()
        self.psql(f"SELECT ann_filter_bitmap_build('{self.name}', 'tenant = 3')")
        self.assertEqual(self.psql(ids).strip(), before)
        self.assertEqual(len(set(before.split(','))), len(filters))
        for f in filters:
            output = self.psql(f"""
                SELECT count(*) FILTER (WHERE a IS DISTINCT FROM b) FROM (
                    SELECT ARRAY(SELECT s.id FROM ann_bitmap_search('{self.name}', q.emb, 5, $f${f}$f$) s
                            ORDER BY s.distance, s.id) a,
                        ARRAY(SELECT t.id FROM {self.table} t WHERE {f} ORDER BY t.emb <-> q.emb, t.id LIMIT 5) b
                    FROM {self.table} q WHERE q.id % 89 = 0) x
            """)
            self.assertEqual(output.strip(), '0', f)
        self.assertIn('no ann filter bitmap', self.psql(
            f"SELECT * FROM ann_bitmap_search('{self.name}', '[1,1,1,1]', 3, 'tenant = 4')", check=False))

        # A restarted worker, loaded without PostgreSQL: searches push the
        # bitmaps they need; ann_filter_bitmap_push and ann_index_load push all.
        other = Server('--backend', os.environ.get('ANN_INDEX_BACKEND', 'cpu-exact'),
                       executable=IndexWorkerTests.worker)
        try:
            with other.connect() as connection:
                connection.sendall(load_frame(1, self.name, self.path))
                self.assertEqual(receive(connection)[2], 0)

            def run(script):
                result = subprocess.run([PSQL, DSN, '-X', '-q', '-At', '-v', 'ON_ERROR_STOP=1'],
                                        input=f"SET ann.worker_socket = '{other.path}';\n" + script,
                                        capture_output=True, text=True, timeout=120)
                self.assertEqual(result.returncode, 0, result.stderr)
                return result.stdout.split()

            def cached():
                with other.connect() as connection:
                    connection.sendall(frame(15, 2, self.name.encode()))
                    return int(receive(connection)[3].split(b' bitmaps ')[1].split()[0])

            self.assertEqual(cached(), 0)
            self.assertEqual(run(f"""
                SELECT count(*) FILTER (WHERE a IS DISTINCT FROM b) FROM (
                    SELECT ARRAY(SELECT s.id FROM ann_bitmap_search('{self.name}', q.emb, 5, 'tenant = 3') s
                            ORDER BY s.distance, s.id) a,
                        ARRAY(SELECT t.id FROM {self.table} t WHERE tenant = 3 ORDER BY t.emb <-> q.emb, t.id LIMIT 5) b
                    FROM {self.table} q WHERE q.id % 89 = 0) x"""), ['0'])
            self.assertEqual(cached(), 1)
            self.assertEqual(run(f"SELECT ann_filter_bitmap_push('{self.name}')"), [str(len(filters))])
            self.assertEqual(cached(), len(filters))
        finally:
            other.close()
        self.psql(f"DELETE FROM ann_filter_bitmap WHERE index_name = '{self.name}'")

    def test_overloaded_worker_is_retried_for_a_bounded_time(self):
        # A worker whose admission budget refuses every search: the search
        # retries for ann.worker_overload_wait_ms, then reports overload.
        import time
        busy = Server('--backend', os.environ.get('ANN_INDEX_BACKEND', 'cpu-exact'), '--max-pending-mb', '0',
                      executable=IndexWorkerTests.worker)
        try:
            with busy.connect() as connection:
                connection.sendall(load_frame(1, self.name, self.path))
                self.assertEqual(receive(connection)[2], 0)
            for wait, low, high in [(0, 0, 0.5), (400, 0.25, 2.0)]:
                started = time.monotonic()
                result = subprocess.run([PSQL, DSN, '-X', '-q', '-At'], capture_output=True, text=True, timeout=60,
                                        input=f"SET ann.worker_socket = '{busy.path}';\n"
                                        f"SET ann.worker_overload_wait_ms = {wait};\n"
                                        f"SELECT * FROM ann_index_search('{self.name}', '[1,1,1,1]', 3);")
                elapsed = time.monotonic() - started
                self.assertIn('overloaded', result.stderr)
                self.assertGreaterEqual(elapsed, low, wait)
                self.assertLess(elapsed, high, wait)
        finally:
            busy.close()

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
