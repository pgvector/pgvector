"""CAGRA shards built by scripts/ann/build_cuvs_index.py, served through SQL on GPUs.

Needs a cuVS host: ANN_CUVS_TEST=1, ANN_PG_DSN for a local superuser, the
ann-worker-cuvs build, and Python packages cuvs, cupy, numpy, psycopg.
"""

import os
import subprocess
import sys
import tempfile
import unittest
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_server import ROOT, Server, frame, receive  # noqa: E402

DSN = os.environ.get('ANN_PG_DSN')
PSQL = os.environ.get('ANN_PSQL', 'psql')
BUILD = os.path.join(ROOT, 'scripts', 'ann', 'build_cuvs_index.py')


@unittest.skipUnless(os.environ.get('ANN_CUVS_TEST') and DSN, 'set ANN_CUVS_TEST=1 and ANN_PG_DSN on a cuVS host')
class CuvsIndexTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        # A small exact limit sends 2% filters to the GPU: the gather scan by
        # default, filtered CAGRA when the gather scan is off.
        cls.server = Server('--backend', 'cuvs', '--lanes', '8', '--exact-limit', '500',
                            executable=os.path.join(ROOT, 'ann', 'ann-worker-cuvs'))
        # Attribute values all get prebuilt bitsets on the graph worker.
        cls.graph = Server('--backend', 'cuvs', '--lanes', '8', '--exact-limit', '500', '--gather-limit', '0',
                           '--bitset-gather-limit', '0', '--bitset-min', '0',
                           executable=os.path.join(ROOT, 'ann', 'ann-worker-cuvs'))
        cls.table = 'ann_cuvs_' + uuid.uuid4().hex[:10]
        cls.loaded = {}
        cls.psql(f"""
            CREATE TABLE {cls.table} (id bigint PRIMARY KEY, tenant int, emb vector(32));
            SELECT setseed(0.1);
            INSERT INTO {cls.table} SELECT i * 3, i % 50,
                (SELECT array_agg(random()::real) FROM generate_series(1, 32) WHERE i > 0)::vector
                FROM generate_series(1, 40000) i;
        """)
        for dtype in ['float32', 'float16']:
            path = os.path.join(cls.directory.name, dtype)
            subprocess.run([sys.executable, BUILD, '--dsn', DSN, '--table', f'public.{cls.table}', '--key', 'id',
                            '--vector', 'emb', '--out', path, '--shards', '4', '--dtype', dtype,
                            '--attribute', 'tenant'],
                           check=True, capture_output=True)
            for server in [cls.server, cls.graph]:
                cls.psql(f"SELECT ann_index_load('{cls.table}_{dtype}', '{path}')", server)
        cls.psql(f"""SELECT ann_filter_bitmap_build('{cls.table}_float32', 'tenant = ' || t)
                     FROM generate_series(0, 49) t""")
        # Quantized copies for the Blackwell module's gather scan.
        for quant in ['nvfp4', 'fp8']:
            path = os.path.join(cls.directory.name, quant)
            subprocess.run([sys.executable, BUILD, '--dsn', DSN, '--table', f'public.{cls.table}', '--key', 'id',
                            '--vector', 'emb', '--out', path, '--shards', '4', '--quantize', quant],
                           check=True, capture_output=True)
            cls.loaded[quant] = cls.psql(f"SELECT ann_index_load('{cls.table}_{quant}', '{path}')")

    @classmethod
    def tearDownClass(cls):
        cls.psql(f'DROP TABLE {cls.table}')
        cls.server.close()
        cls.graph.close()
        cls.directory.cleanup()

    @classmethod
    def psql(cls, script, server=None):
        result = subprocess.run([PSQL, DSN, '-X', '-q', '-At', '-v', 'ON_ERROR_STOP=1'],
                                input=f"SET ann.worker_socket = '{(server or cls.server).path}';\n"
                                # Send every filter to the worker, never to local SQL.
                                "SET ann.gpu_min_eligible = 0;\n" + script,
                                capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            raise AssertionError(result.stderr)
        return result.stdout.strip()

    def recall(self, index, filtered, server=None):
        fltr = f', ARRAY(SELECT id FROM {self.table} WHERE tenant = q.tenant)' if filtered else ''
        where = 't.tenant = q.tenant' if filtered else 'true'
        return float(self.psql(f"""
            SELECT avg(cardinality(ARRAY(SELECT unnest(a) INTERSECT SELECT unnest(b)))) / 10 FROM (
              SELECT ARRAY(SELECT s.id FROM ann_index_search('{index}', q.emb, 10{fltr}) s) a,
                     ARRAY(SELECT t.id FROM {self.table} t WHERE {where} ORDER BY t.emb <-> q.emb LIMIT 10) b
              FROM {self.table} q WHERE q.id % 1200 = 0) x""", server))

    def filtered(self, function, server=None):
        """(mismatches with exact SQL, recall@10, rows outside the filter) for one tenant per query."""
        call = {'attribute': f"ann_attribute_search('{self.table}_float32', q.emb, 10, 'tenant', ARRAY[q.tenant])",
                'bitmap': f"ann_bitmap_search('{self.table}_float32', q.emb, 10, 'tenant = ' || q.tenant)"}[function]
        return self.psql(f"""
            SELECT count(*) FILTER (WHERE a IS DISTINCT FROM b),
                   round(avg(cardinality(ARRAY(SELECT unnest(a) INTERSECT SELECT unnest(b)))) / 10, 3),
                   sum((SELECT count(*) FROM {self.table} t WHERE t.id = ANY (a) AND t.tenant <> x.tenant))
            FROM (
              SELECT q.tenant, ARRAY(SELECT s.id FROM {call} s ORDER BY s.distance, s.id) a,
                     ARRAY(SELECT t.id FROM {self.table} t WHERE t.tenant = q.tenant
                           ORDER BY t.emb <-> q.emb, t.id LIMIT 10) b
              FROM {self.table} q WHERE q.id % 1200 = 0) x""", server).split('|')

    def test_attribute_and_bitmap_gather_route_match_sql(self):
        for function in ['attribute', 'bitmap']:
            mismatches, _, outside = self.filtered(function)
            self.assertEqual((mismatches, outside), ('0', '0'), function)

    def test_attribute_and_bitmap_graph_route(self):
        # Filtered CAGRA with cuVS bitsets: prebuilt per attribute value, or
        # sliced from the stored bitmap (shards of 10,000 rows are not
        # 32-row aligned).
        for function in ['attribute', 'bitmap']:
            _, recall, outside = self.filtered(function, self.graph)
            self.assertGreaterEqual(float(recall), 0.9, function)
            self.assertEqual(outside, '0', function)

    def test_blackwell_quantized_gather(self):
        # On compute capability 10.x GPUs the gather scan scores the NVFP4 or
        # FP8 copy and re-scores candidates in FP32: rows stay within the
        # filter, distances stay exact, and recall holds.
        import ctypes
        major = ctypes.c_int()
        ctypes.CDLL('libcudart.so').cudaDeviceGetAttribute(ctypes.byref(major), 75, 0)
        for quant in ['nvfp4', 'fp8']:
            index = f'{self.table}_{quant}'
            with self.server.connect() as connection:
                connection.sendall(frame(15, 1, index.encode()))
                info = receive(connection)[3]
            expected = b'quant_scan on' if major.value == 10 else b'quant_scan off'
            self.assertIn(f'quant {quant} '.encode() + expected, info)
            stats = self.psql(f"""
                SELECT round(avg(cardinality(ARRAY(SELECT unnest(a) INTERSECT SELECT unnest(b)))) / 10, 3),
                       max(e), sum(o) FROM (
                  SELECT ARRAY(SELECT s.id FROM ann_index_search('{index}', q.emb, 10,
                            ARRAY(SELECT id FROM {self.table} WHERE tenant = q.tenant)) s) a,
                         ARRAY(SELECT t.id FROM {self.table} t WHERE t.tenant = q.tenant
                               ORDER BY t.emb <-> q.emb LIMIT 10) b,
                         (SELECT max(abs(s.distance - (t.emb <-> q.emb))) FROM ann_index_search('{index}', q.emb, 10,
                            ARRAY(SELECT id FROM {self.table} WHERE tenant = q.tenant)) s JOIN {self.table} t USING (id)) e,
                         (SELECT count(*) FROM ann_index_search('{index}', q.emb, 10,
                            ARRAY(SELECT id FROM {self.table} WHERE tenant = q.tenant)) s JOIN {self.table} t USING (id)
                          WHERE t.tenant <> q.tenant) o
                  FROM {self.table} q WHERE q.id % 1200 = 0) x""").split('|')
            self.assertGreaterEqual(float(stats[0]), 0.99, quant)
            self.assertLess(float(stats[1]), 1e-4, quant)
            self.assertEqual(stats[2], '0', quant)

    def test_same_filter_searches_group_exactly(self):
        # Searches over one cached bitmap, pipelined from several connections
        # so they queue up, are grouped into shared tensor-core scans (of
        # the quantized copy, or of the FP32 and FP16 datasets in place);
        # every answer must equal exact SQL, and the stats must show groups.
        import json
        import struct
        import threading
        from test_index import search_cached_frame, decode_hits
        rows = self.psql(f"SELECT id, emb::text FROM {self.table} WHERE id % 997 = 0 ORDER BY id LIMIT 8").split('\n')
        queries = [(int(line.split('|')[0]), [float(x) for x in line.split('|')[1].strip('[]').split(',')]) for line in rows]
        for quant in ['nvfp4', 'float32', 'float16']:
            index = f'{self.table}_{quant}'
            self.psql(f"SELECT ann_filter_bitmap_build('{index}', 'tenant = 7')")
            filter_id = int(self.psql(f"SELECT id FROM ann_filter_bitmap WHERE index_name = '{index}' AND filter = 'tenant = 7'"))
            exact = {}
            for qid, _ in queries:
                exact[qid] = [int(x) for x in self.psql(f"""
                    SELECT t.id FROM {self.table} t, {self.table} q WHERE q.id = {qid} AND t.tenant = 7
                    ORDER BY t.emb <-> q.emb, t.id LIMIT 10""").split()]
            failures = []

            def client(offset):
                try:
                    with self.server.connect() as connection:
                        for request in range(40):
                            qid, vector = queries[(offset + request) % 8]
                            connection.sendall(search_cached_frame(request + 1, index, vector, 10, filter_id))
                        for request in range(40):
                            _, got, status, payload = receive(connection)
                            self.assertEqual(status, 0)
                            qid = queries[(offset + got - 1) % 8][0]
                            ids = [key for key, _ in decode_hits(payload)[1]]
                            self.assertEqual(ids, exact[qid], (quant, qid))
                except Exception as error:  # noqa: BLE001
                    failures.append(error)
            threads = [threading.Thread(target=client, args=(i,)) for i in range(6)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(failures, [], quant)
            with self.server.connect() as connection:
                connection.sendall(frame(8, 1))
                stats = json.loads(receive(connection)[3])
            self.assertGreater(stats['groups'], 0, quant)
            self.psql(f"DELETE FROM ann_filter_bitmap WHERE index_name = '{index}' AND filter = 'tenant = 7'")

    def test_ann_recall(self):
        for dtype in ['float32', 'float16']:
            self.assertGreaterEqual(self.recall(f'{self.table}_{dtype}', False), 0.9, dtype)

    def test_filtered_graph_route(self):
        for dtype in ['float32', 'float16']:
            index = f'{self.table}_{dtype}'
            self.assertGreaterEqual(self.recall(index, True, self.graph), 0.9, dtype)
            outside = self.psql(f"""
                SELECT count(*) FROM {self.table} q, ann_index_search('{index}', q.emb, 10,
                    ARRAY(SELECT id FROM {self.table} WHERE tenant = q.tenant)) s
                JOIN {self.table} t USING (id) WHERE q.id % 1200 = 0 AND t.tenant <> q.tenant""", self.graph)
            self.assertEqual(outside, '0')

    def test_gather_route_matches_sql(self):
        # 800 eligible keys (2%) across all 4 shards, plus keys missing from
        # the index: the gather scan is exact, so ids, order and distances
        # match SQL.
        for dtype in ['float32', 'float16']:
            mismatches = self.psql(f"""
                SELECT count(*) FILTER (WHERE a IS DISTINCT FROM b OR e > 1e-4) FROM (
                  SELECT ARRAY(SELECT s.id FROM ann_index_search('{self.table}_{dtype}', q.emb, 10,
                            ARRAY(SELECT id FROM {self.table} WHERE tenant = q.tenant UNION ALL
                                  SELECT -1 - g FROM generate_series(1, 50) g)) s ORDER BY s.distance, s.id) a,
                         ARRAY(SELECT t.id FROM {self.table} t WHERE t.tenant = q.tenant
                               ORDER BY t.emb <-> q.emb, t.id LIMIT 10) b,
                         (SELECT max(abs(s.distance - (t.emb <-> q.emb))) FROM ann_index_search(
                            '{self.table}_{dtype}', q.emb, 10, ARRAY(SELECT id FROM {self.table}
                            WHERE tenant = q.tenant)) s JOIN {self.table} t USING (id)) e
                  FROM {self.table} q WHERE q.id % 1200 = 0) x""")
            self.assertEqual(mismatches, '0', dtype)

    def test_exact_route_matches_sql(self):
        # 100 eligible keys is under the exact limit, so results are exact.
        mismatches = self.psql(f"""
            SELECT count(*) FILTER (WHERE a IS DISTINCT FROM b) FROM (
              SELECT ARRAY(SELECT s.id FROM ann_index_search('{self.table}_float16', q.emb, 10,
                        ARRAY(SELECT id FROM {self.table} WHERE id % 1200 < 3)) s ORDER BY s.distance, s.id) a,
                     ARRAY(SELECT t.id FROM {self.table} t WHERE t.id % 1200 < 3 ORDER BY t.emb <-> q.emb, t.id LIMIT 10) b
              FROM {self.table} q WHERE q.id % 3000 = 0) x""")
        self.assertEqual(mismatches, '0')

    def test_distances_are_exact(self):
        error = self.psql(f"""
            SELECT max(abs(s.distance - (t.emb <-> q.emb))) FROM {self.table} q,
                ann_index_search('{self.table}_float16', q.emb, 10) s JOIN {self.table} t USING (id)
            WHERE q.id % 2400 = 0""")
        self.assertLess(float(error), 1e-4)


if __name__ == '__main__':
    unittest.main()
