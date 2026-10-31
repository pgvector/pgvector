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
from test_server import ROOT, Server  # noqa: E402

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
                           '--bitset-min', '0', executable=os.path.join(ROOT, 'ann', 'ann-worker-cuvs'))
        cls.table = 'ann_cuvs_' + uuid.uuid4().hex[:10]
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
