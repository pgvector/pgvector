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
        # A small exact limit makes 2% filters take the filtered CAGRA route.
        cls.server = Server('--backend', 'cuvs', '--lanes', '8', '--exact-limit', '500',
                            executable=os.path.join(ROOT, 'ann', 'ann-worker-cuvs'))
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
                            '--vector', 'emb', '--out', path, '--shards', '4', '--dtype', dtype],
                           check=True, capture_output=True)
            cls.psql(f"SELECT ann_index_load('{cls.table}_{dtype}', '{path}')")

    @classmethod
    def tearDownClass(cls):
        cls.psql(f'DROP TABLE {cls.table}')
        cls.server.close()
        cls.directory.cleanup()

    @classmethod
    def psql(cls, script):
        result = subprocess.run([PSQL, DSN, '-X', '-q', '-At', '-v', 'ON_ERROR_STOP=1'],
                                input=f"SET ann.worker_socket = '{cls.server.path}';\n" + script,
                                capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            raise AssertionError(result.stderr)
        return result.stdout.strip()

    def recall(self, index, filtered):
        fltr = f', ARRAY(SELECT id FROM {self.table} WHERE tenant = q.tenant)' if filtered else ''
        where = 't.tenant = q.tenant' if filtered else 'true'
        return float(self.psql(f"""
            SELECT avg(cardinality(ARRAY(SELECT unnest(a) INTERSECT SELECT unnest(b)))) / 10 FROM (
              SELECT ARRAY(SELECT s.id FROM ann_index_search('{index}', q.emb, 10{fltr}) s) a,
                     ARRAY(SELECT t.id FROM {self.table} t WHERE {where} ORDER BY t.emb <-> q.emb LIMIT 10) b
              FROM {self.table} q WHERE q.id % 1200 = 0) x"""))

    def test_ann_recall(self):
        for dtype in ['float32', 'float16']:
            self.assertGreaterEqual(self.recall(f'{self.table}_{dtype}', False), 0.9, dtype)

    def test_filtered_graph_route(self):
        for dtype in ['float32', 'float16']:
            index = f'{self.table}_{dtype}'
            self.assertGreaterEqual(self.recall(index, True), 0.9, dtype)
            outside = self.psql(f"""
                SELECT count(*) FROM {self.table} q, ann_index_search('{index}', q.emb, 10,
                    ARRAY(SELECT id FROM {self.table} WHERE tenant = q.tenant)) s
                JOIN {self.table} t USING (id) WHERE q.id % 1200 = 0 AND t.tenant <> q.tenant""")
            self.assertEqual(outside, '0')

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
