"""Live worker-pipeline tests for the filtered CustomScan, driven through psql.

Requires ANN_PG_DSN for a superuser on the same host as this checkout, since
the server connects to the worker's Unix socket. ANN_PSQL overrides psql.
"""

import os
import re
import subprocess
import sys
import threading
import time
import unittest
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_server import ROOT, Server  # noqa: E402

DSN = os.environ.get('ANN_PG_DSN')
PSQL = os.environ.get('ANN_PSQL', 'psql')

SESSION = """
LOAD 'vector';
SET ann.enable_filtered_search = on;
SET max_parallel_workers_per_gather = 0;
SET ann.worker_chunk_rows = 256;
"""

# Compare the worker route against local scoring for many queries and bounds.
COMPARE = """
DO $$
DECLARE q int; lim int; a int[]; b int[]; v vector; bad int := 0;
BEGIN
  FOR q IN 1..25 LOOP
    SELECT emb INTO v FROM {table} WHERE id = q * 37;
    FOREACH lim IN ARRAY ARRAY[1, 7, 60] LOOP
      PERFORM set_config('ann.worker_socket', '{socket}', true);
      EXECUTE 'SELECT array_agg(id) FROM (SELECT id FROM {table} WHERE tenant IN (1, 2) OR id % 211 = 0 '
              'ORDER BY emb <-> $1, id LIMIT $2 OFFSET $3) s' INTO a USING v, lim, q % 3;
      PERFORM set_config('ann.worker_socket', '', true);
      EXECUTE 'SELECT array_agg(id) FROM (SELECT id FROM {table} WHERE tenant IN (1, 2) OR id % 211 = 0 '
              'ORDER BY emb <-> $1, id LIMIT $2 OFFSET $3) s' INTO b USING v, lim, q % 3;
      IF a IS DISTINCT FROM b THEN bad := bad + 1; END IF;
    END LOOP;
  END LOOP;
  RAISE NOTICE 'mismatches=%', bad;
END $$;
"""

QUERY = ("SELECT array_agg(id) FROM (SELECT id FROM {table} WHERE tenant IN (1, 2) "
         "ORDER BY emb <-> (ARRAY(SELECT 1.5::real FROM generate_series(1, 8)))::vector(8), id LIMIT 12) s")
EXPLAIN = ("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT id FROM {table} WHERE tenant IN (1, 2) "
           "ORDER BY emb <-> '[1.5,1.5,1.5,1.5,1.5,1.5,1.5,1.5]', id LIMIT 12")


@unittest.skipUnless(DSN, 'set ANN_PG_DSN for live PostgreSQL tests')
class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(['make', '-C', os.path.join(ROOT, 'ann'), 'ann-worker'], check=True,
                       stdout=subprocess.DEVNULL)
        cls.table = 'ann_pipeline_' + uuid.uuid4().hex[:12]
        cls.psql(f"""
            CREATE EXTENSION IF NOT EXISTS vector;
            CREATE TABLE {cls.table} (id int, tenant int, emb vector(8));
            SELECT setseed(0.25);
            INSERT INTO {cls.table} SELECT i, i % 5, CASE WHEN i % 97 = 0 THEN NULL ELSE
                (SELECT array_agg(round((random() * 3)::numeric, 1)::real) FROM generate_series(1, 8)
                 WHERE i > 0)::vector END FROM generate_series(1, 6000) i;
            CREATE INDEX ON {cls.table} USING ann (emb vector_l2_ops);
            ANALYZE {cls.table};
        """)

    @classmethod
    def tearDownClass(cls):
        cls.psql(f'DROP TABLE {cls.table}')

    @staticmethod
    def psql(script, settings=''):
        result = subprocess.run([PSQL, DSN, '-X', '-q', '-At', '-v', 'ON_ERROR_STOP=1'],
                                input=SESSION + settings + script, capture_output=True, text=True,
                                timeout=120)
        if result.returncode != 0:
            raise AssertionError(result.stderr)
        return result.stdout + result.stderr

    def setUp(self):
        self.server = None

    def tearDown(self):
        if self.server:
            self.server.close()

    def start(self, *args, env=None):
        self.server = Server('--backend', 'mock-gpu', *args, env=env)
        return f"SET ann.worker_socket = '{self.server.path}';\n"

    def explain(self, settings):
        output = self.psql(EXPLAIN.format(table=self.table) + ';', settings)
        return {k: int(v) for k, v in re.findall(r'(Worker Chunks|Local Chunks|Worker Candidates|'
                                                 r'Distance Evaluations): (\d+)', output)}

    def local_result(self):
        return self.psql(QUERY.format(table=self.table) + ';').strip()

    def test_worker_route_matches_local_scoring(self):
        self.start()
        output = self.psql(COMPARE.format(table=self.table, socket=self.server.path))
        self.assertIn('mismatches=0', output)

    def test_explain_reports_worker_chunks(self):
        stats = self.explain(self.start())
        self.assertGreater(stats['Worker Chunks'], 1)
        # Backends re-score only returned candidates, plus any chunk shed to
        # local scoring while the pipeline was full and NULL-vector rows.
        self.assertLessEqual(stats['Distance Evaluations'],
                             stats['Worker Candidates'] + 256 * stats['Local Chunks'])
        self.assertLess(stats['Distance Evaluations'], 2400)

    def test_inline_transport_without_shared_memory(self):
        settings = self.start() + 'SET ann.worker_shared_memory_mb = 0;\n'
        self.assertEqual(self.psql(QUERY.format(table=self.table) + ';', settings).strip(),
                         self.local_result())
        self.assertGreater(self.explain(settings)['Worker Chunks'], 1)

    def test_overloaded_worker_falls_back_per_chunk(self):
        settings = self.start('--max-pending-mb', '0')
        self.assertEqual(self.psql(QUERY.format(table=self.table) + ';', settings).strip(),
                         self.local_result())
        stats = self.explain(settings)
        self.assertEqual(stats['Worker Chunks'], 0)
        self.assertGreater(stats['Local Chunks'], 1)

    def test_worker_exit_mid_scan_keeps_exact_results(self):
        expected = self.local_result()
        settings = self.start('--lanes', '1', env={'ANN_MOCK_GPU_LAUNCH_US': '300000'})
        process = subprocess.Popen([PSQL, DSN, '-X', '-q', '-At', '-v', 'ON_ERROR_STOP=1'],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True)
        replies = []
        thread = threading.Thread(target=lambda: replies.append(process.communicate(
            SESSION + settings + QUERY.format(table=self.table) + ';\n' +
            EXPLAIN.format(table=self.table) + ';\n', timeout=120)))
        thread.start()
        time.sleep(0.5)
        self.server.process.kill()
        thread.join()
        output, errors = replies[0]
        self.assertEqual(process.returncode, 0, errors)
        self.assertEqual(output.splitlines()[0], expected)
        # Whichever statement the exit interrupts scores its remaining chunks locally.
        local = re.search(r'Local Chunks: (\d+)', output)
        self.assertTrue(local is None or int(local.group(1)) > 0, output)

    def test_timeout_falls_back(self):
        settings = self.start('--lanes', '1', env={'ANN_MOCK_GPU_LAUNCH_US': '2000000'})
        settings += 'SET ann.worker_timeout_ms = 200;\n'
        started = time.monotonic()
        self.assertEqual(self.psql(QUERY.format(table=self.table) + ';', settings).strip(),
                         self.local_result())
        self.assertLess(time.monotonic() - started, 10)


if __name__ == '__main__':
    unittest.main()
