"""Opt-in live PostgreSQL tests, isolated in a uniquely named test schema."""

import os
from pathlib import Path
import sys
import unittest
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


@unittest.skipUnless(os.environ.get('ANN_PG_DSN'), 'set ANN_PG_DSN for live PostgreSQL tests')
class PostgresTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        from psycopg import sql

        self.psycopg = psycopg
        self.sql = sql
        self.dsn = os.environ['ANN_PG_DSN']
        self.schema = 'ann_test_' + uuid.uuid4().hex
        self.db = psycopg.connect(self.dsn, autocommit=True)
        self.db.execute('CREATE EXTENSION IF NOT EXISTS vector')
        self.db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
        self.relation = sql.Identifier(self.schema, 'documents')
        self.db.execute(sql.SQL('CREATE TABLE {} (id integer PRIMARY KEY, tenant text, '
                               'tags text[], terms tsvector, embedding vector(3))').format(self.relation))
        for i in range(64):
            self.db.execute(sql.SQL('INSERT INTO {} VALUES (%s, %s, %s, '
                                    "to_tsvector('simple', %s), %s::vector)").format(self.relation),
                            (i, 'even' if i % 2 == 0 else 'odd',
                             ['database'] if i % 3 == 0 else ['storage'],
                             'database query' if i % 3 == 0 else 'storage', f'[{i},1,2]'))
        self.db.execute(sql.SQL('CREATE INDEX ON {} USING gin (terms)').format(self.relation))

    def tearDown(self):
        self.db.execute(self.sql.SQL('DROP SCHEMA {} CASCADE').format(self.sql.Identifier(self.schema)))
        self.db.close()

    def test_combined_predicate_and_shared_query_batch(self):
        from ann.postgres import FrozenIndex

        predicate = self.sql.SQL("tenant = %s AND tags @> %s AND terms @@ to_tsquery('simple', %s)")
        params = ('even', ['database'], 'database & query')
        with FrozenIndex(self.dsn, 'documents', schema=self.schema) as index:
            for query, row in zip(['[0,1,2]', '[24,1,2]'],
                                  index.search([[0, 1, 2], [24, 1, 2]], 5, predicate, params)):
                oracle = self.db.execute(self.sql.SQL(
                    'SELECT id, embedding <-> %s::vector AS distance FROM {} WHERE {} '
                    'ORDER BY distance, id LIMIT 5').format(self.relation, predicate),
                    (query,) + params).fetchall()
                self.assertEqual([v[0] for v in row], [v[0] for v in oracle])
                for found, expected in zip(row, oracle):
                    self.assertAlmostEqual(found[1], expected[1], places=5)
            self.assertEqual(index.search([[0, 1, 2]], 5, self.sql.SQL('FALSE')), [[]])
            self.assertEqual(index.last_search['eligible_rows'], 0)
            self.assertEqual(index.last_search['query_vectors'], 1)
            self.assertGreaterEqual(index.last_search['total_seconds'],
                                    index.last_search['worker_roundtrip_seconds'])
            with self.assertRaises(TypeError):
                index.search([[0, 1, 2]], 1, 'TRUE')
        with self.assertRaises(RuntimeError):
            index.search([[0, 1, 2]], 1)

    def test_freeze_blocks_all_external_mutations(self):
        from ann.postgres import FrozenIndex

        with FrozenIndex(self.dsn, 'documents', schema=self.schema):
            self.db.execute("SET lock_timeout = '100ms'")
            for statement in [
                    'UPDATE {} SET tenant = \'changed\' WHERE id = 0',
                    'INSERT INTO {0} SELECT 1000, tenant, tags, terms, embedding FROM {0} WHERE id = 0',
                    'DELETE FROM {} WHERE id = 0', 'TRUNCATE {}',
                    'ALTER TABLE {} ADD COLUMN extra integer']:
                with self.assertRaises(self.psycopg.errors.LockNotAvailable):
                    self.db.execute(self.sql.SQL(statement).format(self.relation))
        self.db.execute(self.sql.SQL('UPDATE {} SET tenant = %s WHERE id = 0').format(self.relation),
                        ('changed',))

    def test_budget_cleanup_releases_lock(self):
        from ann.postgres import FrozenIndex

        with self.assertRaises(ValueError):
            with FrozenIndex(self.dsn, 'documents', schema=self.schema, max_rows=1):
                self.fail('oversized corpus accepted')
        self.db.execute("SET lock_timeout = '100ms'")
        self.db.execute(self.sql.SQL('DELETE FROM {} WHERE id = 0').format(self.relation))

    def test_private_freeze_transaction_is_read_only(self):
        from ann.postgres import FrozenIndex

        with FrozenIndex(self.dsn, 'documents', schema=self.schema) as index:
            self.assertEqual(index._connection.execute('SHOW transaction_read_only').fetchone(), ('on',))
            with self.assertRaises(self.psycopg.errors.ReadOnlySqlTransaction):
                index._connection.execute(self.sql.SQL(
                    'DELETE FROM {} WHERE id = 0').format(self.relation))

    def test_freeze_loss_during_external_search_discards_results(self):
        from ann.postgres import FrozenIndex

        with FrozenIndex(self.dsn, 'documents', schema=self.schema) as index:
            pid = index._connection.info.backend_pid
            search = index._worker.search

            def lose_session(*args, **kwargs):
                result = search(*args, **kwargs)
                self.db.execute('SELECT pg_terminate_backend(%s)', (pid,))
                return result

            index._worker.search = lose_session
            with self.assertRaises(self.psycopg.Error):
                index.search([[0, 1, 2]], 5)

    def test_predicate_timeout_releases_freeze(self):
        from ann.postgres import FrozenIndex

        with FrozenIndex(self.dsn, 'documents', schema=self.schema, statement_timeout_ms=100) as index:
            with self.assertRaises(self.psycopg.errors.QueryCanceled):
                index.search([[0, 1, 2]], 5, self.sql.SQL('pg_sleep(1) IS NULL'))
            self.assertIsNone(index._connection)
            self.assertIsNone(index.last_search)
            self.db.execute("SET lock_timeout = '100ms'")
            self.db.execute(self.sql.SQL('DELETE FROM {} WHERE id = 0').format(self.relation))


if __name__ == '__main__':
    unittest.main()
