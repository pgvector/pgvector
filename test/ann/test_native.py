"""Live tests of the native C access method against sequential SQL execution."""

from concurrent.futures import ThreadPoolExecutor
import os
import random
import unittest
import uuid


@unittest.skipUnless(os.environ.get('ANN_PG_DSN'), 'set ANN_PG_DSN for live PostgreSQL tests')
class NativeTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        from psycopg import sql

        self.psycopg = psycopg
        self.sql = sql
        self.dsn = os.environ['ANN_PG_DSN']
        self.db = psycopg.connect(self.dsn, autocommit=True)
        self.schema = 'ann_native_' + uuid.uuid4().hex
        self.db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
        self.table = sql.Identifier(self.schema, 'documents')
        self.index = sql.Identifier(self.schema, 'documents_ann')
        self.db.execute(sql.SQL('CREATE TABLE {} (id integer, tenant text, '
                                'terms tsvector, embedding vector(3))').format(self.table))
        self.db.execute(sql.SQL("INSERT INTO {} SELECT i, CASE WHEN i % 101 = 0 THEN 'rare' "
                                "ELSE 'common' END, to_tsvector('simple', CASE WHEN i % 7 = 0 "
                                "THEN 'database search' ELSE 'storage' END), "
                                'ARRAY[i * 0.123, i % 13, i % 17]::vector '
                                'FROM generate_series(1, 4096) i').format(self.table))
        self.db.execute(sql.SQL('CREATE INDEX documents_ann ON {} USING ann '
                                '(embedding vector_l2_ops)').format(self.table))
        self.db.execute('SET enable_seqscan = off')

    def tearDown(self):
        self.db.execute(self.sql.SQL('DROP SCHEMA {} CASCADE').format(self.sql.Identifier(self.schema)))
        self.db.close()

    def query(self, predicate='TRUE', limit=31):
        return self.sql.SQL('SELECT id, embedding <-> (SELECT %s::vector) AS distance FROM {} WHERE {} '
                            'ORDER BY distance, id LIMIT {}').format(
                                self.table, self.sql.SQL(predicate), self.sql.Literal(limit))

    def assert_oracle(self, predicate='TRUE', query='[13.4,3,5]', limit=31, db=None):
        db = db or self.db
        statement = self.query(predicate, limit)
        db.execute('SET enable_seqscan = off')
        plan = db.execute(self.sql.SQL('EXPLAIN (FORMAT JSON) {}').format(statement), (query,)).fetchone()[0]
        if predicate != 'FALSE':
            self.assertIn('documents_ann', str(plan))
        actual = db.execute(statement, (query,)).fetchall()
        db.execute('SET enable_indexscan = off')
        db.execute('SET enable_bitmapscan = off')
        db.execute('SET enable_seqscan = on')
        expected = db.execute(statement, (query,)).fetchall()
        db.execute('SET enable_indexscan = on')
        db.execute('SET enable_bitmapscan = on')
        db.execute('SET enable_seqscan = off')
        self.assertEqual(actual, expected)
        return actual

    def test_selective_text_and_metadata(self):
        for predicate in ["tenant = 'rare'", "terms @@ to_tsquery('simple', 'database & search')",
                          "tenant = 'rare' AND terms @@ to_tsquery('simple', 'database')", 'FALSE']:
            self.assert_oracle(predicate)
        rng = random.Random(42)
        for _ in range(12):
            self.assert_oracle(query=str([rng.random() * 500, rng.random() * 13, rng.random() * 17]))

    def test_nulls_and_spilled_sort(self):
        self.db.execute(self.sql.SQL('INSERT INTO {} VALUES (5000, NULL, NULL, NULL)').format(self.table))
        self.db.execute("SET work_mem = '64kB'")
        rows = self.assert_oracle(limit=10000)
        self.assertEqual(rows[-1], (5000, None))
        self.assertEqual(len(self.assert_oracle(query=None, limit=10000)), 4097)

    def test_update_delete_abort_and_vacuum_reuse(self):
        self.db.execute(self.sql.SQL("UPDATE {} SET tenant = 'rare' WHERE id % 29 = 0").format(self.table))
        self.db.execute(self.sql.SQL("UPDATE {} SET embedding = '[0,0,0]' WHERE id % 31 = 0").format(self.table))
        before = self.db.execute('SELECT pg_relation_size(%s::regclass)',
                                 (self.schema + '.documents_ann',)).fetchone()[0]
        self.db.execute(self.sql.SQL('DELETE FROM {} WHERE id % 2 = 0').format(self.table))
        self.db.execute(self.sql.SQL('VACUUM {}').format(self.table))
        self.db.execute(self.sql.SQL('INSERT INTO {} SELECT i, NULL, NULL, ARRAY[i,0,0]::vector '
                                    'FROM generate_series(6000, 6999) i').format(self.table))
        after = self.db.execute('SELECT pg_relation_size(%s::regclass)',
                                (self.schema + '.documents_ann',)).fetchone()[0]
        self.assertLessEqual(after, before + 8192)
        self.db.execute('BEGIN')
        self.db.execute(self.sql.SQL("INSERT INTO {} VALUES (9000, NULL, NULL, '[0,0,0]')").format(self.table))
        self.assert_oracle(query='[0,0,0]')
        self.db.execute('ROLLBACK')
        self.assert_oracle(query='[0,0,0]')
        self.db.execute(self.sql.SQL('REINDEX INDEX {}').format(self.index))
        self.assert_oracle("tenant = 'rare'")

    def test_repeatable_read_and_uncommitted_insert(self):
        with self.psycopg.connect(self.dsn, autocommit=True) as reader:
            reader.execute('BEGIN ISOLATION LEVEL REPEATABLE READ')
            original = self.assert_oracle(db=reader)
            self.db.execute(self.sql.SQL("UPDATE {} SET embedding = '[13.4,3,5]' WHERE id = 1").format(self.table))
            self.db.execute(self.sql.SQL('DELETE FROM {} WHERE id = 2').format(self.table))
            self.db.execute(self.sql.SQL('VACUUM {}').format(self.table))
            self.assertEqual(self.assert_oracle(db=reader), original)
            self.db.execute('BEGIN')
            self.db.execute(self.sql.SQL("INSERT INTO {} VALUES (9000, NULL, NULL, '[13.4,3,5]')").format(self.table))
            self.assertEqual(self.assert_oracle(db=reader), original)
            self.db.execute('COMMIT')
            reader.execute('COMMIT')
            self.assert_oracle(db=reader)

    def test_concurrent_writers(self):
        statement = self.sql.SQL('INSERT INTO {} SELECT i, NULL, NULL, ARRAY[i,1,2]::vector '
                                 'FROM generate_series(%s::integer, %s::integer) i').format(self.table)

        def write(start):
            with self.psycopg.connect(self.dsn) as db:
                db.execute(statement, (start, start + 299))

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(write, [10000, 11000, 12000, 13000]))
        self.assertEqual(self.db.execute(self.sql.SQL('SELECT count(*) FROM {}').format(self.table)).fetchone()[0], 5296)
        self.assert_oracle(query='[11001,1,2]')

    def test_existing_hot_chains_and_concurrent_build(self):
        self.db.execute(self.sql.SQL('DROP INDEX {}').format(self.index))
        # Leave heap-page room so the unindexed updates can form HOT chains.
        self.db.execute(self.sql.SQL('ALTER TABLE {} SET (fillfactor = 50)').format(self.table))
        self.db.execute(self.sql.SQL('VACUUM FULL {}').format(self.table))
        self.db.execute(self.sql.SQL("UPDATE {} SET embedding = '[13.4,3,5]' "
                                    'WHERE id % 17 = 0').format(self.table))
        self.db.execute(self.sql.SQL('CREATE INDEX CONCURRENTLY documents_ann ON {} '
                                    'USING ann (embedding vector_l2_ops)').format(self.table))
        self.assert_oracle()
        self.db.execute(self.sql.SQL('VACUUM FULL {}').format(self.table))
        self.assert_oracle()

    def test_maximum_dimension(self):
        name = self.sql.Identifier(self.schema, 'wide_vectors')
        self.db.execute(self.sql.SQL('CREATE TABLE {} (v vector(2000))').format(name))
        vector = str([1] * 2000)
        self.db.execute(self.sql.SQL('INSERT INTO {} VALUES (%s::vector)').format(name), (vector,))
        self.db.execute(self.sql.SQL('CREATE INDEX ON {} USING ann (v vector_l2_ops)').format(name))
        self.db.execute(self.sql.SQL('INSERT INTO {} VALUES (%s::vector)').format(name), (vector,))
        rows = self.db.execute(self.sql.SQL('SELECT v <-> %s::vector FROM {} '
                                           'ORDER BY v <-> %s::vector').format(name), (vector, vector)).fetchall()
        self.assertEqual(rows, [(0.0,), (0.0,)])

    def test_unlogged_and_temporary(self):
        for kind in ['UNLOGGED', 'TEMPORARY']:
            name = self.sql.Identifier('ann_lifecycle_' + uuid.uuid4().hex)
            self.db.execute(self.sql.SQL('CREATE {} TABLE {} (embedding vector(3))').format(self.sql.SQL(kind), name))
            self.db.execute(self.sql.SQL('CREATE INDEX ON {} USING ann (embedding vector_l2_ops)').format(name))
            self.db.execute(self.sql.SQL("INSERT INTO {} VALUES ('[1,2,3]'), (NULL)").format(name))
            rows = self.db.execute(self.sql.SQL("SELECT embedding::text FROM {} ORDER BY embedding <-> '[0,0,0]'").format(name)).fetchall()
            self.assertEqual(rows, [('[1,2,3]',), (None,)])
            self.db.execute(self.sql.SQL('DROP TABLE {}').format(name))


if __name__ == '__main__':
    unittest.main()
