"""Opt-in native filtered CustomScan tests against ordinary PostgreSQL SQL."""

import os
import unittest
import uuid


@unittest.skipUnless(os.environ.get('ANN_PG_DSN'), 'set ANN_PG_DSN for live PostgreSQL tests')
class FilteredTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        from psycopg import sql

        self.psycopg = psycopg
        self.sql = sql
        self.dsn = os.environ['ANN_PG_DSN']
        self.db = psycopg.connect(self.dsn, autocommit=True, prepare_threshold=None)
        self.db.execute("LOAD 'vector'")
        self.db.execute('SET ann.enable_filtered_search = on')
        self.db.execute('SET max_parallel_workers_per_gather = 0')
        self.schema = 'ann_filtered_' + uuid.uuid4().hex
        self.table = sql.Identifier(self.schema, 'documents')
        self.db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
        self.db.execute(sql.SQL('CREATE TABLE {} (id integer, tenant text, terms tsvector, '
                                'embedding vector(3))').format(self.table))
        self.db.execute(sql.SQL("INSERT INTO {} SELECT i, CASE WHEN i % 101 = 0 THEN 'rare' "
                                "ELSE 'common' END, to_tsvector('simple', CASE WHEN i % 101 = 0 "
                                "THEN 'database search' ELSE 'storage' END), "
                                'ARRAY[i * 0.123, i % 13, i % 17]::vector '
                                'FROM generate_series(1, 4096) i').format(self.table))
        self.db.execute(sql.SQL('CREATE INDEX documents_ann ON {} USING ann '
                                '(embedding vector_l2_ops)').format(self.table))
        self.db.execute(sql.SQL('CREATE INDEX documents_tenant ON {} (tenant)').format(self.table))
        self.db.execute(sql.SQL('CREATE INDEX documents_terms ON {} USING gin (terms)').format(self.table))
        self.db.execute(sql.SQL('ANALYZE {}').format(self.table))

    def tearDown(self):
        self.db.execute('RESET ROLE')
        self.db.execute(self.sql.SQL('DROP SCHEMA {} CASCADE').format(self.sql.Identifier(self.schema)))
        self.db.close()

    def statement(self, predicate="tenant = 'rare'", ending='LIMIT 10', projection='id'):
        return self.sql.SQL('SELECT {}, embedding <-> %s::vector AS distance FROM {} '
                            'WHERE {} ORDER BY distance, id {}').format(
                                self.sql.SQL(projection), self.table,
                                self.sql.SQL(predicate), self.sql.SQL(ending))

    @staticmethod
    def custom_nodes(plan):
        nodes = []
        if plan.get('Custom Plan Provider') == 'AnnFilteredSearch':
            nodes.append(plan)
        for child in plan.get('Plans', []):
            nodes.extend(FilteredTests.custom_nodes(child))
        return nodes

    def explain(self, statement, params=('[13.4,3,5]',), analyze=True, db=None):
        db = db or self.db
        prefix = 'EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF)' if analyze else 'EXPLAIN (FORMAT JSON)'
        return db.execute(self.sql.SQL('{} {}').format(self.sql.SQL(prefix), statement), params).fetchone()[0][0]['Plan']

    def assert_oracle(self, statement, params=('[13.4,3,5]',), db=None):
        db = db or self.db
        db.execute('SET ann.enable_filtered_search = on')
        plan = self.explain(statement, params, db=db)
        nodes = self.custom_nodes(plan)
        self.assertEqual(len(nodes), 1, plan)
        actual = db.execute(statement, params).fetchall()
        db.execute('SET ann.enable_filtered_search = off')
        db.execute('SET enable_indexscan = off')
        db.execute('SET enable_bitmapscan = off')
        expected = db.execute(statement, params).fetchall()
        db.execute('SET enable_indexscan = on')
        db.execute('SET enable_bitmapscan = on')
        db.execute('SET ann.enable_filtered_search = on')
        self.assertEqual(actual, expected)
        return nodes[0], actual

    def test_btree_eligibility_and_projection(self):
        node, rows = self.assert_oracle(self.statement(projection='id, upper(tenant), embedding'))
        self.assertEqual(len(rows), 10)
        self.assertEqual(node['Eligible Rows'], 40)
        self.assertEqual(node['Distance Evaluations'], 40)
        self.assertIn('documents_tenant', str(node['Plans']))
        self.assertEqual(node['Sort Method'], 'top-N heapsort')

    def test_gin_and_combined_filters(self):
        for predicate in ["terms @@ to_tsquery('simple', 'database & search')",
                          "tenant = 'rare' AND terms @@ to_tsquery('simple', 'database')",
                          "tenant = 'missing'"]:
            node, _ = self.assert_oracle(self.statement(predicate))
            self.assertEqual(node['Eligible Rows'], 0 if 'missing' in predicate else 40)
        self.db.execute('SET enable_seqscan = off')
        self.db.execute('SET enable_indexscan = off')
        node = self.custom_nodes(self.explain(self.statement(
            "terms @@ to_tsquery('simple', 'database & search')")))[0]
        self.assertIn('documents_terms', str(node['Plans']))
        self.assertEqual(node['Distance Evaluations'], 40)

    def test_ties_nulls_offsets_and_spill(self):
        self.db.execute(self.sql.SQL("UPDATE {} SET embedding = '[1,2,3]' "
                                    'WHERE id IN (101,202,303,404,505)').format(self.table))
        self.db.execute(self.sql.SQL("INSERT INTO {} VALUES (5000, 'rare', NULL, NULL)").format(self.table))
        for ending in ['LIMIT 3 OFFSET 2', 'LIMIT ALL OFFSET 3', 'LIMIT 5000', 'LIMIT NULL']:
            self.assert_oracle(self.statement(ending=ending), ('[1,2,3]',))
        self.db.execute("SET work_mem = '64kB'")
        node, rows = self.assert_oracle(self.statement("tenant IS NOT NULL", ''))
        self.assertEqual(node['Sort Space Type'], 'Disk')
        self.assertEqual(len(rows), 4097)
        self.assertEqual(rows[-1][0], 5000)
        statement = self.sql.SQL("SELECT id FROM {} WHERE tenant = 'rare' "
                                 "ORDER BY embedding <-> '[1,2,3]' FETCH FIRST 2 ROWS WITH TIES").format(self.table)
        node = self.custom_nodes(self.explain(statement, ()))[0]
        self.assertNotEqual(node['Sort Method'], 'top-N heapsort')
        self.assertEqual(sorted(row[0] for row in self.db.execute(statement)), [101, 202, 303, 404, 505])

    def test_generic_prepared_parameters(self):
        self.db.execute('SET plan_cache_mode = force_generic_plan')
        self.db.execute(self.sql.SQL('PREPARE filtered_query(vector, bigint, bigint) AS '
                                    "SELECT id, embedding <-> $1 AS distance FROM {} WHERE tenant = 'rare' "
                                    'ORDER BY distance, id LIMIT $2 OFFSET $3').format(self.table))
        for query, limit, offset in [('[13.4,3,5]', 2, 0), ('[100,2,3]', 19, 3),
                                     (None, 5, 4), ('[0,0,0]', None, 7), ('[1,2,3]', 0, 0)]:
            execute = self.sql.SQL('EXECUTE filtered_query({}, {}, {})').format(
                self.sql.Literal(query), self.sql.Literal(limit), self.sql.Literal(offset))
            actual = self.db.execute(execute).fetchall()
            plan = self.explain(execute, ())
            self.assertEqual(len(self.custom_nodes(plan)), 1, plan)
            self.db.execute('SET ann.enable_filtered_search = off')
            expected = self.db.execute(self.statement(ending='LIMIT %s OFFSET %s'), (query, limit, offset)).fetchall()
            self.db.execute('SET ann.enable_filtered_search = on')
            self.assertEqual(actual, expected)
        for limit, offset in [(-1, 0), (1, -1)]:
            with self.assertRaises(self.psycopg.errors.InvalidRowCountInLimitClause if limit < 0
                                   else self.psycopg.errors.InvalidRowCountInResultOffsetClause):
                self.db.execute(self.sql.SQL('EXECUTE filtered_query(NULL, {}, {})').format(
                    self.sql.Literal(limit), self.sql.Literal(offset)))

    def test_fuzzy_gin_and_cached_plan_guard(self):
        self.db.execute('SET plan_cache_mode = force_generic_plan')
        self.db.execute(self.sql.SQL('PREPARE guarded_query(vector) AS '
                                    "SELECT id FROM {} WHERE tenant = 'rare' "
                                    'ORDER BY embedding <-> $1 LIMIT 10').format(self.table))
        execute = self.sql.SQL("EXECUTE guarded_query('[1,2,3]')")
        self.assertEqual(len(self.custom_nodes(self.explain(execute, ()))), 1)
        self.db.execute('SET gin_fuzzy_search_limit = 100')
        self.assertEqual(self.custom_nodes(self.explain(self.statement(), analyze=False)), [])
        with self.assertRaisesRegex(self.psycopg.errors.FeatureNotSupported, 'gin_fuzzy_search_limit'):
            self.db.execute(execute)
        self.db.execute('SET gin_fuzzy_search_limit = 0')
        self.assertEqual(len(self.db.execute(execute).fetchall()), 10)

    def test_snapshot_visibility_and_writes(self):
        with self.psycopg.connect(self.dsn, autocommit=True, prepare_threshold=None) as reader:
            reader.execute("LOAD 'vector'")
            reader.execute('SET max_parallel_workers_per_gather = 0')
            reader.execute('BEGIN ISOLATION LEVEL REPEATABLE READ')
            _, original = self.assert_oracle(self.statement(), db=reader)
            self.db.execute(self.sql.SQL("DELETE FROM {} WHERE id = 101").format(self.table))
            self.db.execute(self.sql.SQL("UPDATE {} SET embedding = '[0,0,0]' WHERE id = 202").format(self.table))
            self.db.execute(self.sql.SQL('VACUUM {}').format(self.table))
            _, old = self.assert_oracle(self.statement(), db=reader)
            self.assertEqual(old, original)
            reader.execute('COMMIT')
        self.db.execute('BEGIN')
        self.db.execute(self.sql.SQL("INSERT INTO {} VALUES (9000, 'rare', NULL, '[13.4,3,5]')").format(self.table))
        _, rows = self.assert_oracle(self.statement())
        self.assertEqual(rows[0][0], 9000)
        self.db.execute('ROLLBACK')
        self.assert_oracle(self.statement())

    def test_rls_child_plan(self):
        allowed = self.db.execute("SELECT rolsuper OR rolcreaterole FROM pg_roles WHERE rolname = current_user").fetchone()[0]
        if not allowed:
            self.skipTest('RLS fixture needs permission to create a temporary role')
        role = self.sql.Identifier('ann_rls_' + uuid.uuid4().hex)
        self.db.execute(self.sql.SQL('CREATE ROLE {}').format(role))
        try:
            self.db.execute(self.sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(self.sql.Identifier(self.schema), role))
            self.db.execute(self.sql.SQL('GRANT SELECT ON {} TO {}').format(self.table, role))
            self.db.execute(self.sql.SQL('ALTER TABLE {} ENABLE ROW LEVEL SECURITY').format(self.table))
            self.db.execute(self.sql.SQL("CREATE POLICY tenant_policy ON {} USING (tenant = 'rare')").format(self.table))
            self.db.execute(self.sql.SQL('SET ROLE {}').format(role))
            node, rows = self.assert_oracle(self.statement('id > 0'))
            self.assertEqual(node['Eligible Rows'], 40)
            self.assertEqual(len(rows), 10)
        finally:
            self.db.execute('RESET ROLE')
            self.db.execute(self.sql.SQL('DROP OWNED BY {}').format(role))
            self.db.execute(self.sql.SQL('DROP ROLE {}').format(role))

    def test_distance_function_execute_permission(self):
        allowed = self.db.execute("SELECT rolsuper OR rolcreaterole FROM pg_roles WHERE rolname = current_user").fetchone()[0]
        if not allowed:
            self.skipTest('permission fixture needs a temporary role')
        function = self.sql.Identifier(self.schema, 'private_distance')
        operator = self.sql.SQL('OPERATOR({}.<~>)').format(self.sql.Identifier(self.schema))
        self.db.execute(self.sql.SQL('CREATE FUNCTION {} (vector, vector) RETURNS double precision '
                                    'LANGUAGE plpgsql IMMUTABLE STRICT AS '
                                    '$$BEGIN RETURN l2_distance($1, $2); END$$').format(function))
        self.db.execute(self.sql.SQL('CREATE OPERATOR {}.<~> (LEFTARG = vector, RIGHTARG = vector, '
                                    'FUNCTION = {})').format(self.sql.Identifier(self.schema), function))
        opclass = self.sql.Identifier(self.schema, 'private_ops')
        self.db.execute(self.sql.SQL('CREATE OPERATOR CLASS {} FOR TYPE vector USING ann AS '
                                    'OPERATOR 1 {}.<~> (vector, vector) FOR ORDER BY float_ops, '
                                    'FUNCTION 1 {} (vector, vector)').format(opclass, self.sql.Identifier(self.schema), function))
        self.db.execute(self.sql.SQL('CREATE INDEX ON {} USING ann (embedding {})').format(self.table, opclass))
        self.db.execute(self.sql.SQL('REVOKE EXECUTE ON FUNCTION {} (vector, vector) FROM PUBLIC').format(function))
        statement = self.sql.SQL("SELECT id, embedding {} %s::vector AS distance FROM {} WHERE tenant = 'rare' "
                                 'ORDER BY distance, id LIMIT 10').format(operator, self.table)
        role = self.sql.Identifier('ann_acl_' + uuid.uuid4().hex)
        self.db.execute(self.sql.SQL('CREATE ROLE {}').format(role))
        try:
            self.db.execute(self.sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(self.sql.Identifier(self.schema), role))
            self.db.execute(self.sql.SQL('GRANT SELECT ON {} TO {}').format(self.table, role))
            self.db.execute(self.sql.SQL('SET ROLE {}').format(role))
            for enabled in ['on', 'off']:
                self.db.execute('SET ann.enable_filtered_search = ' + enabled)
                with self.assertRaises(self.psycopg.errors.InsufficientPrivilege):
                    self.db.execute(statement, ('[1,2,3]',))
            self.db.execute('RESET ROLE')
            self.db.execute(self.sql.SQL('GRANT EXECUTE ON FUNCTION {} (vector, vector) TO {}').format(function, role))
            self.db.execute(self.sql.SQL('SET ROLE {}').format(role))
            self.assert_oracle(statement, ('[1,2,3]',))
        finally:
            self.db.execute('RESET ROLE')
            self.db.execute(self.sql.SQL('DROP OWNED BY {}').format(role))
            self.db.execute(self.sql.SQL('DROP ROLE {}').format(role))

    def test_index_only_fallback(self):
        self.db.execute(self.sql.SQL('DROP INDEX {}').format(self.sql.Identifier(self.schema, 'documents_tenant')))
        self.db.execute(self.sql.SQL('CREATE INDEX documents_cover ON {} (tenant, id) '
                                    'INCLUDE (embedding)').format(self.table))
        self.db.execute(self.sql.SQL('VACUUM ANALYZE {}').format(self.table))
        self.db.execute('SET enable_seqscan = off')
        self.db.execute('SET enable_bitmapscan = off')
        plan = self.explain(self.statement())
        self.assertIn('Index Only Scan', str(plan))
        self.assertEqual(self.custom_nodes(plan), [])

    def test_toasted_vectors(self):
        wide = self.sql.Identifier(self.schema, 'wide_documents')
        self.db.execute(self.sql.SQL('CREATE TABLE {} (id integer, tenant text, '
                                    'embedding vector(2000))').format(wide))
        vector = str([i / 2000 for i in range(2000)])
        for i in range(8):
            self.db.execute(self.sql.SQL("INSERT INTO {} VALUES (%s, 'rare', %s::vector)").format(wide), (i, vector))
        self.db.execute(self.sql.SQL('CREATE INDEX ON {} USING ann (embedding vector_l2_ops)').format(wide))
        old_table = self.table
        self.table = wide
        try:
            node, rows = self.assert_oracle(self.statement(ending='LIMIT 5'), (vector,))
            self.assertEqual(node['Distance Evaluations'], 8)
            self.assertEqual(rows, [(i, 0.0) for i in range(5)])
        finally:
            self.table = old_table

    def test_cancellation_cleanup(self):
        function = self.sql.Identifier(self.schema, 'slow_eligible')
        self.db.execute(self.sql.SQL('CREATE FUNCTION {} (i integer) RETURNS boolean LANGUAGE plpgsql STABLE AS '
                                    "$$BEGIN PERFORM pg_sleep(0.002); RETURN i > 0; END$$").format(function))
        statement = self.sql.SQL("SELECT id FROM {} WHERE tenant = 'rare' AND {}(id) "
                                 "ORDER BY embedding <-> '[1,2,3]' LIMIT 10").format(self.table, function)
        self.assertEqual(len(self.custom_nodes(self.explain(statement, (), analyze=False))), 1)
        self.db.execute("SET statement_timeout = '20ms'")
        with self.assertRaises(self.psycopg.errors.QueryCanceled):
            self.db.execute(statement)
        self.db.execute('SET statement_timeout = 0')
        self.assert_oracle(self.statement())

    def test_unsupported_shapes_and_disabled_path(self):
        statements = [
            self.statement(ending='LIMIT 10 FOR UPDATE'),
            self.sql.SQL("SELECT id FROM {} WHERE tenant = 'rare' ORDER BY embedding <-> %s::vector DESC LIMIT 10").format(self.table),
            self.sql.SQL("SELECT id FROM {} WHERE tenant = 'rare' ORDER BY embedding <-> %s::vector NULLS FIRST LIMIT 10").format(self.table),
            self.sql.SQL('SELECT id FROM {} ORDER BY embedding <-> %s::vector LIMIT 10').format(self.table),
        ]
        for statement in statements:
            self.assertEqual(self.custom_nodes(self.explain(statement, analyze=False)), [])
            self.db.execute(statement, ('[13.4,3,5]',)).fetchall()
        self.db.execute('SET ann.enable_filtered_search = off')
        self.assertEqual(self.custom_nodes(self.explain(self.statement(), analyze=False)), [])
        self.db.execute('SET ann.enable_filtered_search = on')
        self.db.execute(self.sql.SQL('DROP INDEX {}').format(self.sql.Identifier(self.schema, 'documents_ann')))
        self.assertEqual(self.custom_nodes(self.explain(self.statement(), analyze=False)), [])


if __name__ == '__main__':
    unittest.main()
