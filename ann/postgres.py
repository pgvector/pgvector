"""Experimental frozen heap-table adapter, not a PostgreSQL index access method."""

import json
import time

from .client import MAX_BYTES, Worker


class FrozenIndex:
    """Own a table freeze and private read-only transaction for a worker generation.

    Predicates are trusted psycopg SQL expressions, with values bound separately.
    Keys must be unique non-null integers and vectors non-null FP32 pgvector values.
    """

    def __init__(self, dsn, table, schema='public', key='id', vector='embedding',
                 executable=None, backend='cpu-exact', max_rows=100000,
                 worker_timeout=30, statement_timeout_ms=30000):
        self.dsn = dsn
        self.table = table
        self.schema = schema
        self.key = key
        self.vector = vector
        self.executable = executable
        self.backend = backend
        self.max_rows = max_rows
        self.worker_timeout = worker_timeout
        self.statement_timeout_ms = statement_timeout_ms
        self.last_search = None
        self._connection = None
        self._worker = None
        self._keys = []
        self._ordinals = {}

    def __enter__(self):
        import psycopg
        from psycopg import sql

        if self._connection is not None:
            raise RuntimeError('index context is already active')
        if self.max_rows <= 0:
            raise ValueError('max_rows must be positive')
        if not isinstance(self.statement_timeout_ms, int) or self.statement_timeout_ms <= 0:
            raise ValueError('statement timeout must be a positive integer')
        self._keys = []
        self._ordinals = {}
        self._connection = psycopg.connect(self.dsn)
        try:
            self._connection.execute("SELECT set_config('statement_timeout', %s, true)",
                                     (str(self.statement_timeout_ms),))
            relation = sql.Identifier(self.schema, self.table)
            # Lock first, then prohibit this private connection's own writes too.
            self._connection.execute(sql.SQL('LOCK TABLE {} IN SHARE MODE').format(relation))
            self._connection.execute('SET TRANSACTION READ ONLY')
            self._connection.execute('SET LOCAL gin_fuzzy_search_limit = 0')
            metadata = self._connection.execute(
                'SELECT c.relkind, am.amname, a.atttypmod, t.typname '
                'FROM pg_class c JOIN pg_am am ON am.oid = c.relam '
                'JOIN pg_attribute a ON a.attrelid = c.oid '
                'JOIN pg_type t ON t.oid = a.atttypid '
                'WHERE c.oid = %s::regclass AND a.attname = %s AND NOT a.attisdropped',
                (relation.as_string(self._connection), self.vector)).fetchone()
            if metadata is None or metadata[:2] != ('r', 'heap') or metadata[3] != 'vector':
                raise ValueError('only heap tables with a vector column are supported')
            dimensions = metadata[2] if metadata[2] > 0 else None
            vectors = []
            query = sql.SQL('SELECT {}, {}::text FROM {} ORDER BY {}').format(
                sql.Identifier(self.key), sql.Identifier(self.vector), relation,
                sql.Identifier(self.key))
            with self._connection.cursor(name='ann_build') as cursor:
                cursor.execute(query)
                for key, value in cursor:
                    if not isinstance(key, int) or isinstance(key, bool) or key in self._ordinals:
                        raise ValueError('keys must be unique non-null integers')
                    if value is None:
                        raise ValueError('null vectors are not supported by this prototype')
                    vector = json.loads(value)
                    if dimensions is None:
                        dimensions = len(vector)
                    if len(vector) != dimensions:
                        raise ValueError('inconsistent vector dimensions')
                    rows = len(vectors) + 1
                    if rows > self.max_rows or 8 + rows * dimensions * 4 > MAX_BYTES:
                        raise ValueError('prototype corpus materialization budget exceeded')
                    self._ordinals[key] = len(self._keys)
                    self._keys.append(key)
                    vectors.append(vector)
            if dimensions is None:
                raise ValueError('empty unconstrained vector column has no known dimension')
            self._worker = Worker(self.executable, self.backend, timeout=self.worker_timeout)
            self._worker.build(1, vectors, dimensions=dimensions)
            return self
        except BaseException:
            self.close()
            raise

    def search(self, queries, k, predicate=None, params=(), metric=0):
        try:
            return self._search(queries, k, predicate, params, metric)
        except BaseException:
            self.last_search = None
            self.close()
            raise

    def _search(self, queries, k, predicate, params, metric):
        from psycopg import sql

        if self._connection is None or self._worker is None:
            raise RuntimeError('index context is not active')
        self.last_search = None
        started = time.perf_counter()
        if predicate is None:
            predicate = sql.SQL('TRUE')
        if not isinstance(predicate, sql.Composable):
            raise TypeError('predicate must be a trusted psycopg SQL expression')
        query = sql.SQL('SELECT {} FROM {} WHERE {}').format(
            sql.Identifier(self.key), sql.Identifier(self.schema, self.table), predicate)
        allowed = []
        with self._connection.cursor(name='ann_eligible') as cursor:
            cursor.execute(query, params)
            for (key,) in cursor:
                if key not in self._ordinals:
                    raise RuntimeError('source identity changed while frozen')
                allowed.append(self._ordinals[key])
        # Heap/key order is not a promise of local ordinal order.
        allowed.sort()
        prepared = time.perf_counter()
        try:
            result = self._worker.search(1, queries, k, metric=metric, allowed=allowed)
            searched = time.perf_counter()
            # Detect loss of the lock-owning session while external work was running.
            self._connection.execute('SELECT 1').fetchone()
        except BaseException:
            self.close()  # A failed external request must not retain the freeze.
            raise
        result = [[(self._keys[ordinal], distance) for ordinal, distance in row] for row in result]
        finished = time.perf_counter()
        self.last_search = {
            'eligible_rows': len(allowed), 'query_vectors': len(queries),
            'eligibility_mapping_seconds': prepared - started,
            'worker_roundtrip_seconds': searched - prepared,
            'session_check_projection_seconds': finished - searched,
            'total_seconds': finished - started,
        }
        return result

    def close(self):
        try:
            if self._worker is not None:
                self._worker.close()
        finally:
            self._worker = None
            try:
                if self._connection is not None:
                    self._connection.close()  # Rolls back and releases the table freeze.
            finally:
                self._connection = None
                self._keys = []
                self._ordinals = {}

    def __exit__(self, *args):
        self.close()
