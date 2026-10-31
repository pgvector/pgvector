"""Networked GPU nodes: TCP transport, auth, routing, failover, and heartbeats.

Needs ANN_PG_DSN for a superuser on this host (the test starts local
networked workers); ANN_PSQL overrides psql. Uses the CPU worker, so it runs
without a GPU; ANN_INDEX_WORKER/ANN_INDEX_BACKEND select the cuVS build.
"""

import os
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_index import IndexWorkerTests, write_index  # noqa: E402
from test_server import ROOT, Server, frame, receive  # noqa: E402

DSN = os.environ.get('ANN_PG_DSN')
PSQL = os.environ.get('ANN_PSQL', 'psql')
TOKEN = 'test-token-' + uuid.uuid4().hex


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class Node:
    def __init__(self, directory):
        self.port = free_port()
        token = os.path.join(directory, f'token-{self.port}')
        with open(token, 'w') as out:
            out.write(TOKEN + '\n')
        self.server = Server('--backend', IndexWorkerTests.backend, '--listen-tcp', f'127.0.0.1:{self.port}',
                             '--auth-token-file', token, executable=IndexWorkerTests.worker)
        deadline = time.monotonic() + 5
        while True:
            try:
                socket.create_connection(('127.0.0.1', self.port), timeout=1).close()
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)

    @property
    def address(self):
        return f'127.0.0.1:{self.port}'

    def kill(self):
        self.server.process.kill()
        self.server.process.wait()

    def close(self):
        if self.server.process.poll() is None:
            self.server.close()


class WorkerAuthTests(unittest.TestCase):
    def test_unauthenticated_connections_are_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            node = Node(directory)
            try:
                with socket.create_connection(('127.0.0.1', node.port)) as connection:
                    connection.sendall(frame(8, 1))
                    self.assertEqual(connection.recv(32), b'')
                with socket.create_connection(('127.0.0.1', node.port)) as connection:
                    connection.sendall(frame(13, 1, b'wrong-token-value'))
                    self.assertEqual(connection.recv(32), b'')
                with socket.create_connection(('127.0.0.1', node.port)) as connection:
                    connection.sendall(frame(13, 1, TOKEN.encode()))
                    self.assertEqual(receive(connection)[2], 0)
                    # Shared memory is refused over the network.
                    connection.sendall(frame(9, 2, b'/pgann.x'))
                    self.assertEqual(receive(connection)[2], 2)
                    connection.sendall(frame(5, 3))
                    self.assertEqual(receive(connection)[2], 0)
            finally:
                node.close()

    def test_packed_filter_decoding(self):
        with tempfile.TemporaryDirectory() as directory:
            node = Node(directory)
            path = os.path.join(directory, 'index')
            keys, vectors = write_index(path, [-5, 3, 4, 300, 70000], [[1], [2], [3], [4], [5]])
            try:
                with socket.create_connection(('127.0.0.1', node.port)) as connection:
                    connection.sendall(frame(13, 1, TOKEN.encode()))
                    receive(connection)
                    connection.sendall(frame(11, 2, struct.pack('<I', 1) + b'i' + path.encode()))
                    self.assertEqual(receive(connection)[2], 0)

                    def varint(value):
                        out = b''
                        while True:
                            byte = value & 0x7f
                            value >>= 7
                            out += bytes([byte | (0x80 if value else 0)])
                            if not value:
                                return out
                    allowed = [-5, 4, 70000]
                    # zigzag(-5) = 9, then gaps 9 (to 4) and 69996 (to 70000).
                    packed = varint(9) + varint(9) + varint(69996)
                    payload = struct.pack('<I', 1) + b'i' + struct.pack('<IIII', 1, 3, len(allowed), len(packed))
                    payload += struct.pack('<f', 10.0) + packed
                    connection.sendall(frame(14, 3, payload))
                    _, _, status, data = receive(connection)
                    self.assertEqual(status, 0)
                    count = struct.unpack_from('<I', data)[0]
                    hits = [struct.unpack_from('<qf', data, 8 + i * 12)[0] for i in range(count)]
                    self.assertEqual(hits, [70000, 4, -5])  # values 5, 3, 1 from query 10
                    # A zero gap (duplicate key) is malformed.
                    bad = varint(10) + varint(0)
                    payload = struct.pack('<I', 1) + b'i' + struct.pack('<IIII', 1, 3, 2, len(bad))
                    payload += struct.pack('<f', 1.0) + bad
                    connection.sendall(frame(14, 4, payload))
                    self.assertEqual(receive(connection)[2], 1)
            finally:
                node.close()


@unittest.skipUnless(DSN, 'set ANN_PG_DSN for live PostgreSQL tests')
class RemoteSqlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(['make', '-C', os.path.join(ROOT, 'ann'), 'ann-worker'], check=True,
                       stdout=subprocess.DEVNULL)
        cls.directory = tempfile.TemporaryDirectory()
        cls.nodes = [Node(cls.directory.name), Node(cls.directory.name)]
        cls.table = 'ann_remote_' + uuid.uuid4().hex[:12]
        cls.psql(f"""
            CREATE TABLE {cls.table} (id bigint PRIMARY KEY, tenant int, emb vector(4));
            SELECT setseed(0.3);
            INSERT INTO {cls.table} SELECT i * 5, i % 20,
                (SELECT array_agg((random() * 4)::real) FROM generate_series(1, 4) WHERE i > 0)::vector
                FROM generate_series(1, 3000) i;
        """)
        rows = cls.psql(f'COPY (SELECT id, emb FROM {cls.table}) TO STDOUT').split('\n')
        keys, vectors = [], []
        for line in filter(None, rows):
            key, vector = line.split('\t')
            keys.append(int(key))
            vectors.append([float(x) for x in vector.strip('[]').split(',')])
        cls.path = os.path.join(cls.directory.name, 'index')
        write_index(cls.path, keys, vectors, shards=2, source=f'public.{cls.table}')
        cls.name = cls.table
        # Loads go to every node.
        cls.psql(f"SELECT ann_index_load('{cls.name}', '{cls.path}')")

    @classmethod
    def tearDownClass(cls):
        cls.psql(f"DROP TABLE {cls.table}; DELETE FROM ann_index_catalog WHERE name LIKE 'ann_remote_%'")
        for node in cls.nodes:
            node.close()
        cls.directory.cleanup()

    @classmethod
    def settings(cls, nodes=None):
        nodes = nodes or cls.nodes
        return (f"SET ann.worker_address = '{','.join(n.address for n in nodes)}';\n"
                f"SET ann.worker_auth_token = '{TOKEN}';\n")

    @classmethod
    def psql(cls, script, check=True, nodes=None):
        result = subprocess.run([PSQL, DSN, '-X', '-q', '-At', '-v', 'ON_ERROR_STOP=1'],
                                input=cls.settings(nodes) + script, capture_output=True, text=True, timeout=120)
        if check and result.returncode != 0:
            raise AssertionError(result.stderr)
        return result.stdout if check else result.stdout + result.stderr

    def mismatches(self, search, exact, where='q.id % 211 = 0'):
        return self.psql(f"""
            SELECT count(*) FILTER (WHERE a IS DISTINCT FROM b) FROM (
                SELECT ARRAY(SELECT s.id FROM {search} s ORDER BY s.distance, s.id) a,
                       ARRAY(SELECT t.id FROM {self.table} t {exact} ORDER BY t.emb <-> q.emb, t.id LIMIT 7) b
                FROM {self.table} q WHERE {where}) x""").strip()

    def test_ann_over_tcp(self):
        self.assertEqual(self.mismatches(f"ann_index_search('{self.name}', q.emb, 7)", ''), '0')

    def test_hybrid_routes_agree_with_sql(self):
        # 150 eligible rows per tenant: exact on this host at 1000, on the GPU node at 1.
        for threshold in (1000, 1):
            output = self.psql(f"""SET ann.gpu_min_eligible = {threshold};
                SELECT count(*) FILTER (WHERE a IS DISTINCT FROM b) FROM (
                    SELECT ARRAY(SELECT s.id FROM ann_hybrid_search('{self.name}', q.emb, 7,
                               format('tenant = %s', q.tenant)) s ORDER BY s.distance, s.id) a,
                           ARRAY(SELECT t.id FROM {self.table} t WHERE t.tenant = q.tenant
                               ORDER BY t.emb <-> q.emb, t.id LIMIT 7) b
                    FROM {self.table} q WHERE q.id % 211 = 0) x""").strip()
            self.assertEqual(output, '0', threshold)
        # The filter is one read-only statement.
        self.assertIn('ERROR', self.psql(
            f"SELECT * FROM ann_hybrid_search('{self.name}', '[1,1,1,1]', 3, 'true); DELETE FROM {self.table}; --')",
            check=False))
        self.assertEqual(self.psql(f'SELECT count(*) FROM {self.table}').strip(), '3000')

    def test_key_filter_routes_agree_with_sql(self):
        for threshold in (1000, 1):
            output = self.psql(f"""SET ann.gpu_min_eligible = {threshold};
                SELECT count(*) FILTER (WHERE a IS DISTINCT FROM b) FROM (
                    SELECT ARRAY(SELECT s.id FROM ann_index_search('{self.name}', q.emb, 7,
                               ARRAY(SELECT id FROM {self.table} WHERE tenant = q.tenant)) s ORDER BY s.distance, s.id) a,
                           ARRAY(SELECT t.id FROM {self.table} t WHERE t.tenant = q.tenant
                               ORDER BY t.emb <-> q.emb, t.id LIMIT 7) b
                    FROM {self.table} q WHERE q.id % 211 = 0) x""").strip()
            self.assertEqual(output, '0', threshold)

    def test_attach_and_catalog(self):
        self.assertEqual(self.psql(f"SELECT ann_index_attach('{self.name}')").strip(), '3000')
        self.assertEqual(self.psql(f"SELECT key_column || ',' || vector_column FROM ann_index_catalog "
                                   f"WHERE name = '{self.name}'").strip(), 'id,emb')
        self.assertIn('not in this database', self.psql(
            "SELECT * FROM ann_hybrid_search('missing', '[1,1,1,1]', 3, 'true')", check=False))

    def test_wrong_token_is_unavailable(self):
        output = self.psql(f"SET ann.worker_auth_token = 'nope';\n"
                           f"SELECT * FROM ann_index_search('{self.name}', '[1,1,1,1]', 3)", check=False)
        self.assertIn('unavailable', output)

    def test_failover_and_heartbeat(self):
        spare = Node(self.directory.name)
        try:
            self.psql(f"SELECT ann_index_load('{self.name}_spare', '{self.path}')", nodes=[spare])
            self.psql(f"SELECT ann_index_load('{self.name}_spare', '{self.path}')", nodes=[self.nodes[0]])
            query = f"SELECT count(*) FROM ann_index_search('{self.name}_spare', '[1,1,1,1]', 3);\n"
            process = subprocess.Popen([PSQL, DSN, '-X', '-q', '-At', '-v', 'ON_ERROR_STOP=1'],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True)
            # Connect to the spare only, then list a second node and kill the spare while idle.
            process.stdin.write(self.settings([spare]) + 'SET ann.worker_heartbeat_ms = 200;\n' + query)
            process.stdin.write(f"SET ann.worker_address = '{spare.address},{self.nodes[0].address}';\n")
            process.stdin.flush()
            time.sleep(1)
            spare.kill()
            time.sleep(0.5)
            started = time.monotonic()
            process.stdin.write(query)
            output, errors = process.communicate(timeout=60)
            self.assertEqual(process.returncode, 0, errors)
            self.assertEqual(output.split(), ['3', '3'])
            self.assertLess(time.monotonic() - started, 5)
        finally:
            spare.close()

    def test_all_nodes_down_is_unavailable(self):
        output = self.psql("SET ann.worker_address = '127.0.0.1:1';\n"
                           f"SELECT * FROM ann_index_search('{self.name}', '[1,1,1,1]', 3)", check=False)
        self.assertIn('unavailable', output)

if __name__ == '__main__':
    unittest.main()
