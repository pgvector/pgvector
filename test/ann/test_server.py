"""Pipelined Unix-socket server tests for gathered exact search."""

import json
import math
import os
import random
import socket
import struct
import subprocess
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WORKER = os.path.join(ROOT, 'ann', 'ann-worker')
GATHERED = 7
STATS = 8


class Server:
    def __init__(self, *args, env=None, executable=WORKER):
        self.directory = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.directory.name, 'ann.sock')
        self.process = subprocess.Popen([executable, '--listen', self.path, *args],
                                        stderr=subprocess.PIPE, env={**os.environ, **(env or {})})
        deadline = time.monotonic() + 5
        while not os.path.exists(self.path):
            if time.monotonic() > deadline or self.process.poll() is not None:
                raise RuntimeError('worker did not start')
            time.sleep(0.01)

    def connect(self):
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.connect(self.path)
        return connection

    def close(self):
        self.process.terminate()
        self.process.wait(5)
        self.process.stderr.close()
        self.directory.cleanup()


def frame(operation, request, payload=b''):
    return b'PVAN' + struct.pack('<HHQQII', 1, operation, request, 0, len(payload), 0) + payload


def gathered(query, vectors, k, metric=0):
    values = [x for row in [query] + vectors for x in row]
    return struct.pack('<IIII', len(vectors), len(query), k, metric) + struct.pack('<%df' % len(values), *values)


def receive(connection):
    def exact(size):
        data = b''
        while len(data) < size:
            chunk = connection.recv(size - len(data))
            if not chunk:
                raise EOFError
            data += chunk
        return data

    header = exact(32)
    assert header[:4] == b'PVAN'
    _, operation, request, _, size, status = struct.unpack('<HHQQII', header[4:])
    return operation, request, status, exact(size)


def decode(payload):
    count = struct.unpack_from('<I', payload)[0]
    return [struct.unpack_from('<Id', payload, 4 + i * 12) for i in range(count)]


def f32(value):
    return struct.unpack('<f', struct.pack('<f', value))[0]


def expected(query, vectors, k):
    distances = sorted((math.sqrt(sum((f32(a) - f32(b)) ** 2 for a, b in zip(row, query))), i)
                       for i, row in enumerate(vectors))
    if k >= len(distances):
        return [i for _, i in distances]
    threshold = distances[k - 1][0]
    threshold += 1e-3 * abs(threshold) + 1e-6
    return [i for d, i in distances if d <= threshold]


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(['make', '-C', os.path.join(ROOT, 'ann'), 'ann-worker'], check=True,
                       stdout=subprocess.DEVNULL)

    def setUp(self):
        self.server = Server('--backend', 'mock-gpu', '--lanes', '1', env={'ANN_MOCK_GPU_LAUNCH_US': '2000'})

    def tearDown(self):
        self.server.close()

    def stats(self, connection):
        connection.sendall(frame(STATS, 1))
        _, _, status, payload = receive(connection)
        self.assertEqual(status, 0)
        return json.loads(payload)

    def test_pipelined_requests_match_exact_results(self):
        rng = random.Random(7)
        connections = [self.server.connect() for _ in range(3)]
        sent = {}
        for request in range(1, 31):
            dimensions = 8
            vectors = [[rng.uniform(-1, 1) for _ in range(dimensions)] for _ in range(rng.randint(1, 300))]
            query = [rng.uniform(-1, 1) for _ in range(dimensions)]
            k = rng.randint(1, 20)
            connection = connections[request % 3]
            connection.sendall(frame(GATHERED, request, gathered(query, vectors, k)))
            sent[request] = (connection, query, vectors, k)
        for connection in connections:
            mine = {r for r, value in sent.items() if value[0] is connection}
            while mine:
                operation, request, status, payload = receive(connection)
                self.assertEqual((operation, status), (GATHERED, 0))
                self.assertIn(request, mine)
                mine.remove(request)
                _, query, vectors, k = sent[request]
                ids = [i for i, _ in decode(payload)]
                self.assertEqual(ids, expected(query, vectors, k))
        stats = self.stats(connections[0])
        self.assertEqual(stats['jobs'], 30)
        # One lane with a 2 ms launch forces later jobs to share submissions.
        self.assertLess(stats['batches'], 30)
        self.assertGreater(stats['max_batch'], 1)
        for connection in connections:
            connection.close()

    def test_ties_at_the_boundary_are_retained(self):
        with self.server.connect() as connection:
            vectors = [[1, 0], [0, 1], [-1, 0], [0, -1], [3, 3]]
            connection.sendall(frame(GATHERED, 9, gathered([0, 0], vectors, 1)))
            _, request, status, payload = receive(connection)
            self.assertEqual((request, status), (9, 0))
            self.assertEqual(sorted(i for i, _ in decode(payload)), [0, 1, 2, 3])

    def test_invalid_frames_fail_without_closing(self):
        with self.server.connect() as connection:
            connection.sendall(frame(GATHERED, 1, struct.pack('<IIII', 2, 3, 1, 0) + b'\0' * 4))
            connection.sendall(frame(GATHERED, 2, gathered([0, 0], [[float('nan'), 0]], 1)))
            connection.sendall(frame(4, 3))
            connection.sendall(frame(GATHERED, 4, gathered([0], [[2], [1]], 1)))
            replies = {}
            for _ in range(4):
                _, request, status, payload = receive(connection)
                replies[request] = (status, payload)
            self.assertEqual(replies[1][0], 1)
            self.assertEqual(replies[2][0], 1)
            self.assertEqual(replies[3][0], 2)
            self.assertEqual(replies[4][0], 0)
            self.assertEqual(decode(replies[4][1]), [(1, 1.0)])

    def test_admission_rejects_over_budget(self):
        self.server.close()
        self.server = Server('--max-pending-mb', '1')
        with self.server.connect() as connection:
            vectors = [[0.0] * 512 for _ in range(1024)]
            connection.sendall(frame(GATHERED, 1, gathered([0.0] * 512, vectors, 1)))
            _, _, status, _ = receive(connection)
            self.assertEqual(status, 7)
            self.assertEqual(self.stats(connection)['rejected'], 1)

    def test_disconnect_with_pending_work(self):
        connection = self.server.connect()
        vectors = [[float(i)] * 4 for i in range(2000)]
        for request in range(10):
            connection.sendall(frame(GATHERED, request, gathered([0.0] * 4, vectors, 5)))
        connection.close()
        with self.server.connect() as other:
            other.sendall(frame(GATHERED, 1, gathered([0.0], [[1.0]], 1)))
            self.assertEqual(receive(other)[2], 0)
        self.assertIsNone(self.server.process.poll())


if __name__ == '__main__':
    unittest.main()
