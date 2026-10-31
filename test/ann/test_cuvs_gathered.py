"""Gathered search on a real GPU through the cuVS worker build.

Set ANN_CUVS_TEST=1 after `make -C ann ann-worker-cuvs`; see ann/README.md.
"""

import json
import os
import random
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_server import (GATHERED, ROOT, STATS, Server, decode, expected,  # noqa: E402
                         frame, gathered, receive)

EXECUTABLE = os.path.join(ROOT, 'ann', 'ann-worker-cuvs')


@unittest.skipUnless(os.environ.get('ANN_CUVS_TEST'), 'set ANN_CUVS_TEST=1 on a cuVS host')
class CuvsGatheredTests(unittest.TestCase):
    def setUp(self):
        self.server = Server('--backend', 'cuvs', '--lanes', '4', executable=EXECUTABLE)

    def tearDown(self):
        self.server.close()

    def device_jobs(self, connection):
        connection.sendall(frame(STATS, 1))
        return json.loads(receive(connection)[3])['device_jobs']

    def search(self, connection, requests):
        for request, (query, vectors, k) in requests.items():
            connection.sendall(frame(GATHERED, request, gathered(query, vectors, k)))
        replies = {}
        while len(replies) < len(requests):
            _, request, status, payload = receive(connection)
            self.assertEqual(status, 0)
            replies[request] = [i for i, _ in decode(payload)]
        return replies

    def test_matches_exact_results_on_device(self):
        rng = random.Random(11)
        requests = {}
        for request in range(1, 13):
            dimensions = rng.choice([3, 16, 96])
            rows = rng.randint(2048, 5000)
            vectors = [[round(rng.uniform(-2, 2), 2) for _ in range(dimensions)] for _ in range(rows)]
            query = [round(rng.uniform(-2, 2), 2) for _ in range(dimensions)]
            requests[request] = (query, vectors, rng.choice([1, 10, 100, 400]))
        with self.server.connect() as connection:
            replies = self.search(connection, requests)
            for request, (query, vectors, k) in requests.items():
                # Order among near-equal distances is not significant; callers re-sort.
                self.assertEqual(sorted(replies[request]), sorted(expected(query, vectors, k)), request)
            self.assertEqual(self.device_jobs(connection), len(requests))

    def test_heavy_ties_stay_exact(self):
        # Ties beyond the GPU candidate window must fall back to the CPU kernel.
        vectors = [[1.0, 0.0]] * 3000 + [[5.0, 5.0]] * 100
        with self.server.connect() as connection:
            replies = self.search(connection, {1: ([0.0, 0.0], vectors, 5)})
            self.assertEqual(sorted(replies[1]), list(range(3000)))

    def test_small_jobs_use_the_cpu(self):
        with self.server.connect() as connection:
            replies = self.search(connection, {1: ([0.0], [[2.0], [1.0], [3.0]], 1)})
            self.assertEqual(replies[1], [1])
            self.assertEqual(self.device_jobs(connection), 0)


if __name__ == '__main__':
    unittest.main()
