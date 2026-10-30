"""GPU tests skip by default, but fail on missing SDK/device when explicitly enabled."""

import os
from pathlib import Path
import random
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from ann.client import Worker, WorkerError


@unittest.skipUnless(os.environ.get('ANN_CUVS_TEST') == '1', 'set ANN_CUVS_TEST=1 on a cuVS host')
class CuvsTests(unittest.TestCase):
    def test_filtered_recall_reload_and_capabilities(self):
        rng = random.Random(23)
        vectors = [[rng.random() for _ in range(32)] for _ in range(4096)]
        queries = vectors[::1024]
        filters = [list(range(i, 4096, 4)) for i in range(4)]
        with Worker() as cpu, Worker(ROOT / 'ann/ann-worker-cuvs', 'cuvs') as gpu:
            caps = gpu.capabilities()
            self.assertFalse(caps['exact'])
            self.assertEqual(caps['metrics_mask'], 1)
            self.assertEqual(caps['algorithm'], 'cagra')
            cpu.build(1, vectors)
            gpu.build(1, vectors)
            for allowed, private in [(None, False), (filters[0], False), (filters, True)]:
                truth = cpu.search(1, queries, 10, allowed=allowed, per_query=private)
                actual = gpu.search(1, queries, 10, allowed=allowed, per_query=private)
                for i, (expected, found) in enumerate(zip(truth, actual)):
                    self.assertEqual(len(found), 10)
                    expected_ids = {r[0] for r in expected}
                    self.assertGreaterEqual(len(expected_ids & {r[0] for r in found}) / 10, .9)
                    if allowed is not None:
                        eligible = allowed[i] if private else allowed
                        self.assertTrue(all(r[0] in eligible for r in found))
                    self.assertEqual(found, sorted(found, key=lambda r: (r[1], r[0])))
            self.assertEqual(gpu.search(1, queries, 10, allowed=[]), [[]] * 4)
            with self.assertRaises(WorkerError) as error:
                gpu.search(1, queries, 10, metric=2)
            self.assertEqual(error.exception.status, 2)
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'canonical'
                gpu.save(1, path)
                gpu.unload(1)
                gpu.load(2, path)
                self.assertEqual(gpu.search(2, [vectors[0]], 1)[0][0], (0, 0))

    def test_failed_build_preserves_generation(self):
        vectors = [[float(i), 1] for i in range(256)]
        with Worker(ROOT / 'ann/ann-worker-cuvs', 'cuvs') as gpu:
            gpu.build(1, vectors)
            vectors[0][0] = float('nan')
            with self.assertRaises(WorkerError):
                gpu.build(2, vectors)
            self.assertEqual(gpu.search(1, [[0, 1]], 1), [[(0, 0)]])


if __name__ == '__main__':
    unittest.main()
