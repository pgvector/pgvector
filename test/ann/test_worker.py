import math
from pathlib import Path
import random
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'test/ann'))
from support.client import HEADER, Worker, WorkerError


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.worker = Worker()

    def tearDown(self):
        self.worker.close()

    def assertStatus(self, status, function, *args, **kwargs):
        with self.assertRaises(WorkerError) as error:
            function(*args, **kwargs)
        self.assertEqual(error.exception.status, status)

    def test_capabilities(self):
        caps = self.worker.capabilities()
        self.assertTrue(caps['exact'])
        self.assertEqual(caps['backend'], 'cpu-exact')
        self.assertEqual(caps['filters_mask'], 7)
        self.assertEqual(caps['metrics_mask'], 7)
        self.assertEqual(caps['storage'], 'fp32')

    def test_deadline_bounds_blocked_reads_and_writes(self):
        for payload in [b'', bytes(1024 * 1024)]:
            child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            with patch('support.client.subprocess.Popen', return_value=child):
                with Worker(timeout=.05) as stalled:
                    with self.assertRaises(TimeoutError):
                        stalled.request(0, payload=payload)
                    self.assertIsNotNone(child.poll())
                    with self.assertRaises(RuntimeError):
                        stalled.capabilities()

    def test_transport_corruption_invalidates_session(self):
        code = ('import sys; sys.stdin.buffer.read(32); '
                'sys.stdout.buffer.write(bytes(32)); sys.stdout.buffer.flush()')
        child = subprocess.Popen([sys.executable, '-c', code],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        with patch('support.client.subprocess.Popen', return_value=child):
            with Worker(timeout=2) as broken:
                with self.assertRaises(ValueError):
                    broken.capabilities()
                with self.assertRaises(RuntimeError):
                    broken.capabilities()

    def test_filtered_batches_against_independent_oracle(self):
        rng = random.Random(42)
        vectors = [[rng.randint(-5, 5) for _ in range(7)] for _ in range(80)]
        queries = [[rng.randint(1, 5) for _ in range(7)] for _ in range(5)]
        filters = [sorted(rng.sample(range(80), n)) for n in [0, 1, 8, 25, 80]]
        self.worker.build(1, vectors)
        for metric in range(3):
            for k in [0, 1, 7, 100]:
                actual = self.worker.search(1, queries, k, metric, filters, True)
                for query, ids, row in zip(queries, filters, actual):
                    expected = []
                    for i in ids:
                        vector = vectors[i]
                        dot = sum(a * b for a, b in zip(vector, query))
                        if metric == 0:
                            distance = math.sqrt(sum((a - b) ** 2 for a, b in zip(vector, query)))
                        elif metric == 1:
                            distance = -dot
                        else:
                            norm = sum(a * a for a in vector) * sum(b * b for b in query)
                            if not norm:
                                continue
                            distance = 1 - max(-1, min(1, dot / math.sqrt(norm)))
                        expected.append((i, distance))
                    expected.sort(key=lambda x: (x[1], x[0]))
                    self.assertEqual([r[0] for r in row], [r[0] for r in expected[:k]])
                    for found, want in zip(row, expected[:k]):
                        self.assertAlmostEqual(found[1], want[1], places=12)

    def test_shared_none_empty_and_ties(self):
        self.worker.build(1, [[1, 0], [-1, 0], [0, 0]])
        self.assertEqual(self.worker.search(1, [[0, 0]], 4)[0], [(2, 0), (0, 1), (1, 1)])
        self.assertEqual(self.worker.search(1, [[0, 0]], 4, allowed=[]), [[]])
        self.assertEqual(self.worker.search(1, [[0, 0]] * 2, 4, allowed=[1]), [[(1, 1)]] * 2)
        self.assertStatus(1, self.worker.search, 1, [[0, 0]], 2, metric=2)
        self.assertEqual(len(self.worker.search(1, [[1, 0]], 9, metric=2)[0]), 2)

    def test_generation_and_failed_build_atomicity(self):
        self.assertStatus(3, self.worker.search, 1, [[1]], 1)
        self.worker.build(1, [[2]])
        self.assertStatus(1, self.worker.build, 2, [[float('nan')]])
        self.assertEqual(self.worker.search(1, [[1]], 1), [[(0, 1)]])
        self.assertStatus(3, self.worker.search, 2, [[1]], 1)
        self.worker.build(2, [[3]])
        self.assertStatus(3, self.worker.search, 1, [[1]], 1)
        self.worker.unload(2)
        self.assertStatus(3, self.worker.search, 2, [[1]], 1)
        self.assertStatus(3, self.worker.build, 2, [[4]])

    def test_snapshot_reload_and_corruption(self):
        self.worker.build(1, [[1, 2], [3, 4]])
        before = self.worker.search(1, [[1, 1]], 2)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'snapshot'
            self.worker.save(1, path)
            self.worker.unload(1)
            self.worker.load(2, path)
            self.assertEqual(self.worker.search(2, [[1, 1]], 2), before)
            data = bytearray(path.read_bytes())
            data[-1] ^= 1
            path.write_bytes(data)
            with self.assertRaises(ValueError):
                self.worker.load(3, path)
            self.assertEqual(self.worker.search(2, [[1, 1]], 2), before)

    def test_transport_probe_is_not_search(self):
        self.worker.build(1, [[1]])
        self.assertEqual(self.worker.request(5, 1, b'diagnostic bytes'), b'')

    def test_empty_index(self):
        self.worker.build(1, [], dimensions=2)
        self.assertEqual(self.worker.search(1, [[1, 2]], 10), [[]])

    def test_invalid_inputs_and_stream_recovery(self):
        self.worker.build(1, [[1], [2]])
        for ids in [[0, 0], [1, 0], [2]]:
            self.assertStatus(1, self.worker.search, 1, [[1]], 1, allowed=ids)
        self.assertStatus(1, self.worker.search, 1, [[math.inf]], 1)
        self.assertStatus(1, self.worker.search, 1, [[1, 2]], 1)
        self.assertStatus(2, self.worker.search, 1, [[1]], 1, metric=99)
        self.assertStatus(1, self.worker.request, 2, 1, b'')
        self.assertStatus(1, self.worker.request, 1, 2, struct.pack('<II', 2**32-1, 16000))
        self.assertEqual(self.worker.search(1, [[1]], 1), [[(0, 0)]])

    def test_bad_frames_exit(self):
        frames = [b'bad', HEADER.pack(b'PVAN', 2, 0, 1, 0, 0, 0),
                  HEADER.pack(b'PVAN', 1, 0, 1, 0, 2**32-1, 0),
                  HEADER.pack(b'PVAN', 1, 1, 1, 1, 8, 0) + b'bad']
        for frame in frames:
            result = subprocess.run([str(ROOT / 'ann/ann-worker')], input=frame,
                                    capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)

    def test_vendor_selection_never_silently_falls_back(self):
        for vendor in ['cuvs', 'hipvs', 'fused-nvfp4', 'unknown']:
            result = subprocess.run([str(ROOT / 'ann/ann-worker'), '--backend', vendor],
                                    input=b'', capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 2)
            self.assertIn(b'require vendor adapters', result.stderr)


if __name__ == '__main__':
    unittest.main()
