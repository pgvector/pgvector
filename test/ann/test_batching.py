"""Python invokes native C scenarios; scheduler implementation is C only."""

from pathlib import Path
import subprocess
import unittest

EXECUTABLE = Path(__file__).resolve().parents[2] / 'ann/ann-batch-tests'


class NativeBatchTests(unittest.TestCase):
    def scenario(self, name):
        result = subprocess.run([str(EXECUTABLE), name], capture_output=True,
                                text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.strip(), 'ok')

    def test_batch_and_result_lifetime(self):
        self.scenario('batch')

    def test_admission_budgets(self):
        self.scenario('admission')

    def test_queue_and_running_deadlines(self):
        self.scenario('deadlines')

    def test_concurrent_producers(self):
        self.scenario('concurrent')

    def test_validation_and_shutdown(self):
        self.scenario('validation')

    def test_early_release_and_running_shutdown(self):
        self.scenario('lifetime')

    def test_backend_error_propagation(self):
        self.scenario('errors')


if __name__ == '__main__':
    unittest.main()
