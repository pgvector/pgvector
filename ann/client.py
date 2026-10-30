"""Binary-protocol reference client; no PostgreSQL or GPU dependencies."""

import json
import hashlib
import os
import struct
import subprocess
import tempfile
import math
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from pathlib import Path

HEADER = struct.Struct('<4sHHQQII')
MAX_BYTES = 64 * 1024 * 1024


class WorkerError(RuntimeError):
    def __init__(self, status):
        self.status = status
        super().__init__(f'worker returned status {status}')


class Worker:
    def __init__(self, executable=None, backend='cpu-exact', timeout=30):
        if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError('timeout must be a positive finite number of seconds')
        executable = executable or Path(__file__).with_name('ann-worker')
        self.process = subprocess.Popen(
            [str(executable), '--backend', backend],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        self.request_id = 0
        self.timeout = timeout
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='ann-transport')
        self._request_lock = threading.Lock()
        self._closed = False

    def _read(self, size):
        result = bytearray()
        while len(result) < size:
            chunk = self.process.stdout.read(size - len(result))
            if not chunk:
                raise EOFError('worker closed the protocol stream')
            result.extend(chunk)
        return bytes(result)

    def request(self, operation, generation=0, payload=b''):
        if len(payload) > MAX_BYTES:
            raise ValueError('frame exceeds protocol limit')
        if not self._request_lock.acquire(blocking=False):
            raise RuntimeError('concurrent requests on one worker are not supported')
        try:
            if self._closed:
                raise RuntimeError('worker connection is closed')
            future = self._executor.submit(self._exchange, operation, generation, payload)
            try:
                return future.result(timeout=self.timeout)
            except FutureTimeout:
                self.cancel()
                raise TimeoutError('worker request timed out; generation invalidated') from None
            except WorkerError:
                raise  # A framed operation error leaves the stream synchronized.
            except BaseException:
                self.cancel()
                raise
        finally:
            self._request_lock.release()

    def _exchange(self, operation, generation, payload):
        self.request_id += 1
        self.process.stdin.write(HEADER.pack(
            b'PVAN', 1, operation, self.request_id, generation, len(payload), 0))
        self.process.stdin.write(payload)
        self.process.stdin.flush()
        magic, version, op, request, gen, size, status = HEADER.unpack(self._read(32))
        if (magic, version, op, request, gen) != (
                b'PVAN', 1, operation, self.request_id, generation) or size > MAX_BYTES:
            raise ValueError('invalid worker response header')
        result = self._read(size)
        if status:
            raise WorkerError(status)
        return result

    def capabilities(self):
        return json.loads(self.request(0))

    def build(self, generation, vectors, dimensions=None):
        dimensions = dimensions if dimensions is not None else len(vectors[0])
        if dimensions <= 0 or any(len(row) != dimensions for row in vectors):
            raise ValueError('inconsistent dimensions')
        values = [value for row in vectors for value in row]
        payload = struct.pack('<II', len(vectors), dimensions)
        payload += struct.pack(f'<{len(values)}f', *values)
        self.request(1, generation, payload)

    def search(self, generation, queries, k, metric=0, allowed=None, per_query=False):
        if not queries or not queries[0] or any(len(q) != len(queries[0]) for q in queries):
            raise ValueError('inconsistent query dimensions')
        if per_query and (allowed is None or len(allowed) != len(queries)):
            raise ValueError('one allowlist is required per query')
        kind = 2 if per_query else 1 if allowed is not None else 0
        values = [value for row in queries for value in row]
        payload = struct.pack('<IIIII', len(queries), len(queries[0]), k, metric, kind)
        payload += struct.pack(f'<{len(values)}f', *values)
        if kind:
            for ids in allowed if per_query else [allowed]:
                payload += struct.pack('<I', len(ids))
                payload += struct.pack(f'<{len(ids)}I', *ids)
        data = self.request(2, generation, payload)
        offset = 0
        result = []
        for _ in queries:
            count, = struct.unpack_from('<I', data, offset)
            offset += 4
            row = []
            for _ in range(count):
                row.append(struct.unpack_from('<Id', data, offset))
                offset += 12
            result.append(row)
        if offset != len(data):
            raise ValueError('unexpected trailing response data')
        return result

    def unload(self, generation):
        self.request(3, generation)

    def save(self, generation, path):
        """Persist canonical FP32 vectors, not a vendor graph or PostgreSQL index."""
        payload = self.request(4, generation)
        path = Path(path)
        header = struct.pack('<4sIQ32s', b'PVAS', 1, len(payload), hashlib.sha256(payload).digest())
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
                temporary = stream.name
                stream.write(header + payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)

    def load(self, generation, path):
        with open(path, 'rb') as stream:
            header = stream.read(48)
            if len(header) != 48:
                raise ValueError('truncated snapshot header')
            magic, version, size, digest = struct.unpack('<4sIQ32s', header)
            if magic != b'PVAS' or version != 1 or size > MAX_BYTES:
                raise ValueError('unsupported snapshot format or size')
            payload = stream.read(size + 1)
            if len(payload) != size or hashlib.sha256(payload).digest() != digest:
                raise ValueError('snapshot size or checksum mismatch')
        self.request(1, generation, payload)

    def close(self):
        idle = self._request_lock.acquire(blocking=False)
        self._closed = True
        try:
            if not idle:
                self.cancel()
            self._executor.shutdown(wait=True)
            try:
                self.process.stdin.close()
            except (OSError, ValueError):
                pass
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.cancel()
            self.process.stdout.close()
        finally:
            if idle:
                self._request_lock.release()

    def cancel(self):
        """Invalidate the entire process/session; this is not GPU kernel preemption."""
        self._closed = True
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=5)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
