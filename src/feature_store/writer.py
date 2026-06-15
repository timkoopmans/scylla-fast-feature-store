"""Pipelined, shard-aware write path.

Uses the driver's `execute_async` with a bounded number of in-flight requests
(a Semaphore gate). With the token/shard-aware load-balancing policy each
prepared statement is routed straight to the replica shard that owns the key,
so there is no coordinator hop. This is what sustains high write throughput at
LOCAL_ONE without batching unrelated partitions.
"""
from __future__ import annotations

import threading
import time
from collections import deque

from cassandra.query import PreparedStatement


class Pipeline:
    def __init__(self, session, max_inflight: int = 4096, sample_every: int = 256):
        self.session = session
        self.gate = threading.Semaphore(max_inflight)
        self.errors = 0
        self.count = 0
        self._lock = threading.Lock()
        self._sample_every = sample_every
        self._latencies = deque(maxlen=200_000)  # sampled write latencies (ms)
        self._n = 0

    def execute(self, prepared: PreparedStatement, params, exec_profile="write"):
        self.gate.acquire()
        self._n += 1
        sampled = (self._n % self._sample_every) == 0
        t0 = time.perf_counter() if sampled else None
        fut = self.session.execute_async(prepared, params, execution_profile=exec_profile)

        def _done(_result, t0=t0):
            self.gate.release()
            with self._lock:
                self.count += 1
                if t0 is not None:
                    self._latencies.append((time.perf_counter() - t0) * 1000.0)

        def _err(exc):
            self.gate.release()
            with self._lock:
                self.errors += 1

        fut.add_callbacks(_done, _err)

    def drain(self, max_inflight: int = 4096):
        """Block until all in-flight requests have completed."""
        for _ in range(max_inflight):
            self.gate.acquire()
        for _ in range(max_inflight):
            self.gate.release()

    def write_latency_ms(self):
        with self._lock:
            return sorted(self._latencies)
