"""Benchmark the real WorkStealingScheduling class end-to-end (controller side).

Drives the actual scheduler with fake worker nodes, simulating the DSession
event loop: initial distribution, FIFO test completions, and the full
steal/unschedule round trip. Measures controller CPU cost per completed test.

Usage: python bench_sched.py [label]
"""

from collections import deque
import sys
import time

from xdist.remote import Producer
from xdist.scheduler.worksteal import WorkStealingScheduling


class FakeHook:
    def pytest_collectreport(self, report):
        pass


class FakeConfig:
    def __init__(self, num_workers):
        self._tx = [f"{num_workers}*popen"]
        self.hook = FakeHook()

    def getvalue(self, name):
        assert name == "tx"
        return self._tx


class FakeGateway:
    def __init__(self, i):
        self.id = f"gw{i}"


class FakeNode:
    """Mirrors the worker's local queue so completions arrive in FIFO order."""

    def __init__(self, i, steal_responses):
        self.gateway = FakeGateway(i)
        self.local = deque()
        self._steal_responses = steal_responses
        self._shutdown = False

    @property
    def shutting_down(self):
        return self._shutdown

    def send_runtest_some(self, indices):
        self.local.extend(indices)

    def send_steal(self, indices):
        # emulate the worker: all requested tests are still queued, give them up
        requested = set(indices)
        stolen = [i for i in self.local if i in requested]
        self.local = deque(i for i in self.local if i not in requested)
        self._steal_responses.append((self, stolen))

    def shutdown(self):
        self._shutdown = True


def run(num_tests, num_workers, uneven=True):
    config = FakeConfig(num_workers)
    sched = WorkStealingScheduling(config, Producer("bench", enabled=False))
    steal_responses = deque()
    nodes = [FakeNode(i, steal_responses) for i in range(num_workers)]
    collection = [f"tests/test_mod_{i // 50}.py::test_case_{i}" for i in range(num_tests)]
    for node in nodes:
        sched.add_node(node)
    for node in nodes:
        sched.add_node_collection(node, collection)
    assert sched.collection_is_completed

    t0 = time.perf_counter()
    sched.schedule()

    # Simulate the run. With uneven=True the first half of the workers run
    # twice as fast, forcing steal traffic like real-world imbalance does.
    completions = 0
    steals = 0
    while True:
        progressed = False
        for i, node in enumerate(nodes):
            speed = 2 if (uneven and i < num_workers // 2) else 1
            for _ in range(speed):
                if node.local:
                    sched.mark_test_complete(node, node.local.popleft())
                    completions += 1
                    progressed = True
            while steal_responses:
                victim, stolen = steal_responses.popleft()
                steals += 1
                sched.remove_pending_tests_from_node(victim, stolen)
                progressed = True
        if not progressed:
            break
    elapsed = time.perf_counter() - t0

    assert completions == num_tests, (completions, num_tests)
    assert sched.tests_finished
    return elapsed, steals


if __name__ == "__main__":
    label = sys.argv[1] if len(sys.argv) > 1 else "current"
    print(f"== {label} ==")
    for n, w in [(20_000, 8), (100_000, 8), (100_000, 32), (100_000, 64), (500_000, 16)]:
        # warm-up run at small size once
        best = min(run(n, w)[0] for _ in range(3))
        _, steals = run(n, w)
        per_test = best / n * 1e6
        print(
            f"n={n:>7} w={w:>2}: {best * 1000:9.1f} ms total "
            f"({per_test:6.2f} us/test, {steals} steal round-trips)"
        )
