"""Measure controller-side memory retained and time spent ingesting worker
collections in LoadScheduling / WorkStealingScheduling.

Each worker's collection arrives from execnet as freshly deserialized string
objects; we simulate that by building N distinct strings per worker.

Usage: python bench_collect.py [label]
"""

import gc
import sys
import time
import tracemalloc

from xdist.remote import Producer
from xdist.scheduler.load import LoadScheduling
from xdist.scheduler.worksteal import WorkStealingScheduling


class FakeHook:
    def pytest_collectreport(self, report):
        pass


class FakeConfig:
    def __init__(self, num_workers):
        self._values = {"tx": [f"{num_workers}*popen"], "maxschedchunk": None}
        self.hook = FakeHook()

    def getvalue(self, name):
        return self._values[name]

    getoption = getvalue


class FakeGateway:
    def __init__(self, i):
        self.id = f"gw{i}"


class FakeNode:
    def __init__(self, i):
        self.gateway = FakeGateway(i)
        self.shutting_down = False

    def send_runtest_some(self, indices):
        pass

    def send_steal(self, indices):
        pass

    def shutdown(self):
        pass


def fresh_collection(num_tests):
    # str() on a slice forces distinct string objects, like deserialization does
    return [
        str(f"tests/test_module_{i // 50}.py::TestClass::test_case_{i}")
        for i in range(num_tests)
    ]


def run(sched_cls, num_tests, num_workers):
    config = FakeConfig(num_workers)
    sched = sched_cls(config, Producer("bench", enabled=False))
    nodes = [FakeNode(i) for i in range(num_workers)]
    for node in nodes:
        sched.add_node(node)

    gc.collect()
    tracemalloc.start()
    base, _ = tracemalloc.get_traced_memory()
    elapsed = 0.0
    # Collections arrive one at a time; DSession does not retain the message,
    # so anything still alive afterwards is held by the scheduler.
    for node in nodes:
        ids = fresh_collection(num_tests)
        t0 = time.perf_counter()
        sched.add_node_collection(node, ids)
        elapsed += time.perf_counter() - t0
        del ids
    t0 = time.perf_counter()
    sched.schedule()
    elapsed += time.perf_counter() - t0
    gc.collect()
    current, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert sched.collection_is_completed and sched.collection is not None
    retained_mb = (current - base) / 1e6
    return elapsed, retained_mb, sched  # keep sched alive through measurement


if __name__ == "__main__":
    label = sys.argv[1] if len(sys.argv) > 1 else "current"
    print(f"== {label} ==")
    for cls in (LoadScheduling, WorkStealingScheduling):
        for n, w in [(20_000, 8), (100_000, 8), (100_000, 32), (100_000, 64)]:
            elapsed, retained, _s = run(cls, n, w)
            print(
                f"{cls.__name__:<24} n={n:>7} w={w:>2}: "
                f"ingest+schedule {elapsed * 1000:8.1f} ms, retained {retained:8.1f} MB"
            )
