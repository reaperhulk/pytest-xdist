import os
import time

if not os.environ.get("PYTEST_XDIST_WORKER"):
    _t0 = time.perf_counter()
    _marks = {}

    def pytest_testnodeready(node):
        _marks["last_ready"] = time.perf_counter() - _t0

    def pytest_runtest_logstart(nodeid, location):
        _marks.setdefault("first_test", time.perf_counter() - _t0)

    def pytest_sessionfinish(session):
        total = time.perf_counter() - _t0
        ready = _marks.get("last_ready", 0.0)
        first = _marks.get("first_test", 0.0)
        print(
            f"\n[bench] ready={ready:.2f}s first_test={first:.2f}s "
            f"startup={first - ready:.2f}s total={total:.2f}s",
            flush=True,
        )
