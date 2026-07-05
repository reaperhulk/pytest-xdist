import os
import threading
import time

if not os.environ.get("PYTEST_XDIST_WORKER"):
    _samples = []
    _stop = threading.Event()

    def _sampler(dsession):
        while not _stop.is_set():
            _samples.append(dsession.queue.qsize())
            time.sleep(0.02)

    def pytest_configure(config):
        def start():
            ds = config.pluginmanager.getplugin("dsession")
            if ds is not None:
                threading.Thread(target=_sampler, args=(ds,), daemon=True).start()
        # dsession registers during configure; defer one tick
        threading.Timer(1.0, start).start()

    def pytest_sessionfinish(session):
        _stop.set()
        ds = session.config.pluginmanager.getplugin("dsession")
        sched = getattr(ds, "sched", None)
        if _samples:
            s = sorted(_samples)
            print(
                f"\n[queue-probe] samples={len(s)} max={s[-1]} "
                f"p50={s[len(s)//2]} p90={s[int(len(s)*0.9)]} p99={s[int(len(s)*0.99)]}"
            )
        if sched is not None and getattr(sched, "steal_requests", 0):
            print(
                f"[steal-stats] requests={sched.steal_requests} "
                f"failed={sched.steal_requests_failed} deferred={sched.steal_deferred} "
                f"stolen={sched.tests_stolen} "
                f"avg_rtt={sched.steal_in_flight_total / sched.steal_requests * 1000:.1f}ms"
            )
