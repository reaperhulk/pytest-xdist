# pytest-xdist performance measurement tools

Standalone tools used to measure and validate xdist performance work.
Nothing in this directory ships in the package or runs in CI; the pytest
plugins work against any pytest-xdist version (including releases), which
makes them suitable for A/B benchmarking.

## Scheduler benchmarks (no workers involved)

These drive the real scheduler classes in-process with fake worker nodes,
isolating controller-side CPU and memory from test execution:

- `bench_sched.py` — drives `WorkStealingScheduling` through a full
  simulated session (initial distribution, FIFO completions with uneven
  worker speeds, the complete steal/unschedule protocol). Measures
  controller cost per completed test.

      python bench/bench_sched.py <label>

- `bench_collect.py` — measures time and retained memory while the
  controller ingests per-worker collections (distinct string objects per
  worker, as deserialization produces), for `LoadScheduling` and
  `WorkStealingScheduling`.

      python bench/bench_collect.py <label>

## pytest plugins (end-to-end, real runs)

Load with `-p` and a `PYTHONPATH` entry; both only activate on the
controller and print machine-greppable lines:

- `xdist_full_bench.py` — startup and total wall time:

      PYTHONPATH=bench pytest -n auto --dist=worksteal -q -p xdist_full_bench ...
      [bench] ready=0.46s first_test=1.23s startup=0.77s total=18.20s

  `ready` = last worker ready, `first_test` = first test dispatched,
  `startup` = the distributed collection/exchange phase between them.

- `xdist_queue_probe.py` — samples the controller event-queue depth at
  20ms intervals and dumps work-stealing statistics at session end (the
  steal statistics require a scheduler that exposes them, i.e. this
  branch; the queue depth works everywhere):

      PYTHONPATH=bench pytest -n auto --dist=worksteal -q -p xdist_queue_probe ...
      [queue-probe] samples=705 max=297 p50=0 p90=5 p99=59
      [steal-stats] requests=17 failed=0 deferred=36 stolen=1023 avg_rtt=2.1ms

A note on interpreting steal round-trip times: a worker answers steal
requests from its execnet receiver thread, so round-trips are normally a
few milliseconds. Sustained sub-second round-trips indicate the victim
worker's receiver thread cannot get the GIL — i.e. the test is inside a
long native call that does not release it. This tooling found exactly
that in pyca/cryptography's KDF paths.
