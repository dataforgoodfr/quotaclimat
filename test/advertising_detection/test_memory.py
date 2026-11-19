"""Memory of the detection: measures for the logs, gc frozen around the pools of workers,
one process per channel."""

import gc
import sys
import time

from quotaclimat.data_ingestion.advertising.s01_detection.run import (
    _run_in_subprocess,
)
from quotaclimat.data_ingestion.advertising.s01_detection.tools.memory import (
    MemoryPeak,
    main_process_memory,
    memory_summary,
)
from quotaclimat.data_ingestion.advertising.s01_detection.tools.parallel import (
    frozen_gc,
    ordered_map,
)


def _square(x):
    return x * x


def test_memory_summary_gives_the_job_and_the_main_process():
    summary = memory_summary()
    assert "job " in summary and "main process " in summary
    # Linux: /proc/self/status is always there (the cgroup may not be)
    assert main_process_memory() > 0


def test_memory_peak_keeps_the_highest_value():
    with MemoryPeak(interval_sec=0.01) as peak:
        data = bytearray(200 * 1024**2)
        time.sleep(0.05)
        del data
    assert peak.main_process >= 200 * 1024**2
    assert "GB" in peak.summary()


def test_frozen_gc_unfreezes_even_on_error():
    try:
        with frozen_gc():
            assert gc.get_freeze_count() > 0
            raise ValueError
    except ValueError:
        pass
    assert gc.get_freeze_count() == 0


def test_ordered_map_with_workers_unfreezes_the_gc():
    tasks = [(i, i) for i in range(20)]
    assert list(ordered_map(_square, tasks, state=None, num_workers=2)) == [
        (i, i * i) for i in range(20)
    ]
    assert gc.get_freeze_count() == 0


def test_run_in_subprocess_gives_the_outcome_of_the_process():
    assert _run_in_subprocess(time.sleep, 0)
    assert not _run_in_subprocess(sys.exit, 3)
