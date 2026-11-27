"""
Memory used by the detection, written in the logs of every step.

- job: memory of the container (cgroup), the one Scaleway compares to the job's limit. It
  counts the main process, the worker processes and the page cache of the local files.
- main process: resident memory of the process that runs the channel (VmRSS). Python does
  not give back to the system the memory of the objects it freed: this value does not go
  down at the end of a step.

Reads /sys/fs/cgroup and /proc only (Linux): elsewhere, the values are missing.
"""

import threading
from pathlib import Path

GB = 1024**3

# cgroup v2, then v1
_CGROUP_CURRENT = (
    Path("/sys/fs/cgroup/memory.current"),
    Path("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
)


def _read_int(path: Path) -> int | None:
    try:
        return int(path.read_text().split()[0])
    except (OSError, ValueError, IndexError):
        return None


def job_memory() -> int | None:
    """Memory of the container (cgroup), in bytes."""
    for path in _CGROUP_CURRENT:
        value = _read_int(path)
        if value is not None:
            return value
    return None


def main_process_memory() -> int | None:
    """Resident memory of this process (VmRSS), in bytes."""
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def _format(value: int | None) -> str:
    return "?" if value is None else f"{value / GB:.2f} GB"


def memory_summary() -> str:
    return f"job {_format(job_memory())}, main process {_format(main_process_memory())}"


class MemoryPeak:
    """
    Highest memory of the job and of the main process while the block runs, sampled
    every `interval_sec` seconds by a thread (a peak shorter than that can be missed).
    """

    def __init__(self, interval_sec: float = 5.0):
        self.interval_sec = interval_sec
        self.job: int | None = None
        self.main_process: int | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _sample(self):
        for attr, value in (
            ("job", job_memory()),
            ("main_process", main_process_memory()),
        ):
            if value is not None:
                current = getattr(self, attr)
                setattr(self, attr, value if current is None else max(current, value))

    def _run(self):
        while not self._stop.wait(self.interval_sec):
            self._sample()

    def __enter__(self):
        self._sample()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()
        self._sample()

    def summary(self) -> str:
        return f"job {_format(self.job)}, main process {_format(self.main_process)}"
