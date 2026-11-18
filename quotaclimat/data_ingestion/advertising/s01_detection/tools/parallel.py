"""
Runs the CPU bound loops of the detection (comparison of fingerprints) in worker processes.

The data every task reads (fingerprints of the week, index) is given once to each worker as
its "state", not with every task: with the fork start method (Linux), the workers inherit it
without any copy. The fingerprints are packed in numpy arrays (PackedFingerprints): forked
workers do not touch their memory pages, and a pickled copy (spawn) stays small, while the
Python objects of the tens of thousands of chunks of a week weigh about a gigabyte.
"""

import gc
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import numpy as np

from .common_objects import Fingerprint

_state: Any = None


@contextmanager
def frozen_gc() -> Iterator[None]:
    """
    Wrap the creation and the life of a pool of forked workers.

    A forked worker shares the memory pages of the main process until it writes in them.
    The garbage collector of a worker writes in the header of every Python object it
    inherited (the tens of thousands of chunks of the week, about a gigabyte) as soon as
    it does a full collection: each worker then gets its own copy of these pages.
    gc.freeze() puts the objects existing at the fork out of reach of the collector, of
    the workers as of the main process, until the pool is closed.
    """
    gc.collect()
    gc.freeze()
    try:
        yield
    finally:
        gc.unfreeze()


def _set_state(state: Any) -> None:
    global _state
    _state = state


def worker_state() -> Any:
    """State given to ordered_map, read by the tasks."""
    return _state


def ordered_map(
    func: Callable[[Any], Any],
    tasks: Iterable[tuple[Any, Any]],
    state: Any,
    num_workers: int,
    max_in_flight: int | None = None,
) -> Iterator[tuple[Any, Any]]:
    """
    Yields (context, func(payload)) for each (context, payload) of tasks, in their order.

    func runs in num_workers processes, where worker_state() returns state; the context stays
    in this process (data needed with the result, not by the task). At most max_in_flight
    tasks (default: 4 per worker) are submitted ahead, so that tasks read from a stream (rows
    of the database) are not all held in memory. With num_workers <= 1, everything runs in
    this process.
    """
    if num_workers <= 1:
        previous = _state
        _set_state(state)
        try:
            for context, payload in tasks:
                yield context, func(payload)
        finally:
            _set_state(previous)
        return

    max_in_flight = max_in_flight or 4 * num_workers
    with frozen_gc(), ProcessPoolExecutor(
        max_workers=num_workers, initializer=_set_state, initargs=(state,)
    ) as executor:
        pending = deque()
        for context, payload in tasks:
            pending.append((context, executor.submit(func, payload)))
            if len(pending) >= max_in_flight:
                context, future = pending.popleft()
                yield context, future.result()
        while pending:
            context, future = pending.popleft()
            yield context, future.result()


@dataclass
class PackedFingerprints:
    """
    Fingerprints in 3 numpy arrays, for the worker processes.

    Only what the comparison reads is kept (FingerprintsCompare.is_similar and the index):
    the acoustic features and the first 4 values of each pair (f1, f2, dt, t_offset), not the
    peaks. get(i) gives back a Fingerprint whose pairs are a numpy array.
    """

    features: (
        np.ndarray
    )  # (n, 4) float64: duration_sec, energy_mean, spectral_centroid, zcr_mean
    pairs: np.ndarray  # (total number of pairs, 4) int32
    offsets: (
        np.ndarray
    )  # (n + 1,) int64: pairs of fingerprint i = pairs[offsets[i]:offsets[i + 1]]

    @classmethod
    def pack(cls, fingerprints: list[Fingerprint]) -> "PackedFingerprints":
        features = np.array(
            [
                (fp.duration_sec, fp.energy_mean, fp.spectral_centroid, fp.zcr_mean)
                for fp in fingerprints
            ],
            dtype=np.float64,
        ).reshape(len(fingerprints), 4)
        pairs_per_fp = [
            np.asarray(fp.pairs, dtype=np.int32)[:, :4]
            for fp in fingerprints
            if fp.pairs is not None and len(fp.pairs)
        ]
        counts = [len(fp.pairs) if fp.pairs is not None else 0 for fp in fingerprints]
        offsets = np.zeros(len(fingerprints) + 1, dtype=np.int64)
        np.cumsum(counts, out=offsets[1:])
        pairs = (
            np.concatenate(pairs_per_fp)
            if pairs_per_fp
            else np.empty((0, 4), dtype=np.int32)
        )
        return cls(features=features, pairs=pairs, offsets=offsets)

    def __len__(self) -> int:
        return len(self.features)

    def get(self, i: int) -> Fingerprint:
        duration_sec, energy_mean, spectral_centroid, zcr_mean = self.features[i]
        return Fingerprint(
            duration_sec=float(duration_sec),
            energy_mean=float(energy_mean),
            spectral_centroid=float(spectral_centroid),
            zcr_mean=float(zcr_mean),
            pairs=self.pairs[self.offsets[i] : self.offsets[i + 1]],
        )
