import hashlib
import json
from collections import Counter, defaultdict
from typing import List, Tuple

import numpy as np

from ..common_objects import Fingerprint


# 27 neighbor offsets for 3D adjacency (±1 in each of f1, f2, dt)
_ADJ_3D = [
    (df1, df2, ddt)
    for df1 in (-1, 0, 1)
    for df2 in (-1, 0, 1)
    for ddt in (-1, 0, 1)
]


def build_pairs_index(
    fingerprints: list[Fingerprint],
    freq_tol: int = 2,
    dt_tol: int = 1,
) -> dict[tuple[int, int, int], set[int]]:
    """
    Build an inverted index mapping quantized 3D pair keys to fingerprint indices.

    Key = (f1 // freq_tol, f2 // freq_tol, dt // dt_tol).
    Spreads pairs across a large key space so each cell stays small,
    enabling fast candidate lookups in query_pairs_index().
    """
    index: dict[tuple[int, int, int], set[int]] = defaultdict(set)
    for i, fp in enumerate(fingerprints):
        pairs = fp.pairs or []
        if not pairs:
            continue
        arr = np.array(pairs, dtype=np.int32)
        for key in set(zip(
            (arr[:, 0] // freq_tol).tolist(),
            (arr[:, 1] // freq_tol).tolist(),
            (arr[:, 2] // dt_tol).tolist(),
        )):
            index[key].add(i)
    return dict(index)


def query_pairs_index(
    query_fp: Fingerprint,
    index: dict[tuple[int, int, int], set[int]],
    freq_tol: int = 2,
    dt_tol: int = 1,
    min_matching_pairs: int = 5,
) -> set[int]:
    """
    Return indices of fingerprints that are candidate matches for query_fp.

    For each pair in query_fp, looks up 27 neighboring 3D cells and counts
    co-occurrences. Returns indices with count >= min_matching_pairs.
    Keeping per-pair duplicates in the query ensures that k matching pairs
    always yield a count >= k (no false negatives).
    """
    pairs = query_fp.pairs or []
    if not pairs:
        return set()
    arr = np.array(pairs, dtype=np.int32)
    keys = list(zip(
        (arr[:, 0] // freq_tol).tolist(),
        (arr[:, 1] // freq_tol).tolist(),
        (arr[:, 2] // dt_tol).tolist(),
    ))
    neighbor_counts: Counter[int] = Counter()
    for kf1, kf2, kdt in keys:
        for df1, df2, ddt in _ADJ_3D:
            for j in index.get((kf1 + df1, kf2 + df2, kdt + ddt), ()):
                neighbor_counts[j] += 1
    return {j for j, count in neighbor_counts.items() if count >= min_matching_pairs}

class PairGenerator:
    """
    Generate peak-pair fingerprint tuples from a constellation map.

    Each pair = (freq1, freq2, delta_t, time_offset) — shift-invariant,
    noise-tolerant when matched with distance-based scoring.
    """

    def __init__(
        self,
        fan_out: int = 4,
        time_delta_max: int = 100,
        time_delta_min: int = 1,
        max_pairs: int = 80,
    ):
        self.fan_out = fan_out
        self.time_delta_max = time_delta_max
        self.time_delta_min = time_delta_min
        self.max_pairs = max_pairs

    def generate(self, peaks: np.ndarray) -> List[Tuple[int, int, int, int]]:
        if len(peaks) < 2:
            return []

        peaks = peaks[peaks[:, 0].argsort()]

        pairs = []
        for i, (t1, f1) in enumerate(peaks):
            j = i + 1
            count = 0
            while j < len(peaks) and count < self.fan_out:
                t2, f2 = peaks[j]
                delta_t = t2 - t1

                if delta_t > self.time_delta_max:
                    break
                if delta_t >= self.time_delta_min:
                    pairs.append((int(f1), int(f2), int(delta_t), int(t1)))
                    count += 1
                j += 1

        return pairs[:self.max_pairs]



def make_params_hash(params: dict) -> str:
    """Stable 16-char hash of a parameter dict, used as a cache/DB key."""
    serialized = json.dumps(params, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _score(
    fp_a: Fingerprint,
    fp_b: Fingerprint,
    min_matching: int,
    freq_tol: int = 2,
    dt_tol: int = 1,
    offset_tol: int = 2,
) -> float:
    """
    Distance-based similarity score with temporal coherence.

    For each pair in A, find the closest pair in B (within per-dimension
    tolerance). Then check temporal coherence: matched pairs should share
    a consistent time offset between the two chunks.

    Uses a sum-based index (f1+f2+dt) to pre-filter candidates: two pairs
    can only match if their sums differ by at most 2*freq_tol + dt_tol.
    This avoids the O(Na*Nb) full cross-product.
    """
    pairs_a_raw = fp_a.pairs or []
    pairs_b_raw = fp_b.pairs or []
    if len(pairs_a_raw) < min_matching or len(pairs_b_raw) < min_matching:
        return 0.0

    pairs_a = np.array(pairs_a_raw, dtype=np.int32)  # (Na, 4): f1, f2, dt, t_offset
    pairs_b = np.array(pairs_b_raw, dtype=np.int32)  # (Nb, 4)

    # Sort B by sum of first 3 dims for binary-search pre-filtering
    sums_b = pairs_b[:, 0] + pairs_b[:, 1] + pairs_b[:, 2]
    order_b = np.argsort(sums_b)
    pairs_b_sorted = pairs_b[order_b]
    sums_b_sorted = sums_b[order_b]

    sum_tol = 2 * freq_tol + dt_tol

    matched_a = []
    matched_b = []
    for i in range(len(pairs_a)):
        s_a = int(pairs_a[i, 0]) + int(pairs_a[i, 1]) + int(pairs_a[i, 2])
        lo = np.searchsorted(sums_b_sorted, s_a - sum_tol, side="left")
        hi = np.searchsorted(sums_b_sorted, s_a + sum_tol, side="right")
        if lo >= hi:
            continue

        candidates_sorted = pairs_b_sorted[lo:hi]
        # Per-dimension check within the narrow candidate window
        close_mask = (
            (np.abs(pairs_a[i, 0] - candidates_sorted[:, 0]) <= freq_tol)
            & (np.abs(pairs_a[i, 1] - candidates_sorted[:, 1]) <= freq_tol)
            & (np.abs(pairs_a[i, 2] - candidates_sorted[:, 2]) <= dt_tol)
        )
        close_idxs = np.where(close_mask)[0]
        if len(close_idxs) > 0:
            dists = np.abs(pairs_a[i, :3] - candidates_sorted[close_idxs, :3]).sum(axis=1)
            best_local = close_idxs[dists.argmin()]
            matched_a.append(i)
            matched_b.append(int(order_b[lo + best_local]))

    if len(matched_a) < min_matching:
        return 0.0

    # Temporal coherence with tolerance (sliding window on sorted offsets)
    offsets = pairs_a[matched_a, 3] - pairs_b[matched_b, 3]
    sorted_offsets = np.sort(offsets)

    best_count = 0
    left = 0
    for right in range(len(sorted_offsets)):
        while sorted_offsets[right] - sorted_offsets[left] > 2 * offset_tol:
            left += 1
        best_count = max(best_count, right - left + 1)

    return best_count / (min(len(pairs_a), len(pairs_b)) + 1)


def _features_compatible(
    a: Fingerprint,
    b: Fingerprint,
    duration_tol: float = 0.3,
    rms_tol: float = 0.05,
    centroid_tol: float = 0.05,
    zcr_tol: float = 0.1,
) -> bool:
    """Acoustic pre-filter: reject pairs that differ too much in basic features."""
    if abs(a.duration_sec - b.duration_sec) > duration_tol:
        return False

    def rel_diff(x: float, y: float) -> float:
        return abs(x - y) / max(abs(x), abs(y), 1e-8)

    if a.energy_mean > 0 and b.energy_mean > 0:
        if rel_diff(a.energy_mean, b.energy_mean) > rms_tol:
            return False
    if a.spectral_centroid > 0 and b.spectral_centroid > 0:
        if rel_diff(a.spectral_centroid, b.spectral_centroid) > centroid_tol:
            return False
    if a.zcr_mean > 0 and b.zcr_mean > 0:
        if rel_diff(a.zcr_mean, b.zcr_mean) > zcr_tol:
            return False

    return True


def are_fingerprints_similar(
    fp_a: Fingerprint,
    fp_b: Fingerprint,
    min_matching_pairs: int,
    similarity_threshold: float,
    freq_tol: int = 2,
    dt_tol: int = 1,
    offset_tol: int = 2,
    duration_tol: float = 0.3,
    rms_tol: float = 0.05,
    centroid_tol: float = 0.05,
    zcr_tol: float = 0.1,
) -> bool:
    """Return True if two fingerprints pass the acoustic pre-filter and the similarity threshold."""
    if not _features_compatible(
        fp_a, fp_b, duration_tol, rms_tol, centroid_tol, zcr_tol
    ):
        return False
    return (
        _score(fp_a, fp_b, min_matching_pairs, freq_tol, dt_tol, offset_tol)
        >= similarity_threshold
    )
