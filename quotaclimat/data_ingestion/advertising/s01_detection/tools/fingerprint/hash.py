import hashlib
import json
from typing import List, Tuple

import numpy as np

from ..common_objects import Fingerprint


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
    ):
        self.fan_out = fan_out
        self.time_delta_max = time_delta_max
        self.time_delta_min = time_delta_min

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

        return pairs


# Keep old name as alias for backward compatibility in imports
HashGenerator = PairGenerator


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
    """
    hashes_a = fp_a.hashes or []
    hashes_b = fp_b.hashes or []
    if len(hashes_a) < min_matching or len(hashes_b) < min_matching:
        return 0.0

    pairs_a = np.array(hashes_a, dtype=np.int32)  # (Na, 4): f1, f2, dt, t_offset
    pairs_b = np.array(hashes_b, dtype=np.int32)  # (Nb, 4)

    # Per-dimension closeness check (Na, Nb) boolean
    close = (
        (np.abs(pairs_a[:, None, 0] - pairs_b[None, :, 0]) <= freq_tol)
        & (np.abs(pairs_a[:, None, 1] - pairs_b[None, :, 1]) <= freq_tol)
        & (np.abs(pairs_a[:, None, 2] - pairs_b[None, :, 2]) <= dt_tol)
    )

    # For each pair in A, find closest match in B (L1 on first 3 dims)
    matched_a = []
    matched_b = []
    for i in range(len(pairs_a)):
        candidates = np.where(close[i])[0]
        if len(candidates) > 0:
            dists = np.abs(pairs_a[i, :3] - pairs_b[candidates, :3]).sum(axis=1)
            best = candidates[dists.argmin()]
            matched_a.append(i)
            matched_b.append(best)

    if len(matched_a) < min_matching:
        return 0.0

    # Temporal coherence with tolerance
    offsets = pairs_a[matched_a, 3] - pairs_b[matched_b, 3]
    sorted_offsets = np.sort(offsets)

    best_count = 0
    for i in range(len(sorted_offsets)):
        count = int(np.sum(np.abs(sorted_offsets - sorted_offsets[i]) <= offset_tol))
        if count > best_count:
            best_count = count

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
    min_matching_hashes: int,
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
        _score(fp_a, fp_b, min_matching_hashes, freq_tol, dt_tol, offset_tol)
        >= similarity_threshold
    )
