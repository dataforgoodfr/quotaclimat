import hashlib
import json
from collections import Counter
from typing import List, Tuple

import numpy as np


class HashGenerator:
    """
    Génère des hashes robustes en combinant des PAIRES de pics proches
    de la constellation map.

    Un hash = (freq1, freq2, delta_temps) → invariant au décalage temporel,
    résistant au bruit.
    """

    def __init__(
        self,
        fan_out: int = 15,
        time_delta_max: int = 100,
        time_delta_min: int = 1,
    ):
        self.fan_out = fan_out
        self.time_delta_max = time_delta_max
        self.time_delta_min = time_delta_min

    def generate(self, peaks: np.ndarray) -> List[Tuple[str, int]]:
        if len(peaks) < 2:
            return []

        peaks = peaks[peaks[:, 0].argsort()]

        hashes = []
        for i, (t1, f1) in enumerate(peaks):
            j = i + 1
            count = 0
            while j < len(peaks) and count < self.fan_out:
                t2, f2 = peaks[j]
                delta_t = t2 - t1

                if delta_t > self.time_delta_max:
                    break
                if delta_t >= self.time_delta_min:
                    h = self._make_hash(f1, f2, delta_t)
                    hashes.append((h, int(t1)))
                    count += 1
                j += 1

        return hashes

    def _make_hash(self, f1: int, f2: int, dt: int) -> str:
        raw = f"{f1}|{f2}|{dt}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]


def make_params_hash(params: dict) -> str:
    """Stable 16-char hash of a parameter dict, used as a cache/DB key."""
    serialized = json.dumps(params, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _build_hash_sets(chunks: list) -> list[set[str]]:
    """Precompute the set of hash keys for each chunk (for O(1) lookup)."""
    return [{h for h, _ in (c.hashes or [])} for c in chunks]


def _score(
    chunk_a: "Chunk", chunk_b: "Chunk", set_a: set, set_b: set, min_matching: int
) -> float:
    """
    Similarity score based on temporal coherence of shared hashes.
    Common hashes must align on a consistent time offset.
    """
    common = set_a & set_b
    if len(common) < min_matching:
        return 0.0

    hashes_a = chunk_a.hashes or []
    hashes_b = chunk_b.hashes or []

    index_a = {h: t for h, t in hashes_a if h in common}
    index_b = {h: t for h, t in hashes_b if h in common}

    offsets = [index_a[h] - index_b[h] for h in common if h in index_a and h in index_b]
    if not offsets:
        return 0.0

    coherent_count = Counter(offsets).most_common(1)[0][1]
    min_hashes = min(len(hashes_a), len(hashes_b)) + 1
    return coherent_count / min_hashes


def _features_compatible(
    a: "Chunk",
    b: "Chunk",
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


def are_chunks_similar(
    chunk_a: "Chunk",
    chunk_b: "Chunk",
    set_a: set,
    set_b: set,
    min_matching_hashes: int,
    similarity_threshold: float,
    duration_tol: float = 0.3,
    rms_tol: float = 0.05,
    centroid_tol: float = 0.05,
    zcr_tol: float = 0.1,
) -> bool:
    """Return True if two chunks pass the acoustic pre-filter and the fingerprint similarity threshold."""
    if not _features_compatible(
        chunk_a, chunk_b, duration_tol, rms_tol, centroid_tol, zcr_tol
    ):
        return False
    return (
        _score(chunk_a, chunk_b, set_a, set_b, min_matching_hashes)
        >= similarity_threshold
    )
