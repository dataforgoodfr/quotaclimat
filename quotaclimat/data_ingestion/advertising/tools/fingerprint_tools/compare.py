from __future__ import annotations

from collections import Counter, defaultdict

import numpy as np

from quotaclimat.data_ingestion.advertising.tools.hashing import make_params_hash

from .fingerprint import Fingerprint

# 27 neighbor offsets for 3D adjacency (±1 in each of f1, f2, dt)
_ADJ_3D = [
    (df1, df2, ddt) for df1 in (-1, 0, 1) for df2 in (-1, 0, 1) for ddt in (-1, 0, 1)
]


class FingerprintsIndex:
    """
    Inverted index of quantized pair keys, for candidate lookup by fingerprint similarity.

    Tolerances are set once at construction and reused consistently across
    build() and get_similar_indices() calls.
    """

    def __init__(
        self,
        freq_tol: int = 2,
        dt_tol: int = 1,
        min_matching_pairs: int = 5,
    ):
        self.freq_tol = freq_tol
        self.dt_tol = dt_tol
        self.min_matching_pairs = min_matching_pairs
        self._index: dict[tuple[int, int, int], set[int]] = {}

    def build(self, fingerprints: list[Fingerprint]) -> "FingerprintsIndex":
        """
        Build the index over the given fingerprints, keyed by their position in the list.

        Key = (f1 // freq_tol, f2 // freq_tol, dt // dt_tol).
        Spreads pairs across a large key space so each cell stays small,
        enabling fast candidate lookups in get_similar_indices().
        """
        index: dict[tuple[int, int, int], set[int]] = defaultdict(set)
        for i, fp in enumerate(fingerprints):
            pairs = fp.pairs or []
            if not pairs:
                continue
            arr = np.array(pairs, dtype=np.int32)
            for key in set(
                zip(
                    (arr[:, 0] // self.freq_tol).tolist(),
                    (arr[:, 1] // self.freq_tol).tolist(),
                    (arr[:, 2] // self.dt_tol).tolist(),
                )
            ):
                index[key].add(i)
        self._index = dict(index)
        return self

    def get_similar_indices(
        self,
        query_fp: Fingerprint,
        min_matching_pairs: int | None = None,
    ) -> set[int]:
        """
        Return indices (as passed to build()) of candidate matches for query_fp.

        For each pair in query_fp, looks up 27 neighboring 3D cells and counts
        co-occurrences. Returns indices with count >= min_matching_pairs.
        Keeping per-pair duplicates in the query ensures that k matching pairs
        always yield a count >= k (no false negatives).
        """
        pairs = query_fp.pairs or []
        if not pairs:
            return set()
        threshold = (
            min_matching_pairs
            if min_matching_pairs is not None
            else self.min_matching_pairs
        )
        arr = np.array(pairs, dtype=np.int32)
        keys = list(
            zip(
                (arr[:, 0] // self.freq_tol).tolist(),
                (arr[:, 1] // self.freq_tol).tolist(),
                (arr[:, 2] // self.dt_tol).tolist(),
            )
        )
        neighbor_counts: Counter[int] = Counter()
        for kf1, kf2, kdt in keys:
            for df1, df2, ddt in _ADJ_3D:
                for j in self._index.get((kf1 + df1, kf2 + df2, kdt + ddt), ()):
                    neighbor_counts[j] += 1
        return {j for j, count in neighbor_counts.items() if count >= threshold}


class FingerprintsCompare:
    """
    Compare two fingerprints for similarity via acoustic pre-filter + distance-based scoring.

    Wraps the pre-filter and scoring logic so tolerances/thresholds are set once
    at construction and reused consistently across is_similar() calls.
    """

    def __init__(
        self,
        min_matching_pairs: int,
        similarity_threshold: float,
        freq_tol: int = 2,
        dt_tol: int = 1,
        offset_tol: int = 2,
        duration_tol: float = 0.3,
        rms_tol: float = 0.05,
        centroid_tol: float = 0.05,
        zcr_tol: float = 0.1,
    ):
        self.min_matching_pairs = min_matching_pairs
        self.similarity_threshold = similarity_threshold
        self.freq_tol = freq_tol
        self.dt_tol = dt_tol
        self.offset_tol = offset_tol
        self.duration_tol = duration_tol
        self.rms_tol = rms_tol
        self.centroid_tol = centroid_tol
        self.zcr_tol = zcr_tol

    def _features_compatible(self, a: Fingerprint, b: Fingerprint) -> bool:
        """Acoustic pre-filter: reject pairs that differ too much in basic features."""
        if abs(a.duration_sec - b.duration_sec) > self.duration_tol:
            return False

        def rel_diff(x: float, y: float) -> float:
            return abs(x - y) / max(abs(x), abs(y), 1e-8)

        if a.energy_mean > 0 and b.energy_mean > 0:
            if rel_diff(a.energy_mean, b.energy_mean) > self.rms_tol:
                return False
        if a.spectral_centroid > 0 and b.spectral_centroid > 0:
            if rel_diff(a.spectral_centroid, b.spectral_centroid) > self.centroid_tol:
                return False
        if a.zcr_mean > 0 and b.zcr_mean > 0:
            if rel_diff(a.zcr_mean, b.zcr_mean) > self.zcr_tol:
                return False

        return True

    def _score(self, fp_a: Fingerprint, fp_b: Fingerprint) -> float:
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
        if (
            len(pairs_a_raw) < self.min_matching_pairs
            or len(pairs_b_raw) < self.min_matching_pairs
        ):
            return 0.0

        pairs_a = np.array(pairs_a_raw, dtype=np.int32)  # (Na, 4): f1, f2, dt, t_offset
        pairs_b = np.array(pairs_b_raw, dtype=np.int32)  # (Nb, 4)

        # Sort B by sum of first 3 dims for binary-search pre-filtering
        sums_b = pairs_b[:, 0] + pairs_b[:, 1] + pairs_b[:, 2]
        order_b = np.argsort(sums_b)
        pairs_b_sorted = pairs_b[order_b]
        sums_b_sorted = sums_b[order_b]

        sum_tol = 2 * self.freq_tol + self.dt_tol

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
                (np.abs(pairs_a[i, 0] - candidates_sorted[:, 0]) <= self.freq_tol)
                & (np.abs(pairs_a[i, 1] - candidates_sorted[:, 1]) <= self.freq_tol)
                & (np.abs(pairs_a[i, 2] - candidates_sorted[:, 2]) <= self.dt_tol)
            )
            close_idxs = np.where(close_mask)[0]
            if len(close_idxs) > 0:
                dists = np.abs(pairs_a[i, :3] - candidates_sorted[close_idxs, :3]).sum(
                    axis=1
                )
                best_local = close_idxs[dists.argmin()]
                matched_a.append(i)
                matched_b.append(int(order_b[lo + best_local]))

        if len(matched_a) < self.min_matching_pairs:
            return 0.0

        # Temporal coherence with tolerance (sliding window on sorted offsets)
        offsets = pairs_a[matched_a, 3] - pairs_b[matched_b, 3]
        sorted_offsets = np.sort(offsets)

        best_count = 0
        left = 0
        for right in range(len(sorted_offsets)):
            while sorted_offsets[right] - sorted_offsets[left] > 2 * self.offset_tol:
                left += 1
            best_count = max(best_count, right - left + 1)

        return best_count / (min(len(pairs_a), len(pairs_b)) + 1)

    def is_similar(self, fp_a: Fingerprint, fp_b: Fingerprint) -> bool:
        """Return True if two fingerprints pass the acoustic pre-filter and the similarity threshold."""
        if not self._features_compatible(fp_a, fp_b):
            return False
        return self._score(fp_a, fp_b) >= self.similarity_threshold

    def params(self) -> dict:
        return {
            "min_matching_pairs": self.min_matching_pairs,
            "similarity_threshold": self.similarity_threshold,
            "freq_tol": self.freq_tol,
            "dt_tol": self.dt_tol,
            "offset_tol": self.offset_tol,
            "duration_tol": self.duration_tol,
            "rms_tol": self.rms_tol,
            "centroid_tol": self.centroid_tol,
            "zcr_tol": self.zcr_tol,
        }

    def params_hash(self) -> str:
        return make_params_hash(self.params())

    def build_similarity_index(
        self, fingerprints: list[Fingerprint]
    ) -> FingerprintsIndex:
        """Build a FingerprintsIndex over fingerprints, using this instance's tolerances."""
        return FingerprintsIndex(
            self.freq_tol, self.dt_tol, self.min_matching_pairs
        ).build(fingerprints)
