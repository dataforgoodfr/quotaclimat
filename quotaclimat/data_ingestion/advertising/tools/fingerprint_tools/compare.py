from __future__ import annotations

from collections import Counter

import numpy as np

from quotaclimat.data_ingestion.advertising.tools.hashing import make_params_hash

from .fingerprint import Fingerprint

# 27 neighbor offsets for 3D adjacency (±1 in each of f1, f2, dt)
_ADJ_3D = [
    (df1, df2, ddt) for df1 in (-1, 0, 1) for df2 in (-1, 0, 1) for ddt in (-1, 0, 1)
]

# A key (f1 // freq_tol, f2 // freq_tol, dt // dt_tol) is packed in one int64, 21 bits
# per dimension, so that the index is a sorted array instead of a dict of tuples. Values
# are frequency bins and frame counts, far below this range; a value outside is clipped,
# which can only add candidates (two keys one cell apart stay at most one cell apart).
_KEY_BITS = 21
_KEY_OFFSET = 1 << (_KEY_BITS - 1)
_KEY_MIN = 1 - _KEY_OFFSET
_KEY_MAX = _KEY_OFFSET - 2
_KEY_WEIGHTS = np.array([1 << (2 * _KEY_BITS), 1 << _KEY_BITS, 1], dtype=np.int64)
# Added to the code of a key, gives the codes of its 27 neighbor cells
_NEIGHBOR_CODE_OFFSETS = np.array(_ADJ_3D, dtype=np.int64) @ _KEY_WEIGHTS


class FingerprintsIndex:
    """
    Inverted index of quantized pair keys, for candidate lookup by fingerprint similarity.

    Tolerances are set once at construction and reused consistently across
    build() and get_similar_indices() calls.

    The index is stored as numpy arrays (sorted key codes and, for each of them, the
    slice of `_postings` listing the fingerprints holding that key) and queried without
    any Python loop: the detection queries it once per fingerprint of the database
    (about a million times), against the tens of thousands of chunks of a week.
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
        self._tols = np.array([freq_tol, freq_tol, dt_tol], dtype=np.int64)
        self._size = 0
        self._codes = np.empty(0, dtype=np.int64)
        self._starts = np.empty(0, dtype=np.int64)
        self._lengths = np.empty(0, dtype=np.int64)
        self._postings = np.empty(0, dtype=np.int64)

    def _pair_codes(self, pairs: list) -> np.ndarray:
        """Code of the key of each pair, duplicates included."""
        keys = np.array(pairs, dtype=np.int64)[:, :3] // self._tols
        np.clip(keys, _KEY_MIN, _KEY_MAX, out=keys)
        keys += _KEY_OFFSET
        return keys @ _KEY_WEIGHTS

    def build(self, fingerprints: list[Fingerprint]) -> "FingerprintsIndex":
        """
        Build the index over the given fingerprints, keyed by their position in the list.

        Key = (f1 // freq_tol, f2 // freq_tol, dt // dt_tol).
        Spreads pairs across a large key space so each cell stays small,
        enabling fast candidate lookups in get_similar_indices().
        A fingerprint is listed once per key, however many of its pairs share it.
        """
        codes_per_fp: list[np.ndarray] = []
        positions: list[int] = []
        for i, fp in enumerate(fingerprints):
            pairs = fp.pairs or []
            if not pairs:
                continue
            codes_per_fp.append(np.unique(self._pair_codes(pairs)))
            positions.append(i)

        self._size = len(fingerprints)
        if not codes_per_fp:
            self._codes = np.empty(0, dtype=np.int64)
            self._starts = np.empty(0, dtype=np.int64)
            self._lengths = np.empty(0, dtype=np.int64)
            self._postings = np.empty(0, dtype=np.int64)
            return self

        all_codes = np.concatenate(codes_per_fp)
        all_positions = np.repeat(
            np.array(positions, dtype=np.int64), [len(c) for c in codes_per_fp]
        )
        order = np.argsort(all_codes, kind="stable")
        self._postings = all_positions[order]
        self._codes, self._starts, self._lengths = np.unique(
            all_codes[order], return_index=True, return_counts=True
        )
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
        if not pairs or not len(self._codes):
            return set()
        threshold = (
            min_matching_pairs
            if min_matching_pairs is not None
            else self.min_matching_pairs
        )

        # Codes of the 27 neighbor cells of each pair, then their rank in the index
        cells = (self._pair_codes(pairs)[:, None] + _NEIGHBOR_CODE_OFFSETS).ravel()
        ranks = np.searchsorted(self._codes, cells)
        ranks[ranks == len(self._codes)] = 0
        ranks = ranks[self._codes[ranks] == cells]
        lengths = self._lengths[ranks]
        total = int(lengths.sum())
        if total == 0 or total < threshold:
            return set()

        # Positions in _postings of the fingerprints of every cell found: for a cell
        # starting at `start` with `length` fingerprints, start, start + 1, ...
        ends = np.cumsum(lengths)
        where = np.arange(total) + np.repeat(
            self._starts[ranks] - ends + lengths, lengths
        )
        counts = np.bincount(self._postings[where], minlength=self._size)
        return set(np.flatnonzero(counts >= max(threshold, 1)).tolist())


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
        amp_tol: int = 100,
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
        # Tolerance on quantized combined peak amplitude (a1+a2, x1000), used
        # only by _score_alt as an extra join filter — see PairGenerator.
        self.amp_tol = amp_tol

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

        Every pair of A is compared to every pair of B at once (a fingerprint has a few
        dozen pairs): the detection scores hundreds of thousands of fingerprint couples.
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

        # B sorted by the sum of its first 3 dims: between pairs of B at the same
        # distance of a pair of A, the first one in this order is the one matched
        order_b = np.argsort(pairs_b[:, 0] + pairs_b[:, 1] + pairs_b[:, 2])
        pairs_b = pairs_b[order_b]

        # (Na, Nb, 3) distances per dimension, then the closest pair of B in tolerance
        diffs = np.abs(pairs_a[:, None, :3] - pairs_b[None, :, :3])
        close = (
            (diffs[:, :, 0] <= self.freq_tol)
            & (diffs[:, :, 1] <= self.freq_tol)
            & (diffs[:, :, 2] <= self.dt_tol)
        )
        matched_a = np.flatnonzero(close.any(axis=1))
        if len(matched_a) < self.min_matching_pairs:
            return 0.0
        dists = np.where(close, diffs.sum(axis=2), np.iinfo(np.int32).max)
        matched_b = dists[matched_a].argmin(axis=1)

        # Temporal coherence with tolerance: the largest number of matched pairs whose
        # offsets fit in a window of 2 * offset_tol
        offsets = np.sort(pairs_a[matched_a, 3] - pairs_b[matched_b, 3])
        window_starts = np.searchsorted(offsets, offsets - 2 * self.offset_tol)
        best_count = int((np.arange(len(offsets)) - window_starts).max()) + 1

        return best_count / (min(len(pairs_a), len(pairs_b)) + 1)

    def _score_alt(self, fp_a: Fingerprint, fp_b: Fingerprint) -> float:
        """
        Alternate scoring, structured to mirror a plain SQL query instead of a
        numpy perf-optimized scan. Same result shape as _score(), but drops the
        sum-based binary-search prefilter (reverts to an O(Na*Nb) join) and
        replaces the exact sliding-window offset scan with a bucketed histogram.

        Also uses the pair's combined peak amplitude (5th field, see
        PairGenerator) as an extra join condition, narrowing candidates
        further before the distance check. Falls back to no amplitude filter
        for pairs generated before this field existed (4-tuples).

        SQL equivalent (conceptually):
            WITH matches AS (
              SELECT a.id AS a_id, b.id AS b_id, a.t_offset - b.t_offset AS offset,
                     ROW_NUMBER() OVER (
                       PARTITION BY a.id
                       ORDER BY abs(a.f1-b.f1) + abs(a.f2-b.f2) + abs(a.dt-b.dt)
                     ) AS rn
              FROM pairs_a a JOIN pairs_b b
                ON abs(a.f1 - b.f1) <= freq_tol
               AND abs(a.f2 - b.f2) <= freq_tol
               AND abs(a.dt - b.dt) <= dt_tol
               AND abs(a.amp - b.amp) <= amp_tol
            ),
            nearest AS (SELECT * FROM matches WHERE rn = 1),
            buckets AS (
              SELECT round(offset / (2.0 * offset_tol)) AS bucket, count(*) AS n
              FROM nearest GROUP BY bucket
            )
            SELECT max(n) FROM buckets  -- best_count
        """
        pairs_a_raw = fp_a.pairs or []
        pairs_b_raw = fp_b.pairs or []
        if (
            len(pairs_a_raw) < self.min_matching_pairs
            or len(pairs_b_raw) < self.min_matching_pairs
        ):
            return 0.0

        pairs_a = np.array(pairs_a_raw, dtype=np.int32)  # (Na, 4 or 5): f1, f2, dt, t_offset[, amp]
        pairs_b = np.array(pairs_b_raw, dtype=np.int32)  # (Nb, 4 or 5)
        has_amp = pairs_a.shape[1] >= 5 and pairs_b.shape[1] >= 5

        # JOIN pairs_a a, pairs_b b ON per-dimension tolerance
        close_mask = (
            np.abs(pairs_a[:, None, 0] - pairs_b[None, :, 0]) <= self.freq_tol
        ) & (
            np.abs(pairs_a[:, None, 1] - pairs_b[None, :, 1]) <= self.freq_tol
        ) & (
            np.abs(pairs_a[:, None, 2] - pairs_b[None, :, 2]) <= self.dt_tol
        )
        if has_amp:
            close_mask &= (
                np.abs(pairs_a[:, None, 4] - pairs_b[None, :, 4]) <= self.amp_tol
            )

        offsets = []
        for i in range(len(pairs_a)):
            j_candidates = np.where(close_mask[i])[0]
            if len(j_candidates) == 0:
                continue
            # ORDER BY distance ... QUALIFY ROW_NUMBER() OVER (...) = 1
            dists = np.abs(pairs_a[i, :3] - pairs_b[j_candidates, :3]).sum(axis=1)
            best_j = j_candidates[dists.argmin()]
            offsets.append(int(pairs_a[i, 3]) - int(pairs_b[best_j, 3]))

        if len(offsets) < self.min_matching_pairs:
            return 0.0

        # GROUP BY round(offset / (2*offset_tol)), take the largest bucket
        bucket_width = 2 * self.offset_tol
        buckets = Counter(
            round(offset / bucket_width) for offset in offsets
        )
        best_count = max(buckets.values())

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
            "amp_tol": self.amp_tol,
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
