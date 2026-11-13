"""FingerprintsIndex and FingerprintsCompare._score are vectorized with numpy: they must
give the results of the plain Python algorithms they replace, kept here as references.
"""

from collections import Counter, defaultdict

import numpy as np
import pytest

from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.compare import (
    _ADJ_3D,
    FingerprintsCompare,
    FingerprintsIndex,
)
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.fingerprint import (
    Fingerprint,
)


def fingerprint(pairs: list[tuple]) -> Fingerprint:
    return Fingerprint(
        duration_sec=5.0,
        energy_mean=0.05,
        spectral_centroid=2000.0,
        zcr_mean=0.1,
        pairs=pairs,
    )


def random_fingerprint(rng, n_pairs: int, spread: int, pair_size: int = 5):
    """`spread` is the range of the frequencies: low, the pairs of two fingerprints are often
    close to each other, so that ties and multiple matches are tested."""
    return fingerprint(
        [
            (
                int(rng.integers(0, spread)),
                int(rng.integers(0, spread)),
                int(rng.integers(1, max(2, spread // 2))),
                int(rng.integers(0, 200)),
                int(rng.integers(1000, 2000)),
            )[:pair_size]
            for _ in range(n_pairs)
        ]
    )


def other_broadcast(rng, source: Fingerprint, kept: float) -> Fingerprint:
    """The same audio broadcast another time: a share of the pairs, slightly moved."""
    shift = int(rng.integers(-5, 6))
    return fingerprint(
        [
            (
                f1 + int(rng.integers(-2, 3)),
                f2 + int(rng.integers(-2, 3)),
                dt + int(rng.integers(-1, 2)),
                t + shift + int(rng.integers(-2, 3)),
                *rest,
            )
            for f1, f2, dt, t, *rest in source.pairs
            if rng.random() < kept
        ]
    )


def reference_candidates(
    fingerprints: list[Fingerprint],
    query: Fingerprint,
    freq_tol: int,
    dt_tol: int,
    min_matching_pairs: int,
) -> set[int]:
    """FingerprintsIndex before it was vectorized: a dict of key -> set of fingerprints."""
    index = defaultdict(set)
    for i, fp in enumerate(fingerprints):
        for f1, f2, dt, *_ in fp.pairs or []:
            index[(f1 // freq_tol, f2 // freq_tol, dt // dt_tol)].add(i)

    counts = Counter()
    for f1, f2, dt, *_ in query.pairs or []:
        for df1, df2, ddt in _ADJ_3D:
            key = (f1 // freq_tol + df1, f2 // freq_tol + df2, dt // dt_tol + ddt)
            for j in index.get(key, ()):
                counts[j] += 1
    return {j for j, count in counts.items() if count >= min_matching_pairs}


def reference_score(
    compare: FingerprintsCompare, fp_a: Fingerprint, fp_b: Fingerprint
) -> float:
    """FingerprintsCompare._score before it was vectorized: one loop over the pairs of A."""
    if (
        len(fp_a.pairs) < compare.min_matching_pairs
        or len(fp_b.pairs) < compare.min_matching_pairs
    ):
        return 0.0
    pairs_a = np.array(fp_a.pairs, dtype=np.int32)
    pairs_b = np.array(fp_b.pairs, dtype=np.int32)
    sums_b = pairs_b[:, 0] + pairs_b[:, 1] + pairs_b[:, 2]
    order_b = np.argsort(sums_b)
    pairs_b_sorted = pairs_b[order_b]
    sums_b_sorted = sums_b[order_b]
    sum_tol = 2 * compare.freq_tol + compare.dt_tol

    matched_a = []
    matched_b = []
    for i in range(len(pairs_a)):
        s_a = int(pairs_a[i, 0]) + int(pairs_a[i, 1]) + int(pairs_a[i, 2])
        lo = np.searchsorted(sums_b_sorted, s_a - sum_tol, side="left")
        hi = np.searchsorted(sums_b_sorted, s_a + sum_tol, side="right")
        if lo >= hi:
            continue
        candidates = pairs_b_sorted[lo:hi]
        close = np.where(
            (np.abs(pairs_a[i, 0] - candidates[:, 0]) <= compare.freq_tol)
            & (np.abs(pairs_a[i, 1] - candidates[:, 1]) <= compare.freq_tol)
            & (np.abs(pairs_a[i, 2] - candidates[:, 2]) <= compare.dt_tol)
        )[0]
        if len(close) > 0:
            dists = np.abs(pairs_a[i, :3] - candidates[close, :3]).sum(axis=1)
            matched_a.append(i)
            matched_b.append(int(order_b[lo + close[dists.argmin()]]))

    if len(matched_a) < compare.min_matching_pairs:
        return 0.0

    sorted_offsets = np.sort(pairs_a[matched_a, 3] - pairs_b[matched_b, 3])
    best_count = 0
    left = 0
    for right in range(len(sorted_offsets)):
        while sorted_offsets[right] - sorted_offsets[left] > 2 * compare.offset_tol:
            left += 1
        best_count = max(best_count, right - left + 1)
    return best_count / (min(len(pairs_a), len(pairs_b)) + 1)


@pytest.mark.parametrize(
    "freq_tol,dt_tol,min_matching_pairs,spread",
    [(2, 1, 10, 40), (2, 1, 10, 12), (3, 2, 5, 40), (1, 1, 1, 200), (2, 1, 3, 1024)],
)
def test_index_gives_the_candidates_of_the_reference(
    freq_tol, dt_tol, min_matching_pairs, spread
):
    rng = np.random.default_rng(spread)
    indexed = [
        random_fingerprint(rng, int(rng.integers(0, 35)), spread) for _ in range(300)
    ]
    queries = [random_fingerprint(rng, 30, spread) for _ in range(100)]
    # Other broadcasts of indexed fingerprints, and the indexed fingerprints themselves
    # (the grouping of the chunks queries the index with the chunks it holds)
    queries += [other_broadcast(rng, fp, kept=0.8) for fp in indexed[:100] if fp.pairs]
    queries += indexed[:100]

    index = FingerprintsIndex(freq_tol, dt_tol, min_matching_pairs).build(indexed)

    found = 0
    for query in queries:
        expected = reference_candidates(
            indexed, query, freq_tol, dt_tol, min_matching_pairs
        )
        assert index.get_similar_indices(query) == expected
        assert index.get_similar_indices(query, 2) == reference_candidates(
            indexed, query, freq_tol, dt_tol, 2
        )
        found += len(expected)
    assert found > 0


def test_index_without_pairs():
    empty = fingerprint([])
    full = fingerprint([(10, 20, 5, 0, 1500)] * 12)

    assert FingerprintsIndex(2, 1, 5).build([]).get_similar_indices(full) == set()
    assert FingerprintsIndex(2, 1, 5).build([empty]).get_similar_indices(full) == set()

    index = FingerprintsIndex(2, 1, 5).build([empty, full, fingerprint(None)])
    assert index.get_similar_indices(empty) == set()
    assert index.get_similar_indices(fingerprint(None)) == set()
    assert index.get_similar_indices(full) == {1}


def test_index_counts_a_fingerprint_once_per_key_and_the_query_pairs_with_duplicates():
    # 12 identical pairs: one key in the index, counted once per pair of the query
    repeated = fingerprint([(10, 20, 5, 0, 1500)] * 12)
    index = FingerprintsIndex(2, 1, 10).build([repeated])

    assert index.get_similar_indices(repeated) == {0}
    assert index.get_similar_indices(fingerprint([(10, 20, 5, 0, 1500)] * 9)) == set()
    # A pair in a neighbor cell (frequency 12 // 2 = 6, next to 10 // 2 = 5) is found,
    # one two cells away (14 // 2 = 7) is not
    assert index.get_similar_indices(fingerprint([(12, 20, 5, 0, 1500)] * 10)) == {0}
    assert index.get_similar_indices(fingerprint([(14, 20, 5, 0, 1500)] * 10)) == set()


def test_index_with_values_out_of_the_packed_range():
    # Far above any frequency bin or frame count: clipped, never an error nor a miss
    huge = fingerprint([(5_000_000, -5_000_000, 3_000_000, 0, 1500)] * 10)
    index = FingerprintsIndex(2, 1, 10).build(
        [huge, fingerprint([(1, 1, 1, 0, 1)] * 10)]
    )

    assert index.get_similar_indices(huge) == {0}


@pytest.mark.parametrize(
    "freq_tol,dt_tol,offset_tol,min_matching_pairs,spread,pair_size",
    [
        (2, 1, 2, 10, 40, 5),
        (2, 1, 2, 10, 8, 5),
        (3, 2, 1, 5, 20, 5),
        (1, 1, 0, 1, 6, 5),
        (2, 1, 2, 10, 40, 4),
    ],
)
def test_score_is_the_score_of_the_reference(
    freq_tol, dt_tol, offset_tol, min_matching_pairs, spread, pair_size
):
    rng = np.random.default_rng(spread + pair_size)
    compare = FingerprintsCompare(
        min_matching_pairs=min_matching_pairs,
        similarity_threshold=0.05,
        freq_tol=freq_tol,
        dt_tol=dt_tol,
        offset_tol=offset_tol,
    )

    positive_scores = 0
    for case in range(1500):
        fp_a = random_fingerprint(rng, int(rng.integers(1, 40)), spread, pair_size)
        if case % 2:
            fp_b = other_broadcast(rng, fp_a, kept=float(rng.uniform(0.3, 1)))
            if not fp_b.pairs:
                continue
        else:
            fp_b = random_fingerprint(rng, int(rng.integers(1, 40)), spread, pair_size)

        expected = reference_score(compare, fp_a, fp_b)
        assert compare._score(fp_a, fp_b) == expected
        assert compare._score(fp_b, fp_a) == reference_score(compare, fp_b, fp_a)
        positive_scores += expected > 0
    assert positive_scores > 100
