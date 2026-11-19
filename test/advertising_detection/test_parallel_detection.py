"""The comparison of fingerprints runs in worker processes, and the merges of groups of the
classification no longer go through every fragment: same results as before."""

import random

import numpy as np

from quotaclimat.data_ingestion.advertising.s01_detection.e04_group_chunks import (
    _cluster,
)
from quotaclimat.data_ingestion.advertising.s01_detection.e05_classify_fragments import (
    FragmentsClassifier,
)
from quotaclimat.data_ingestion.advertising.s01_detection.tools.common_objects import (
    Chunk,
    Fragment,
)
from quotaclimat.data_ingestion.advertising.s01_detection.tools.parallel import (
    PackedFingerprints,
    ordered_map,
    worker_state,
)
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.compare import (
    FingerprintsCompare,
)
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.fingerprint import (
    Fingerprint,
)

COMPARE = FingerprintsCompare(
    min_matching_pairs=10,
    similarity_threshold=0.05,
    freq_tol=2,
    dt_tol=1,
    offset_tol=2,
    duration_tol=1.0,
    rms_tol=0.1,
    centroid_tol=0.05,
    zcr_tol=0.1,
)


def random_fingerprint(rng, n_pairs: int) -> Fingerprint:
    return Fingerprint(
        duration_sec=float(rng.uniform(4, 6)),
        energy_mean=float(rng.uniform(0.04, 0.05)),
        spectral_centroid=float(rng.uniform(1950, 2050)),
        zcr_mean=0.1,
        peaks=[[1, 2, 0.5]],
        pairs=[
            tuple(
                int(v)
                for v in rng.integers([0, 0, 1, 0, 1000], [40, 40, 20, 200, 2000])
            )
            for _ in range(n_pairs)
        ]
        or None,
    )


def rebroadcast(rng, source: Fingerprint) -> Fingerprint:
    """The same audio broadcast another time: most pairs, slightly moved."""
    shift = int(rng.integers(-5, 6))
    return Fingerprint(
        duration_sec=source.duration_sec,
        energy_mean=source.energy_mean,
        spectral_centroid=source.spectral_centroid,
        zcr_mean=source.zcr_mean,
        pairs=[
            (f1 + int(rng.integers(-1, 2)), f2, dt, t + shift, amp)
            for f1, f2, dt, t, amp in source.pairs
            if rng.random() < 0.8
        ],
    )


def week_of_chunks(seed: int) -> list[Chunk]:
    rng = np.random.default_rng(seed)
    sources = [random_fingerprint(rng, int(rng.integers(0, 40))) for _ in range(60)]
    fingerprints = list(sources)
    for _ in range(300):
        source = sources[int(rng.integers(0, len(sources)))]
        fingerprints.append(rebroadcast(rng, source) if source.pairs else source)
    return [
        Chunk(start_sec=10.0 * i, end_sec=10.0 * i + 5, channel="tf1", fingerprint=fp)
        for i, fp in enumerate(fingerprints)
    ]


def test_packed_fingerprints_compare_like_the_originals():
    rng = np.random.default_rng(1)
    fingerprints = [
        random_fingerprint(rng, int(rng.integers(0, 40))) for _ in range(80)
    ]
    fingerprints += [rebroadcast(rng, fp) for fp in fingerprints if fp.pairs]
    packed = PackedFingerprints.pack(fingerprints)

    assert len(packed) == len(fingerprints)
    index = COMPARE.build_similarity_index(fingerprints)
    packed_index = COMPARE.build_similarity_index(
        [packed.get(i) for i in range(len(packed))]
    )
    for i, fp in enumerate(fingerprints):
        assert packed.get(i).duration_sec == fp.duration_sec
        assert index.get_similar_indices(fp) == packed_index.get_similar_indices(
            packed.get(i)
        )
        for j, other in enumerate(fingerprints):
            assert COMPARE.is_similar(fp, other) == COMPARE.is_similar(
                packed.get(i), packed.get(j)
            )


def test_packed_fingerprints_of_nothing():
    assert len(PackedFingerprints.pack([])) == 0


def _square(payload):
    return payload * payload + worker_state()


def test_ordered_map_keeps_the_order_of_the_tasks():
    tasks = [(f"task {i}", i) for i in range(50)]
    for num_workers in (1, 3):
        assert list(
            ordered_map(
                _square, tasks, state=1, num_workers=num_workers, max_in_flight=4
            )
        ) == [(f"task {i}", i * i + 1) for i in range(50)]


def test_clustering_in_workers_gives_the_groups_of_one_process():
    chunks = week_of_chunks(seed=2)
    in_process = _cluster(chunks, COMPARE, num_workers=1)
    assert len(in_process) < len(chunks)  # rebroadcasts are grouped with their source
    assert _cluster(chunks, COMPARE, num_workers=3) == in_process


def make_fragments(rng: random.Random, n: int, group_ids: list[str]) -> list[Fragment]:
    fragments = []
    start = 0.0
    for i in range(n):
        group_id = rng.choice([None, "known_ad", *group_ids])
        classification = (
            "already_known_ad"
            if group_id == "known_ad"
            else rng.choice(["unknown", "new_ad"])
        )
        fragments.append(
            Fragment(start, start + 5, "tf1", classification, group_id, [i])
        )
        start += 5 + rng.choice([0, 0, 10])
    return fragments


def test_merges_give_the_result_of_merging_groups_one_by_one():
    classifier = FragmentsClassifier()
    rng = random.Random(3)
    for _ in range(2000):
        group_ids = [f"group_{k}" for k in range(rng.randint(1, 6))]
        fragments = make_fragments(rng, rng.randint(0, 40), group_ids)
        # Couples of groups, a group with itself and the id of an ad of the database included
        ids = group_ids + ["known_ad"]
        groups_to_merge = [
            (rng.choice(ids), rng.choice(ids)) for _ in range(rng.randint(0, 8))
        ]

        expected = fragments
        for group_id_to_merge, group_id_to_merge_with in groups_to_merge:
            expected = classifier._merge_groups(
                expected, group_id_to_merge, group_id_to_merge_with
            )
        assert classifier._apply_merges(fragments, groups_to_merge) == expected
