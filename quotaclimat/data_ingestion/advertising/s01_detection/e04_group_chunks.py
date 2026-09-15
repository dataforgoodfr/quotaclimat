"""
Regroupement de chunks audio par fingerprints pré-calculés
=============================================================
Utilise les paires de pics pré-calculées par e02 (constellation maps)
pour comparer et regrouper les chunks identiques via distance-based scoring.
"""

import logging
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Dict, List

import numpy as np

from quotaclimat.data_ingestion.advertising.s01_detection.tools.common_objects import (
    Chunk,
    Fingerprint,
)
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.compare import (
    FingerprintsCompare,
)
from quotaclimat.data_ingestion.advertising.tools.interactive_tqdm import (
    interactive_tqdm,
)

logger = logging.getLogger(__name__)


@dataclass
class ChunkGroup:
    count: int
    duration_mean: float
    duration_std: float
    occurrences: list[Chunk]

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            count=data["count"],
            duration_mean=data["duration_mean"],
            duration_std=data["duration_std"],
            occurrences=[Chunk.from_dict(occ) for occ in data["occurrences"]],
        )


def _cluster(
    chunks: list[Chunk],
    compare: FingerprintsCompare,
) -> Dict[int, List[int]]:
    """
    Group similar chunks using inverted index on pair sums + Union-Find.

    Builds an inverted index of quantized pair sums (f1+f2+dt) to prune
    candidate pairs before running the full distance-based comparison.
    Only chunk pairs sharing >= min_matching_pairs bucket overlaps are compared.
    Returns {group_id: [chunk indices]}.
    """
    n = len(chunks)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        parent[find(x)] = find(y)

    # --- Inverted index on 3D pair keys to prune candidate pairs ---

    # Step 1: build index over all chunks
    fps = [
        c.fingerprint for c in interactive_tqdm(chunks, desc="Indexation fingerprints")
    ]
    index = compare.build_similarity_index(fps)

    # Step 2: for each chunk query the index to find candidates with j > i
    candidates: set[tuple[int, int]] = set()
    for i, fp in interactive_tqdm(enumerate(fps), desc="Recherche candidats", total=n):
        for j in index.get_similar_indices(fp):
            if j > i:
                candidates.add((i, j))

    logger.debug(
        f"Clustering {n} chunks — {len(candidates)} candidate pairs "
        f"(pruned from {n * (n - 1) // 2} exhaustive)"
    )

    # Step 4: full comparison on candidates only
    matches = 0
    for i, j in interactive_tqdm(
        candidates,
        desc="Comparaison fingerprints",
        total=len(candidates),
    ):
        if compare.is_similar(chunks[i].fingerprint, chunks[j].fingerprint):
            union(i, j)
            matches += 1

    logger.debug(f"  {matches} similar pairs found")

    groups = defaultdict(list)
    for i in range(n):
        groups[find(i)].append(i)

    return dict(groups)


def group_chunks(source: List[Chunk], compare: FingerprintsCompare) -> list[ChunkGroup]:
    # Filter out very short chunks
    chunks = [c for c in source if c.fingerprint.duration_sec >= 0.5]
    logger.debug(f"{len(chunks)} chunks to group")

    groups = _cluster(chunks, compare)

    report_groups: list[ChunkGroup] = []
    for member_idxs in groups.values():
        members = sorted([chunks[i] for i in member_idxs], key=lambda c: c.start_sec)
        durations = [c.fingerprint.duration_sec for c in members]

        report_groups.append(
            ChunkGroup(
                count=len(members),
                duration_mean=round(float(np.mean(durations)), 2),
                duration_std=round(float(np.std(durations)), 2),
                occurrences=members,
            )
        )

    report_groups.sort(key=lambda g: -g.count)
    return report_groups


def canonical(chunks: list[Chunk], freq_tol: int = 2, dt_tol: int = 1) -> Chunk:
    """
    Build a canonical Chunk from multiple occurrences of the same audio segment.

    Uses cluster + median approach for maximum future matching probability:
    1. Pool all (f1, f2, dt, t_offset) tuples from all occurrences, tagged by occurrence index
    2. Greedily cluster: for each unvisited tuple, collect close tuples from other occurrences
    3. Keep clusters spanning >= 50% of occurrences
    4. Canonical tuple = component-wise median (int-rounded) of each cluster

    Acoustic features: median across all occurrences (minimises distance to any future instance).
    Peaks: taken from the richest occurrence (for potential re-computation).
    """
    if len(chunks) == 1:
        return chunks[0]

    n_occurrences = len(chunks)
    min_freq = max(1, n_occurrences // 2)

    # Pool all tuples tagged by occurrence index
    all_tuples = []  # (f1, f2, dt, t_offset, occurrence_idx)
    for occ_idx, chunk in enumerate(chunks):
        for pair in chunk.fingerprint.pairs or []:
            all_tuples.append((*pair[:4], occ_idx))

    if not all_tuples:
        # No pairs at all, fall back to richest occurrence
        richest = max(chunks, key=lambda c: len(c.fingerprint.pairs or []))
        return _build_canonical_chunk(chunks, richest.fingerprint.peaks, [])

    all_tuples_arr = np.array(all_tuples, dtype=np.int32)
    visited = np.zeros(len(all_tuples_arr), dtype=bool)

    canonical_pairs = []
    MIN_CANONICAL_PAIRS = 10

    for idx in range(len(all_tuples_arr)):
        if visited[idx]:
            continue
        visited[idx] = True

        ref = all_tuples_arr[idx]
        # Find close tuples from OTHER occurrences
        cluster_members = [idx]
        cluster_occurrences = {int(ref[4])}

        for jdx in range(idx + 1, len(all_tuples_arr)):
            if visited[jdx]:
                continue
            other = all_tuples_arr[jdx]
            if int(other[4]) in cluster_occurrences:
                continue  # one per occurrence
            if (
                abs(int(ref[0]) - int(other[0])) <= freq_tol
                and abs(int(ref[1]) - int(other[1])) <= freq_tol
                and abs(int(ref[2]) - int(other[2])) <= dt_tol
            ):
                visited[jdx] = True
                cluster_members.append(jdx)
                cluster_occurrences.add(int(other[4]))

        if len(cluster_occurrences) >= min_freq:
            members = all_tuples_arr[cluster_members]
            canonical_pairs.append(
                (
                    int(np.median(members[:, 0])),
                    int(np.median(members[:, 1])),
                    int(np.median(members[:, 2])),
                    int(np.median(members[:, 3])),
                )
            )

    # Fallback: if too few stable pairs, take pairs from the richest occurrence
    if len(canonical_pairs) < MIN_CANONICAL_PAIRS:
        richest = max(chunks, key=lambda c: len(c.fingerprint.pairs or []))
        canonical_pairs = list(richest.fingerprint.pairs or [])
        # Ensure they are 4-tuples
        canonical_pairs = [tuple(p[:4]) for p in canonical_pairs]

    richest = max(chunks, key=lambda c: len(c.fingerprint.pairs or []))
    return _build_canonical_chunk(chunks, richest.fingerprint.peaks, canonical_pairs)


def _build_canonical_chunk(
    chunks: list[Chunk], peaks: list, canonical_pairs: list
) -> Chunk:
    """Helper to build a canonical Chunk with median acoustic features."""
    durations = [c.fingerprint.duration_sec for c in chunks]
    energies = [c.fingerprint.energy_mean for c in chunks]
    centroids = [c.fingerprint.spectral_centroid for c in chunks]
    zcrs = [c.fingerprint.zcr_mean for c in chunks]

    richest = max(chunks, key=lambda c: len(c.fingerprint.pairs or []))

    return Chunk(
        start_sec=richest.start_sec,
        end_sec=richest.end_sec,
        channel=richest.channel,
        fingerprint=Fingerprint(
            duration_sec=float(np.median(durations)),
            energy_mean=float(np.median(energies)),
            spectral_centroid=float(np.median(centroids)),
            zcr_mean=float(np.median(zcrs)),
            peaks=peaks,
            pairs=canonical_pairs,
        ),
    )


def debug_pair(a: Chunk, b: Chunk, compare: "FingerprintsCompare") -> None:
    """
    Print a step-by-step explanation of why two chunks are or are not grouped.

    Usage:
        debug_pair(chunks[i], chunks[j], fingerprints_compare)
    """
    PASS = "✓"
    FAIL = "✗"

    def rel_diff(x: float, y: float) -> float:
        return abs(x - y) / max(abs(x), abs(y), 1e-8)

    print("=" * 60)
    print("DEBUG: chunk pair grouping analysis")
    print(f"  A: [{a.start_sec:.2f}s – {a.end_sec:.2f}s]  channel={a.channel}")
    print(f"  B: [{b.start_sec:.2f}s – {b.end_sec:.2f}s]  channel={b.channel}")
    print("=" * 60)

    # ── Step 1: minimum duration filter (applied in run()) ──────────────
    print("\n[1] Minimum duration filter (>= 0.5 s)")
    a_ok = a.fingerprint.duration_sec >= 0.5
    b_ok = b.fingerprint.duration_sec >= 0.5
    print(
        f"    A duration: {a.fingerprint.duration_sec:.3f}s  {PASS if a_ok else FAIL}"
    )
    print(
        f"    B duration: {b.fingerprint.duration_sec:.3f}s  {PASS if b_ok else FAIL}"
    )
    if not (a_ok and b_ok):
        print(
            "  → BLOCKED: one or both chunks are too short and would be filtered out."
        )
        return

    # ── Step 2: acoustic pre-filter (_features_compatible) ──────────────
    print("\n[2] Acoustic pre-filter (_features_compatible)")

    fp_a, fp_b = a.fingerprint, b.fingerprint

    dur_diff = abs(fp_a.duration_sec - fp_b.duration_sec)
    dur_ok = dur_diff <= compare.duration_tol
    print(
        f"    duration |A-B| = {dur_diff:.3f}s  (tol={compare.duration_tol})  {PASS if dur_ok else FAIL}"
    )

    rms_ok = True
    if fp_a.energy_mean > 0 and fp_b.energy_mean > 0:
        rms_diff = rel_diff(fp_a.energy_mean, fp_b.energy_mean)
        rms_ok = rms_diff <= compare.rms_tol
        print(
            f"    energy_mean rel_diff = {rms_diff:.4f}  (tol={compare.rms_tol})  {PASS if rms_ok else FAIL}"
            f"  (A={fp_a.energy_mean:.4f}, B={fp_b.energy_mean:.4f})"
        )
    else:
        print("    energy_mean: skipped (one value is 0)")

    centroid_ok = True
    if fp_a.spectral_centroid > 0 and fp_b.spectral_centroid > 0:
        centroid_diff = rel_diff(fp_a.spectral_centroid, fp_b.spectral_centroid)
        centroid_ok = centroid_diff <= compare.centroid_tol
        print(
            f"    spectral_centroid rel_diff = {centroid_diff:.4f}  (tol={compare.centroid_tol})  {PASS if centroid_ok else FAIL}"
            f"  (A={fp_a.spectral_centroid:.1f}, B={fp_b.spectral_centroid:.1f})"
        )
    else:
        print("    spectral_centroid: skipped (one value is 0)")

    zcr_ok = True
    if fp_a.zcr_mean > 0 and fp_b.zcr_mean > 0:
        zcr_diff = rel_diff(fp_a.zcr_mean, fp_b.zcr_mean)
        zcr_ok = zcr_diff <= compare.zcr_tol
        print(
            f"    zcr_mean rel_diff = {zcr_diff:.4f}  (tol={compare.zcr_tol})  {PASS if zcr_ok else FAIL}"
            f"  (A={fp_a.zcr_mean:.4f}, B={fp_b.zcr_mean:.4f})"
        )
    else:
        print("    zcr_mean: skipped (one value is 0)")

    features_ok = dur_ok and rms_ok and centroid_ok and zcr_ok
    if not features_ok:
        print("  → BLOCKED: acoustic pre-filter rejected this pair.")
        return

    # ── Step 3: distance-based pair matching ────────────────────────────
    print("\n[3] Distance-based pair matching")
    pairs_a = fp_a.pairs or []
    pairs_b = fp_b.pairs or []
    print(f"    pairs A: {len(pairs_a)},  pairs B: {len(pairs_b)}")

    if not pairs_a or not pairs_b:
        print("  → BLOCKED: one or both chunks have no pairs.")
        return

    arr_a = np.array(pairs_a, dtype=np.int32)
    arr_b = np.array(pairs_b, dtype=np.int32)

    close = (
        (np.abs(arr_a[:, None, 0] - arr_b[None, :, 0]) <= compare.freq_tol)
        & (np.abs(arr_a[:, None, 1] - arr_b[None, :, 1]) <= compare.freq_tol)
        & (np.abs(arr_a[:, None, 2] - arr_b[None, :, 2]) <= compare.dt_tol)
    )

    matched_a, matched_b = [], []
    for i in range(len(arr_a)):
        candidates = np.where(close[i])[0]
        if len(candidates) > 0:
            dists = np.abs(arr_a[i, :3] - arr_b[candidates, :3]).sum(axis=1)
            best = candidates[dists.argmin()]
            matched_a.append(i)
            matched_b.append(best)

    n_matched = len(matched_a)
    match_ok = n_matched >= compare.min_matching_pairs
    print(
        f"    close matches: {n_matched}  (min={compare.min_matching_pairs})  {PASS if match_ok else FAIL}"
    )

    if n_matched > 0:
        print("    Sample matches (first 5):")
        for k in range(min(5, n_matched)):
            ia, ib = matched_a[k], matched_b[k]
            pa, pb = arr_a[ia], arr_b[ib]
            dist = np.abs(pa[:3] - pb[:3]).sum()
            print(f"      A[{ia}]={tuple(pa[:3])} ↔ B[{ib}]={tuple(pb[:3])}  L1={dist}")

    if not match_ok:
        print("  → BLOCKED: not enough close pairs to score.")
        return

    # ── Step 4: temporal coherence score ────────────────────────────────
    print("\n[4] Temporal coherence score")
    offsets = arr_a[matched_a, 3] - arr_b[matched_b, 3]
    sorted_offsets = np.sort(offsets)

    best_count = 0
    best_offset = 0
    left = 0
    for right in range(len(sorted_offsets)):
        while sorted_offsets[right] - sorted_offsets[left] > 2 * compare.offset_tol:
            left += 1
        count = right - left + 1
        if count > best_count:
            best_count = count
            best_offset = int(sorted_offsets[(left + right) // 2])

    min_pairs = min(len(arr_a), len(arr_b)) + 1
    score = best_count / min_pairs
    print(
        f"    best offset cluster: center={best_offset}, coherent={best_count}/{n_matched}"
    )
    print(f"    score = {best_count} / {min_pairs} = {score:.4f}")
    print(f"    similarity_threshold = {compare.similarity_threshold}")
    score_ok = score >= compare.similarity_threshold
    print(
        f"    {PASS if score_ok else FAIL} score {'≥' if score_ok else '<'} threshold"
    )
    if not score_ok:
        print("  → BLOCKED: score below similarity threshold.")
        return

    print("\n" + "=" * 60)
    print("  → These two chunks WOULD be grouped together.")
    print("=" * 60)


if __name__ == "__main__":
    debug_pair(
        Chunk.from_dict(
            {
                "start_sec": 1746729816.595737,
                "end_sec": 1746729847.9891157,
                "channel": "tf1",
                "duration_sec": 6.21,
                "energy_mean": 0.04,
                "spectral_centroid": 1978.38,
                "zcr_mean": 0.14,
                "peaks": [
                    [40, 8, 1.0],
                    [25, 8, 0.9966],
                    [2, 37, 0.996],
                    [46, 44, 0.987],
                    [4, 90, 0.9841],
                    [57, 27, 0.981],
                    [3, 8, 0.9802],
                    [28, 35, 0.9798],
                    [35, 83, 0.9787],
                    [48, 77, 0.9658],
                    [38, 47, 0.9576],
                    [77, 42, 0.9546],
                    [79, 32, 0.9479],
                    [18, 110, 0.9471],
                    [48, 96, 0.946],
                    [79, 98, 0.9446],
                    [81, 23, 0.9372],
                    [84, 62, 0.9346],
                    [66, 57, 0.9311],
                    [68, 68, 0.931],
                ],
                "pairs": [
                    [8, 8, 15, 25, 1997],
                    [37, 8, 38, 2, 1996],
                    [37, 8, 23, 2, 1993],
                    [8, 44, 6, 40, 1987],
                    [90, 8, 36, 4, 1984],
                    [8, 44, 21, 25, 1984],
                    [37, 44, 44, 2, 1983],
                    [8, 27, 17, 40, 1981],
                    [90, 8, 21, 4, 1981],
                    [8, 8, 37, 3, 1980],
                    [37, 90, 2, 2, 1980],
                    [35, 8, 12, 28, 1980],
                    [83, 8, 5, 35, 1979],
                    [8, 27, 32, 25, 1978],
                    [8, 8, 22, 3, 1977],
                    [8, 35, 3, 25, 1976],
                    [90, 44, 42, 4, 1971],
                    [44, 27, 11, 46, 1968],
                    [8, 44, 43, 3, 1967],
                    [35, 44, 18, 28, 1967],
                    [8, 77, 8, 40, 1966],
                    [83, 44, 11, 35, 1966],
                    [90, 27, 53, 4, 1965],
                    [8, 90, 1, 3, 1964],
                    [35, 27, 29, 28, 1961],
                    [83, 27, 22, 35, 1960],
                    [35, 83, 7, 28, 1958],
                    [47, 8, 2, 38, 1958],
                    [8, 42, 37, 40, 1955],
                    [44, 77, 2, 46, 1953],
                ],
            }
        ),
        Chunk.from_dict(
            {
                "start_sec": 1746729816.595737,
                "end_sec": 1746729847.9891157,
                "channel": "tf1",
                "duration_sec": 6.21,
                "energy_mean": 0.04,
                "spectral_centroid": 2007.12,
                "zcr_mean": 0.14,
                "peaks": [
                    [3, 36, 1.0],
                    [41, 8, 0.9977],
                    [26, 8, 0.9919],
                    [5, 89, 0.9847],
                    [47, 44, 0.9742],
                    [78, 42, 0.973],
                    [28, 33, 0.9712],
                    [58, 27, 0.9699],
                    [19, 109, 0.9697],
                    [3, 8, 0.9658],
                    [11, 8, 0.9614],
                    [80, 32, 0.9513],
                    [38, 78, 0.949],
                    [41, 89, 0.9471],
                    [48, 77, 0.9466],
                    [13, 94, 0.9417],
                    [80, 97, 0.9414],
                    [30, 60, 0.938],
                    [82, 22, 0.9366],
                    [62, 81, 0.9355],
                ],
                "pairs": [
                    (36, 8, 38, 3, 1998),
                    (36, 8, 23, 3, 1992),
                    (8, 8, 15, 26, 1990),
                    (36, 89, 2, 3, 1985),
                    (89, 8, 36, 5, 1982),
                    (89, 8, 21, 5, 1977),
                    (36, 44, 44, 3, 1974),
                    (8, 44, 6, 41, 1972),
                    (8, 42, 37, 41, 1971),
                    (33, 8, 13, 28, 1969),
                    (8, 27, 17, 41, 1968),
                    (109, 8, 22, 19, 1967),
                    (8, 44, 21, 26, 1966),
                    (8, 42, 52, 26, 1965),
                    (8, 8, 38, 3, 1964),
                    (8, 33, 2, 26, 1963),
                    (109, 8, 7, 19, 1962),
                    (8, 8, 30, 11, 1959),
                    (89, 44, 42, 5, 1959),
                    (8, 8, 23, 3, 1958),
                    (89, 42, 73, 5, 1958),
                    (8, 8, 15, 11, 1953),
                    (8, 89, 2, 3, 1950),
                    (8, 32, 39, 41, 1949),
                    (44, 42, 31, 47, 1947),
                    (78, 8, 3, 38, 1947),
                    (33, 44, 19, 28, 1945),
                    (33, 42, 50, 28, 1944),
                    (44, 27, 11, 47, 1944),
                    (109, 44, 28, 19, 1944),
                ],
            }
        ),
        FingerprintsCompare(
            min_matching_pairs=5,
            similarity_threshold=0.05,
            duration_tol=0.4,
            rms_tol=0.05,
            centroid_tol=0.05,
            zcr_tol=0.1,
        ),
    )
