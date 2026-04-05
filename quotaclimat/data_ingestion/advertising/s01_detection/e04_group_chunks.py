"""
Regroupement de chunks audio par fingerprints pré-calculés
=============================================================
Utilise les paires de pics pré-calculées par e02 (constellation maps)
pour comparer et regrouper les chunks identiques via distance-based scoring.
"""

import logging
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from typing import Dict, List

import numpy as np
from tqdm import tqdm

from .tools.common_objects import Chunk, Fingerprint
from .tools.fingerprint.pairs import (
    are_fingerprints_similar,
    make_params_hash,
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
    min_matching_pairs: int,
    similarity_threshold: float,
    freq_tol: int = 2,
    dt_tol: int = 1,
    offset_tol: int = 2,
    duration_tol: float = 0.3,
    rms_tol: float = 0.05,
    centroid_tol: float = 0.05,
    zcr_tol: float = 0.1,
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

    # --- Inverted index on pair sums to prune candidate pairs ---
    sum_tol = 2 * freq_tol + dt_tol

    # Step 1: compute quantized pair-sum buckets per chunk
    chunk_buckets: list[list[int]] = []
    for chunk in tqdm(chunks, desc="Indexation fingerprints"):
        pairs = chunk.fingerprint.pairs or []
        if pairs:
            arr = np.array(pairs, dtype=np.int32)
            raw_sums = arr[:, 0] + arr[:, 1] + arr[:, 2]
            chunk_buckets.append((raw_sums // sum_tol).tolist())
        else:
            chunk_buckets.append([])

    # Step 2: build inverted index (deduplicated per chunk)
    bucket_to_chunks: dict[int, set[int]] = defaultdict(set)
    for i, buckets in enumerate(chunk_buckets):
        for b in set(buckets):
            bucket_to_chunks[b].add(i)

    # Step 3: generate candidate pairs via co-occurrence counting
    # Keep duplicates on query side so that k matching pairs yield count >= k
    candidates: set[tuple[int, int]] = set()
    for i, buckets in tqdm(enumerate(chunk_buckets), desc="Recherche candidats", total=n):
        if not buckets:
            continue
        neighbor_counts: Counter[int] = Counter()
        for b in buckets:
            for adj in (b - 1, b, b + 1):
                for j in bucket_to_chunks.get(adj, ()):
                    if j > i:
                        neighbor_counts[j] += 1
        for j, count in neighbor_counts.items():
            if count >= min_matching_pairs:
                candidates.add((i, j))

    logger.debug(
        f"Clustering {n} chunks — {len(candidates)} candidate pairs "
        f"(pruned from {n * (n - 1) // 2} exhaustive)"
    )

    # Step 4: full comparison on candidates only
    matches = 0
    for i, j in tqdm(
        candidates, desc="Comparaison fingerprints", total=len(candidates),
    ):
        if are_fingerprints_similar(
            chunks[i].fingerprint,
            chunks[j].fingerprint,
            min_matching_pairs,
            similarity_threshold,
            freq_tol,
            dt_tol,
            offset_tol,
            duration_tol,
            rms_tol,
            centroid_tol,
            zcr_tol,
        ):
            union(i, j)
            matches += 1

    logger.debug(f"  {matches} similar pairs found")

    groups = defaultdict(list)
    for i in range(n):
        groups[find(i)].append(i)

    return dict(groups)


class ChunkGrouping:
    def __init__(
        self,
        similarity_threshold: float = 0.08,  # Min fingerprint score to consider two chunks identical.
        min_matching_pairs: int = 5,  # Min close pairs before considering a match.
        freq_tol: int = 2,  # Frequency bin tolerance for pair matching (~15.6 Hz per bin).
        dt_tol: int = 1,  # Time delta tolerance for pair matching (~64 ms per frame).
        offset_tol: int = 2,  # Temporal coherence tolerance (~128 ms).
        duration_tol: float = 0.3,
        rms_tol: float = 0.05,
        centroid_tol: float = 0.05,
        zcr_tol: float = 0.1,
    ):
        self.similarity_threshold = similarity_threshold
        self.min_matching_pairs = min_matching_pairs
        self.freq_tol = freq_tol
        self.dt_tol = dt_tol
        self.offset_tol = offset_tol
        self.duration_tol = duration_tol
        self.rms_tol = rms_tol
        self.centroid_tol = centroid_tol
        self.zcr_tol = zcr_tol

    def params(self) -> dict:
        return {
            "similarity_threshold": self.similarity_threshold,
            "min_matching_pairs": self.min_matching_pairs,
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

    def run(self, source: List[Chunk]) -> list[ChunkGroup]:
        # Filter out very short chunks
        chunks = [c for c in source if c.fingerprint.duration_sec >= 0.5]
        logger.debug(f"{len(chunks)} chunks to group")

        groups = _cluster(
            chunks,
            self.min_matching_pairs,
            self.similarity_threshold,
            self.freq_tol,
            self.dt_tol,
            self.offset_tol,
            self.duration_tol,
            self.rms_tol,
            self.centroid_tol,
            self.zcr_tol,
        )

        report_groups: list[ChunkGroup] = []
        for member_idxs in groups.values():
            members = sorted(
                [chunks[i] for i in member_idxs], key=lambda c: c.start_sec
            )
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
        for pair in (chunk.fingerprint.pairs or []):
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
            if (abs(int(ref[0]) - int(other[0])) <= freq_tol
                    and abs(int(ref[1]) - int(other[1])) <= freq_tol
                    and abs(int(ref[2]) - int(other[2])) <= dt_tol):
                visited[jdx] = True
                cluster_members.append(jdx)
                cluster_occurrences.add(int(other[4]))

        if len(cluster_occurrences) >= min_freq:
            members = all_tuples_arr[cluster_members]
            canonical_pairs.append((
                int(np.median(members[:, 0])),
                int(np.median(members[:, 1])),
                int(np.median(members[:, 2])),
                int(np.median(members[:, 3])),
            ))

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


def debug_pair(a: Chunk, b: Chunk, grouping: "ChunkGrouping") -> None:
    """
    Print a step-by-step explanation of why two chunks are or are not grouped.

    Usage:
        debug_pair(chunks[i], chunks[j], chunk_grouping)
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
    print(f"    A duration: {a.fingerprint.duration_sec:.3f}s  {PASS if a_ok else FAIL}")
    print(f"    B duration: {b.fingerprint.duration_sec:.3f}s  {PASS if b_ok else FAIL}")
    if not (a_ok and b_ok):
        print(
            "  → BLOCKED: one or both chunks are too short and would be filtered out."
        )
        return

    # ── Step 2: acoustic pre-filter (_features_compatible) ──────────────
    print("\n[2] Acoustic pre-filter (_features_compatible)")

    fp_a, fp_b = a.fingerprint, b.fingerprint

    dur_diff = abs(fp_a.duration_sec - fp_b.duration_sec)
    dur_ok = dur_diff <= grouping.duration_tol
    print(
        f"    duration |A-B| = {dur_diff:.3f}s  (tol={grouping.duration_tol})  {PASS if dur_ok else FAIL}"
    )

    rms_ok = True
    if fp_a.energy_mean > 0 and fp_b.energy_mean > 0:
        rms_diff = rel_diff(fp_a.energy_mean, fp_b.energy_mean)
        rms_ok = rms_diff <= grouping.rms_tol
        print(
            f"    energy_mean rel_diff = {rms_diff:.4f}  (tol={grouping.rms_tol})  {PASS if rms_ok else FAIL}"
            f"  (A={fp_a.energy_mean:.4f}, B={fp_b.energy_mean:.4f})"
        )
    else:
        print("    energy_mean: skipped (one value is 0)")

    centroid_ok = True
    if fp_a.spectral_centroid > 0 and fp_b.spectral_centroid > 0:
        centroid_diff = rel_diff(fp_a.spectral_centroid, fp_b.spectral_centroid)
        centroid_ok = centroid_diff <= grouping.centroid_tol
        print(
            f"    spectral_centroid rel_diff = {centroid_diff:.4f}  (tol={grouping.centroid_tol})  {PASS if centroid_ok else FAIL}"
            f"  (A={fp_a.spectral_centroid:.1f}, B={fp_b.spectral_centroid:.1f})"
        )
    else:
        print("    spectral_centroid: skipped (one value is 0)")

    zcr_ok = True
    if fp_a.zcr_mean > 0 and fp_b.zcr_mean > 0:
        zcr_diff = rel_diff(fp_a.zcr_mean, fp_b.zcr_mean)
        zcr_ok = zcr_diff <= grouping.zcr_tol
        print(
            f"    zcr_mean rel_diff = {zcr_diff:.4f}  (tol={grouping.zcr_tol})  {PASS if zcr_ok else FAIL}"
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
        (np.abs(arr_a[:, None, 0] - arr_b[None, :, 0]) <= grouping.freq_tol)
        & (np.abs(arr_a[:, None, 1] - arr_b[None, :, 1]) <= grouping.freq_tol)
        & (np.abs(arr_a[:, None, 2] - arr_b[None, :, 2]) <= grouping.dt_tol)
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
    match_ok = n_matched >= grouping.min_matching_pairs
    print(
        f"    close matches: {n_matched}  (min={grouping.min_matching_pairs})  {PASS if match_ok else FAIL}"
    )

    if n_matched > 0:
        print(f"    Sample matches (first 5):")
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
        while sorted_offsets[right] - sorted_offsets[left] > 2 * grouping.offset_tol:
            left += 1
        count = right - left + 1
        if count > best_count:
            best_count = count
            best_offset = int(sorted_offsets[(left + right) // 2])

    min_pairs = min(len(arr_a), len(arr_b)) + 1
    score = best_count / min_pairs
    print(f"    best offset cluster: center={best_offset}, coherent={best_count}/{n_matched}")
    print(f"    score = {best_count} / {min_pairs} = {score:.4f}")
    print(f"    similarity_threshold = {grouping.similarity_threshold}")
    score_ok = score >= grouping.similarity_threshold
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
                "duration_sec": 31.39337868480726,
                "energy_mean": 0.039986010640859604,
                "spectral_centroid": 3366.8334208995534,
                "zcr_mean": 0.16432030018715524,
                "peaks": [
                    [1288, 35],
                    [822, 5],
                    [553, 6],
                    [1038, 2],
                    [1132, 5],
                    [734, 8],
                    [987, 42],
                    [404, 47],
                    [132, 54],
                    [1011, 33],
                    [364, 50],
                    [647, 8],
                    [534, 6],
                    [572, 7],
                    [475, 7],
                    [584, 7],
                    [960, 26],
                    [690, 4],
                    [932, 55],
                    [924, 56],
                    [673, 8],
                    [609, 8],
                    [309, 102],
                    [753, 8],
                    [1101, 28],
                    [462, 6],
                    [309, 51],
                    [1180, 50],
                    [881, 22],
                    [1197, 6],
                ],
                "pairs": [
                    [102, 51, 55, 309],
                    [51, 50, 55, 309],
                    [50, 47, 95, 309],
                    [47, 7, 166, 309],
                    [50, 6, 153, 364],
                    [6, 47, 40, 404],
                    [47, 7, 71, 404],
                    [6, 7, 58, 462],
                    [7, 6, 13, 462],
                    [7, 7, 59, 475],
                    [6, 7, 19, 534],
                    [7, 7, 38, 534],
                    [7, 8, 37, 572],
                    [8, 8, 38, 609],
                    [8, 8, 24, 647],
                    [8, 4, 43, 647],
                    [8, 8, 17, 673],
                    [4, 8, 63, 690],
                    [8, 8, 69, 734],
                    [8, 26, 226, 753],
                    [5, 42, 165, 822],
                    [22, 26, 79, 881],
                    [56, 55, 8, 924],
                    [55, 26, 28, 932],
                    [26, 42, 27, 960],
                    [42, 33, 24, 987],
                    [33, 2, 27, 1011],
                    [2, 28, 63, 1038],
                    [28, 5, 31, 1101],
                    [5, 50, 48, 1132],
                ],
            }
        ),
        Chunk.from_dict(
            {
                "start_sec": 1746874679.1687982,
                "end_sec": 1746874691.9862132,
                "channel": "tf1",
                "duration_sec": 12.817414965986472,
                "energy_mean": 0.03261508792638779,
                "spectral_centroid": 2682.5048359943517,
                "zcr_mean": 0.14810633429672446,
                "peaks": [
                    [160, 20],
                    [416, 20],
                    [251, 32],
                    [389, 18],
                    [156, 36],
                    [201, 20],
                    [201, 40],
                    [397, 19],
                    [290, 19],
                    [227, 24],
                    [211, 27],
                    [265, 21],
                    [243, 24],
                    [432, 69],
                    [387, 37],
                    [504, 6],
                    [436, 20],
                    [267, 40],
                    [175, 6],
                    [206, 64],
                    [450, 16],
                    [227, 48],
                    [260, 81],
                    [293, 34],
                    [405, 42],
                    [302, 30],
                    [268, 59],
                    [243, 47],
                    [303, 15],
                    [279, 36],
                ],
                "pairs": [
                    [36, 20, 4, 156],
                    [20, 6, 19, 160],
                    [6, 20, 26, 175],
                    [20, 40, 41, 201],
                    [40, 64, 5, 201],
                    [64, 24, 21, 206],
                    [24, 27, 16, 211],
                    [27, 24, 16, 211],
                    [24, 48, 16, 227],
                    [48, 24, 16, 227],
                    [24, 47, 16, 243],
                    [47, 21, 8, 243],
                    [32, 81, 9, 251],
                    [81, 21, 5, 260],
                    [21, 40, 2, 265],
                    [40, 59, 1, 267],
                    [59, 36, 11, 268],
                    [36, 19, 11, 279],
                    [19, 34, 3, 290],
                    [34, 30, 9, 293],
                    [30, 15, 1, 302],
                    [15, 37, 84, 303],
                    [37, 42, 18, 387],
                    [18, 19, 8, 389],
                    [19, 42, 8, 397],
                    [42, 20, 11, 405],
                    [20, 20, 20, 416],
                    [20, 69, 16, 432],
                    [69, 20, 4, 432],
                    [20, 16, 14, 436],
                ],
            }
        ),
        ChunkGrouping(
            similarity_threshold=0.05,
            min_matching_pairs=5,
            duration_tol=0.4,
            rms_tol=0.05,
            centroid_tol=0.05,
            zcr_tol=0.1,
        ),
    )
