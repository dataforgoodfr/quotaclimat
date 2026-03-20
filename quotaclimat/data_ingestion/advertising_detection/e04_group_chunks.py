"""
Regroupement de chunks audio par fingerprints pré-calculés
=============================================================
Utilise les hashes pré-calculés par e02 (constellation maps style Shazam)
pour comparer et regrouper les chunks identiques.
"""

import hashlib
import itertools
import json
import logging
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from typing import Dict, List, Tuple

import numpy as np
from tqdm import tqdm

from .e02_create_chunks import Chunk

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
            occurrences=[Chunk(**occ) for occ in data["occurrences"]],
        )


def _build_hash_sets(chunks: list[Chunk]) -> list[set[str]]:
    """Precompute the set of hash keys for each chunk (for O(1) lookup)."""
    return [{h for h, _ in (c.hashes or [])} for c in chunks]


def _score(
    chunk_a: Chunk, chunk_b: Chunk, set_a: set, set_b: set, min_matching: int
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
    a: Chunk,
    b: Chunk,
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


def _cluster(
    chunks: list[Chunk],
    hash_sets: list[set[str]],
    min_matching_hashes: int,
    similarity_threshold: float,
) -> Dict[int, List[int]]:
    """
    Group similar chunks via inverted index + Union-Find.
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

    # Inverted index: hash → chunk indices
    logger.debug(f"Clustering {n} chunks — building inverted index...")
    hash_index: Dict[str, List[int]] = defaultdict(list)
    for i, hs in enumerate(hash_sets):
        for h in hs:
            hash_index[h].append(i)

    # Count shared hashes per pair
    shared_counts: Dict[Tuple[int, int], int] = defaultdict(int)
    for bucket in hash_index.values():
        if len(bucket) < 2:
            continue
        for a, b in itertools.combinations(bucket, 2):
            key = (a, b) if a < b else (b, a)
            shared_counts[key] += 1

    # Filter: enough shared hashes + acoustic compatibility
    candidates = [
        (i, j)
        for (i, j), count in shared_counts.items()
        if count >= min_matching_hashes and _features_compatible(chunks[i], chunks[j])
    ]
    logger.debug(f"  {len(candidates)} candidate pairs to score")

    matches = 0
    for i, j in tqdm(candidates, desc="Comparaison fingerprints"):
        if (
            _score(
                chunks[i], chunks[j], hash_sets[i], hash_sets[j], min_matching_hashes
            )
            >= similarity_threshold
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
        #   Score = temporally coherent hashes / min(hashes_a, hashes_b).
        #   Lower = more matches (more false positives). Higher = stricter.
        min_matching_hashes: int = 2,  # Min shared hashes before scoring a pair.
        #   Acts as a fast pre-filter via the inverted index.
        #   1 = very permissive (more candidates to score). 3+ = stricter.
        # Legacy params kept for params_hash cache invalidation
        sr: int = 22050,
        n_peaks_by_chunk: int = 5,
        neighborhood_peaks_filter: int = 15,
        min_peak_amplitude: float = 0.01,
    ):
        self.similarity_threshold = similarity_threshold
        self.min_matching_hashes = min_matching_hashes
        self.sr = sr
        self.n_peaks_by_chunk = n_peaks_by_chunk
        self.neighborhood_peaks_filter = neighborhood_peaks_filter
        self.min_peak_amplitude = min_peak_amplitude

    def params(self) -> dict:
        return {
            "similarity_threshold": self.similarity_threshold,
            "min_matching_hashes": self.min_matching_hashes,
            "sr": self.sr,
            "n_peaks_by_chunk": self.n_peaks_by_chunk,
            "neighborhood_peaks_filter": self.neighborhood_peaks_filter,
            "min_peak_amplitude": self.min_peak_amplitude,
        }

    def params_hash(self) -> str:
        serialized = json.dumps(self.params(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]

    def run(self, source: List[Chunk]) -> list[ChunkGroup]:
        # Filter out very short chunks
        chunks = [c for c in source if c.duration_sec >= 0.5]
        logger.debug(f"{len(chunks)} chunks to group")

        hash_sets = _build_hash_sets(chunks)
        groups = _cluster(
            chunks, hash_sets, self.min_matching_hashes, self.similarity_threshold
        )
        channel = source[0].channel

        report_groups: list[ChunkGroup] = []
        for member_idxs in groups.values():
            members = sorted(
                [chunks[i] for i in member_idxs], key=lambda c: c.start_sec
            )
            durations = [c.duration_sec for c in members]

            report_groups.append(
                ChunkGroup(
                    count=len(members),
                    duration_mean=round(float(np.mean(durations)), 2),
                    duration_std=round(float(np.std(durations)), 2),
                    occurrences=[
                        Chunk(
                            start_sec=round(c.start_sec, 2),
                            end_sec=round(c.end_sec, 2),
                            channel=channel,
                            duration_sec=round(c.duration_sec, 2),
                            energy_mean=round(c.energy_mean, 2),
                            spectral_centroid=round(c.spectral_centroid, 2),
                            zcr_mean=round(c.zcr_mean, 2),
                        )
                        for c in members
                    ],
                )
            )

        report_groups.sort(key=lambda g: -g.count)
        return report_groups

    def generate_report(self, groups: list[ChunkGroup]) -> str:
        """Generate a multiline text report summarizing grouped chunks."""
        if not groups:
            return "No groups to report."

        total_chunks = sum(g.count for g in groups)
        singletons = sum(1 for g in groups if g.count == 1)
        repeated = [g for g in groups if g.count > 1]
        repeated_chunks = sum(g.count for g in repeated)

        lines = [
            "=" * 60,
            "  CHUNK GROUPING REPORT",
            "=" * 60,
            f"  Total chunks     : {total_chunks}",
            f"  Total groups     : {len(groups)}",
            f"  Singletons       : {singletons} (unique chunks)",
            f"  Repeated groups  : {len(repeated)} ({repeated_chunks} chunks total)",
            "",
            "  Parameters:",
        ]
        for key, value in self.params().items():
            lines.append(f"    {key:<28} = {value}")

        if repeated:
            lines += [
                "",
                "  TOP 15 MOST REPEATED GROUPS:",
                f"  {'Group':<8} {'Count':>6}  {'Duration':>10}  {'Std':>6}",
                "  " + "-" * 40,
            ]
            for i, g in enumerate(repeated[:15]):
                lines.append(
                    f"  G{i:<7} {g.count:>6}  {g.duration_mean:>8.1f}s  {g.duration_std:>5.2f}s"
                )
                for occ in g.occurrences[:3]:
                    lines.append(
                        f"    -> {occ.start_sec:.1f}s — {occ.end_sec:.1f}s  ({occ.duration_sec:.1f}s)"
                    )
                if g.count > 3:
                    lines.append(f"    -> ... +{g.count - 3} more occurrences")

        lines.append("=" * 60)
        return "\n".join(lines)
