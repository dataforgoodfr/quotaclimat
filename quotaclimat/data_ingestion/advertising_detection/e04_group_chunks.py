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
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Set, Tuple

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


@dataclass
class Fingerprint:
    chunk_index: int
    start_sec: float
    end_sec: float
    duration_sec: float
    hashes: List[Tuple[str, int]] = field(default_factory=list)
    hash_set: Set[str] = field(default_factory=set)
    rms: float = 0.0
    spectral_centroid: float = 0.0
    zcr: float = 0.0

    def build_hash_set(self):
        self.hash_set = {h for h, _ in self.hashes}


class FingerprintMatcher:
    """
    Compare deux fingerprints via cohérence temporelle des hashes :
    les offsets temporels des hashes communs doivent être alignés.
    """

    def __init__(self, min_matching_hashes: int = 2):
        self.min_matching_hashes = min_matching_hashes

    def score(self, fp_a: Fingerprint, fp_b: Fingerprint) -> float:
        common_hashes = fp_a.hash_set & fp_b.hash_set
        if len(common_hashes) < self.min_matching_hashes:
            return 0.0

        index_a = {h: t for h, t in fp_a.hashes if h in common_hashes}
        index_b = {h: t for h, t in fp_b.hashes if h in common_hashes}

        offsets = [
            index_a[h] - index_b[h]
            for h in common_hashes
            if h in index_a and h in index_b
        ]
        if not offsets:
            return 0.0

        coherent_count = Counter(offsets).most_common(1)[0][1]
        min_hashes = min(len(fp_a.hashes), len(fp_b.hashes)) + 1
        return coherent_count / min_hashes


class RepetitionClusterer:
    """
    Groupe les chunks similaires via un index inversé (hash → chunks)
    et un Union-Find. Pré-filtre acoustique sur durée/RMS/centroid/ZCR.
    """

    def __init__(
        self,
        similarity_threshold: float = 0.08,
        duration_tolerance_sec: float = 0.3,
        rms_tolerance: float = 0.05,
        centroid_tolerance: float = 0.05,
        zcr_tolerance: float = 0.1,
    ):
        self.threshold = similarity_threshold
        self.duration_tolerance_sec = duration_tolerance_sec
        self.rms_tolerance = rms_tolerance
        self.centroid_tolerance = centroid_tolerance
        self.zcr_tolerance = zcr_tolerance

    def _features_compatible(self, fp_a: Fingerprint, fp_b: Fingerprint) -> bool:
        if abs(fp_a.duration_sec - fp_b.duration_sec) > self.duration_tolerance_sec:
            return False

        def rel_diff(a: float, b: float) -> float:
            return abs(a - b) / max(abs(a), abs(b), 1e-8)

        if fp_a.rms > 0 and fp_b.rms > 0:
            if rel_diff(fp_a.rms, fp_b.rms) > self.rms_tolerance:
                return False
        if fp_a.spectral_centroid > 0 and fp_b.spectral_centroid > 0:
            if rel_diff(fp_a.spectral_centroid, fp_b.spectral_centroid) > self.centroid_tolerance:
                return False
        if fp_a.zcr > 0 and fp_b.zcr > 0:
            if rel_diff(fp_a.zcr, fp_b.zcr) > self.zcr_tolerance:
                return False

        return True

    def cluster(
        self,
        fingerprints: List[Fingerprint],
        matcher: FingerprintMatcher,
    ) -> Dict[int, List[int]]:
        """Retourne {groupe_id: [indices de fingerprints]}."""
        n = len(fingerprints)
        parent = list(range(n))

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x, y):
            parent[find(x)] = find(y)

        # Index inversé : hash → indices de chunks
        logger.debug(f"Clustering {n} chunks — building inverted index...")
        hash_index: Dict[str, List[int]] = defaultdict(list)
        for i, fp in enumerate(fingerprints):
            for h in fp.hash_set:
                hash_index[h].append(i)

        # Compter les hashes partagés par paire
        shared_counts: Dict[Tuple[int, int], int] = defaultdict(int)
        for bucket in hash_index.values():
            if len(bucket) < 2:
                continue
            for a, b in itertools.combinations(bucket, 2):
                key = (a, b) if a < b else (b, a)
                shared_counts[key] += 1

        # Ne scorer que les paires au-dessus du seuil + pré-filtre acoustique
        candidates = [
            (i, j)
            for (i, j), count in shared_counts.items()
            if count >= matcher.min_matching_hashes
            and self._features_compatible(fingerprints[i], fingerprints[j])
        ]
        logger.debug(f"  {len(candidates)} candidate pairs to score")

        matches = 0
        for i, j in tqdm(candidates, desc="Comparaison fingerprints"):
            if matcher.score(fingerprints[i], fingerprints[j]) >= self.threshold:
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
        similarity_threshold: float = 0.08,
        min_matching_hashes: int = 2,
        # Legacy params kept for params_hash cache invalidation
        sr: int = 22050,
        n_peaks_by_chunk: int = 5,
        neighborhood_peaks_filter: int = 15,
        min_peak_amplitude: float = 0.01,
    ):
        self.sr = sr
        self.n_peaks_by_chunk = n_peaks_by_chunk
        self.neighborhood_peaks_filter = neighborhood_peaks_filter
        self.min_peak_amplitude = min_peak_amplitude
        self.matcher = FingerprintMatcher(min_matching_hashes=min_matching_hashes)
        self.clusterer = RepetitionClusterer(similarity_threshold)

    def params(self) -> dict:
        return {
            "similarity_threshold": self.clusterer.threshold,
            "min_matching_hashes": self.matcher.min_matching_hashes,
            "sr": self.sr,
            "n_peaks_by_chunk": self.n_peaks_by_chunk,
            "neighborhood_peaks_filter": self.neighborhood_peaks_filter,
            "min_peak_amplitude": self.min_peak_amplitude,
        }

    def params_hash(self) -> str:
        serialized = json.dumps(self.params(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]

    def _build_fingerprints(self, chunks: List[Chunk]) -> List[Fingerprint]:
        fingerprints = []
        for i, chunk in enumerate(chunks):
            if chunk.duration_sec < 0.5:
                continue

            fp = Fingerprint(
                chunk_index=i,
                start_sec=chunk.start_sec,
                end_sec=chunk.end_sec,
                duration_sec=chunk.duration_sec,
                hashes=chunk.hashes or [],
                rms=chunk.energy_mean,
                spectral_centroid=chunk.spectral_centroid,
                zcr=chunk.zcr_mean,
            )
            fp.build_hash_set()
            fingerprints.append(fp)

        return fingerprints

    def run(self, source: List[Chunk]) -> list[ChunkGroup]:
        logger.debug(f"{len(source)} chunks to group")

        fingerprints = self._build_fingerprints(source)
        groups = self.clusterer.cluster(fingerprints, self.matcher)
        channel = source[0].channel

        report_groups: list[ChunkGroup] = []
        for member_idxs in groups.values():
            members = sorted(
                [fingerprints[i] for i in member_idxs], key=lambda x: x.start_sec
            )
            durations = [m.duration_sec for m in members]

            report_groups.append(
                ChunkGroup(
                    count=len(members),
                    duration_mean=round(float(np.mean(durations)), 2),
                    duration_std=round(float(np.std(durations)), 2),
                    occurrences=[
                        Chunk(
                            start_sec=round(m.start_sec, 2),
                            end_sec=round(m.end_sec, 2),
                            channel=channel,
                            duration_sec=round(m.duration_sec, 2),
                            energy_mean=round(m.rms, 2),
                            spectral_centroid=round(m.spectral_centroid, 2),
                            zcr_mean=round(m.zcr, 2),
                        )
                        for m in members
                    ],
                )
            )

        report_groups.sort(key=lambda x: -x.count)
        return report_groups
