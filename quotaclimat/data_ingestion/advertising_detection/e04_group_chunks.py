"""
Regroupement de chunks audio par fingerprints pré-calculés
=============================================================
Utilise les hashes pré-calculés par e02 (constellation maps style Shazam)
pour comparer et regrouper les chunks identiques.

Le calcul lourd (extraction de pics spectraux + génération de hashes)
est fait en amont dans e02_create_chunks. Ici on ne fait que
la comparaison, le clustering et le rapport.

Dépendances :
    pip install numpy tqdm
"""

import hashlib
import itertools
import json
import logging
import time
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


# ─────────────────────────────────────────────────────────────
#  Fingerprint d'un chunk
# ─────────────────────────────────────────────────────────────


@dataclass
class Fingerprint:
    chunk_index: int
    start_sec: float
    end_sec: float
    duration_sec: float
    label: str
    hashes: List[Tuple[str, int]] = field(default_factory=list)  # [(hash, t_ancrage)]
    hash_set: Set[str] = field(default_factory=set)  # Pour lookup rapide
    # Features acoustiques pour pré-filtrage
    rms: float = 0.0
    spectral_centroid: float = 0.0
    zcr: float = 0.0

    def build_hash_set(self):
        self.hash_set = {h for h, _ in self.hashes}

    def to_dict(self) -> dict:
        return {
            "chunk_index": self.chunk_index,
            "start_sec": self.start_sec,
            "end_sec": self.end_sec,
            "duration_sec": self.duration_sec,
            "label": self.label,
            "hashes": self.hashes,  # List[Tuple[str, int]] → JSON array of [str, int]
            "rms": self.rms,
            "spectral_centroid": self.spectral_centroid,
            "zcr": self.zcr,
        }

    @staticmethod
    def from_dict(d: dict) -> "Fingerprint":
        fp = Fingerprint(
            chunk_index=d["chunk_index"],
            start_sec=d["start_sec"],
            end_sec=d["end_sec"],
            duration_sec=d["duration_sec"],
            label=d["label"],
            hashes=[(h, t) for h, t in d["hashes"]],
            rms=d.get("rms", 0.0),
            spectral_centroid=d.get("spectral_centroid", 0.0),
            zcr=d.get("zcr", 0.0),
        )
        fp.build_hash_set()
        return fp


# ─────────────────────────────────────────────────────────────
#  Comparaison et scoring entre deux fingerprints
# ─────────────────────────────────────────────────────────────


class FingerprintMatcher:
    """
    Compare deux fingerprints et retourne un score de similarité.

    La vraie robustesse vient de la cohérence temporelle :
    si deux chunks se ressemblent, non seulement leurs hashes
    doivent matcher, mais les offsets temporels correspondants
    doivent être cohérents (alignés sur la même droite t1 - t2 = constante).
    """

    def __init__(self, min_matching_hashes: int = 2):
        self.min_matching_hashes = min_matching_hashes

    def score(self, fp_a: Fingerprint, fp_b: Fingerprint) -> dict:
        # 1. Hashes en commun (lookup O(1) grâce au set)
        common_hashes = fp_a.hash_set & fp_b.hash_set
        n_common = len(common_hashes)

        if n_common < self.min_matching_hashes:
            return {"score": 0.0, "n_common": n_common, "coherent": 0}

        # 2. Cohérence temporelle : vérifier que les offsets sont alignés
        #    Construire un index hash→temps pour chaque fingerprint
        index_a = {h: t for h, t in fp_a.hashes if h in common_hashes}
        index_b = {h: t for h, t in fp_b.hashes if h in common_hashes}

        offsets = []
        for h in common_hashes:
            if h in index_a and h in index_b:
                offsets.append(index_a[h] - index_b[h])

        if not offsets:
            return {"score": 0.0, "n_common": n_common, "coherent": 0}

        # Compter le offset dominant (alignement temporel)
        offset_counts = Counter(offsets)
        best_offset, coherent_count = offset_counts.most_common(1)[0]

        # Score final : hashes cohérents / taille minimale des deux FP
        min_hashes = min(len(fp_a.hashes), len(fp_b.hashes)) + 1
        score = coherent_count / min_hashes

        return {
            "score": round(score, 4),
            "n_common": n_common,
            "coherent": coherent_count,
            "best_offset": best_offset,
        }


# ─────────────────────────────────────────────────────────────
#  Clustering des chunks similaires
# ─────────────────────────────────────────────────────────────


class RepetitionClusterer:
    """
    Groupe les chunks ayant des fingerprints similaires.

    Utilise un index inversé (hash → chunks) pour éviter la comparaison
    exhaustive O(n²) : seules les paires partageant au moins
    `min_shared_hashes` hashes sont évaluées (comme Shazam côté indexation).

    Un pré-filtre acoustique élimine les paires dont la durée, le RMS,
    le centroide spectral ou le ZCR diffèrent trop, afin d'éviter les
    faux positifs issus de coïncidences de hashes.
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

    def _features_compatible(self, fp_a: "Fingerprint", fp_b: "Fingerprint") -> bool:
        """Vérifie la cohérence acoustique de base avant le scoring complet."""
        # Durée ±300ms
        if abs(fp_a.duration_sec - fp_b.duration_sec) > self.duration_tolerance_sec:
            return False

        def rel_diff(a: float, b: float) -> float:
            denom = max(abs(a), abs(b), 1e-8)
            return abs(a - b) / denom

        # Ne filtrer sur RMS/centroid/ZCR que si les features ont été calculées
        if fp_a.rms > 0 and fp_b.rms > 0:
            if rel_diff(fp_a.rms, fp_b.rms) > self.rms_tolerance:
                return False
        if fp_a.spectral_centroid > 0 and fp_b.spectral_centroid > 0:
            if (
                rel_diff(fp_a.spectral_centroid, fp_b.spectral_centroid)
                > self.centroid_tolerance
            ):
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
        """
        Retourne un dict {groupe_id: [indices de chunks]}.
        """
        n = len(fingerprints)
        parent = list(range(n))

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x, y):
            parent[find(x)] = find(y)

        # ── Index inversé : hash → liste d'indices de chunks ──
        print(f"\n[Clustering] {n} chunks — construction de l'index inversé...")
        hash_index: Dict[str, List[int]] = defaultdict(list)
        for i, fp in enumerate(fingerprints):
            for h in fp.hash_set:
                hash_index[h].append(i)

        # ── Compter les hashes partagés par paire (sans énumérer toutes les paires) ──
        # Seuil max : on ignore les hashes présents dans plus de la moitié des chunks
        # (silence, fond musical constant…) mais on garde les hashes de pubs répétées
        # popular_threshold = max(50, n // 2)
        shared_counts: Dict[Tuple[int, int], int] = defaultdict(int)
        for bucket in hash_index.values():
            if len(bucket) < 2:
                continue
            # if len(bucket) > popular_threshold:
            #     continue
            for a, b in itertools.combinations(bucket, 2):
                key = (a, b) if a < b else (b, a)
                shared_counts[key] += 1

        # ── Ne scorer que les paires au-dessus du seuil minimal ──
        candidates = [
            (i, j)
            for (i, j), count in shared_counts.items()
            if count >= matcher.min_matching_hashes
        ]
        print(
            f"  {len(shared_counts)} paires candidates → "
            f"{len(candidates)} retenues (≥ {matcher.min_matching_hashes} hashes communs)"
            f"  [vs {n * (n - 1) // 2} paires en O(n²)]"
        )

        # Pré-filtrage acoustique : durée ±300ms + cohérence RMS/centroid/ZCR
        filtered_candidates = [
            (i, j)
            for i, j in candidates
            if self._features_compatible(fingerprints[i], fingerprints[j])
        ]
        n_prefiltered = len(candidates) - len(filtered_candidates)
        if n_prefiltered:
            print(
                f"  Pré-filtre acoustique : {n_prefiltered} paires éliminées"
                f" → {len(filtered_candidates)} restantes"
            )

        matches = 0
        for i, j in tqdm(filtered_candidates, desc="Comparaison fingerprints"):
            result = matcher.score(fingerprints[i], fingerprints[j])
            if result["score"] >= self.threshold:
                union(i, j)
                matches += 1

        print(f"  {matches} paires similaires trouvées")

        # Construire les groupes
        groups = defaultdict(list)
        for i in range(n):
            groups[find(i)].append(i)

        return dict(groups)


# ─────────────────────────────────────────────────────────────
#  Pipeline principal
# ─────────────────────────────────────────────────────────────


class ChunkGrouping:
    def __init__(
        self,
        similarity_threshold: float = 0.08,
        min_matching_hashes: int = 2,
        # Legacy params kept for _params_hash cache invalidation
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
        """Returns all constructor parameters as a dict."""
        return {
            "similarity_threshold": self.clusterer.threshold,
            "min_matching_hashes": self.matcher.min_matching_hashes,
            "sr": self.sr,
            "n_peaks_by_chunk": self.n_peaks_by_chunk,
            "neighborhood_peaks_filter": self.neighborhood_peaks_filter,
            "min_peak_amplitude": self.min_peak_amplitude,
        }

    def params_hash(self) -> str:
        """Returns a stable SHA256 hash of all constructor parameters.
        Changes when any parameter value changes, identical otherwise."""
        serialized = json.dumps(self.params(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]

    def fingerprint_source(self, chunks: List[Chunk]) -> List[Fingerprint]:
        """Converts chunks (with pre-computed hashes from e02) into Fingerprints."""
        fingerprints = []
        for i, seg in enumerate(chunks):
            duration_seg = seg.end_sec - seg.start_sec
            if duration_seg < 0.5:  # Ignorer < 0.5s
                continue

            # Hashes are pre-computed in e02 (ChunkCreator.build_chunks)
            hashes = seg.hashes if seg.hashes else []

            fp = Fingerprint(
                chunk_index=i,
                start_sec=seg.start_sec,
                end_sec=seg.end_sec,
                duration_sec=seg.end_sec - seg.start_sec,
                label="",
                hashes=hashes,
                rms=seg.energy_mean,
                spectral_centroid=seg.spectral_centroid,
                zcr=seg.zcr_mean,
            )
            fp.build_hash_set()
            fingerprints.append(fp)

        return fingerprints

    def _fingerprints(self, sources) -> List[Fingerprint]:
        fingerprints = []
        for chunks in sources:
            fingerprints.extend(self.fingerprint_source(chunks))
        return fingerprints

    def run(
        self,
        source: List[Chunk],
    ) -> list[ChunkGroup]:
        t0 = time.time()

        logger.debug(f"{len(source)} chunks à regrouper")

        fingerprints = self._fingerprints([source])

        groups = self.clusterer.cluster(fingerprints, self.matcher)

        results = self.build_report(fingerprints, groups, channel=source[0].channel)
        results["processing_time_sec"] = round(time.time() - t0, 2)

        return results["groups"]

    def build_report(
        self, fingerprints: List[Fingerprint], groups: Dict, channel: str
    ) -> dict:
        """Construit le rapport de répétitions."""

        report_groups: list[ChunkGroup] = []
        for group_id, member_idxs in groups.items():
            members = [fingerprints[i] for i in member_idxs]
            members.sort(key=lambda x: x.start_sec)

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

        # Trier par nombre d'occurrences décroissant
        report_groups.sort(key=lambda x: -x.count)

        return {
            "total_chunks": len(fingerprints),
            "total_groups": len(groups),
            "groups": report_groups,
        }


# ─────────────────────────────────────────────────────────────
#  Résumé console
# ─────────────────────────────────────────────────────────────


# def print_report(report: dict):
#     groups = report["groups"]
#     print("\n" + "═" * 70)
#     print("  RAPPORT DE RÉPÉTITIONS")
#     print("═" * 70)
#     print(f"  Chunks analysés : {report['total_chunks']}")
#     print(f"  Groupes détectés  : {report['total_groups']}")

#     # Stats par classification
#     class_counts = Counter(g["classification"] for g in groups)
#     seg_counts = Counter()
#     for g in groups:
#         seg_counts[g["classification"]] += g["count"]

#     print(f"\n  {'Classification':<25} {'Groupes':>8}  {'Occurrences tot':>16}")
#     print("  " + "─" * 52)
#     for cls, gc in sorted(class_counts.items(), key=lambda x: -seg_counts[x[0]]):
#         print(f"  {cls:<25} {gc:>8}  {seg_counts[cls]:>16}")

#     print("\n  TOP 15 GROUPES LES PLUS FRÉQUENTS :")
#     print(f"  {'Groupe':<8} {'Occurrences':>12}  {'Durée moy':>10}  {'Classification'}")
#     print("  " + "─" * 60)
#     for g in groups[:15]:
#         print(
#             f"  G{g['group_id']:<7} {g['count']:>12}  {g['duration_mean']:>8.1f}s  {g['classification']}"
#         )
#         # Afficher les 3 premières occurrences
#         for occ in g["occurrences"][:3]:
#             print(f"    ↳ [{occ['start_tc']}]  {occ['duration_sec']:.1f}s")
#         if g["count"] > 3:
#             print(f"    ↳ ... +{g['count'] - 3} autres occurrences")

#     print("═" * 70)
#     print(f"\n  Temps de traitement : {report.get('processing_time_sec', '?')}s\n")
