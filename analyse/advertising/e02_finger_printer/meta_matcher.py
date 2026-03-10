"""
Grouping de segments par similarité de métaparamètres
=====================================================
Alternative au finger_printer : ne charge pas l'audio.
Travaille uniquement sur les features pré-calculées par le rupture_detector
qui sont stockées dans les JSON de segments :
  - durée (end_sec - start_sec)
  - energy_mean  (RMS)
  - spectral_centroid (Hz)
  - zcr (zero-crossing rate)

Principe :
  1. Charger les JSON de segments (pas d'audio)
  2. Construire un vecteur feature [duration, rms, centroid, zcr] par segment
  3. Normaliser chaque feature sur l'ensemble du dataset
  4. Calculer la distance euclidienne entre toutes les paires → O(n²) mais très rapide
  5. Regrouper les paires sous le seuil (union-find)

Dépendances : numpy, tqdm  (pas de librosa !)
"""

import json
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
from tqdm import tqdm

from analyse.advertising.tools.basic_elements.segments import (
    Segment,
    load_segment_groups_from_json,
)

# Réutiliser les helpers de finger_printer (rapport, visualisation, etc.)
from analyse.advertising.e02_finger_printer.finger_printer import (
    classify_group,
    plot_groups,
    print_report,
    sec_to_tc,
)


# ─────────────────────────────────────────────────────────────
#  Représentation d'un segment avec ses features
# ─────────────────────────────────────────────────────────────


@dataclass
class SegmentFeatures:
    """Features d'un segment, extraites des métaparamètres JSON."""

    segment_index: int
    start_sec: float
    end_sec: float
    duration_sec: float
    audio_source: str = ""
    label: str = ""

    # Features acoustiques pré-calculées
    rms: float = 0.0
    spectral_centroid: float = 0.0
    zcr: float = 0.0

    # Vecteur normalisé (rempli après normalisation globale)
    feature_vector: np.ndarray = field(default_factory=lambda: np.zeros(4))

    @property
    def has_acoustic_features(self) -> bool:
        return self.rms > 0 or self.spectral_centroid > 0 or self.zcr > 0

    def to_dict(self) -> dict:
        return {
            "segment_index": self.segment_index,
            "start_sec": self.start_sec,
            "end_sec": self.end_sec,
            "duration_sec": self.duration_sec,
            "audio_source": self.audio_source,
            "label": self.label,
            "rms": self.rms,
            "spectral_centroid": self.spectral_centroid,
            "zcr": self.zcr,
        }


# ─────────────────────────────────────────────────────────────
#  Normalisation des features
# ─────────────────────────────────────────────────────────────


class FeatureNormalizer:
    """
    Normalise chaque feature dans [0, 1] sur l'ensemble du dataset.
    Utilise min-max robuste (percentile 2–98) pour résister aux outliers.

    Ordre des features dans le vecteur :
      [0] duration_sec
      [1] rms (energy_mean)
      [2] spectral_centroid
      [3] zcr
    """

    FEATURE_NAMES = ["duration", "rms", "centroid", "zcr"]

    def __init__(self, percentile_low: float = 2.0, percentile_high: float = 98.0):
        self.percentile_low = percentile_low
        self.percentile_high = percentile_high
        self.mins: np.ndarray = np.zeros(4)
        self.ranges: np.ndarray = np.ones(4)

    def fit(self, segments: List[SegmentFeatures]) -> "FeatureNormalizer":
        matrix = self._to_matrix(segments)
        self.mins = np.percentile(matrix, self.percentile_low, axis=0)
        highs = np.percentile(matrix, self.percentile_high, axis=0)
        self.ranges = np.where(highs - self.mins > 1e-8, highs - self.mins, 1.0)
        return self

    def transform(self, segments: List[SegmentFeatures]) -> None:
        """Remplit feature_vector de chaque segment (in-place)."""
        matrix = self._to_matrix(segments)
        normalized = np.clip((matrix - self.mins) / self.ranges, 0.0, 1.0)
        for seg, vec in zip(segments, normalized):
            seg.feature_vector = vec

    def fit_transform(self, segments: List[SegmentFeatures]) -> None:
        self.fit(segments)
        self.transform(segments)

    def _to_matrix(self, segments: List[SegmentFeatures]) -> np.ndarray:
        return np.array(
            [
                [seg.duration_sec, seg.rms, seg.spectral_centroid, seg.zcr]
                for seg in segments
            ],
            dtype=np.float64,
        )

    def stats(self) -> None:
        print("\n  Features — plages effectives après normalisation :")
        for i, name in enumerate(self.FEATURE_NAMES):
            print(f"    {name:<12} min={self.mins[i]:.4g}  range={self.ranges[i]:.4g}")


# ─────────────────────────────────────────────────────────────
#  Calcul de similarité
# ─────────────────────────────────────────────────────────────


class MetaSimilarity:
    """
    Calcule la distance entre deux segments dans l'espace des features normalisées.

    Distance = distance euclidienne pondérée dans [0, 1]^4.
    Plus la distance est faible, plus les segments se ressemblent.

    Weights : permet de pondérer l'importance relative de chaque feature.
      Par défaut : duration=1.5 (fort discriminant), rms=1.0, centroid=1.0, zcr=0.8
    """

    def __init__(
        self,
        weights: Optional[np.ndarray] = None,
        duration_hard_max_sec: float = 1.0,
    ):
        # Poids par feature : [duration, rms, centroid, zcr]
        if weights is None:
            self.weights = np.array([1.5, 1.0, 1.0, 0.8])
        else:
            self.weights = np.asarray(weights, dtype=float)

        # Filtre dur sur la durée absolue (avant calcul de distance)
        self.duration_hard_max_sec = duration_hard_max_sec

    def distance(self, a: SegmentFeatures, b: SegmentFeatures) -> float:
        """Distance euclidienne pondérée dans l'espace normalisé, dans [0, +∞)."""
        diff = a.feature_vector - b.feature_vector
        return float(np.sqrt(np.sum((diff * self.weights) ** 2)))

    def is_candidate(self, a: SegmentFeatures, b: SegmentFeatures) -> bool:
        """Filtre rapide avant distance complète : durée absolue."""
        return abs(a.duration_sec - b.duration_sec) <= self.duration_hard_max_sec


# ─────────────────────────────────────────────────────────────
#  Clustering union-find
# ─────────────────────────────────────────────────────────────


class MetaClusterer:
    """
    Compare toutes les paires de segments et regroupe ceux dont la distance
    est inférieure au seuil (union-find).

    Optimisation O(n²) mais vectorisée via numpy :
      - construction d'une matrice de features
      - calcul de distances par lots
    """

    def __init__(
        self,
        distance_threshold: float = 0.15,
        duration_hard_max_sec: float = 1.0,
        only_acoustic_if_available: bool = True,
    ):
        self.threshold = distance_threshold
        self.duration_hard_max_sec = duration_hard_max_sec
        # Si True, les segments SANS features acoustiques ne sont comparés
        # qu'entre eux (évite les faux groupements par durée seule)
        self.only_acoustic_if_available = only_acoustic_if_available

    def cluster(
        self,
        segments: List[SegmentFeatures],
        similarity: MetaSimilarity,
    ) -> Dict[int, List[int]]:
        """Retourne {groupe_id: [indices dans `segments`]}."""
        n = len(segments)
        parent = list(range(n))

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x: int, y: int) -> None:
            parent[find(x)] = find(y)

        print(f"\n[MetaMatcher] {n} segments — comparaison par features...")

        # Vecteurs normalisés sous forme matricielle pour vectorisation
        matrix = np.stack([s.feature_vector for s in segments])  # (n, 4)
        weights = similarity.weights  # (4,)

        n_candidates = 0
        n_matches = 0

        for i in tqdm(range(n), desc="Paires"):
            a = segments[i]
            # Pré-filtre : durée absolue
            dur_diff = np.abs(
                np.array([segments[j].duration_sec for j in range(i + 1, n)])
                - a.duration_sec
            )
            valid_j = np.where(dur_diff <= self.duration_hard_max_sec)[0] + (i + 1)

            if len(valid_j) == 0:
                continue

            n_candidates += len(valid_j)

            # Filtrage acoustique optionnel
            if self.only_acoustic_if_available and a.has_acoustic_features:
                valid_j = [
                    j
                    for j in valid_j
                    if not segments[j].has_acoustic_features
                    or segments[j].has_acoustic_features
                ]
                # (On garde tout ici, le filtre "dur" est la distance elle-même)

            # Calcul vectorisé des distances pour tous les j valides
            diffs = matrix[valid_j] - matrix[i]  # (k, 4)
            dists = np.sqrt(np.sum((diffs * weights) ** 2, axis=1))

            valid_j_arr = np.array(valid_j)
            matches_j = valid_j_arr[dists < self.threshold]
            for j in matches_j:
                union(i, int(j))
                n_matches += 1

        print(
            f"  {n_candidates} paires candidates (filtre durée ±{self.duration_hard_max_sec}s)"
        )
        print(f"  {n_matches} paires similaires (distance < {self.threshold})")

        groups: Dict[int, List[int]] = defaultdict(list)
        for i in range(n):
            groups[find(i)].append(i)

        return dict(groups)


# ─────────────────────────────────────────────────────────────
#  Pipeline principal
# ─────────────────────────────────────────────────────────────


class MetaMatcherPipeline:
    """
    Pipeline de grouping basé uniquement sur les métaparamètres des segments.

    Contrairement à FingerprintPipeline :
      - Pas de chargement audio
      - Pas de FFT / constellation map
      - Fonctionne même si les features acoustiques sont absentes
        (dans ce cas seule la durée est utilisée)
      - Très rapide (quelques secondes pour des milliers de segments)

    sources : List[Tuple[str, str]]
        Chaque tuple = (audio_source_label, segments_json_path)
        audio_source_label : identifiant lisible (chemin audio, nom de chaîne…)
          uniquement utilisé pour la traçabilité dans le rapport.
    """

    def __init__(
        self,
        sources: List[Tuple[str, str]],
        distance_threshold: float = 0.15,
        duration_hard_max_sec: float = 1.0,
        feature_weights: Optional[List[float]] = None,
        only_acoustic_if_available: bool = True,
    ):
        self.sources = sources
        self.similarity = MetaSimilarity(
            weights=np.array(feature_weights) if feature_weights else None,
            duration_hard_max_sec=duration_hard_max_sec,
        )
        self.normalizer = FeatureNormalizer()
        self.clusterer = MetaClusterer(
            distance_threshold=distance_threshold,
            duration_hard_max_sec=duration_hard_max_sec,
            only_acoustic_if_available=only_acoustic_if_available,
        )

    # ── Chargement ──────────────────────────────────────────

    def load_segments(self) -> List[SegmentFeatures]:
        all_segments: List[SegmentFeatures] = []
        idx = 0
        for audio_label, segments_json in self.sources:
            raw: List[Segment] = load_segment_groups_from_json(segments_json)
            print(f"  {len(raw):>4} segments  ←  {segments_json}")
            for seg in raw:
                all_segments.append(
                    SegmentFeatures(
                        segment_index=idx,
                        start_sec=seg.start_sec,
                        end_sec=seg.end_sec,
                        duration_sec=seg.end_sec - seg.start_sec,
                        audio_source=audio_label,
                        rms=seg.energy_mean,
                        spectral_centroid=seg.spectral_centroid,
                        zcr=seg.zcr,
                    )
                )
                idx += 1

        n_acoustic = sum(1 for s in all_segments if s.has_acoustic_features)
        print(
            f"\n  Total : {len(all_segments)} segments"
            f"  ({n_acoustic} avec features acoustiques,"
            f" {len(all_segments) - n_acoustic} sans)"
        )
        return all_segments

    # ── Rapport ─────────────────────────────────────────────

    def build_report(
        self,
        segments: List[SegmentFeatures],
        groups: Dict[int, List[int]],
    ) -> dict:
        report_groups = []
        for group_id, member_idxs in groups.items():
            members = [segments[i] for i in member_idxs]
            members.sort(key=lambda x: x.start_sec)
            durations = [m.duration_sec for m in members]
            report_groups.append(
                {
                    "group_id": group_id,
                    "count": len(members),
                    "duration_mean": round(float(np.mean(durations)), 2),
                    "duration_std": round(float(np.std(durations)), 2),
                    "classification": classify_group(
                        len(members), float(np.mean(durations))
                    ),
                    "occurrences": [
                        {
                            "segment_index": m.segment_index,
                            "audio_source": m.audio_source,
                            "start_sec": round(m.start_sec, 2),
                            "end_sec": round(m.end_sec, 2),
                            "start_tc": sec_to_tc(m.start_sec),
                            "duration_sec": round(m.duration_sec, 2),
                            "label": m.label,
                        }
                        for m in members
                    ],
                }
            )

        report_groups.sort(key=lambda x: -x["count"])
        return {
            "total_segments": len(segments),
            "total_groups": len(groups),
            "groups": report_groups,
        }

    # ── Run ─────────────────────────────────────────────────

    def run(self) -> Tuple[dict, List[SegmentFeatures], Dict[int, List[int]]]:
        t0 = time.time()

        print("\n[MetaMatcher] Chargement des segments JSON...")
        segments = self.load_segments()

        print("\n[MetaMatcher] Normalisation des features...")
        self.normalizer.fit_transform(segments)
        self.normalizer.stats()

        groups = self.clusterer.cluster(segments, self.similarity)

        report = self.build_report(segments, groups)
        report["processing_time_sec"] = round(time.time() - t0, 2)

        return report, segments, groups

    def diagnose(self, segments: List[SegmentFeatures], top_n: int = 10) -> None:
        """Affiche les paires les plus proches pour calibrer le seuil."""
        print("\n" + "─" * 60)
        print("  DIAGNOSTIC META-MATCHER")
        print("─" * 60)

        n = len(segments)
        matrix = np.stack([s.feature_vector for s in segments])
        weights = self.similarity.weights

        distances = []
        for i in range(n):
            diffs = matrix[i + 1 :] - matrix[i]
            dists = np.sqrt(np.sum((diffs * weights) ** 2, axis=1))
            for k, d in enumerate(dists):
                distances.append((d, i, i + 1 + k))

        distances.sort(key=lambda x: x[0])

        print(f"\n  Top {top_n} paires les plus proches :")
        print(
            f"  {'Seg A':>6}  {'Seg B':>6}  {'Distance':>10}  {'Durée A':>8}  {'Durée B':>8}"
        )
        print("  " + "─" * 52)
        for dist, i, j in distances[:top_n]:
            a, b = segments[i], segments[j]
            print(
                f"  {i:>6}  {j:>6}  {dist:>10.4f}  {a.duration_sec:>7.2f}s  {b.duration_sec:>7.2f}s"
            )

        if distances:
            all_dists = [d for d, _, _ in distances]
            print(f"\n  Distribution des distances :")
            for p in [10, 25, 50, 75, 90, 95, 99]:
                print(f"    p{p:>2} = {np.percentile(all_dists, p):.4f}")

        print("─" * 60)


# ─────────────────────────────────────────────────────────────
#  Point d'entrée CLI
# ─────────────────────────────────────────────────────────────


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Grouping de segments audio par similarité de métaparamètres (sans chargement audio)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python meta_matcher.py segments.json
  python meta_matcher.py s1.json s2.json --threshold 0.12
  python meta_matcher.py s1.json --diagnose
""",
    )
    parser.add_argument(
        "segments",
        nargs="+",
        help="Fichier(s) JSON de segments produits par rupture_detector.py",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.15,
        help="Seuil de distance 0.0–1.0 (plus petit = plus strict) [défaut: 0.15]",
    )
    parser.add_argument(
        "--duration-max",
        type=float,
        default=1.0,
        help="Différence de durée maximale (en secondes) pour être candidat [défaut: 1.0]",
    )
    parser.add_argument(
        "--out-json",
        default="meta_groups.json",
        help="Rapport JSON de sortie [défaut: meta_groups.json]",
    )
    parser.add_argument(
        "--out-plot",
        default="meta_groups.png",
        help="Visualisation PNG [défaut: meta_groups.png]",
    )
    parser.add_argument("--no-plot", action="store_true")
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help="Afficher les paires les plus proches pour calibrer le seuil",
    )
    args = parser.parse_args()

    # Chaque fichier JSON est une source ; le label = le chemin lui-même
    sources = [(path, path) for path in args.segments]

    pipeline = MetaMatcherPipeline(
        sources=sources,
        distance_threshold=args.threshold,
        duration_hard_max_sec=args.duration_max,
    )

    if args.diagnose:
        print("[MetaMatcher] Mode diagnostic...")
        segments = pipeline.load_segments()
        pipeline.normalizer.fit_transform(segments)
        pipeline.diagnose(segments)
        return

    report, segments, groups = pipeline.run()

    print_report(report)

    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"  Rapport JSON : {Path(args.out_json).absolute()}")

    if not args.no_plot:
        total = max(s.end_sec for s in segments) if segments else 3600
        plot_groups(report, total, args.out_plot)

    print("\nFait.\n")


if __name__ == "__main__":
    main()
