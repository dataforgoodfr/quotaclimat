"""
Fingerprinting audio par constellation maps (style Shazam)
===========================================================
Génère une empreinte robuste pour chaque segment fourni,
compare toutes les empreintes entre elles,
et regroupe les segments identiques.

Dépendances :
    pip install librosa numpy scipy matplotlib tqdm
"""

import hashlib
import itertools
import json
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple

import librosa
import numpy as np
from scipy.ndimage import maximum_filter
from tqdm import tqdm

from .e02_create_segments import Segments

# ─────────────────────────────────────────────────────────────
#  Constellation Map : extraction des pics dans le spectrogramme
# ─────────────────────────────────────────────────────────────


class ConstellationMap:
    """
    Transforme un segment audio en un nuage de points saillants
    dans le plan (temps × fréquence).

    Principe :
      1. Calcul du spectrogramme (magnitude)
      2. Détection des maxima locaux (pics d'énergie)
      3. Chaque pic = un point dans la constellation
    """

    def __init__(
        self,
        sr: int = 22050,
        n_fft: int = 2048,  # Taille FFT → résolution fréquence
        hop_length: int = 512,  # Pas → résolution temporelle (~23ms)
        n_peaks: int = 30,  # Nombre ABSOLU de pics à garder (indépendant de la durée)
        neighborhood: int = 15,  # Taille du filtre de maximum local (frames × bins)
        min_amplitude: float = 0.01,  # Seuil minimal d'amplitude
    ):
        self.sr = sr
        self.n_fft = n_fft
        self.hop_length = hop_length
        self.n_peaks = n_peaks
        self.neighborhood = neighborhood
        self.min_amplitude = min_amplitude

    def extract(self, y: np.ndarray) -> np.ndarray:
        """
        Retourne un tableau (N_pics × 2) avec colonnes [temps_frame, bin_freq].
        """
        # Spectrogramme en magnitude
        D = np.abs(librosa.stft(y, n_fft=self.n_fft, hop_length=self.hop_length))

        # Log-compression pour équilibrer grave et aigu
        D_log = librosa.amplitude_to_db(D, ref=np.max)
        D_norm = (D_log - D_log.min()) / (D_log.max() - D_log.min() + 1e-8)

        # Maxima locaux : un pic est retenu seulement s'il est
        # le plus fort dans son voisinage (neighborhood × neighborhood)
        local_max = maximum_filter(D_norm, size=self.neighborhood)
        is_peak = (D_norm == local_max) & (D_norm > self.min_amplitude)

        # Coordonnées des pics : (bin_freq, temps_frame)
        freq_idxs, time_idxs = np.where(is_peak)

        if len(freq_idxs) == 0:
            return np.empty((0, 2), dtype=np.int32)

        # Trier par amplitude décroissante et garder les N meilleurs (cap absolu)
        amplitudes = D_norm[freq_idxs, time_idxs]
        order = np.argsort(-amplitudes)[: self.n_peaks]

        peaks = np.column_stack(
            [
                time_idxs[order].astype(np.int32),
                freq_idxs[order].astype(np.int32),
            ]
        )

        return peaks  # shape : (N, 2)  — N peut varier selon le contenu


# ─────────────────────────────────────────────────────────────
#  Génération des hashes par paires de pics (fan-out)
# ─────────────────────────────────────────────────────────────


class HashGenerator:
    """
    À partir de la constellation, génère des hashes robustes
    en combinant des PAIRES de pics proches.

    Pourquoi des paires ?
      → Un hash = (freq1, freq2, delta_temps)
      → Invariant au décalage temporel absolu
      → Résistant au bruit (un pic isolé peut disparaître,
        une paire cohérente est beaucoup plus stable)
    """

    def __init__(
        self,
        fan_out: int = 15,  # Nombre de voisins à combiner avec chaque pic
        time_delta_max: int = 100,  # Fenêtre temporelle max entre deux pics (frames)
        time_delta_min: int = 1,  # Fenêtre minimale (évite les auto-paires)
    ):
        self.fan_out = fan_out
        self.time_delta_max = time_delta_max
        self.time_delta_min = time_delta_min

    def generate(self, peaks: np.ndarray) -> List[Tuple[str, int]]:
        """
        Retourne une liste de (hash_str, temps_ancrage).

        hash_str   = empreinte d'une paire de pics
        temps_ancrage = position temporelle du pic de référence (en frames)
        """
        if len(peaks) < 2:
            return []

        # Trier par temps croissant
        peaks = peaks[peaks[:, 0].argsort()]

        hashes = []
        for i, (t1, f1) in enumerate(peaks):
            # Chercher les pics suivants dans la fenêtre temporelle
            j = i + 1
            count = 0
            while j < len(peaks) and count < self.fan_out:
                t2, f2 = peaks[j]
                delta_t = t2 - t1

                if delta_t > self.time_delta_max:
                    break
                if delta_t >= self.time_delta_min:
                    # Hash = combinaison des deux fréquences + écart temporel
                    # → identique quelle que soit la position dans le flux
                    h = self._make_hash(f1, f2, delta_t)
                    hashes.append((h, int(t1)))
                    count += 1
                j += 1

        return hashes

    def _make_hash(self, f1: int, f2: int, dt: int) -> str:
        raw = f"{f1}|{f2}|{dt}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]


# ─────────────────────────────────────────────────────────────
#  Fingerprint d'un segment
# ─────────────────────────────────────────────────────────────


@dataclass
class Fingerprint:
    segment_index: int
    start_sec: float
    end_sec: float
    duration_sec: float
    label: str
    audio_source: str = ""
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
            "segment_index": self.segment_index,
            "start_sec": self.start_sec,
            "end_sec": self.end_sec,
            "duration_sec": self.duration_sec,
            "label": self.label,
            "audio_source": self.audio_source,
            "hashes": self.hashes,  # List[Tuple[str, int]] → JSON array of [str, int]
            "rms": self.rms,
            "spectral_centroid": self.spectral_centroid,
            "zcr": self.zcr,
        }

    @staticmethod
    def from_dict(d: dict) -> "Fingerprint":
        fp = Fingerprint(
            segment_index=d["segment_index"],
            start_sec=d["start_sec"],
            end_sec=d["end_sec"],
            duration_sec=d["duration_sec"],
            label=d["label"],
            audio_source=d["audio_source"],
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
    si deux segments se ressemblent, non seulement leurs hashes
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
#  Clustering des segments similaires
# ─────────────────────────────────────────────────────────────


class RepetitionClusterer:
    """
    Groupe les segments ayant des fingerprints similaires.

    Utilise un index inversé (hash → segments) pour éviter la comparaison
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
        Retourne un dict {groupe_id: [indices de segments]}.
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

        # ── Index inversé : hash → liste d'indices de segments ──
        print(f"\n[Clustering] {n} segments — construction de l'index inversé...")
        hash_index: Dict[str, List[int]] = defaultdict(list)
        for i, fp in enumerate(fingerprints):
            for h in fp.hash_set:
                hash_index[h].append(i)

        # ── Compter les hashes partagés par paire (sans énumérer toutes les paires) ──
        # Seuil max : on ignore les hashes présents dans plus de la moitié des segments
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


class SegmentGroupingPipeline:
    def __init__(
        self,
        similarity_threshold: float = 0.08,
        sr: int = 22050,
        min_matching_hashes: int = 2,
        n_peaks_by_segment: int = 5,
        neighborhood_peaks_filter: int = 15,
        min_peak_amplitude: float = 0.01,
    ):
        self.sr = sr
        self.constellation = ConstellationMap(
            sr=sr,
            n_peaks=n_peaks_by_segment,
            neighborhood=neighborhood_peaks_filter,
            min_amplitude=min_peak_amplitude,
        )
        self.hasher = HashGenerator()
        self.matcher = FingerprintMatcher(min_matching_hashes=min_matching_hashes)
        self.clusterer = RepetitionClusterer(similarity_threshold)

    def _params_hash(self) -> str:
        """Hash des paramètres qui influent sur le calcul des fingerprints."""
        c = self.constellation
        h = self.hasher
        params = (
            f"sr={c.sr}|n_fft={c.n_fft}|hop={c.hop_length}|n_peaks={c.n_peaks}"
            f"|neighborhood={c.neighborhood}|min_amp={c.min_amplitude}"
            f"|fan_out={h.fan_out}|dt_max={h.time_delta_max}|dt_min={h.time_delta_min}"
            f"|features=v1"  # Invalide les caches sans rms/centroid/zcr
        )
        return hashlib.md5(params.encode()).hexdigest()[:8]

    def fingerprint_source(
        self, audio_path: str, segments: List[Segments], start_time: float
    ) -> List[Fingerprint]:
        fingerprints = []
        for i, seg in enumerate(segments):
            duration_seg = seg.end_sec - seg.start_sec
            if duration_seg < 0.5:  # Ignorer < 0.5s
                continue

            peaks = (
                np.array(seg.peaks, dtype=np.int32)
                if seg.peaks
                else np.empty((0, 2), dtype=np.int32)
            )

            hashes = self.hasher.generate(peaks)

            rms = seg.energy_mean
            centroid = seg.spectral_centroid
            zcr = seg.zcr

            fp = Fingerprint(
                segment_index=i,
                start_sec=seg.start_sec,
                end_sec=seg.end_sec,
                duration_sec=seg.end_sec - seg.start_sec,
                label="",
                audio_source=audio_path,
                hashes=hashes,
                rms=rms,
                spectral_centroid=centroid,
                zcr=zcr,
            )
            fp.build_hash_set()
            fingerprints.append(fp)

        return fingerprints

    def _fingerprints(self, sources) -> List[Fingerprint]:
        fingerprints = []
        for segments in sources:
            fingerprints.extend(self.fingerprint_source(segments))
        return fingerprints

    def run(
        self,
        sources: List[List[Segments]],
    ) -> dict:
        t0 = time.time()

        fingerprints = self._fingerprints(sources)

        groups = self.clusterer.cluster(fingerprints, self.matcher)

        results = self.build_report(fingerprints, groups)
        results["processing_time_sec"] = round(time.time() - t0, 2)

        return results["groups"]

    def build_report(self, fingerprints: List[Fingerprint], groups: Dict) -> dict:
        """Construit le rapport de répétitions."""

        report_groups = []
        for group_id, member_idxs in groups.items():
            members = [fingerprints[i] for i in member_idxs]
            members.sort(key=lambda x: x.start_sec)

            durations = [m.duration_sec for m in members]

            report_groups.append(
                {
                    "group_id": group_id,
                    "count": len(members),
                    "duration_mean": round(float(np.mean(durations)), 2),
                    "duration_std": round(float(np.std(durations)), 2),
                    "occurrences": [
                        {
                            "segment_index": m.segment_index,
                            "audio_source": m.audio_source,
                            "start_sec": round(m.start_sec, 2),
                            "end_sec": round(m.end_sec, 2),
                            "duration_sec": round(m.duration_sec, 2),
                            "label": m.label,
                        }
                        for m in members
                    ],
                }
            )

        # Trier par nombre d'occurrences décroissant
        report_groups.sort(key=lambda x: -x["count"])

        return {
            "total_segments": len(fingerprints),
            "total_groups": len(groups),
            "groups": report_groups,
        }


# ─────────────────────────────────────────────────────────────
#  Résumé console
# ─────────────────────────────────────────────────────────────


def print_report(report: dict):
    groups = report["groups"]
    print("\n" + "═" * 70)
    print("  RAPPORT DE RÉPÉTITIONS")
    print("═" * 70)
    print(f"  Segments analysés : {report['total_segments']}")
    print(f"  Groupes détectés  : {report['total_groups']}")

    # Stats par classification
    class_counts = Counter(g["classification"] for g in groups)
    seg_counts = Counter()
    for g in groups:
        seg_counts[g["classification"]] += g["count"]

    print(f"\n  {'Classification':<25} {'Groupes':>8}  {'Occurrences tot':>16}")
    print("  " + "─" * 52)
    for cls, gc in sorted(class_counts.items(), key=lambda x: -seg_counts[x[0]]):
        print(f"  {cls:<25} {gc:>8}  {seg_counts[cls]:>16}")

    print("\n  TOP 15 GROUPES LES PLUS FRÉQUENTS :")
    print(f"  {'Groupe':<8} {'Occurrences':>12}  {'Durée moy':>10}  {'Classification'}")
    print("  " + "─" * 60)
    for g in groups[:15]:
        print(
            f"  G{g['group_id']:<7} {g['count']:>12}  {g['duration_mean']:>8.1f}s  {g['classification']}"
        )
        # Afficher les 3 premières occurrences
        for occ in g["occurrences"][:3]:
            print(f"    ↳ [{occ['start_tc']}]  {occ['duration_sec']:.1f}s")
        if g["count"] > 3:
            print(f"    ↳ ... +{g['count'] - 3} autres occurrences")

    print("═" * 70)
    print(f"\n  Temps de traitement : {report.get('processing_time_sec', '?')}s\n")


# ─────────────────────────────────────────────────────────────
#  Point d'entrée
# ─────────────────────────────────────────────────────────────


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Fingerprinting et détection de répétitions dans un ou plusieurs flux audio",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  # Source unique
  finger_printer.py audio.mp3 --segments segments.json

  # Sources multiples (audio et segments appariés par ordre)
  finger_printer.py a1.mp3 a2.mp3 --segments s1.json s2.json
""",
    )
    parser.add_argument(
        "audio",
        nargs="+",
        help="Fichier(s) audio source",
    )
    parser.add_argument(
        "--segments",
        nargs="+",
        default=["segments.json"],
        help="JSON(s) produit(s) par rupture_detector.py, dans le même ordre que les audios [défaut: segments.json]",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.08,
        help="Seuil de similarité 0.0–1.0 [défaut: 0.08]",
    )
    parser.add_argument(
        "--out-json",
        default="repetitions.json",
        help="Rapport JSON de sortie [défaut: repetitions.json]",
    )
    parser.add_argument(
        "--out-plot",
        default="fingerprint_groupes.png",
        help="Image de visualisation [défaut: fingerprint_groupes.png]",
    )
    parser.add_argument("--no-plot", action="store_true")
    args = parser.parse_args()

    if len(args.audio) != len(args.segments):
        parser.error(
            f"Le nombre de fichiers audio ({len(args.audio)}) "
            f"doit correspondre au nombre de fichiers segments ({len(args.segments)})"
        )

    sources = list(zip(args.audio, args.segments))

    pipeline = SegmentGroupingPipeline(
        sources=sources,
        similarity_threshold=args.threshold,
    )

    report, fingerprints, groups = pipeline.run()

    print_report(report)

    # Export JSON
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"  Rapport JSON : {args.out_json}")

    print("\nFait. Prochaine étape : analyse de fréquence et rapport final.\n")


if __name__ == "__main__":
    main()
