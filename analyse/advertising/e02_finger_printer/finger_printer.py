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
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from scipy.ndimage import maximum_filter
from tqdm import tqdm

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
        n_peaks: int = 20,  # Nombre de pics à garder par frame
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

        # Trier par amplitude décroissante et garder les N meilleurs
        amplitudes = D_norm[freq_idxs, time_idxs]
        order = np.argsort(-amplitudes)[: self.n_peaks * D_norm.shape[1] // 10]

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
    hashes: List[Tuple[str, int]] = field(default_factory=list)  # [(hash, t_ancrage)]
    hash_set: Set[str] = field(default_factory=set)  # Pour lookup rapide

    def build_hash_set(self):
        self.hash_set = {h for h, _ in self.hashes}


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

    def __init__(self, min_matching_hashes: int = 5):
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
    Union-Find pour construire les groupes de manière efficace.
    """

    def __init__(self, similarity_threshold: float = 0.08):
        self.threshold = similarity_threshold

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

        # Comparer toutes les paires
        # (optimisation possible avec LSH pour de très grands corpus)
        pairs = list(itertools.combinations(range(n), 2))
        print(f"\n[Clustering] {n} segments → {len(pairs)} paires à comparer...")

        matches = 0
        for i, j in tqdm(pairs, desc="Comparaison fingerprints"):
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


class FingerprintPipeline:
    def __init__(
        self,
        audio_path: str,
        segments_json: str,
        similarity_threshold: float = 0.08,
        sr: int = 22050,
    ):
        self.audio_path = audio_path
        self.segments_json = segments_json
        self.sr = sr
        self.constellation = ConstellationMap(sr=sr)
        self.hasher = HashGenerator()
        self.matcher = FingerprintMatcher()
        self.clusterer = RepetitionClusterer(similarity_threshold)

    def load_audio(self) -> np.ndarray:
        print(f"Chargement audio : {self.audio_path}")
        y, _ = librosa.load(self.audio_path, sr=self.sr, mono=True)
        return y

    def load_segments(self) -> list:
        with open(self.segments_json, encoding="utf-8") as f:
            return json.load(f)

    def fingerprint_all(self, y: np.ndarray, segments: list) -> List[Fingerprint]:
        print(f"\n[Fingerprinting] {len(segments)} segments...")
        fingerprints = []

        for seg in tqdm(segments, desc="Empreintes"):
            t_start = seg["start_sec"]
            t_end = seg["end_sec"]

            # Extraire l'audio du segment
            s = int(t_start * self.sr)
            e = int(t_end * self.sr)
            y_seg = y[s:e]

            if len(y_seg) < self.sr * 0.5:  # Ignorer < 0.5s
                continue

            # Constellation → hashes
            peaks = self.constellation.extract(y_seg)
            hashes = self.hasher.generate(peaks)

            fp = Fingerprint(
                segment_index=seg["index"],
                start_sec=t_start,
                end_sec=t_end,
                duration_sec=seg["duration_sec"],
                label=seg.get("label", "?"),
                hashes=hashes,
            )
            fp.build_hash_set()
            fingerprints.append(fp)

        print(f"  {len(fingerprints)} empreintes générées")
        return fingerprints

    def run(self) -> dict:
        t0 = time.time()

        y = self.load_audio()
        segments = self.load_segments()
        fingerprints = self.fingerprint_all(y, segments)
        groups = self.clusterer.cluster(fingerprints, self.matcher)

        results = self.build_report(fingerprints, groups)
        results["processing_time_sec"] = round(time.time() - t0, 2)

        return results, fingerprints, groups

    def build_report(self, fingerprints: List[Fingerprint], groups: Dict) -> dict:
        """Construit le rapport de répétitions."""

        # Construire l'index segment_index → fingerprint
        fp_by_idx = {fp.segment_index: fp for fp in fingerprints}

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
                    "classification": classify_group(
                        len(members), float(np.mean(durations))
                    ),
                    "occurrences": [
                        {
                            "segment_index": m.segment_index,
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

        # Trier par nombre d'occurrences décroissant
        report_groups.sort(key=lambda x: -x["count"])

        return {
            "total_segments": len(fingerprints),
            "total_groups": len(groups),
            "groups": report_groups,
        }


# ─────────────────────────────────────────────────────────────
#  Classification des groupes par fréquence + durée
# ─────────────────────────────────────────────────────────────


def classify_group(count: int, mean_duration: float) -> str:
    """
    Heuristique de classification des groupes.
    Ces seuils sont calibrés pour une semaine de diffusion (~168h).
    À ajuster selon le volume de données.
    """
    if count == 1:
        if mean_duration > 300:
            return "EMISSION_UNIQUE"
        return "SEGMENT_UNIQUE"

    if mean_duration < 10 and count > 30:
        return "JINGLE"

    if mean_duration < 10 and count > 5:
        return "JINGLE_RARE"

    if 10 <= mean_duration <= 120 and count > 10:
        return "PUBLICITE"

    if 10 <= mean_duration <= 120 and count > 3:
        return "PUBLICITE_RARE"

    if mean_duration > 120 and count > 3:
        return "HABILLAGE_ANTENNE"

    if mean_duration > 300:
        return "EMISSION_RECURRENTE"

    return "CONTENU_REPETE"


def sec_to_tc(t: float) -> str:
    h = int(t // 3600)
    m = int((t % 3600) // 60)
    s = t % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"


# ─────────────────────────────────────────────────────────────
#  Visualisation des groupes
# ─────────────────────────────────────────────────────────────

CLASS_COLORS = {
    "JINGLE": "#e74c3c",
    "JINGLE_RARE": "#e67e22",
    "PUBLICITE": "#f39c12",
    "PUBLICITE_RARE": "#f1c40f",
    "HABILLAGE_ANTENNE": "#9b59b6",
    "EMISSION_RECURRENTE": "#2ecc71",
    "EMISSION_UNIQUE": "#27ae60",
    "SEGMENT_UNIQUE": "#95a5a6",
    "CONTENU_REPETE": "#3498db",
}


def plot_groups(
    report: dict,
    total_duration_sec: float,
    output_path: str = "fingerprint_groupes.png",
):
    """
    Visualise les groupes comme une frise chronologique colorée
    + un histogramme des groupes par fréquence.
    """
    groups = report["groups"]

    fig, axes = plt.subplots(2, 1, figsize=(20, 10))
    fig.patch.set_facecolor("#0f0f1a")
    for ax in axes:
        ax.set_facecolor("#16213e")
        ax.tick_params(colors="white")
        ax.yaxis.label.set_color("white")
        ax.xaxis.label.set_color("white")
        for spine in ax.spines.values():
            spine.set_color("#444")

    # ── Panel 1 : Frise chronologique ──
    ax = axes[0]
    ax.set_title("Frise chronologique — groupes de répétition", color="white", pad=10)
    ax.set_xlim(0, total_duration_sec)
    ax.set_ylim(-0.5, 0.5)
    ax.set_yticks([])
    ax.set_xlabel("Temps (secondes)", color="white")

    for grp in groups:
        color = CLASS_COLORS.get(grp["classification"], "#555")
        for occ in grp["occurrences"]:
            ax.barh(
                0,
                occ["duration_sec"],
                left=occ["start_sec"],
                height=0.8,
                color=color,
                alpha=0.7,
                linewidth=0,
            )

    # Légende
    patches = [
        mpatches.Patch(color=c, label=l)
        for l, c in CLASS_COLORS.items()
        if any(g["classification"] == l for g in groups)
    ]
    ax.legend(
        handles=patches,
        loc="upper right",
        facecolor="#0f0f1a",
        edgecolor="#555",
        labelcolor="white",
        fontsize=7,
        ncol=2,
    )

    # ── Panel 2 : Top groupes par fréquence ──
    ax = axes[1]
    ax.set_title("Top 30 groupes par nombre d'occurrences", color="white", pad=10)

    top = groups[:30]
    labels = [
        f"G{g['group_id']}\n{g['classification'][:10]}\n{g['duration_mean']:.0f}s"
        for g in top
    ]
    counts = [g["count"] for g in top]
    colors = [CLASS_COLORS.get(g["classification"], "#555") for g in top]

    bars = ax.bar(range(len(top)), counts, color=colors, alpha=0.85)
    ax.set_xticks(range(len(top)))
    ax.set_xticklabels(labels, fontsize=6, color="white")
    ax.set_ylabel("Nombre d'occurrences", color="white")

    for bar, count in zip(bars, counts):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.3,
            str(count),
            ha="center",
            va="bottom",
            color="white",
            fontsize=7,
        )

    plt.tight_layout()
    plt.savefig(
        output_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor()
    )
    plt.close()
    print(f"  Visualisation sauvegardée : {output_path}")


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
        description="Fingerprinting et détection de répétitions dans un flux audio"
    )
    parser.add_argument(
        "audio", help="Fichier audio source (même que pour rupture_detector.py)"
    )
    parser.add_argument(
        "--segments",
        default="segments.json",
        help="JSON produit par rupture_detector.py [défaut: segments.json]",
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

    pipeline = FingerprintPipeline(
        audio_path=args.audio,
        segments_json=args.segments,
        similarity_threshold=args.threshold,
    )

    report, fingerprints, groups = pipeline.run()

    print_report(report)

    # Export JSON
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"  Rapport JSON : {args.out_json}")

    if not args.no_plot:
        # Durée totale = fin du dernier segment
        total = max(fp.end_sec for fp in fingerprints) if fingerprints else 3600
        plot_groups(report, total, args.out_plot)

    print("\nFait. Prochaine étape : analyse de fréquence et rapport final.\n")


if __name__ == "__main__":
    main()
