"""
Détection de ruptures dans un flux audio (TV/Radio)
=====================================================
Segmente automatiquement un fichier audio en unités naturelles
(jingles, pubs, émissions) en détectant les changements de nature sonore.

Dépendances :
    pip install librosa numpy scipy matplotlib soundfile
"""

import argparse
import json
from dataclasses import asdict, dataclass
from typing import List

import librosa
import librosa.display
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from scipy.ndimage import uniform_filter1d
from scipy.signal import find_peaks

# ─────────────────────────────────────────────
#  Structure de données pour un segment détecté
# ─────────────────────────────────────────────


@dataclass
class Segment:
    index: int
    start_sec: float
    end_sec: float
    duration_sec: float
    energy_mean: (
        float  # Énergie RMS moyenne (utile pour discriminer silence/parole/musique)
    )
    spectral_centroid: float  # "Brillance" moyenne — grave vs aigu
    zcr_mean: float  # Zero-crossing rate — parole vs musique
    label: str = "?"  # Classification heuristique

    def to_timecode(self, t: float) -> str:
        h = int(t // 3600)
        m = int((t % 3600) // 60)
        s = t % 60
        return f"{h:02d}:{m:02d}:{s:06.3f}"

    @property
    def start_tc(self):
        return self.to_timecode(self.start_sec)

    @property
    def end_tc(self):
        return self.to_timecode(self.end_sec)


# ─────────────────────────────────────────────
#  Détection de ruptures
# ─────────────────────────────────────────────


class RuptureDetector:
    """
    Stratégie :
      1. Calcul de features frame-par-frame (MFCC + énergie + centroïde spectral)
      2. Calcul d'une courbe de "nouveauté" : à quel point chaque instant
         diffère-t-il de son voisinage ?
      3. Lissage + détection de pics → frontières de segments
      4. Extraction de descripteurs par segment
      5. Classification heuristique simple
    """

    def __init__(
        self,
        sr: int = 22050,  # Fréquence d'échantillonnage de travail
        hop_length: int = 512,  # Pas entre frames (~23ms à 22050Hz)
        n_mfcc: int = 20,  # Nombre de coefficients MFCC
        novelty_smooth: int = 10,  # Lissage de la courbe de nouveauté (en frames)
        min_segment_sec: float = 2.0,  # Durée minimale d'un segment (évite les micro-coupures)
        sensitivity: float = 0.3,  # 0.0 = peu sensible (peu de ruptures) / 1.0 = très sensible
    ):
        self.sr = sr
        self.hop_length = hop_length
        self.n_mfcc = n_mfcc
        self.novelty_smooth = novelty_smooth
        self.min_segment_sec = min_segment_sec
        self.sensitivity = sensitivity

    def load(self, path: str) -> np.ndarray:
        print(f"[1/5] Chargement : {path}")
        y, sr = librosa.load(path, sr=self.sr, mono=True)
        print(f"      Durée : {len(y) / sr:.1f}s  ({len(y) / sr / 60:.1f} min)")
        return y

    def extract_features(self, y: np.ndarray) -> dict:
        """Calcule les features frame-par-frame."""
        print("[2/5] Extraction des features...")

        mfcc = librosa.feature.mfcc(
            y=y, sr=self.sr, n_mfcc=self.n_mfcc, hop_length=self.hop_length
        )
        delta = librosa.feature.delta(mfcc)  # Dérivée temporelle des MFCC
        energy = librosa.feature.rms(y=y, hop_length=self.hop_length)[0]
        centroid = librosa.feature.spectral_centroid(
            y=y, sr=self.sr, hop_length=self.hop_length
        )[0]
        zcr = librosa.feature.zero_crossing_rate(y, hop_length=self.hop_length)[0]

        # Stack en une seule matrice (features × frames)
        stack = np.vstack([mfcc, delta, energy, centroid / centroid.max(), zcr])

        return {
            "stack": stack,
            "mfcc": mfcc,
            "energy": energy,
            "centroid": centroid,
            "zcr": zcr,
        }

    def compute_novelty(self, stack: np.ndarray) -> np.ndarray:
        """
        Courbe de nouveauté par matrice de similarité locale.

        On calcule la similarité cosinus entre des fenêtres glissantes
        "passé proche" vs "futur proche". Un pic = rupture de contexte.
        """
        print("[3/5] Calcul de la courbe de nouveauté...")

        n_frames = stack.shape[1]
        kernel = 8  # Taille de la fenêtre de contexte (en frames, ~0.2s)

        novelty = np.zeros(n_frames)

        # Normaliser chaque frame
        norms = np.linalg.norm(stack, axis=0, keepdims=True) + 1e-8
        X = stack / norms  # shape : (features, frames)

        for i in range(kernel, n_frames - kernel):
            past = X[:, i - kernel : i].mean(axis=1)
            future = X[:, i : i + kernel].mean(axis=1)
            cos_sim = np.dot(past, future) / (
                np.linalg.norm(past) * np.linalg.norm(future) + 1e-8
            )
            novelty[i] = 1.0 - cos_sim  # 0 = continuité, 1 = rupture forte

        # Lissage pour éviter les faux positifs sur micro-fluctuations
        novelty = uniform_filter1d(novelty, size=self.novelty_smooth)

        return novelty

    def detect_boundaries(self, novelty: np.ndarray, duration_sec: float) -> np.ndarray:
        """Trouve les pics de nouveauté = frontières naturelles."""
        print("[4/5] Détection des frontières...")

        frames_per_sec = self.sr / self.hop_length
        min_dist_frames = int(self.min_segment_sec * frames_per_sec)

        # Seuil adaptatif basé sur la distribution de la courbe
        threshold = np.percentile(novelty, 100 * (1 - self.sensitivity))

        peaks, props = find_peaks(
            novelty,
            height=threshold,
            distance=min_dist_frames,
        )

        # Convertir en secondes
        boundaries_sec = peaks / frames_per_sec

        # Ajouter début et fin
        boundaries_sec = np.concatenate([[0.0], boundaries_sec, [duration_sec]])

        print(f"      {len(peaks)} ruptures détectées → {len(peaks) + 1} segments")
        return boundaries_sec

    def build_segments(self, boundaries: np.ndarray, features: dict) -> List[Segment]:
        """Construit les segments avec leurs descripteurs."""
        frames_per_sec = self.sr / self.hop_length
        segments = []

        for i in range(len(boundaries) - 1):
            t_start = boundaries[i]
            t_end = boundaries[i + 1]
            dur = t_end - t_start

            f_start = int(t_start * frames_per_sec)
            f_end = int(t_end * frames_per_sec)

            # Éviter les segments vides
            if f_end <= f_start:
                continue

            e = float(np.mean(features["energy"][f_start:f_end]))
            c = float(np.mean(features["centroid"][f_start:f_end]))
            z = float(np.mean(features["zcr"][f_start:f_end]))

            seg = Segment(
                index=i,
                start_sec=float(t_start),
                end_sec=float(t_end),
                duration_sec=float(dur),
                energy_mean=e,
                spectral_centroid=c,
                zcr_mean=z,
            )
            seg.label = self._classify(seg)
            segments.append(seg)

        return segments

    def _classify(self, seg: Segment) -> str:
        """
        Heuristiques simples — à affiner selon ta chaîne.
        Ces seuils sont indicatifs, basés sur des distributions typiques.
        La vraie classification fine se fera au moment du clustering (étape suivante).
        """
        dur = seg.duration_sec

        # Silence / transition
        if seg.energy_mean < 0.01:
            return "silence"

        # Jingle : court, énergique, souvent aigu (centroïde élevé)
        if dur < 8 and seg.energy_mean > 0.05 and seg.spectral_centroid > 2500:
            return "jingle_candidat"

        # Pub : durée typique 15–90s, énergie modérée à forte
        if 8 <= dur <= 100:
            return "pub_candidat"

        # Émission : long
        if dur > 100:
            return "emission_candidat"

        return "inconnu"

    def run(self, path: str) -> tuple[List[Segment], np.ndarray, dict, np.ndarray]:
        y = self.load(path)
        features = self.extract_features(y)
        novelty = self.compute_novelty(features["stack"])
        duration = len(y) / self.sr
        boundaries = self.detect_boundaries(novelty, duration)
        segments = self.build_segments(boundaries, features)

        print(f"[5/5] Segmentation terminée : {len(segments)} segments")
        return segments, novelty, features, y


# ─────────────────────────────────────────────
#  Visualisation
# ─────────────────────────────────────────────

LABEL_COLORS = {
    "silence": "#95a5a6",
    "jingle_candidat": "#e74c3c",
    "pub_candidat": "#e67e22",
    "emission_candidat": "#2ecc71",
    "inconnu": "#bdc3c7",
}


def plot_results(
    y,
    sr,
    novelty,
    segments: List[Segment],
    hop_length=512,
    output_path="rupture_analyse.png",
):
    fig, axes = plt.subplots(3, 1, figsize=(18, 10), sharex=True)
    fig.patch.set_facecolor("#1a1a2e")
    for ax in axes:
        ax.set_facecolor("#16213e")
        ax.tick_params(colors="white")
        ax.yaxis.label.set_color("white")
        ax.spines[:].set_color("#444")

    times = np.linspace(0, len(y) / sr, len(y))
    frames_per_sec = sr / hop_length
    novelty_times = np.arange(len(novelty)) / frames_per_sec

    # ── Panel 1 : Forme d'onde + segments colorés ──
    ax = axes[0]
    ax.plot(times, y, color="#3498db", alpha=0.6, linewidth=0.4)
    ax.set_ylabel("Amplitude", color="white")
    ax.set_title("Forme d'onde + segments détectés", color="white", pad=8)

    for seg in segments:
        color = LABEL_COLORS.get(seg.label, "#bdc3c7")
        ax.axvspan(seg.start_sec, seg.end_sec, alpha=0.25, color=color)
        ax.axvline(seg.start_sec, color=color, linewidth=0.8, alpha=0.7)

    # ── Panel 2 : Courbe de nouveauté ──
    ax = axes[1]
    ax.plot(novelty_times, novelty, color="#e74c3c", linewidth=1.0)
    ax.fill_between(novelty_times, novelty, alpha=0.3, color="#e74c3c")
    ax.set_ylabel("Nouveauté", color="white")
    ax.set_title("Courbe de nouveauté (pics = ruptures)", color="white", pad=8)

    for seg in segments:
        ax.axvline(seg.start_sec, color="#f39c12", linewidth=0.6, alpha=0.5)

    # ── Panel 3 : Durée des segments (barres) ──
    ax = axes[2]
    centers = [(s.start_sec + s.end_sec) / 2 for s in segments]
    durations = [s.duration_sec for s in segments]
    colors = [LABEL_COLORS.get(s.label, "#bdc3c7") for s in segments]

    ax.bar(centers, durations, width=durations, color=colors, alpha=0.8, align="center")
    ax.set_ylabel("Durée (s)", color="white")
    ax.set_xlabel("Temps (s)", color="white")
    ax.set_title("Durée de chaque segment", color="white", pad=8)

    # Légende
    legend_patches = [
        mpatches.Patch(color=c, label=lab.replace("_", " ").capitalize())
        for lab, c in LABEL_COLORS.items()
    ]
    axes[0].legend(
        handles=legend_patches,
        loc="upper right",
        facecolor="#1a1a2e",
        edgecolor="#555",
        labelcolor="white",
        fontsize=8,
    )

    plt.tight_layout(rect=(0, 0, 1, 0.97))
    plt.savefig(
        output_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor()
    )
    plt.close()
    print(f"    Visualisation sauvegardée : {output_path}")


# ─────────────────────────────────────────────
#  Export
# ─────────────────────────────────────────────


def export_json(segments: List[Segment], path: str):
    data = []
    for s in segments:
        d = asdict(s)
        d["start_tc"] = s.start_tc
        d["end_tc"] = s.end_tc
        data.append(d)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"    Export JSON : {path}")


def print_summary(segments: List[Segment]):
    from collections import Counter

    counts = Counter(s.label for s in segments)
    total = len(segments)

    print("\n" + "═" * 55)
    print("  RÉSUMÉ DE SEGMENTATION")
    print("═" * 55)
    print(f"  Total segments : {total}")
    print(f"  {'Label':<22} {'Nb':>5}  {'%':>6}  {'Durée moy':>10}")
    print("  " + "─" * 50)

    for label, count in sorted(counts.items(), key=lambda x: -x[1]):
        segs_l = [s for s in segments if s.label == label]
        mean_d = np.mean([s.duration_sec for s in segs_l])
        pct = count / total * 100
        print(f"  {label:<22} {count:>5}  {pct:>5.1f}%  {mean_d:>8.1f}s")

    print("═" * 55)

    print("\n  10 segments les plus courts :")
    for s in sorted(segments, key=lambda x: x.duration_sec)[:10]:
        print(f"    [{s.start_tc}]  {s.duration_sec:6.2f}s  {s.label}")

    print("\n  10 segments les plus longs :")
    for s in sorted(segments, key=lambda x: -x.duration_sec)[:10]:
        print(f"    [{s.start_tc}]  {s.duration_sec:7.1f}s  {s.label}")


# ─────────────────────────────────────────────
#  Point d'entrée
# ─────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Détection de ruptures dans un flux audio TV/Radio"
    )
    parser.add_argument("input", help="Fichier audio d'entrée (wav, mp3, flac, ogg...)")
    parser.add_argument(
        "--sensitivity",
        type=float,
        default=0.35,
        help="Sensibilité 0.1 (peu) à 0.9 (très sensible) [défaut: 0.35]",
    )
    parser.add_argument(
        "--min-seg",
        type=float,
        default=2.0,
        help="Durée minimale d'un segment en secondes [défaut: 2.0]",
    )
    parser.add_argument(
        "--out-json",
        default="segments.json",
        help="Fichier JSON de sortie [défaut: segments.json]",
    )
    parser.add_argument(
        "--out-plot",
        default="rupture_analyse.png",
        help="Image de visualisation [défaut: rupture_analyse.png]",
    )
    parser.add_argument(
        "--no-plot", action="store_true", help="Désactiver la visualisation"
    )
    args = parser.parse_args()

    detector = RuptureDetector(
        sensitivity=args.sensitivity,
        min_segment_sec=args.min_seg,
    )

    segments, novelty, features, y = detector.run(args.input)

    print_summary(segments)
    export_json(segments, args.out_json)

    if not args.no_plot:
        plot_results(
            y,
            detector.sr,
            novelty,
            segments,
            hop_length=detector.hop_length,
            output_path=args.out_plot,
        )

    print(
        "\nFait. Prochaine étape : fingerprinting des segments pour détecter les répétitions.\n"
    )


if __name__ == "__main__":
    main()
