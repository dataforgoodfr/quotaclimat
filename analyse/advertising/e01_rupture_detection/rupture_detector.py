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
import time
from dataclasses import asdict, dataclass
from typing import List

import librosa
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from scipy.ndimage import maximum_filter1d, uniform_filter1d
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

    def to_dict(self):
        d = asdict(self)
        d["start_tc"] = self.start_tc
        d["end_tc"] = self.end_tc
        return d


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
        sr: int = 22050,
        hop_length: int = 512,
        n_mfcc: int = 20,
        context_sec: float = 1.0,
        # ↑ CLEF : durée analysée de chaque côté d'un point pour évaluer la rupture.
        # Avant : hardcodé à 8 frames = 184ms → beaucoup trop court, capturait le bruit.
        # 1.0s = bon équilibre général (radio/TV).
        # Augmenter (1.5–3s) si trop de faux positifs.
        # Diminuer (0.3–0.5s) seulement si tu cherches des micro-transitions.
        novelty_smooth_sec: float = 0.5,
        # ↑ Lissage temporel de la courbe de nouveauté.
        # Avant : 10 frames = 230ms. 0.5s filtre mieux les fluctuations courtes.
        # Augmenter (1–2s) si la courbe est encore trop bruitée.
        min_segment_sec: float = 5.0,
        # ↑ Durée minimale imposée entre deux ruptures.
        # Relevé de 2s → 5s : les segments < 5s sont presque toujours du bruit.
        # Mettre à 10–15s pour des émissions longues.
        sensitivity: float = 0.25,
        # ↑ Fraction des pics de nouveauté retenus comme ruptures.
        # 0.1 = 10% des pics les plus intenses seulement (peu de segments).
        # 0.5 = la moitié des pics (beaucoup de segments).
        # Baissé de 0.3 → 0.25 par défaut.
        max_ruptures: int = 0,
        # ↑ Si > 0 : ne garder que les N ruptures les plus intenses, quelle que
        # soit la sensitivity. Utile pour forcer un nombre max de segments.
        # Ex : max_ruptures=50 garantit au plus 51 segments sur toute la durée.
        silence_percentile: float = 5.0,
        # ↑ Percentile d'énergie RMS en dessous duquel une frame est considérée
        # comme silencieuse. 5 = 5% des frames les plus silencieuses du fichier.
        # Augmenter (8–15) si les silences sont moins nets (parole continue).
        # Baisser (1–3) si seuls les vrais blancs doivent compter.
        cosine_weight: float = 1.0,
        # ↑ Poids de la dissimilarité cosinus dans le calcul de nouveauté.
        # novelty = silence_mask × (cosine_weight × cosine_dissim + (1 − cosine_weight))
        # 0.0 = silence pur : tout silence déclenche une rupture, indépendamment
        #       du contenu sonore avant/après. À utiliser pour diagnostiquer les
        #       silences manqués ou si le contenu se ressemble beaucoup (pub→pub).
        # 1.0 = comportement par défaut : le cosinus module l'intensité du pic.
        # 0.3 = bon compromis si certaines vraies ruptures sont manquées.
    ):
        self.sr = sr
        self.hop_length = hop_length
        self.n_mfcc = n_mfcc
        self.context_sec = context_sec
        self.novelty_smooth_sec = novelty_smooth_sec
        self.min_segment_sec = min_segment_sec
        self.sensitivity = sensitivity
        self.max_ruptures = max_ruptures
        self.silence_percentile = silence_percentile
        self.cosine_weight = cosine_weight
        self._fps = sr / hop_length  # frames/sec ≈ 43 à sr=22050, hop=512

    def get_novelty_peaks(self, novelty: np.ndarray) -> list:
        """
        Retourne tous les pics locaux de la courbe de nouveauté,
        avec uniquement le filtre distance (min_segment_sec) —
        avant le seuil sensitivity et le filtre max_ruptures.
        Utile pour la visualisation interactive du choix des paramètres.
        """
        min_dist_frames = int(self.min_segment_sec * self._fps)
        peaks, _ = find_peaks(novelty, distance=min_dist_frames)
        return [
            {
                "time_sec": round(float(p / self._fps), 4),
                "novelty": round(float(novelty[p]), 6),
            }
            for p in peaks
        ]

    def get_params(self) -> dict:
        """Retourne les paramètres du détecteur sous forme de dict (pour l'embarquer dans le player)."""
        return {
            "sr": self.sr,
            "hop_length": self.hop_length,
            "fps": round(self._fps, 2),
            "n_mfcc": self.n_mfcc,
            "context_sec": self.context_sec,
            "novelty_smooth_sec": self.novelty_smooth_sec,
            "min_segment_sec": self.min_segment_sec,
            "sensitivity": self.sensitivity,
            "max_ruptures": self.max_ruptures,
            "silence_percentile": self.silence_percentile,
            "cosine_weight": self.cosine_weight,
        }

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

    def compute_novelty(self, stack: np.ndarray, energy: np.ndarray) -> np.ndarray:
        """
        Courbe de nouveauté en deux étapes complémentaires :

        1. SILENCE (critère primaire)
           Les transitions pub/jingle/émission passent systématiquement par un
           silence visible sur la forme d'onde. Seuls les instants silencieux
           peuvent produire une forte nouveauté.
           → silence_mask[i] = 1 si energy[i] < percentile(silence_percentile), 0 sinon.
           → Dilaté légèrement pour couvrir les bords du silence.

        2. DISSIMILARITÉ COSINUS (critère secondaire)
           Parmi les instants silencieux, mesure à quel point le contenu sonore
           avant et après diffère. Permet de distinguer une vraie rupture
           (pub→pub, jingle→émission) d'une simple pause au sein d'un même contenu.

        novelty[i] = silence_mask[i] × cosine_dissimilarity[i]

        Un instant sans silence aura novelty = 0, quelle que soit la dissimilarité
        cosinus (les variations de timbre en cours d'émission sont ignorées).
        """
        print("[3/5] Calcul de la courbe de nouveauté...")

        n_frames = stack.shape[1]
        kernel = max(4, int(self.context_sec * self._fps))
        print(
            f"      Fenêtre de contexte : {kernel} frames = {kernel / self._fps:.2f}s de chaque côté"
        )

        # ── 1. Masque silence ────────────────────────────────────────────────
        silence_threshold = np.percentile(energy, self.silence_percentile)
        silence_mask = (energy < silence_threshold).astype(float)

        # Dilatation : élargit le masque autour de chaque silence (~100 ms)
        # pour que le pic de nouveauté puisse tomber aux bords du silence.
        dilation_frames = max(1, int(0.1 * self._fps))
        silence_mask = maximum_filter1d(silence_mask, size=dilation_frames * 2 + 1)

        n_silent = int(silence_mask.sum())
        print(
            f"      Seuil silence       : énergie < {silence_threshold:.5f} "
            f"(percentile {self.silence_percentile}) "
            f"→ {n_silent} frames silencieuses ({100 * n_silent / n_frames:.1f}%)"
        )

        # ── 2. Dissimilarité cosinus ─────────────────────────────────────────
        cosine_dissim = np.zeros(n_frames)
        norms = np.linalg.norm(stack, axis=0, keepdims=True) + 1e-8
        X = stack / norms

        for i in range(kernel, n_frames - kernel):
            past = X[:, i - kernel : i].mean(axis=1)
            future = X[:, i : i + kernel].mean(axis=1)
            cos_sim = np.dot(past, future) / (
                np.linalg.norm(past) * np.linalg.norm(future) + 1e-8
            )
            cosine_dissim[i] = 1.0 - cos_sim

        # ── 3. Combinaison : silence × (cosinus pondéré) ────────────────────
        # cosine_weight=0 → novelty = silence_mask  (tout silence = rupture)
        # cosine_weight=1 → novelty = silence_mask × cosine_dissim (défaut)
        # Valeurs intermédiaires → le cosinus module sans bloquer.
        novelty = silence_mask * (
            self.cosine_weight * cosine_dissim + (1.0 - self.cosine_weight)
        )

        # Lissage léger pour faciliter la détection de pics
        smooth_frames = max(3, int(self.novelty_smooth_sec * self._fps))
        novelty = uniform_filter1d(novelty, size=smooth_frames)
        print(
            f"      Lissage             : {smooth_frames} frames = {smooth_frames / self._fps:.2f}s"
        )

        return novelty

    def detect_peaks(self, novelty: np.ndarray) -> np.ndarray:
        """
        Trouve les pics de nouveauté = frontières naturelles.

        Trois filtres successifs :
          1. Seuil de hauteur (sensitivity)  → élimine les pics faibles
          2. Distance minimale (min_segment_sec) → impose un écart entre pics
          3. Top-N (max_ruptures)            → ne garde que les N plus intenses
        """
        print("[4/5] Détection des frontières...")

        min_dist_frames = int(self.min_segment_sec * self._fps)

        # Filtre 1 : seuil adaptatif sur la distribution de la courbe
        # sensitivity=0.25 → seuil au percentile 75 → on garde le quart supérieur
        threshold = np.percentile(novelty, 100 * (1 - self.sensitivity))
        print(
            f"      Seuil sensitivity={self.sensitivity:.2f} → "
            f"hauteur min {threshold:.4f} "
            f"(percentile {100 * (1 - self.sensitivity):.0f})"
        )

        peaks, props = find_peaks(
            novelty,
            height=threshold,
            distance=min_dist_frames,
        )
        print(
            f"      Après seuil + distance min ({self.min_segment_sec}s) : "
            f"{len(peaks)} ruptures candidates"
        )

        # Filtre 2 : top-N ruptures les plus intenses (optionnel)
        if self.max_ruptures > 0 and len(peaks) > self.max_ruptures:
            # Trier par hauteur décroissante, garder les N meilleurs,
            # puis re-trier par position temporelle pour reconstruire l'ordre
            heights = props["peak_heights"]
            top_idx = np.argsort(-heights)[: self.max_ruptures]
            peaks = np.sort(peaks[top_idx])
            print(
                f"      Filtre top-{self.max_ruptures} appliqué → "
                f"{len(peaks)} ruptures conservées"
            )

        # Convertir en secondes
        peaks_sec = peaks / self._fps

        print(
            f"      Résultat final : {len(peaks_sec)} ruptures → {len(peaks_sec) + 1} segments"
        )
        return peaks_sec

    def build_segments(
        self, peaks: np.ndarray, features: dict, duration_sec: float
    ) -> List[Segment]:
        """Construit les segments avec leurs descripteurs."""
        frames_per_sec = self.sr / self.hop_length
        segments = []

        # Ajoute début et fin
        boundaries = np.concatenate([[0.0], peaks, [duration_sec]])

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
            seg.label = "SEGMENT"
            segments.append(seg)

        return segments

    def run(
        self, path: str
    ) -> tuple[List[Segment], np.ndarray, np.ndarray, dict, np.ndarray]:
        t0 = time.perf_counter()

        y = self.load(path)
        t1 = time.perf_counter()
        print(f"      → {t1 - t0:.2f}s")

        features = self.extract_features(y)
        t2 = time.perf_counter()
        print(f"      → {t2 - t1:.2f}s")

        novelty = self.compute_novelty(features["stack"], features["energy"])
        t3 = time.perf_counter()
        print(f"      → {t3 - t2:.2f}s")

        duration = len(y) / self.sr
        peaks = self.detect_peaks(novelty)
        t4 = time.perf_counter()
        print(f"      → {t4 - t3:.2f}s")

        segments = self.build_segments(peaks, features, duration)
        t5 = time.perf_counter()

        print(
            f"[5/5] Segmentation terminée : {len(segments)} segments  → {t5 - t4:.2f}s"
        )
        print(f"      Durée totale : {t5 - t0:.2f}s")
        return segments, peaks, novelty, features, y


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
    data = [s.to_dict() for s in segments]
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
        default=0.25,
        help="Fraction des pics retenus, 0.1 (peu) à 0.5 (beaucoup) [défaut: 0.25]",
    )
    parser.add_argument(
        "--min-seg",
        type=float,
        default=5.0,
        help="Durée minimale d'un segment en secondes [défaut: 5.0]",
    )
    parser.add_argument(
        "--context",
        type=float,
        default=1.0,
        help="Secondes de contexte analysées de chaque côté d'un point [défaut: 1.0]",
    )
    parser.add_argument(
        "--smooth",
        type=float,
        default=0.5,
        help="Lissage de la courbe de nouveauté en secondes [défaut: 0.5]",
    )
    parser.add_argument(
        "--max-ruptures",
        type=int,
        default=0,
        dest="max_ruptures",
        help="Garder seulement les N ruptures les plus intenses (0 = pas de limite) [défaut: 0]",
    )
    parser.add_argument(
        "--cosine-weight",
        type=float,
        default=1.0,
        dest="cosine_weight",
        help="Poids du cosinus (0=silence pur, 1=cosinus plein) [défaut: 1.0]",
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
        "--out-novelty",
        default="novelty_peaks.json",
        help="Fichier JSON des pics de nouveauté (pour le player) [défaut: novelty_peaks.json]",
    )
    parser.add_argument(
        "--no-plot", action="store_true", help="Désactiver la visualisation"
    )
    args = parser.parse_args()

    detector = RuptureDetector(
        sensitivity=args.sensitivity,
        min_segment_sec=args.min_seg,
        context_sec=args.context,
        novelty_smooth_sec=args.smooth,
        max_ruptures=args.max_ruptures,
        cosine_weight=args.cosine_weight,
    )

    segments, peaks, novelty, features, y = detector.run(args.input)

    novelty_peaks = detector.get_novelty_peaks(novelty)

    print_summary(segments)
    export_json(segments, args.out_json)

    with open(args.out_novelty, "w", encoding="utf-8") as f:
        json.dump(novelty_peaks, f, separators=(",", ":"))
    print(f"    Export novelty peaks : {args.out_novelty} ({len(novelty_peaks)} pics)")

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
