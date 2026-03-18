"""
Détection de ruptures dans un flux audio (TV/Radio)
=====================================================
Segmente automatiquement un fichier audio en unités naturelles,
en coupant dans les micro silences.
Extrait les descripteurs de chaque segment (énergie, centroïde spectral, ZCR) et une constellation map de pics spectraux (MFCC + delta).

"""

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from typing import List, Tuple

import librosa
import numpy as np
from scipy.ndimage import maximum_filter, maximum_filter1d, uniform_filter1d
from scipy.signal import find_peaks

from .e01_download_audio import ProcessingTask

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


# ─────────────────────────────────────────────
#  Structure de données pour un segment détecté
# ─────────────────────────────────────────────


@dataclass
class Segment:
    start_sec: float
    end_sec: float
    channel: str
    duration_sec: float
    energy_mean: (
        float  # Énergie RMS moyenne (utile pour discriminer silence/parole/musique)
    )
    spectral_centroid: float  # "Brillance" moyenne — grave vs aigu
    zcr_mean: float  # Zero-crossing rate — parole vs musique
    peaks: list = None  # Constellation map : liste de [time_frame, freq_bin] relatifs au début du segment
    hashes: list = (
        None  # Liste de [hash_str, anchor_time] pré-calculés à partir des peaks
    )

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


# ─────────────────────────────────────────────
#  Création de segments
# ─────────────────────────────────────────────


class SegmentCreator:
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
        # ── Paramètres de la constellation map (pics spectraux) ──────────────
        n_fft: int = 2048,
        # ↑ Taille FFT pour le spectrogramme de la constellation. 2048 ≈ 93ms @ 22050Hz.
        n_peaks: int = 30,
        # ↑ Nombre maximum de pics spectraux retenus par segment (indépendant de la durée).
        neighborhood: int = 15,
        # ↑ Taille du filtre de maximum local dans le plan temps×fréquence.
        #   Un pic est retenu uniquement s'il est le plus fort dans son voisinage.
        min_amplitude: float = 0.01,
        # ↑ Seuil minimal d'amplitude normalisée (0–1) pour qu'un point soit retenu.
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
        self.n_fft = n_fft
        self.n_peaks = n_peaks
        self.neighborhood = neighborhood
        self.min_amplitude = min_amplitude
        self._fps = sr / hop_length  # frames/sec ≈ 43 à sr=22050, hop=512
        self._hasher = HashGenerator()

    def load(self, path: str) -> np.ndarray:
        y, sr = librosa.load(path, sr=self.sr, mono=True)
        return y

    def extract_features(self, y: np.ndarray) -> dict:
        """Calcule les features frame-par-frame."""

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

        n_frames = stack.shape[1]
        kernel = max(4, int(self.context_sec * self._fps))

        # ── 1. Masque silence ────────────────────────────────────────────────
        silence_threshold = np.percentile(energy, self.silence_percentile)
        silence_mask = (energy < silence_threshold).astype(float)

        # Dilatation : élargit le masque autour de chaque silence (~100 ms)
        # pour que le pic de nouveauté puisse tomber aux bords du silence.
        dilation_frames = max(1, int(0.1 * self._fps))
        silence_mask = maximum_filter1d(silence_mask, size=dilation_frames * 2 + 1)

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

        return novelty

    def detect_peaks(self, novelty: np.ndarray) -> np.ndarray:
        """
        Trouve les pics de nouveauté = frontières naturelles.

        Trois filtres successifs :
          1. Seuil de hauteur (sensitivity)  → élimine les pics faibles
          2. Distance minimale (min_segment_sec) → impose un écart entre pics
          3. Top-N (max_ruptures)            → ne garde que les N plus intenses
        """

        min_dist_frames = int(self.min_segment_sec * self._fps)

        # Filtre 1 : seuil adaptatif sur la distribution de la courbe
        # sensitivity=0.25 → seuil au percentile 75 → on garde le quart supérieur
        threshold = np.percentile(novelty, 100 * (1 - self.sensitivity))

        peaks, props = find_peaks(
            novelty,
            height=threshold,
            distance=min_dist_frames,
        )

        # Filtre 2 : top-N ruptures les plus intenses (optionnel)
        if self.max_ruptures > 0 and len(peaks) > self.max_ruptures:
            # Trier par hauteur décroissante, garder les N meilleurs,
            # puis re-trier par position temporelle pour reconstruire l'ordre
            heights = props["peak_heights"]
            top_idx = np.argsort(-heights)[: self.max_ruptures]
            peaks = np.sort(peaks[top_idx])

        # Convertir en secondes
        peaks_sec = peaks / self._fps

        return peaks_sec

    def _extract_peaks(self, y_seg: np.ndarray) -> list:
        """
        Extrait la constellation map d'un segment audio.
        Retourne une liste de [time_frame, freq_bin] relatifs au début du segment.
        """
        if len(y_seg) < self.sr * 0.5:
            return []

        D = np.abs(librosa.stft(y_seg, n_fft=self.n_fft, hop_length=self.hop_length))
        D_log = librosa.amplitude_to_db(D, ref=np.max)
        D_norm = (D_log - D_log.min()) / (D_log.max() - D_log.min() + 1e-8)

        local_max = maximum_filter(D_norm, size=self.neighborhood)
        is_peak = (D_norm == local_max) & (D_norm > self.min_amplitude)

        freq_idxs, time_idxs = np.where(is_peak)
        if len(freq_idxs) == 0:
            return []

        amplitudes = D_norm[freq_idxs, time_idxs]
        order = np.argsort(-amplitudes)[: self.n_peaks]
        return np.column_stack(
            [time_idxs[order].astype(np.int32), freq_idxs[order].astype(np.int32)]
        ).tolist()

    def build_segments(
        self,
        peaks: np.ndarray,
        features: dict,
        duration_sec: float,
        y: np.ndarray,
        start_epoch: float,
        channel: str,
    ) -> List[Segment]:
        """Construit les segments avec leurs descripteurs et leur constellation map."""
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

            energy_seg = features["energy"][f_start:f_end]
            e = float(np.mean(energy_seg))

            # Pour centroïde et ZCR : ignorer les frames silencieuses en début/fin
            # (un blanc de quelques ms sinon tire le centroïde vers 0 et fausse le ZCR)
            silence_thr = np.percentile(features["energy"], self.silence_percentile)
            non_silent = np.where(energy_seg > silence_thr)[0]
            if len(non_silent) >= 2:
                fc_start = f_start + int(non_silent[0])
                fc_end = f_start + int(non_silent[-1]) + 1
            else:
                fc_start, fc_end = f_start, f_end

            c = float(np.mean(features["centroid"][fc_start:fc_end]))
            z = float(np.mean(features["zcr"][fc_start:fc_end]))

            s_start = int(t_start * self.sr)
            s_end = int(t_end * self.sr)
            seg_peaks = self._extract_peaks(y[s_start:s_end])

            # Pré-calcul des hashes à partir des pics (constellation fingerprinting)
            peaks_array = (
                np.array(seg_peaks, dtype=np.int32)
                if seg_peaks
                else np.empty((0, 2), dtype=np.int32)
            )
            seg_hashes = self._hasher.generate(peaks_array)

            seg = Segment(
                start_sec=start_epoch + float(t_start),
                end_sec=start_epoch + float(t_end),
                duration_sec=float(dur),
                energy_mean=e,
                spectral_centroid=c,
                zcr_mean=z,
                peaks=seg_peaks,
                hashes=seg_hashes,
                channel=channel,
            )
            segments.append(seg)

        return segments

    def run(self, task: ProcessingTask) -> List[Segment]:
        y = self.load(task.audio_file_path)
        features = self.extract_features(y)
        novelty = self.compute_novelty(features["stack"], features["energy"])
        duration = len(y) / self.sr
        peaks = self.detect_peaks(novelty)
        segments = self.build_segments(
            peaks,
            features,
            duration,
            y,
            task.download_task.start_date.timestamp(),
            channel=task.download_task.channel,
        )

        return segments

    # ─────────────────────────────────────────────
    #  Visualisation
    # ─────────────────────────────────────────────

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

    detector = SegmentCreator(
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

    with open(args.out_novelty, "w", encoding="utf-8") as f:
        json.dump(novelty_peaks, f, separators=(",", ":"))
    print(f"    Export novelty peaks : {args.out_novelty} ({len(novelty_peaks)} pics)")

    print(
        "\nFait. Prochaine étape : fingerprinting des segments pour détecter les répétitions.\n"
    )


if __name__ == "__main__":
    main()
