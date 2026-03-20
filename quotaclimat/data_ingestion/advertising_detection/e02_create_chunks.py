"""
Détection de ruptures dans un flux audio (TV/Radio)
=====================================================
Chunke automatiquement un fichier audio en unités naturelles,
en coupant dans les micro silences où le contenu change.

Deux étapes explicites :
  1. Détection de pics dans les zones de silence (critère primaire)
  2. Filtrage par dissimilarité cosinus : ne garder que les silences
     où le contenu audio change réellement (critère secondaire)
"""

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import List, Tuple

import librosa
import numpy as np
from scipy.ndimage import maximum_filter, maximum_filter1d, uniform_filter1d
from scipy.signal import find_peaks

from .e00_partition_window import Segment


class HashGenerator:
    """
    Génère des hashes robustes en combinant des PAIRES de pics proches
    de la constellation map.

    Un hash = (freq1, freq2, delta_temps) → invariant au décalage temporel,
    résistant au bruit.
    """

    def __init__(
        self,
        fan_out: int = 15,
        time_delta_max: int = 100,
        time_delta_min: int = 1,
    ):
        self.fan_out = fan_out
        self.time_delta_max = time_delta_max
        self.time_delta_min = time_delta_min

    def generate(self, peaks: np.ndarray) -> List[Tuple[str, int]]:
        if len(peaks) < 2:
            return []

        peaks = peaks[peaks[:, 0].argsort()]

        hashes = []
        for i, (t1, f1) in enumerate(peaks):
            j = i + 1
            count = 0
            while j < len(peaks) and count < self.fan_out:
                t2, f2 = peaks[j]
                delta_t = t2 - t1

                if delta_t > self.time_delta_max:
                    break
                if delta_t >= self.time_delta_min:
                    h = self._make_hash(f1, f2, delta_t)
                    hashes.append((h, int(t1)))
                    count += 1
                j += 1

        return hashes

    def _make_hash(self, f1: int, f2: int, dt: int) -> str:
        raw = f"{f1}|{f2}|{dt}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]


@dataclass
class Chunk:
    start_sec: float
    end_sec: float
    channel: str
    duration_sec: float
    energy_mean: float
    spectral_centroid: float
    zcr_mean: float
    peaks: list = None
    hashes: list = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


class ChunkCreator:
    """
    Stratégie en deux passes :
      1. Trouver les pics de silence (transitions naturelles)
      2. Ne garder que ceux où le contenu audio change (dissimilarité cosinus)
      3. Extraire les descripteurs et la constellation map par chunk
    """

    def __init__(
        self,
        sr: int = 22050,  # Sample rate (Hz). Standard for audio analysis.
        hop_length: int = 512,  # STFT hop size (samples). Controls frame rate: fps = sr/hop_length ≈ 43.
        n_mfcc: int = 20,  # Number of MFCC coefficients for feature extraction.
        context_sec: float = 1.0,  # Context window (seconds) on each side for cosine dissimilarity.
        #   1.0s = good general balance. Increase (1.5-3s) if too many false positives.
        novelty_smooth_sec: float = 0.5,  # Smoothing window (seconds) applied to the novelty curve.
        #   Filters out short fluctuations before peak detection.
        min_chunk_sec: float = 5.0,  # Minimum duration (seconds) between two boundaries.
        #   Chunks shorter than this are merged. Increase (10-15s) for long programs.
        sensitivity: float = 0.25,  # Fraction of novelty peaks retained as boundaries.
        #   0.1 = only top 10% (few chunks). 0.5 = top 50% (many chunks).
        silence_percentile: float = 5.0,  # Energy percentile below which a frame is silent.
        #   5 = bottom 5% frames. Increase (8-15) if silences are less clear.
        n_fft: int = 2048,  # FFT size for constellation map. 2048 ≈ 93ms @ 22050Hz.
        n_peaks: int = 30,  # Max spectral peaks retained per chunk (constellation map).
        neighborhood: int = 15,  # Local max filter size for peak detection in time×frequency plane.
        min_amplitude: float = 0.01,  # Min normalized amplitude (0-1) for a spectral peak to be retained.
    ):
        self.sr = sr
        self.hop_length = hop_length
        self.n_mfcc = n_mfcc
        self.context_sec = context_sec
        self.novelty_smooth_sec = novelty_smooth_sec
        self.min_chunk_sec = min_chunk_sec
        self.sensitivity = sensitivity
        self.silence_percentile = silence_percentile
        self.n_fft = n_fft
        self.n_peaks = n_peaks
        self.neighborhood = neighborhood
        self.min_amplitude = min_amplitude
        self._fps = sr / hop_length
        self._hasher = HashGenerator()

    def load(self, path: str) -> np.ndarray:
        y, _ = librosa.load(path, sr=self.sr, mono=True)
        return y

    def extract_features(self, y: np.ndarray) -> dict:
        mfcc = librosa.feature.mfcc(
            y=y, sr=self.sr, n_mfcc=self.n_mfcc, hop_length=self.hop_length
        )
        delta = librosa.feature.delta(mfcc)
        energy = librosa.feature.rms(y=y, hop_length=self.hop_length)[0]
        centroid = librosa.feature.spectral_centroid(
            y=y, sr=self.sr, hop_length=self.hop_length
        )[0]
        zcr = librosa.feature.zero_crossing_rate(y, hop_length=self.hop_length)[0]

        stack = np.vstack([mfcc, delta, energy, centroid / centroid.max(), zcr])

        return {
            "stack": stack,
            "energy": energy,
            "centroid": centroid,
            "zcr": zcr,
        }

    def _compute_silence_mask(self, energy: np.ndarray) -> np.ndarray:
        """
        Step 1: build a binary mask of silent frames.

        A frame is silent if its energy is below the `silence_percentile`
        of the full signal. The mask is dilated by ~100ms to cover
        silence edges.
        """
        silence_threshold = np.percentile(energy, self.silence_percentile)
        silence_mask = (energy < silence_threshold).astype(float)

        dilation_frames = max(1, int(0.1 * self._fps))
        silence_mask = maximum_filter1d(silence_mask, size=dilation_frames * 2 + 1)

        return silence_mask

    def _compute_cosine_dissimilarity(self, stack: np.ndarray) -> np.ndarray:
        """
        Step 2: for each frame, measure how much the audio content
        changes between the past and future context windows.

        Returns a curve where high values indicate a content change.
        """
        n_frames = stack.shape[1]
        kernel = max(4, int(self.context_sec * self._fps))

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

        return cosine_dissim

    def _detect_peaks(
        self, silence_mask: np.ndarray, cosine_dissim: np.ndarray
    ) -> np.ndarray:
        """
        Combine both signals and find peaks.

        novelty = silence_mask × cosine_dissim
        → Only silent frames can produce peaks, and their height
          reflects how much the content changes at that point.
        """
        novelty = silence_mask * cosine_dissim

        smooth_frames = max(3, int(self.novelty_smooth_sec * self._fps))
        novelty = uniform_filter1d(novelty, size=smooth_frames)

        min_dist_frames = int(self.min_chunk_sec * self._fps)
        threshold = np.percentile(novelty, 100 * (1 - self.sensitivity))

        peaks, _ = find_peaks(novelty, height=threshold, distance=min_dist_frames)

        return peaks / self._fps

    def _extract_peaks(self, y_seg: np.ndarray) -> list:
        """Extract constellation map peaks from a chunk's audio."""
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

    def build_chunks(
        self,
        peaks_sec: np.ndarray,
        features: dict,
        duration_sec: float,
        y: np.ndarray,
        start_epoch: float,
        end_epoch: float,
        channel: str,
    ) -> List[Chunk]:
        """Build chunks with descriptors and constellation maps."""
        frames_per_sec = self._fps
        chunks = []

        for i in range(len(peaks_sec) - 1):
            t_start = peaks_sec[i]
            t_end = peaks_sec[i + 1]
            dur = t_end - t_start

            # Skip chunks that would end after the segment's end time (can happen if the last peak is close to the end)
            if float(t_start) + start_epoch > end_epoch:
                continue

            f_start = int(t_start * frames_per_sec)
            f_end = int(t_end * frames_per_sec)
            if f_end <= f_start:
                continue

            energy_seg = features["energy"][f_start:f_end]
            e = float(np.mean(energy_seg))

            # For centroid and ZCR: ignore silent frames at edges
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

            peaks_array = (
                np.array(seg_peaks, dtype=np.int32)
                if seg_peaks
                else np.empty((0, 2), dtype=np.int32)
            )
            seg_hashes = self._hasher.generate(peaks_array)

            chunks.append(
                Chunk(
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
            )

        return chunks

    def run(self, segment: Segment, audio_file_path: str) -> List[Chunk]:
        y = self.load(audio_file_path)
        features = self.extract_features(y)
        duration = len(y) / self.sr

        # Step 1: identify silent frames
        silence_mask = self._compute_silence_mask(features["energy"])
        # Step 2: measure content change at each frame
        cosine_dissim = self._compute_cosine_dissimilarity(features["stack"])
        # Combine and find peaks
        peaks_sec = self._detect_peaks(silence_mask, cosine_dissim)

        return self.build_chunks(
            peaks_sec,
            features,
            duration,
            y,
            segment.start_date.timestamp(),
            segment.end_date.timestamp(),
            channel=segment.channel,
        )

    def params(self) -> dict:
        return {
            "sr": self.sr,
            "hop_length": self.hop_length,
            "n_mfcc": self.n_mfcc,
            "context_sec": self.context_sec,
            "novelty_smooth_sec": self.novelty_smooth_sec,
            "min_chunk_sec": self.min_chunk_sec,
            "sensitivity": self.sensitivity,
            "silence_percentile": self.silence_percentile,
            "n_fft": self.n_fft,
            "n_peaks": self.n_peaks,
            "neighborhood": self.neighborhood,
            "min_amplitude": self.min_amplitude,
        }

    def params_hash(self) -> str:
        serialized = json.dumps(self.params(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]
