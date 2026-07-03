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

from typing import List

import librosa
import numpy as np
from scipy.ndimage import maximum_filter1d

from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.computer import (
    FingerprintComputer,
)
from quotaclimat.data_ingestion.advertising.tools.hashing import make_params_hash

from .e00_partition_window import Segment
from .tools.common_objects import Chunk


class ChunkCreator:
    """
    Stratégie en deux passes :
      1. Trouver les pics de silence (transitions naturelles)
      2. Ne garder que ceux où le contenu audio change (dissimilarité cosinus)
      3. Extraire les descripteurs et la constellation map par chunk
    """

    def __init__(
        self,
        fingerprint_computer: FingerprintComputer,
        min_chunk_sec: float = 5.0,  # Minimum duration (seconds) between two boundaries.
        #   Chunks shorter than this are merged. Increase (10-15s) for long programs.
        silence_percentile: float = 5.0,  # Energy percentile below which a frame is silent.
        #   5 = bottom 5% frames. Increase (8-15) if silences are less clear.
    ):
        self.fingerprint_computer = fingerprint_computer
        self.min_chunk_sec = min_chunk_sec
        self.silence_percentile = silence_percentile

        self.sr = fingerprint_computer.sr
        self.hop_length = fingerprint_computer.hop_length
        self._fps = self.sr / self.hop_length

    def load(self, path: str) -> np.ndarray:
        y, _ = librosa.load(path, sr=self.sr, mono=True)
        return y

    def extract_features(self, y: np.ndarray) -> dict:
        energy = librosa.feature.rms(y=y, hop_length=self.hop_length)[0]
        centroid = librosa.feature.spectral_centroid(
            y=y, sr=self.sr, hop_length=self.hop_length
        )[0]
        zcr = librosa.feature.zero_crossing_rate(y, hop_length=self.hop_length)[0]

        return {
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
        non_zero = energy[energy > 0]
        if len(non_zero) > 0:
            silence_threshold = np.percentile(non_zero, self.silence_percentile)
        else:
            silence_threshold = np.percentile(energy, self.silence_percentile)
        silence_mask = (energy <= silence_threshold).astype(float)

        dilation_frames = max(1, int(0.1 * self._fps))
        silence_mask = maximum_filter1d(silence_mask, size=dilation_frames * 2 + 1)

        return silence_mask

    def _detect_peaks(
        self,
        silence_mask: np.ndarray,
        energy: np.ndarray,
    ) -> np.ndarray:
        """
        Fully deterministic boundary detection using only local properties.

        1. Find contiguous silence regions from the binary silence mask.
        2. In each region, pick the frame with the lowest energy (deepest
           silence point) — purely local and deterministic.
        3. Enforce ``min_chunk_sec`` spacing: when two candidates are too
           close, keep the one with the deeper silence (lower energy).

        No global threshold or percentile is used, so adding/removing
        content elsewhere in the audio cannot affect boundary placement.
        """
        n_frames = len(silence_mask)

        # --- 1. Find contiguous silence regions ---
        diff = np.diff(np.concatenate([[0], silence_mask, [0]]))
        starts = np.where(diff > 0.5)[0]
        ends = np.where(diff < -0.5)[0]

        if len(starts) == 0:
            return np.array([])

        # --- 2. Anchor each region at its energy minimum ---
        candidates = []  # (frame_index, energy_at_min)
        for s, e in zip(starts, ends):
            e = min(e, n_frames)
            region_energy = energy[s:e]
            if len(region_energy) == 0:
                continue
            min_idx = s + int(np.argmin(region_energy))
            candidates.append((min_idx, float(energy[min_idx])))

        if not candidates:
            return np.array([])

        # Sort by energy ascending (deepest silences first) so the greedy
        # spacing filter keeps the best candidates.
        candidates.sort(key=lambda x: x[1])

        # --- 3. Enforce minimum spacing (greedy, deterministic) ---
        min_dist_frames = int(self.min_chunk_sec * self._fps)
        selected_frames: list[int] = []
        for frame, _ in candidates:
            if all(abs(frame - s) >= min_dist_frames for s in selected_frames):
                selected_frames.append(frame)

        selected_frames.sort()

        return np.array(selected_frames) / self._fps

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
            fingerprint = self.fingerprint_computer.from_audio_with_precomputed(
                y[s_start:s_end],
                duration_sec=float(dur),
                energy_mean=e,
                spectral_centroid=c,
                zcr_mean=z,
            )

            chunks.append(
                Chunk(
                    start_sec=round(start_epoch + float(t_start), 2),
                    end_sec=round(start_epoch + float(t_end), 2),
                    channel=channel,
                    fingerprint=fingerprint,
                )
            )

        return chunks

    def run(self, segment: Segment, audio_file_path: str) -> List[Chunk]:
        y = self.load(audio_file_path)
        features = self.extract_features(y)
        duration = len(y) / self.sr

        # Step 1: identify silent frames
        silence_mask = self._compute_silence_mask(features["energy"])
        # Step 2: find boundaries at deepest silences
        peaks_sec = self._detect_peaks(silence_mask, features["energy"])

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
            "min_chunk_sec": self.min_chunk_sec,
            "silence_percentile": self.silence_percentile,
        }

    def params_hash(self) -> str:
        return make_params_hash(self.params())
