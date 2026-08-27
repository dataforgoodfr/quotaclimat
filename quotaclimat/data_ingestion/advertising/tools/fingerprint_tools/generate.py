from typing import List, Tuple

import librosa
import numpy as np
from scipy.ndimage import maximum_filter

from quotaclimat.data_ingestion.advertising.tools.hashing import make_params_hash

from .fingerprint import Fingerprint


class PairGenerator:
    """
    Generate peak-pair fingerprint tuples from a constellation map.

    Each pair = (freq1, freq2, delta_t, time_offset) — shift-invariant,
    noise-tolerant when matched with distance-based scoring.

    Peaks are expected as (time, freq, amplitude) rows. Within each anchor's
    time window, the most intense candidate targets are preferred; across all
    anchors, the resulting candidate pairs are ranked by combined intensity so
    that the most intense peaks pair up first, and the top max_pairs survive.
    """

    def __init__(
        self,
        fan_out: int = 4,
        time_delta_max: int = 100,
        time_delta_min: int = 1,
        max_pairs: int = 80,
    ):
        self.fan_out = fan_out
        self.time_delta_max = time_delta_max
        self.time_delta_min = time_delta_min
        self.max_pairs = max_pairs

    def generate(self, peaks: np.ndarray) -> List[Tuple[int, int, int, int]]:
        if len(peaks) < 2:
            return []

        peaks = peaks[peaks[:, 0].argsort()]
        times = peaks[:, 0]
        n = len(peaks)

        candidates = []  # (intensity, f1, f2, delta_t, t1)
        for i in range(n):
            t1, f1, a1 = peaks[i]

            window = []
            j = i + 1
            while j < n:
                delta_t = times[j] - t1
                if delta_t > self.time_delta_max:
                    break
                if delta_t >= self.time_delta_min:
                    window.append(j)
                j += 1

            window.sort(key=lambda idx: -peaks[idx][2])
            for idx in window[: self.fan_out]:
                t2, f2, a2 = peaks[idx]
                candidates.append(
                    (a1 + a2, int(f1), int(f2), int(t2 - t1), int(t1))
                )

        candidates.sort(key=lambda c: -c[0])
        return [(f1, f2, dt, t1) for _, f1, f2, dt, t1 in candidates[: self.max_pairs]]


class FingerprintGenerator:
    """
    Instantiate once with audio-processing params, then call from_audio()
    or from_audio_with_precomputed() to produce Fingerprint instances.
    """

    def __init__(
        self,
        sr: int = 22050,
        hop_length: int = 512,
        n_fft: int = 2048,
        neighborhood: int = 15,
        min_amplitude: float = 0.01,
        n_peaks: int = 30,
        fan_out: int = 4,
        max_pairs: int = 80,
    ):
        self.sr = sr
        self.hop_length = hop_length
        self.n_fft = n_fft
        self.neighborhood = neighborhood
        self.min_amplitude = min_amplitude
        self.n_peaks = n_peaks
        self.fan_out = fan_out
        self.max_pairs = max_pairs
        self._pair_generator = PairGenerator(fan_out=fan_out, max_pairs=max_pairs)

    def _extract_peaks(self, y_seg: np.ndarray) -> list:
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
        return [
            [int(time_idxs[o]), int(freq_idxs[o]), round(float(amplitudes[o]), 4)]
            for o in order
        ]

    def from_audio_with_precomputed(
        self,
        y_seg: np.ndarray,
        duration_sec: float,
        energy_mean: float,
        spectral_centroid: float,
        zcr_mean: float,
    ) -> Fingerprint:
        """Build a Fingerprint reusing already-computed descriptors; only peaks and pairs come from the audio."""
        peaks = self._extract_peaks(y_seg)
        pairs = self._pair_generator.generate(
            np.array(peaks, dtype=np.float64)
            if peaks
            else np.empty((0, 3), dtype=np.float64)
        )
        return Fingerprint(
            duration_sec=round(duration_sec, 2),
            energy_mean=round(energy_mean, 2),
            spectral_centroid=round(spectral_centroid, 2),
            zcr_mean=round(zcr_mean, 2),
            peaks=peaks,
            pairs=pairs,
        )

    def from_audio(self, y_seg: np.ndarray) -> Fingerprint:
        """Build a Fingerprint from a raw audio segment, computing all descriptors from scratch."""
        duration_sec = len(y_seg) / self.sr
        energy = librosa.feature.rms(y=y_seg, hop_length=self.hop_length)[0]
        centroid = librosa.feature.spectral_centroid(
            y=y_seg, sr=self.sr, hop_length=self.hop_length
        )[0]
        zcr = librosa.feature.zero_crossing_rate(y_seg, hop_length=self.hop_length)[0]

        peaks = self._extract_peaks(y_seg)
        pairs = self._pair_generator.generate(
            np.array(peaks, dtype=np.float64)
            if peaks
            else np.empty((0, 3), dtype=np.float64)
        )
        return Fingerprint(
            duration_sec=round(duration_sec, 2),
            energy_mean=round(float(np.mean(energy)), 2),
            spectral_centroid=round(float(np.mean(centroid)), 2),
            zcr_mean=round(float(np.mean(zcr)), 2),
            peaks=peaks,
            pairs=pairs,
        )

    def params(self) -> dict:
        return {
            "sr": self.sr,
            "hop_length": self.hop_length,
            "n_fft": self.n_fft,
            "neighborhood": self.neighborhood,
            "min_amplitude": self.min_amplitude,
            "n_peaks": self.n_peaks,
            "fan_out": self.fan_out,
            "max_pairs": self.max_pairs,
        }

    def params_hash(self) -> str:
        return make_params_hash(self.params())
