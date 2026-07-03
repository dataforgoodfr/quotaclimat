import librosa
import numpy as np
from scipy.ndimage import maximum_filter

from .fingerprint import Fingerprint
from .pairs import PairGenerator


class FingerprintComputer:
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
        return np.column_stack(
            [time_idxs[order].astype(np.int32), freq_idxs[order].astype(np.int32)]
        ).tolist()

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
            np.array(peaks, dtype=np.int32) if peaks else np.empty((0, 2), dtype=np.int32)
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
            np.array(peaks, dtype=np.int32) if peaks else np.empty((0, 2), dtype=np.int32)
        )
        return Fingerprint(
            duration_sec=round(duration_sec, 2),
            energy_mean=round(float(np.mean(energy)), 2),
            spectral_centroid=round(float(np.mean(centroid)), 2),
            zcr_mean=round(float(np.mean(zcr)), 2),
            peaks=peaks,
            pairs=pairs,
        )
