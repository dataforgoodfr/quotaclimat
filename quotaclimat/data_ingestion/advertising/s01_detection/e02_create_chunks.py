"""
Détection de ruptures dans un flux audio (TV/Radio)
=====================================================
Chunke automatiquement un fichier audio en unités naturelles,
en coupant dans les micro silences où le contenu change.
"""

from dataclasses import dataclass
from typing import List

import librosa
import numpy as np
from scipy.ndimage import maximum_filter1d, percentile_filter, uniform_filter1d

from quotaclimat.data_ingestion.advertising.tools.correlation import (
    find_best_correlation,
)
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.fingerprint import (
    Fingerprint,
)
from quotaclimat.data_ingestion.advertising.tools.fingerprint_tools.generate import (
    FingerprintGenerator,
)
from quotaclimat.data_ingestion.advertising.tools.hashing import make_params_hash
from quotaclimat.data_ingestion.advertising.tools.segments import Segment

from .tools.common_objects import Chunk

# Consecutive 2-minutes mediatree parts sometimes share a bit of duplicate audio at their
# boundary. Rather than assume a fixed size, we cross-correlate a short window near the
# join (same approach as overlay_correction.detect_overlap) and only trim what's actually
# found, so a boundary with no overlap is left untouched.
_CORRELATION_SEARCH_SEC = 3.0
_CORRELATION_MATCH_SEC = 0.15
_CORRELATION_THRESHOLD = 0.6

# Bump this when the detection algorithm itself changes (not just its parameter
# values), so that params_hash() changes too and the on-disk chunk cache is
# invalidated even though the visible config didn't change.
_ALGO_VERSION = 3


@dataclass
class ChunkCreatorJob:
    segment: Segment
    audio_file_path: str
    has_previous_segment: bool = False
    next_audio_file_path: str | None = None


class ChunkCreator:
    """
    Stratégie :
      1. Trouver les pics de silence (transitions naturelles)
      2. Extraire les descripteurs et la constellation map par chunk
    """

    def __init__(
        self,
        fingerprinter: FingerprintGenerator,
        sr: int = 22050,  # Sample rate (Hz) used for splitting/feature extraction.
        # Decoupled from fingerprinter.sr: the two audio uses have different needs
        # (fine-grained silence detection here vs. stable, cache-friendly fingerprints
        # there), so segments are resampled to fingerprinter.sr before being fingerprinted.
        hop_length: int = 512,  # STFT/RMS hop size (samples). Controls frame rate: fps = sr/hop_length ≈ 43.
        frame_length: int = 1024,  # Analysis window (samples) for RMS/centroid/ZCR.
        # Smaller than hop_length*2 default would give a noisier curve; kept at 2x
        # hop_length like librosa's own default ratio, just scaled down for more detail.
        min_chunk_sec: float = 0.8,  # Minimum duration (seconds) between two boundaries.
        #   Chunks shorter than this are merged. Increase (10-15s) for long programs.
        silence_percentile: float = 5.0,  # Energy percentile below which a frame is silent.
        #   5 = bottom 5% frames. Increase (8-15) if silences are less clear.
        energy_smoothing_sec: float = 0,  # seconds
        # Moving-average window applied to the energy curve before it's used for
        # silence detection. Absorbs single-frame noise (e.g. mp3-encoding artifacts)
        # so the same audio, encoded twice, doesn't flip silent/non-silent on a frame
        # that happens to sit right on the threshold.
        silence_margin: float = 0.1,  # fraction of the local threshold
        # A frame must be below `local_threshold * (1 - silence_margin)` to count as
        # silent, not just below `local_threshold`. Biases borderline frames toward
        # "not silent" so small energy differences between two encodings of the same
        # audio are less likely to be the thing deciding the outcome.
        silence_mask_sec: float = 2,  # seconds
        # Half-width of the rolling window used to compute the local silence
        # threshold: at each frame, silence_percentile is taken over the
        # +/- silence_mask_sec of energy around it, instead of over the whole
        # signal. Keeps the threshold reproducible across two jobs that only
        # share a few seconds of overlapping audio at a segment boundary
        # (see _local_silence_threshold). Larger = threshold adapts more
        # slowly to loudness changes; smaller = more locally reactive but
        # noisier.
        seconds_reserved_for_previous_segment: float = 5,  # seconds
        # Peaks are not extracted from the first 5 seconds, so the first extracted chunk start after that.
        margin_extracted_from_next_segment: float = 30,  # seconds
        # But peaks are extracted from the 30 seconds after the audio, keeping only one after 5 seconds, so the first chunks of the next segment are extracted here, as well as the one overpassing 5 seconds.
    ):
        self.fingerprinter = fingerprinter
        self.min_chunk_sec = min_chunk_sec
        self.silence_percentile = silence_percentile
        self.energy_smoothing_sec = energy_smoothing_sec
        self.silence_margin = silence_margin
        self.seconds_reserved_for_previous_segment = (
            seconds_reserved_for_previous_segment
        )
        self.margin_extracted_from_next_segment = margin_extracted_from_next_segment
        self.silence_mask_sec = silence_mask_sec

        self.sr = sr
        self.hop_length = hop_length
        self.frame_length = frame_length
        self._fps = self.sr / self.hop_length

    def load(
        self, path: str, duration: float | None = None, offset: float = 0.0
    ) -> np.ndarray:
        y, _ = librosa.load(
            path, sr=self.sr, mono=True, duration=duration, offset=offset
        )
        return y

    def _extend_with_next_segment(
        self, y: np.ndarray, next_audio_file_path: str
    ) -> np.ndarray:
        """Append the first `margin_extracted_from_next_segment` seconds of the next
        segment's audio to `y`, after trimming away any duplicate content shared with
        the end of `y` (see the module-level `_CORRELATION_*` constants).
        """
        next_y = self.load(
            next_audio_file_path,
            duration=self.margin_extracted_from_next_segment + _CORRELATION_SEARCH_SEC,
        )

        search_samples = int(_CORRELATION_SEARCH_SEC * self.sr)
        match_samples = int(_CORRELATION_MATCH_SEC * self.sr)

        tail = y[-search_samples:]
        needle = next_y[:match_samples]

        overlap_samples = 0
        if len(needle) == match_samples and len(tail) > len(needle):
            start, correlation = find_best_correlation(tail, needle)
            if correlation >= _CORRELATION_THRESHOLD:
                overlap_samples = len(tail) - start

        margin_samples = int(self.margin_extracted_from_next_segment * self.sr)
        margin = next_y[overlap_samples : overlap_samples + margin_samples]

        return np.concatenate([y, margin])

    def extract_features(self, y: np.ndarray) -> dict:
        energy = librosa.feature.rms(
            y=y, frame_length=self.frame_length, hop_length=self.hop_length
        )[0]
        centroid = librosa.feature.spectral_centroid(
            y=y, sr=self.sr, n_fft=self.frame_length, hop_length=self.hop_length
        )[0]
        zcr = librosa.feature.zero_crossing_rate(
            y, frame_length=self.frame_length, hop_length=self.hop_length
        )[0]

        return {
            "energy": energy,
            "centroid": centroid,
            "zcr": zcr,
        }

    def _local_silence_threshold(
        self, energy: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Smooth the energy curve (to absorb single-frame noise, e.g. mp3-encoding
        artifacts that shift a frame's energy by a couple of percent) and compute
        the local silence threshold over it: the `silence_percentile` of energy
        within a window of +/- silence_mask_sec around each frame, rather than
        over the whole signal. This is what makes boundary detection reproducible
        across two independent runs that only share a few seconds of overlapping
        audio (this segment's job and the previous/next segment's job): since the
        threshold at a given point only depends on audio within that shared
        window, both runs land on the exact same peak there, so one job's last
        chunk end and the neighbouring job's first chunk start always coincide
        instead of leaving a gap (or overlap).

        The returned threshold already has `silence_margin` applied: a frame must
        be below it by that margin to count as silent, not just below the raw
        percentile, so borderline frames lean toward "not silent" instead of
        flipping on small energy differences between two encodings of the same
        audio.
        """
        smoothing_frames = max(1, int(round(self.energy_smoothing_sec * self._fps)))
        smoothed_energy = uniform_filter1d(
            energy, size=smoothing_frames, mode="nearest"
        )

        window_frames = 2 * int(round(self.silence_mask_sec * self._fps)) + 1
        local_threshold = percentile_filter(
            smoothed_energy,
            percentile=self.silence_percentile,
            size=window_frames,
            mode="nearest",
        )
        return smoothed_energy, local_threshold * (1 - self.silence_margin)

    def _compute_silence_mask(self, energy: np.ndarray) -> np.ndarray:
        """
        Step 1: build a binary mask of silent frames, then dilate it by ~100ms to
        cover silence edges. See `_local_silence_threshold` for how the threshold
        itself is computed.
        """
        smoothed_energy, local_threshold = self._local_silence_threshold(energy)
        silence_mask = (smoothed_energy <= local_threshold).astype(float)

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

    def _split_from_peaks(
        self,
        peaks_sec: np.ndarray,
        min_start_sec: float | None = None,
        max_start_sec: float | None = None,
    ) -> List[tuple[float, float]]:
        """Split based on pre computed peaks."""
        if min_start_sec is not None:
            peaks_sec = peaks_sec[peaks_sec >= min_start_sec]

        segments: List[tuple(float, float)] = []

        for i in range(len(peaks_sec) - 1):
            t_start = peaks_sec[i]
            t_end = peaks_sec[i + 1]

            if max_start_sec is not None and float(t_start) > max_start_sec:
                continue

            if t_end <= t_start:
                continue

            segments.append((t_start, t_end))

        return segments

    def _build_fingerprint(
        self,
        t_start: float,
        t_end: float,
        y: np.ndarray,
        features: dict,
    ):
        dur = t_end - t_start

        f_start = int(t_start * self._fps)
        f_end = int(t_end * self._fps)

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
        y_seg = y[s_start:s_end]
        if self.sr != self.fingerprinter.sr:
            # The fingerprinter has its own sr, decoupled from ours, so segments must be
            # resampled to it before fingerprinting rather than fingerprinted as-is.
            y_seg = librosa.resample(
                y_seg, orig_sr=self.sr, target_sr=self.fingerprinter.sr
            )
        return self.fingerprinter.from_audio_with_precomputed(
            y_seg,
            duration_sec=float(dur),
            energy_mean=e,
            spectral_centroid=c,
            zcr_mean=z,
        )

    def build_chunks(
        self,
        peaks_sec,
        features,
        duration,
        y,
        start_epoch,
        max_start_sec,
        channel,
    ):
        min_start_sec = None
        segments = self._split_from_peaks(peaks_sec, min_start_sec, max_start_sec)

        fingerprints = [
            (
                t_start,
                t_end,
                self._build_fingerprint(t_start, t_end, y, features),
            )
            for (t_start, t_end) in segments
        ]

        return [
            Chunk(
                start_sec=round(start_epoch + float(t_start), 2),
                end_sec=round(start_epoch + float(t_end), 2),
                channel=channel,
                fingerprint=fingerprint,
            )
            for (t_start, t_end, fingerprint) in fingerprints
        ]

    def split_in_chunks_and_build_fingerprints(
        self,
        y: np.ndarray,
        min_start_sec: float | None = None,
        max_start_sec: float | None = None,
    ) -> List[tuple[float, float, Fingerprint]]:
        """Build fingerprints with descriptors and constellation maps."""

        features = self.extract_features(y)

        silence_mask = self._compute_silence_mask(features["energy"])
        peaks_sec = self._detect_peaks(silence_mask, features["energy"])

        segments = self._split_from_peaks(peaks_sec, min_start_sec, max_start_sec)

        return [
            (
                t_start,
                t_end,
                self._build_fingerprint(t_start, t_end, y, features),
            )
            for (t_start, t_end) in segments
        ]

    def run(self, job: ChunkCreatorJob) -> List[Chunk]:
        """Main usage of the ChunkCreator:
        The function extract chunk, which are identified segments on specific timestamps on a specific media.
        A ChunkCreatorJob object is the only argument, it describe the spec of the extraction.
        """
        start_epoch = job.segment.start_date.timestamp()
        end_epoch = job.segment.end_date.timestamp()
        duration = end_epoch - start_epoch

        y = self.load(job.audio_file_path)

        if job.next_audio_file_path is not None:
            y = self._extend_with_next_segment(y, job.next_audio_file_path)

        # The previous segment's own extraction already covered this leading window
        # (via its margin_extracted_from_next_segment); drop peaks in it so chunk
        # creation only starts at seconds_reserved_for_previous_segment.
        min_start_sec = (
            self.seconds_reserved_for_previous_segment
            if job.has_previous_segment
            else None
        )

        max_start_sec = duration
        if job.next_audio_file_path is not None:
            # Allow chunks to start into the appended margin, but only up to
            # seconds_reserved_for_previous_segment past the original audio's end — the
            # next segment's own run (has_previous_segment=True) picks up from there.
            max_start_sec += self.seconds_reserved_for_previous_segment

        fingerprints = self.split_in_chunks_and_build_fingerprints(
            y=y,
            min_start_sec=min_start_sec,
            max_start_sec=max_start_sec,
        )

        return [
            Chunk(
                start_sec=round(start_epoch + float(t_start), 2),
                end_sec=round(start_epoch + float(t_end), 2),
                channel=job.segment.channel,
                fingerprint=fingerprint,
            )
            for (t_start, t_end, fingerprint) in fingerprints
        ]

    def run_on_audio_file(
        self, audio_file_path: str, offset: float, duration: float
    ) -> List[Fingerprint]:
        """Alternative function in order to run the same extraction from a different payload.
        The argument is only an audio_file_path, which means we do not know where and when it happens, we extract relative timestamps.
        We only return the fingerprint of chunks, which is the part of the chunks that only depend on the content, not the position in time and space.

        `start_sec`/`end_sec` are assumed to already be natural boundaries (e.g.
        the audio was split there when the file was created), so we crop the
        signal to that window and make sure the returned chunks cover it fully.
        """
        y = self.load(audio_file_path, offset=offset, duration=duration)

        fingerprints = self.split_in_chunks_and_build_fingerprints(y=y)

        return [fingerprint for (start, end, fingerprint) in fingerprints]

    def params(self) -> dict:
        return {
            "algo_version": _ALGO_VERSION,
            "sr": self.sr,
            "hop_length": self.hop_length,
            "frame_length": self.frame_length,
            "min_chunk_sec": self.min_chunk_sec,
            "silence_percentile": self.silence_percentile,
            "energy_smoothing_sec": self.energy_smoothing_sec,
            "silence_margin": self.silence_margin,
            "silence_mask_sec": self.silence_mask_sec,
            "seconds_reserved_for_previous_segment": self.seconds_reserved_for_previous_segment,
            "margin_extracted_from_next_segment": self.margin_extracted_from_next_segment,
        }

    def params_hash(self) -> str:
        return make_params_hash(self.params())


def debug_split(job: ChunkCreatorJob, cc: ChunkCreator, verbose: bool = True) -> dict:
    """
    Print a step-by-step explanation of how a single audio window gets split
    into chunks, and return the intermediate values (silence mask, candidate
    peaks, accepted/dropped peaks, final chunks, ...) so a visualizer can
    reuse them without recomputing everything.

    Usage:
        trace = debug_split(job, chunk_creator)
    """

    def log(*args):
        if verbose:
            print(*args)

    log("=" * 60)
    log("DEBUG: audio window splitting analysis")
    log(
        f"  segment: [{job.segment.start_date} -> {job.segment.end_date}]  channel={job.segment.channel}"
    )
    log(f"  audio_file_path: {job.audio_file_path}")
    log(
        f"  has_previous_segment={job.has_previous_segment}  next_audio_file_path={job.next_audio_file_path}"
    )
    log("=" * 60)

    # ── [1] Load audio (+ optional next-segment margin) ─────────────────
    y = cc.load(job.audio_file_path)
    log(f"\n[1] Load audio: {len(y)} samples @ {cc.sr}Hz = {len(y) / cc.sr:.2f}s")

    if job.next_audio_file_path is not None:
        base_len = len(y)
        y = cc._extend_with_next_segment(y, job.next_audio_file_path)
        added_sec = (len(y) - base_len) / cc.sr
        log(
            f"    + extended with next segment margin: +{added_sec:.2f}s "
            f"(target {cc.margin_extracted_from_next_segment:.2f}s)"
        )

    features = cc.extract_features(y)
    duration = len(y) / cc.sr
    log(
        f"    total window duration (with margin): {duration:.2f}s, {len(features['energy'])} frames"
    )

    # ── [2] Silence mask ─────────────────────────────────────────────────
    log("\n[2] Silence mask (local percentile threshold)")
    smoothed_energy, local_threshold = cc._local_silence_threshold(features["energy"])
    silence_mask = cc._compute_silence_mask(features["energy"])
    n_silent = int(silence_mask.sum())
    log(
        f"    silence_percentile={cc.silence_percentile}  window=±{cc.silence_mask_sec}s  "
        f"smoothing={cc.energy_smoothing_sec}s  margin={cc.silence_margin}"
    )
    log(
        f"    silent frames: {n_silent}/{len(silence_mask)} ({100 * n_silent / len(silence_mask):.1f}%)"
    )

    # ── [3] Peak candidates (deepest point of each silence region) ───────
    log("\n[3] Peak candidates (deepest point of each silence region)")
    n_frames = len(silence_mask)
    diff = np.diff(np.concatenate([[0], silence_mask, [0]]))
    starts = np.where(diff > 0.5)[0]
    ends = np.where(diff < -0.5)[0]

    region_candidates = []
    for s, e in zip(starts, ends):
        e = min(e, n_frames)
        region_energy = features["energy"][s:e]
        if len(region_energy) == 0:
            continue
        min_idx = s + int(np.argmin(region_energy))
        region_candidates.append(
            {
                "region": (int(s), int(e)),
                "frame": min_idx,
                "energy": float(features["energy"][min_idx]),
                "time_sec": min_idx / cc._fps,
            }
        )

    log(
        f"    {len(region_candidates)} silence regions found -> {len(region_candidates)} candidate peaks"
    )
    for rc in region_candidates[:10]:
        log(
            f"      region[{rc['region'][0]}:{rc['region'][1]}] -> t={rc['time_sec']:.2f}s energy={rc['energy']:.5f}"
        )
    if len(region_candidates) > 10:
        log(f"      ... and {len(region_candidates) - 10} more")

    # ── [4] Minimum spacing filter ─────────────────────────────────────────
    log(f"\n[4] Minimum spacing filter (min_chunk_sec={cc.min_chunk_sec:.2f}s)")
    min_dist_frames = int(cc.min_chunk_sec * cc._fps)
    sorted_by_energy = sorted(region_candidates, key=lambda c: c["energy"])
    accepted_frames: list[int] = []
    accepted_candidates = []
    dropped_candidates = []
    for rc in sorted_by_energy:
        frame = rc["frame"]
        if all(abs(frame - s) >= min_dist_frames for s in accepted_frames):
            accepted_frames.append(frame)
            accepted_candidates.append(rc)
        else:
            blocker = min(accepted_frames, key=lambda s: abs(frame - s))
            dropped_candidates.append({**rc, "blocked_by_time_sec": blocker / cc._fps})

    accepted_frames_sorted = sorted(accepted_frames)
    peaks_sec = np.array(accepted_frames_sorted) / cc._fps

    log(
        f"    kept {len(accepted_frames_sorted)}/{len(region_candidates)} peaks, "
        f"dropped {len(dropped_candidates)} (too close to a deeper silence)"
    )
    for d in dropped_candidates[:10]:
        log(
            f"      dropped t={d['time_sec']:.2f}s energy={d['energy']:.5f}  (blocked by peak @ {d['blocked_by_time_sec']:.2f}s)"
        )
    if len(dropped_candidates) > 10:
        log(f"      ... and {len(dropped_candidates) - 10} more")

    # ── [5] Drop peaks covered by the previous job ────────────────────────
    kept_peaks_sec = peaks_sec
    if job.has_previous_segment:
        before = len(kept_peaks_sec)
        kept_peaks_sec = kept_peaks_sec[
            kept_peaks_sec >= cc.seconds_reserved_for_previous_segment
        ]
        log(
            f"\n[5] Drop peaks before seconds_reserved_for_previous_segment="
            f"{cc.seconds_reserved_for_previous_segment}s (covered by the previous job)"
        )
        log(f"    kept {len(kept_peaks_sec)}/{before} peaks")
    else:
        log("\n[5] No previous segment: keep all peaks from t=0")

    # ── [6] Build final chunks ─────────────────────────────────────────────
    chunk_start_cutoff_epoch = job.segment.end_date.timestamp()
    if job.next_audio_file_path is not None:
        chunk_start_cutoff_epoch += cc.seconds_reserved_for_previous_segment

    start_epoch = job.segment.start_date.timestamp()

    chunks = [
        Chunk(
            start_sec=round(start_epoch + float(t_start), 2),
            end_sec=round(start_epoch + float(t_end), 2),
            channel=job.segment.channel,
            fingerprint=cc._build_fingerprint(t_start, t_end, y, features),
        )
        for (t_start, t_end) in cc._split_from_peaks(
            peaks_sec, None, chunk_start_cutoff_epoch
        )
    ]

    log(f"\n[6] Final chunks built: {len(chunks)}")
    for c in chunks:
        log(
            f"      [{c.start_sec:.2f} -> {c.end_sec:.2f}]  dur={c.fingerprint.duration_sec:.2f}s  "
            f"energy={c.fingerprint.energy_mean:.4f}  centroid={c.fingerprint.spectral_centroid:.1f}Hz"
        )

    log("\n" + "=" * 60)

    return {
        "y": y,
        "features": features,
        "duration": duration,
        "silence_mask": silence_mask,
        "smoothed_energy": smoothed_energy,
        "local_threshold": local_threshold,
        "region_candidates": region_candidates,
        "accepted_candidates": accepted_candidates,
        "dropped_candidates": dropped_candidates,
        "peaks_sec": peaks_sec,
        "kept_peaks_sec": kept_peaks_sec,
        "chunk_start_cutoff_sec": chunk_start_cutoff_epoch
        - job.segment.start_date.timestamp(),
        "chunks": chunks,
    }
