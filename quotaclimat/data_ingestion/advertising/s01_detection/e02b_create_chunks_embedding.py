"""
Détection de ruptures dans un flux audio via embeddings audio pré-entraînés
============================================================================
Alternative à e02_create_chunks.py qui utilise PANNs Cnn14 pour détecter
les transitions sémantiques (parole→musique, contenu→jingle) en plus
des silences.

Produit des EmbeddingChunk avec un vecteur de 2048 dimensions par segment,
au lieu de constellation maps.
"""

import logging
from typing import List

import librosa
import numpy as np
from scipy.ndimage import maximum_filter1d

from .e00_partition_window import Segment
from .tools.common_objects import EmbeddingChunk, EmbeddingFingerprint
from .tools.embedding.model import AudioEmbeddingModel
from .tools.fingerprint.pairs import make_params_hash

logger = logging.getLogger(__name__)

# AudioSet label index → name mapping from panns_inference
_LABELS = None


def _get_labels():
    global _LABELS
    if _LABELS is None:
        from panns_inference import labels

        _LABELS = labels
    return _LABELS


class EmbeddingChunkCreator:
    """
    Segmentation audio par embeddings PANNs + détection de silence.

    Stratégie :
      1. Extraire les embeddings PANNs frame par frame (fenêtre glissante)
      2. Calculer une courbe de nouveauté par dissimilarité cosinus sur embeddings
      3. Combiner avec un masque de silence (énergie)
      4. Détecter les pics de la courbe composite → frontières de segments
      5. Pour chaque segment : mean-pooling des embeddings + descripteurs acoustiques
    """

    def __init__(
        self,
        sr: int = 16000,
        embedding_window_sec: float = 1.0,
        embedding_hop_sec: float = 0.5,
        novelty_context_sec: float = 2.0,
        novelty_smooth_sec: float = 0.5,
        min_chunk_sec: float = 1.0,
        silence_percentile: float = 5.0,
        novelty_weight: float = 0.6,
        silence_weight: float = 0.4,
        hop_length: int = 1024,
        top_k_labels: int = 3,
    ):
        self.sr = sr
        self.embedding_window_sec = embedding_window_sec
        self.embedding_hop_sec = embedding_hop_sec
        self.novelty_context_sec = novelty_context_sec
        self.novelty_smooth_sec = novelty_smooth_sec
        self.min_chunk_sec = min_chunk_sec
        self.silence_percentile = silence_percentile
        self.novelty_weight = novelty_weight
        self.silence_weight = silence_weight
        self.hop_length = hop_length
        self.top_k_labels = top_k_labels
        self._fps = sr / hop_length
        self._model = AudioEmbeddingModel()

    def load(self, path: str) -> np.ndarray:
        y, _ = librosa.load(path, sr=self.sr, mono=True)
        return y

    def _compute_embeddings(
        self, y: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Compute frame-level embeddings and class probabilities.

        Returns:
            embeddings: (n_frames, 2048)
            class_probs: (n_frames, 527)
            frame_times: (n_frames,) center time of each frame in seconds
        """
        embeddings, class_probs = self._model.embed_chunks(
            y,
            sr=self.sr,
            window_sec=self.embedding_window_sec,
            hop_sec=self.embedding_hop_sec,
        )
        # Frame times: center of each window
        frame_times = np.arange(len(embeddings)) * self.embedding_hop_sec + (
            self.embedding_window_sec / 2
        )
        return embeddings, class_probs, frame_times

    def _compute_embedding_novelty(
        self, embeddings: np.ndarray, frame_times: np.ndarray
    ) -> np.ndarray:
        """Cosine dissimilarity between past and future context windows on embeddings.

        Returns a novelty curve aligned with frame_times.
        """
        n_frames = len(embeddings)
        context_frames = max(
            1, int(self.novelty_context_sec / self.embedding_hop_sec)
        )

        # L2-normalize embeddings
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True) + 1e-8
        emb_normed = embeddings / norms

        novelty = np.zeros(n_frames)
        for i in range(context_frames, n_frames - context_frames):
            past = emb_normed[i - context_frames : i].mean(axis=0)
            future = emb_normed[i : i + context_frames].mean(axis=0)
            cos_sim = np.dot(past, future) / (
                np.linalg.norm(past) * np.linalg.norm(future) + 1e-8
            )
            novelty[i] = 1.0 - cos_sim

        # Gaussian smoothing
        if self.novelty_smooth_sec > 0:
            smooth_frames = max(
                1, int(self.novelty_smooth_sec / self.embedding_hop_sec)
            )
            from scipy.ndimage import gaussian_filter1d

            novelty = gaussian_filter1d(novelty, sigma=smooth_frames)

        # Normalize to [0, 1]
        if novelty.max() > 0:
            novelty = novelty / novelty.max()

        return novelty

    def _compute_silence_score(self, y: np.ndarray, duration_sec: float) -> np.ndarray:
        """Energy-based silence score aligned with embedding frame times.

        Returns an inverted energy curve (high = silence) at embedding frame rate.
        """
        energy = librosa.feature.rms(y=y, hop_length=self.hop_length)[0]
        silence_threshold = np.percentile(energy, self.silence_percentile)
        silence_mask = (energy < silence_threshold).astype(float)

        # Dilate silence edges
        dilation_frames = max(1, int(0.1 * self._fps))
        silence_mask = maximum_filter1d(silence_mask, size=dilation_frames * 2 + 1)

        # Resample to embedding frame rate
        n_emb_frames = int(duration_sec / self.embedding_hop_sec)
        if n_emb_frames <= 0:
            return np.zeros(1)

        # Map embedding frame indices to energy frame indices
        emb_frame_times = (
            np.arange(n_emb_frames) * self.embedding_hop_sec
            + self.embedding_window_sec / 2
        )
        energy_frame_times = np.arange(len(energy)) / self._fps
        # Nearest-neighbor interpolation
        indices = np.searchsorted(energy_frame_times, emb_frame_times, side="right") - 1
        indices = np.clip(indices, 0, len(silence_mask) - 1)
        return silence_mask[indices]

    def _detect_boundaries(
        self, composite: np.ndarray, frame_times: np.ndarray
    ) -> np.ndarray:
        """Peak-pick on composite novelty curve, enforce min_chunk_sec spacing.

        Returns boundary times in seconds.
        """
        # Find local maxima
        from scipy.signal import find_peaks

        # Minimum distance between peaks in frames
        min_dist_frames = max(1, int(self.min_chunk_sec / self.embedding_hop_sec))
        peaks, properties = find_peaks(
            composite, distance=min_dist_frames, height=0.05
        )

        if len(peaks) == 0:
            return np.array([])

        # Sort by height (strongest boundaries first) for greedy spacing
        heights = composite[peaks]
        order = np.argsort(-heights)
        peaks_sorted = peaks[order]

        selected = []
        for p in peaks_sorted:
            t = frame_times[p] if p < len(frame_times) else frame_times[-1]
            if all(abs(t - s) >= self.min_chunk_sec for s in selected):
                selected.append(t)

        selected.sort()
        return np.array(selected)

    def _get_top_labels(self, class_probs: np.ndarray) -> list:
        """Get top-k AudioSet label names from mean class probabilities."""
        labels_module = _get_labels()
        top_indices = np.argsort(-class_probs)[: self.top_k_labels]
        return [labels_module.ix_to_lb[int(idx)] for idx in top_indices]

    def build_chunks(
        self,
        boundaries_sec: np.ndarray,
        embeddings: np.ndarray,
        class_probs: np.ndarray,
        frame_times: np.ndarray,
        y: np.ndarray,
        start_epoch: float,
        end_epoch: float,
        channel: str,
    ) -> List[EmbeddingChunk]:
        """Build EmbeddingChunks with mean-pooled embeddings and basic features."""
        duration = len(y) / self.sr
        # Add start (0) and end boundaries
        all_boundaries = np.concatenate([[0.0], boundaries_sec, [duration]])
        all_boundaries = np.unique(np.clip(all_boundaries, 0, duration))

        chunks = []
        for i in range(len(all_boundaries) - 1):
            t_start = all_boundaries[i]
            t_end = all_boundaries[i + 1]
            dur = t_end - t_start

            if dur < 0.1:  # Skip very tiny segments
                continue

            abs_start = start_epoch + float(t_start)
            if abs_start > end_epoch:
                continue

            # Find embedding frames within this chunk
            mask = (frame_times >= t_start) & (frame_times < t_end)
            if not mask.any():
                # If no frame center falls in this chunk, use the nearest
                nearest = np.argmin(np.abs(frame_times - (t_start + t_end) / 2))
                mask[nearest] = True

            chunk_embeddings = embeddings[mask]
            chunk_class_probs = class_probs[mask]

            # Mean-pool embedding
            mean_embedding = chunk_embeddings.mean(axis=0)
            mean_class_probs = chunk_class_probs.mean(axis=0)

            # Basic acoustic features from raw audio
            s_start = int(t_start * self.sr)
            s_end = int(t_end * self.sr)
            y_seg = y[s_start:s_end]

            if len(y_seg) < self.hop_length:
                continue

            energy = librosa.feature.rms(y=y_seg, hop_length=self.hop_length)[0]
            centroid = librosa.feature.spectral_centroid(
                y=y_seg, sr=self.sr, hop_length=self.hop_length
            )[0]
            zcr = librosa.feature.zero_crossing_rate(
                y_seg, hop_length=self.hop_length
            )[0]

            e = float(np.mean(energy))
            c = float(np.mean(centroid))
            z = float(np.mean(zcr))

            semantic_labels = self._get_top_labels(mean_class_probs)

            chunks.append(
                EmbeddingChunk(
                    start_sec=round(abs_start, 2),
                    end_sec=round(start_epoch + float(t_end), 2),
                    channel=channel,
                    fingerprint=EmbeddingFingerprint(
                        duration_sec=round(float(dur), 2),
                        energy_mean=round(e, 6),
                        spectral_centroid=round(c, 2),
                        zcr_mean=round(z, 6),
                        embedding=mean_embedding.tolist(),
                        semantic_labels=semantic_labels,
                    ),
                )
            )

        return chunks

    def run(self, segment: Segment, audio_file_path: str) -> List[EmbeddingChunk]:
        logger.info(f"[e02b] Processing {segment.channel} {segment.identifier}")

        y = self.load(audio_file_path)
        duration = len(y) / self.sr
        logger.info(f"[e02b] Audio loaded: {duration:.1f}s at {self.sr}Hz")

        # Step 1: compute frame-level embeddings
        embeddings, class_probs, frame_times = self._compute_embeddings(y)
        logger.info(f"[e02b] Embeddings computed: {len(embeddings)} frames")

        # Step 2: embedding-based novelty curve
        novelty = self._compute_embedding_novelty(embeddings, frame_times)
        logger.info("[e02b] Novelty curve computed")

        # Step 3: silence score
        silence = self._compute_silence_score(y, duration)
        logger.info("[e02b] Silence score computed")

        # Align lengths (silence may differ by 1 frame due to rounding)
        min_len = min(len(novelty), len(silence))
        novelty = novelty[:min_len]
        silence = silence[:min_len]
        frame_times_aligned = frame_times[:min_len]

        # Step 4: composite score
        composite = (
            self.novelty_weight * novelty + self.silence_weight * silence
        )

        # Step 5: detect boundaries
        boundaries_sec = self._detect_boundaries(composite, frame_times_aligned)
        logger.info(f"[e02b] Boundaries detected: {len(boundaries_sec)} boundaries")

        # Step 6: build chunks
        chunks = self.build_chunks(
            boundaries_sec,
            embeddings,
            class_probs,
            frame_times,
            y,
            segment.start_date.timestamp(),
            segment.end_date.timestamp(),
            channel=segment.channel,
        )
        logger.info(f"[e02b] Built {len(chunks)} chunks from {segment.identifier}")
        return chunks

    def params(self) -> dict:
        return {
            "strategy": "embedding",
            "sr": self.sr,
            "embedding_window_sec": self.embedding_window_sec,
            "embedding_hop_sec": self.embedding_hop_sec,
            "novelty_context_sec": self.novelty_context_sec,
            "novelty_smooth_sec": self.novelty_smooth_sec,
            "min_chunk_sec": self.min_chunk_sec,
            "silence_percentile": self.silence_percentile,
            "novelty_weight": self.novelty_weight,
            "silence_weight": self.silence_weight,
            "hop_length": self.hop_length,
            "top_k_labels": self.top_k_labels,
        }

    def params_hash(self) -> str:
        return make_params_hash(self.params())
