"""
Singleton wrapper around PANNs Cnn14 for audio embedding extraction.

Uses panns_inference (PyTorch) to extract:
  - 2048-dim embeddings from the penultimate layer
  - 527-class AudioSet probabilities

The model expects 32kHz mono audio. Resampling from 16kHz is handled here.
"""

import logging
import threading

import librosa
import numpy as np

logger = logging.getLogger(__name__)

PANNS_SR = 32000  # PANNs native sample rate


class AudioEmbeddingModel:
    """Lazy-loading singleton for PANNs Cnn14.

    Thread-safe: the model is loaded once on first use and reused.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._model = None
        return cls._instance

    def _ensure_loaded(self):
        if self._model is not None:
            return
        logger.info("Loading PANNs Cnn14 model (first use)...")
        from panns_inference import AudioTagging

        self._model = AudioTagging(checkpoint_path=None, device="cpu")
        logger.info("PANNs Cnn14 loaded.")

    def _resample_if_needed(self, audio: np.ndarray, sr: int) -> np.ndarray:
        if sr != PANNS_SR:
            return librosa.resample(audio, orig_sr=sr, target_sr=PANNS_SR)
        return audio

    def embed_chunks(
        self,
        audio: np.ndarray,
        sr: int,
        window_sec: float = 1.0,
        hop_sec: float = 0.5,
        batch_size: int = 32,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Extract frame-level embeddings and class probabilities via sliding window.

        Args:
            audio: 1D mono audio array at any sample rate.
            sr: Sample rate of the input audio.
            window_sec: Window duration in seconds.
            hop_sec: Hop between windows in seconds.
            batch_size: Number of windows per forward pass (higher = faster but more RAM).

        Returns:
            embeddings: (n_frames, 2048) float32 array.
            class_probs: (n_frames, 527) float32 array.
        """
        import time

        import torch

        self._ensure_loaded()

        t0 = time.monotonic()
        audio_32k = self._resample_if_needed(audio, sr)

        window_samples = int(window_sec * PANNS_SR)
        hop_samples = int(hop_sec * PANNS_SR)

        n_windows = max(0, (len(audio_32k) - window_samples) // hop_samples + 1)

        if n_windows == 0:
            # Audio shorter than one window — process the whole thing
            logger.info("Audio shorter than one window, processing as single frame")
            padded = np.pad(audio_32k, (0, max(0, window_samples - len(audio_32k))))
            chunk_tensor = torch.from_numpy(padded[None, :]).float()
            with torch.no_grad():
                clipwise_output, embedding = self._model.inference(chunk_tensor)
            return (
                np.array([embedding[0]], dtype=np.float32),
                np.array([clipwise_output[0]], dtype=np.float32),
            )

        audio_duration = len(audio_32k) / PANNS_SR
        n_batches = (n_windows + batch_size - 1) // batch_size
        logger.info(
            f"Extracting embeddings: {n_windows} windows from {audio_duration:.1f}s audio "
            f"({n_batches} batches of {batch_size})"
        )

        all_embeddings = []
        all_class_probs = []

        for batch_idx in range(n_batches):
            batch_start = batch_idx * batch_size
            batch_end = min(batch_start + batch_size, n_windows)

            # Stack windows into (batch, samples) tensor
            windows = np.stack(
                [
                    audio_32k[i * hop_samples : i * hop_samples + window_samples]
                    for i in range(batch_start, batch_end)
                ]
            )
            batch_tensor = torch.from_numpy(windows).float()

            with torch.no_grad():
                clipwise_output, embedding = self._model.inference(batch_tensor)

            all_embeddings.append(embedding)
            all_class_probs.append(clipwise_output)

            # Log progress every 10 batches
            if batch_idx % 10 == 0 or batch_idx == n_batches - 1:
                processed_sec = batch_end * hop_sec
                elapsed = time.monotonic() - t0
                logger.info(
                    f"  Embedding progress: {batch_end}/{n_windows} windows "
                    f"({processed_sec:.0f}s / {audio_duration:.0f}s audio) "
                    f"[{elapsed:.1f}s elapsed]"
                )

        elapsed = time.monotonic() - t0
        logger.info(
            f"Embedding extraction complete: {n_windows} frames in {elapsed:.1f}s "
            f"({n_windows / elapsed:.0f} frames/s)"
        )

        return (
            np.vstack(all_embeddings).astype(np.float32),
            np.vstack(all_class_probs).astype(np.float32),
        )

    def embed_single(self, audio: np.ndarray, sr: int) -> tuple[np.ndarray, np.ndarray]:
        """Extract a single embedding for an entire audio clip.

        Returns:
            embedding: (2048,) float32 array.
            class_probs: (527,) float32 array.
        """
        import torch

        self._ensure_loaded()
        audio_32k = self._resample_if_needed(audio, sr)
        chunk_tensor = torch.from_numpy(audio_32k[None, :]).float()

        with torch.no_grad():
            clipwise_output, embedding = self._model.inference(chunk_tensor)

        return embedding[0], clipwise_output[0]
