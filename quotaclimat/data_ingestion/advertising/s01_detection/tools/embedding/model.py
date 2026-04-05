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
    ) -> tuple[np.ndarray, np.ndarray]:
        """Extract frame-level embeddings and class probabilities via sliding window.

        Args:
            audio: 1D mono audio array at any sample rate.
            sr: Sample rate of the input audio.
            window_sec: Window duration in seconds.
            hop_sec: Hop between windows in seconds.

        Returns:
            embeddings: (n_frames, 2048) float32 array.
            class_probs: (n_frames, 527) float32 array.
        """
        import torch

        self._ensure_loaded()
        audio_32k = self._resample_if_needed(audio, sr)

        window_samples = int(window_sec * PANNS_SR)
        hop_samples = int(hop_sec * PANNS_SR)

        embeddings = []
        class_probs = []

        for start in range(0, len(audio_32k) - window_samples + 1, hop_samples):
            chunk = audio_32k[start : start + window_samples]
            chunk_tensor = torch.from_numpy(chunk[None, :]).float()

            with torch.no_grad():
                clipwise_output, embedding = self._model.inference(chunk_tensor)

            embeddings.append(embedding[0])
            class_probs.append(clipwise_output[0])

        if not embeddings:
            # Audio shorter than one window — process the whole thing
            padded = np.pad(audio_32k, (0, max(0, window_samples - len(audio_32k))))
            chunk_tensor = torch.from_numpy(padded[None, :]).float()
            with torch.no_grad():
                clipwise_output, embedding = self._model.inference(chunk_tensor)
            embeddings.append(embedding[0])
            class_probs.append(clipwise_output[0])

        return np.array(embeddings, dtype=np.float32), np.array(
            class_probs, dtype=np.float32
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
