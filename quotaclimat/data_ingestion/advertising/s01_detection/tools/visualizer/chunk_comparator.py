"""
chunk_comparator.py
===================
Génère un fichier HTML autonome pour comparer visuellement deux chunks
côte à côte, avec toutes les courbes audio intermédiaires (RMS, centroïde
spectral, ZCR, masque de silence, dissimilarité cosinus, courbe de nouveauté,
spectrogramme + constellation map).

Ajoute ±1 seconde de contexte autour de chaque chunk pour voir les
frontières de découpage.

Usage programmatique :
    from quotaclimat.data_ingestion.advertising.s01_detection.tools.visualizer.chunk_comparator import (
        generate_chunk_comparator,
    )
    from quotaclimat.data_ingestion.advertising.s01_detection.e02_create_chunks import Chunk, ChunkCreator

    html = generate_chunk_comparator(
        chunk_a=chunk_a,           # Chunk
        chunk_b=chunk_b,           # Chunk
        audio_a_path="path/a.mp3", # fichier audio contenant le chunk A
        audio_b_path="path/b.mp3", # fichier audio contenant le chunk B
        segment_a_start_epoch=..., # epoch du début du segment audio A
        segment_b_start_epoch=..., # epoch du début du segment audio B
        chunk_creator=ChunkCreator(),
    )

Dépendances : librosa, numpy, scipy (déjà présentes dans le projet)
"""

import base64
import io
import json
from pathlib import Path

import librosa
import numpy as np
import scipy.io.wavfile
from scipy.ndimage import maximum_filter, maximum_filter1d, uniform_filter1d

from quotaclimat.data_ingestion.advertising.s01_detection.e02_create_chunks import (
    Chunk,
    ChunkCreator,
)

TEMPLATE_PATH = Path(__file__).parent / "chunk_comparator.html"

# Nombre max de points pour la forme d'onde (downsampling pour le HTML)
_MAX_WAVEFORM_POINTS = 2000


def _audio_to_base64(y: np.ndarray, sr: int) -> str:
    """Encode a float32 mono audio array as a base64-encoded WAV (int16)."""
    buf = io.BytesIO()
    y_int16 = np.clip(y * 32767.0, -32768, 32767).astype(np.int16)
    scipy.io.wavfile.write(buf, sr, y_int16)
    return base64.b64encode(buf.getvalue()).decode("ascii")


# Nombre max de bins fréquentiels pour le spectrogramme
_MAX_FREQ_BINS = 128


def _extract_chunk_data(
    chunk: Chunk,
    audio_path: str,
    segment_start_epoch: float,
    cc: ChunkCreator,
    padding_sec: float = 1.0,
) -> dict:
    """
    Charge l'audio autour du chunk (±padding_sec) et extrait toutes
    les courbes intermédiaires du pipeline de segmentation.
    """
    y_full, _ = librosa.load(audio_path, sr=cc.sr, mono=True)
    total_duration = len(y_full) / cc.sr

    # Position du chunk dans le fichier audio
    chunk_start_in_file = chunk.start_sec - segment_start_epoch
    chunk_end_in_file = chunk.end_sec - segment_start_epoch

    # Fenêtre élargie avec padding
    pad_start = max(0.0, chunk_start_in_file - padding_sec)
    pad_end = min(total_duration, chunk_end_in_file + padding_sec)

    # Position du chunk dans la fenêtre paddée
    chunk_rel_start = chunk_start_in_file - pad_start
    chunk_rel_end = chunk_end_in_file - pad_start

    # Extraire l'audio de la fenêtre paddée
    s_start = int(pad_start * cc.sr)
    s_end = int(pad_end * cc.sr)
    y = y_full[s_start:s_end]

    window_duration = len(y) / cc.sr
    fps = cc.sr / cc.hop_length

    # ── Features frame-level ────────────────────────────
    mfcc = librosa.feature.mfcc(
        y=y, sr=cc.sr, n_mfcc=cc.n_mfcc, hop_length=cc.hop_length
    )
    delta = librosa.feature.delta(mfcc)
    energy = librosa.feature.rms(y=y, hop_length=cc.hop_length)[0]
    centroid = librosa.feature.spectral_centroid(
        y=y, sr=cc.sr, hop_length=cc.hop_length
    )[0]
    zcr = librosa.feature.zero_crossing_rate(y, hop_length=cc.hop_length)[0]

    n_frames = energy.shape[0]

    # ── Silence mask ────────────────────────────────────
    silence_threshold = np.percentile(energy, cc.silence_percentile)
    silence_mask = (energy < silence_threshold).astype(float)
    dilation_frames = max(1, int(0.1 * fps))
    silence_mask_dilated = maximum_filter1d(silence_mask, size=dilation_frames * 2 + 1)

    # ── Cosine dissimilarity ────────────────────────────
    centroid_max = centroid.max() if centroid.max() > 0 else 1.0
    stack = np.vstack([mfcc, delta, energy, centroid / centroid_max, zcr])

    kernel = max(4, int(cc.context_sec * fps))
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

    # ── Novelty curve ───────────────────────────────────
    novelty = silence_mask_dilated * cosine_dissim
    smooth_frames = max(3, int(cc.novelty_smooth_sec * fps))
    novelty_smooth = uniform_filter1d(novelty, size=smooth_frames)

    # ── Spectrogram ─────────────────────────────────────
    D = np.abs(librosa.stft(y, n_fft=cc.n_fft, hop_length=cc.hop_length))
    D_db = librosa.amplitude_to_db(D, ref=np.max)
    # Downsample frequency axis
    freq_bins = D_db.shape[0]
    if freq_bins > _MAX_FREQ_BINS:
        indices = np.linspace(0, freq_bins - 1, _MAX_FREQ_BINS, dtype=int)
        D_db = D_db[indices, :]
    # Normaliser entre 0 et 1 pour l'affichage
    d_min, d_max = D_db.min(), D_db.max()
    D_norm = (D_db - d_min) / (d_max - d_min + 1e-8)

    # ── Constellation peaks (dans la fenêtre du chunk uniquement) ──
    chunk_s_start = int(chunk_rel_start * cc.sr)
    chunk_s_end = int(chunk_rel_end * cc.sr)
    y_chunk = y[chunk_s_start:chunk_s_end]
    if len(y_chunk) >= cc.sr * 0.5:
        D_chunk = np.abs(
            librosa.stft(y_chunk, n_fft=cc.n_fft, hop_length=cc.hop_length)
        )
        D_chunk_db = librosa.amplitude_to_db(D_chunk, ref=np.max)
        D_chunk_norm = (D_chunk_db - D_chunk_db.min()) / (
            D_chunk_db.max() - D_chunk_db.min() + 1e-8
        )
        local_max = maximum_filter(D_chunk_norm, size=cc.neighborhood)
        is_peak = (D_chunk_norm == local_max) & (D_chunk_norm > cc.min_amplitude)
        freq_idxs, time_idxs = np.where(is_peak)
        if len(freq_idxs) > 0:
            amplitudes = D_chunk_norm[freq_idxs, time_idxs]
            order = np.argsort(-amplitudes)[: cc.n_peaks]
            # Décaler les time_idxs pour les positionner dans la fenêtre paddée
            chunk_frame_offset = int(chunk_rel_start * fps)
            constellation_peaks = [
                [int(time_idxs[o]) + chunk_frame_offset, int(freq_idxs[o])]
                for o in order
            ]
        else:
            constellation_peaks = []
    else:
        constellation_peaks = []

    # ── Waveform downsamplé ─────────────────────────────
    if len(y) > _MAX_WAVEFORM_POINTS:
        indices = np.linspace(0, len(y) - 1, _MAX_WAVEFORM_POINTS, dtype=int)
        waveform = y[indices].tolist()
    else:
        waveform = y.tolist()

    # ── Time axis pour les courbes frame-level ──────────
    frame_times = (np.arange(n_frames) / fps).tolist()

    return {
        "label": f"Chunk @ {chunk.start_sec:.2f}",
        "absStart": round(chunk.start_sec, 3),
        "absEnd": round(chunk.end_sec, 3),
        "duration": round(chunk.duration_sec, 3),
        "channel": chunk.channel,
        # Métadonnées globales du chunk
        "metadata": {
            "energy_mean": round(chunk.energy_mean, 6),
            "spectral_centroid": round(chunk.spectral_centroid, 2),
            "zcr_mean": round(chunk.zcr_mean, 6),
            "n_peaks": len(chunk.peaks) if chunk.peaks else 0,
            "n_hashes": len(chunk.hashes) if chunk.hashes else 0,
        },
        # Fenêtre d'analyse
        "window": {
            "padStart": round(pad_start, 4),
            "padEnd": round(pad_end, 4),
            "chunkRelStart": round(chunk_rel_start, 4),
            "chunkRelEnd": round(chunk_rel_end, 4),
            "windowDuration": round(window_duration, 4),
        },
        # Courbes
        "frameTimes": [round(t, 4) for t in frame_times],
        "waveform": [round(float(v), 5) for v in waveform],
        "energy": [round(float(v), 6) for v in energy],
        "centroid": [round(float(v), 2) for v in centroid],
        "zcr": [round(float(v), 6) for v in zcr],
        "silenceMask": [round(float(v), 2) for v in silence_mask_dilated],
        "cosineDissim": [round(float(v), 6) for v in cosine_dissim],
        "novelty": [round(float(v), 6) for v in novelty_smooth],
        # Spectrogramme (matrice 2D aplatie + dimensions)
        "spectrogram": {
            "data": [round(float(v), 3) for v in D_norm.flatten()],
            "nFreq": D_norm.shape[0],
            "nTime": D_norm.shape[1],
        },
        "constellationPeaks": constellation_peaks,
        # Audio embarqué (fenêtre paddée, WAV mono int16 en base64)
        "audioBase64": _audio_to_base64(y, cc.sr),
    }


def generate_chunk_comparator(
    chunk_a: Chunk,
    chunk_b: Chunk,
    audio_a_path: str,
    audio_b_path: str,
    segment_a_start_epoch: float,
    segment_b_start_epoch: float,
    chunk_creator: ChunkCreator | None = None,
    padding_sec: float = 1.0,
) -> str:
    """
    Génère un fichier HTML autonome de comparaison de deux chunks.

    Arguments :
        chunk_a, chunk_b           : les deux Chunks à comparer
        audio_a_path, audio_b_path : chemins vers les fichiers audio source
        segment_a_start_epoch      : epoch du début du segment contenant chunk_a
        segment_b_start_epoch      : epoch du début du segment contenant chunk_b
        chunk_creator              : instance de ChunkCreator (pour les paramètres)
        padding_sec                : secondes de contexte avant/après (défaut: 1.0)

    Retourne : chaîne HTML complète
    """
    cc = chunk_creator or ChunkCreator()

    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Template HTML introuvable : {str(TEMPLATE_PATH)}")

    data_a = _extract_chunk_data(
        chunk_a, audio_a_path, segment_a_start_epoch, cc, padding_sec
    )
    data_b = _extract_chunk_data(
        chunk_b, audio_b_path, segment_b_start_epoch, cc, padding_sec
    )

    payload = {
        "chunkA": data_a,
        "chunkB": data_b,
        "params": cc.params(),
        "paddingSec": padding_sec,
    }

    with open(TEMPLATE_PATH, encoding="utf-8") as f:
        html = f.read()

    placeholder = '<script id="embedded-data" type="application/json">null</script>'
    if placeholder not in html:
        raise ValueError(
            "Placeholder introuvable dans le template HTML. "
            "Vérifiez que chunk_comparator.html est intact."
        )

    payload_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    html = html.replace(
        placeholder,
        f'<script id="embedded-data" type="application/json">{payload_str}</script>',
    )

    return html
