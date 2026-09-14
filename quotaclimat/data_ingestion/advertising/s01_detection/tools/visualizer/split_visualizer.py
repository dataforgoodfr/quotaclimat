"""
split_visualizer.py
====================
Génère un fichier HTML autonome pour visualiser, étape par étape, comment
deux fenêtres audio sont découpées en chunks par ChunkCreator (e02).

Affiche côte à côte, pour chaque fenêtre : forme d'onde + zones de chunks,
énergie (RMS) + seuil de silence local, masque de silence, spectrogramme
avec les frontières retenues, et la liste des pics candidats (acceptés /
rejetés par le filtre d'espacement minimum) ainsi que des chunks finaux.

Usage programmatique :
    from quotaclimat.data_ingestion.advertising.s01_detection.tools.visualizer.split_visualizer import (
        generate_split_visualizer,
    )
    from quotaclimat.data_ingestion.advertising.s01_detection.e02_create_chunks import (
        ChunkCreatorJob,
    )
    from quotaclimat.data_ingestion.advertising.s01_detection.processor import chunk_creator
    from quotaclimat.data_ingestion.advertising.s01_detection.tools.segments import Segment

    html = generate_split_visualizer(
        job_a=ChunkCreatorJob(segment=segment_a, audio_file_path="a.mp3", has_previous_segment=False),
        job_b=ChunkCreatorJob(segment=segment_b, audio_file_path="b.mp3", has_previous_segment=False),
        chunk_creator=chunk_creator,
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
from scipy.ndimage import maximum_filter

from quotaclimat.data_ingestion.advertising.s01_detection.e02_create_chunks import (
    ChunkCreator,
    ChunkCreatorJob,
    debug_split,
)

TEMPLATE_PATH = Path(__file__).parent / "split_visualizer.html"

# Nombre max de points pour la forme d'onde (downsampling pour le HTML)
_MAX_WAVEFORM_POINTS = 4000
# Nombre max de bins fréquentiels pour le spectrogramme
_MAX_FREQ_BINS = 128


def _audio_to_base64(y: np.ndarray, sr: int) -> str:
    """Encode a float32 mono audio array as a base64-encoded WAV (int16)."""
    buf = io.BytesIO()
    y_int16 = np.clip(y * 32767.0, -32768, 32767).astype(np.int16)
    scipy.io.wavfile.write(buf, sr, y_int16)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _extract_window_data(
    job: ChunkCreatorJob,
    cc: ChunkCreator,
    label: str,
    max_audio_sec: float | None,
    focus_epoch: float | None,
    zoom_sec: float,
) -> dict:
    """
    Runs the real splitting pipeline (via debug_split) on one audio window
    and packages every intermediate step into a JSON-serialisable payload.

    If `focus_epoch` is given, every rendered curve/waveform/spectrogram/audio
    is cropped to the [focus_epoch - zoom_sec, focus_epoch + zoom_sec] window
    (clamped to the job's own window) and re-based so that crop start = t=0,
    to "zoom in" on the timestamp of interest. Candidate peaks and chunks are
    kept in full (for table context) but their times are shifted the same way,
    so a marker line at the focus timestamp lines up with everything else.
    Falls back to the old behaviour (optionally truncated to `max_audio_sec`
    from the window's start) when `focus_epoch` is None.
    """
    trace = debug_split(job, cc, verbose=False)

    y = trace["y"]
    features = trace["features"]
    duration = trace["duration"]
    fps = cc._fps
    n_frames = len(features["energy"])

    start_epoch = job.segment.start_date.timestamp()

    if focus_epoch is not None:
        focus_sec = focus_epoch - start_epoch
        crop_start = max(0.0, focus_sec - zoom_sec)
        crop_end = min(duration, focus_sec + zoom_sec)
    else:
        focus_sec = None
        crop_start = 0.0
        crop_end = duration if max_audio_sec is None else min(duration, max_audio_sec)

    def shift(t: float) -> float:
        return t - crop_start

    # ── Frame-level curves cropped to the display window ────────────────
    f_start = int(crop_start * fps)
    f_end = min(n_frames, int(np.ceil(crop_end * fps)))
    frame_times = [round(shift(i / fps), 4) for i in range(f_start, f_end)]
    energy_crop = features["energy"][f_start:f_end]
    local_threshold_crop = trace["local_threshold"][f_start:f_end]
    silence_mask_crop = trace["silence_mask"][f_start:f_end]

    # ── Audio/spectrogram/waveform cropped to the same display window ───
    fp = cc.fingerprinter
    s_start = int(crop_start * cc.sr)
    s_end = min(len(y), int(crop_end * cc.sr))
    audio_crop = y[s_start:s_end]

    D = np.abs(librosa.stft(audio_crop, n_fft=fp.n_fft, hop_length=cc.hop_length))
    D_db = librosa.amplitude_to_db(D, ref=np.max)
    freq_bins = D_db.shape[0]
    if freq_bins > _MAX_FREQ_BINS:
        indices = np.linspace(0, freq_bins - 1, _MAX_FREQ_BINS, dtype=int)
        D_db = D_db[indices, :]
    d_min, d_max = D_db.min(), D_db.max()
    D_norm = (D_db - d_min) / (d_max - d_min + 1e-8)

    if len(audio_crop) > _MAX_WAVEFORM_POINTS:
        indices = np.linspace(0, len(audio_crop) - 1, _MAX_WAVEFORM_POINTS, dtype=int)
        waveform = audio_crop[indices].tolist()
    else:
        waveform = audio_crop.tolist()
    waveform_duration = len(audio_crop) / cc.sr

    chunks_payload = [
        {
            "startSec": round(shift(c.start_sec - start_epoch), 3),
            "endSec": round(shift(c.end_sec - start_epoch), 3),
            "absStart": round(c.start_sec, 3),
            "absEnd": round(c.end_sec, 3),
            "duration": round(c.fingerprint.duration_sec, 3),
            "energyMean": round(c.fingerprint.energy_mean, 6),
            "spectralCentroid": round(c.fingerprint.spectral_centroid, 2),
            "zcrMean": round(c.fingerprint.zcr_mean, 6),
            "nPeaks": len(c.fingerprint.peaks) if c.fingerprint.peaks else 0,
            "nPairs": len(c.fingerprint.pairs) if c.fingerprint.pairs else 0,
        }
        for c in trace["chunks"]
    ]

    region_candidates_payload = [
        {
            "timeSec": round(shift(rc["time_sec"]), 4),
            "energy": round(rc["energy"], 6),
            "accepted": rc in trace["accepted_candidates"],
        }
        for rc in trace["region_candidates"]
    ]

    dropped_payload = [
        {
            "timeSec": round(shift(d["time_sec"]), 4),
            "energy": round(d["energy"], 6),
            "blockedByTimeSec": round(shift(d["blocked_by_time_sec"]), 4),
        }
        for d in trace["dropped_candidates"]
    ]

    return {
        "label": label,
        "channel": job.segment.channel,
        "segmentStart": job.segment.start_date.isoformat(),
        "segmentEnd": job.segment.end_date.isoformat(),
        "startEpoch": round(start_epoch, 3),
        "hasPreviousSegment": job.has_previous_segment,
        "hasNextSegment": job.next_audio_file_path is not None,
        "duration": round(duration, 3),
        "reservedSec": round(shift(cc.seconds_reserved_for_previous_segment), 3),
        "chunkStartCutoffSec": round(shift(trace["chunk_start_cutoff_sec"]), 3),
        "focusSec": round(shift(focus_sec), 3) if focus_sec is not None else None,
        "frameTimes": frame_times,
        "energy": [round(float(v), 6) for v in energy_crop],
        "localThreshold": [round(float(v), 6) for v in local_threshold_crop],
        "silenceMask": [round(float(v), 2) for v in silence_mask_crop],
        "regionCandidates": region_candidates_payload,
        "droppedCandidates": dropped_payload,
        "peaksSec": [round(shift(float(p)), 4) for p in trace["peaks_sec"]],
        "keptPeaksSec": [round(shift(float(p)), 4) for p in trace["kept_peaks_sec"]],
        "chunks": chunks_payload,
        "waveform": [round(float(v), 5) for v in waveform],
        "waveformDuration": round(waveform_duration, 4),
        "spectrogram": {
            "data": [round(float(v), 3) for v in D_norm.flatten()],
            "nFreq": D_norm.shape[0],
            "nTime": D_norm.shape[1],
        },
        "audioBase64": _audio_to_base64(audio_crop, cc.sr),
    }


def generate_split_visualizer(
    job_a: ChunkCreatorJob,
    job_b: ChunkCreatorJob,
    chunk_creator: ChunkCreator,
    label_a: str = "Window A",
    label_b: str = "Window B",
    max_audio_sec: float | None = 90.0,
    focus_epoch_a: float | None = None,
    focus_epoch_b: float | None = None,
    zoom_sec: float = 10.0,
) -> str:
    """
    Génère un fichier HTML autonome visualisant le découpage en chunks
    (e02_create_chunks) de deux fenêtres audio, côte à côte.

    Arguments :
        job_a, job_b     : les deux ChunkCreatorJob à analyser (mêmes objets
                            que ceux consommés par ChunkCreator.run())
        chunk_creator    : instance de ChunkCreator utilisée pour le découpage
        label_a, label_b : libellés affichés pour chaque fenêtre
        max_audio_sec    : tronque l'audio/spectrogramme affiché à N secondes
                            pour garder le fichier HTML léger (ignoré quand
                            focus_epoch_a/b est fourni ; None = pas de troncature)
        focus_epoch_a    : epoch (même unité que job_a.segment.start_date) sur
                            lequel zoomer pour la fenêtre A — typiquement le
                            timestamp exact que vous investiguez. Quand fourni,
                            tout (courbes, spectrogramme, audio) est recadré
                            sur [focus_epoch_a - zoom_sec, focus_epoch_a + zoom_sec]
        focus_epoch_b    : idem pour la fenêtre B
        zoom_sec         : demi-largeur (en secondes) de la fenêtre de zoom
                            autour de focus_epoch_a/b (défaut : 10s avant/après)

    Retourne : chaîne HTML complète
    """
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Template HTML introuvable : {str(TEMPLATE_PATH)}")

    data_a = _extract_window_data(job_a, chunk_creator, label_a, max_audio_sec, focus_epoch_a, zoom_sec)
    data_b = _extract_window_data(job_b, chunk_creator, label_b, max_audio_sec, focus_epoch_b, zoom_sec)

    payload = {
        "windowA": data_a,
        "windowB": data_b,
        "params": chunk_creator.params(),
    }

    with open(TEMPLATE_PATH, encoding="utf-8") as f:
        html = f.read()

    placeholder = '<script id="embedded-data" type="application/json">null</script>'
    if placeholder not in html:
        raise ValueError(
            "Placeholder introuvable dans le template HTML. "
            "Vérifiez que split_visualizer.html est intact."
        )

    payload_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    html = html.replace(
        placeholder,
        f'<script id="embedded-data" type="application/json">{payload_str}</script>',
    )

    return html
