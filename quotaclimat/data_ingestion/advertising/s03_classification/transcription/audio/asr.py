"""Transcribes the audio of an ad using an ASR model served with an OpenAI-compatible ``/audio/transcriptions`` endpoint.
Run with ``python -m quotaclimat.data_ingestion.advertising.s03_classification.transcription.audio.asr``.
"""

import io
import logging
import os
import wave

import av
import numpy as np
from openai import NOT_GIVEN, APIError, OpenAI
from sqlalchemy import select, update

from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.utils import (
    database,
    llm_runner,
    s3,
)
from quotaclimat.utils.logger import getLogger

STAGE = "asr"
ASR_MODEL_NAME = os.environ.get("ASR_MODEL_NAME")
ASR_API_URL = os.environ.get("ASR_API_URL")
ASR_API_TOKEN = os.environ.get("ASR_API_TOKEN")
# e.g. "fr". Unset, the model detects the language of each ad.
ASR_LANGUAGE = os.environ.get("ASR_LANGUAGE")
SAMPLE_RATE = 16000
# Loudness (16-bit scale) under which an ad is taken as silent: ASR models invent text on silence.
SILENCE_RMS = 30.0


def select_pending(limit: int | None, rerun: bool) -> list[str]:
    stmt = (
        select(Ad.id)
        .where(Ad.fragment_type != "no_data")
        .order_by(Ad.first_detection_date.desc())
    )
    if not rerun:
        stmt = stmt.where(Ad.transcript.is_(None))
    if limit:
        stmt = stmt.limit(limit)
    with database.engine.connect() as conn:
        return list(conn.scalars(stmt))


def new_asr_client() -> OpenAI:
    return OpenAI(
        base_url=ASR_API_URL, api_key=ASR_API_TOKEN, timeout=120, max_retries=1
    )


def audio_samples(video: bytes) -> np.ndarray:
    """The audio track as 16 kHz mono 16-bit samples, empty when there is none."""
    resampler = av.AudioResampler(format="s16", layout="mono", rate=SAMPLE_RATE)
    parts = []
    with av.open(io.BytesIO(video)) as container:
        if container.streams.audio:
            for frame in container.decode(audio=0):
                parts += [f.to_ndarray().reshape(-1) for f in resampler.resample(frame)]
            parts += [f.to_ndarray().reshape(-1) for f in resampler.resample(None)]
    return np.concatenate(parts) if parts else np.zeros(0, dtype=np.int16)


def ad_audio(ad_id: str) -> np.ndarray | None:
    """The audio of the ad's video, or None when there is no readable video."""
    video = s3.video_bytes(ad_id)
    if video is None:
        logging.warning("[%s] %s: no video in the bucket", STAGE, ad_id)
        return None
    try:
        return audio_samples(video)
    except av.FFmpegError as e:
        logging.warning("[%s] %s: %s", STAGE, ad_id, e)
        return None


def is_silent(samples: np.ndarray) -> bool:
    if samples.size == 0:
        return True
    return np.sqrt(np.mean(samples.astype(np.float64) ** 2)) < SILENCE_RMS


def wav_bytes(samples: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(SAMPLE_RATE)
        wav.writeframes(samples.astype("<i2").tobytes())
    return buffer.getvalue()


def transcribe(client: OpenAI, samples: np.ndarray) -> llm_runner.InferenceResult:
    # Long audio is cut by the server (vLLM does it for Whisper-type models): ads are short.
    try:
        response = client.audio.transcriptions.create(
            model=ASR_MODEL_NAME,
            file=("audio.wav", wav_bytes(samples), "audio/wav"),
            language=ASR_LANGUAGE or NOT_GIVEN,
            temperature=0.0,
        )
    except APIError as e:
        return llm_runner.InferenceResult("error", error=f"{type(e).__name__}: {e}")
    return llm_runner.InferenceResult("ok", response.text.strip())


def run(limit: int | None = None, rerun: bool = False, workers: int = 30) -> None:
    client = new_asr_client()

    def process_one(ad_id: str) -> llm_runner.StageResult:
        samples = ad_audio(ad_id)
        if samples is None:
            return llm_runner.StageResult(ad_id, "no_readable_video")
        if is_silent(samples):
            statement = update(Ad).where(Ad.id == ad_id).values(transcript="")
            return llm_runner.StageResult(ad_id, "silent", statement)
        result = transcribe(client, samples)
        if result.status == "error":
            return llm_runner.StageResult(ad_id, "left_pending", error=result.error)
        statement = update(Ad).where(Ad.id == ad_id).values(transcript=result.content)
        return llm_runner.StageResult(ad_id, "ok", statement)

    ad_ids = select_pending(limit, rerun)
    logging.info("[%s] %d ad(s) to transcribe", STAGE, len(ad_ids))
    llm_runner.run_stage(ad_ids, process_one, workers, STAGE)


if __name__ == "__main__":
    getLogger()
    run(
        limit=int(os.environ.get("LIMIT", 0)) or None,
        rerun=os.environ.get("RERUN") == "1",
    )
