"""The video is split into frames (one a second by default), each frame is read on its own,
and the readings are merged into one transcript in ``Ad.ocr_transcript``. An ad is written
as soon as all its frames are read; a failed request leaves it for the next run.

Run in the sector and brand classification, as ``sector_and_brand.e2_ocr``: after
``e1_classify_ad``, which sets the ``fragment_type='AD'`` read here, and before
``e5_canonicalise_brand``, which reads the OCR text.
"""

import logging
import os

import av
from openai import OpenAI
from sqlalchemy import select, update

from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.transcription.ocr.ocr_merge import (
    transcript,
)
from quotaclimat.data_ingestion.advertising.s03_classification.utils import (
    database,
    frame_splitting,
    llm_runner,
    s3,
)
from quotaclimat.utils.logger import getLogger

STAGE = "ocr"
OCR_MODEL_NAME = os.environ.get("OCR_MODEL_NAME")
OCR_API_URL = os.environ.get("OCR_API_URL")
OCR_API_TOKEN = os.environ.get("OCR_API_TOKEN")
# PaddleOCR-VL expects the image followed by exactly this text, and no system prompt.
OCR_TASK = "OCR:"


def select_pending(limit: int | None, rerun: bool) -> list[str]:
    stmt = (
        select(Ad.id)
        .where(Ad.fragment_type == "AD")
        .order_by(Ad.first_detection_date.desc())
    )
    if not rerun:
        stmt = stmt.where(Ad.ocr_transcript.is_(None))
    if limit:
        stmt = stmt.limit(limit)
    with database.engine.connect() as conn:
        return list(conn.scalars(stmt))


def new_ocr_client() -> OpenAI:
    return OpenAI(
        base_url=OCR_API_URL, api_key=OCR_API_TOKEN, timeout=120, max_retries=1
    )


def frame_images(ad_id: str, fps: float) -> list[str] | None:
    """The frames of the ad's video as images, or None when there is no readable video."""
    video = s3.video_bytes(ad_id)
    if video is None:
        logging.warning("[%s] %s: no video in the bucket", STAGE, ad_id)
        return None
    try:
        return [
            frame_splitting.data_url(frame)
            for _, frame in frame_splitting.split_frames(video, fps)
        ]
    except av.FFmpegError as e:
        logging.warning("[%s] %s: %s", STAGE, ad_id, e)
        return None


def read_frame(client: OpenAI, image: str) -> llm_runner.InferenceResult:
    content = [
        {"type": "image_url", "image_url": {"url": image}},
        {"type": "text", "text": OCR_TASK},
    ]
    return llm_runner.call_llm(
        client,
        [{"role": "user", "content": content}],
        model=OCR_MODEL_NAME,
        max_tokens=768,
        temperature=0.0,
        extra_body={"repetition_penalty": 1.05},
    )


def run(
    limit: int | None = None,
    rerun: bool = False,
    fps: float = 1.0,
    workers: int = 30,
) -> None:
    client = new_ocr_client()

    # A worker reads the frames of its ad one after the other.
    def process_one(ad_id: str) -> llm_runner.StageResult:
        images = frame_images(ad_id, fps)
        if images is None:
            return llm_runner.StageResult(ad_id, "no_readable_video")
        texts = []
        for image in images:  # in time order
            result = read_frame(client, image)
            if result.status == "ok":
                texts.append(result.content)
            elif not result.error.startswith("empty response"):  # empty: no text
                return llm_runner.StageResult(ad_id, "left_pending", error=result.error)
        text = transcript(texts)
        statement = update(Ad).where(Ad.id == ad_id).values(ocr_transcript=text)
        return llm_runner.StageResult(ad_id, "ok", statement)

    ad_ids = select_pending(limit, rerun)
    logging.info("[%s] %d ad(s) to read at %s fps", STAGE, len(ad_ids), fps)
    llm_runner.run_stage(ad_ids, process_one, workers, STAGE)


def main() -> None:
    getLogger()
    run(
        limit=int(os.environ.get("LIMIT", 0)) or None,
        rerun=os.environ.get("RERUN") == "1",
        fps=float(os.environ.get("FPS", 1)),
    )


if __name__ == "__main__":
    main()
