"""Content type, sector + brand + product (for ads only).

Run with ``python -m quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.e1_classify_ad``.
"""

import logging
import os
from pathlib import Path

from sqlalchemy import Update, exists, select, update

from postgres.schemas.advertising.models import Ad, Ad_Occurrence
from quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.classification_schema import (
    build_ad_classification_model,
)
from quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data import (
    taxonomy,
)
from quotaclimat.data_ingestion.advertising.s03_classification.utils import (
    database,
    llm_runner,
    s3,
    stage_version_stamp,
)
from quotaclimat.utils.logger import getLogger

STAGE = "ad_type"
DONE_STATUS = "ad_type_done"
PARSE_ERROR_STATUS = "ad_type_parse_error"
DEFAULT_PROMPT = Path(__file__).resolve().parent / "prompts" / "ad_type.yaml"

CONFIDENCE_MAP = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}


def select_pending(limit: int | None, channel: str | None = None) -> list[str]:
    stmt = (
        select(Ad.id)
        .where(Ad.prediction_status.is_(None))
        .where(Ad.fragment_type != "no_data")
        .order_by(Ad.first_detection_date.desc())
    )
    if channel:
        stmt = stmt.where(
            exists().where(
                Ad_Occurrence.ad_id == Ad.id,
                Ad_Occurrence.channel_name == channel,
                Ad_Occurrence.deleted_at.is_(None),
            )
        )
    if limit:
        stmt = stmt.limit(limit)
    with database.engine.connect() as conn:
        return list(conn.scalars(stmt))


def _confidence(parsed: dict) -> float | None:
    ad = CONFIDENCE_MAP.get(parsed.get("ad_type_confidence"))
    sector = CONFIDENCE_MAP.get(parsed.get("sector_confidence"))
    if ad is None:
        return None
    return float(ad) if sector is None else (ad + sector) / 2


def to_update(
    stamp: dict, ad_id: str, result: llm_runner.InferenceResult
) -> Update | None:
    """The UPDATE storing one result, or None for a transient error left to the next run."""
    error = result.error
    if result.status == "ok":
        parsed = result.parsed
        values = {
            "fragment_type": parsed["content_type"],
            # Only real ads continue to the dictionary match; everything else is terminal.
            "prediction_status": DONE_STATUS
            if parsed["content_type"] == "AD"
            else "not_ad",
            "predicted_sector": parsed["sector_code"],
            "prediction_confidence": _confidence(parsed),
        }
    elif result.content:
        # Invalid or truncated JSON, kept for inspection but never read back as a result.
        parsed, error = llm_runner.rejected_response(result)
        values = {
            "fragment_type": "advertising",
            "prediction_status": PARSE_ERROR_STATUS,
            "predicted_sector": None,
            "prediction_confidence": None,
        }
    elif llm_runner.is_video_decode_error(error):
        parsed = None
        values = {"prediction_status": "video_decode_error"}
    else:
        return None
    entry = {**stamp, "raw_response": parsed, "error": error}
    return update(Ad).where(Ad.id == ad_id).values(prediction=[entry], **values)


def run(
    limit: int | None = None,
    channel: str | None = None,
    workers: int = 15,
    prompt_path: str | os.PathLike = DEFAULT_PROMPT,
) -> None:
    prompt = llm_runner.load_prompt(prompt_path)
    stamp = stage_version_stamp.new_stamp(
        STAGE,
        model=llm_runner.MODEL_NAME,
        sampling=llm_runner.SAMPLING_VERSION,
        prompt=stage_version_stamp.prompt_version(prompt),
        taxonomy=taxonomy.VERSION,
    )
    logging.info(
        "[%s] run_id=%s versions=%s", STAGE, stamp["run_id"], stamp["versions"]
    )
    schema = build_ad_classification_model(taxonomy.SECTORS)
    client = llm_runner.new_vlm_client()

    def process_one(ad_id: str) -> llm_runner.StageResult:
        url = s3.video_url(ad_id)
        if url is None:
            no_data = update(Ad).where(Ad.id == ad_id).values(fragment_type="no_data")
            return llm_runner.StageResult(ad_id, "no_video", no_data)
        ad = database.load_row(select(Ad.transcript).where(Ad.id == ad_id))
        record = {**ad, "sector_list": taxonomy.SECTOR_LIST}
        result = llm_runner.call_vlm(client, prompt, record, schema, video_url=url)
        return llm_runner.stage_result(ad_id, result, to_update(stamp, ad_id, result))

    ad_ids = select_pending(limit, channel)
    logging.info("[%s] %d ad(s) to process", STAGE, len(ad_ids))
    llm_runner.run_stage(ad_ids, process_one, workers, STAGE)


if __name__ == "__main__":
    getLogger()
    run(
        limit=int(os.environ.get("LIMIT", 0)) or None,
        channel=os.environ.get("CHANNEL") or None,
    )
