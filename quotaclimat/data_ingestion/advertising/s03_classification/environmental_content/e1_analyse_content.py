"""Stage 1: the environmental content of each ad, in one pass.
Run with ``python -m quotaclimat.data_ingestion.advertising.s03_classification.environmental_content.e1_analyse_content``.
"""

import logging
import os
from collections.abc import Sequence
from pathlib import Path

from sqlalchemy import Update, select, update

from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.environmental_content.content_schema import (
    EnvironmentalContent,
)
from quotaclimat.data_ingestion.advertising.s03_classification.utils import (
    database,
    llm_runner,
    s3,
    schema_base,
    stage_version_stamp,
)
from quotaclimat.utils.logger import getLogger

STAGE = "environmental_content"
DEFAULT_PROMPT = (
    Path(__file__).resolve().parent / "prompts" / "environmental_content.yaml"
)
MAX_TOKENS = 8000  # the claim and evidence lists are unbounded


def select_pending(
    limit: int | None, sectors: Sequence[str] | None, rerun: bool
) -> list[str]:
    stmt = (
        select(Ad.id)
        .where(Ad.fragment_type == "AD")
        .where(database.IN_POOL)
        .order_by(Ad.first_detection_date.desc())
    )
    if not rerun:
        stmt = stmt.where(Ad.environmental_content_prediction.is_(None))
    if sectors:
        stmt = stmt.where(Ad.predicted_sector.in_(sectors))
    if limit:
        stmt = stmt.limit(limit)
    with database.engine.connect() as conn:
        return list(conn.scalars(stmt))


def _update(
    stamp: dict,
    ad_id: str,
    record: dict | None = None,
    error: str | None = None,
    anomalies: list[str] | None = None,
) -> Update:
    entry = {**stamp, "raw_response": record, "error": error, "anomalies": anomalies}
    return (
        update(Ad).where(Ad.id == ad_id).values(environmental_content_prediction=entry)
    )


def to_update(
    stamp: dict, ad_id: str, result: llm_runner.InferenceResult
) -> Update | None:
    """The UPDATE storing one result, or None for a transient error left to the next run."""
    if result.status == "ok":
        # The answer was validated already: validating it again collects the coercions
        # the schema made (a flag corrected, a claim excluded...).
        with schema_base.coercion_scope() as anomalies:
            EnvironmentalContent.model_validate(llm_runner.parse_json(result.content))
        return _update(stamp, ad_id, result.parsed, anomalies=anomalies)
    if result.content:
        record, error = llm_runner.rejected_response(result)
        return _update(stamp, ad_id, record, error)
    if llm_runner.is_video_decode_error(result.error):
        return _update(stamp, ad_id, error=result.error)
    return None


def run(
    limit: int | None = None,
    sectors: Sequence[str] | None = None,
    rerun: bool = False,
    workers: int = 50,
    prompt_path: str | os.PathLike = DEFAULT_PROMPT,
) -> None:
    prompt = llm_runner.load_prompt(prompt_path)
    stamp = stage_version_stamp.new_stamp(
        STAGE,
        model=llm_runner.MODEL_NAME,
        sampling=llm_runner.SAMPLING_VERSION,
        prompt=stage_version_stamp.prompt_version(prompt),
    )
    logging.info(
        "[%s] run_id=%s versions=%s", STAGE, stamp["run_id"], stamp["versions"]
    )
    client = llm_runner.new_vlm_client(timeout=500)

    def process_one(ad_id: str) -> llm_runner.StageResult:
        url = s3.video_url(ad_id)
        if url is None:
            no_video = _update(stamp, ad_id, error="no video in the bucket")
            return llm_runner.StageResult(ad_id, "no_video", no_video)
        ad = database.load_row(select(Ad.transcript).where(Ad.id == ad_id))
        result = llm_runner.call_vlm(
            client,
            prompt,
            ad,
            EnvironmentalContent,
            video_url=url,
            max_tokens=MAX_TOKENS,
        )
        return llm_runner.stage_result(ad_id, result, to_update(stamp, ad_id, result))

    ad_ids = select_pending(limit, sectors, rerun)
    logging.info("[%s] %d ad(s) to process", STAGE, len(ad_ids))
    llm_runner.run_stage(ad_ids, process_one, workers, STAGE)


if __name__ == "__main__":
    getLogger()
    sectors = os.environ.get("SECTOR", "")
    run(
        limit=int(os.environ.get("LIMIT", 0)) or None,
        sectors=[s.strip() for s in sectors.split(",") if s.strip()],
        rerun=os.environ.get("RERUN") == "1",
    )
