"""Sub-category, for the ads whose sub-category the dictionary couldn't decide.

Run with ``python -m quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.e4_classify_subcat``.
"""

import logging
import os
from pathlib import Path

from sqlalchemy import Update, or_, select, update

from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.classification_schema import (
    build_subcategory_model,
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

STAGE = "subcat"
DONE_STATUS = "subcat_done"
PARSE_ERROR_STATUS = "subcat_parse_error"
DEFAULT_PROMPT = Path(__file__).resolve().parent / "prompts" / "subcat.yaml"

# The statuses e3_dict_match leaves for this stage, and how the final answer is then produced.
METHOD_BY_STATUS = {
    "dict_tier2_no_kw": "dict_tier2+llm_subcat",
    "dict_tier3_no_kw": "llm_subcat",
    "dict_miss": "llm_subcat",
}
PENDING_STATUSES = tuple(METHOD_BY_STATUS)
AD_COLUMNS = (
    Ad.id,
    Ad.prediction,
    Ad.prediction_status,
    Ad.predicted_sector,
    Ad.transcript,
)


def mark_no_sector_found() -> int:
    stmt = (
        update(Ad)
        .where(Ad.prediction_status.in_(PENDING_STATUSES))
        .where(
            or_(
                Ad.predicted_sector.is_(None),
                Ad.predicted_sector.not_in(list(taxonomy.SUBCATEGORIES)),
            )
        )
        .values(prediction_status="no_sector_found", prediction_method=None)
    )
    with database.engine.begin() as conn:
        return conn.execute(stmt).rowcount


def select_pending(limit: int | None) -> list[str]:
    stmt = (
        select(Ad.id)
        .where(Ad.prediction_status.in_(PENDING_STATUSES))
        .order_by(Ad.first_detection_date.desc())
    )
    if limit:
        stmt = stmt.limit(limit)
    with database.engine.connect() as conn:
        return list(conn.scalars(stmt))


def to_record(ad: dict) -> dict:
    sector = ad["predicted_sector"]
    return {
        "transcript": ad["transcript"],
        "sector_code": sector,
        "sector_label": taxonomy.SECTORS[sector]["sector_label_en"],
        "ad_subject": stage_version_stamp.find_response(
            ad["prediction"], "ad_type"
        ).get("ad_subject"),
        "sub_cat_list": taxonomy.SUBCATEGORY_LISTS[sector],
    }


def _update(
    stamp: dict,
    ad: dict,
    status: str,
    parsed: dict | None = None,
    error: str | None = None,
) -> Update:
    done = status == DONE_STATUS
    entry = {**stamp, "raw_response": parsed, "error": error}
    kept = [e for e in ad["prediction"] if e.get("stage") != STAGE]
    return (
        update(Ad)
        .where(Ad.id == ad["id"])
        .values(
            prediction=[*kept, entry],
            prediction_status=status,
            # How the final answer was produced; none when there is no answer.
            prediction_method=METHOD_BY_STATUS[ad["prediction_status"]]
            if done
            else None,
            predicted_product_category=parsed["sub_category_code"] if done else None,
        )
    )


def to_update(
    stamp: dict, ad: dict, result: llm_runner.InferenceResult
) -> Update | None:
    if result.status == "ok":
        return _update(stamp, ad, DONE_STATUS, result.parsed)
    if result.content:
        parsed, error = llm_runner.rejected_response(result)
        return _update(stamp, ad, PARSE_ERROR_STATUS, parsed, error)
    if llm_runner.is_video_decode_error(result.error):
        return _update(stamp, ad, "video_decode_error", error=result.error)
    return None


def run(
    limit: int | None = None,
    workers: int = 15,
    prompt_path: str | os.PathLike = DEFAULT_PROMPT,
) -> None:
    no_sector = mark_no_sector_found()
    logging.info("[%s] %d ad(s) without a sector: no_sector_found", STAGE, no_sector)

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
    # Sub-category codes belong to one sector, so each sector has its own answer schema.
    schemas = {
        sector: build_subcategory_model(sub_categories)
        for sector, sub_categories in taxonomy.SUBCATEGORIES.items()
    }

    client = llm_runner.new_vlm_client()

    def process_one(ad_id: str) -> llm_runner.StageResult:
        ad = database.load_row(select(*AD_COLUMNS).where(Ad.id == ad_id))
        url = s3.video_url(ad_id)
        if url is None:
            # Left pending rather than no_data: the exportation (s02) may export it again.
            return llm_runner.StageResult(ad_id, "no_video")
        schema = schemas[ad["predicted_sector"]]
        result = llm_runner.call_vlm(
            client, prompt, to_record(ad), schema, video_url=url
        )
        return llm_runner.stage_result(ad_id, result, to_update(stamp, ad, result))

    ad_ids = select_pending(limit)
    logging.info("[%s] %d ad(s) to process", STAGE, len(ad_ids))
    llm_runner.run_stage(ad_ids, process_one, workers, STAGE)


if __name__ == "__main__":
    getLogger()
    run(limit=int(os.environ.get("LIMIT", 0)) or None)
