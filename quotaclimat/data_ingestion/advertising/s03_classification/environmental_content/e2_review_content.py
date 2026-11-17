"""Stage 2: a second model reviews stage 1's findings, and every ad gets its final
environmental content record.

Two columns of ``ad`` are written, never stage 1's own:
  - ``environmental_content_review``: the review. Its ``raw_response`` holds ``reviewed_run_id``
    (the stage-1 run reviewed), ``input`` (that run's record, which the verdict ids point
    into) and ``review`` (the verdicts).
  - ``environmental_content_final``: the record to use downstream, the same shape as stage 1's.
    ``source`` says where it comes from: ``environmental_content_review`` (stage 1's record with
    the verdicts applied) or ``environmental_content`` (stage 1's record as is: nothing
    to review, or the review failed or hasn't run yet).
"""

import json
import logging
import os
from collections.abc import Sequence
from pathlib import Path

from sqlalchemy import Update, bindparam, select, update

from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.environmental_content.review_schema import (
    ContentReview,
    apply_review,
    evidence_items,
)
from quotaclimat.data_ingestion.advertising.s03_classification.utils import (
    database,
    llm_runner,
    schema_base,
    stage_version_stamp,
)
from quotaclimat.utils.logger import getLogger

STAGE = "environmental_content_review"
DEFAULT_PROMPT = Path(__file__).resolve().parent / "prompts" / "content_review.yaml"

# The claim fields the reviewer sees.
CLAIM_VIEW = (
    "quote",
    "assertion_words",
    "environmental_object",
    "benefit_predicate",
    "claim_types",
    "channel",
    "subject",
)
AD_COLUMNS = (
    Ad.id,
    Ad.predicted_brand,
    Ad.transcript,
    Ad.ocr_transcript,
    Ad.environmental_content_prediction,
    Ad.environmental_content_review,
)
FINAL_UPDATE = (
    update(Ad)
    .where(Ad.id == bindparam("b_id"))
    .values(environmental_content_final=bindparam("b_final"))
)


def usable_review(prediction: dict, review: dict | None) -> dict | None:
    if not review or review.get("error") or not review.get("raw_response"):
        return None
    if review["raw_response"].get("reviewed_run_id") != prediction.get("run_id"):
        return None
    return review


def final_entry(prediction: dict, review: dict | None = None) -> dict:
    if not review:
        return {
            **prediction,
            "source": "environmental_content",
            "prediction_run_id": prediction.get("run_id"),
        }
    reviewed = review["raw_response"]
    with schema_base.coercion_scope() as anomalies:
        corrected = apply_review(reviewed["input"], reviewed["review"])
    return {
        **review,
        "raw_response": corrected,
        "anomalies": (review.get("anomalies") or []) + anomalies,
        "source": STAGE,
        "prediction_run_id": reviewed["reviewed_run_id"],
    }


def needs_review(ad: dict, prompt: str, rerun: bool) -> bool:
    prediction = ad["environmental_content_prediction"]
    record = prediction.get("raw_response")
    if prediction.get("error") or not record:
        return False
    if not (record.get("I3_claims") or evidence_items(record)):
        return False
    review = usable_review(prediction, ad["environmental_content_review"])
    return rerun or not review or review["versions"].get("prompt") != prompt


def refresh_finals(
    sectors: Sequence[str] | None, prompt: str, rerun: bool
) -> list[str]:
    """Writes the final record of the ads whose final changed, and returns the ads to review.

    The ads are read, and their finals written, a chunk at a time: neither the memory used
    nor a transaction grows with the table.
    """
    stmt = (
        select(*AD_COLUMNS, Ad.environmental_content_final)
        .where(Ad.environmental_content_prediction.is_not(None))
        .order_by(Ad.first_detection_date.desc())
    )
    if sectors:
        stmt = stmt.where(Ad.predicted_sector.in_(sectors))
    to_review = []

    def changed_finals():
        for ad in database.stream_rows(stmt):
            prediction = ad["environmental_content_prediction"]
            review = usable_review(prediction, ad["environmental_content_review"])
            final = final_entry(prediction, review)
            if final != ad["environmental_content_final"]:
                yield {"b_id": ad["id"], "b_final": final}
            if needs_review(ad, prompt, rerun):
                to_review.append(ad["id"])

    written = database.save_in_chunks(FINAL_UPDATE, changed_finals())
    logging.info("[%s] %d final record(s) written", STAGE, written)
    return to_review


def _numbered(items: list[dict]) -> str:
    if not items:
        return "None."
    numbered = [{"id": i, **item} for i, item in enumerate(items, 1)]
    return json.dumps(numbered, ensure_ascii=False, indent=1)


def to_record(ad: dict) -> dict:
    record = ad["environmental_content_prediction"]["raw_response"]
    setting = record.get("I1_setting") or {}
    visuals = {
        key: setting.get(key)
        for key in (
            "setting_notes",
            "nature_markers",
            "green_dominant_in_typography",
            "green_dominant_in_graphic_elements",
            "green_dominant_in_product_or_packaging",
        )
    }
    claims = [{k: c.get(k) for k in CLAIM_VIEW} for c in record.get("I3_claims") or []]
    evidence = [
        {"marker": kind, **{k: item.get(k) for k in ("channel", "text", "note")}}
        for kind, item in evidence_items(record)
    ]
    return {
        "brand": ad["predicted_brand"],
        "transcript": ad["transcript"],
        "ocr_text": ad["ocr_transcript"],
        "visuals": json.dumps(visuals, ensure_ascii=False, indent=1),
        "claims": _numbered(claims),
        "evidence": _numbered(evidence),
        "impression": json.dumps(
            record.get("I4_overall_impression") or {}, ensure_ascii=False
        ),
    }


def to_update(
    stamp: dict, ad: dict, result: llm_runner.InferenceResult
) -> Update | None:
    prediction = ad["environmental_content_prediction"]
    if result.status == "ok":
        with schema_base.coercion_scope() as anomalies:
            ContentReview.model_validate(llm_runner.parse_json(result.content))
        reviewed = {
            "reviewed_run_id": prediction.get("run_id"),
            "input": prediction["raw_response"],
            "review": result.parsed,
        }
        review = {
            **stamp,
            "raw_response": reviewed,
            "error": None,
            "anomalies": anomalies,
        }
    elif result.content:
        parsed, error = llm_runner.rejected_response(result)
        review = {**stamp, "raw_response": parsed, "error": error, "anomalies": None}
    else:
        return None
    return (
        update(Ad)
        .where(Ad.id == ad["id"])
        .values(
            environmental_content_review=review,
            environmental_content_final=final_entry(
                prediction, usable_review(prediction, review)
            ),
        )
    )


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
    ad_ids = refresh_finals(sectors, stamp["versions"]["prompt"], rerun)[:limit]
    client = llm_runner.new_vlm_client(timeout=350)

    def process_one(ad_id: str) -> llm_runner.StageResult:
        ad = database.load_row(select(*AD_COLUMNS).where(Ad.id == ad_id))
        result = llm_runner.call_vlm(client, prompt, to_record(ad), ContentReview)
        return llm_runner.stage_result(ad_id, result, to_update(stamp, ad, result))

    logging.info("[%s] %d ad(s) to review", STAGE, len(ad_ids))
    llm_runner.run_stage(ad_ids, process_one, workers, STAGE)


if __name__ == "__main__":
    getLogger()
    sectors = os.environ.get("SECTOR", "")
    run(
        limit=int(os.environ.get("LIMIT", 0)) or None,
        sectors=[s.strip() for s in sectors.split(",") if s.strip()],
        rerun=os.environ.get("RERUN") == "1",
    )
