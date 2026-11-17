"""Brand dictionary lookup for the ads found by e1_classify_ad. No model involved.

Two modes:
  - normal: ads at ``ad_type_done``; whatever ``dictionary.match`` says is written.
  - full rerun (FULL_RERUN=1): once the dictionary changed (brands added, corrected or
    removed), run it again over every ad it already matched, and the new ones. An ad whose dictionary answer
    (method, sector, sub-category, brand) is the same as last time is left as it is.
    Any other is rewritten as on a first match, so e4_classify_subcat and e5_canonicalise_brand pick it up again when the dictionary
    doesn't decide.
    Then run e5_canonicalise_brand with RECANONICALISE=1, so the brands reset here are compared with all the others.

DRY_RUN=1 runs everything, then rolls back the writes.

Run with ``python -m quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.e3_dict_match``, after syncing the dictionary.
"""

import logging
import os
from collections import Counter

import tqdm
from sqlalchemy import Select, select, update

from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data import (
    dictionary,
)
from quotaclimat.data_ingestion.advertising.s03_classification.utils import (
    database,
    stage_version_stamp,
)
from quotaclimat.utils.logger import getLogger

STAGE = "dict_match"
# The statuses an ad can have once the dictionary matched it, whatever e4_classify_subcat made of it.
MATCHED_STATUSES = (
    "dict_tier1",
    "dict_tier2",
    "dict_tier3",
    "dict_tier2_no_kw",
    "dict_tier3_no_kw",
    "dict_miss",
    "subcat_done",
    "subcat_parse_error",
    "no_sector_found",
    "video_decode_error",
    "no_data",
)
# What a full rerun compares with the answer stored last time.
ANSWER_FIELDS = ("method", "sector_code", "subcat_code", "matched_brand")


def select_ads(limit: int | None, full_rerun: bool) -> Select:
    statuses = ("ad_type_done", *MATCHED_STATUSES) if full_rerun else ("ad_type_done",)
    stmt = (
        select(Ad.id, Ad.prediction, Ad.prediction_status, Ad.transcript)
        .where(Ad.prediction_status.in_(statuses))
        .order_by(Ad.first_detection_date.desc())
    )
    if limit:
        stmt = stmt.limit(limit)
    return stmt


def answer(match: dict) -> dict:
    return {field: match.get(field) for field in ANSWER_FIELDS}


def new_values(stamp: dict, ad: dict, full_rerun: bool) -> dict | None:
    """The columns to write, or None to leave the ad as it is."""
    ad_type = stage_version_stamp.find_response(ad["prediction"], "ad_type")
    if ad_type.get("content_type") != "AD":
        return {"prediction_status": "not_ad"}

    match = dictionary.match(
        ad_type.get("brand_name"), ad_type.get("product_name"), ad["transcript"]
    )
    if full_rerun and answer(match) == answer(
        stage_version_stamp.find_entry(ad["prediction"], STAGE)
    ):
        return None

    method = match["method"]
    trusted = method not in ("dict_miss", "dict_tier3_no_kw")
    # A new dictionary answer replaces the old one and the sub-category found from it.
    kept = [e for e in ad["prediction"] if e.get("stage") not in (STAGE, "subcat")]
    return {
        "prediction": [*kept, {**stamp, **match}],
        "prediction_status": method,
        "prediction_method": method,
        "predicted_sector": match["sector_code"]
        if trusted
        else ad_type.get("sector_code"),
        "predicted_product_category": match.get("subcat_code"),
        "predicted_brand": match["matched_brand"] if trusted else None,
    }


def run(
    limit: int | None = None, full_rerun: bool = False, dry_run: bool = False
) -> None:
    stamp = stage_version_stamp.new_stamp(STAGE, dictionary=dictionary.VERSION)
    logging.info(
        "[%s] %s mode, dictionary %s%s",
        STAGE,
        "full rerun" if full_rerun else "normal",
        dictionary.VERSION,
        ", dry run" if dry_run else "",
    )
    counts: Counter[str] = Counter()
    with database.engine.connect() as conn:
        for ad in tqdm.tqdm(
            database.stream_rows(select_ads(limit, full_rerun)), desc=STAGE
        ):
            before = stage_version_stamp.find_entry(ad["prediction"], STAGE).get(
                "method"
            )
            if before is None and ad["prediction_status"] != "ad_type_done":
                continue  # stopped at e1_classify_ad (e.g. its video_decode_error)
            values = new_values(stamp, ad, full_rerun)
            if values is None:
                counts["unchanged"] += 1
                continue
            counts[f"{before or 'new'} -> {values['prediction_status']}"] += 1
            conn.execute(update(Ad).where(Ad.id == ad["id"]).values(**values))
        if not dry_run:
            conn.commit()
    logging.info("[%s] %s", STAGE, dict(counts))


if __name__ == "__main__":
    getLogger()
    run(
        limit=int(os.environ.get("LIMIT", 0)) or None,
        full_rerun=os.environ.get("FULL_RERUN") == "1",
        dry_run=os.environ.get("DRY_RUN") == "1",
    )
