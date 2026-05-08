"""Stage 3: rules based dictionary lookup over ad_type_done rows, for ad where content_type == AD"""

import logging
import os
from datetime import datetime, timezone
from typing import Any

import tqdm
from sqlalchemy import bindparam, select
from sqlalchemy.orm import sessionmaker

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.dictionary.matcher import BrandDictionary, DictMatch
from quotaclimat.utils.logger import getLogger


def select_pending(engine, limit: int | None) -> list[tuple[str, list[dict] | None, str | None]]:
    Session = sessionmaker(bind=engine)
    with Session() as session:
        stmt = (
            select(Ad.id, Ad.prediction, Ad.transcript)
            .where(Ad.prediction_status == "ad_type_done")
            .order_by(Ad.first_detection_date.desc())
        )
        if limit:
            stmt = stmt.limit(limit)
        return [(r[0], r[1], r[2]) for r in session.execute(stmt).all()]


def _ad_type_entry(prediction: list[dict] | None) -> dict:
    if not prediction:
        return {}
    for entry in prediction:
        if entry.get("stage") == "ad_type":
            return entry.get("raw_response") or {}
    return {}


def _build_dict_match_entry(match: DictMatch | None, brand_in: str | None) -> dict | None:
    timestamp = datetime.now(timezone.utc).isoformat()

    if match is None:
        if not brand_in:
            return None
        return {
            "stage": "dict_match",
            "timestamp": timestamp,
            "method": "dict_miss",
            "brand_in": brand_in,
        }

    entry: dict[str, Any] = {
        "stage": "dict_match",
        "timestamp": timestamp,
        "method": match.method,
        "matched_brand": match.matched_brand,
        "sector_code": match.sector_code,
        "subcat_code": match.subcat_code,
    }
    if match.matched_keyword is not None:
        entry["matched_keyword"] = match.matched_keyword
    if match.rule_keywords is not None:
        entry["rule_keywords"] = match.rule_keywords
    if match.tried_rules is not None:
        entry["tried_rules"] = match.tried_rules
    if match.haystack is not None:
        entry["haystack"] = match.haystack
    return entry


_AD = Ad.__table__
_UPDATE = (
    _AD.update()
    .where(_AD.c.id == bindparam("b_id"))
    .values(
        prediction=bindparam("b_prediction"),
        prediction_status=bindparam("b_status"),
        prediction_method=bindparam("b_method"),
        predicted_sector=bindparam("b_sector"),
        predicted_product_category=bindparam("b_subcat"),
        predicted_brand=bindparam("b_brand"),
    )
)


def _flush(session_factory, rows: list[dict]) -> None:
    if not rows:
        return
    with session_factory() as session:
        session.execute(_UPDATE, rows)
        session.commit()


def run(batch_size: int = 500, limit: int | None = None) -> dict[str, int]:
    engine = connect_to_db(use_custom_json_serializer=True)
    session_factory = sessionmaker(bind=engine)
    pending = select_pending(engine, limit)
    if not pending:
        logging.info("[dict_match] nothing to process")
        engine.dispose()
        return {}

    bd = BrandDictionary()
    counts: dict[str, int] = {}
    buf: list[dict] = []
    progress = tqdm.tqdm(pending, desc="dict_match", smoothing=0.05)

    try:
        for ad_id, prediction, transcript in progress:
            ad_type = _ad_type_entry(prediction)

            if ad_type.get("content_type") != "AD":
                continue

            brand = ad_type.get("brand_name")
            product = ad_type.get("product_name")
            llm_sector = ad_type.get("sector_code")

            match = bd.match(brand, product, transcript)

            if match is None:
                status, method = "dict_miss", "dict_miss"
                sector = llm_sector
                subcat = None
                canonical_brand = None
            elif match.method == "dict_tier3_no_kw":
                status, method = "dict_tier3_no_kw", "dict_tier3_no_kw"
                sector = llm_sector
                subcat = None
                canonical_brand = None
            else:
                status = match.method
                method = match.method
                sector = match.sector_code
                subcat = match.subcat_code
                canonical_brand = match.matched_brand

            existing = list(prediction) if prediction else []
            audit_entry = _build_dict_match_entry(match, brand)
            if audit_entry is not None:
                existing = [e for e in existing if e.get("stage") != "dict_match"]
                existing.append(audit_entry)

            counts[status] = counts.get(status, 0) + 1
            buf.append({
                "b_id": ad_id,
                "b_prediction": existing,
                "b_status": status,
                "b_method": method,
                "b_sector": sector,
                "b_subcat": subcat,
                "b_brand": canonical_brand,
            })
            if len(buf) >= batch_size:
                _flush(session_factory, buf)
                buf = []
            progress.set_postfix(counts)
    finally:
        _flush(session_factory, buf)
        progress.close()
        engine.dispose()

    logging.info("[dict_match] done. %s", counts)
    return counts


if __name__ == "__main__":
    getLogger()
    run(
        batch_size=int(os.environ.get("BATCH_SIZE", 500)),
        limit=int(os.environ.get("LIMIT", 0)) or None,
    )