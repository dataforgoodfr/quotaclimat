"""Stage 3: which of a fixed list of generic green terms each ad uses.
Run with ``python -m quotaclimat.data_ingestion.advertising.s03_classification.environmental_content.e3_scan_terms``.

Every ad is scanned again on each run, as its texts or records may have changed since, but
only the ads whose terms changed are written.
"""

import logging
import os
from collections import Counter
from collections.abc import Sequence

from sqlalchemy import Select, bindparam, or_, select, update

from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.environmental_content.terms import (
    VERSION,
    scan_ad,
)
from quotaclimat.data_ingestion.advertising.s03_classification.utils import (
    database,
    stage_version_stamp,
)
from quotaclimat.utils.logger import getLogger

STAGE = "environmental_content_terms"
TERMS_UPDATE = (
    update(Ad)
    .where(Ad.id == bindparam("b_id"))
    .values(environmental_content_terms=bindparam("b_terms"))
)


def select_ads(sectors: Sequence[str] | None) -> Select:
    stmt = (
        select(
            Ad.id,
            Ad.predicted_brand,
            Ad.transcript,
            Ad.ocr_transcript,
            Ad.environmental_content_final["raw_response"].label("final"),
            Ad.environmental_content_prediction["raw_response"].label("prediction"),
            Ad.environmental_content_terms,
        )
        .where(Ad.fragment_type == "AD")
        .where(or_(Ad.transcript.is_not(None), Ad.ocr_transcript.is_not(None)))
        .where(database.IN_POOL)
    )
    if sectors:
        stmt = stmt.where(Ad.predicted_sector.in_(sectors))
    return stmt


def run(sectors: Sequence[str] | None = None, text_only: bool = False) -> None:
    sources = "text" if text_only else "text+model"
    stamp = stage_version_stamp.new_stamp(STAGE, terms=VERSION, sources=sources)
    counts = Counter()

    def changed_terms():
        for ad in database.stream_rows(select_ads(sectors)):
            record = {} if text_only else ad["final"] or ad["prediction"] or {}
            result = scan_ad(
                ad["transcript"], ad["ocr_transcript"], record, ad["predicted_brand"]
            )
            result["model_searched"] = bool(record)
            counts["scanned"] += 1
            counts["with a term"] += bool(result["terms"])
            previous = ad["environmental_content_terms"] or {}
            if (
                previous.get("raw_response") != result
                or previous.get("versions") != stamp["versions"]
            ):
                entry = {**stamp, "raw_response": result, "error": None}
                yield {"b_id": ad["id"], "b_terms": entry}

    written = database.save_in_chunks(TERMS_UPDATE, changed_terms())
    logging.info(
        "[%s] %d ad(s) scanned (%s), %d with a term (%s), %d changed and written",
        STAGE,
        counts["scanned"],
        sources,
        counts["with a term"],
        VERSION,
        written,
    )


if __name__ == "__main__":
    getLogger()
    sectors = os.environ.get("SECTOR", "")
    run(
        sectors=[s.strip() for s in sectors.split(",") if s.strip()],
        text_only=os.environ.get("TEXT_ONLY") == "1",
    )
