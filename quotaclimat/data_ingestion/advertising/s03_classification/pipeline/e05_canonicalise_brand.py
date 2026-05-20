"""Stage 5: brand canonicalisation"""

import logging
import os
from collections import defaultdict
from pathlib import Path

import pandas as pd
from sqlalchemy import bindparam, select
from sqlalchemy.orm import sessionmaker
from thefuzz import fuzz

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.dictionary.matcher import \
    BrandDictionary
from quotaclimat.data_ingestion.advertising.s03_classification.dictionary.normalize import (
    normalize, nospace)
from quotaclimat.utils.logger import getLogger

LEADING_NOISE = {"le", "la", "les", "l", "de", "du", "des", "st", "saint"}
DICT_FUZZY_THRESHOLD = 92
DICT_MAX_LENGTH_DIFF = 2


def _ad_type_entry(prediction: list[dict] | None) -> dict:
    if not prediction:
        return {}
    for entry in prediction:
        if entry.get("stage") == "ad_type":
            return entry.get("raw_response") or {}
    return {}


def _select_rows(engine, recanonicalize_existing: bool = False) -> pd.DataFrame:
    """Rows with an LLM brand to canonicalise.

    By default, only process rows that do not have a canonical brand yet.
    recanonicalize_existing=True to rerun if changes to merge rules.
    """
    Session = sessionmaker(bind=engine)
    with Session() as session:
        stmt = select(
            Ad.id, Ad.prediction, Ad.predicted_sector, Ad.predicted_brand
        ).where(
            Ad.prediction_status.isnot(None)
        )
        if not recanonicalize_existing:
            stmt = stmt.where(Ad.predicted_brand.is_(None))
        rows = session.execute(stmt).all()
    out = []
    for ad_id, prediction, sector, predicted_brand in rows:
        ad_type = _ad_type_entry(prediction)
        brand = predicted_brand if recanonicalize_existing else ad_type.get("brand_name")
        if not brand:
            continue
        out.append(
            {
                "ad_id": ad_id,
                "brand_raw": brand,
                "brand_clean": normalize(brand),
                "sector": sector,
            }
        )
    return pd.DataFrame(out)


def _build_dict_lookup(brand_dict: BrandDictionary) -> dict[str, str]:
    """brand_key (nospace+normalised): display name from the dictionary"""
    out: dict[str, str] = {}
    for key, rows in brand_dict.t3.items():
        out[key] = rows[0][3]
    for key, entry in brand_dict.t2.items():
        out[key] = entry["brand_raw"]
    for key, (_sector, _subcat, brand_raw) in brand_dict.t1.items():
        out[key] = brand_raw
    return out


def build_canonical_map(
    df: pd.DataFrame,
    *,
    fuzzy_threshold: int = 88,
    short_threshold: int = 92,
    short_len: int = 5,
    dict_lookup: dict[str, str] | None = None,
) -> dict[str, str]:
    dict_lookup = dict_lookup or {}
    freq = df["brand_clean"].value_counts().to_dict()
    brand_keys = df["brand_clean"].map(nospace)
    group_freq = (
        df.assign(_brand_key=brand_keys)
        .groupby("_brand_key")["brand_clean"]
        .agg(lambda values: sum(freq.get(value, 0) for value in values.unique()))
        .to_dict()
    )
    sector_map = (
        df.assign(_brand_key=brand_keys)
        .groupby("_brand_key")["sector"]
        .agg(lambda s: set(s.dropna()))
        .to_dict()
    )

    def shares_sector(a_key: str, b_key: str) -> bool:
        return bool(sector_map.get(a_key, set()) & sector_map.get(b_key, set()))

    def pick_readable(variants: list[str]) -> str:
        """among spacing variants of the same brand, pick the best display form"""
        return max(
            variants,
            key=lambda v: (
                freq.get(v, 0),  # most frequent wins
                " " in v,  # prefer spaced form
                not any(v.startswith(a + " ") for a in LEADING_NOISE),  # avoid le la"
            ),
        )

    def dict_display_for_key(key: str) -> str | None:
        if key in dict_lookup:
            return dict_lookup[key]

        matches = [
            (fuzz.ratio(key, dict_key), abs(len(key) - len(dict_key)), dict_key)
            for dict_key in dict_lookup
            if abs(len(key) - len(dict_key)) <= DICT_MAX_LENGTH_DIFF
        ]
        if not matches:
            return None

        score, _length_diff, dict_key = max(matches)
        if score >= DICT_FUZZY_THRESHOLD:
            return dict_lookup[dict_key]
        return None

    nospace_groups: dict[str, list[str]] = defaultdict(list)
    for clean in df["brand_clean"].unique():
        nospace_groups[nospace(clean)].append(clean)

    nospace_to_readable = {
        ns: dict_display_for_key(ns) or pick_readable(vs)
        for ns, vs in nospace_groups.items()
    }

    keys_by_freq = sorted(
        nospace_to_readable,
        key=lambda k: (
            dict_display_for_key(k) is None,  # prefer dictionary spellings
            -group_freq.get(k, 0),
        ),
    )
    canonical_of: dict[str, str] = {}
    for key in keys_by_freq:
        threshold = short_threshold if len(key) <= short_len else fuzzy_threshold
        match = next(
            (
                ck
                for ck in set(canonical_of.values())
                if fuzz.ratio(key, ck) >= threshold
                and shares_sector(key, ck)
            ),
            None,
        )
        canonical_of[key] = match if match else key

    return {
        clean: nospace_to_readable[canonical_of[nospace(clean)]]
        for clean in df["brand_clean"].unique()
    }


_AD = Ad.__table__
_UPDATE = (
    _AD.update()
    .where(_AD.c.id == bindparam("b_id"))
    .values(predicted_brand=bindparam("b_brand"))
)


def _flush(session_factory, rows: list[dict], batch_size: int) -> None:
    if not rows:
        return
    with session_factory() as session:
        for i in range(0, len(rows), batch_size):
            session.execute(_UPDATE, rows[i : i + batch_size])
        session.commit()


def run(
    audit_path: str = "data/brand_merges.csv",
    batch_size: int = 1000,
    recanonicalize_existing: bool = False,
) -> dict[str, int]:
    engine = connect_to_db(use_custom_json_serializer=True)
    session_factory = sessionmaker(bind=engine)
    df = _select_rows(engine, recanonicalize_existing=recanonicalize_existing)
    if df.empty:
        logging.info("[canonicalise_brand] nothing to process")
        engine.dispose()
        return {"processed": 0}

    brand_dict = BrandDictionary()
    dict_lookup = _build_dict_lookup(brand_dict)
    logging.info(
        "[canonicalise_brand] dict spellings available for %d brand keys",
        len(dict_lookup),
    )

    brand_map = build_canonical_map(df, dict_lookup=dict_lookup)
    df["brand_canonical"] = df["brand_clean"].map(brand_map)

    Path(audit_path).parent.mkdir(parents=True, exist_ok=True)
    audit = (
        df[["brand_clean", "brand_canonical"]]
        .drop_duplicates()
        .query("brand_clean != brand_canonical")
        .sort_values("brand_canonical")
    )
    audit.to_csv(audit_path, index=False)
    logging.info("[canonicalise_brand] wrote %d merges to %s", len(audit), audit_path)

    rows = [
        {"b_id": r.ad_id, "b_brand": r.brand_canonical}
        for r in df.itertuples(index=False)
    ]
    try:
        _flush(session_factory, rows, batch_size)
    finally:
        engine.dispose()

    logging.info("[canonicalise_brand] done. processed=%d", len(rows))
    return {"processed": len(rows), "merges": len(audit)}


if __name__ == "__main__":
    getLogger()
    run(
        audit_path=os.environ.get("AUDIT_PATH", "data/brand_merges.csv"),
        batch_size=int(os.environ.get("BATCH_SIZE", 1000)),
        recanonicalize_existing=os.environ.get("RECANONICALIZE_EXISTING", "0") == "1",
    )
