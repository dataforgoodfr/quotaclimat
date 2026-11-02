"""Database operations to merge ads split by the detection into one ad (A then B -> AB).

Reusable by every script that merges ads (automatic detection of the split pieces, manual merges...).
The functions only add to the session, they never commit: the caller decides of the transaction.

A merge is done in two steps:
1. create_merged_ad: the merged ad, from the source ads in broadcast order. Its fingerprints are the
   ones of the source ads one after the other, so that the detection recognises the whole ad when the
   pieces are broadcast together (the longest known ads are tried first). When the source ads are
   classified advertisings with the same brand, it takes their classification if they have the same sector
   and category (inherit_classification, status merge_inherited), or else the one the brand dictionary of
   the classification gives to their whole transcript (status of the dictionary); otherwise it has no
   prediction, and the exportation and classification steps process it like a new ad;
2. merge_occurrences: for each broadcast, one occurrence of the merged ad replaces the occurrences of
   the pieces (and of the short segments between them), which are soft deleted (deleted_at).

Both steps are logged: advertising.ad_merge (the merge rule) is kept when the detection reprocesses a
window, advertising.ad_merge_occurrence is deleted with the occurrences of the window (foreign keys on
delete cascade), and the merge is redone on the new occurrences.
revert_merge undoes the occurrence merges of a merged ad.
inherit_classification_of_merges gives their classification to the merged ads created before it existed,
or whose pieces were classified after the merge.
"""

import hashlib
import re
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, datetime

import pandas as pd
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from postgres.schemas.advertising.models import (
    Ad,
    Ad_Merge,
    Ad_Merge_Occurrence,
    Ad_Occurrence,
)
from quotaclimat.data_ingestion.advertising.s03_classification.dictionary.matcher import (
    BrandDictionary,
)
from quotaclimat.data_ingestion.advertising.s03_classification.pipeline.e03_dict_match import (
    _build_dict_match_entry,
)
from quotaclimat.data_ingestion.advertising.s03_classification.prediction_status import (
    AD_CONTENT_TYPE,
    FINAL_PREDICTION_STATUSES,
    SUBCATEGORY_PENDING_STATUSES,
)

MERGE_STATUS_ACTIVE = "active"
MERGE_STATUS_REVERTED = "reverted"
ROLE_PIECE = "piece"
ROLE_ABSORBED = "absorbed"
# prediction_method and prediction stage of a merged ad classified like its pieces
INHERITED_METHOD = "merge_inherited"
# why the classification of the pieces is not inherited (logged by inherit_classification_of_merges)
INHERITED = "inherited"
INHERITED_FROM_DICTIONARY = "inherited_from_dictionary"
NOT_CLASSIFIED = "piece_not_classified"
SUBCATEGORY_PENDING = "piece_waiting_for_subcategory"
OTHER_BRAND = "other_brand"
OTHER_SECTOR = "other_sector"
OTHER_CATEGORY = "other_category"
# statuses of a dictionary match that decide the sector and the category (not *_no_kw)
DICTIONARY_DECISIONS = ("dict_tier1", "dict_tier2", "dict_tier3")
# copy of the brand dictionary in the database (dbt seeds of the Dictionnaire_marques_secteurs Google Sheet)
DICTIONARY_TABLES = (
    "ref_ome_marques_type_1",
    "ref_ome_marques_type_2",
    "ref_ome_marques_type_3",
)
# source ad columns read to inherit their classification (not the fingerprints, heavy)
CLASSIFICATION_COLUMNS = (
    Ad.id,
    Ad.duration_sec,
    Ad.fragment_type,
    Ad.transcript,
    Ad.prediction,
    Ad.prediction_status,
    Ad.prediction_confidence,
    Ad.prediction_method,
    Ad.predicted_brand,
    Ad.predicted_sector,
    Ad.predicted_product_category,
)


class MergeRevertedError(ValueError):
    """The merge was reverted (judged wrong): it is reactivated only on explicit request."""


def _utcnow() -> datetime:
    # occurrence dates are stored as naive UTC timestamps
    return datetime.now(UTC).replace(tzinfo=None)


def merged_ad_id(source_ad_ids: Sequence[str]) -> str:
    """Stable id of the ad merging the given ads, in broadcast order."""
    raw = "merge|" + "|".join(source_ad_ids)
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def merged_occurrence_id(
    ad_id: str, occurrence_date: datetime, channel_name: str
) -> str:
    """Occurrence id, same formula as the detection (s01_detection/e06_export_classification.py)."""
    occurrence_sec = occurrence_date.replace(tzinfo=UTC).timestamp()
    raw = f"{ad_id}|{occurrence_sec}|{channel_name}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def concatenate_chunks(source_ads: Sequence[Ad]) -> list[dict]:
    """Fingerprints of the source ads one after the other, for each fingerprint hash shared by all of them."""
    entries_by_ad = [
        {entry.get("hash"): entry for entry in (ad.chunks or [])} for ad in source_ads
    ]
    return [
        {
            "hash": chunk_hash,
            "fingerprints": [
                fingerprint
                for entries in entries_by_ad
                for fingerprint in entries[chunk_hash].get("fingerprints", [])
            ],
        }
        for chunk_hash in entries_by_ad[0]
        if all(chunk_hash in entries for entries in entries_by_ad[1:])
    ]


def brand_key(brand: str | None) -> str:
    """Brand compared without case, spaces nor punctuation, like the detection of the split pieces
    (s04_consolidation/e01_merge_split_ads/detect.py)."""
    return re.sub(r"[\W_]", "", (brand or "").lower())


def _ad_type_entry(prediction) -> dict | None:
    if not isinstance(prediction, list):
        return None
    return next(
        (e for e in prediction if isinstance(e, dict) and e.get("stage") == "ad_type"),
        None,
    )


def load_brand_dictionary(session: Session) -> BrandDictionary | None:
    """The brand dictionary of the classification (s03_classification/dictionary/matcher.py) from its copy in
    the database: the dbt seeds advertising.ref_ome_marques_type_1/2/3, downloaded from the
    Dictionnaire_marques_secteurs Google Sheet (my_dbt_project/external_sources.yml), as the merge job runs
    without the Excel file the classification reads (s03_classification/reference_data/, not versioned). The
    rows are read in the order the seeds were loaded (the order of the tabs): the first rule of a brand with
    a keyword hit wins. None when a table is missing (extended perimeter, tests)."""
    frames = []
    for table in DICTIONARY_TABLES:
        if (
            session.execute(
                text("SELECT to_regclass(:name)"), {"name": f"advertising.{table}"}
            ).scalar()
            is None
        ):
            return None
        frames.append(
            pd.DataFrame(
                session.execute(
                    text(f"SELECT * FROM advertising.{table} ORDER BY ctid")
                )
                .mappings()
                .all()
            )
        )
    return BrandDictionary.from_frames(*frames)


def inherited_classification(
    source_ads: Sequence, dictionary: BrandDictionary | None = None
) -> tuple[dict | None, str]:
    """Classification of the merged ad taken from its source ads (Ad, or rows of CLASSIFICATION_COLUMNS),
    in broadcast order, and why. Only for source ads that are all advertisings with a final status and their
    sub-category found (not waiting for stage 4), with the same brand (compared with brand_key):

    1. INHERITED: they all have the same sector and category (an empty category included). Status
       merge_inherited, the brand as spelled for the longest piece, the lowest confidence;
    2. INHERITED_FROM_DICTIONARY: they disagree on the sector or the category, and the brand dictionary of
       the classification (dictionary), applied like stage 3 (e03_dict_match.py) to the product names and
       the transcripts of the pieces one after the other, decides: the brand alone (tier 1) or a product
       keyword (tiers 2 and 3). Each piece was classified on its own part of the transcript, so with its own
       keywords: the whole transcript holds all of them. Status and brand of the dictionary match
       (dict_tier1, dict_tier2, dict_tier3), method merge_inherited+<status>, no confidence.
    Otherwise (None, reason): the merged ad goes through the classification.
    Both keep the transcripts of the pieces one after the other (the transcription stage only processes ads
    without transcript, and would mark the merged ad no_data until its video is exported), the ad_type
    prediction of the longest piece and a merge_inherited entry with the statuses and methods of the pieces
    (and the dict_match entry of the dictionary, like stage 3).
    """
    for ad in source_ads:
        if (
            ad.fragment_type != AD_CONTENT_TYPE
            or ad.prediction_status not in FINAL_PREDICTION_STATUSES
        ):
            return None, NOT_CLASSIFIED
        if ad.prediction_status in SUBCATEGORY_PENDING_STATUSES:
            return None, SUBCATEGORY_PENDING
    keys = {brand_key(ad.predicted_brand) for ad in source_ads}
    if len(keys) != 1 or keys == {""}:
        return None, OTHER_BRAND

    longest = max(source_ads, key=lambda ad: ad.duration_sec)
    transcript = " ".join(ad.transcript for ad in source_ads if ad.transcript)
    merge_entry = {
        "stage": INHERITED_METHOD,
        "source_ad_ids": [ad.id for ad in source_ads],
        "source_prediction_statuses": [ad.prediction_status for ad in source_ads],
        "source_prediction_methods": [ad.prediction_method for ad in source_ads],
    }
    ad_type_entries = [
        entry for entry in [_ad_type_entry(longest.prediction)] if entry is not None
    ]

    if len({ad.predicted_sector for ad in source_ads}) != 1 or (
        source_ads[0].predicted_sector is None
    ):
        disagreement = OTHER_SECTOR
    elif len({ad.predicted_product_category for ad in source_ads}) != 1:
        disagreement = OTHER_CATEGORY
    else:
        confidences = [
            ad.prediction_confidence
            for ad in source_ads
            if ad.prediction_confidence is not None
        ]
        return (
            dict(
                prediction_status="merge_inherited",
                prediction_method=INHERITED_METHOD,
                fragment_type=AD_CONTENT_TYPE,
                predicted_brand=longest.predicted_brand,
                predicted_sector=longest.predicted_sector,
                predicted_product_category=longest.predicted_product_category,
                prediction_confidence=min(confidences) if confidences else None,
                prediction=ad_type_entries + [merge_entry],
                transcript=transcript,
            ),
            INHERITED,
        )

    if dictionary is None:
        return None, disagreement
    product_names = " ".join(
        ((_ad_type_entry(ad.prediction) or {}).get("raw_response") or {}).get(
            "product_name"
        )
        or ""
        for ad in source_ads
    )
    match = dictionary.match(longest.predicted_brand, product_names, transcript)
    if match is None or match.method not in DICTIONARY_DECISIONS:
        return None, disagreement
    return (
        dict(
            prediction_status=match.method,
            prediction_method=f"{INHERITED_METHOD}+{match.method}",
            fragment_type=AD_CONTENT_TYPE,
            predicted_brand=match.matched_brand,
            predicted_sector=match.sector_code,
            predicted_product_category=match.subcat_code,
            prediction_confidence=None,
            prediction=ad_type_entries
            + [
                _build_dict_match_entry(match, longest.predicted_brand),
                merge_entry | {"rule": INHERITED_FROM_DICTIONARY},
            ],
            transcript=transcript,
        ),
        INHERITED_FROM_DICTIONARY,
    )


def inherit_classification(
    session: Session,
    merged_ad_id: str,
    source_ads: Sequence,
    dictionary: BrandDictionary | None = None,
) -> str:
    """Give the merged ad the classification of its source ads (inherited_classification), when it has
    no classification yet (prediction_status empty): a classification of the merged ad itself, or a
    manual correction, is never replaced. Its transcript, when it has one, is kept.
    Returns the reason of inherited_classification (INHERITED or INHERITED_FROM_DICTIONARY when classified).
    """
    values, reason = inherited_classification(source_ads, dictionary)
    if values is None:
        return reason
    transcript = values.pop("transcript")
    session.execute(
        update(Ad)
        .where(Ad.id == merged_ad_id)
        .where(Ad.prediction_status.is_(None))
        .values(transcript=func.coalesce(Ad.transcript, transcript), **values)
        .execution_options(synchronize_session="fetch")
    )
    return reason


def inherit_classification_of_merges(
    session: Session,
    dry_run: bool = False,
    batch_size: int = 1000,
    dictionary: BrandDictionary | None = None,
) -> Counter:
    """inherit_classification for every active merge whose merged ad has no classification yet: the
    merges created before the merged ads took the classification of their pieces, or whose pieces were
    classified after the merge.

    dry_run: nothing is written, the counts say what would be done.
    Returns the number of merges per reason of inherited_classification.
    """
    pending = session.execute(
        select(Ad_Merge.merged_ad_id, Ad_Merge.source_ad_ids)
        .join(Ad, Ad.id == Ad_Merge.merged_ad_id)
        .where(Ad_Merge.status == MERGE_STATUS_ACTIVE)
        .where(Ad.prediction_status.is_(None))
        .order_by(Ad_Merge.merged_ad_id)
    ).all()
    reasons = Counter()
    for start in range(0, len(pending), batch_size):
        batch = pending[start : start + batch_size]
        source_ids = {ad_id for _, ids in batch for ad_id in ids}
        sources = {
            row.id: row
            for row in session.execute(
                select(*CLASSIFICATION_COLUMNS).where(Ad.id.in_(source_ids))
            )
        }
        for merged_id, source_ad_ids in batch:
            if any(ad_id not in sources for ad_id in source_ad_ids):
                reasons[NOT_CLASSIFIED] += 1
                continue
            source_ads = [sources[ad_id] for ad_id in source_ad_ids]
            if dry_run:
                reasons[inherited_classification(source_ads, dictionary)[1]] += 1
            else:
                reasons[
                    inherit_classification(session, merged_id, source_ads, dictionary)
                ] += 1
        session.flush()
    return reasons


def create_merged_ad(
    session: Session,
    source_ad_ids: Sequence[str],
    method: str,
    duration_sec: float | None = None,
    details: dict | None = None,
    reactivate_reverted: bool = False,
    dictionary: BrandDictionary | None = None,
) -> str:
    """Create the ad merging the given ads (in broadcast order) and its advertising.ad_merge row.

    The merged ad takes the classification of the source ads when they agree, or the one the brand
    dictionary (load_brand_dictionary) gives to their whole transcript (inherit_classification).
    Idempotent: when the merged ad already exists, its duration and details stay the ones of the
    creation, and it takes the classification of the source ads if it still has none. A reverted merge
    raises MergeRevertedError, unless reactivate_reverted (its fingerprints are then restored): an
    automatic script must not redo a merge reverted by a human.
    duration_sec defaults to the sum of the source durations.
    Returns the id of the merged ad.
    """
    source_ad_ids = list(source_ad_ids)
    if len(source_ad_ids) < 2:
        raise ValueError(f"At least 2 ads are needed to merge, got {source_ad_ids}")
    ads_by_id = {
        ad.id: ad
        for ad in session.scalars(select(Ad).where(Ad.id.in_(source_ad_ids))).all()
    }
    missing = [ad_id for ad_id in source_ad_ids if ad_id not in ads_by_id]
    if missing:
        raise ValueError(f"Unknown ads: {missing}")
    source_ads = [ads_by_id[ad_id] for ad_id in source_ad_ids]

    ad_id = merged_ad_id(source_ad_ids)
    merge = session.get(Ad_Merge, ad_id)
    if (
        merge is not None
        and merge.status == MERGE_STATUS_REVERTED
        and not reactivate_reverted
    ):
        raise MergeRevertedError(f"The merge {ad_id} of {source_ad_ids} was reverted")
    chunks = concatenate_chunks(source_ads)
    merged_ad = session.get(Ad, ad_id)
    if merged_ad is None:
        session.add(
            Ad(
                id=ad_id,
                first_detection_date=min(ad.first_detection_date for ad in source_ads),
                duration_sec=duration_sec
                if duration_sec is not None
                else sum(ad.duration_sec for ad in source_ads),
                chunks=chunks,
                # same type as a new ad from the detection: exported and classified by the next steps,
                # unless it inherits the classification of the source ads below
                fragment_type="advertising",
            )
        )
    else:
        merged_ad.chunks = chunks
    session.flush()
    inherit_classification(session, ad_id, source_ads, dictionary)

    now = _utcnow()
    if merge is None:
        session.add(
            Ad_Merge(
                merged_ad_id=ad_id,
                source_ad_ids=source_ad_ids,
                method=method,
                status=MERGE_STATUS_ACTIVE,
                details=details,
                created_at=now,
                updated_at=now,
            )
        )
    else:
        # the details of the creation are kept
        merge.status = MERGE_STATUS_ACTIVE
        merge.updated_at = now
    session.flush()
    return ad_id


def merge_occurrences(
    session: Session,
    merged_ad_id: str,
    piece_occurrence_ids: Sequence[str],
    absorbed_occurrence_ids: Sequence[str] = (),
) -> str:
    """Replace one broadcast of the pieces of a merged ad by one occurrence of the merged ad.

    piece_occurrence_ids: occurrences of the source ads of the merge, in broadcast order (checked).
    absorbed_occurrence_ids: other occurrences inside the merged ad (short segments between two pieces).
    All of them are soft deleted and logged in advertising.ad_merge_occurrence. The merged occurrence
    starts with the first piece. Returns its id.
    """
    merge = session.get(Ad_Merge, merged_ad_id)
    if merge is None or merge.status != MERGE_STATUS_ACTIVE:
        raise ValueError(f"No active merge for the ad {merged_ad_id}")

    piece_occurrence_ids = list(piece_occurrence_ids)
    absorbed_occurrence_ids = list(absorbed_occurrence_ids)
    all_ids = piece_occurrence_ids + absorbed_occurrence_ids
    if len(set(all_ids)) != len(all_ids):
        raise ValueError(f"Occurrence given twice: {all_ids}")
    occurrences = {
        occ.id: occ
        for occ in session.scalars(
            select(Ad_Occurrence)
            .where(Ad_Occurrence.id.in_(all_ids))
            .where(Ad_Occurrence.deleted_at.is_(None))
        ).all()
    }
    missing = [occ_id for occ_id in all_ids if occ_id not in occurrences]
    if missing:
        raise ValueError(f"Unknown or deleted occurrences: {missing}")
    pieces = [occurrences[occ_id] for occ_id in piece_occurrence_ids]
    if [occ.ad_id for occ in pieces] != list(merge.source_ad_ids):
        raise ValueError(
            f"The pieces {piece_occurrence_ids} are not occurrences of {list(merge.source_ad_ids)}"
        )
    if [occ.occurrence_date for occ in pieces] != sorted(
        occ.occurrence_date for occ in pieces
    ):
        raise ValueError(
            f"The pieces {piece_occurrence_ids} are not in broadcast order"
        )
    channels = {occ.channel_name for occ in occurrences.values()}
    if len(channels) != 1:
        raise ValueError(f"Occurrences on several channels: {channels}")

    channel_name = pieces[0].channel_name
    start = pieces[0].occurrence_date
    new_id = merged_occurrence_id(merged_ad_id, start, channel_name)
    session.execute(
        insert(Ad_Occurrence)
        .values(
            id=new_id,
            occurrence_date=start,
            channel_name=channel_name,
            ad_id=merged_ad_id,
        )
        .on_conflict_do_update(
            index_elements=[Ad_Occurrence.id], set_={"deleted_at": None}
        )
    )

    now = _utcnow()
    session.execute(
        update(Ad_Occurrence)
        .where(Ad_Occurrence.id.in_(all_ids))
        .values(deleted_at=now)
    )
    roles = {occ_id: ROLE_PIECE for occ_id in piece_occurrence_ids} | {
        occ_id: ROLE_ABSORBED for occ_id in absorbed_occurrence_ids
    }
    ordered = sorted(
        occurrences.values(), key=lambda occ: (occ.occurrence_date, occ.id)
    )
    for position, occ in enumerate(ordered):
        session.execute(
            insert(Ad_Merge_Occurrence)
            .values(
                merged_occurrence_id=new_id,
                source_occurrence_id=occ.id,
                merged_ad_id=merged_ad_id,
                source_ad_id=occ.ad_id,
                role=roles[occ.id],
                position=position,
                created_at=now,
            )
            .on_conflict_do_update(
                index_elements=[
                    Ad_Merge_Occurrence.merged_occurrence_id,
                    Ad_Merge_Occurrence.source_occurrence_id,
                ],
                set_={"reverted_at": None, "created_at": now},
            )
        )
    session.flush()
    return new_id


def revert_merge(session: Session, merged_ad_id: str) -> int:
    """Undo the occurrence merges of a merged ad.

    The source occurrences still in the database are restored, the occurrences of the merged ad are
    soft deleted, the merge is marked as reverted and the fingerprints of the merged ad are emptied so
    that the detection stops recognising it (the ad itself is kept for the history).
    Returns the number of restored occurrences.
    """
    merge = session.get(Ad_Merge, merged_ad_id)
    if merge is None:
        raise ValueError(f"No merge for the ad {merged_ad_id}")

    now = _utcnow()
    source_ids = select(Ad_Merge_Occurrence.source_occurrence_id).where(
        Ad_Merge_Occurrence.merged_ad_id == merged_ad_id,
        Ad_Merge_Occurrence.reverted_at.is_(None),
    )
    restored = session.execute(
        update(Ad_Occurrence)
        .where(Ad_Occurrence.id.in_(source_ids))
        .where(Ad_Occurrence.deleted_at.is_not(None))
        .values(deleted_at=None)
    ).rowcount
    session.execute(
        update(Ad_Occurrence)
        .where(Ad_Occurrence.ad_id == merged_ad_id)
        .where(Ad_Occurrence.deleted_at.is_(None))
        .values(deleted_at=now)
    )
    session.execute(
        update(Ad_Merge_Occurrence)
        .where(Ad_Merge_Occurrence.merged_ad_id == merged_ad_id)
        .where(Ad_Merge_Occurrence.reverted_at.is_(None))
        .values(reverted_at=now)
    )
    merge.status = MERGE_STATUS_REVERTED
    merge.updated_at = now
    merged_ad = session.get(Ad, merged_ad_id)
    if merged_ad is not None:
        merged_ad.chunks = []
    session.flush()
    return restored
