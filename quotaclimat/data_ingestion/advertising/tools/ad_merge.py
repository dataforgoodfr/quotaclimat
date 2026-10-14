"""Database operations to merge ads split by the detection into one ad (A then B -> AB).

Reusable by every script that merges ads (automatic detection of the split pieces, manual merges...).
The functions only add to the session, they never commit: the caller decides of the transaction.

A merge is done in two steps:
1. create_merged_ad: the merged ad, from the source ads in broadcast order. Its fingerprints are the
   ones of the source ads one after the other, so that the detection recognises the whole ad when the
   pieces are broadcast together (the longest known ads are tried first). It has no prediction, so the
   exportation and classification steps process it like a new ad;
2. merge_occurrences: for each broadcast, one occurrence of the merged ad replaces the occurrences of
   the pieces (and of the short segments between them), which are soft deleted (deleted_at).

Both steps are logged: advertising.ad_merge (the merge rule) is kept when the detection reprocesses a
window, advertising.ad_merge_occurrence is deleted with the occurrences of the window (foreign keys on
delete cascade), and the merge is redone on the new occurrences.
revert_merge undoes the occurrence merges of a merged ad.
"""

import hashlib
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from postgres.schemas.advertising.models import (
    Ad,
    Ad_Merge,
    Ad_Merge_Occurrence,
    Ad_Occurrence,
)

MERGE_STATUS_ACTIVE = "active"
MERGE_STATUS_REVERTED = "reverted"
ROLE_PIECE = "piece"
ROLE_ABSORBED = "absorbed"


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


def create_merged_ad(
    session: Session,
    source_ad_ids: Sequence[str],
    method: str,
    duration_sec: float | None = None,
    details: dict | None = None,
    reactivate_reverted: bool = False,
) -> str:
    """Create the ad merging the given ads (in broadcast order) and its advertising.ad_merge row.

    Idempotent: when the merged ad already exists, its duration and details stay the ones of the
    creation. A reverted merge raises MergeRevertedError, unless reactivate_reverted (its fingerprints
    are then restored): an automatic script must not redo a merge reverted by a human.
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
                # same type as a new ad from the detection: exported and classified by the next steps
                fragment_type="advertising",
            )
        )
    else:
        merged_ad.chunks = chunks

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
